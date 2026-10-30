const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const vm = require("node:vm");
const { createRequire } = require("node:module");
const ts = require("typescript");
const { Level } = require("level");
const { Aedes } = require("aedes");
const createPersistence = require("aedes-persistence-level");

async function main() {
    const filename = path.resolve("src/main.ts");
    const source = fs.readFileSync(filename, "utf8");
    const compiled = ts.transpileModule(source, {
        compilerOptions: {
            target: ts.ScriptTarget.ES2022,
            module: ts.ModuleKind.CommonJS,
            esModuleInterop: true,
        },
    }).outputText;

    const loaded = { exports: {} };
    const wrapper = vm.runInThisContext(
        `(function(require, module, exports, __filename, __dirname) {
${compiled}
module.exports = NukiLocal;
})`,
        { filename },
    );
    const sourceRequire = createRequire(filename);
    const testRequire = id => (id === "@iobroker/adapter-core" ? { Adapter: class {} } : sourceRequire(id));

    wrapper(testRequire, loaded, loaded.exports, filename, path.dirname(filename));
    const AdapterClass = loaded.exports;

    const directory = fs.mkdtempSync(path.join(os.tmpdir(), "nuki-removal-test-"));
    let database;
    let broker;
    let persistence;

    async function open() {
        database = new Level(directory);
        persistence = createPersistence(database);
        broker = await Aedes.createBroker({ persistence });
    }

    async function close() {
        if (broker) {
            await new Promise((resolve, reject) => broker.close(error => (error ? reject(error) : resolve())));
            broker = undefined;
        }
        if (database) {
            await database.close();
            database = undefined;
        }
    }

    async function retainedTopics() {
        const topics = [];
        for await (const packet of persistence.createRetainedStreamCombi(["nuki/#"])) {
            topics.push(packet.topic);
        }
        return topics.sort();
    }

    try {
        await open();

        for (const topic of ["nuki/OLDLOCK/state", "nuki/OLDLOCK/name", "nuki/OLDLOCK2/state"]) {
            await new Promise((resolve, reject) =>
                broker.publish(
                    {
                        cmd: "publish",
                        topic,
                        payload: Buffer.from("test"),
                        qos: 1,
                        dup: false,
                        retain: true,
                    },
                    error => (error ? reject(error) : resolve()),
                ),
            );
        }

        const objects = new Map([
            ["OLDLOCK", { type: "device" }],
            ["OLDLOCK2", { type: "device" }],
        ]);
        const adapter = Object.create(AdapterClass.prototype);
        const states = new Map();
        const timers = new Map();
        let nextTimer = 0;
        const properties = {
            namespace: "nuki-local.0",
            removalAuthorizations: new Map(),
            setStateAsync: async (id, value, ack) => {
                assert.equal(ack, true);
                states.set(id, value);
            },
            setTimeout: callback => {
                const timer = ++nextTimer;
                timers.set(timer, callback);
                return timer;
            },
            clearTimeout: timer => timers.delete(timer),
            mqttBroker: broker,
            mqttPersistence: persistence,
            deviceTasks: new Map(),
            removedDevices: new Set(),
            initializedDevices: new Set(["OLDLOCK", "OLDLOCK2"]),
            initializingDevices: new Map(),
            webSmartlockIds: new Map(),
            authorizationNames: new Map(),
            lastLoggedStates: new Map([
                ["OLDLOCK.lockState", 1],
                ["OLDLOCK2.lockState", 3],
            ]),
            getObjectAsync: async id => objects.get(id),
            delObjectAsync: async (id, options) => {
                assert.equal(options.recursive, true);
                objects.delete(id);
            },
            log: { info() {}, warn() {}, error() {}, debug() {} },
        };

        for (const [key, value] of Object.entries(properties)) {
            Object.defineProperty(adapter, key, { value, writable: true, configurable: true });
        }

        async function write(deviceId, channel, command, value, ack = false) {
            await adapter.onStateChange(`nuki-local.0.${deviceId}.${channel}.${command}`, { val: value, ack });
        }

        async function assertUnchanged() {
            assert.equal(objects.has("OLDLOCK"), true);
            assert.equal(objects.has("OLDLOCK2"), true);
            assert.equal((await retainedTopics()).length, 3);
        }

        await assertUnchanged();

        // No confirmation: deletion must be rejected.
        await write("OLDLOCK", "maintenance", "removeDevice", true);
        await assertUnchanged();

        // Acknowledged state changes cannot authorize deletion.
        await write("OLDLOCK", "maintenance", "confirmRemoval", true, true);
        assert.equal(adapter.removalAuthorizations.size, 0);

        // The legacy command must remain disabled even while authorized.
        await write("OLDLOCK", "maintenance", "confirmRemoval", true);
        await write("OLDLOCK", "commands", "removeDevice", true);
        await assertUnchanged();

        // Explicitly withdrawing confirmation prevents deletion.
        await write("OLDLOCK", "maintenance", "confirmRemoval", false);
        await write("OLDLOCK", "maintenance", "removeDevice", true);
        await assertUnchanged();
        assert.equal(timers.size, 0);

        // Expired authorization must fail even before its timer callback runs.
        await write("OLDLOCK", "maintenance", "confirmRemoval", true);
        adapter.removalAuthorizations.get("OLDLOCK").expiresAt = Date.now() - 1;
        await write("OLDLOCK", "maintenance", "removeDevice", true);
        await assertUnchanged();

        // Timer expiry resets both the authorization and its visible state.
        await write("OLDLOCK", "maintenance", "confirmRemoval", true);
        const authorization = adapter.removalAuthorizations.get("OLDLOCK");
        const callback = timers.get(authorization.timer);
        timers.delete(authorization.timer);
        callback();
        assert.equal(adapter.removalAuthorizations.size, 0);
        assert.equal(states.get("OLDLOCK.maintenance.confirmRemoval"), false);
        await write("OLDLOCK", "maintenance", "removeDevice", true);
        await assertUnchanged();

        // Authorization applies only to the selected device.
        await write("OLDLOCK", "maintenance", "confirmRemoval", true);
        await write("OLDLOCK2", "maintenance", "removeDevice", true);
        await assertUnchanged();

        // Valid confirmation permits deletion once and is consumed.
        await write("OLDLOCK", "maintenance", "removeDevice", true);
        assert.equal(adapter.removalAuthorizations.size, 0);
        assert.equal(timers.size, 0);
        assert.equal(states.get("OLDLOCK.maintenance.confirmRemoval"), false);

        assert.equal(objects.has("OLDLOCK"), false);
        assert.equal(objects.has("OLDLOCK2"), true);
        assert.equal(adapter.initializedDevices.has("OLDLOCK"), false);
        assert.equal(adapter.initializedDevices.has("OLDLOCK2"), true);
        assert.equal(adapter.lastLoggedStates.has("OLDLOCK.lockState"), false);
        assert.equal(adapter.lastLoggedStates.has("OLDLOCK2.lockState"), true);
        assert.deepEqual(await retainedTopics(), ["nuki/OLDLOCK2/state"]);

        await assert.rejects(adapter.removeDevice("../OLDLOCK"));
        await assert.rejects(adapter.removeDevice("MISSING"));

        await close();
        await open();
        assert.deepEqual(await retainedTopics(), ["nuki/OLDLOCK2/state"]);

        console.log("OK: Altgerät entfernt, zweites Gerät erhalten, Löschung nach Datenbank-Neustart bestätigt.");
    } finally {
        await close();
        fs.rmSync(directory, { recursive: true, force: true });
    }
}

main().catch(error => {
    console.error(error);
    process.exitCode = 1;
});
