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
        const properties = {
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

        assert.equal((await retainedTopics()).length, 3);
        await adapter.removeDevice("OLDLOCK");

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
