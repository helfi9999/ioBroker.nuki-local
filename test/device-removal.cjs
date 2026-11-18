const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const net = require("node:net");
const { setTimeout: delay } = require("node:timers/promises");
const path = require("node:path");
const vm = require("node:vm");
const { createRequire } = require("node:module");
const ts = require("typescript");
const { Level } = require("level");
const { Aedes } = require("aedes");
const createPersistence = require("aedes-persistence-level");

async function testMqttConnection(AdapterClass, directory) {
    const adapter = Object.create(AdapterClass.prototype);
    let connected = false;
    const writes = [];
    const sockets = [];
    Object.assign(adapter, {
        config: { mqttUsername: "test-user", mqttPassword: "test-password" },
        connectedNukiClients: new Set(),
        deviceTasks: new Map(),
        log: { info() {}, warn() {}, error() {}, debug() {} },
        setStateAsync: async (id, value, ack) => {
            assert.equal(id, "info.connection");
            assert.equal(ack, true);
            connected = value;
            writes.push(value);
        },
    });

    async function waitForWrites(count) {
        for (let i = 0; i < 100 && writes.length < count; i++) {
            await delay(20);
        }
        assert.equal(writes.length, count, "Expected connection state update");
    }

    async function connect(clientId, password, expectedCode) {
        const field = value => {
            const data = Buffer.from(value);
            return Buffer.concat([Buffer.from([0, data.length]), data]);
        };
        // MQTT 3.1.1 CONNECT: username, password and clean session.
        const body = Buffer.concat([
            Buffer.from([0, 4, 77, 81, 84, 84, 4, 194, 0, 10]),
            field(clientId),
            field("test-user"),
            field(password),
        ]);
        const socket = net.createConnection(adapter.mqttServer.address().port, "127.0.0.1");
        sockets.push(socket);
        await new Promise((resolve, reject) => {
            let response = Buffer.alloc(0);
            socket.on("error", reject);
            socket.once("close", () => reject(new Error("Connection closed before CONNACK")));
            socket.setTimeout(3000, () => reject(new Error("No MQTT CONNACK received")));
            socket.on("data", data => {
                response = Buffer.concat([response, data]);
                if (response.length >= 4) {
                    socket.setTimeout(0);
                    resolve(response.subarray(0, 4));
                }
            });
            socket.write(Buffer.concat([Buffer.from([16, body.length]), body]));
        }).then(response => assert.deepEqual([...response], [32, 2, 0, expectedCode]));
        return socket;
    }

    try {
        await adapter.startMqttBroker();
        assert.equal(connected, false);
        await connect("Nuki_00000001", "wrong-password", 5);
        assert.deepEqual(writes, []);

        const first = await connect("Nuki_00000001", "test-password", 0);
        await waitForWrites(1);
        assert.equal(connected, true);

        const second = await connect("SL3P_00000002", "test-password", 0);
        await waitForWrites(2);
        first.destroy();
        await waitForWrites(3);
        assert.equal(connected, true);

        second.destroy();
        await waitForWrites(4);
        assert.equal(connected, false);
        console.log("OK: MQTT rejected login, connection and multi-device disconnect verified.");
    } finally {
        for (const socket of sockets) {
            socket.destroy();
        }
        if (adapter.mqttBroker) {
            await new Promise(resolve => adapter.mqttBroker.close(resolve));
        }
        if (adapter.mqttServer?.listening) {
            await new Promise(resolve => adapter.mqttServer.close(resolve));
        }
        await adapter.mqttDatabase?.close();
        fs.rmSync(directory, { recursive: true, force: true });
    }
}

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
    let brokerTestDirectory;
    const testRequire = id => {
        if (id === "@iobroker/adapter-core") {
            return { Adapter: class {}, getAbsoluteInstanceDataDir: () => brokerTestDirectory };
        }
        if (id === "node:net") {
            return {
                ...net,
                createServer: handler => {
                    const server = net.createServer(handler);
                    server.listen = () => net.Server.prototype.listen.call(server, 0, "127.0.0.1");
                    return server;
                },
            };
        }
        return sourceRequire(id);
    };

    wrapper(testRequire, loaded, loaded.exports, filename, path.dirname(filename));
    const AdapterClass = loaded.exports;
    brokerTestDirectory = fs.mkdtempSync(path.join(os.tmpdir(), "nuki-connection-test-"));
    await testMqttConnection(AdapterClass, brokerTestDirectory);

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
