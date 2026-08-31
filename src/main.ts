import * as utils from "@iobroker/adapter-core";
import { createServer, type Server } from "node:net";
import { join } from "node:path";
import { mkdir } from "node:fs/promises";
import { Level } from "level";
import { Aedes, type Aedes as AedesInstance } from "aedes";
import type { AedesPersistence } from "aedes-persistence";

import aedesPersistenceLevel from "aedes-persistence-level";

const DEVICE_TYPES: Record<number, string> = {
    0: "Smart Lock",
    2: "Opener",
    3: "Smart Door",
    4: "Smart Lock 3.0 / 4th Generation",
    5: "Smart Lock Ultra / 5th Generation / Go",
};

const MODES: Record<number, string> = {
    2: "Door mode",
    3: "Continuous mode",
};

const LOCK_STATES: Record<number, string> = {
    0: "Nicht kalibriert",
    1: "Zugesperrt",
    2: "Wird aufgesperrt",
    3: "Aufgesperrt",
    4: "Wird zugesperrt",
    5: "Falle gezogen",
    6: "Aufgesperrt (Lock'n'Go)",
    7: "Falle wird gezogen",
    253: "Boot run",
    254: "Motor blockiert",
    255: "Unbekannter Zustand",
};

const DOOR_STATES: Record<number, string> = {
    1: "Türsensor deaktiviert",
    2: "Tür geschlossen",
    3: "Tür geöffnet",
    4: "Türstatus unbekannt",
    5: "Türsensor wird kalibriert",
    16: "Türsensor nicht kalibriert",
    240: "Türsensor manipuliert",
    255: "Unbekannt",
};

const LOCK_ACTIONS: Record<number, string> = {
    1: "Aufsperren",
    2: "Zusperren",
    3: "Falle ziehen",
    4: "Lock'n'Go",
    5: "Lock'n'Go mit Falle ziehen",
    6: "Vollständig zusperren",
    80: "Fob ohne Aktion",
    90: "Button ohne Aktion",
};

const NUKI_ICONS = {
    locked: "/adapter/nuki-local/icons/Nuki_Vis/nuki_locked.png",
    unlocked: "/adapter/nuki-local/icons/Nuki_Vis/nuki_unlocked.png",
    doorOpen: "/adapter/nuki-local/icons/Nuki_Vis/nuki_door_open.png",
    doorClosed: "/adapter/nuki-local/icons/Nuki_Vis/nuki_door_closed.png",
    charging: "/adapter/nuki-local/icons/Nuki_Vis/nuki_charging.png",
    pairing: "/adapter/nuki-local/icons/Nuki_Vis/nuki_pairing.png",
    unknown: "/adapter/nuki-local/icons/Nuki_Vis/nuki_unknown.png",
} as const;

type NukiIconState = "locked" | "unlocked" | "door_open" | "door_closed" | "charging" | "pairing" | "unknown";

interface NukiWebState {
    mode?: number;
    state?: number;
    trigger?: number;
    lastAction?: number;
    batteryCritical?: boolean;
    batteryCharging?: boolean;
    batteryCharge?: number;
    keypadBatteryCritical?: boolean;
    doorsensorBatteryCritical?: boolean;
    doorState?: number;
}

interface NukiWebDevice {
    smartlockId: number;
    type?: number;
    name?: string;
    serverState?: number;
    state?: NukiWebState;
}

interface NukiWebAuthorization {
    id?: string;
    smartlockId?: number;
    authId?: number;
    code?: number;
    type?: number;
    name?: string;
    enabled?: boolean;
    remoteAllowed?: boolean;
}

interface NukiWebLog {
    id?: string;
    smartlockId?: number;
    authId?: number;
    name?: string;
    action?: number;
    state?: number;
    trigger?: number;
    date?: string;
}

class NukiLocal extends utils.Adapter {
    private mqttBroker: AedesInstance | null = null;
    private mqttServer: Server | null = null;
    private mqttDatabase: Level | null = null;

    private mqttPersistence: AedesPersistence | null = null;

    private webApiTimer: ioBroker.Interval | undefined | null = null;

    private readonly initializedDevices = new Set<string>();

    private readonly initializingDevices = new Map<string, Promise<void>>();

    private readonly webSmartlockIds = new Map<string, number>();

    private readonly authorizationNames = new Map<number, Map<number, string>>();

    public constructor(options: Partial<utils.AdapterOptions> = {}) {
        super({
            ...options,
            name: "nuki-local",
        });

        this.on("ready", this.onReady.bind(this));

        this.on("stateChange", (id, state) => {
            void this.onStateChange(id, state);
        });

        this.on("unload", this.onUnload.bind(this));
    }

    private async onReady(): Promise<void> {
        await this.ensureInstanceObjects();

        await this.setStateAsync("info.connection", false, true);

        this.subscribeStates("*");

        try {
            await this.startMqttBroker();

            await this.restoreRetainedNukiStates();

            if (this.config.webApiEnabled) {
                await this.startWebApi();
            }

            await this.setStateAsync("info.connection", true, true);

            this.log.info("Nuki Local adapter started");
        } catch (error) {
            const message = error instanceof Error ? error.message : String(error);

            this.log.error(`Could not start adapter: ${message}`);

            await this.setStateAsync("info.connection", false, true);
        }
    }

    private async ensureInstanceObjects(): Promise<void> {
        await this.extendObjectAsync("info.connection", {
            type: "state",
            common: {
                name: "Connection",
                type: "boolean",
                role: "indicator.connected",
                read: true,
                write: false,
                def: false,
            },
            native: {},
        });
    }

    /*
     * ============================================================
     * MQTT
     * ============================================================
     */

    private async startMqttBroker(): Promise<void> {
        const port = Number(this.config.mqttPort) || 1883;

        const username = String(this.config.mqttUsername ?? "nuki");

        const password = String(this.config.mqttPassword ?? "");

        const instanceDataDir = utils.getAbsoluteInstanceDataDir(this);

        const mqttDataDir = join(instanceDataDir, "mqtt-leveldb");

        await mkdir(mqttDataDir, {
            recursive: true,
        });

        this.log.info(`MQTT persistence directory: ${mqttDataDir}`);

        this.mqttDatabase = new Level(mqttDataDir);

        this.mqttPersistence = aedesPersistenceLevel(this.mqttDatabase);

        this.mqttBroker = await Aedes.createBroker({
            persistence: this.mqttPersistence,
        });

        this.mqttBroker.authenticate = (client, mqttUsername, mqttPassword, callback): void => {
            const receivedUsername = mqttUsername ?? "";

            const receivedPassword = mqttPassword?.toString() ?? "";

            const valid = receivedUsername === username && receivedPassword === password;

            if (valid) {
                this.log.info(`MQTT client authenticated: ${client?.id ?? "unknown"}`);
            } else {
                this.log.warn(`MQTT authentication rejected for client: ${client?.id ?? "unknown"}`);
            }

            callback(null, valid);
        };

        this.mqttBroker.on("client", client => {
            this.log.info(`MQTT client connected: ${client.id}`);
        });

        this.mqttBroker.on("clientDisconnect", client => {
            this.log.info(`MQTT client disconnected: ${client.id}`);
        });

        this.mqttBroker.on("publish", (packet, client) => {
            if (!client) {
                return;
            }

            if (!packet.topic.startsWith("nuki/")) {
                return;
            }

            const value = packet.payload.toString();

            this.log.info(`Nuki MQTT: ${packet.topic} = ${value}`);

            void this.handleNukiMessage(packet.topic, value);
        });

        this.mqttServer = createServer(this.mqttBroker.handle);

        await new Promise<void>((resolve, reject) => {
            if (!this.mqttServer) {
                reject(new Error("MQTT server was not created"));

                return;
            }

            const onError = (error: Error): void => {
                reject(error);
            };

            const onListening = (): void => {
                this.mqttServer?.off("error", onError);
                resolve();
            };

            this.mqttServer.once("error", onError);

            this.mqttServer.once("listening", onListening);

            this.mqttServer.listen(port, "0.0.0.0");
        });

        this.log.info(`Integrated MQTT broker listening on port ${port}`);
    }

    private async restoreRetainedNukiStates(): Promise<void> {
        if (!this.mqttPersistence) {
            return;
        }

        this.log.info("Restoring retained Nuki MQTT states...");

        let restored = 0;

        const stream = this.mqttPersistence.createRetainedStreamCombi(["nuki/#"]);

        for await (const packet of stream) {
            if (!packet || typeof packet.topic !== "string") {
                continue;
            }

            if (!packet.topic.startsWith("nuki/")) {
                continue;
            }

            const payload = Buffer.isBuffer(packet.payload) ? packet.payload.toString() : String(packet.payload ?? "");

            await this.handleNukiMessage(packet.topic, payload);

            restored++;
        }

        this.log.info(`Restored ${restored} retained Nuki MQTT state(s)`);
    }

    private async handleNukiMessage(topic: string, payload: string): Promise<void> {
        try {
            const parts = topic.split("/");

            if (parts.length !== 3 || parts[0] !== "nuki") {
                return;
            }

            const deviceId = parts[1];

            const property = parts[2];

            if (!deviceId || !property) {
                return;
            }

            await this.ensureDevice(deviceId);

            switch (property) {
                case "name":
                    await this.setStringState(`${deviceId}.device.name`, "Name", payload);
                    break;

                case "firmware":
                    await this.setStringState(`${deviceId}.device.firmware`, "Firmware", payload);
                    break;

                case "deviceType":
                    await this.setMappedNumberState(
                        `${deviceId}.device.deviceType`,
                        "Device type",
                        payload,
                        DEVICE_TYPES,
                    );
                    break;

                case "mode":
                    await this.setMappedNumberState(`${deviceId}.device.mode`, "Mode", payload, MODES);
                    break;

                case "connected":
                case "serverConnected":
                    await this.setBooleanState(`${deviceId}.device.online`, "Online", payload, "indicator.connected");
                    break;

                case "timestamp":
                    await this.setStringState(`${deviceId}.status.timestamp`, "Timestamp", payload);
                    break;

                case "batteryChargeState":
                    await this.setNumberState(`${deviceId}.battery.percent`, "Battery", payload, "%", "value.battery");
                    break;

                case "batteryCharging":
                    await this.setBooleanState(`${deviceId}.battery.charging`, "Battery charging", payload);

                    await this.updateDeviceIcon(deviceId);
                    break;

                case "batteryCritical":
                    await this.setBooleanState(
                        `${deviceId}.battery.critical`,
                        "Battery critical",
                        payload,
                        "indicator.maintenance",
                    );
                    break;

                case "doorsensorBatteryCritical":
                    await this.setBooleanState(
                        `${deviceId}.battery.doorSensorCritical`,
                        "Door sensor battery critical",
                        payload,
                        "indicator.maintenance",
                    );
                    break;

                case "keypadBatteryCritical":
                    await this.setBooleanState(
                        `${deviceId}.battery.keypadCritical`,
                        "Keypad battery critical",
                        payload,
                        "indicator.maintenance",
                    );
                    break;

                case "state":
                    await this.handleLockState(deviceId, payload);
                    break;

                case "doorsensorState":
                    await this.handleDoorState(deviceId, payload);
                    break;

                case "lockActionEvent":
                    await this.handleLockActionEvent(deviceId, payload);
                    break;

                case "commandResponse":
                    await this.handleCommandResponse(deviceId, payload);
                    break;

                case "lock":
                case "unlock":
                case "lockAction":
                    break;

                default:
                    await this.setStringState(`${deviceId}.raw.${this.sanitizeId(property)}`, property, payload);
                    break;
            }
        } catch (error) {
            const message = error instanceof Error ? error.message : String(error);

            this.log.error(`Could not process Nuki MQTT message ${topic}: ${message}`);
        }
    }

    /*
     * ============================================================
     * DEVICE INITIALIZATION
     * ============================================================
     */

    private async ensureDevice(deviceId: string): Promise<void> {
        const normalized = deviceId.toUpperCase();

        if (this.initializedDevices.has(normalized)) {
            return;
        }

        const existingPromise = this.initializingDevices.get(normalized);

        if (existingPromise) {
            await existingPromise;
            return;
        }

        const initialization = this.initializeDevice(deviceId, normalized);

        this.initializingDevices.set(normalized, initialization);

        try {
            await initialization;
        } finally {
            this.initializingDevices.delete(normalized);
        }
    }

    private async initializeDevice(deviceId: string, normalized: string): Promise<void> {
        await this.setObjectNotExistsAsync(deviceId, {
            type: "device",
            common: {
                name: `Nuki ${deviceId}`,
            },
            native: {},
        });

        const channels = [
            ["status", "Status"],
            ["battery", "Battery"],
            ["keypad", "Keypad"],
            ["activity", "Activity"],
            ["device", "Device"],
            ["commands", "Commands"],
            ["advanced", "Advanced"],
            ["raw", "Raw MQTT"],
        ] as const;

        for (const [id, name] of channels) {
            await this.setObjectNotExistsAsync(`${deviceId}.${id}`, {
                type: "channel",
                common: {
                    name,
                },
                native: {},
            });
        }

        await this.ensureKnownDeviceObjects(deviceId);

        await this.cleanupLegacyObjects(deviceId);

        this.initializedDevices.add(normalized);

        await this.updateDeviceIcon(deviceId);

        this.log.info(`Nuki device initialized: ${deviceId}`);
    }

    private async ensureKnownDeviceObjects(deviceId: string): Promise<void> {
        await this.ensureMappedStateObject(`${deviceId}.status.lockState`, "Lock state", LOCK_STATES);

        await this.ensureStateObject(`${deviceId}.status.lockStateText`, "Lock state text", "string", "text");

        await this.ensureStateObject(`${deviceId}.status.locked`, "Locked", "boolean", "indicator");

        await this.ensureMappedStateObject(`${deviceId}.status.doorState`, "Door state", DOOR_STATES);

        await this.ensureStateObject(`${deviceId}.status.doorStateText`, "Door state text", "string", "text");

        await this.ensureStateObject(`${deviceId}.status.doorOpen`, "Door open", "boolean", "sensor.door");

        await this.ensureStateObject(`${deviceId}.status.timestamp`, "Timestamp", "string", "text");

        await this.ensureStateObject(`${deviceId}.status.iconState`, "Icon state", "string", "text");

        await this.ensureStateObject(`${deviceId}.status.icon`, "Icon", "string", "text");

        await this.ensureStateObject(`${deviceId}.battery.percent`, "Battery", "number", "value.battery", false, "%");

        await this.ensureStateObject(
            `${deviceId}.battery.critical`,
            "Battery critical",
            "boolean",
            "indicator.maintenance",
        );

        await this.ensureStateObject(`${deviceId}.battery.charging`, "Battery charging", "boolean", "indicator");

        await this.ensureStateObject(
            `${deviceId}.battery.keypadCritical`,
            "Keypad battery critical",
            "boolean",
            "indicator.maintenance",
        );

        await this.ensureStateObject(
            `${deviceId}.battery.doorSensorCritical`,
            "Door sensor battery critical",
            "boolean",
            "indicator.maintenance",
        );

        await this.ensureStateObject(`${deviceId}.keypad.lastType`, "Last access type", "string", "text");

        await this.ensureStateObject(`${deviceId}.keypad.lastUser`, "Last user", "string", "text");

        await this.ensureStateObject(`${deviceId}.keypad.lastTimestamp`, "Last access timestamp", "string", "text");

        await this.ensureStateObject(`${deviceId}.activity.lastAction`, "Last action", "number", "value");

        await this.ensureStateObject(`${deviceId}.activity.lastActionText`, "Last action text", "string", "text");

        await this.ensureStateObject(`${deviceId}.activity.lastUser`, "Last user", "string", "text");

        await this.ensureStateObject(`${deviceId}.activity.lastDate`, "Last date", "string", "text");

        await this.ensureStateObject(`${deviceId}.device.name`, "Name", "string", "text");

        await this.ensureStateObject(`${deviceId}.device.firmware`, "Firmware", "string", "text");

        await this.ensureMappedStateObject(`${deviceId}.device.deviceType`, "Device type", DEVICE_TYPES);

        await this.ensureMappedStateObject(`${deviceId}.device.mode`, "Mode", MODES);

        await this.ensureStateObject(`${deviceId}.device.online`, "Online", "boolean", "indicator.connected");

        await this.ensureStateObject(`${deviceId}.advanced.authId`, "Authorization ID", "number", "value");

        await this.ensureStateObject(`${deviceId}.advanced.codeId`, "Code ID", "number", "value");

        await this.ensureStateObject(`${deviceId}.advanced.source`, "Source", "number", "value");

        await this.ensureStateObject(`${deviceId}.advanced.trigger`, "Trigger", "number", "value");

        await this.ensureStateObject(`${deviceId}.advanced.smartlockId`, "Web API Smartlock ID", "number", "value");

        await this.ensureStateObject(`${deviceId}.advanced.serverState`, "Nuki server state", "number", "value");

        await this.ensureStateObject(`${deviceId}.advanced.authorizations`, "Authorizations", "string", "json");

        await this.createCommandState(`${deviceId}.commands.lock`, "Lock");

        await this.createCommandState(`${deviceId}.commands.unlock`, "Unlock");

        await this.createCommandState(`${deviceId}.commands.unlatch`, "Unlatch");

        await this.createCommandState(`${deviceId}.commands.lockNgo`, "Lock'n'Go");

        await this.createCommandState(`${deviceId}.commands.lockNgoUnlatch`, "Lock'n'Go with unlatch");

        await this.createCommandState(`${deviceId}.commands.fullLock`, "Full lock");

        await this.ensureStateObject(`${deviceId}.commands.lastResult`, "Last command result", "number", "value");

        await this.ensureStateObject(
            `${deviceId}.commands.lastResultText`,
            "Last command result text",
            "string",
            "text",
        );

        await this.initializeDefaultValues(deviceId);
    }

    private async initializeDefaultValues(deviceId: string): Promise<void> {
        const defaults: Array<{
            id: string;
            value: string | boolean;
        }> = [
            {
                id: `${deviceId}.device.online`,
                value: false,
            },
            {
                id: `${deviceId}.battery.charging`,
                value: false,
            },
            {
                id: `${deviceId}.battery.critical`,
                value: false,
            },
            {
                id: `${deviceId}.battery.keypadCritical`,
                value: false,
            },
            {
                id: `${deviceId}.battery.doorSensorCritical`,
                value: false,
            },
            {
                id: `${deviceId}.device.firmware`,
                value: "",
            },
            {
                id: `${deviceId}.keypad.lastUser`,
                value: "",
            },
            {
                id: `${deviceId}.keypad.lastType`,
                value: "",
            },
            {
                id: `${deviceId}.keypad.lastTimestamp`,
                value: "",
            },
            {
                id: `${deviceId}.activity.lastUser`,
                value: "",
            },
            {
                id: `${deviceId}.activity.lastActionText`,
                value: "",
            },
            {
                id: `${deviceId}.activity.lastDate`,
                value: "",
            },
            {
                id: `${deviceId}.commands.lastResultText`,
                value: "",
            },
            {
                id: `${deviceId}.status.iconState`,
                value: "unknown",
            },
            {
                id: `${deviceId}.status.icon`,
                value: NUKI_ICONS.unknown,
            },
        ];

        for (const item of defaults) {
            const state = await this.getStateAsync(item.id);

            if (!state) {
                await this.setStateAsync(item.id, item.value, true);
            }
        }
    }

    private async cleanupLegacyObjects(deviceId: string): Promise<void> {
        const legacy = [
            `${deviceId}.info`,
            `${deviceId}.lock`,
            `${deviceId}.door`,
            `${deviceId}.events`,
            `${deviceId}.web`,
        ];

        for (const id of legacy) {
            const object = await this.getObjectAsync(id);

            if (!object) {
                continue;
            }

            try {
                await this.delObjectAsync(id, {
                    recursive: true,
                });
            } catch (error) {
                const message = error instanceof Error ? error.message : String(error);

                this.log.warn(`Could not remove old object ${id}: ${message}`);
            }
        }
    }

    private async cleanupOldWebDeviceId(oldDeviceId: string, realDeviceId: string): Promise<void> {
        if (oldDeviceId === realDeviceId) {
            return;
        }

        const object = await this.getObjectAsync(oldDeviceId);

        if (!object) {
            return;
        }

        try {
            await this.delObjectAsync(oldDeviceId, {
                recursive: true,
            });

            this.initializedDevices.delete(oldDeviceId.toUpperCase());

            this.log.info(`Removed old Web API device ID ${oldDeviceId}; using MQTT ID ${realDeviceId}`);
        } catch (error) {
            const message = error instanceof Error ? error.message : String(error);

            this.log.warn(`Could not remove old Web API device ${oldDeviceId}: ${message}`);
        }
    }

    /*
     * ============================================================
     * ICON
     * ============================================================
     */

    private async updateDeviceIcon(deviceId: string): Promise<void> {
        const chargingState = await this.getStateAsync(`${deviceId}.battery.charging`);

        const doorOpenState = await this.getStateAsync(`${deviceId}.status.doorOpen`);

        const lockState = await this.getStateAsync(`${deviceId}.status.lockState`);

        const charging = chargingState?.val === true;

        const hasDoorState =
            doorOpenState !== null &&
            doorOpenState !== undefined &&
            doorOpenState.val !== null &&
            doorOpenState.val !== undefined;

        const doorOpen = doorOpenState?.val === true;

        const lockStateValue = typeof lockState?.val === "number" ? lockState.val : Number(lockState?.val);

        let iconState: NukiIconState = "unknown";

        let icon: string = NUKI_ICONS.unknown;

        /*
         * Priorität:
         *
         * 1. Akku lädt
         * 2. Tür offen
         * 3. Schloss zugesperrt
         * 4. Schloss aufgesperrt
         * 5. Tür geschlossen
         * 6. unbekannt
         */

        if (charging) {
            iconState = "charging";

            icon = NUKI_ICONS.charging;
        } else if (doorOpen) {
            iconState = "door_open";

            icon = NUKI_ICONS.doorOpen;
        } else if (lockStateValue === 1) {
            iconState = "locked";

            icon = NUKI_ICONS.locked;
        } else if (lockStateValue === 3 || lockStateValue === 5 || lockStateValue === 6) {
            iconState = "unlocked";

            icon = NUKI_ICONS.unlocked;
        } else if (hasDoorState && !doorOpen) {
            iconState = "door_closed";

            icon = NUKI_ICONS.doorClosed;
        }

        await this.setStateAsync(`${deviceId}.status.iconState`, iconState, true);

        await this.setStateAsync(`${deviceId}.status.icon`, icon, true);
    }

    /*
     * ============================================================
     * STATUS / EVENTS
     * ============================================================
     */

    private async handleLockState(deviceId: string, payload: string): Promise<void> {
        const state = Number(payload);

        if (!Number.isFinite(state)) {
            return;
        }

        await this.setMappedNumberState(`${deviceId}.status.lockState`, "Lock state", payload, LOCK_STATES);

        await this.setStringState(
            `${deviceId}.status.lockStateText`,
            "Lock state text",
            this.getMappedText(LOCK_STATES, state),
        );

        await this.setStateAsync(`${deviceId}.status.locked`, state === 1, true);

        await this.updateDeviceIcon(deviceId);
    }

    private async handleDoorState(deviceId: string, payload: string): Promise<void> {
        const state = Number(payload);

        if (!Number.isFinite(state)) {
            return;
        }

        await this.setMappedNumberState(`${deviceId}.status.doorState`, "Door state", payload, DOOR_STATES);

        await this.setStringState(
            `${deviceId}.status.doorStateText`,
            "Door state text",
            this.getMappedText(DOOR_STATES, state),
        );

        await this.setStateAsync(`${deviceId}.status.doorOpen`, state === 3, true);

        await this.updateDeviceIcon(deviceId);
    }

    private async handleLockActionEvent(deviceId: string, payload: string): Promise<void> {
        const parts = payload.split(",");

        if (parts.length < 5) {
            this.log.warn(`Invalid lockActionEvent: ${payload}`);

            return;
        }

        const action = Number(parts[0]);

        const trigger = Number(parts[1]);

        const authId = Number(parts[2]);

        const codeId = Number(parts[3]);

        const source = Number(parts[4]);

        if (
            !Number.isFinite(action) ||
            !Number.isFinite(trigger) ||
            !Number.isFinite(authId) ||
            !Number.isFinite(codeId) ||
            !Number.isFinite(source)
        ) {
            this.log.warn(`Invalid numeric values in lockActionEvent: ${payload}`);

            return;
        }

        const now = new Date().toISOString();

        await this.setStateAsync(`${deviceId}.activity.lastAction`, action, true);

        await this.setStringState(
            `${deviceId}.activity.lastActionText`,
            "Last action text",
            this.getMappedText(LOCK_ACTIONS, action),
        );

        await this.setStringState(`${deviceId}.activity.lastDate`, "Last date", now);

        await this.setStateAsync(`${deviceId}.advanced.authId`, authId, true);

        await this.setStateAsync(`${deviceId}.advanced.codeId`, codeId, true);

        await this.setStateAsync(`${deviceId}.advanced.source`, source, true);

        await this.setStateAsync(`${deviceId}.advanced.trigger`, trigger, true);

        const configuredKeypadUser = codeId > 0 ? this.findConfiguredKeypadUser(codeId) : undefined;

        const resolvedUserName = this.findAuthorizationName(deviceId, authId);

        const displayUser =
            configuredKeypadUser ??
            resolvedUserName ??
            (codeId > 0 ? `Unbekannt (Code-ID ${codeId})` : `Unbekannt (Auth-ID ${authId})`);

        await this.setStringState(`${deviceId}.activity.lastUser`, "Last user", displayUser);

        if (codeId > 0) {
            let type = `Quelle ${source}`;

            switch (source) {
                case 0:
                    type = "Back-Taste";
                    break;

                case 1:
                    type = "Code";
                    break;

                case 2:
                    type = "Fingerprint";
                    break;
            }

            await this.setStringState(`${deviceId}.keypad.lastType`, "Last access type", type);

            await this.setStringState(`${deviceId}.keypad.lastUser`, "Last user", displayUser);

            await this.setStringState(`${deviceId}.keypad.lastTimestamp`, "Last access timestamp", now);

            if (source === 2) {
                this.log.info(`Fingerprint detected: authId=${authId}, codeId=${codeId}, user=${displayUser}`);
            } else if (source === 1) {
                this.log.info(`Keypad code detected: authId=${authId}, codeId=${codeId}, user=${displayUser}`);
            }
        }

        this.log.debug(
            `Lock action processed: action=${action}, trigger=${trigger}, authId=${authId}, codeId=${codeId}, source=${source}`,
        );
    }

    private findConfiguredKeypadUser(codeId: number): string | undefined {
        const users = Array.isArray(this.config.keypadUsers) ? this.config.keypadUsers : [];

        const match = users.find(user => Number(user.codeId) === codeId);

        if (!match || typeof match.name !== "string") {
            return undefined;
        }

        const name = match.name.trim();

        return name || undefined;
    }

    private findAuthorizationName(deviceId: string, authId: number): string | undefined {
        const smartlockId = this.webSmartlockIds.get(this.normalizeDeviceId(deviceId));

        if (smartlockId === undefined) {
            return undefined;
        }

        return this.authorizationNames.get(smartlockId)?.get(authId);
    }

    /*
     * ============================================================
     * COMMANDS
     * ============================================================
     */

    private async createCommandState(id: string, name: string): Promise<void> {
        await this.extendObjectAsync(id, {
            type: "state",
            common: {
                name,
                type: "boolean",
                role: "button",
                read: true,
                write: true,
                def: false,
            },
            native: {},
        });

        const state = await this.getStateAsync(id);

        if (!state) {
            await this.setStateAsync(id, false, true);
        }
    }

    private async onStateChange(id: string, state: ioBroker.State | null | undefined): Promise<void> {
        if (!state || state.ack || state.val !== true) {
            return;
        }

        const prefix = `${this.namespace}.`;

        if (!id.startsWith(prefix)) {
            return;
        }

        const relativeId = id.substring(prefix.length);

        const parts = relativeId.split(".");

        if (parts.length !== 3 || parts[1] !== "commands") {
            return;
        }

        const deviceId = parts[0];

        const command = parts[2];

        try {
            switch (command) {
                case "unlock":
                    await this.publishNukiCommand(`nuki/${deviceId}/lockAction`, "1");
                    break;

                case "lock":
                    await this.publishNukiCommand(`nuki/${deviceId}/lockAction`, "2");
                    break;

                case "unlatch":
                    await this.publishNukiCommand(`nuki/${deviceId}/lockAction`, "3");
                    break;

                case "lockNgo":
                    await this.publishNukiCommand(`nuki/${deviceId}/lockAction`, "4");
                    break;

                case "lockNgoUnlatch":
                    await this.publishNukiCommand(`nuki/${deviceId}/lockAction`, "5");
                    break;

                case "fullLock":
                    await this.publishNukiCommand(`nuki/${deviceId}/lockAction`, "6");
                    break;

                default:
                    return;
            }

            await this.setStateAsync(relativeId, false, true);
        } catch (error) {
            const message = error instanceof Error ? error.message : String(error);

            this.log.error(`Could not execute command ${command}: ${message}`);

            await this.setStateAsync(relativeId, false, true);
        }
    }

    private async publishNukiCommand(topic: string, payload: string): Promise<void> {
        if (!this.mqttBroker) {
            throw new Error("MQTT broker is not running");
        }

        await new Promise<void>((resolve, reject) => {
            this.mqttBroker?.publish(
                {
                    cmd: "publish",
                    topic,
                    payload: Buffer.from(payload),
                    qos: 2,
                    dup: false,
                    retain: false,
                },
                error => {
                    if (error) {
                        reject(error);

                        return;
                    }

                    resolve();
                },
            );
        });

        this.log.info(`Nuki command published: ${topic} = ${payload}`);
    }

    private async handleCommandResponse(deviceId: string, payload: string): Promise<void> {
        const result = Number(payload);

        if (!Number.isFinite(result)) {
            return;
        }

        await this.setNumberState(`${deviceId}.commands.lastResult`, "Last command result", payload);

        await this.setStringState(
            `${deviceId}.commands.lastResultText`,
            "Last command result text",
            result === 0 ? "Erfolgreich" : `Fehler ${result}`,
        );
    }

    /*
     * ============================================================
     * WEB API
     * ============================================================
     */

    private async startWebApi(): Promise<void> {
        const token = String(this.config.webApiToken ?? "").trim();

        if (!token) {
            this.log.warn("Nuki Web API is enabled but no API token is configured");

            return;
        }

        const intervalSeconds = Math.max(60, Number(this.config.webApiInterval) || 300);

        this.log.info(`Nuki Web API enabled, update interval: ${intervalSeconds} seconds`);

        try {
            await this.updateNukiWebData();
        } catch (error) {
            const message = error instanceof Error ? error.message : String(error);

            this.log.warn(`Initial Nuki Web API update failed: ${message}`);
        }

        this.webApiTimer = this.setInterval(() => {
            void this.updateNukiWebData().catch(error => {
                const message = error instanceof Error ? error.message : String(error);

                this.log.warn(`Nuki Web API update failed: ${message}`);
            });
        }, intervalSeconds * 1000);
    }

    private async updateNukiWebData(): Promise<void> {
        const devices = await this.nukiWebRequest<NukiWebDevice[]>("/smartlock");

        if (!Array.isArray(devices)) {
            throw new Error("Invalid response from /smartlock");
        }

        let authorizations: NukiWebAuthorization[] = [];

        try {
            const result = await this.nukiWebRequest<NukiWebAuthorization[]>("/smartlock/auth");

            if (Array.isArray(result)) {
                authorizations = result;
            }
        } catch (error) {
            const message = error instanceof Error ? error.message : String(error);

            this.log.warn(`Could not load Nuki authorizations: ${message}`);
        }

        for (const device of devices) {
            const smartlockId = Number(device.smartlockId);

            if (!Number.isFinite(smartlockId)) {
                continue;
            }

            const conversion = this.getMqttDeviceIdFromWebDevice(device);

            const deviceId = conversion.deviceId;

            if (conversion.originalHex !== conversion.deviceId) {
                await this.cleanupOldWebDeviceId(conversion.originalHex, conversion.deviceId);
            }

            this.webSmartlockIds.set(this.normalizeDeviceId(deviceId), smartlockId);

            await this.ensureDevice(deviceId);

            await this.storeWebDeviceData(deviceId, device);

            const deviceAuths = authorizations.filter(auth => Number(auth.smartlockId) === smartlockId);

            await this.storeWebAuthorizations(deviceId, smartlockId, deviceAuths);

            try {
                const logs = await this.nukiWebRequest<NukiWebLog[]>(`/smartlock/${smartlockId}/log?limit=20`);

                if (Array.isArray(logs)) {
                    await this.storeWebLogs(deviceId, logs);
                }
            } catch (error) {
                const message = error instanceof Error ? error.message : String(error);

                this.log.warn(`Could not load activity logs for ${deviceId}: ${message}`);
            }
        }
    }

    private async nukiWebRequest<T>(path: string): Promise<T> {
        const token = String(this.config.webApiToken ?? "").trim();

        if (!token) {
            throw new Error("Nuki Web API token missing");
        }

        const response = await fetch(`https://api.nuki.io${path}`, {
            method: "GET",
            headers: {
                Accept: "application/json",
                Authorization: `Bearer ${token}`,
            },
        });

        if (!response.ok) {
            const body = await response.text().catch(() => "");

            throw new Error(
                `HTTP ${response.status} ${response.statusText}${body ? `: ${body.substring(0, 300)}` : ""}`,
            );
        }

        return (await response.json()) as T;
    }

    private getMqttDeviceIdFromWebDevice(device: NukiWebDevice): {
        deviceId: string;
        originalHex: string;
    } {
        try {
            const originalHex = BigInt(device.smartlockId).toString(16).toUpperCase();

            const deviceId = originalHex.slice(-8).padStart(8, "0");

            return {
                deviceId,
                originalHex,
            };
        } catch {
            const value = String(device.smartlockId);

            return {
                deviceId: value,
                originalHex: value,
            };
        }
    }

    private normalizeDeviceId(deviceId: string): string {
        const normalized = deviceId.trim().toUpperCase().replace(/^0X/, "");

        return normalized.slice(-8).padStart(8, "0");
    }

    private async storeWebDeviceData(deviceId: string, device: NukiWebDevice): Promise<void> {
        await this.setStateAsync(`${deviceId}.advanced.smartlockId`, Number(device.smartlockId), true);

        if (typeof device.name === "string") {
            await this.setStringState(`${deviceId}.device.name`, "Name", device.name);
        }

        if (typeof device.type === "number") {
            await this.setMappedNumberState(
                `${deviceId}.device.deviceType`,
                "Device type",
                String(device.type),
                DEVICE_TYPES,
            );
        }

        if (typeof device.serverState === "number") {
            await this.setStateAsync(`${deviceId}.advanced.serverState`, device.serverState, true);

            await this.setStateAsync(`${deviceId}.device.online`, device.serverState === 0, true);
        }

        const state = device.state;

        if (!state) {
            await this.updateDeviceIcon(deviceId);

            return;
        }

        if (typeof state.batteryCharge === "number") {
            await this.setStateAsync(`${deviceId}.battery.percent`, state.batteryCharge, true);
        }

        if (typeof state.batteryCritical === "boolean") {
            await this.setStateAsync(`${deviceId}.battery.critical`, state.batteryCritical, true);
        }

        if (typeof state.batteryCharging === "boolean") {
            await this.setStateAsync(`${deviceId}.battery.charging`, state.batteryCharging, true);
        }

        if (typeof state.keypadBatteryCritical === "boolean") {
            await this.setStateAsync(`${deviceId}.battery.keypadCritical`, state.keypadBatteryCritical, true);
        }

        if (typeof state.doorsensorBatteryCritical === "boolean") {
            await this.setStateAsync(`${deviceId}.battery.doorSensorCritical`, state.doorsensorBatteryCritical, true);
        }

        if (typeof state.state === "number") {
            await this.setStateAsync(`${deviceId}.status.lockState`, state.state, true);

            await this.setStringState(
                `${deviceId}.status.lockStateText`,
                "Lock state text",
                this.getMappedText(LOCK_STATES, state.state),
            );

            await this.setStateAsync(`${deviceId}.status.locked`, state.state === 1, true);
        }

        if (typeof state.doorState === "number") {
            await this.setStateAsync(`${deviceId}.status.doorState`, state.doorState, true);

            await this.setStringState(
                `${deviceId}.status.doorStateText`,
                "Door state text",
                this.getMappedText(DOOR_STATES, state.doorState),
            );

            await this.setStateAsync(`${deviceId}.status.doorOpen`, state.doorState === 3, true);
        }

        await this.updateDeviceIcon(deviceId);
    }

    private async storeWebAuthorizations(
        deviceId: string,
        smartlockId: number,
        authorizations: NukiWebAuthorization[],
    ): Promise<void> {
        const authNames = new Map<number, string>();

        for (const auth of authorizations) {
            if (typeof auth.authId === "number" && typeof auth.name === "string") {
                authNames.set(auth.authId, auth.name);
            }
        }

        this.authorizationNames.set(smartlockId, authNames);

        const safe = authorizations.map(auth => ({
            id: auth.id,
            smartlockId: auth.smartlockId,
            authId: auth.authId,
            type: auth.type,
            name: auth.name,
            enabled: auth.enabled,
            remoteAllowed: auth.remoteAllowed,
        }));

        await this.setStringState(`${deviceId}.advanced.authorizations`, "Authorizations", JSON.stringify(safe));
    }

    private async storeWebLogs(deviceId: string, logs: NukiWebLog[]): Promise<void> {
        if (logs.length === 0) {
            return;
        }

        const sorted = [...logs].sort((a, b) => {
            const bTime = Date.parse(b.date ?? "");

            const aTime = Date.parse(a.date ?? "");

            return (Number.isFinite(bTime) ? bTime : 0) - (Number.isFinite(aTime) ? aTime : 0);
        });

        const last = sorted[0];

        if (!last) {
            return;
        }

        if (typeof last.action === "number") {
            await this.setStateAsync(`${deviceId}.activity.lastAction`, last.action, true);

            await this.setStringState(
                `${deviceId}.activity.lastActionText`,
                "Last action text",
                this.getMappedText(LOCK_ACTIONS, last.action),
            );
        }

        if (typeof last.name === "string") {
            await this.setStringState(`${deviceId}.activity.lastUser`, "Last user", last.name);
        }

        if (typeof last.date === "string") {
            await this.setStringState(`${deviceId}.activity.lastDate`, "Last date", last.date);
        }

        if (typeof last.authId === "number") {
            await this.setStateAsync(`${deviceId}.advanced.authId`, last.authId, true);
        }
    }

    /*
     * ============================================================
     * HELPERS
     * ============================================================
     */

    private getMappedText(map: Record<number, string>, value: number): string {
        return map[value] ?? `Unbekannt (${value})`;
    }

    private async ensureStateObject(
        id: string,
        name: string,
        type: "string" | "number" | "boolean",
        role: string,
        write = false,
        unit?: string,
    ): Promise<void> {
        await this.extendObjectAsync(id, {
            type: "state",
            common: {
                name,
                type,
                role,
                read: true,
                write,
                ...(unit
                    ? {
                          unit,
                      }
                    : {}),
            },
            native: {},
        });
    }

    private async ensureMappedStateObject(id: string, name: string, states: Record<number, string>): Promise<void> {
        const ioBrokerStates: Record<string, string> = {};

        for (const [key, text] of Object.entries(states)) {
            ioBrokerStates[key] = text;
        }

        await this.extendObjectAsync(id, {
            type: "state",
            common: {
                name,
                type: "number",
                role: "value",
                read: true,
                write: false,
                states: ioBrokerStates,
            },
            native: {},
        });
    }

    private async setStringState(id: string, name: string, value: string): Promise<void> {
        await this.ensureStateObject(id, name, "string", "text");

        await this.setStateAsync(id, value, true);
    }

    private async setNumberState(
        id: string,
        name: string,
        value: string,
        unit?: string,
        role = "value",
    ): Promise<void> {
        const numberValue = Number(value);

        if (!Number.isFinite(numberValue)) {
            return;
        }

        await this.ensureStateObject(id, name, "number", role, false, unit);

        await this.setStateAsync(id, numberValue, true);
    }

    private async setMappedNumberState(
        id: string,
        name: string,
        value: string,
        states: Record<number, string>,
    ): Promise<void> {
        const numberValue = Number(value);

        if (!Number.isFinite(numberValue)) {
            return;
        }

        await this.ensureMappedStateObject(id, name, states);

        await this.setStateAsync(id, numberValue, true);
    }

    private async setBooleanState(id: string, name: string, value: string, role = "indicator"): Promise<void> {
        await this.ensureStateObject(id, name, "boolean", role);

        const boolValue = value.toLowerCase() === "true" || value === "1";

        await this.setStateAsync(id, boolValue, true);
    }

    private sanitizeId(value: string): string {
        return value.replace(/[^a-zA-Z0-9_-]/g, "_");
    }

    /*
     * ============================================================
     * UNLOAD
     * ============================================================
     */

    private onUnload(callback: () => void): void {
        void (async () => {
            try {
                if (this.webApiTimer) {
                    clearInterval(this.webApiTimer);

                    this.webApiTimer = null;
                }

                await this.setStateAsync("info.connection", false, true);

                if (this.mqttServer) {
                    await new Promise<void>(resolve => {
                        this.mqttServer?.close(() => resolve());
                    });

                    this.mqttServer = null;
                }

                if (this.mqttBroker) {
                    await new Promise<void>(resolve => {
                        this.mqttBroker?.close(() => resolve());
                    });

                    this.mqttBroker = null;
                }

                this.mqttPersistence = null;

                this.mqttDatabase = null;

                this.log.info("Nuki Local adapter stopped");
            } catch (error) {
                const message = error instanceof Error ? error.message : String(error);

                this.log.warn(`Error while stopping adapter: ${message}`);
            } finally {
                callback();
            }
        })();
    }
}

if (require.main !== module) {
    module.exports = (options: Partial<utils.AdapterOptions> | undefined) => new NukiLocal(options);
} else {
    (() => new NukiLocal())();
}
