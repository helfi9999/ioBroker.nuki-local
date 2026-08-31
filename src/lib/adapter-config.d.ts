declare global {
    namespace ioBroker {
        interface AdapterConfig {
            mqttPort: number;
            mqttUsername: string;
            mqttPassword: string;

            webApiEnabled: boolean;
            webApiToken: string;
            webApiInterval: number;
        }
    }
}

export {};
