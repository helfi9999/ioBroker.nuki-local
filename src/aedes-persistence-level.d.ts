declare module "aedes-persistence-level" {
    import type { Level } from "level";
    import type { AedesPersistence } from "aedes-persistence";

    export default function aedesPersistenceLevel(db: Level): AedesPersistence;
}
