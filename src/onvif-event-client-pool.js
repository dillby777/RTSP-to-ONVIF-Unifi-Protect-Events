const OnvifEventClient = require('./onvif-event-client');

module.exports = class OnvifEventClientPool {
    constructor(logger) {
        this.logger = logger;
        this.clients = new Map();
    }

    acquire(config) {
        if (!config.events || !config.events.enabled) {
            return null;
        }

        const key = config.target.hostname;
        let entry = this.clients.get(key);
        if (!entry) {
            this.logger.info(`EVENTS: creating upstream client for ${key}`);
            entry = {
                refCount: 0,
                client: new OnvifEventClient(this.logger, key, config)
            };
            this.clients.set(key, entry);
        }

        entry.refCount += 1;
        return entry.client;
    }

    release(config) {
        if (!config.events || !config.events.enabled) {
            return;
        }

        const key = config.target.hostname;
        let entry = this.clients.get(key);
        if (!entry) {
            return;
        }

        entry.refCount -= 1;
        if (entry.refCount <= 0) {
            this.logger.info(`EVENTS: closing upstream client for ${key}`);
            entry.client.close();
            this.clients.delete(key);
        }
    }

    closeAll() {
        for (let entry of this.clients.values()) {
            entry.client.close();
        }

        this.clients.clear();
    }
};