const uuid = require('node-uuid');

function asArray(value) {
    if (Array.isArray(value)) {
        return value;
    }

    if (value === undefined || value === null) {
        return [];
    }

    return [value];
}

module.exports = class OnvifEventService {
    constructor(logger, config) {
        this.logger = logger;
        this.config = config;
        this.subscriptions = new Map();
        this.client = null;
        this.onUpstreamEvent = this.handleUpstreamEvent.bind(this);
        this.cleanupTimer = setInterval(() => this.cleanupExpiredSubscriptions(), 30000);
        if (typeof this.cleanupTimer.unref === 'function') {
            this.cleanupTimer.unref();
        }
    }

    close() {
        if (this.cleanupTimer) {
            clearInterval(this.cleanupTimer);
            this.cleanupTimer = null;
        }

        if (this.client) {
            this.client.removeListener('event', this.onUpstreamEvent);
            this.client = null;
        }
    }

    attachClient(client) {
        this.client = client;
        if (!this.client) {
            return;
        }

        this.client.on('event', this.onUpstreamEvent);
        this.client.start().catch((error) => {
            this.logger.warn(`EVENTS: ${this.config.name} failed to start upstream relay: ${error.message}`);
        });
    }

    isEnabled() {
        return Boolean(this.config.events && this.config.events.enabled);
    }

    isEventsPath(requestPath) {
        return requestPath === '/onvif/events_service' || /^\/onvif\/events\/subscriptions\//.test(requestPath);
    }

    handleRequest(action, requestPath, body) {
        switch (action) {
            case 'GetEventProperties':
                return this.getEventPropertiesResponse();
            case 'GetServiceCapabilities':
                return this.getServiceCapabilitiesResponse();
            case 'CreatePullPointSubscription':
                return this.createPullPointSubscriptionResponse(body);
            case 'PullMessages':
            case 'Pull':
                return this.pullMessagesResponse(requestPath, body);
            case 'Renew':
                return this.renewResponse(requestPath, body);
            case 'Unsubscribe':
                return this.unsubscribeResponse(requestPath);
            default:
                throw new Error(`Unsupported ONVIF event action ${action || '(none)'}`);
        }
    }

    getEventPropertiesResponse() {
        return `    <tev:GetEventPropertiesResponse>
      <tev:TopicNamespaceLocation>http://www.onvif.org/ver10/topics/topicns.xml</tev:TopicNamespaceLocation>
      <tev:TopicSet xmlns:tns1="http://www.onvif.org/ver10/topics" xmlns:wstop="http://docs.oasis-open.org/wsn/t-1">
        <tns1:VideoSource>
          <tns1:CellMotionDetector>
            <tns1:Motion wstop:topic="true"/>
          </tns1:CellMotionDetector>
        </tns1:VideoSource>
      </tev:TopicSet>
      <tev:TopicExpressionDialect>http://www.onvif.org/ver10/tev/topicExpression/ConcreteSet</tev:TopicExpressionDialect>
      <tev:MessageContentFilterDialect>http://www.onvif.org/ver10/tev/messageContentFilter/ItemFilter</tev:MessageContentFilterDialect>
      <tev:ProducerPropertiesFilterDialect>http://www.onvif.org/ver10/tev/producerPropertiesFilter/ItemFilter</tev:ProducerPropertiesFilterDialect>
      <tev:MessageContentSchemaLocation>http://www.onvif.org/ver10/schema/onvif.xsd</tev:MessageContentSchemaLocation>
    </tev:GetEventPropertiesResponse>`;
    }

    getServiceCapabilitiesResponse() {
        return `    <tev:GetServiceCapabilitiesResponse>
      <tev:Capabilities WSSubscriptionPolicySupport="false" WSPullPointSupport="true" WSPausableSubscriptionManagerInterfaceSupport="false" MaxNotificationProducers="1" MaxPullPoints="32" PersistentNotificationStorage="false"/>
    </tev:GetServiceCapabilitiesResponse>`;
    }

    createPullPointSubscriptionResponse(body) {
        let timeout = this.parseDuration(this.getRequestValue(body, 'InitialTerminationTime') || this.getRequestValue(body, 'TerminationTime'), 300000);
        let subscription = this.createSubscription(timeout);

        return `    <tev:CreatePullPointSubscriptionResponse>
      <tev:SubscriptionReference>
        <wsa5:Address>${this.getSubscriptionAddress(subscription.id)}</wsa5:Address>
      </tev:SubscriptionReference>
      <wsnt:CurrentTime>${new Date().toISOString()}</wsnt:CurrentTime>
      <wsnt:TerminationTime>${subscription.terminationTime}</wsnt:TerminationTime>
    </tev:CreatePullPointSubscriptionResponse>`;
    }

    pullMessagesResponse(requestPath, body) {
        let subscription = this.getSubscriptionFromPath(requestPath);
        let timeout = this.parseDuration(this.getRequestValue(body, 'Timeout'), 0);
        let messageLimit = this.parseInteger(this.getRequestValue(body, 'MessageLimit'), 10);

        return new Promise((resolve) => {
            if (!subscription.queue.length && timeout > 0) {
                subscription.pendingPull = {
                    resolve,
                    messageLimit,
                    timer: setTimeout(() => {
                        subscription.pendingPull = null;
                        resolve(this.buildPullMessagesResponse(subscription, []));
                    }, timeout)
                };

                if (typeof subscription.pendingPull.timer.unref === 'function') {
                    subscription.pendingPull.timer.unref();
                }

                return;
            }

            resolve(this.buildPullMessagesResponse(subscription, subscription.queue.splice(0, messageLimit)));
        });
    }

    renewResponse(requestPath, body) {
        let subscription = this.getSubscriptionFromPath(requestPath);
        let timeout = this.parseDuration(this.getRequestValue(body, 'TerminationTime'), 300000);
        subscription.terminationTime = new Date(Date.now() + timeout).toISOString();

        return `    <wsnt:RenewResponse>
      <wsnt:CurrentTime>${new Date().toISOString()}</wsnt:CurrentTime>
      <wsnt:TerminationTime>${subscription.terminationTime}</wsnt:TerminationTime>
    </wsnt:RenewResponse>`;
    }

    unsubscribeResponse(requestPath) {
        let subscription = this.getSubscriptionFromPath(requestPath);
        this.deleteSubscription(subscription.id);
        return `    <wsnt:UnsubscribeResponse/>`;
    }

    createSubscription(timeout) {
        let id = uuid.v4();
        let subscription = {
            id,
            queue: [],
            pendingPull: null,
            terminationTime: new Date(Date.now() + timeout).toISOString()
        };

        this.subscriptions.set(id, subscription);
        return subscription;
    }

    deleteSubscription(id) {
        let subscription = this.subscriptions.get(id);
        if (!subscription) {
            return;
        }

        if (subscription.pendingPull) {
            clearTimeout(subscription.pendingPull.timer);
            subscription.pendingPull.resolve(this.buildPullMessagesResponse(subscription, []));
            subscription.pendingPull = null;
        }

        this.subscriptions.delete(id);
    }

    cleanupExpiredSubscriptions() {
        let now = Date.now();
        for (let subscription of this.subscriptions.values()) {
            if (Date.parse(subscription.terminationTime) <= now) {
                this.deleteSubscription(subscription.id);
            }
        }
    }

    handleUpstreamEvent(event) {
        if (!this.matchesSourceFilter(event)) {
            this.logger.info(`EVENTS: ${this.config.name} dropped upstream event due to source filter '${this.config.events.source}' (topic='${event.topic || ''}' source='${event.sourceText || ''}')`);
            return;
        }

        for (let subscription of this.subscriptions.values()) {
            subscription.queue.push(event);
            if (subscription.pendingPull) {
                clearTimeout(subscription.pendingPull.timer);
                let pendingPull = subscription.pendingPull;
                subscription.pendingPull = null;
                pendingPull.resolve(this.buildPullMessagesResponse(subscription, subscription.queue.splice(0, pendingPull.messageLimit)));
            }
        }
    }

    matchesSourceFilter(event) {
        let filter = this.config.events && this.config.events.source;
        if (!filter) {
            return true;
        }

        if ((event.sourceText || '').includes(filter)) {
            return true;
        }

        for (let item of asArray(event.sourceItems)) {
            if ((item.value || '').includes(filter) || `${item.name}=${item.value}`.includes(filter)) {
                return true;
            }
        }

        return false;
    }

    buildPullMessagesResponse(subscription, events) {
        return `    <tev:PullMessagesResponse>
      <wsnt:CurrentTime>${new Date().toISOString()}</wsnt:CurrentTime>
      <wsnt:TerminationTime>${subscription.terminationTime}</wsnt:TerminationTime>
${events.map((event) => this.notificationMessageXml(event)).join('\n')}
    </tev:PullMessagesResponse>`;
    }

    notificationMessageXml(event) {
        return `      <wsnt:NotificationMessage>
        <wsnt:Topic Dialect="http://www.onvif.org/ver10/tev/topicExpression/ConcreteSet">${this.xmlEscape(event.topic || 'tns1:VideoSource/CellMotionDetector/Motion')}</wsnt:Topic>
        <wsnt:Message>
          <tt:Message UtcTime="${this.xmlEscape(event.utcTime || new Date().toISOString())}" PropertyOperation="${this.xmlEscape(event.propertyOperation || 'Changed')}">
            <tt:Source>
${(event.sourceItems || []).map((item) => `              <tt:SimpleItem Name="${this.xmlEscape(item.name)}" Value="${this.xmlEscape(item.value)}"/>`).join('\n')}
            </tt:Source>
            <tt:Data>
${(event.dataItems || []).map((item) => `              <tt:SimpleItem Name="${this.xmlEscape(item.name)}" Value="${this.xmlEscape(item.value)}"/>`).join('\n')}
            </tt:Data>
          </tt:Message>
        </wsnt:Message>
      </wsnt:NotificationMessage>`;
    }

    parseDuration(value, defaultMilliseconds) {
        if (!value) {
            return defaultMilliseconds;
        }

        let match = String(value).trim().match(/^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$/i);
        if (!match) {
            return defaultMilliseconds;
        }

        let hours = Number(match[1] || 0);
        let minutes = Number(match[2] || 0);
        let seconds = Number(match[3] || 0);
        return (((hours * 60) + minutes) * 60 + seconds) * 1000;
    }

    parseInteger(value, defaultValue) {
        let parsed = parseInt(value, 10);
        return Number.isFinite(parsed) ? parsed : defaultValue;
    }

    getSubscriptionAddress(id) {
        return `http://${this.config.hostname}:${this.config.ports.server}/onvif/events/subscriptions/${id}`;
    }

    getSubscriptionFromPath(requestPath) {
        let match = requestPath.match(/^\/onvif\/events\/subscriptions\/([^/]+)$/);
        if (!match) {
            throw new Error('Subscription reference missing from request path');
        }

        let subscription = this.subscriptions.get(match[1]);
        if (!subscription) {
            throw new Error(`Unknown ONVIF subscription ${match[1]}`);
        }

        return subscription;
    }

    getRequestValue(body, name) {
        let match = body.match(new RegExp(`<(?:\\w+:)?${name}\\b[^>]*>([\\s\\S]*?)</(?:\\w+:)?${name}>`));
        return match ? match[1].trim() : undefined;
    }

    xmlEscape(value) {
        return String(value)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&apos;');
    }
};