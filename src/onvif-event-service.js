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
        this.recentEventsLimit = 32;
        this.recentEvents = [];
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

    getPrimaryEventsPath() {
        return '/onvif/events_service';
    }

    isEventsPath(requestPath) {
        return this.isEventsServicePath(requestPath) || this.isSubscriptionPath(requestPath);
    }

    isEventsServicePath(requestPath) {
        return requestPath === '/onvif/event_service'
            || requestPath === '/onvif/events_service'
            || requestPath === '/onvif/event'
            || requestPath === '/onvif/events';
    }

    isSubscriptionPath(requestPath) {
        return /^\/onvif\/(?:event_service|events_service|event|events)\/subscriptions\//.test(requestPath);
    }

    handleRequest(action, requestPath, body) {
        let normalizedAction = this.normalizeAction(action);

        switch (normalizedAction) {
            case 'GetEventProperties':
                return this.getEventPropertiesResponse();
            case 'GetServiceCapabilities':
                return this.getServiceCapabilitiesResponse();
            case 'Subscribe':
                return this.createSubscribeResponse(body);
            case 'CreatePullPointSubscription':
                return this.createPullPointSubscriptionResponse(body);
            case 'PullMessages':
            case 'Pull':
                return this.pullMessagesResponse(requestPath, body);
            case 'Renew':
                return this.renewResponse(requestPath, body);
            case 'Unsubscribe':
                return this.unsubscribeResponse(requestPath, body);
            default:
                throw new Error(`Unsupported ONVIF event action ${normalizedAction || action || '(none)'}`);
        }
    }

    normalizeAction(action) {
        if (!action) {
            return action;
        }

        let normalized = String(action).trim().replace(/^.*[/:]/, '');
        normalized = normalized.replace(/Request$/, '');

        if (normalized === 'Pull') {
            return 'PullMessages';
        }

        return normalized;
    }

    getEventPropertiesResponse() {
        return `    <tev:GetEventPropertiesResponse>
      <tev:TopicNamespaceLocation>http://www.onvif.org/ver10/topics/topicns.xml</tev:TopicNamespaceLocation>
      <wsnt:TopicSet xmlns:tns1="http://www.onvif.org/ver10/topics" xmlns:wstop="http://docs.oasis-open.org/wsn/t-1">
                <tns1:RuleEngine wstop:topic="true">
                    <tns1:CellMotionDetector wstop:topic="true">
                        <tns1:Motion wstop:topic="true"/>
                    </tns1:CellMotionDetector>
                </tns1:RuleEngine>
                <tns1:VideoSource wstop:topic="true">
                    <tns1:CellMotionDetector wstop:topic="true">
                        <tns1:Motion wstop:topic="true"/>
                    </tns1:CellMotionDetector>
                    <tns1:MotionAlarm wstop:topic="true"/>
                    <tns1:GlobalSceneChange wstop:topic="true">
                        <tns1:AnalyticsService wstop:topic="true"/>
                    </tns1:GlobalSceneChange>
                </tns1:VideoSource>
      </wsnt:TopicSet>
            <tev:FixedTopicSet>true</tev:FixedTopicSet>
      <tev:TopicExpressionDialect>http://www.onvif.org/ver10/tev/topicExpression/ConcreteSet</tev:TopicExpressionDialect>
            <tev:TopicExpressionDialect>http://docs.oasis-open.org/wsn/t-1/TopicExpression/Concrete</tev:TopicExpressionDialect>
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
                this.logger.info(`EVENTS: ${this.config.name} created local pull-point subscription ${subscription.id}`);

        return `    <tev:CreatePullPointSubscriptionResponse>
            ${this.subscriptionReferenceXml(subscription.id)}
      <wsnt:CurrentTime>${new Date().toISOString()}</wsnt:CurrentTime>
      <wsnt:TerminationTime>${subscription.terminationTime}</wsnt:TerminationTime>
    </tev:CreatePullPointSubscriptionResponse>`;
    }

        createSubscribeResponse(body) {
                let timeout = this.parseDuration(this.getRequestValue(body, 'InitialTerminationTime') || this.getRequestValue(body, 'TerminationTime'), 300000);
                let subscription = this.createSubscription(timeout);
                this.logger.info(`EVENTS: ${this.config.name} created local WS-Notification subscription ${subscription.id}`);

                return `    <wsnt:SubscribeResponse>
            ${this.subscriptionReferenceXml(subscription.id)}
            <wsnt:CurrentTime>${new Date().toISOString()}</wsnt:CurrentTime>
            <wsnt:TerminationTime>${subscription.terminationTime}</wsnt:TerminationTime>
        </wsnt:SubscribeResponse>`;
        }

        subscriptionReferenceXml(subscriptionId) {
                let subscriptionAddress = this.getSubscriptionAddress(subscriptionId);
                return `<wsnt:SubscriptionReference>
                    <wsa:Address>${subscriptionAddress}</wsa:Address>
                    <wsa:ReferenceParameters>
                    <tev:SubscriptionId>${subscriptionId}</tev:SubscriptionId>
                    </wsa:ReferenceParameters>
            </wsnt:SubscriptionReference>`;
        }

    pullMessagesResponse(requestPath, body) {
        let subscription = this.resolveSubscription(requestPath, body);
        let timeout = this.parseDuration(this.getRequestValue(body, 'Timeout'), 0);
        let messageLimit = this.parseInteger(this.getRequestValue(body, 'MessageLimit'), 10);

        this.logger.info(`EVENTS: ${this.config.name} local pull request on ${requestPath} (queued=${subscription.queue.length}, timeoutMs=${timeout}, limit=${messageLimit})`);

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
        let subscription = this.resolveSubscription(requestPath, body);
        let timeout = this.parseDuration(this.getRequestValue(body, 'TerminationTime'), 300000);
        subscription.terminationTime = new Date(Date.now() + timeout).toISOString();
        this.logger.debug(`EVENTS: ${this.config.name} renewed local subscription ${subscription.id} for ${timeout}ms`);

        return `    <wsnt:RenewResponse>
      <wsnt:CurrentTime>${new Date().toISOString()}</wsnt:CurrentTime>
      <wsnt:TerminationTime>${subscription.terminationTime}</wsnt:TerminationTime>
    </wsnt:RenewResponse>`;
    }

    unsubscribeResponse(requestPath, body) {
        let subscription = this.resolveSubscription(requestPath, body);
        this.logger.info(`EVENTS: ${this.config.name} unsubscribed local pull-point ${subscription.id}`);
        this.deleteSubscription(subscription.id);
        return `    <wsnt:UnsubscribeResponse/>`;
    }

    createSubscription(timeout) {
        let id = uuid.v4();
        let bootstrapEvents = this.getRecentEventsSnapshot();
        let subscription = {
            id,
            queue: bootstrapEvents,
            pendingPull: null,
            terminationTime: new Date(Date.now() + timeout).toISOString()
        };

        this.subscriptions.set(id, subscription);

        if (bootstrapEvents.length) {
            this.logger.info(`EVENTS: ${this.config.name} primed local subscription ${id} with ${bootstrapEvents.length} recent event(s)`);
        }

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

        let eventsToDeliver = this.expandEventVariants(event);
        for (let expandedEvent of eventsToDeliver) {
            this.pushRecentEvent(expandedEvent);
        }

        if (this.subscriptions.size === 0) {
            this.logger.info(`EVENTS: ${this.config.name} received upstream event but no local ONVIF subscriptions are active yet`);
        }

        for (let subscription of this.subscriptions.values()) {
            for (let expandedEvent of eventsToDeliver) {
                subscription.queue.push(expandedEvent);
            }

            if (subscription.pendingPull) {
                clearTimeout(subscription.pendingPull.timer);
                let pendingPull = subscription.pendingPull;
                subscription.pendingPull = null;
                pendingPull.resolve(this.buildPullMessagesResponse(subscription, subscription.queue.splice(0, pendingPull.messageLimit)));
            }
        }
    }

    matchesSourceFilter(event) {
        let filter = this.config.events && typeof this.config.events.source === 'string' ? this.config.events.source.trim() : '';
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
                let topic = this.xmlEscape(event.topic || 'tns1:VideoSource/CellMotionDetector/Motion');
        return `      <wsnt:NotificationMessage>
                <wsnt:Topic xmlns:tns1="http://www.onvif.org/ver10/topics" Dialect="http://www.onvif.org/ver10/tev/topicExpression/ConcreteSet">${topic}</wsnt:Topic>
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

    expandEventVariants(event) {
        let baseEvent = this.cloneEvent(event);
        let variants = [baseEvent];
        let topic = String(baseEvent.topic || '');
        let looksLikeMotion = topic.includes('CellMotionDetector/Motion') || topic.includes('MotionAlarm');
        let motionValue = this.extractMotionState(baseEvent.dataItems);

        if (!looksLikeMotion || motionValue === null) {
            return variants;
        }

        let sourceToken = this.extractSourceToken(baseEvent.sourceItems, baseEvent.sourceText);
        let sourceItems = this.withSourceAliases(baseEvent.sourceItems, sourceToken);
        let dataItems = [{ name: 'IsMotion', value: motionValue ? 'true' : 'false' }];

        variants.push(this.createDerivedEvent(baseEvent, 'tns1:RuleEngine/CellMotionDetector/Motion', sourceItems, dataItems));
        variants.push(this.createDerivedEvent(baseEvent, 'tns1:VideoSource/MotionAlarm', sourceItems, dataItems));
        variants.push(this.createDerivedEvent(baseEvent, 'tns1:VideoSource/CellMotionDetector/Motion', sourceItems, dataItems));

        return this.dedupeEvents(variants);
    }

    cloneEvent(event) {
        return {
            topic: event.topic || '',
            sourceItems: asArray(event.sourceItems).map((item) => ({ name: item.name || '', value: item.value || '' })),
            sourceText: event.sourceText || '',
            dataItems: asArray(event.dataItems).map((item) => ({ name: item.name || '', value: item.value || '' })),
            dataText: event.dataText || '',
            utcTime: event.utcTime || new Date().toISOString(),
            propertyOperation: event.propertyOperation || 'Changed'
        };
    }

    createDerivedEvent(baseEvent, topic, sourceItems, dataItems) {
        return {
            topic,
            sourceItems,
            sourceText: sourceItems.map((item) => `${item.name}: ${item.value}`).join(' '),
            dataItems,
            dataText: dataItems.map((item) => `${item.name}: ${item.value}`).join(' '),
            utcTime: baseEvent.utcTime,
            propertyOperation: baseEvent.propertyOperation
        };
    }

    extractMotionState(dataItems) {
        for (let item of asArray(dataItems)) {
            let name = String(item.name || '').toLowerCase();
            if (name !== 'ismotion' && name !== 'state') {
                continue;
            }

            let normalized = String(item.value || '').trim().toLowerCase();
            if (normalized === 'true' || normalized === '1' || normalized === 'on') {
                return true;
            }

            if (normalized === 'false' || normalized === '0' || normalized === 'off') {
                return false;
            }
        }

        return null;
    }

    extractSourceToken(sourceItems, sourceText) {
        for (let item of asArray(sourceItems)) {
            let name = String(item.name || '').toLowerCase();
            if (name.includes('videosourceconfigurationtoken') || name === 'source' || name === 'token' || name.includes('rule')) {
                let value = String(item.value || '').trim();
                if (value) {
                    return value;
                }
            }
        }

        let sourceMatch = String(sourceText || '').match(/\b(?:source|videosourceconfigurationtoken|token)\s*[:=]\s*([A-Za-z0-9_-]+)/i);
        return sourceMatch ? sourceMatch[1] : '';
    }

    withSourceAliases(sourceItems, sourceToken) {
        let aliases = asArray(sourceItems).map((item) => ({ name: item.name || '', value: item.value || '' }));
        if (!sourceToken) {
            return aliases;
        }

        let hasSource = aliases.some((item) => String(item.name || '').toLowerCase() === 'source');
        if (!hasSource) {
            aliases.push({ name: 'Source', value: sourceToken });
        }

        let hasToken = aliases.some((item) => String(item.name || '').toLowerCase() === 'token');
        if (!hasToken) {
            aliases.push({ name: 'Token', value: sourceToken });
        }

        return aliases;
    }

    dedupeEvents(events) {
        let seen = new Set();
        let deduped = [];

        for (let event of events) {
            let key = `${event.topic}|${event.sourceText}|${event.dataText}|${event.utcTime}`;
            if (seen.has(key)) {
                continue;
            }

            seen.add(key);
            deduped.push(event);
        }

        return deduped;
    }

    pushRecentEvent(event) {
        this.recentEvents.push(event);
        if (this.recentEvents.length > this.recentEventsLimit) {
            this.recentEvents.splice(0, this.recentEvents.length - this.recentEventsLimit);
        }
    }

    getRecentEventsSnapshot() {
        return this.recentEvents.slice();
    }

    getSubscriptionAddress(id) {
        return `http://${this.config.hostname}:${this.config.ports.server}${this.getPrimaryEventsPath()}/subscriptions/${id}`;
    }

    getSubscriptionFromPath(requestPath) {
        let match = requestPath.match(/^\/onvif\/(?:event_service|events_service|event|events)\/subscriptions\/([^/]+)$/);
        if (!match) {
            throw new Error('Subscription reference missing from request path');
        }

        let subscription = this.subscriptions.get(match[1]);
        if (!subscription) {
            throw new Error(`Unknown ONVIF subscription ${match[1]}`);
        }

        return subscription;
    }

    getSubscriptionIdFromBody(body) {
        let idFromPath = this.getRequestValue(body || '', 'Address');
        if (idFromPath) {
            let pathMatch = idFromPath.match(/\/onvif\/(?:event_service|events_service|event|events)\/subscriptions\/([^/\s<]+)$/);
            if (pathMatch) {
                return pathMatch[1];
            }
        }

        let idFromReference = this.getRequestValue(body || '', 'SubscriptionId');
        if (idFromReference) {
            return idFromReference;
        }

        return null;
    }

    resolveSubscription(requestPath, body) {
        if (this.isSubscriptionPath(requestPath)) {
            return this.getSubscriptionFromPath(requestPath);
        }

        let subscriptionId = this.getSubscriptionIdFromBody(body);
        if (subscriptionId && this.subscriptions.has(subscriptionId)) {
            return this.subscriptions.get(subscriptionId);
        }

        if (this.subscriptions.size === 1) {
            return this.subscriptions.values().next().value;
        }

        if (this.subscriptions.size === 0 && this.isEventsServicePath(requestPath)) {
            // Compatibility fallback for clients that call PullMessages without creating a pull-point first.
            let fallbackSubscription = this.createSubscription(300000);
            this.logger.info(`EVENTS: ${this.config.name} auto-created local pull-point subscription ${fallbackSubscription.id} for compatibility`);
            return fallbackSubscription;
        }

        if (this.subscriptions.size === 0) {
            throw new Error('No active local ONVIF event subscription');
        }

        throw new Error('Subscription reference missing or ambiguous in request');
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