const http = require('http');
const https = require('https');
const crypto = require('crypto');
const { EventEmitter } = require('events');
const xml2js = require('xml2js');

function asArray(value) {
    if (Array.isArray(value)) {
        return value;
    }

    if (value === undefined || value === null) {
        return [];
    }

    return [value];
}

function firstText(value) {
    if (value === undefined || value === null) {
        return undefined;
    }

    if (typeof value === 'string') {
        return value;
    }

    if (typeof value === 'object' && typeof value._ === 'string') {
        return value._;
    }

    return undefined;
}

function findNodes(node, key, matches = []) {
    if (node === null || node === undefined) {
        return matches;
    }

    if (Array.isArray(node)) {
        for (let entry of node) {
            findNodes(entry, key, matches);
        }

        return matches;
    }

    if (typeof node !== 'object') {
        return matches;
    }

    for (let [childKey, childValue] of Object.entries(node)) {
        if (childKey === key) {
            matches.push(...asArray(childValue));
        }

        findNodes(childValue, key, matches);
    }

    return matches;
}

function parseSimpleItems(section) {
    let items = [];
    for (let simpleItem of asArray(section && section.SimpleItem)) {
        items.push({
            name: simpleItem && simpleItem.$ ? simpleItem.$.Name : undefined,
            value: simpleItem && simpleItem.$ ? simpleItem.$.Value : undefined
        });
    }
    return items.filter((item) => item.name !== undefined || item.value !== undefined);
}

function renderSimpleItems(items) {
    return items.map((item) => `${item.name}: ${item.value}`).join(' ');
}

module.exports = class OnvifEventClient extends EventEmitter {
    constructor(logger, key, config) {
        super();

        this.logger = logger;
        this.key = key;
        this.config = config;
        this.eventsConfig = config.events;
        this.discoveryUrl = new URL(`http://${config.target.hostname}:${config.events.port}${config.events.path}`);
        this.eventsServiceUrl = null;
        this.pullPointUrl = null;
        this.terminationTime = null;
        this.closed = false;
        this.startPromise = null;
        this.reconnectTimer = null;
        this.preferredAuth = this.eventsConfig.auth || 'digest';
        this.activePull = false;
    }

    start() {
        if (this.startPromise) {
            return this.startPromise;
        }

        this.startPromise = this.connectLoop();
        return this.startPromise;
    }

    close() {
        this.closed = true;
        if (this.reconnectTimer) {
            clearTimeout(this.reconnectTimer);
            this.reconnectTimer = null;
        }
    }

    async connectLoop() {
        while (!this.closed) {
            try {
                await this.ensureRemoteSubscription();
                await this.pollLoop();
            } catch (error) {
                if (this.closed) {
                    return;
                }

                this.logger.warn(`EVENTS: ${this.config.name} upstream event relay error: ${error.message}`);
                this.pullPointUrl = null;
                this.terminationTime = null;
                await this.wait(5000);
            }
        }
    }

    async wait(milliseconds) {
        return new Promise((resolve) => {
            this.reconnectTimer = setTimeout(() => {
                this.reconnectTimer = null;
                resolve();
            }, milliseconds);

            if (typeof this.reconnectTimer.unref === 'function') {
                this.reconnectTimer.unref();
            }
        });
    }

    async ensureRemoteSubscription() {
        if (!this.eventsServiceUrl) {
            this.eventsServiceUrl = await this.discoverEventsServiceUrl();
            this.logger.info(`EVENTS: ${this.config.name} using upstream event service ${this.eventsServiceUrl}`);
        }

        if (!this.pullPointUrl) {
            let responseXml = await this.sendSoapRequest(this.eventsServiceUrl, `    <tev:CreatePullPointSubscription xmlns:tev="http://www.onvif.org/ver10/events/wsdl">
      <tev:InitialTerminationTime>PT5M</tev:InitialTerminationTime>
    </tev:CreatePullPointSubscription>`);

            let parsed = await this.parseXml(responseXml);
            let addresses = findNodes(parsed, 'Address').map(firstText).filter(Boolean);
            this.pullPointUrl = addresses[0] || this.eventsServiceUrl;

            let terminationTimes = findNodes(parsed, 'TerminationTime').map(firstText).filter(Boolean);
            this.terminationTime = terminationTimes[0] || null;
            this.logger.info(`EVENTS: ${this.config.name} subscribed to upstream pull point ${this.pullPointUrl}`);
        }
    }

    async discoverEventsServiceUrl() {
        let responseXml = await this.sendSoapRequest(this.discoveryUrl.toString(), `    <tds:GetServices xmlns:tds="http://www.onvif.org/ver10/device/wsdl">
      <tds:IncludeCapability>false</tds:IncludeCapability>
    </tds:GetServices>`);
        let parsed = await this.parseXml(responseXml);
        let services = findNodes(parsed, 'Service');

        for (let service of services) {
            let namespace = firstText(asArray(service.Namespace)[0]);
            let xaddr = firstText(asArray(service.XAddr)[0]);
            if (namespace && namespace.includes('/events/wsdl') && xaddr) {
                return xaddr;
            }
        }

        return this.discoveryUrl.toString();
    }

    async pollLoop() {
        while (!this.closed && this.pullPointUrl) {
            await this.renewIfNeeded();
            let responseXml = await this.sendSoapRequest(this.pullPointUrl, `    <tev:PullMessages xmlns:tev="http://www.onvif.org/ver10/events/wsdl">
      <tev:Timeout>PT30S</tev:Timeout>
      <tev:MessageLimit>10</tev:MessageLimit>
    </tev:PullMessages>`);
            let events = await this.parseNotificationMessages(responseXml);

            for (let event of events) {
                this.emit('event', event);
            }
        }
    }

    async renewIfNeeded() {
        if (!this.pullPointUrl || !this.terminationTime) {
            return;
        }

        let expiration = Date.parse(this.terminationTime);
        if (Number.isNaN(expiration) || expiration - Date.now() > 60000) {
            return;
        }

        let responseXml = await this.sendSoapRequest(this.pullPointUrl, `    <wsnt:Renew xmlns:wsnt="http://docs.oasis-open.org/wsn/b-2">
      <wsnt:TerminationTime>PT5M</wsnt:TerminationTime>
    </wsnt:Renew>`);
        let parsed = await this.parseXml(responseXml);
        let terminationTimes = findNodes(parsed, 'TerminationTime').map(firstText).filter(Boolean);
        this.terminationTime = terminationTimes[0] || this.terminationTime;
    }

    async parseNotificationMessages(xml) {
        let parsed = await this.parseXml(xml);
        let notificationMessages = findNodes(parsed, 'NotificationMessage');

        return notificationMessages.map((notification) => this.normalizeNotificationMessage(notification)).filter(Boolean);
    }

    normalizeNotificationMessage(notification) {
        let topicValue = firstText(asArray(notification.Topic)[0]) || '';
        let messageWrapper = asArray(notification.Message)[0] || {};
        let message = asArray(messageWrapper.Message)[0] || messageWrapper;

        let sourceSection = asArray(message.Source)[0] || {};
        let dataSection = asArray(message.Data)[0] || {};
        let sourceItems = parseSimpleItems(sourceSection);
        let dataItems = parseSimpleItems(dataSection);

        return {
            topic: topicValue,
            sourceItems,
            sourceText: renderSimpleItems(sourceItems),
            dataItems,
            dataText: renderSimpleItems(dataItems),
            utcTime: message.$ && message.$.UtcTime ? message.$.UtcTime : new Date().toISOString(),
            propertyOperation: message.$ && message.$.PropertyOperation ? message.$.PropertyOperation : 'Changed'
        };
    }

    async parseXml(xml) {
        return xml2js.parseStringPromise(xml, {
            tagNameProcessors: [xml2js.processors.stripPrefix],
            explicitArray: true
        });
    }

    soapEnvelope(content) {
        return `<?xml version="1.0" encoding="UTF-8"?>
<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope" xmlns:tds="http://www.onvif.org/ver10/device/wsdl" xmlns:tev="http://www.onvif.org/ver10/events/wsdl" xmlns:wsnt="http://docs.oasis-open.org/wsn/b-2">
  <s:Body>
${content}
  </s:Body>
</s:Envelope>`;
    }

    async sendSoapRequest(endpoint, content) {
        let requestXml = this.soapEnvelope(content);
        let endpointUrl = new URL(endpoint);
        let preferredMode = this.preferredAuth === 'basic' ? 'basic' : 'digest';
        let endpointPath = `${endpointUrl.pathname}${endpointUrl.search}`;
        let attemptedModes = [];

        let attemptBasic = async () => {
            attemptedModes.push('basic');
            return this.requestOnce(endpointUrl, requestXml, {
                Authorization: this.basicAuthorizationHeader()
            });
        };

        let attemptDigest = async (challenge) => {
            attemptedModes.push('digest');
            return this.requestOnce(endpointUrl, requestXml, {
                Authorization: this.digestAuthorizationHeader(challenge, 'POST', endpointPath)
            });
        };

        if (preferredMode === 'basic') {
            let basicResponse = await attemptBasic();
            if (basicResponse.statusCode >= 200 && basicResponse.statusCode < 300) {
                return basicResponse.body;
            }

            if (basicResponse.statusCode === 401 && /digest/i.test(basicResponse.headers['www-authenticate'] || '')) {
                let digestResponse = await attemptDigest(basicResponse.headers['www-authenticate']);
                if (digestResponse.statusCode >= 200 && digestResponse.statusCode < 300) {
                    return digestResponse.body;
                }

                throw new Error(this.buildAuthError(attemptedModes, digestResponse.statusCode, digestResponse.headers));
            }

            throw new Error(this.buildAuthError(attemptedModes, basicResponse.statusCode, basicResponse.headers));
        }

        let initialResponse = await this.requestOnce(endpointUrl, requestXml, {});
        if (initialResponse.statusCode >= 200 && initialResponse.statusCode < 300) {
            return initialResponse.body;
        }

        if (initialResponse.statusCode !== 401) {
            throw new Error(`request failed with status ${initialResponse.statusCode}`);
        }

        let challenge = initialResponse.headers['www-authenticate'] || '';
        if (/digest/i.test(challenge)) {
            let digestResponse = await attemptDigest(challenge);
            if (digestResponse.statusCode >= 200 && digestResponse.statusCode < 300) {
                return digestResponse.body;
            }

            if (digestResponse.statusCode !== 401) {
                throw new Error(`request failed with status ${digestResponse.statusCode}`);
            }
        }

        let basicResponse = await attemptBasic();
        if (basicResponse.statusCode >= 200 && basicResponse.statusCode < 300) {
            return basicResponse.body;
        }

        throw new Error(this.buildAuthError(attemptedModes, basicResponse.statusCode, basicResponse.headers));
    }

    buildAuthError(attemptedModes, statusCode, headers) {
        let uniqueModes = [...new Set(attemptedModes)];
        let challenge = headers && headers['www-authenticate'] ? headers['www-authenticate'] : 'none';
        return `authentication failed (attempted: ${uniqueModes.join(', ')}, status: ${statusCode}, challenge: ${challenge})`;
    }

    basicAuthorizationHeader() {
        return `Basic ${Buffer.from(`${this.eventsConfig.username}:${this.eventsConfig.password}`).toString('base64')}`;
    }

    digestAuthorizationHeader(challenge, method, uri) {
        let params = {};
        for (let match of challenge.matchAll(/([a-zA-Z]+)="?([^",]+)"?/g)) {
            params[match[1].toLowerCase()] = match[2];
        }

        let realm = params.realm;
        let nonce = params.nonce;
        let qop = params.qop ? params.qop.split(',')[0].trim() : 'auth';
        let opaque = params.opaque;
        let cnonce = crypto.randomBytes(8).toString('hex');
        let nc = '00000001';

        let ha1 = crypto.createHash('md5').update(`${this.eventsConfig.username}:${realm}:${this.eventsConfig.password}`).digest('hex');
        let ha2 = crypto.createHash('md5').update(`${method}:${uri}`).digest('hex');
        let response = crypto.createHash('md5').update(`${ha1}:${nonce}:${nc}:${cnonce}:${qop}:${ha2}`).digest('hex');

        let header = `Digest username="${this.eventsConfig.username}", realm="${realm}", nonce="${nonce}", uri="${uri}", response="${response}", qop=${qop}, nc=${nc}, cnonce="${cnonce}"`;
        if (opaque) {
            header += `, opaque="${opaque}"`;
        }

        return header;
    }

    requestOnce(endpointUrl, body, headers) {
        let transport = endpointUrl.protocol === 'https:' ? https : http;
        let requestHeaders = Object.assign({
            'Content-Type': 'application/soap+xml; charset=utf-8',
            'Content-Length': Buffer.byteLength(body)
        }, headers);

        return new Promise((resolve, reject) => {
            let request = transport.request({
                method: 'POST',
                hostname: endpointUrl.hostname,
                port: endpointUrl.port || (endpointUrl.protocol === 'https:' ? 443 : 80),
                path: `${endpointUrl.pathname}${endpointUrl.search}`,
                headers: requestHeaders
            }, (response) => {
                let chunks = [];
                response.on('data', (chunk) => chunks.push(chunk));
                response.on('end', () => resolve({
                    statusCode: response.statusCode,
                    headers: response.headers,
                    body: Buffer.concat(chunks).toString('utf8')
                }));
            });

            request.on('error', reject);
            request.write(body);
            request.end();
        });
    }
};