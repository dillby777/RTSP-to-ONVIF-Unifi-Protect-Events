const YAML = require('yaml');
const fs = require('fs');
const { execSync } = require('child_process');

const { getIp4FromMac, generateUUIDv4, generateNetworkMac } = require('./net-tools')


function failConfig(logger, message) {
    logger.error(`CONFIG: ${message}`);
    process.exit(-1);
}

function validateEventsConfig(logger, onvifConfig) {
    if (!onvifConfig.events) {
        return;
    }

    if (typeof onvifConfig.events !== 'object') {
        failConfig(logger, `${onvifConfig.name}: events must be a map when present`);
    }

    if (onvifConfig.events.enabled === undefined) {
        onvifConfig.events.enabled = true;
    } else {
        onvifConfig.events.enabled = Boolean(onvifConfig.events.enabled);
    }

    if (!onvifConfig.events.enabled) {
        return;
    }

    let requiredStringFields = ['path', 'username', 'password'];
    for (let field of requiredStringFields) {
        if (typeof onvifConfig.events[field] !== 'string' || !onvifConfig.events[field].trim()) {
            failConfig(logger, `${onvifConfig.name}: events.${field} must be a non-empty string when events are enabled`);
        }
    }

    if (!Number.isInteger(onvifConfig.events.port) || onvifConfig.events.port < 1 || onvifConfig.events.port > 65535) {
        failConfig(logger, `${onvifConfig.name}: events.port must be an integer between 1 and 65535 when events are enabled`);
    }

    if (!onvifConfig.events.path.startsWith('/')) {
        failConfig(logger, `${onvifConfig.name}: events.path must start with '/'`);
    }

    if (!onvifConfig.events.auth) {
        onvifConfig.events.auth = 'digest';
    }

    onvifConfig.events.auth = String(onvifConfig.events.auth).toLowerCase();
    if (!['basic', 'digest'].includes(onvifConfig.events.auth)) {
        failConfig(logger, `${onvifConfig.name}: events.auth must be either 'basic' or 'digest'`);
    }

    if (onvifConfig.events.source !== undefined) {
        if (typeof onvifConfig.events.source !== 'string') {
            failConfig(logger, `${onvifConfig.name}: events.source must be a string when set (quote numeric-like values, e.g. "00000")`);
        }

        let normalizedSource = onvifConfig.events.source.trim();
        if (!normalizedSource) {
            // Empty source means "no source filter" for easier troubleshooting and onboarding.
            delete onvifConfig.events.source;
        } else {
            onvifConfig.events.source = normalizedSource;
        }
    }
}


function readConfig(logger, configFile) {

    let configData;
    try {
        configData = fs.readFileSync(configFile, 'utf8');
    } catch (error) {
        if (error.code === 'ENOENT') {
            logger.info(`File not found: ${configFile}`);
            process.exit(-1);
        }
        throw error;
    }

    let config;
    try {
        config = YAML.parse(configData);
    } catch (error) {
        logger.info('Failed to read config, invalid yaml syntax.')
        process.exit(-1);
    }

    return config;
}

function sleep(seconds){
    const spawnSync = require('child_process').spawnSync;
    var sleep = spawnSync('sleep', [seconds]);
}

function readAndCheckConfig(logger, configFile) {

    
    let config = readConfig(logger, configFile);

        if (!config || !Array.isArray(config.onvif)) {
            failConfig(logger, 'top-level onvif list is required');
        }

    let isSaveRequired = false;
    let proxyCounter = 0;
    for (let onvifConfig of config.onvif) {
            validateEventsConfig(logger, onvifConfig);

        //Generate a V4 UUID
        if (!onvifConfig.uuid) {
            let newId = generateUUIDv4();
            logger.info(`CONFIG: UUIDv4 - ${newId}`);
            onvifConfig.uuid = newId;
            isSaveRequired = true;
        }

        // Generate Network MAC for Unicast LAA Prefix
        if (!onvifConfig.mac) {
            let newId = generateNetworkMac();
            logger.info(`CONFIG: MAC - ${newId}`);
            onvifConfig.mac = newId;
            isSaveRequired = true;
        }

        if (!getIp4FromMac(logger, onvifConfig.mac)) {
            const vlanName = `rtsp2onvif_${proxyCounter}`;

            logger.info(`NET_CONF: ADD - ${vlanName} MAC: ${onvifConfig.mac}`);
            try {
                const stdout = execSync(`ip link add ${vlanName} link ${onvifConfig.dev} address ${onvifConfig.mac} type macvlan mode bridge`);
                logger.debug(stdout);
            } catch (error) {
                logger.debug(error.message);
            }

            logger.info(`NET_CONF: DHCP - ${vlanName}`);
            try {
                const stdout = execSync(`dhclient ${vlanName}`);
                logger.debug(stdout);
            } catch (error) {
                logger.debug(error.message);
            }

            // logger.info(`NET_CONF: Set ${vlanName} IPv4 ${this.config.ipv4}`)
            // try {
            //     execSync(`ip addr add ${this.config.ipv4} dev ${vlanName}`)
            // } catch (error) {
            //     logger.debug(error.message)
            // }

            // logger.info(`NET_CONF: Set ${vlanName} UP`)
            // try {
            //     execSync(`ip link set ${vlanName} up`)
            // } catch (error) {
            //     logger.debug(error.message)
            // }
        }
        proxyCounter++
    }

    if (isSaveRequired) {
        writeConfig(logger, configFile, config);
        sleep(2);
    }

    return config;
}

function writeConfig(logger, configFile, config) {
    const yamlString = YAML.stringify(config);

    fs.writeFileSync(configFile, yamlString, 'utf8');
    logger.info(`CONFIG: Updated ${configFile}`);
}

module.exports = {
    readAndCheckConfig
}