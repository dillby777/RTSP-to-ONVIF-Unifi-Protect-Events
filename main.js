const tcpProxy = require('node-tcp-proxy');
const argparse = require('argparse');
const logger = require('simple-node-logger').createSimpleLogger();

const OnvifServer = require('./src/onvif-server');
const OnvifEventClientPool = require('./src/onvif-event-client-pool');
const { readAndCheckConfig } = require('./src/config-tools');


const parser = new argparse.ArgumentParser({
    description: 'Virtual RTSP to ONVIF proxy'
});

parser.add_argument('config', { help: 'config filename to use', nargs: '?' });

let args = parser.parse_args();

if (args) {
    if (process.env.DEBUG) {
        logger.setLevel('trace');
    }

    if (!args.config) {
        logger.info('Please specifiy a config filename!');
        return -1;
    }

    let config = readAndCheckConfig(logger, args.config)
    let eventClientPool = new OnvifEventClientPool(logger);

    let proxies = {};
    let servers = [];
    for (let onvifConfig of config.onvif) {

        let server = new OnvifServer(logger, onvifConfig, eventClientPool);

        if (server.getHostname()) {
            servers.push(server);

            logger.info('');
            server.startHttpServer();
            server.startDiscovery();
            if (process.env.DEBUG)
                server.enableDebugOutput()

            if (!proxies[onvifConfig.target.hostname])
                proxies[onvifConfig.target.hostname] = {}

            if (onvifConfig.ports.rtsp && onvifConfig.target.ports.rtsp)
                proxies[onvifConfig.target.hostname][onvifConfig.ports.rtsp] = onvifConfig.target.ports.rtsp;
            if (onvifConfig.ports.snapshot && onvifConfig.target.ports.snapshot)
                proxies[onvifConfig.target.hostname][onvifConfig.ports.snapshot] = onvifConfig.target.ports.snapshot;
        } else {
            logger.error(`Failed to find IP address for MAC address ${onvifConfig.mac}`)
            return -1;
        }
    }

    let shutdown = () => {
        for (let server of servers) {
            server.close();
        }

        eventClientPool.closeAll();
    };

    process.on('SIGINT', shutdown);
    process.on('SIGTERM', shutdown);

    for (let destinationAddress in proxies) {
        for (let sourcePort in proxies[destinationAddress]) {
            logger.info(`PROXY: ${sourcePort} --> ${destinationAddress}:${proxies[destinationAddress][sourcePort]}`);
            tcpProxy.createProxy(sourcePort, destinationAddress, proxies[destinationAddress][sourcePort]);
        }
    }

    return 0;
}