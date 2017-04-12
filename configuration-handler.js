  'use strict';

  const fs = require('fs');
  // const log = require('../../er/er-logger').getLogger('Configuration Handler');
  const storage = require('./grid-util');
  const messaging = require('messaging')('topic-messaging');

// nested `` operator
// like you can have eval inside an eval string,
// you can have `` inside ``
// if `x` = eval("x"), then `x(`y`)` = eval("x(eval\"y\")")
// `` operator provides simplicity by keeping nesting simple
  const rootPath = process.cwd();


  function handleFileError(fErr, resolve, reject) {
    // console.log(`handling error ${fErr}`);
    if (fErr.code === 'ENOENT') {
      resolve('{"v":0,"data":null}');
    } else {
      reject(fErr);
    }
  }

  function init(config) {
    config = config || {};
    function debug(...rest) {
      if (config.debug) {
        console.log(...rest);// eslint-disable-line no-console
      }
    }
    const self = this;
    self.ignoreList = new Set();
    debug('Initialising Configuration Handler with ', config);
    self.debug = config.debug;
    self.exitOn = config.exitOn ? config.exitOn.split(',') : [];
    self.serviceName = config.serviceName || '';
    self.onUpdate = config.onUpdate || (() => true);
    self.updateOn = config.updateOn ? config.updateOn.split(',') : [];
    self.confModel = config.confModel || 'configuration';
    this.configFolder = config.configFolder || 'configuration';
    this.url = config.url;
    debug(`Confguration DB URL: ${this.url}`);
    const defaultConfigMessaging = config.configMessaging || {
      port: 5007,
    };
    return new Promise((oldResolve) => {
      const resolve = () => {
        debug('Configuration messaging initialised');
        self.updateOn.forEach((updateMessage) => {
          messaging.subscribe(`${self.serviceName}-${updateMessage}`, self.onUpdate);
        });
        self.exitOn.forEach((exitMessage) => {
          messaging.subscribe(`${self.serviceName}-${exitMessage}`, () => {
            if (global.__exitingForConfigReoad) {
              debug('Duplicate message for termination recieved');
              return;
            }
            global.__exitingForConfigReoad = true;
            debug('\n\n\n\n\nPROCESS EXITING FOR CONFIGURATION RELOAD');
            for (let i = 1; i <= 10; i++) {
              const j = 10 - i;
              setTimeout(() => debug(`${new Array(j + 1).join('.')} ${j} ${new Array(10 - j).join('.')}`), i * 1000);
            }
            setTimeout(() => process.exit(0), 11000);
          });
        });

        oldResolve(self);
      };
      storage.init(self.url, self.confModel).then(() => {
        debug('Configuration storage initialised');
        messaging.init(defaultConfigMessaging, resolve);
      }).catch(() => {
        debug('Configuration storage failed to initialise');
        debug('db failed to connect at ', self.url);
        // messaging.init(defaultConfigMessaging, resolve);
        resolve();
      });
    });
  }

  function readFile(key, resolve, reject) {
    const self = this;
    fs.readFile(`${rootPath}/${self.configFolder}/${key}`, (errFile, res) => {
      if (errFile) {
        return handleFileError(errFile, resolve, reject);
      }
      resolve(res);
    });
  }

  function writeFile(key, data, resolve, reject) {
    const self = this;
    fs.writeFile(`${rootPath}/${self.configFolder}/${key}`, data, (errf) => {
      if (errf) {
        console.log('Configuration writeFile failed with: ', errf);// eslint-disable-line no-console
        if (reject) {
          reject(errf);
        }
      } else if (resolve) {
        resolve();
      }
    });
  }

  function getPromise(key, rejectIfEmpty) {
    key = `${this.serviceName}-${key}`;
    const self = this;
    function debug(...rest) {
      if (self.debug) {
        console.log(...rest);// eslint-disable-line no-console
      }
    }
    debug(`get key: ${key} called`);
    return () => new Promise((oldResolve, reject) => {
      function resolve(data) {
        if (rejectIfEmpty && data === null) {
          return reject(new Error('Empty Data'));
        }
        oldResolve(data);
      }
      storage.read(key).then((res) => {
        debug(`get-key-${key} from storage`);
        const result = JSON.parse(res);
        self.readFile(key, (fRes) => {
          debug(`get-key-${key} from file after storage\nfile read successful`);
          const fResult = JSON.parse(fRes);
          if (result.v > fResult.v) {
            debug(`${key} remote has higher version,\nwriting to local,\nresolving remote`);
            resolve(result.data);
            self.writeFile(key, res);
          } else if (result.v < fResult.v) {
            debug(`${key} remote has lower version,\nwriting to remote,\nresolving local`);
            resolve(fResult.data);
            self.putPromise(key)(fRes).then(() => {}, () => {});
          } else {
            debug(`${key} remote is in sync with local,\nresolving remote`);
            resolve(result.data);
          }
        }, () => {
          debug(`get-key-${key} failed from file after storage,\nwriting to file,\nresolving\n`);
          resolve(result.data);
          self.writeFile(key, res);
        });
      }).catch((err) => {
        debug(`Error in reading from db for ${key}: `, err.toString());
        debug(`get-key-${key} from files`);
        fs.readFile(`${rootPath}/${self.configFolder}/${key}`, (errFile, res) => {
          if (errFile) {
            return handleFileError(errFile, fRes => resolve(JSON.parse(fRes).data), reject);
          }
          resolve(JSON.parse(res).data);
        });
      });
    });
  }

  function putPromise(key, updateParam, noPublish) {
    const self = this;
    key = `${self.serviceName}-${key}`;
    function debug(...rest) {
      if (self.debug) {
        console.log(...rest);// eslint-disable-line no-console
      }
    }
    debug(`put key: ${key}`);
    return data => new Promise((resolve, reject) => {
      const configuration = JSON.stringify({
        data,
        v: new Date().getTime(),
      });
      storage.overWrite(key, configuration).then((res) => {
        if (!noPublish) {
          messaging.publish(`${self.serviceName}-${updateParam || key}`);
        }
        resolve(res);
        self.writeFile(key, configuration);
      }).catch((err) => {
        debug('db storage failure detected', err);
        self.writeFile(key, configuration, resolve, reject);
      });
    });
  }
  function getInstance() {
    return {
      getInstance: () => require('./configuration-handler'), // eslint-disable-line global-require
      close: storage.close.bind(storage),
      getPromise,
      putPromise,
      writeFile,
      readFile,
      init,
    };
  }
  module.exports = {
    getInstance,
    // close: storage.close.bind(storage),
    // getPromise,
    // putPromise,
    // writeFile,
    // init,
  };
