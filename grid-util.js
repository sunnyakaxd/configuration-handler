'use strict';

const mongodb = require('mongodb');
const stream = require('stream');

const MongoClient = mongodb.MongoClient;
const GridFSBucket = mongodb.GridFSBucket;

function init(url, bucketName) {
  const self = this;
  const initOptions = {
    bucketName,
  };
  if (!url) {
    return new Promise((res, reject) => {
      reject('url can\'t be empty');
    });
  }
  if (typeof url === 'object') {
    self.db = url;
    self.bucket = new GridFSBucket(url, initOptions);
    return new Promise(resolve => resolve(self.bucket));
  }
  return new Promise((resolve, reject) => {
    MongoClient.connect(url, (err, db) => {
      if (err) {
        return reject(err);
      }
      self.db = db;
      self.bucket = new GridFSBucket(db, initOptions);
      resolve(self.bucket);
    });
  });
}

function write(filename, data, isStream) {
  const self = this;
  return new Promise((resolve, reject) => {
    const dataStream = isStream ? data : (() => {
      const newStream = new stream.Readable();
      newStream._read = (() => {});
      newStream.push(data);
      newStream.push(null);
      return newStream;
    })();
    const uploadStream = self.bucket.openUploadStream(filename);
    uploadStream.once('finish', () => {
      resolve(uploadStream.id);
    });
    uploadStream.once('error', (err) => {
      reject(err);
    });
    dataStream.pipe(uploadStream);
    // setTimeout(() => uploadStream.end(), 2000);
  });
}


function read(filename, wantStream) {
  const self = this;
  return new Promise((resolve, reject) => {
    const dataWrapper = [];
    const downloadStream = self.bucket.openDownloadStreamByName(filename);
    if (wantStream) {
      return resolve(wantStream);
    }
    downloadStream.once('error', err => reject(err));
    downloadStream.once('close', () => {
      resolve(dataWrapper.join(''));
    });
    downloadStream.on('data', (data) => {
      dataWrapper.push(data.toString('ascii'));
    });
  });
}

function purge(filename) {
  const self = this;
  return new Promise((resolve, reject) => {
    const cursor = self.bucket.find({
      filename,
    });
    cursor.next((err, fileInfo) => {
      cursor.close();
      if (err) {
        console.log('error has occurrred');
        return reject(err);
      }
      if (!fileInfo) {
        return reject('ENOENT');
      }
      self.bucket.delete(fileInfo._id, (delErr) => {
        if (delErr) {
          reject(delErr);
        }
        resolve();
      });
    });
  });
}
function overWrite(filename, data) {
  const self = this;
  return new Promise((resolve, reject) => {
    const cursor = self.bucket.find({
      filename,
    });
    cursor.next((err, fileInfo) => {
      if (err) {
        return reject(err);
      }
      self.write(filename, data).then((id) => {
        if (fileInfo) {
          return self.bucket.delete(fileInfo._id).then(() => resolve(id));
        }
        resolve(id);
      });
    });
  });
}

function close() {
  const self = this;
  return new Promise((resolve) => {
    if (self.db) {
      self.db.close(resolve);
      delete self.db;
      delete self.bucket;
    } else {
      resolve();
    }
  });
}

module.exports = {
  init,
  write,
  read,
  purge,
  overWrite,
  close,
};
