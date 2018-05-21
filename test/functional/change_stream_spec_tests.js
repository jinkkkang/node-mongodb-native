'use strict';

const EJSON = require('mongodb-extjson');
const chai = require('chai');
const fs = require('fs');
const camelCase = require('lodash.camelcase');
const MongoClient = require('../../lib/mongo_client');
const setupDatabase = require('./shared').setupDatabase;
const delay = require('./shared').delay;
const expect = chai.expect;

describe('Change Stream Spec', function() {
  const specStr = fs.readFileSync(`${__dirname}/spec/change-stream/change-streams.json`, 'utf8');
  const specData = JSON.parse(specStr);
  const ALL_DBS = [specData.database_name, specData.database_name_2];

  const EJSONToJSON = x => JSON.parse(EJSON.stringify(x));

  before(function() {
    return setupDatabase(this.configuration, ALL_DBS).then(() => {
      this.globalClient = new MongoClient(this.configuration.url());
      return this.globalClient.connect();
    });
  });

  after(function() {
    const gc = this.globalClient;
    this.globalClient = undefined;
    return new Promise(r => gc.close(() => r()));
  });

  beforeEach(function() {
    const gc = this.globalClient;
    const sDB = specData.database_name;
    const sColl = specData.collection_name;
    return Promise.all(ALL_DBS.map(db => gc.db(db).dropDatabase()))
      .then(() => gc.db(sDB).createCollection(sColl))
      .then(() => new MongoClient(this.configuration.url(), { monitorCommands: true }).connect())
      .then(client => {
        const ctx = (this.ctx = {});
        const events = (this.events = []);
        ctx.gc = gc;
        ctx.client = client;
        ctx.db = ctx.client.db(sDB);
        ctx.collection = ctx.db.collection(sColl);
        ctx.client.on('commandStarted', e => events.push(e));
      });
  });

  afterEach(function(done) {
    const ctx = this.ctx;
    this.ctx = undefined;
    this.events = undefined;

    if (ctx.client) {
      ctx.client.close(e => done(e));
    } else {
      done();
    }
  });

  specData.tests.forEach(test => {
    const itFn = test.skip ? it.skip : test.only ? it.only : it;
    const metadata = generateMetadata(test);
    const testFn = generateTestFn(test);

    itFn(test.description, { metadata, test: testFn });
  });

  // Fn Generator methods

  function generateMetadata(test) {
    const mongodb = test.minServerVersion;
    const topology = test.topology;
    const requires = {};
    if (mongodb) {
      requires.mongodb = `>=${mongodb}`;
    }
    if (topology) {
      requires.topology = topology;
    }

    return { requires };
  }

  function generateTestFn(test) {
    const testFnRunOperations = makeTestFnRunOperations(test);
    const testSuccess = makeTestSuccess(test);
    const testFailure = makeTestFailure(test);
    const testAPM = makeTestAPM(test);

    return function testFn() {
      return testFnRunOperations(this.ctx)
        .then(testSuccess, testFailure)
        .then(() => testAPM(this.ctx, this.events));
    };
  }

  function makeTestSuccess(test) {
    const result = test.result;

    return function testSuccess(value) {
      if (result.error) {
        throw new Error(`Expected test to return error ${result.error}`);
      }

      if (result.success) {
        value = EJSONToJSON(value);
        assertEquality(value, result.success);
      }
    };
  }

  function makeTestFailure(test) {
    const result = test.result;

    return function testFailure(err) {
      if (!result.error) {
        throw err;
      }

      assertEquality(err, result.error);
    };
  }

  function makeTestAPM(test) {
    const expectedEvents = test.expectations;

    return function testAPM(ctx, events) {
      expectedEvents
        .map(e => e.command_started_event)
        .map(normalizeAPMEvent)
        .forEach((expected, idx) => {
          if (!events[idx]) {
            throw new Error(
              `Expected there to be an APM event at index ${idx}, but there was none`
            );
          }
          const actual = EJSONToJSON(events[idx]);
          assertEquality(actual, expected);
        });
    };
  }

  function makeTestFnRunOperations(test) {
    const target = test.target;
    const operations = test.operations;
    const success = test.result.success || [];

    return function testFnRunOperations(ctx) {
      const changeStreamPipeline = test.changeStreamPipeline;
      const changeStreamOptions = test.changeStreamOptions;
      ctx.changeStream = ctx[target].watch(changeStreamPipeline, changeStreamOptions);

      const changeStreamPromise = readAndCloseChangeStream(ctx.changeStream, success.length);
      const operationsPromise = runOperations(ctx.gc, operations);

      return Promise.all([changeStreamPromise, operationsPromise]).then(args => args[0]);
    };
  }

  function readAndCloseChangeStream(changeStream, numChanges) {
    const close = makeChangeStreamCloseFn(changeStream);
    let changeStreamPromise = changeStream.next().then(r => [r]);

    for (let i = 1; i < numChanges; i += 1) {
      changeStreamPromise = changeStreamPromise.then(results => {
        return changeStream.next().then(result => {
          results.push(result);
          return results;
        });
      });
    }

    return changeStreamPromise.then(result => close(null, result), err => close(err));
  }

  function runOperations(client, operations) {
    return operations
      .map(op => makeOperation(client, op))
      .reduce((p, op) => p.then(op), delay(200));
  }

  function makeChangeStreamCloseFn(changeStream) {
    return function close(error, value) {
      return new Promise((resolve, reject) => {
        changeStream.close(err => {
          if (error || err) {
            return reject(error || err);
          }
          return resolve(value);
        });
      });
    };
  }

  function normalizeAPMEvent(raw) {
    return Object.keys(raw).reduce((agg, key) => {
      agg[camelCase(key)] = raw[key];
      return agg;
    }, {});
  }

  function makeOperation(client, op) {
    const target = client.db(op.database).collection(op.collection);
    const command = op.name;
    const args = [];
    if (op.arguments && op.arguments.document) {
      args.push(op.arguments.document);
    }
    return () => target[command].apply(target, args);
  }

  function assertEquality(actual, expected) {
    try {
      _assertEquality(actual, expected);
    } catch (e) {
      console.dir(actual, { depth: 999 });
      console.dir(expected, { depth: 999 });
      throw e;
    }
  }

  function _assertEquality(actual, expected) {
    try {
      if (expected === '42' || expected === 42) {
        expect(actual).to.exist;
        return;
      }

      expect(actual).to.be.a(Array.isArray(expected) ? 'array' : typeof expected);

      if (expected == null) {
        expect(actual).to.not.exist;
      } else if (Array.isArray(expected)) {
        expected.forEach((ex, idx) => _assertEquality(actual[idx], ex));
      } else if (typeof expected === 'object') {
        for (let i in expected) {
          _assertEquality(actual[i], expected[i]);
        }
      } else {
        expect(actual).to.equal(expected);
      }
    } catch (e) {
      throw e;
    }
  }
});
