'use strict';

var setupDatabase = require('./shared').setupDatabase;
const chai = require('chai');
const fs = require('fs');
const expect = chai.expect;
const camelCase = require('lodash.camelcase');
chai.use(require('chai-subset'));

describe.only('Change Stream Spec', function() {
  const specStr = fs.readFileSync(`${__dirname}/spec/change-stream/change-streams.json`, 'utf8');
  const specData = JSON.parse(specStr);
  const ALL_DBS = [specData.database_name, specData.database_name_2];

  const delay = ms => new Promise(r => setTimeout(r, ms));

  before(function() {
    const MongoClient = this.configuration.require.MongoClient;
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
      .then(() => {
        const MongoClient = this.configuration.require.MongoClient;

        return new MongoClient(this.configuration.url(), { monitorCommands: true }).connect();
      })
      .then(client => {
        const ctx = (this.ctx = {});
        const events = (this.events = []);
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

  specData.tests.map(test => Object.assign({}, specData.defaults, test)).forEach(test => {
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
        expect(value).to.containSubset(result.success);
      }
    };
  }

  function makeTestFailure(test) {
    const result = test.result;

    return function testFailure(err) {
      if (!result.error) {
        throw err;
      }

      expect(err).to.containSubset(result.error);
    };
  }

  function makeTestAPM(test) {
    const expectedEvents = test.expectations;

    return function testAPM(ctx, events) {
      expectedEvents
        .map(e => e.command_started_event)
        .map(normalizeAPMEvent)
        .forEach((expected, idx) => {
          const actual = events[idx];
          expect(actual).to.containSubset(expected);
        });
    };
  }

  function makeTestFnRunOperations(test) {
    const target = test.target;
    const operations = test.operations;

    return function testFnRunOperations(ctx) {
      ctx.changeStream = ctx[target].watch();

      const changeStreamPromise = readAndCloseChangeStream(ctx.changeStream, test.numChanges);
      const operationsPromise = runOperations(ctx, operations);

      return Promise.all([changeStreamPromise, operationsPromise]).then(args => args[0]);
    };
  }

  function readAndCloseChangeStream(changeStream, numChanges) {
    const close = makeChangeStreamCloseFn(changeStream);
    let changeStreamPromise =
      numChanges > 0 ? changeStream.next().then(r => [r]) : Promise.resolve([]);

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

  function runOperations(ctx, operations) {
    return operations.map(op => makeOperation(ctx, op)).reduce((p, op) => p.then(op), delay(100));
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

  function makeOperation(ctx, op) {
    const target = ctx.client.db(op.database).collection(op.collection);
    const command = op.commandName;
    const args = op.arguments || undefined;
    return () => target[command].apply(target, args);
  }
});
