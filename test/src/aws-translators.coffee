chai = require('chai')
expect = chai.expect
chaiAsPromised = require('chai-as-promised')
chai.use(chaiAsPromised)
chance = require('chance').Chance()
lib = require('../lib/lib')["aws-translators"]
sinon = require('sinon')
Promise = require('bluebird')
{ DeleteItemCommand, BatchGetItemCommand, GetItemCommand, QueryCommand, ScanCommand, PutItemCommand, UpdateItemCommand } = require('@aws-sdk/client-dynamodb')

describe 'aws-translators', () ->
  describe '#getKeySchema', () ->
    it 'should parse out a hash key from an aws response', () ->
      hashKeyName = chance.word()
      result = lib.getKeySchema(
        Table:
          KeySchema:[
            AttributeName: hashKeyName
            KeyType: 'HASH'
          ]
          AttributeDefinitions: [
            AttributeName: hashKeyName
            AttributeType: 'N'
          ]
      )

      expect(result).to.have.property('hashKeyName')
      expect(result.hashKeyName).to.equal(hashKeyName)

      expect(result).to.have.property('hashKeyType')
      expect(result.hashKeyType).to.equal('N')

    it 'should parse out a range key from an aws response', () ->
      hashKeyName = chance.word()
      rangeKeyName = chance.word()
      result = lib.getKeySchema(
        Table:
          KeySchema:[
            {
              AttributeName: hashKeyName
              KeyType: 'HASH'
            },
            {
              AttributeName: rangeKeyName
              KeyType: 'RANGE'
            }
          ]
          AttributeDefinitions: [
            {
              AttributeName: hashKeyName
              AttributeType: 'S'
            },
            {
              AttributeName: rangeKeyName
              AttributeType: 'B'
            }
          ]
      )

  describe '#deleteItem', () ->

    dynastyTable = null
    sandbox = null
    lastCommand = null

    beforeEach () ->
      sandbox = sinon.sandbox.create()
      lastCommand = null
      dynastyTable =
        name: chance.name()
        parent:
          dynamo: {
            send: (command) ->
              lastCommand = command
              Promise.resolve('lol')
          }

    afterEach () ->
      sandbox.restore()

    it 'should return an object', () ->
      promise = lib.deleteItem.call(dynastyTable, 'foo', null, null,
        hashKeyName: 'bar'
        hashKeyType: 'S'
      )
      expect(promise).to.be.an('object')

    it 'should return a promise', () ->
      promise = lib.deleteItem.call(dynastyTable, 'foo', null, null,
        hashKeyName: 'bar'
        hashKeyType: 'S'
      )
      promise.then((d) -> expect(d).to.equal('lol'))

    it 'should call deleteItem of aws', () ->
      sandbox.spy(dynastyTable.parent.dynamo, "send")
      lib.deleteItem.call(dynastyTable, 'foo', null, null,
        hashKeyName: 'bar'
        hashKeyType: 'S'
      )
      expect(dynastyTable.parent.dynamo.send.calledOnce)

    it 'should call deleteItem of aws with extra params', () ->
      sandbox.spy(dynastyTable.parent.dynamo, "send")
      options =
        ReturnValues: 'NONE'
      lib.deleteItem.call(dynastyTable, 'foo', options, null,
        hashKeyName: 'bar'
        hashKeyType: 'S'
      )
      expect(dynastyTable.parent.dynamo.send.calledOnce)
      expect(lastCommand.input).to.include.keys('ReturnValues')

    it 'should send the table name to AWS', () ->
      sandbox.spy(dynastyTable.parent.dynamo, "send")

      promise = lib.deleteItem.call(dynastyTable, 'foo', null, null,
        hashKeyName: 'bar'
        hashKeyType: 'S'
      )

      promise.then () ->
        expect(dynastyTable.parent.dynamo.send.calledOnce)
        expect(lastCommand.input.TableName).to.equal(dynastyTable.name)

    it 'should send the hash key to AWS', () ->
      sandbox.spy(dynastyTable.parent.dynamo, "send")

      promise = lib.deleteItem.call(dynastyTable, 'foo', null, null,
        hashKeyName: 'bar'
        hashKeyType: 'S'
      )

      expect(dynastyTable.parent.dynamo.send.calledOnce)
      expect(lastCommand.input.Key).to.include.keys('bar')
      expect(lastCommand.input.Key.bar).to.include.keys('S')
      expect(lastCommand.input.Key.bar.S).to.equal('foo')

    it 'should send the hash and range key to AWS', () ->
      sandbox.spy(dynastyTable.parent.dynamo, "send")

      promise = lib.deleteItem.call(
        dynastyTable,
          hash: 'lol'
          range: 'rofl',
        null,
        null,
          hashKeyName: 'bar'
          hashKeyType: 'S'
          rangeKeyName: 'foo'
          rangeKeyType: 'S')

      expect(dynastyTable.parent.dynamo.send.calledOnce)

      expect(lastCommand.input.Key).to.include.keys('bar')
      expect(lastCommand.input.Key.bar).to.include.keys('S')
      expect(lastCommand.input.Key.bar.S).to.equal('lol')

      expect(lastCommand.input.Key).to.include.keys('foo')
      expect(lastCommand.input.Key.foo).to.include.keys('S')
      expect(lastCommand.input.Key.foo.S).to.equal('rofl')

  describe '#batchGetItem', () ->

    dynastyTable = null
    sandbox = null
    lastCommand = null

    beforeEach () ->
      tableName = chance.name()
      sandbox = sinon.sandbox.create()
      lastCommand = null
      dynastyTable =
        name: tableName
        parent:
          dynamo:
            send: (command) ->
              lastCommand = command
              result = {}
              result.Responses = {}
              result.Responses[tableName] = [
                { foo: S: "bar" },
                foo: S: "baz"
                bazzoo: N: 123
              ]
              Promise.resolve(result);

    afterEach () ->
      sandbox.restore()

    it 'should return a sane response', () ->
      lib.batchGetItem
        .call dynastyTable, ['bar', 'baz'], null,
          hashKeyName: 'foo'
          hashKeyType: 'S'
        .then (data) ->
          expect(data).to.deep.equal([
            { foo: 'bar' },
            foo: 'baz'
            bazzoo: 123])

  describe '#getItem', () ->

    dynastyTable = null
    sandbox = null
    lastCommand = null

    beforeEach () ->
      sandbox = sinon.sandbox.create()
      lastCommand = null
      dynastyTable =
        name: chance.name()
        parent:
          dynamo: {
            send: (command) ->
              lastCommand = command
              Promise.resolve(Item: rofl: S: 'lol')
          }

    afterEach () ->
      sandbox.restore()

    it 'should return an object', () ->
      promise = lib.getItem.call(dynastyTable, 'foo', null, null,
        hashKeyName: 'bar'
        hashKeyType: 'S'
      )

      expect(promise).to.be.an('object')

    it 'should return a promise', () ->
      lib.getItem
        .call dynastyTable, 'foo', null, null,
          hashKeyName: 'bar'
          hashKeyType: 'S'
        .then (data) ->
          expect(data).to.deep.equal(rofl: 'lol')

    it 'should call getItem of aws', () ->
      sandbox.spy(dynastyTable.parent.dynamo, "send")
      lib.getItem.call dynastyTable, 'foo', null, null,
        hashKeyName: 'bar'
        hashKeyType: 'S'

      expect(dynastyTable.parent.dynamo.send.calledOnce)
      expect(lastCommand.input.TableName).to.equal(dynastyTable.name)

    it 'should call getItem of aws with extra params', () ->
      sandbox.spy(dynastyTable.parent.dynamo, "send")
      options =
        ProjectionExpression: 'id, name'
      lib.getItem.call dynastyTable, 'foo', options, null,
        hashKeyName: 'bar'
        hashKeyType: 'S'

      expect(dynastyTable.parent.dynamo.send.calledOnce)
      expect(lastCommand.input.TableName).to.equal(dynastyTable.name)
      expect(lastCommand.input.ProjectionExpression).to.equal('id, name')

    it 'should send the table name to AWS', () ->
      sandbox.spy(dynastyTable.parent.dynamo, "send")

      lib.getItem
        .call dynastyTable, 'foo', null, null,
          hashKeyName: 'bar'
          hashKeyType: 'S'
        .then () ->
          expect(dynastyTable.parent.dynamo.send.calledOnce)
          expect(lastCommand.input.TableName).to.equal(dynastyTable.name)

    it 'should send the hash key to AWS', () ->
      sandbox.spy(dynastyTable.parent.dynamo, "send")

      promise = lib.getItem.call dynastyTable, 'foo', null, null,
        hashKeyName: 'bar'
        hashKeyType: 'S'

      expect(dynastyTable.parent.dynamo.send.calledOnce)
      expect(lastCommand.input.Key).to.include.keys('bar')
      expect(lastCommand.input.Key.bar).to.include.keys('S')
      expect(lastCommand.input.Key.bar.S).to.equal('foo')

    it 'should send the hash and range key to AWS', () ->
      sandbox.spy(dynastyTable.parent.dynamo, "send")

      promise = lib.getItem.call(
        dynastyTable,
          hash: 'lol'
          range: 'rofl',
        null,
        null,
          hashKeyName: 'bar'
          hashKeyType: 'S'
          rangeKeyName: 'foo'
          rangeKeyType: 'S')

      expect(dynastyTable.parent.dynamo.send.calledOnce)

      expect(lastCommand.input.Key).to.include.keys('bar')
      expect(lastCommand.input.Key.bar).to.include.keys('S')
      expect(lastCommand.input.Key.bar.S).to.equal('lol')

      expect(lastCommand.input.Key).to.include.keys('foo')
      expect(lastCommand.input.Key.foo).to.include.keys('S')
      expect(lastCommand.input.Key.foo.S).to.equal('rofl')


  describe '#scanPromise', () ->

    dynastyTable = null
    sandbox = null
    lastCommand = null

    beforeEach () ->
      sandbox = sinon.sandbox.create()
      lastCommand = null
      dynastyTable =
        name: chance.name()
        parent:
          dynamo: {
            send: (command) ->
              lastCommand = command
              Promise.resolve(Items: rofl: S: 'lol')
          }

    afterEach () ->
      sandbox.restore()

    it 'should return an object', () ->
      promise = lib.scan.call(dynastyTable, 'foo', null, null,
        hashKeyName: 'bar'
        hashKeyType: 'S'
      )

      expect(promise).to.be.an('object')

    it 'should return a promise', () ->
      lib.scan
      .call dynastyTable, 'foo', null, null,
        hashKeyName: 'bar'
        hashKeyType: 'S'
      .then (data) ->
        expect(data).to.deep.equal(rofl: 'lol')

    it 'should call scan of aws', () ->
      sandbox.spy(dynastyTable.parent.dynamo, "send")
      lib.scan.call dynastyTable, 'foo', null, null,
        hashKeyName: 'bar'
        hashKeyType: 'S'

      expect(dynastyTable.parent.dynamo.send.calledOnce)
      expect(lastCommand.input.TableName).to.equal(dynastyTable.name)

    it 'should call scan of aws with extra params', () ->
      sandbox.spy(dynastyTable.parent.dynamo, "send")
      options =
        ReturnConsumedCapacity: 'NONE'
      lib.scan.call dynastyTable, 'foo', options, null,
        hashKeyName: 'bar'
        hashKeyType: 'S'

      expect(dynastyTable.parent.dynamo.send.calledOnce)
      expect(lastCommand.input.TableName).to.equal(dynastyTable.name)
      expect(lastCommand.input.ReturnConsumedCapacity).to.equal('NONE')

  describe '#queryByHashKey', () ->

    dynastyTable = null
    sandbox = null
    lastCommand = null

    beforeEach () ->
      sandbox = sinon.sandbox.create()
      lastCommand = null
      dynastyTable =
        name: chance.name()
        parent:
          dynamo: {
            send: (command) ->
              lastCommand = command
              Promise.resolve Items: [{
                  foo: {S: 'bar'},
                  bar: {S: 'baz'}
                }]
          }

    afterEach () ->
      sandbox.restore()

    it 'should translate the response', () ->

      lib.queryByHashKey
        .call dynastyTable, 'bar', null,
          hashKeyName: 'foo'
          hashKeyType: 'S'
          rangeKeyName: 'bar'
          rangeKeyType: 'S'
        .then (data) ->
          expect(data).to.deep.equal [
            foo: 'bar'
            bar: 'baz'
          ]

    it 'should call query', () ->
      sandbox.spy(dynastyTable.parent.dynamo, "send")

      lib.queryByHashKey.call dynastyTable, 'bar', null,
        hashKeyName: 'foo'
        hashKeyType: 'S'
        rangeKeyName: 'bar'
        rangeKeyType: 'S'

      expect(dynastyTable.parent.dynamo.send.calledOnce)
      expect(lastCommand.input).to.include.keys('TableName', 'KeyConditions')

    it 'should send the table name and hash key to AWS', () ->
      sandbox.spy(dynastyTable.parent.dynamo, "send")
      promise = lib.queryByHashKey.call dynastyTable, 'bar', null,
        hashKeyName: 'foo'
        hashKeyType: 'S'
        rangeKeyName: 'bar'
        rangeKeyType: 'S'

      expect(dynastyTable.parent.dynamo.send.calledOnce)
      expect(lastCommand.input.TableName).to.equal(dynastyTable.name)
      expect(lastCommand.input.KeyConditions.foo.ComparisonOperator).to.equal('EQ')
      expect(lastCommand.input.KeyConditions.foo.AttributeValueList[0].S)
        .to.equal('bar')

  describe '#query', () ->

    dynastyTable = null
    sandbox = null
    lastCommand = null

    beforeEach () ->
      sandbox = sinon.sandbox.create()
      lastCommand = null
      dynastyTable =
        name: chance.name()
        parent:
          dynamo: {
            send: (command) ->
              lastCommand = command
              Promise.resolve Items: [{
                  foo: {S: 'bar'},
                  bar: {S: 'baz'}
                }]
          }

    afterEach () ->
      sandbox.restore()

    it 'should translate the response', () ->

      lib.query
        .call dynastyTable, 
          {
            indexName: chance.name(),
            keyConditions: [
              column: 'foo'
              value: 'bar'
            ]
          }, null, null,
        .then (data) ->
          expect(data).to.deep.equal [
            foo: 'bar'
            bar: 'baz'
          ]

    it 'should call query', () ->
      sandbox.spy(dynastyTable.parent.dynamo, "send")

      lib.query.call dynastyTable, 
        {
          indexName: chance.name(),
          keyConditions: [
            column: 'foo'
            value: 'bar'
          ]
        }, null, null

      expect(dynastyTable.parent.dynamo.send.calledOnce)
      expect(lastCommand.input).to.include.keys('TableName', 'IndexName', 'KeyConditions')

  describe '#putItem', () ->

    dynastyTable = null
    sandbox = null
    lastCommand = null

    beforeEach () ->
      sandbox = sinon.sandbox.create()
      lastCommand = null
      dynastyTable =
        name: chance.name()
        parent:
          dynamo: {
            send: (command) ->
              lastCommand = command
              Promise.resolve('lol')
          }

    afterEach () ->
      sandbox.restore()

    it 'should return an object', () ->
      promise = lib.putItem.call(dynastyTable, foo: 'bar', null, null)

      expect(promise).to.be.an('object')

    it 'should return a promise', () ->
      lib.putItem
        .call(dynastyTable, foo: 'bar', null, null)
        .then (data) ->
          expect(data).to.equal('lol')

    it 'should call putItem of aws', () ->
      sandbox.spy(dynastyTable.parent.dynamo, "send")

      lib.putItem.call(dynastyTable, foo: 'bar', null, null)

      expect(dynastyTable.parent.dynamo.send.calledOnce)
      expect(lastCommand.input).to.include.keys('Item', 'TableName')

    it 'should add extra params from options', () ->
      sandbox.spy(dynastyTable.parent.dynamo, "send")

      options =
        ReturnValues: 'ALL_NEW'

      lib.putItem.call(dynastyTable, foo: 'bar', options, null)

      expect(dynastyTable.parent.dynamo.send.calledOnce)
      expect(lastCommand.input).to.include.keys('Item', 'TableName', 'ReturnValues')

    it 'should send the table name to AWS', () ->
      sandbox.spy(dynastyTable.parent.dynamo, "send")

      lib.putItem
        .call(dynastyTable, foo: 'bar', null, null)
        .then () ->
          expect(dynastyTable.parent.dynamo.send.calledOnce)
          expect(lastCommand.input.TableName).to.equal(dynastyTable.name)

    it 'should send the translated object to AWS', () ->
      sandbox.spy(dynastyTable.parent.dynamo, "send")

      promise = lib.putItem.call dynastyTable, foo: 'bar', null, null

      expect(dynastyTable.parent.dynamo.send.calledOnce)
      expect(lastCommand.input.Item).to.be.an('object')
      expect(lastCommand.input.Item.foo).to.be.an('object')
      expect(lastCommand.input.Item.foo.S).to.equal('bar')

  describe '#updateItem', () ->

    dynastyTable = null
    sandbox = null
    lastCommand = null

    beforeEach () ->
      sandbox = sinon.sandbox.create()
      lastCommand = null
      dynastyTable =
        name: chance.name()
        parent:
          dynamo: {
            send: (command) ->
              lastCommand = command
              Promise.resolve('lol')
          }

    afterEach () ->
      sandbox.restore()


    it 'should automatically setup ExpressionAttributeNames mapping', () ->
      sandbox.spy(dynastyTable.parent.dynamo, "send")
      promise = lib.updateItem.call(dynastyTable, {}, foo: 'bar', null, null,
        hashKeyName: 'bar'
        hashKeyType: 'S'
      )
      expect(dynastyTable.parent.dynamo.send.calledOnce)
      expect(lastCommand.input.ExpressionAttributeNames).to.be.eql({"#foo": 'foo'})
