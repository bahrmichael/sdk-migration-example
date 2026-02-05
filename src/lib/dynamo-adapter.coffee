# DynamoDB v3 Adapter
# Provides v2-compatible Promise methods using AWS SDK v3

{ DynamoDBClient, CreateTableCommand, UpdateTableCommand, DescribeTableCommand,
  DeleteTableCommand, ListTablesCommand, DeleteItemCommand, BatchGetItemCommand,
  GetItemCommand, QueryCommand, ScanCommand, PutItemCommand, UpdateItemCommand } = require('@aws-sdk/client-dynamodb')

Promise = require('bluebird')
debug = require('debug')('dynasty:dynamo-adapter')

class DynamoAdapter
  constructor: (config) ->
    debug "Creating DynamoDB v3 client with config: #{JSON.stringify(config, null, 2)}"
    @client = new DynamoDBClient(config)

  # Table operations
  createTablePromise: (params) ->
    debug "createTable: #{JSON.stringify(params)}"
    Promise.resolve(@client.send(new CreateTableCommand(params)))

  updateTablePromise: (params) ->
    debug "updateTable: #{JSON.stringify(params)}"
    Promise.resolve(@client.send(new UpdateTableCommand(params)))

  describeTablePromise: (params) ->
    debug "describeTable: #{JSON.stringify(params)}"
    Promise.resolve(@client.send(new DescribeTableCommand(params)))

  deleteTablePromise: (params) ->
    debug "deleteTable: #{JSON.stringify(params)}"
    Promise.resolve(@client.send(new DeleteTableCommand(params)))

  listTablesPromise: (params) ->
    debug "listTables: #{JSON.stringify(params)}"
    Promise.resolve(@client.send(new ListTablesCommand(params)))

  # Item operations
  deleteItemPromise: (params) ->
    debug "deleteItem: #{JSON.stringify(params)}"
    Promise.resolve(@client.send(new DeleteItemCommand(params)))

  batchGetItemPromise: (params) ->
    debug "batchGetItem: #{JSON.stringify(params)}"
    Promise.resolve(@client.send(new BatchGetItemCommand(params)))

  getItemPromise: (params) ->
    debug "getItem: #{JSON.stringify(params)}"
    Promise.resolve(@client.send(new GetItemCommand(params)))

  queryPromise: (params) ->
    debug "query: #{JSON.stringify(params)}"
    Promise.resolve(@client.send(new QueryCommand(params)))

  scanPromise: (params) ->
    debug "scan: #{JSON.stringify(params)}"
    Promise.resolve(@client.send(new ScanCommand(params)))

  putItemPromise: (params) ->
    debug "putItem: #{JSON.stringify(params)}"
    Promise.resolve(@client.send(new PutItemCommand(params)))

  updateItemPromise: (params) ->
    debug "updateItem: #{JSON.stringify(params)}"
    Promise.resolve(@client.send(new UpdateItemCommand(params)))

  # Direct method access for callback-style pagination (used in processAllPages)
  query: (params, callback) ->
    @client.send(new QueryCommand(params))
      .then((result) -> callback(null, result))
      .catch((err) -> callback(err))

  scan: (params, callback) ->
    @client.send(new ScanCommand(params))
      .then((result) -> callback(null, result))
      .catch((err) -> callback(err))

module.exports = DynamoAdapter
