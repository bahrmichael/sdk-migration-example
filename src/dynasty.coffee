# Main Dynasty Class

{
  DynamoDBClient
  DescribeTableCommand
  CreateTableCommand
  DeleteTableCommand
  ListTablesCommand
  UpdateTableCommand
  GetItemCommand
  PutItemCommand
  DeleteItemCommand
  UpdateItemCommand
  QueryCommand
  ScanCommand
  BatchGetItemCommand
} = require('@aws-sdk/client-dynamodb')

_ = require('lodash')
Promise = require('bluebird')
debug = require('debug')('dynasty')

# See http://vq.io/19EiASB
typeToAwsType =
  string: 'S'
  string_set: 'SS'
  number: 'N'
  number_set: 'NS'
  binary: 'B'
  binary_set: 'BS'

lib = require('./lib')
Table = lib.Table

# Wrap v3 client to provide v2-compatible API with *Promise methods
wrapClient = (client) ->
  sendP = (CommandClass, params) ->
    # Wrap native promise into Bluebird so .nodeify() keeps working
    Promise.resolve client.send(new CommandClass(params))

  cbify = (p, cb) ->
    return p unless cb
    p.then((data) -> cb(null, data)).catch((err) -> cb(err))
    null

  dynamo =
    describeTablePromise: (params) -> sendP(DescribeTableCommand, params)
    describeTable: (params, cb) -> cbify(sendP(DescribeTableCommand, params), cb)

    createTablePromise: (params) -> sendP(CreateTableCommand, params)
    createTable: (params, cb) -> cbify(sendP(CreateTableCommand, params), cb)

    deleteTablePromise: (params) -> sendP(DeleteTableCommand, params)
    deleteTable: (params, cb) -> cbify(sendP(DeleteTableCommand, params), cb)

    listTablesPromise: (params) -> sendP(ListTablesCommand, params)
    listTables: (params, cb) -> cbify(sendP(ListTablesCommand, params), cb)

    updateTablePromise: (params) -> sendP(UpdateTableCommand, params)
    updateTable: (params, cb) -> cbify(sendP(UpdateTableCommand, params), cb)

    getItemPromise: (params) -> sendP(GetItemCommand, params)
    getItem: (params, cb) -> cbify(sendP(GetItemCommand, params), cb)

    putItemPromise: (params) -> sendP(PutItemCommand, params)
    putItem: (params, cb) -> cbify(sendP(PutItemCommand, params), cb)

    deleteItemPromise: (params) -> sendP(DeleteItemCommand, params)
    deleteItem: (params, cb) -> cbify(sendP(DeleteItemCommand, params), cb)

    updateItemPromise: (params) -> sendP(UpdateItemCommand, params)
    updateItem: (params, cb) -> cbify(sendP(UpdateItemCommand, params), cb)

    queryPromise: (params) -> sendP(QueryCommand, params)
    query: (params, cb) -> cbify(sendP(QueryCommand, params), cb)

    scanPromise: (params) -> sendP(ScanCommand, params)
    scan: (params, cb) -> cbify(sendP(ScanCommand, params), cb)

    batchGetItemPromise: (params) -> sendP(BatchGetItemCommand, params)
    batchGetItem: (params, cb) -> cbify(sendP(BatchGetItemCommand, params), cb)

  dynamo

class Dynasty

  constructor: (credentials, url) ->
    debug "dynasty constructed."
    
    # Build client config
    clientConfig = {}
    
    # Determine region
    if credentials.region
      clientConfig.region = credentials.region
    else if process.env.AWS_DEFAULT_REGION
      clientConfig.region = process.env.AWS_DEFAULT_REGION
    else
      clientConfig.region = 'us-east-1'

    # Set up credentials if provided (otherwise SDK uses default chain)
    creds = {}
    hasCredentials = false
    
    if credentials.accessKeyId
      creds.accessKeyId = credentials.accessKeyId
      hasCredentials = true
    else if process.env.AWS_ACCESS_KEY_ID
      creds.accessKeyId = process.env.AWS_ACCESS_KEY_ID
      hasCredentials = true

    if credentials.secretAccessKey
      creds.secretAccessKey = credentials.secretAccessKey
      hasCredentials = true
    else if process.env.AWS_SECRET_ACCESS_KEY
      creds.secretAccessKey = process.env.AWS_SECRET_ACCESS_KEY
      hasCredentials = true
      
    if credentials.sessionToken
      creds.sessionToken = credentials.sessionToken
    else if process.env.AWS_SESSION_TOKEN
      creds.sessionToken = process.env.AWS_SESSION_TOKEN

    # Set endpoint for local DynamoDB
    if url and _.isString url
      debug "connecting to local dynamo at #{url}"
      clientConfig.endpoint = url
      # For local DynamoDB, provide dummy credentials if none set
      if !hasCredentials
        creds.accessKeyId = 'local'
        creds.secretAccessKey = 'local'
        hasCredentials = true

    if hasCredentials
      clientConfig.credentials = creds

    dynamoClient = new DynamoDBClient(clientConfig)
    @dynamo = wrapClient(dynamoClient)
    @name = 'Dynasty'
    @tables = {}

  loadAllTables: =>
    @list()
      .then (data) =>
        for tableName in data.TableNames
          @table(tableName)
        return @tables

  # Given a name, return a Table object
  table: (name) ->
    @tables[name] = @tables[name] || new Table this, name

  ###
  Table Operations
  ###

  # Alter an existing table. Wrapper around AWS updateTable
  alter: (name, params, callback) ->
    debug "alter() - #{name}, #{JSON.stringify(params, null, 4)}"
    # We'll accept either an object with a key of throughput or just
    # an object with the throughput info
    throughput = params.throughput || params

    awsParams =
      TableName: name
      ProvisionedThroughput:
        ReadCapacityUnits: throughput.read
        WriteCapacityUnits: throughput.write

    @dynamo.updateTablePromise(awsParams).nodeify(callback)

  # Create a new table. Wrapper around AWS createTable
  create: (name, params, callback = null) ->
    debug "create() - #{name}, #{JSON.stringify(params, null, 4)}"
    throughput = params.throughput || {read: 10, write: 5}

    keySchema = [
      KeyType: 'HASH'
      AttributeName: params.key_schema.hash[0]
    ]

    attributeDefinitions = [
      AttributeName: params.key_schema.hash[0]
      AttributeType: typeToAwsType[params.key_schema.hash[1]]
    ]

    if params.key_schema.range?
      keySchema.push
        KeyType: 'RANGE',
        AttributeName: params.key_schema.range[0]
      attributeDefinitions.push
        AttributeName: params.key_schema.range[0]
        AttributeType: typeToAwsType[params.key_schema.range[1]]

    awsParams =
      AttributeDefinitions: attributeDefinitions
      TableName: name
      KeySchema: keySchema
      ProvisionedThroughput:
        ReadCapacityUnits: throughput.read
        WriteCapacityUnits: throughput.write

    # Add GlobalSecondaryIndexes to awsParams if provided
    if params.global_secondary_indexes?
      awsParams.GlobalSecondaryIndexes = []
      # Verify valid GSI
      for index in params.global_secondary_indexes
        key_schema = index.key_schema
        # Must provide hash type
        unless key_schema.hash?
          throw TypeError 'Missing hash index for GlobalSecondaryIndex'
        typesProvided = Object.keys(key_schema).length
        # Provide 1-2 types for GSI
        if typesProvided.length > 2 or typesProvided.length < 1
          throw RangeError 'Expected one or two types for GlobalSecondaryIndex'
        # Providing 2 types but the second isn't range type
        if typesProvided.length is 2 and not key_schema.range?
          throw TypeError 'Two types provided but the second isn\'t range'
      # Push each index
      for index in params.global_secondary_indexes
        keySchema = []
        for type, keys of index.key_schema
          keySchema.push({
            AttributeName: keys[0]
            KeyType: type.toUpperCase()
          })
        awsParams.GlobalSecondaryIndexes.push {
          IndexName: index.index_name
          KeySchema: keySchema
          Projection:
            ProjectionType: index.projection_type.toUpperCase()
          # Use the provided or default throughput
          ProvisionedThroughput: unless index.provisioned_throughput? then awsParams.ProvisionedThroughput else {
            ReadCapacityUnits: index.provisioned_throughput.read
            WriteCapacityUnits: index.provisioned_throughput.write
          }
        }
        # Add key name to attributeDefinitions
        for type, keys of index.key_schema
          awsParams.AttributeDefinitions.push {
            AttributeName: keys[0]
            AttributeType: typeToAwsType[keys[1]]
          } if awsParams.AttributeDefinitions.filter( (ad) -> ad.AttributeName == keys[0] ).length == 0

    debug "creating table with params #{JSON.stringify(awsParams, null, 4)}"

    @dynamo.createTablePromise(awsParams).nodeify(callback)

  # describe
  describe: (name, callback) ->
    debug "describe() - #{name}"
    @dynamo.describeTablePromise(TableName: name).nodeify(callback)

  # Drop a table. Wrapper around AWS deleteTable
  drop: (name, callback = null) ->
    debug "drop() - #{name}"
    params =
      TableName: name

    @dynamo.deleteTablePromise(params).nodeify(callback)

  # List tables. Wrapper around AWS listTables
  list: (params, callback) ->
    debug "list() - #{params}"
    awsParams = {}

    if params isnt null
      if _.isString params
        awsParams.ExclusiveStartTableName = params
      else if _.isFunction params
        callback = params
      else if _.isObject params
        if params.limit is not null
          awsParams.Limit = params.limit
        else if params.start is not null
          awsParams.ExclusiveStartTableName = params.start

    @dynamo.listTablesPromise(awsParams).nodeify(callback)

module.exports = (credentials, url) -> new Dynasty(credentials, url)
