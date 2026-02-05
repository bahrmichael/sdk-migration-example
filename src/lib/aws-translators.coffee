_ = require('lodash')
dataTrans = require('./data-translators')
Promise = require('bluebird')
debug = require('debug')('dynasty:aws-translators')
{ GetItemCommand, PutItemCommand, DeleteItemCommand, UpdateItemCommand, QueryCommand, ScanCommand, BatchGetItemCommand } = require('@aws-sdk/client-dynamodb')

buildFilters = (target, filters) ->
  if filters
    scanFilterFunc(target, filter) for filter in filters

scanFilterFunc = (target, filter) ->
  target[filter.column] =
    ComparisonOperator: filter.op || 'EQ'
    AttributeValueList: [{}]
  target[filter.column].AttributeValueList[0][filter.type || 'S'] = filter.value
  target

addAwsParams = (target, params) ->
  for key, value of params
    if key not in target and key[0] == key[0].toUpperCase()
      target[key] = value

getCommandForFunction = (functionName, params) ->
  switch functionName
    when 'query' then new QueryCommand(params)
    when 'scan' then new ScanCommand(params)
    else throw new Error("Unknown function: #{functionName}")

module.exports.processAllPages = (deferred, dynamo, functionName, params)->

  stats =
    Count: 0

  resultHandler = (result) ->
    deferred.notify dataTrans.fromDynamo result.Items
    stats.Count += result.Count
    if result.LastEvaluatedKey
      params.ExclusiveStartKey = result.LastEvaluatedKey
      dynamo.send(getCommandForFunction(functionName, params))
        .then(resultHandler)
        .catch((err) -> deferred.reject(err))
    else
      deferred.resolve stats

  dynamo.send(getCommandForFunction(functionName, params))
    .then(resultHandler)
    .catch((err) -> deferred.reject(err))
  deferred.promise


module.exports.getKeySchema = (tableDescription) ->
  getKeyAndType = (keyType) ->
    keyName = _.find tableDescription.Table.KeySchema, (key) ->
      key.KeyType is keyType
    ?.AttributeName

    keyDataType = _.find tableDescription.Table.AttributeDefinitions,
    (attribute) ->
      attribute.AttributeName is keyName
    ?.AttributeType
    [keyName, keyDataType]

  [hashKeyName, hashKeyType] = getKeyAndType 'HASH'
  [rangeKeyName, rangeKeyType] = getKeyAndType 'RANGE'

  hashKeyName: hashKeyName
  hashKeyType: hashKeyType
  rangeKeyName: rangeKeyName
  rangeKeyType: rangeKeyType

getKey = (params, keySchema) ->
  if !_.isObject params
    params = hash: params+''

  key = {}
  key[keySchema.hashKeyName] = {}
  key[keySchema.hashKeyName][keySchema.hashKeyType] = params.hash+''

  if params.range
    key[keySchema.rangeKeyName] = {}
    key[keySchema.rangeKeyName][keySchema.rangeKeyType] = params.range+''

  key

module.exports.deleteItem = (params, options, callback, keySchema) ->
  awsParams =
    TableName: @name
    Key: getKey(params, keySchema)

  addAwsParams(awsParams, options)

  @parent.dynamo.send(new DeleteItemCommand(awsParams))

module.exports.batchGetItem = (params, callback, keySchema) ->
  awsParams = {}
  awsParams.RequestItems = {}
  name = @name
  awsParams.RequestItems[@name] = Keys: _.map(params, (param) -> getKey(param, keySchema))

  addAwsParams(awsParams, params)

  @parent.dynamo.send(new BatchGetItemCommand(awsParams))
    .then (data) ->
      dataTrans.fromDynamo(data.Responses[name])
    .nodeify(callback)

module.exports.getItem = (params, options, callback, keySchema) ->
  awsParams =
    TableName: @name
    Key: getKey(params, keySchema)

  addAwsParams(awsParams, options)

  @parent.dynamo.send(new GetItemCommand(awsParams))
    .then (data)->
      dataTrans.fromDynamo(data.Item)
    .nodeify(callback)

module.exports.queryByHashKey = (key, callback, keySchema) ->
  awsParams =
    TableName: @name
    KeyConditions: {}

  hashKeyName = keySchema.hashKeyName
  hashKeyType = keySchema.hashKeyType

  awsParams.KeyConditions[hashKeyName] =
    ComparisonOperator: 'EQ'
    AttributeValueList: [{}]
  awsParams.KeyConditions[hashKeyName].AttributeValueList[0][hashKeyType] = key

  @parent.dynamo.send(new QueryCommand(awsParams))
    .then (data) ->
      dataTrans.fromDynamo(data.Items)
    .nodeify(callback)

module.exports.scan = (params, options, callback, keySchema) ->
  params ?= {}
  awsParams =
    TableName: @name
    ScanFilter: {}
    AttributesToGet: params.attrsGet || null
    Limit: params.limit
    TotalSegments: params.totalSegment
    Segment: params.segment

  if params.ExclusiveStartKey?
    awsParams.ExclusiveStartKey = {}
    for prop, val of params.ExclusiveStartKey
      awsParams.ExclusiveStartKey[prop] = dataTrans.toDynamo val

  buildFilters(awsParams.ScanFilter, params.filters)

  addAwsParams(awsParams, options)

  @parent.dynamo.send(new ScanCommand(awsParams))
    .then (data)->
      dataTrans.fromDynamo(data.Items)
    .nodeify(callback)

module.exports.query = (params, options, callback, keySchema) ->
  params ?= {}
  awsParams =
    TableName: @name
    IndexName: params.indexName
    KeyConditions: {}
    QueryFilter: {}

  buildFilters(awsParams.KeyConditions, params.keyConditions)
  buildFilters(awsParams.QueryFilter, params.filters)

  addAwsParams(awsParams, options)

  @parent.dynamo.send(new QueryCommand(awsParams))
    .then (data) ->
      dataTrans.fromDynamo(data.Items)
    .nodeify(callback)

module.exports.putItem = (obj, options, callback) ->
  awsParams =
    TableName: @name
    Item: _.transform(obj, (res, val, key) ->
      res[key] = dataTrans.toDynamo(val))

  addAwsParams(awsParams, options)

  @parent.dynamo.send(new PutItemCommand(awsParams))

module.exports.updateItem = (params, obj, options, callback, keySchema) ->
  key = getKey(params, keySchema)

  # Set up the Expression Attribute Values map.
  expressionAttributeValues = _.mapKeys obj, (value, key) -> return ':' + key
  expressionAttributeValues = _.mapValues expressionAttributeValues, (value, key) -> return dataTrans.toDynamo value

  # Setup ExpressionAttributeNames mapping key -> #key so we don't bump into
  # reserved words
  expressionAttributeNames = {}
  expressionAttributeNames["##{key}"] = key for key, i in Object.keys(obj)

  # Set up the Update Expression
  updateExpression = 'SET ' + _.keys(_.mapKeys obj, (value, key) -> "##{key} = :#{key}").join ','

  awsParams =
    TableName: @name
    Key: getKey(params, keySchema)
    ExpressionAttributeNames: expressionAttributeNames
    ExpressionAttributeValues: expressionAttributeValues
    UpdateExpression: updateExpression

  addAwsParams(awsParams, options)

  @parent.dynamo.send(new UpdateItemCommand(awsParams))
