"use strict"

const AWS = require('aws-sdk')

let dynamodb = null

const table = 'PROGRESS';

const Progress = {
  TableName : "PROGRESS",
  KeySchema: [       
    { AttributeName: "uid", KeyType: "HASH" },
    { AttributeName: "courseId", KeyType: "RANGE" }
  ],
  AttributeDefinitions: [       
    { AttributeName: "uid", AttributeType: "S" },
    { AttributeName: "courseId", AttributeType: "S" }
  ],
  ProvisionedThroughput: {       
      ReadCapacityUnits: 1, 
      WriteCapacityUnits: 1
  }
}

const db = {
  _ready: false,

  createTable(done) {
    if (!this._ready) {
      console.error("DynamoDB is not ready yet")
      return this;
    }

    dynamodb.createTable(Progress, function(err, data) {
      if (err) {
        done && done(err);
      } else {
        done && done();
      }
    });

    return this;
  },

  dropTable(done) {
    if (!this._ready) {
      console.error("DynamoDB is not ready yet")
      return this;
    }
    dynamodb.deleteTable({ TableName: table }, done)
  },

  getProgress({uid, courseId}, done) {
    if (!uid) {
      done && done({error: 'must specify uid'}, null)
      return
    }

    if (!courseId) {
      done && done({error: 'must specify courseId'}, null)
      return
    }
    
    const params = { 
      TableName: table, 
      Key: {
        "uid": uid,
        "courseId": courseId
      }
    }
    const docClient = new AWS.DynamoDB.DocumentClient();
    docClient.get(params, function(err, data) {
      if (err) {
        done && done({ error:`Unable to read item: ${JSON.stringify(err, null, 2)}`}, null);
      } else {
        if (data && data.Item) {
          done && done(null, data.Item);
        } else {
          done && done(null, null);
        }
      }
    });

  },

  getAllProgress({uid}, done) {
    if (!uid) {
      done && done({error: 'must specify uid'}, null)
      return
    }
    const params = { 
      TableName: table, 
      KeyConditionExpression : 'uid = :id',
      ExpressionAttributeValues: {
        ':id': uid
      }
    }
    const docClient = new AWS.DynamoDB.DocumentClient();
    docClient.query(params, function(err, data) {
      if (err) {
        done && done({ error:`Unable to read item: ${JSON.stringify(err, null, 2)}`}, null);
      } else {
        if (data && data.Items) {
          done && done(null, data.Items);
        } else {
          done && done(null, null);
        }
      }
    });
  },

  updateProgress( progress, done) {
    if (!progress) {
      done && done(null, null)
      return
    }
    
    const params = {
      TableName: table,
      Item: progress
    };
    
    const docClient = new AWS.DynamoDB.DocumentClient();
    docClient.put(params, (err, data) => {
      if (err) {
        done && done(err);
      } else {
        done && done();
      }
    });
  },

  removeProgress({uid, courseId}, done) {

  },

}

function DynamoDB(onReady) {
 
  dynamodb = new AWS.DynamoDB();

  if (onReady) {
    dynamodb.listTables(function (err, data) {
      if (err) {
        console.log("Error when checking DynamoDB status")
        db._ready = false;
        onReady(err, null);
      } else {
        db._ready = true;
        onReady(null, data);
      }
    });
  } else {
    db._ready = true;
  }

  return db;

}

module.exports = DynamoDB;

