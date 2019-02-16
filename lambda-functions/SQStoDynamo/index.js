const aws = require('aws-sdk');
const dynamodb = new aws.DynamoDB({apiVersion: '2012-08-10'});

exports.handler = async (event, context, callback) => {
    let promises = [];
    var line = event.Records[0].body;
    var dataFromSqs = line.split(";");
    var item = {
        "timestamp": {
            "S": dataFromSqs[0]
        }
    };
    
    for (var i = 1; i < dataFromSqs.length; i++) {
        item["field" + i.toString()] = {"S": dataFromSqs[i]};
    }
    
    console.log(item);
    
    promises.push(new Promise((resolve, reject) => {
        dynamodb.putItem({
            TableName: "teste-davibr",
            Item: item
        }, function(err, data) {
            console.log(data);
            if (err) {
                console.log(err, err.stack);
                callback(null, {
                    statusCode: '500',
                    body: err
                });
            } else {
                console.log('Line ' + line + ' sent to DynamoDB!')
                resolve();
            }
        });
    }));
    
    // Resolve all promises
    await Promise.all(promises);
    console.log('done');
    callback(null, {
        statusCode: 200,
        body: JSON.stringify('Message processed!'),
    });
};
