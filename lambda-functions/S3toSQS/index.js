var QUEUE_URL = 'https://sqs.us-east-2.amazonaws.com/791353298173/teste-davibr';
const aws = require('aws-sdk');
const s3 = new aws.S3({ apiVersion: '2006-03-01' });
const readline = require('readline');
var sqs = new aws.SQS({region : 'us-east-2'});

exports.handler = async (event, context, callback) => {
    let promises = [];
    // Get bucket name from event
    const bucket = event.Records[0].s3.bucket.name;
    // Get key/file name also from event
    const key = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, ' '));
    // From these two, create s3 params
    const params = {
        Bucket: bucket,
        Key: key,
    };
    //get the file as stream
    const s3ReadStream = s3.getObject(params).createReadStream();

    //create readline interface with the stream
    const rl = readline.createInterface({
      input: s3ReadStream,
      terminal: false
    });
    
    //create an array of promises based on the lines read
    promises.push(new Promise((resolve, reject) => {

        rl.on('line', (line) => {
            console.log(`Line from file: ${line}`);
            console.log("Sending line to SQS");
            //create params to send message to sqs
            var sqsParams = {
                MessageBody: (new Date().getTime()).toString() + ";" + line,
                QueueUrl: QUEUE_URL
            };
            //sending message to sqs
            sqs.sendMessage(sqsParams, function(err,data){
                if(err) {
                    console.log('error:',"Fail Send Message" + err);
                }
                else {
                    console.log('data:',data.MessageId);
                }
            });
        });
        rl.on('error', () => {
            console.log('error');
        });
        rl.on('close', function () {
            console.log('closed');
            resolve();
        });
    }));

    // Resolve all promises
    await Promise.all(promises);
    console.log('done');
    callback(null, {
        statusCode: 200,
        body: JSON.stringify("File processed"),
    });
};