boto3 = require('boto3');
S3 = boto3.client('s3');
const s3 = new S3({ apiVersion: '2006-03-01' });

const JS_code = `
  let x = 'Hello World';
`;

// I'm using the following method inside an async function.
s3.putObject({
        Bucket: 'aws-glue-test-ml-yatzy',
        Key: 'myFileupls.js',
        ContentType:'binary',
        Body: Buffer.from(JS_code, 'binary')
      });