import {
  DeleteMessageCommand,
  ReceiveMessageCommand,
  SQSClient,
} from "@aws-sdk/client-sqs";
import { ECSClient, RunTaskCommand } from "@aws-sdk/client-ecs";
import type { S3Event } from "aws-lambda";

const AWS_ACCESS_KEY_ID = process.env.AWS_ACCESS_KEY_ID;
const AWS_SECRET_ACCESS_KEY = process.env.AWS_SECRET_ACCESS_KEY;

if (!AWS_ACCESS_KEY_ID || !AWS_SECRET_ACCESS_KEY) {
  throw new Error("AWS credentials are missing");
}

const client = new SQSClient({
  region: "us-east-1",
  credentials: {
    accessKeyId: AWS_ACCESS_KEY_ID,
    secretAccessKey: AWS_SECRET_ACCESS_KEY,
  },
});

const ecsClient = new ECSClient({
  region: "us-east-1",
  credentials: {
    accessKeyId: AWS_ACCESS_KEY_ID,
    secretAccessKey: AWS_SECRET_ACCESS_KEY,
  },
});



async function init() {
  const command = new ReceiveMessageCommand({
    QueueUrl:
      "https://sqs.us-east-1.amazonaws.com/782221581483/TanscodingQueue",
    MaxNumberOfMessages: 1,
  });

  while (true) {
    try {
      const { Messages } = await client.send(command);
      if (!Messages) {
        console.log(`No message in Queue`);
        continue;
      }

      for (const message of Messages) {
        const { MessageId, Body } = message;
        console.log(`MessageId: ${MessageId}, Body: ${Body}`);

        if (!Body) continue;
        const event = JSON.parse(Body) as S3Event;

        if ("Service" in event && "Event" in event) {
          if (event.Event === "s3:TestEvent") {
            await client.send(
              new DeleteMessageCommand({
                QueueUrl:
                  "https://sqs.us-east-1.amazonaws.com/782221581483/TanscodingQueue",
                ReceiptHandle: message.ReceiptHandle!,
              }),
            );
            continue;
          }
        }

for (const record of event.Records) {
  const { s3 } = record;

  const bucket = s3.bucket.name;
  const key = decodeURIComponent(
    s3.object.key.replace(/\+/g, " ")
  );

  console.log("Bucket:", bucket);
  console.log("Key:", key);

  const runTaskCommand = new RunTaskCommand({
    taskDefinition:
      "arn:aws:ecs:us-east-1:782221581483:task-definition/video-transcoder",

    cluster:
      "arn:aws:ecs:us-east-1:782221581483:cluster/dev",

    launchType: "FARGATE",

    networkConfiguration: {
      awsvpcConfiguration: {
        securityGroups: [
          "sg-052968aa99f5230f8",
        ],

        assignPublicIp: "ENABLED",

        subnets: [
          "subnet-0a57aed87029fe001",
          "subnet-0f4eabc0a94328a71",
          "subnet-030499bc8e88b34ca",
        ],
      },
    },

    overrides: {
      containerOverrides: [
        {
          name: "video-tanscoder",

          environment: [
            {
              name: "BUCKET_NAME",
              value: bucket,
            },
            {
              name: "KEY",
              value: key,
            },
          ],
        },
      ],
    },
  });

  await ecsClient.send(runTaskCommand);
}
        await client.send(
          new DeleteMessageCommand({
            QueueUrl:
              "https://sqs.us-east-1.amazonaws.com/782221581483/TanscodingQueue",
            ReceiptHandle: message.ReceiptHandle!,
          }),
        );
      }
    } catch (error) {
      console.log("Error receiving messages ", error);
    }
  }
}
init();
//
