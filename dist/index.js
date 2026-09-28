"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
const client_sqs_1 = require("@aws-sdk/client-sqs");
const client_ecs_1 = require("@aws-sdk/client-ecs");
const AWS_ACCESS_KEY_ID = process.env.AWS_ACCESS_KEY_ID;
const AWS_SECRET_ACCESS_KEY = process.env.AWS_SECRET_ACCESS_KEY;
if (!AWS_ACCESS_KEY_ID || !AWS_SECRET_ACCESS_KEY) {
    throw new Error("AWS credentials are missing");
}
const client = new client_sqs_1.SQSClient({
    region: "us-east-1",
    credentials: {
        accessKeyId: AWS_ACCESS_KEY_ID,
        secretAccessKey: AWS_SECRET_ACCESS_KEY,
    },
});
const ecsClient = new client_ecs_1.ECSClient({
    region: "us-east-1",
    credentials: {
        accessKeyId: AWS_ACCESS_KEY_ID,
        secretAccessKey: AWS_SECRET_ACCESS_KEY,
    },
});
function init() {
    return __awaiter(this, void 0, void 0, function* () {
        const command = new client_sqs_1.ReceiveMessageCommand({
            QueueUrl: "https://sqs.us-east-1.amazonaws.com/782221581483/TanscodingQueue",
            MaxNumberOfMessages: 1,
        });
        while (true) {
            try {
                const { Messages } = yield client.send(command);
                if (!Messages) {
                    console.log(`No message in Queue`);
                    continue;
                }
                for (const message of Messages) {
                    const { MessageId, Body } = message;
                    console.log(`MessageId: ${MessageId}, Body: ${Body}`);
                    if (!Body)
                        continue;
                    const event = JSON.parse(Body);
                    if ("Service" in event && "Event" in event) {
                        if (event.Event === "s3:TestEvent") {
                            yield client.send(new client_sqs_1.DeleteMessageCommand({
                                QueueUrl: "https://sqs.us-east-1.amazonaws.com/782221581483/TanscodingQueue",
                                ReceiptHandle: message.ReceiptHandle,
                            }));
                            continue;
                        }
                    }
                    for (const record of event.Records) {
                        const { s3 } = record;
                        const bucket = s3.bucket.name;
                        const key = decodeURIComponent(s3.object.key.replace(/\+/g, " "));
                        console.log("Bucket:", bucket);
                        console.log("Key:", key);
                        const runTaskCommand = new client_ecs_1.RunTaskCommand({
                            taskDefinition: "arn:aws:ecs:us-east-1:782221581483:task-definition/video-transcoder",
                            cluster: "arn:aws:ecs:us-east-1:782221581483:cluster/dev",
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
                        yield ecsClient.send(runTaskCommand);
                    }
                    yield client.send(new client_sqs_1.DeleteMessageCommand({
                        QueueUrl: "https://sqs.us-east-1.amazonaws.com/782221581483/TanscodingQueue",
                        ReceiptHandle: message.ReceiptHandle,
                    }));
                }
            }
            catch (error) {
                console.log("Error receiving messages ", error);
            }
        }
    });
}
init();
//
