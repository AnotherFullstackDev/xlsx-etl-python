import aws_cdk
from aws_cdk import (
    Stack,
    aws_lambda,
    aws_lambda_python_alpha,
    Duration,
    aws_s3,
    aws_lambda_event_sources,
    aws_glue,
    aws_iam,
    aws_sqs,
    aws_s3_notifications
)
from constructs import Construct


class XlsxEtlPipelineStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        raw_s3 = aws_s3.Bucket(
            self,
            "raw-xlsx-bucket",
            bucket_name=f'xlsx-raw-bucket-{aws_cdk.Aws.ACCOUNT_ID}'
        )

        # csv_s3_queue = aws_sqs.Queue(self, "csv-s3-queue", queue_name="csv-s3-queue")
        csv_s3 = aws_s3.Bucket(
            self,
            "csv-transformed-xlsx-bucket",
            bucket_name=f'csv-transformed-xlsx-bucket-{aws_cdk.Aws.ACCOUNT_ID}'
        )
        # csv_s3.add_event_notification(
        #     aws_s3.EventType.OBJECT_CREATED,
        #     aws_s3_notifications.SqsDestination(csv_s3_queue)
        # )

        lambda_role = aws_iam.Role(
            self,
            "lambda-role",
            role_name="xlsx-to-scv-transformer-role",
            assumed_by=aws_iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                aws_iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
                aws_iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess")
            ]
        )

        xlsx_transform_lambda = aws_lambda_python_alpha.PythonFunction(
            self,
            "xlsx-to-scv",
            entry="./xlsx_to_csv_lambda",
            runtime=aws_lambda.Runtime.PYTHON_3_9,
            index="main.py",
            handler="handler",
            timeout=Duration.minutes(15),
            environment={
                "TARGET_BUCKET": csv_s3.bucket_name,
                "TARGET_SHEET": "Sheet1"
            },
            events=[
                aws_lambda_event_sources.S3EventSource(
                    raw_s3,
                    events=[aws_s3.EventType.OBJECT_CREATED]
                )
            ],
            role=lambda_role
        )

        glue_role = aws_iam.Role(
            self,
            "csv-crawler-role",
            role_name="csv-crawler-role",
            assumed_by=aws_iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                aws_iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
            ],
            inline_policies={
                "glue_policy": aws_iam.PolicyDocument(
                    statements=[
                        aws_iam.PolicyStatement(
                            effect=aws_iam.Effect.ALLOW,
                            actions=["s3:*"],
                            resources=[f'arn:aws:s3:::{csv_s3.bucket_name}*']
                        ),
                        # aws_iam.PolicyStatement(
                        #     effect=aws_iam.Effect.ALLOW,
                        #     actions=["sqs:*"],
                        #     resources=[csv_s3_queue.queue_arn]
                        # )
                    ]
                )
            }
        )

        csv_s3_glue_database = aws_glue.CfnDatabase(
            self,
            "csv-transformed-xlsx-database",
            catalog_id=aws_cdk.Aws.ACCOUNT_ID,
            database_input=aws_glue.CfnDatabase.DatabaseInputProperty(
                name="csv-transformed-xlsx-database"
            )
        )

        csv_s3_glue_crawler = aws_glue.CfnCrawler(
            self,
            "csv-transformed-xlsx-parser",
            name="csv-transformed-xlsx-parser",
            database_name=csv_s3_glue_database.database_input.name,
            targets=aws_glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    aws_glue.CfnCrawler.S3TargetProperty(
                        # path=f's3://{csv_s3.bucket_name}/',
                        path=csv_s3.bucket_name,
                        # event_queue_arn=csv_s3_queue.queue_arn
                    )
                ]
            ),
            role=glue_role.role_arn
        )

        start_crawler_lambda_role = aws_iam.Role(
            self,
            "start-crawler-lambda-role",
            role_name="start-crawler-lambda-role",
            assumed_by=aws_iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                aws_iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
            ],
            inline_policies={
                "start_crawler_policy": aws_iam.PolicyDocument(
                    statements=[
                        aws_iam.PolicyStatement(
                            effect=aws_iam.Effect.ALLOW,
                            actions=["*"],
                            resources=["*"]
                        ),
                    ]
                )
            }
        )

        crawler_start_lambda = aws_lambda_python_alpha.PythonFunction(
            self,
            "crawler-start",
            entry="./start_crawler_lambda",
            runtime=aws_lambda.Runtime.PYTHON_3_9,
            index="main.py",
            handler="handler",
            timeout=Duration.minutes(1),
            environment={
                "TARGET_CRAWLER": csv_s3_glue_crawler.name
            },
            role=start_crawler_lambda_role,
            events=[
                aws_lambda_event_sources.S3EventSource(
                    csv_s3,
                    events=[aws_s3.EventType.OBJECT_CREATED]
                )
            ],
        )
