import aws_cdk as core
import aws_cdk.assertions as assertions

from xlsx_etl_pipeline.xlsx_etl_pipeline_stack import XlsxEtlPipelineStack

# example tests. To run these tests, uncomment this file along with the example
# resource in xlsx_etl_pipeline/xlsx_etl_pipeline_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = XlsxEtlPipelineStack(app, "xlsx-etl-pipeline")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
