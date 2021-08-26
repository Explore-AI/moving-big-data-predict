"""
    Moving Big Data Predict: 
    Event-based lambda for data pipeline automation

    © Explore Data Science Academy

"""

# Imports
import json
import boto3
import awscli.customizations.datapipeline.translator as trans

print('Loading function')

# Initialise S3 & Data Pipeline Clients
s3_client = boto3.client('s3')
data_pipeline_client = boto3.client('datapipeline')

# AWS Service Names
# ---- UPDATE DETAILS ----
# The following lines should be updated according to the naming convention used during your predict implementation.
bucket = "de-load-predict-dora-explorer-s3-source"
target_data_pipeline = "Load-Predict-Data-Pipeline"
# ------------------------
unique_pipeline_id = "dp-001"

def lambda_handler(event, context):

    # Checking for old pipeline clones & deleting
    available_pipelines = data_pipeline_client.list_pipelines()["pipelineIdList"]

    index = 0
    delete_index = 0
    delete_pipeline = False

    try: 
        for pipeline in available_pipelines:
            if pipeline["name"] == target_data_pipeline:
                delete_index = index
                delete_pipeline = True
            else:
                pass
            index += 1

        target_data_pipeline_delete = available_pipelines[delete_index]["id"]

        if delete_pipeline:
            data_pipeline_client.delete_pipeline(pipelineId=target_data_pipeline_delete)
            create = data_pipeline_client.create_pipeline(name=target_data_pipeline, uniqueId=unique_pipeline_id)
            print(f"Pipeline {target_data_pipeline} deleted. Empty pipeline definition created '{create['pipelineId']}'")
        else:
            create = data_pipeline_client.create_pipeline(name=target_data_pipeline, uniqueId=unique_pipeline_id)
            print(f"Empty pipeline definition created {create}")
    except:
        create = data_pipeline_client.create_pipeline(name=target_data_pipeline, uniqueId=unique_pipeline_id)
        print(f"Empty pipeline definition created {create}")

    # Get JSON Data Pipeline definition
    s3 = boto3.resource('s3')
    object = s3.Object(bucket,'completed_model_pipeline.json')
    file_content = object.get()['Body'].read().decode('utf-8')
    pipeline_definition = json.loads(file_content)
    
    print (f"Pipeline definition: {pipeline_definition}")
    print (f"Pipeline ID: {create['pipelineId']}")
    

    # Transforming JSON to format required by put_pipeline_definition()
    pipelineObjects = trans.definition_to_api_objects(pipeline_definition)
    
    print ()

    # Create final AWS Data Pipeline
    response = data_pipeline_client.put_pipeline_definition(
        pipelineId=create['pipelineId'],
        pipelineObjects=pipelineObjects
    )
    
    activate = data_pipeline_client.activate_pipeline(pipelineId=create['pipelineId'])
    print("Pipeline activated")
