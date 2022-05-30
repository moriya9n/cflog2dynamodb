import cflog

def lambda_handler(event, context):
    for record in event['Records']:
        s3_info = record['s3']
        bucket_name = s3_info['bucket']['name']
        object_key = s3_info['object']['key']
        if object_key.endswith('/'):
            continue
        cflog.s3cflog2d(bucket_name, object_key)
        
    return {"status": "OK"}
