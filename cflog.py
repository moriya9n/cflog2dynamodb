import gzip
import boto3
import uuid
import tempfile

client = boto3.client('dynamodb')
TABLE_NAME = 'cflog'

def create_log_table():
    table = client.create_table(
        BillingMode = 'PAY_PER_REQUEST',
        TableName = TABLE_NAME,
        KeySchema = [
            {
                'AttributeName': 'LogID',
                'KeyType': 'HASH',
            }
        ],
        AttributeDefinitions = [
            {
                'AttributeName': 'LogID',
                'AttributeType': 'S',
            },
        ],
    )

    table.wait_until_exists()

# reverse uuid1 components
def gen_id():
    return ''.join(str(uuid.uuid1()).split('-')[::-1])

def register_log(items):
    id = gen_id()
    print(id)

    resp = client.execute_statement(
        Statement = "INSERT INTO " + TABLE_NAME + " value {'LogID': ?, 'datetime': ?, 'method': ?, 'status': ?, 'uri_stem': ?, 'referer': ?, 'user_agent': ?, 'sc_bytes': ?, 'time_taken': ?}",
        Parameters = [
                {'S': id},
                {'S': ' '.join([items[0], items[1]])},
                {'S': items[5]},
                {'N': items[8]},
                {'S': items[7]},
                {'S': items[9]},
                {'S': items[10]},
                {'N': items[3]},
                {'N': items[18]},
        ],
    )
    print(resp)

def cflog2d(log_file):
    # create table if not exists
    try:
        cflog_table = client.describe_table(TableName=TABLE_NAME)
    except client.exceptions.ResourceNotFoundException as e:
        create_log_table()
    
    with gzip.open(log_file, mode='rt') as f:
        for l in f:
            if l.startswith('#'):
                continue
            items = l.split('\t')
            register_log(items)

def s3cflog2d(bucket, object_key):
    s3 = boto3.client('s3')
    with tempfile.TemporaryFile() as tf:
        s3.download_fileobj(bucket, object_key, tf)
        tf.seek(0)
        cflog2d(tf)

if __name__ == "__main__":
    import sys
#    for log_file in sys.argv[1:]:
#        cflog2d(log_file)
    bucket = sys.argv[1]
    object_key = sys.argv[2]
    s3cflog2d(bucket, object_key)
    
