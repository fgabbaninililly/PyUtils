import boto3
import time
import pandas as pd
import logging
from IPython.display import clear_output
s3 = boto3.client('s3')

def infer_compression_by_filename(filename):
    compression_type = 'NONE'
    if filename.endswith('.tar.gz'):
        compression_type = 'GZIP'
    return compression_type
    
def execute_s3_select(bucket, key, expression, quote_character):
    compression_type = infer_compression_by_filename(key)
    r = s3.select_object_content(
            Bucket=bucket,
            Key=key,
            ExpressionType='SQL',
            Expression=expression,
            InputSerialization = {'CSV': {"FileHeaderInfo": "Use", "AllowQuotedRecordDelimiter": True, "QuoteCharacter": quote_character}, 'CompressionType': compression_type},
            OutputSerialization = {'CSV': {}},
    )
    event_stream = r['Payload']
    end_event_received = False
    records = ""
    logging.info("Appending rows...")
    cnt = 0
    for event in event_stream:
        cnt = cnt + 1
        if 'Records' in event:
            if cnt%1000 == 0:
                logging.info("...read 1000 blocks...{}".format(cnt))
                                
            records = records + event['Records']['Payload'].decode('utf-8')
            
        if 'End' in event:
            end_event_received = True
    
    if not end_event_received:
        raise Exception("End event not received, request incomplete.")
    
    return records.rstrip(), records.count("\n")

def s3_select_to_df(bucket, key, expression, headers, quote_character, separator = ','):
    logging.info('Quote delimiter set to: [{}]'.format(quote_character))
    start = time.time()
    r, cnt = execute_s3_select(bucket, key, expression, quote_character)
    end = time.time()
    logging.info('Read {0} rows in {1:.2f} seconds'.format(str(cnt), end - start))
    r = headers + "\n" + r
    return pd.read_csv(pd.compat.StringIO(r), sep=separator)

def s3_read_alarms(bucket, alms_esign_key, alms_noesign_key, qry, tbl_flds, quote_character, separator = ","):
    df_esign = s3_select_to_df(bucket, alms_esign_key, qry, tbl_flds, quote_character, separator)
    df_noesign = s3_select_to_df(bucket, alms_noesign_key, qry, tbl_flds, quote_character, separator)
    
    df = pd.concat(list([df_esign, df_noesign]))
    df.sort_values(by=['TIMESTAMP_IN'], inplace=True)
    df.reset_index(drop = True, inplace = True)
    
    return df
