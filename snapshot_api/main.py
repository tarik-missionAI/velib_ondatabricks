import json, boto3, datetime, os, base64
import urllib3

# Define AWS S3 client
s3_client = boto3.client('s3')

#initialise from environment variable
api_url = os.environ["api_url"]
print('this is: ', os.environ["bucket_name"])
print('the uri is: ', api_url)

admin_url =  api_url+ '/system_information.json'
api_content_url = api_url+'/gbfs.json' 
station_list_url =api_url+'/station_information.json'
station_status_url = api_url+'/station_status.json'

def lambda_handler(event, context):
    # steps to capturing data
    # 0. checking event that triggered this function
    # setup default test bucket 
    bucket_name = os.environ["bucket_name"]
    print('raw event; ', event)
    try:
        event_message = event["environment"]
        print('snapshot velib API based on {}'.format(event_message))
        if 'prod' in event_message:
            bucket_name = os.environ["bucket_name_prod"]
    except:
        print('pipeline triggered outside Lambda')
    # 1. get a snapshot of status and list
    print('the bucket being used is: {}'.format(bucket_name))
    try :
        http = urllib3.PoolManager()
        # 1.1 GET request
        station_list = http.request('GET', station_list_url)
        station_status = http.request('GET', station_status_url)
        # 1.2 Manage error upon GET request exit with error (Lambda set to 1 retry)
        if (station_list.status != 200) or (station_status.status != 200):
            error = {'station_list' : station_list.status, 'station_status' : station_status.status}
            print('could not reach server', error)
            raise Exception(error)
    except urllib3.exceptions.HTTPError  as e:
        raise Exception(e)
    # 2. upload list (if required) and status to S3
    # 2.1 define file names (both file even if list is not recorded) 
    current_time = datetime.datetime.now(datetime.timezone.utc)
    current_time_string = current_time.strftime('%Y-%m-%d_%Hh%Mm%Ss')
    status_filename = "landing_zone/station_status/"+current_time_string[0:10]+"/station_status_"+current_time_string+".json"
    list_filename = "landing_zone/station_list/"+current_time_string[0:10]+"/station_list_"+current_time_string+".json"
    #2.1.1 add timestamp into the status file
    station_status_json = json.loads(station_status.data)
    station_status_json['snapshot_timestamp'] = int(current_time.timestamp())
    list_station_json = json.loads(station_list.data)
    list_station_json['snapshot_timestamp'] = int(current_time.timestamp())
    #2.2 upload files to bucket
    s3_client.put_object(Bucket=bucket_name, Key=status_filename, Body=json.dumps(station_status_json, ensure_ascii=False), ContentType='application/json')
    s3_client.put_object(Bucket=bucket_name, Key=list_filename, Body=json.dumps(list_station_json, ensure_ascii=False), ContentType='application/json')
    return  {
        'statusCode': 200,
        'body': json.dumps('written files to: ' + bucket_name + ' with event: ' + json.dumps(event))
    }