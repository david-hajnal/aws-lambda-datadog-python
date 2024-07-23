import json
import os
import sys
import datetime
import time
from datadog import initialize, api

def lambda_handler(event, context):

    status, env, exec_mode, region = process_event(event)
    send_metric(region.lower(), status.lower(), env.lower(), exec_mode.lower())
    
    return {
        'statusCode': 200
    }

def send_metric(region, status, environment, exec_mode):
    api_key = os.environ.get('DATADOG_API_KEY')
    app_key = os.environ.get('DATADOG_APP_KEY')
    
    if not api_key:
        print("Error: DATADOG_API_KEY environment variable is not set")
        sys.exit(1)

    options = {
        "api_key": api_key,
        "app_key": app_key,
        "DD_DOGSTATSD_DISABLE": True
    }

    initialize(**options)
    
    tags = [
        f"region:{region}",
        f"status:{status}",
        f"environment:{environment}",
        "application:impact-lti-usage"
    ]

    metric = f"job.dbt.{exec_mode}"

    now = datetime.datetime.now()
    now_ts = int(time.mktime(now.timetuple()))
    api.Metric.send(metric=metric, type="rate", tags=tags, points=[(now_ts, 1)])
    
def process_event(event):
    try:
        sns_message = event['Records'][0]['Sns']
        message_content = eval(sns_message['Message'])
        status = message_content["Status"]
        env = message_content["Environment"]
        exec_mode = message_content["ExecutionMode"]
        region = message_content["Region"]
        
        return status, env, exec_mode, region
    except (KeyError, IndexError, SyntaxError) as e:
        print(f"Error processing event: {e}")
        return None, None, None, None
