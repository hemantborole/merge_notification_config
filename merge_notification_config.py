#!/usr/bin/env python3

""" This utility helps to add a lambda notification or sns topic notification
to existing S3 bucket. The regular put-bucket-notification-configuration api
overwrites any existing configurations. This utility helps to "APPEND" to an
existing configuration.

  description='Append s3 bucket notification to sns topic')
  parser.add_argument('-b', '--bucket', help='bucket name', required=True)
  parser.add_argument('-k', '--key', help='prefix/key', required=True)
  parser.add_argument('-t', '--topic-arn',
                            help='ARN of the topic to subscribe to')
  parser.add_argument('-l', '--lambda-arn',
                            help='ARN of the Lambda Function to subscribe to')
"""

import argparse
import boto3
import copy
import json
import sys, traceback
import uuid


def merge_config(arn, configuration_type, bucket, key, suffix):
  """ Generates and merges Topic of LambdaFunction configuration with existing
  S3 bucket configuration of the same type.

  Parameters
  ----------
  arn:                The ARN of SNS Topic or LambdaFunction that needs to be
                      notified on a S3 event.
  configuration_type: (Lambda|Topic)
  bucket:             The bucket on which this configuration is APPENDED.
  key:                Filter keys for notification within the bucket.
  suffix:             Filter keys for notification within the bucket.

  Returns
  -------
  dict: existing config to save just in case this code fails.
  """
  session = boto3.Session()
  s3 = session.resource('s3')
  notifications = s3.BucketNotification(bucket)

  if( configuration_type == "Lambda" ):
    cfg = notifications.lambda_function_configurations
    config_key = "LambdaFunctionConfigurations"
    notification_config = json_notification_config( 
                                  "LambdaFunctionArn",
                                  arn,
                                  key,
                                  suffix )
  if( configuration_type == "Topic" ):
    cfg = notifications.topic_configurations
    config_key = "TopicConfigurations"
    notification_config = json_notification_config( 
                                  "TopicArn",
                                  arn,
                                  key,
                                  suffix )

  if None == cfg:
    cfg = []
  print("Existing config. PLEASE SAVE THIS FOR ROLLBACK")
  print(cfg)
  status = "succeed"
  try:
    newcfg = copy.deepcopy(cfg)
    newcfg.append(notification_config)
    print("Appending new config:")
    print(newcfg)
    s3.BucketNotification(bucket).put(NotificationConfiguration={
        config_key: []
      })
    s3.BucketNotification(bucket).put(NotificationConfiguration={
        config_key: newcfg
      })
  except:
    status = "fail"
    print("Failed to merge " + configuration_type + " config. Rolling back.")
    try:
      s3.BucketNotification(bucket).put(NotificationConfiguration={
          config_key: cfg
        })
      traceback.print_stack()
    except:
      print("Fatal error. You may have lost your " + \
            configuration_type + " notification configuration")
      raise
  else:
    print("Config merged for " + configuration_type )
  finally:
    print( configuration_type + " merging done with status " + status)
  return cfg



def json_notification_config(arn_type, arn, prefix, suffix = ""):
  """ Creates a notification config JSON object for S3 Object creation event
  The config JSON object can be used for either sns topic or lambda function.

  Parameters
  ----------
  arn_type: (TopicArn|LambdaFunctionArn)
  arn:      The value of the TopicArn or LambdaFunctionArn
  prefix:   Prefix to which the configuration should be applied to
  suffix:   Suffix, object extension e.g jpg for keys in prefix

  Returns
  -------
  json: A JSON object representating the notification configuration.
  """
  uuid_id=uuid.uuid4()
  if( None == suffix ):
    suffix = ""
  if( None == prefix ):
    prefix = ""
  notification_config = {
            "Filter": {
                "Key": {
                    "FilterRules": [
                        {
                            "Name": "Suffix",
                            "Value": suffix
                        },
                        {
                            "Name": "Prefix",
                            "Value": prefix
                        }
                    ]
                }
            },
            "Id": str(uuid_id),
            arn_type: arn,
            "Events": [
                "s3:ObjectCreated:*"
            ]
        }
  return notification_config


def parse_arguments():
  parser = argparse.ArgumentParser(
                  description='Append s3 bucket notification to sns topic')
  parser.add_argument('-b', '--bucket', help='bucket name', required=True)
  parser.add_argument('-k', '--key', help='prefix/key', required=True)
  parser.add_argument('-s', '--suffix', help='suffix e.g. .jpg', required=False)
  parser.add_argument('-t', '--topic-arn',
                            help='ARN of the topic to subscribe to')
  parser.add_argument('-l', '--lambda-arn',
                            help='ARN of the Lambda Function to subscribe to')
  return parser

def main():
  parser = parse_arguments()
  if __name__ == '__main__':
    args = parser.parse_args()
    if( args.topic_arn == None and args.lambda_arn == None ):
      raise argparse.ArgumentError(None,
          "At least one of --lambda-arn (-l) or --topic-arn (-t) is required.")
    if( args.topic_arn != None ):
      arn = args.topic_arn
      cfg = merge_config( arn, "Topic", args.bucket, args.key, args.suffix)
    if( args.lambda_arn != None ):
      arn = args.lambda_arn
      cfg = merge_config( arn, "Lambda", args.bucket, args.key, args.suffix)


main()
