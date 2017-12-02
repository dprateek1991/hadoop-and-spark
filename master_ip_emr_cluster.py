#####################################################################################
# Script Name   : master_ip_emr_cluster.py
# Author        : Prateek Dubey
# Purpose       : Script to retrieve Master Node IP address
#####################################################################################

import boto3
import argparse
import sys

parser = argparse.ArgumentParser(description='Check if Cluster ID is inputted')
parser.add_argument('CLUSTER_ID', type=str, help='Enter a Cluster ID for which IP address needs to be retrieved')
args = parser.parse_args()

cluster_id = sys.argv[1]

client = boto3.client('emr',region_name= 'us-east-1',aws_access_key_id='xxxxxxxx',aws_secret_access_key='xxxxxxxx')
response = client.list_instances(ClusterId=cluster_id,InstanceGroupTypes=['MASTER'])
master_private_ip = response['Instances'][0]['PrivateIpAddress']
print("Cluster ID: {0}, Master IP Address: {1}".format(cluster_id,master_private_ip))


