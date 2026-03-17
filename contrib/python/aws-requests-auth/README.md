[![Build Status](https://travis-ci.org/DavidMuller/aws-requests-auth.svg?branch=master)](https://travis-ci.org/DavidMuller/aws-requests-auth)

# AWS Signature Version 4 Signing Process with python requests

This package allows you to authenticate to AWS with Amazon's [signature version 4 signing process](https://docs.aws.amazon.com/general/latest/gr/signature-version-4.html) with the python [requests](https://requests.readthedocs.io/en/master/) library.

Tested with both python `2.7` and `3`.

(Conceivably, the authentication class is flexible enough to be used with any AWS service, but it was initially created to interface with AWS Elasticsearch instances.)

# Installation

```
pip install aws-requests-auth
```

# Usage

```python
import requests
from aws_requests_auth.aws_auth import AWSRequestsAuth

# let's talk to our AWS Elasticsearch cluster
auth = AWSRequestsAuth(aws_access_key='YOURKEY',
                       aws_secret_access_key='YOURSECRET',
                       aws_host='search-service-foobar.us-east-1.es.amazonaws.com',
                       aws_region='us-east-1',
                       aws_service='es')

response = requests.get('http://search-service-foobar.us-east-1.es.amazonaws.com',
                        auth=auth)
print response.content

{
  "status" : 200,
  "name" : "Stevie Hunter",
  "cluster_name" : "elasticsearch",
  "version" : {
    "number" : "1.5.2",
    etc....
  },
  "tagline" : "You Know, for Search"
}
```

## elasticsearch-py Client Usage Example

It's possible to inject the `AWSRequestsAuth` class directly into the [elasticsearch-py](https://elasticsearch-py.readthedocs.org/en/master/) library so you can talk to your Amazon AWS cluster directly through the elasticsearch-py client.

```python
from aws_requests_auth.aws_auth import AWSRequestsAuth
from elasticsearch import Elasticsearch, RequestsHttpConnection

es_host = 'search-service-foobar.us-east-1.es.amazonaws.com'
auth = AWSRequestsAuth(aws_access_key='YOURKEY',
                       aws_secret_access_key='YOURSECRET',
                       aws_host=es_host,
                       aws_region='us-east-1',
                       aws_service='es')

# use the requests connection_class and pass in our custom auth class
es_client = Elasticsearch(host=es_host,
                          port=80,
                          connection_class=RequestsHttpConnection,
                          http_auth=auth)
print es_client.info()
```

## Temporary Security Credentials
If you are using [AWS STS](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp.html) to grant temporary access to your Elasticsearch resource, you can use the `aws_token` keyword argument to include your credentials in `AWSRequestsAuth`.  See [issue #9](https://github.com/DavidMuller/aws-requests-auth/issues/9) and [PR #11](https://github.com/DavidMuller/aws-requests-auth/pull/11) for additional details.

## AWS Lambda Quickstart Example
If you are using an AWS lamba to talk to your Elasticsearch cluster and you've assigned an IAM role to your lambda function that allows the lambda to communicate with your Elasticserach cluster, you can instantiate an instance of AWSRequestsAuth by reading your credentials from environment variables:
```python
import os
from aws_requests_auth.aws_auth import AWSRequestsAuth

def lambda_handler(event, context):
    auth = AWSRequestsAuth(aws_access_key=os.environ['AWS_ACCESS_KEY_ID'],
                           aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
                           aws_token=os.environ['AWS_SESSION_TOKEN'],
                           aws_host='search-service-foobar.us-east-1.es.amazonaws.com',
                           aws_region='us-east-1',
                           aws_service='es')
    print 'My lambda finished executing'                           
```
`'AWS_ACCESS_KEY_ID'`, `'AWS_SECRET_ACCESS_KEY'`, `'AWS_SESSION_TOKEN'` are [reserved environment variables in AWS lambdas](https://docs.aws.amazon.com/lambda/latest/dg/current-supported-versions.html#lambda-environment-variables).

## Using Boto To Automatically Gather AWS Credentials
`botocore` (the core functionality of `boto3`) is not a strict requirement of `aws-requests-auth`, but we do provide some convenience methods if you'd like to use `botocore` to automatically retrieve your AWS credentials for you.

`botocore` [can dynamically pull AWS credentials from environment variables, AWS config files, IAM Role,
and other locations](http://boto3.readthedocs.io/en/latest/guide/configuration.html#configuring-credentials). Dynamic credential fetching can come in handy if you need to run a program leveraging `aws-requests-auth` in several places where you may authenticate in different manners. For example, you may rely on a `.aws/credentials` file when running on your local machine, but use an IAM role when running your program in a docker container in the cloud.

To take advantage of these conveniences, and help you authenticate wherever `botocore` finds AWS credentials, you can import the `boto_utils` file and initialize `BotoAWSRequestsAuth` as follows:

```python
# note that this line will fail if you do not have botocore installed
# botocore installation instructions available here:
# https://boto3.readthedocs.io/en/latest/guide/quickstart.html#installation
from aws_requests_auth.boto_utils import BotoAWSRequestsAuth

auth = BotoAWSRequestsAuth(aws_host='search-service-foobar.us-east-1.es.amazonaws.com',
                           aws_region='us-east-1',
                           aws_service='es')
```

Credentials are only accessed when needed at runtime, and they will be refreshed using the underlying methods in `botocore` if needed.


## AWS API Gateway example with IAM authentication and Boto automatic credentials

If you are using AWS API Gateway with IAM authentication
([ref](https://aws.amazon.com/premiumsupport/knowledge-center/iam-authentication-api-gateway/)),
here's how to sign an HTTP request using automatic AWS credentials with boto

```python
from aws_requests_auth.boto_utils import BotoAWSRequestsAuth
auth = BotoAWSRequestsAuth(aws_host='api.example.com',
                           aws_region='us-east-1',
                           aws_service='execute-api')

import requests
response = requests.post('https://api.example.com/test', json={"foo": "bar"}, auth=auth)
```
