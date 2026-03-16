"""
Functions in this file are included as a convenience for working with AWSRequestsAuth.
External libraries, like boto, that this file imports are not a strict requirement for the
aws-requests-auth package.
"""

from botocore.session import Session

from .aws_auth import AWSRequestsAuth


def get_credentials(credentials_obj=None):
    """
    Interacts with boto to retrieve AWS credentials, and returns a dictionary of
    kwargs to be used in AWSRequestsAuth. boto automatically pulls AWS credentials from
    a variety of sources including but not limited to credentials files and IAM role.
    AWS credentials are pulled in the order listed here:
    http://boto3.readthedocs.io/en/latest/guide/configuration.html#configuring-credentials
    """
    if credentials_obj is None:
        credentials_obj = Session().get_credentials()
    # use get_frozen_credentials to avoid the race condition where one or more
    # properties may be refreshed and the other(s) not refreshed
    frozen_credentials = credentials_obj.get_frozen_credentials()
    return {
        'aws_access_key': frozen_credentials.access_key,
        'aws_secret_access_key': frozen_credentials.secret_key,
        'aws_token': frozen_credentials.token,
    }


class BotoAWSRequestsAuth(AWSRequestsAuth):

    def __init__(self, aws_host, aws_region, aws_service):
        """
        Example usage for talking to an AWS Elasticsearch Service:

        BotoAWSRequestsAuth(aws_host='search-service-foobar.us-east-1.es.amazonaws.com',
                            aws_region='us-east-1',
                            aws_service='es')

        The aws_access_key, aws_secret_access_key, and aws_token are discovered
        automatically from the environment, in the order described here:
        http://boto3.readthedocs.io/en/latest/guide/configuration.html#configuring-credentials
        """
        super(BotoAWSRequestsAuth, self).__init__(None, None, aws_host, aws_region, aws_service)
        self._refreshable_credentials = Session().get_credentials()

    def get_aws_request_headers_handler(self, r):
        # provide credentials explicitly during each __call__, to take advantage
        # of botocore's underlying logic to refresh expired credentials
        credentials = get_credentials(self._refreshable_credentials)
        return self.get_aws_request_headers(r, **credentials)
