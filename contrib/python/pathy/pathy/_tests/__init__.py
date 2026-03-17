import os

gcs_testable: bool
gcs_installed: bool


try:
    from ..gcs import BucketClientGCS

    gcs_installed = bool(BucketClientGCS)
    gcs_testable = gcs_installed and "GCS_CREDENTIALS" in os.environ
except ImportError:
    gcs_installed = False
    gcs_testable = False

s3_testable: bool
s3_installed: bool
try:
    from ..s3 import BucketClientS3

    s3_installed = bool(BucketClientS3)
    s3_testable = s3_installed and "PATHY_S3_ACCESS_ID" in os.environ
except ImportError:
    s3_installed = False
    s3_testable = False


azure_testable: bool
azure_installed: bool


try:
    from ..azure import BucketClientAzure

    azure_installed = bool(BucketClientAzure)
    azure_testable = azure_installed and "PATHY_AZURE_CONNECTION_STRING" in os.environ
except ImportError:
    azure_installed = False
    azure_testable = False
