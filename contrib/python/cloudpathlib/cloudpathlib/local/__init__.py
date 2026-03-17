"""This module implements "Local" classes that mimic their associated `cloudpathlib` non-local
counterparts but use the local filesystem in place of cloud storage. They can be used as drop-in
replacements, with the intent that you can use them as mock or monkepatch substitutes in your
tests. See ["Testing code that uses cloudpathlib"](../../testing_mocked_cloudpathlib/) for usage
examples.
"""

from .implementations import (
    local_azure_blob_implementation,
    LocalAzureBlobClient,
    LocalAzureBlobPath,
    local_gs_implementation,
    LocalGSClient,
    LocalGSPath,
    local_s3_implementation,
    LocalS3Client,
    LocalS3Path,
)
from .localclient import LocalClient
from .localpath import LocalPath

__all__ = [
    "local_azure_blob_implementation",
    "LocalAzureBlobClient",
    "LocalAzureBlobPath",
    "LocalClient",
    "local_gs_implementation",
    "LocalGSClient",
    "LocalGSPath",
    "LocalPath",
    "local_s3_implementation",
    "LocalS3Client",
    "LocalS3Path",
]
