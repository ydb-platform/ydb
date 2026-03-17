"""Backwards compat shim."""

from storages.backends.s3 import S3File as S3Boto3StorageFile  # noqa
from storages.backends.s3 import S3ManifestStaticStorage  # noqa
from storages.backends.s3 import S3StaticStorage  # noqa
from storages.backends.s3 import S3Storage as S3Boto3Storage  # noqa
