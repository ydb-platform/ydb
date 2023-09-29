from __future__ import unicode_literals
import logging
# logging.getLogger('boto').setLevel(logging.CRITICAL)

__title__ = 'moto'
__version__ = '1.3.7'

from .ec2 import mock_ec2, mock_ec2_deprecated  # flake8: noqa
from .iam import mock_iam, mock_iam_deprecated  # flake8: noqa
from .kms import mock_kms, mock_kms_deprecated  # flake8: noqa
from .route53 import mock_route53, mock_route53_deprecated  # flake8: noqa
from .s3 import mock_s3, mock_s3_deprecated  # flake8: noqa
from .sts import mock_sts, mock_sts_deprecated  # flake8: noqa


try:
    # Need to monkey-patch botocore requests back to underlying urllib3 classes
    from botocore.awsrequest import HTTPSConnectionPool, HTTPConnectionPool, HTTPConnection, VerifiedHTTPSConnection
except ImportError:
    pass
else:
    HTTPSConnectionPool.ConnectionCls = VerifiedHTTPSConnection
    HTTPConnectionPool.ConnectionCls = HTTPConnection
