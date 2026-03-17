"""Abstraction for sessions in various clouds."""

import logging
import os
import warnings

from fiona._path import _parse_path, _UnparsedPath

log = logging.getLogger(__name__)

try:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        import boto3
except ImportError:
    log.debug("Could not import boto3, continuing with reduced functionality.")
    boto3 = None


class Session:
    """Base for classes that configure access to secured resources.

    Attributes
    ----------
    credentials : dict
        Keys and values for session credentials.

    Notes
    -----
    This class is not intended to be instantiated.

    """

    @classmethod
    def hascreds(cls, config):
        """Determine if the given configuration has proper credentials

        Parameters
        ----------
        cls : class
            A Session class.
        config : dict
            GDAL configuration as a dict.

        Returns
        -------
        bool

        """
        return NotImplemented

    def get_credential_options(self):
        """Get credentials as GDAL configuration options

        Returns
        -------
        dict

        """
        return NotImplemented

    @staticmethod
    def from_foreign_session(session, cls=None):
        """Create a session object matching the foreign `session`.

        Parameters
        ----------
        session : obj
            A foreign session object.
        cls : Session class, optional
            The class to return.

        Returns
        -------
        Session

        """
        if not cls:
            return DummySession()
        else:
            return cls(session)

    @staticmethod
    def cls_from_path(path):
        """Find the session class suited to the data at `path`.

        Parameters
        ----------
        path : str
            A dataset path or identifier.

        Returns
        -------
        class

        """
        if not path:
            return DummySession

        path = _parse_path(path)

        if isinstance(path, _UnparsedPath) or path.is_local:
            return DummySession

        elif (
            path.scheme == "s3" or "amazonaws.com" in path.path
        ) and "X-Amz-Signature" not in path.path:
            if boto3 is not None:
                return AWSSession
            else:
                log.info("boto3 not available, falling back to a DummySession.")
                return DummySession

        elif path.scheme == "oss" or "aliyuncs.com" in path.path:
            return OSSSession

        elif path.path.startswith("/vsiswift/"):
            return SwiftSession

        elif path.scheme == "az":
            return AzureSession

        # This factory can be extended to other cloud providers here.
        # elif path.scheme == "cumulonimbus":  # for example.
        #     return CumulonimbusSession(*args, **kwargs)

        else:
            return DummySession

    @staticmethod
    def from_path(path, *args, **kwargs):
        """Create a session object suited to the data at `path`.

        Parameters
        ----------
        path : str
            A dataset path or identifier.
        args : sequence
            Positional arguments for the foreign session constructor.
        kwargs : dict
            Keyword arguments for the foreign session constructor.

        Returns
        -------
        Session

        """
        return Session.cls_from_path(path)(*args, **kwargs)

    @staticmethod
    def aws_or_dummy(*args, **kwargs):
        """Create an AWSSession if boto3 is available, else DummySession
        
        Parameters
        ----------
        path : str
            A dataset path or identifier.
        args : sequence
            Positional arguments for the foreign session constructor.
        kwargs : dict
            Keyword arguments for the foreign session constructor.
            
        Returns
        -------
        Session
        """
        if boto3 is not None:
            return AWSSession(*args, **kwargs)
        else:
            return DummySession(*args, **kwargs)

    @staticmethod
    def from_environ(*args, **kwargs):
        """Create a session object suited to the environment.
        
        Parameters
        ----------
        path : str
            A dataset path or identifier.
        args : sequence
            Positional arguments for the foreign session constructor.
        kwargs : dict
            Keyword arguments for the foreign session constructor.
            
        Returns
        -------
        Session
        """
        try:
            session = Session.aws_or_dummy(*args, **kwargs)
            session.credentials
        except RuntimeError:
            log.warning(
                "Credentials in environment have expired. Creating a DummySession."
            )
            session = DummySession(*args, **kwargs)
        return session


class DummySession(Session):
    """A dummy session.

    Attributes
    ----------
    credentials : dict
        The session credentials.

    """

    def __init__(self, *args, **kwargs):
        self._session = None
        self.credentials = {}

    @classmethod
    def hascreds(cls, config):
        """Determine if the given configuration has proper credentials

        Parameters
        ----------
        cls : class
            A Session class.
        config : dict
            GDAL configuration as a dict.

        Returns
        -------
        bool

        """
        return True

    def get_credential_options(self):
        """Get credentials as GDAL configuration options

        Returns
        -------
        dict

        """
        return {}


class AWSSession(Session):
    """Configures access to secured resources stored in AWS S3.
    """

    def __init__(
        self,
        session=None,
        aws_unsigned=False,
        aws_access_key_id=None,
        aws_secret_access_key=None,
        aws_session_token=None,
        region_name=None,
        profile_name=None,
        endpoint_url=None,
        requester_pays=False,
    ):
        """Create a new AWS session

        Parameters
        ----------
        session : optional
            A boto3 session object.
        aws_unsigned : bool, optional (default: False)
            If True, requests will be unsigned.
        aws_access_key_id : str, optional
            An access key id, as per boto3.
        aws_secret_access_key : str, optional
            A secret access key, as per boto3.
        aws_session_token : str, optional
            A session token, as per boto3.
        region_name : str, optional
            A region name, as per boto3.
        profile_name : str, optional
            A shared credentials profile name, as per boto3.
        endpoint_url: str, optional
            An endpoint_url, as per GDAL's AWS_S3_ENDPOINT
        requester_pays : bool, optional
            True if the requester agrees to pay transfer costs (default:
            False)

        """
        if session:
            self._session = session
        else:
            self._session = boto3.Session(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
                region_name=region_name,
                profile_name=profile_name)

        self.requester_pays = requester_pays
        self.unsigned = bool(os.getenv("AWS_NO_SIGN_REQUEST", aws_unsigned))
        self.endpoint_url = endpoint_url
        self._creds = (
            self._session.get_credentials()
            if not self.unsigned and self._session
            else None
        )

    @classmethod
    def hascreds(cls, config):
        """Determine if the given configuration has proper credentials

        Parameters
        ----------
        cls : class
            A Session class.
        config : dict
            GDAL configuration as a dict.

        Returns
        -------
        bool

        """
        return {"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"}.issubset(config.keys())

    @property
    def credentials(self):
        """The session credentials as a dict"""
        res = {}
        if self._creds:  # pragma: no branch
            frozen_creds = self._creds.get_frozen_credentials()
            if frozen_creds.access_key:  # pragma: no branch
                res["aws_access_key_id"] = frozen_creds.access_key
            if frozen_creds.secret_key:  # pragma: no branch
                res["aws_secret_access_key"] = frozen_creds.secret_key
            if frozen_creds.token:
                res["aws_session_token"] = frozen_creds.token
        if self._session.region_name:
            res["aws_region"] = self._session.region_name
        if self.requester_pays:
            res["aws_request_payer"] = "requester"
        if self.endpoint_url:
            res["aws_s3_endpoint"] = self.endpoint_url
        return res

    def get_credential_options(self):
        """Get credentials as GDAL configuration options

        Returns
        -------
        dict

        """
        if self.unsigned:
            opts = {"AWS_NO_SIGN_REQUEST": "YES"}
            if "aws_region" in self.credentials:
                opts["AWS_REGION"] = self.credentials["aws_region"]
            return opts
        else:
            return {k.upper(): v for k, v in self.credentials.items()}


class GSSession(Session):
    """Configures access to secured resources stored in Google Cloud Storage
    """
    def __init__(self, google_application_credentials=None):
        """Create new Google Cloud Storage session

        Parameters
        ----------
        google_application_credentials: string
            Path to the google application credentials JSON file.

        """
        self._creds = {}
        if google_application_credentials is not None:
            self._creds['google_application_credentials'] = google_application_credentials

    @classmethod
    def hascreds(cls, config):
        """Determine if the given configuration has proper credentials

        Parameters
        ----------
        cls : class
            A Session class.
        config : dict
            GDAL configuration as a dict.

        Returns
        -------
        bool

        """
        return 'GOOGLE_APPLICATION_CREDENTIALS' in config

    @property
    def credentials(self):
        """The session credentials as a dict"""
        return self._creds

    def get_credential_options(self):
        """Get credentials as GDAL configuration options

        Returns
        -------
        dict

        """
        return {k.upper(): v for k, v in self.credentials.items()}


class OSSSession(Session):
    """Configures access to secured resources stored in Alibaba Cloud OSS."""

    def __init__(
        self, oss_access_key_id=None, oss_secret_access_key=None, oss_endpoint=None
    ):
        """Create new Alibaba Cloud OSS session

        Parameters
        ----------
        oss_access_key_id: string, optional (default: None)
            An access key id
        oss_secret_access_key: string, optional (default: None)
            An secret access key
        oss_endpoint: string, optional (default: None)
            the region attached to the bucket

        """
        self._creds = {
            "oss_access_key_id": oss_access_key_id,
            "oss_secret_access_key": oss_secret_access_key,
            "oss_endpoint": oss_endpoint,
        }

    @classmethod
    def hascreds(cls, config):
        """Determine if the given configuration has proper credentials

        Parameters
        ----------
        cls : class
            A Session class.
        config : dict
            GDAL configuration as a dict.

        Returns
        -------
        bool

        """
        return {"OSS_ACCESS_KEY_ID", "OSS_SECRET_ACCESS_KEY"}.issubset(config.keys())

    @property
    def credentials(self):
        """The session credentials as a dict"""
        return self._creds

    def get_credential_options(self):
        """Get credentials as GDAL configuration options

        Returns
        -------
        dict

        """
        return {k.upper(): v for k, v in self.credentials.items()}


class SwiftSession(Session):
    """Configures access to secured resources stored in OpenStack Swift Object Storage."""

    def __init__(
        self,
        session=None,
        swift_storage_url=None,
        swift_auth_token=None,
        swift_auth_v1_url=None,
        swift_user=None,
        swift_key=None,
    ):
        """Create new OpenStack Swift Object Storage Session.

        Three methods are possible:
            1. Create session by the swiftclient library.
            2. The SWIFT_STORAGE_URL and SWIFT_AUTH_TOKEN (this method
               is recommended by GDAL docs).
            3. The SWIFT_AUTH_V1_URL, SWIFT_USER and SWIFT_KEY (This
               depends on the swiftclient library).

        Parameters
        ----------
        session: optional
            A swiftclient connection object
        swift_storage_url:
            the storage URL
        swift_auth_token:
            the value of the x-auth-token authorization token
        swift_storage_url: string, optional
            authentication URL
        swift_user: string, optional
            user name to authenticate as
        swift_key: string, optional
            key/password to authenticate with

        Examples
        --------
        >>> import rasterio
        >>> from rasterio.session import SwiftSession
        >>> fp = '/vsiswift/bucket/key.tif'
        >>> conn = Connection(
        ...     authurl='http://127.0.0.1:7777/auth/v1.0',
        ...     user='test:tester',
        ...     key='testing'
        ... )
        >>> session = SwiftSession(conn)
        >>> with rasterio.Env(session):
        >>>     with rasterio.open(fp) as src:
        >>>         print(src.profile)

        """
        if swift_storage_url and swift_auth_token:
            self._creds = {
                "swift_storage_url": swift_storage_url,
                "swift_auth_token": swift_auth_token,
            }
        else:
            from swiftclient.client import Connection

            if session:
                self._session = session
            else:
                self._session = Connection(
                    authurl=swift_auth_v1_url, user=swift_user, key=swift_key
                )
            self._creds = {
                "swift_storage_url": self._session.get_auth()[0],
                "swift_auth_token": self._session.get_auth()[1],
            }

    @classmethod
    def hascreds(cls, config):
        """Determine if the given configuration has proper credentials

        Parameters
        ----------
        cls : class
            A Session class.
        config : dict
            GDAL configuration as a dict.

        Returns
        -------
        bool

        """
        return {"SWIFT_STORAGE_URL", "SWIFT_AUTH_TOKEN"}.issubset(config.keys())

    @property
    def credentials(self):
        """The session credentials as a dict"""
        return self._creds

    def get_credential_options(self):
        """Get credentials as GDAL configuration options

        Returns
        -------
        dict

        """
        return {k.upper(): v for k, v in self.credentials.items()}


class AzureSession(Session):
    """Configures access to secured resources stored in Microsoft Azure Blob Storage."""

    def __init__(
        self,
        azure_storage_connection_string=None,
        azure_storage_account=None,
        azure_storage_access_key=None,
        azure_unsigned=False,
    ):
        """Create new Microsoft Azure Blob Storage session

        Parameters
        ----------
        azure_storage_connection_string: string
            A connection string contains both an account name and a secret key.
        azure_storage_account: string
            An account name
        azure_storage_access_key: string
            A secret key
        azure_unsigned : bool, optional (default: False)
            If True, requests will be unsigned.

        """
        self.unsigned = bool(os.getenv("AZURE_NO_SIGN_REQUEST", azure_unsigned))
        self.storage_account = os.getenv("AZURE_STORAGE_ACCOUNT", azure_storage_account)

        if azure_storage_connection_string:
            self._creds = {
                "azure_storage_connection_string": azure_storage_connection_string
            }
        elif not self.unsigned:
            self._creds = {
                "azure_storage_account": self.storage_account,
                "azure_storage_access_key": azure_storage_access_key,
            }
        else:
            self._creds = {"azure_storage_account": self.storage_account}

    @classmethod
    def hascreds(cls, config):
        """Determine if the given configuration has proper credentials

        Parameters
        ----------
        cls : class
            A Session class.
        config : dict
            GDAL configuration as a dict.

        Returns
        -------
        bool

        """
        return (
            "AZURE_STORAGE_CONNECTION_STRING" in config
            or {"AZURE_STORAGE_ACCOUNT", "AZURE_STORAGE_ACCESS_KEY"}.issubset(
                config.keys()
            )
            or {"AZURE_STORAGE_ACCOUNT", "AZURE_NO_SIGN_REQUEST"}.issubset(
                config.keys()
            )
        )

    @property
    def credentials(self):
        """The session credentials as a dict"""
        return self._creds

    def get_credential_options(self):
        """Get credentials as GDAL configuration options

        Returns
        -------
        dict

        """
        if self.unsigned:
            return {
                "AZURE_NO_SIGN_REQUEST": "YES",
                "AZURE_STORAGE_ACCOUNT": self.storage_account,
            }
        else:
            return {k.upper(): v for k, v in self.credentials.items()}
