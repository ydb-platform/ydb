"""Rasterio's GDAL/AWS environment"""

from functools import wraps, total_ordering
from inspect import getfullargspec as getargspec
import logging
import os
import re
import threading
import warnings

import attr

from rasterio._env import (
    GDALEnv,
    get_gdal_config,
    set_gdal_config,
    GDALDataFinder,
    PROJDataFinder,
    set_proj_data_search_path,
)
from rasterio._version import gdal_version
from rasterio.errors import EnvError, GDALVersionError, RasterioDeprecationWarning
from rasterio.session import Session, DummySession


class ThreadEnv(threading.local):
    def __init__(self):
        self._env = None  # Initialises in each thread

        # When the outermost 'rasterio.Env()' executes '__enter__' it
        # probes the GDAL environment to see if any of the supplied
        # config options already exist, the assumption being that they
        # were set with 'osgeo.gdal.SetConfigOption()' or possibly
        # 'rasterio.env.set_gdal_config()'.  The discovered options are
        # reinstated when the outermost Rasterio environment exits.
        # Without this check any environment options that are present in
        # the GDAL environment and are also passed to 'rasterio.Env()'
        # will be unset when 'rasterio.Env()' tears down, regardless of
        # their value.  For example:
        #
        #   from osgeo import gdal import rasterio
        #
        #   gdal.SetConfigOption('key', 'value') with
        #   rasterio.Env(key='something'): pass
        #
        # The config option 'key' would be unset when 'Env()' exits.
        # A more comprehensive solution would also leverage
        # https://trac.osgeo.org/gdal/changeset/37273 but this gets
        # Rasterio + older versions of GDAL halfway there.  One major
        # assumption is that environment variables are not set directly
        # with 'osgeo.gdal.SetConfigOption()' OR
        # 'rasterio.env.set_gdal_config()' inside of a 'rasterio.Env()'.
        self._discovered_options = None


local = ThreadEnv()

log = logging.getLogger(__name__)


class Env:
    """Abstraction for GDAL and AWS configuration

    The GDAL library is stateful: it has a registry of format drivers,
    an error stack, and dozens of configuration options.

    Rasterio's approach to working with GDAL is to wrap all the state
    up using a Python context manager (see PEP 343,
    https://www.python.org/dev/peps/pep-0343/). When the context is
    entered GDAL drivers are registered, error handlers are
    configured, and configuration options are set. When the context
    is exited, drivers are removed from the registry and other
    configurations are removed.

    Example
    -------
    .. code-block:: python

        with rasterio.Env(GDAL_CACHEMAX=128000000) as env:
            # All drivers are registered, GDAL's raster block cache
            # size is set to 128 MB.
            # Commence processing...
            ...
            # End of processing.

        # At this point, configuration options are set to their
        # previous (possible unset) values.

    A boto3 session or boto3 session constructor arguments
    `aws_access_key_id`, `aws_secret_access_key`, `aws_session_token`
    may be passed to Env's constructor. In the latter case, a session
    will be created as soon as needed. AWS credentials are configured
    for GDAL as needed.
    """

    @classmethod
    def default_options(cls):
        """Default configuration options

        Parameters
        ----------
        None

        Returns
        -------
        dict
        """
        return {
            'GTIFF_IMPLICIT_JPEG_OVR': False,
            "RASTERIO_ENV": True
        }

    def __init__(self, session=None, aws_unsigned=False, profile_name=None,
                 session_class=Session.aws_or_dummy, **options):
        """Create a new GDAL/AWS environment.

        Note: this class is a context manager. GDAL isn't configured
        until the context is entered via `with rasterio.Env():`

        Parameters
        ----------
        session : optional
            A Session object.
        aws_unsigned : bool, optional
            Do not sign cloud requests.
        profile_name : str, optional
            A shared credentials profile name, as per boto3.
        session_class : Session, optional
            A sub-class of Session.
        **options : optional
            A mapping of GDAL configuration options, e.g.,
            `CPL_DEBUG=True, CHECK_WITH_INVERT_PROJ=False`.

        Returns
        -------
        Env

        Notes
        -----
        We raise EnvError if the GDAL config options
        AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY are given. AWS
        credentials are handled exclusively by boto3.

        Examples
        --------

        >>> with Env(CPL_DEBUG=True, CPL_CURL_VERBOSE=True):
        ...     with rasterio.open("https://example.com/a.tif") as src:
        ...         print(src.profile)

        For access to secured cloud resources, a Rasterio Session or a
        foreign session object may be passed to the constructor.

        >>> import boto3
        >>> from rasterio.session import AWSSession
        >>> boto3_session = boto3.Session(...)
        >>> with Env(AWSSession(boto3_session)):
        ...     with rasterio.open("s3://mybucket/a.tif") as src:
        ...         print(src.profile)

        """
        aws_access_key_id = options.pop('aws_access_key_id', None)
        # Before 1.0, Rasterio only supported AWS. We will special
        # case AWS in 1.0.x. TODO: warn deprecation in 1.1.
        if aws_access_key_id:
            warnings.warn(
                "Passing abstract session keyword arguments is deprecated. "
                "Pass a Rasterio AWSSession object instead.",
                RasterioDeprecationWarning
            )

        aws_secret_access_key = options.pop('aws_secret_access_key', None)
        aws_session_token = options.pop('aws_session_token', None)
        region_name = options.pop('region_name', None)

        if ('AWS_ACCESS_KEY_ID' in options or
                'AWS_SECRET_ACCESS_KEY' in options):
            raise EnvError(
                "GDAL's AWS config options can not be directly set. "
                "AWS credentials are handled exclusively by boto3.")

        if session:
            # Passing a session via keyword argument is the canonical
            # way to configure access to secured cloud resources.
            if not isinstance(session, Session):
                warnings.warn(
                    "Passing a boto3 session is deprecated. Pass a Rasterio "
                    "AWSSession object instead.",
                    RasterioDeprecationWarning
                )
                session = Session.aws_or_dummy(session=session)

            self.session = session

        elif aws_access_key_id or profile_name or aws_unsigned:
            self.session = Session.aws_or_dummy(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
                region_name=region_name,
                profile_name=profile_name,
                aws_unsigned=aws_unsigned)

        elif 'AWS_ACCESS_KEY_ID' in os.environ and 'AWS_SECRET_ACCESS_KEY' in os.environ:
            self.session = Session.from_environ()
            self._session_from_environ = True

        else:
            self.session = DummySession()

        self.options = options.copy()
        self.context_options = {}

    @classmethod
    def from_defaults(cls, *args, **kwargs):
        """Create an environment with default config options

        Parameters
        ----------
        args : optional
            Positional arguments for Env()
        kwargs : optional
            Keyword arguments for Env()

        Returns
        -------
        Env

        Notes
        -----
        The items in kwargs will be overlaid on the default values.

        """
        options = Env.default_options()
        options.update(**kwargs)
        return Env(*args, **options)

    def aws_creds_from_context_options(self):
        return {k: v for k, v in self.context_options.items() if k.startswith('AWS_')}

    def credentialize(self):
        """Get credentials and configure GDAL

        Note well: this method is a no-op if the GDAL environment
        already has credentials, unless session is not None.

        Returns
        -------
        None

        """
        cred_opts = self.session.get_credential_options()
        self.options.update(**cred_opts)
        setenv(**cred_opts)

        if getattr(self, '_session_from_environ', False):
            # if self.context_options has "AWS_*" credentials from parent context then it should
            # always override what comes back from self.session.get_credential_options()
            # b/c __init__ might have created a session from globally exported "AWS_*" os environ variables
            parent_context_creds = self.aws_creds_from_context_options()
            if not parent_context_creds:
                return
            self.options.update(**parent_context_creds)
            setenv(**parent_context_creds)

    def drivers(self):
        """Return a mapping of registered drivers."""
        return local._env.drivers()

    def _dump_open_datasets(self):
        """Writes descriptions of open datasets to stderr

        For debugging and testing purposes.
        """
        return local._env._dump_open_datasets()

    def _dump_vsimem(self):
        """Returns contents of /vsimem/.

        For debugging and testing purposes.
        """
        return local._env._dump_vsimem()

    def __enter__(self):
        log.debug("Entering env context: %r", self)
        if local._env is None:
            log.debug("Starting outermost env")
            self._has_parent_env = False

            # See note directly above where _discovered_options is globally
            # defined.  This MUST happen before calling 'defenv()'.
            local._discovered_options = {}
            # Don't want to reinstate the "RASTERIO_ENV" option.
            probe_env = {k for k in self.options.keys() if k != "RASTERIO_ENV"}
            for key in probe_env:
                val = get_gdal_config(key, normalize=False)
                if val is not None:
                    local._discovered_options[key] = val

            defenv(**self.options)
            self.context_options = {}
        else:
            self._has_parent_env = True
            self.context_options = getenv()
            setenv(**self.options)

        self.credentialize()

        log.debug("Entered env context: %r", self)
        return self

    def __exit__(self, exc_type=None, exc_val=None, exc_tb=None):
        log.debug("Exiting env context: %r", self)
        delenv()
        if self._has_parent_env:
            defenv()
            setenv(**self.context_options)
        else:
            log.debug("Exiting outermost env")
            # See note directly above where _discovered_options is globally
            # defined.
            while local._discovered_options:
                key, val = local._discovered_options.popitem()
                set_gdal_config(key, val, normalize=False)
            local._discovered_options = None
        log.debug("Exited env context: %r", self)


def defenv(**options):
    """Create a default environment if necessary."""
    if local._env:
        log.debug("GDAL environment exists: %r", local._env)
    else:
        log.debug("No GDAL environment exists")
        local._env = GDALEnv()
        local._env.update_config_options(**options)
        log.debug(
            "New GDAL environment %r created", local._env)
    local._env.start()


def getenv():
    """Get a mapping of current options."""
    if not local._env:
        raise EnvError("No GDAL environment exists")
    else:
        log.debug("Got a copy of environment %r options", local._env)
        return local._env.options.copy()


def hasenv():
    return bool(local._env)


def setenv(**options):
    """Set options in the existing environment."""
    if not local._env:
        raise EnvError("No GDAL environment exists")
    else:
        local._env.update_config_options(**options)


def hascreds():
    warnings.warn("Please use Env.session.hascreds() instead", RasterioDeprecationWarning)
    return local._env is not None and all(key in local._env.get_config_options() for key in ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY'])


def delenv():
    """Delete options in the existing environment."""
    if not local._env:
        raise EnvError("No GDAL environment exists")
    else:
        local._env.clear_config_options()
        log.debug("Cleared existing %r options", local._env)
    local._env.stop()
    local._env = None


class NullContextManager:

    def __init__(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


def env_ctx_if_needed():
    """Return an Env if one does not exist

    Returns
    -------
    Env or a do-nothing context manager

    """
    if local._env:
        return NullContextManager()
    else:
        return Env.from_defaults()


def ensure_env(f):
    """A decorator that ensures an env exists before a function
    calls any GDAL C functions."""
    @wraps(f)
    def wrapper(*args, **kwds):
        if local._env:
            return f(*args, **kwds)
        else:
            with Env.from_defaults():
                return f(*args, **kwds)
    return wrapper


def ensure_env_credentialled(f):
    """DEPRECATED alias for ensure_env_with_credentials"""
    warnings.warn("Please use ensure_env_with_credentials instead", RasterioDeprecationWarning)
    return ensure_env_with_credentials(f)


def ensure_env_with_credentials(f):
    """Ensures a config environment exists and is credentialized

    Parameters
    ----------
    f : function
        A function.

    Returns
    -------
    A function wrapper.

    Notes
    -----
    The function wrapper checks the first argument of f and
    credentializes the environment if the first argument is a URI with
    scheme "s3".

    """
    @wraps(f)
    def wrapper(*args, **kwds):
        if local._env:
            env_ctor = Env
        else:
            env_ctor = Env.from_defaults

        fp_arg = kwds.get("fp", None) or args[0]

        if isinstance(fp_arg, str):
            session_cls = Session.cls_from_path(fp_arg)

            if local._env and session_cls.hascreds(getenv()):
                session_cls = DummySession

            session = session_cls()

        else:
            session = DummySession()

        with env_ctor(session=session):
            return f(*args, **kwds)

    return wrapper


@attr.s(slots=True)
@total_ordering
class GDALVersion:
    """Convenience class for obtaining GDAL major and minor version components
    and comparing between versions.  This is highly simplistic and assumes a
    very normal numbering scheme for versions and ignores everything except
    the major and minor components."""

    major = attr.ib(default=0, validator=attr.validators.instance_of(int))
    minor = attr.ib(default=0, validator=attr.validators.instance_of(int))
    patch = attr.ib(default=0, validator=attr.validators.instance_of(int))

    def __eq__(self, other):
        return (self.major, self.minor, self.patch) == (other.major, other.minor, other.patch)

    def __lt__(self, other):
        return (self.major, self.minor, self.patch) < (other.major, other.minor, other.patch)

    def __repr__(self):
        return f"GDALVersion(major={self.major}, minor={self.minor}, patch={self.patch})"

    def __str__(self):
        return f"{self.major}.{self.minor}.{self.patch}"

    @classmethod
    def parse(cls, input, include_patch=False):
        """
        Parses input tuple or string to GDALVersion. If input is a GDALVersion
        instance, it is returned.

        Parameters
        ----------
        input: tuple of (major, minor, patch), string, or instance of GDALVersion
        include_patch: bool, optional
            If True, patch version is included with comparisons.

        Returns
        -------
        GDALVersion instance
        """

        if isinstance(input, cls):
            return input
        if isinstance(input, tuple):
            if not include_patch:
                input = input[:2]
            return cls(*input)
        elif isinstance(input, str):
            # Extract major and minor version components.
            # alpha, beta, rc suffixes ignored
            match = re.search(r'^(?P<major>\d+)\.(?P<minor>\d+)(\.(?P<patch>\d+))?', input)
            if not match:
                raise ValueError(
                    f"value does not appear to be a valid GDAL version number: {input}"
                )
            version = match.groupdict()
            major = int(version["major"])
            minor = int(version["minor"])
            patch = int(version["patch"]) if include_patch and version["patch"] else 0
            return cls(major=major, minor=minor, patch=patch)

        raise TypeError("GDALVersion can only be parsed from a string or tuple")

    @classmethod
    def runtime(cls, include_patch=False):
        """Return GDALVersion of current GDAL runtime"""
        return cls.parse(gdal_version(), include_patch=include_patch)

    def at_least(self, other, include_patch=False):
        other = self.__class__.parse(other, include_patch=include_patch)
        return self >= other


_GDAL_RUNTIME_VERSION = GDALVersion.runtime()
_GDAL_AT_LEAST_3_10 = _GDAL_RUNTIME_VERSION.at_least("3.10")
_GDAL_AT_LEAST_3_11 = _GDAL_RUNTIME_VERSION.at_least("3.11")
_GDAL_AT_LEAST_3_12_1 = GDALVersion.runtime(include_patch=True).at_least("3.12.1", include_patch=True)


def require_gdal_version(version, param=None, values=None, is_max_version=False,
                         reason=''):
    """A decorator that ensures the called function or parameters are supported
    by the runtime version of GDAL.  Raises GDALVersionError if conditions
    are not met.

    Examples
    --------

    .. code-block:: python

        @require_gdal_version('2.2')
        def some_func():

    calling `some_func` with a runtime version of GDAL that is < 2.2 raises a
    GDALVersionErorr.

    .. code-block:: python

        @require_gdal_version('2.2', param='foo')
        def some_func(foo='bar'):

    calling `some_func` with parameter `foo` of any value on GDAL < 2.2 raises
    a GDALVersionError.

    .. code-block:: python

        @require_gdal_version('2.2', param='foo', values=('bar',))
        def some_func(foo=None):

    calling `some_func` with parameter `foo` and value `bar` on GDAL < 2.2
    raises a GDALVersionError.


    Parameters
    ------------
    version: tuple, string, or GDALVersion
    param: string (optional, default: None)
        If `values` are absent, then all use of this parameter with a value
        other than default value requires at least GDAL `version`.
    values: tuple, list, or set (optional, default: None)
        contains values that require at least GDAL `version`.  `param`
        is required for `values`.
    is_max_version: bool (optional, default: False)
        if `True` indicates that the version provided is the maximum version
        allowed, instead of requiring at least that version.
    reason: string (optional: default: '')
        custom error message presented to user in addition to message about
        GDAL version.  Use this to provide an explanation of what changed
        if necessary context to the user.

    Returns
    ---------
    wrapped function
    """

    if values is not None:
        if param is None:
            raise ValueError(
                'require_gdal_version: param must be provided with values')

        if not isinstance(values, (tuple, list, set)):
            raise ValueError(
                'require_gdal_version: values must be a tuple, list, or set')

    version = GDALVersion.parse(version)
    runtime = _GDAL_RUNTIME_VERSION
    inequality = ">=" if runtime < version else "<="
    reason = f"\n{reason}" if reason else reason

    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwds):
            if ((runtime < version and not is_max_version) or
                    (is_max_version and runtime > version)):

                if param is None:
                    raise GDALVersionError(
                        "GDAL version must be {} {}{}".format(
                            inequality, str(version), reason))

                # normalize args and kwds to dict
                argspec = getargspec(f)
                full_kwds = kwds.copy()

                if argspec.args:
                    full_kwds.update(dict(zip(argspec.args[:len(args)], args)))

                if argspec.defaults:
                    defaults = dict(zip(
                        reversed(argspec.args), reversed(argspec.defaults)))
                else:
                    defaults = {}

                if param in full_kwds:
                    if values is None:
                        if param not in defaults or (
                                full_kwds[param] != defaults[param]):
                            raise GDALVersionError(
                                'usage of parameter "{}" requires '
                                "GDAL {} {}{}".format(
                                    param, inequality, version, reason
                                )
                            )

                    elif full_kwds[param] in values:
                        raise GDALVersionError(
                            'parameter "{}={}" requires '
                            "GDAL {} {}{}".format(
                                param, full_kwds[param], inequality, version, reason
                            )
                        )

            return f(*args, **kwds)

        return wrapper

    return decorator


# Patch the environment if needed, such as in the installed wheel case.

if 'GDAL_DATA' not in os.environ:

    path = GDALDataFinder().search_wheel()

    if path:
        log.debug("GDAL data found in package: path=%r.", path)
        set_gdal_config("GDAL_DATA", path)

    # See https://github.com/rasterio/rasterio/issues/1631.
    elif GDALDataFinder().find_file("gdalvrt.xsd"):
        log.debug("GDAL data files are available at built-in paths.")

    else:
        path = GDALDataFinder().search()

        if path:
            set_gdal_config("GDAL_DATA", path)
            log.debug("GDAL data found in other locations: path=%r.", path)

if "PROJ_DATA" in os.environ:
    # PROJ 9.1+
    path = os.environ["PROJ_DATA"]
    set_proj_data_search_path(path)
elif "PROJ_LIB" in os.environ:
    # PROJ < 9.1
    path = os.environ["PROJ_LIB"]
    set_proj_data_search_path(path)

elif PROJDataFinder().search_wheel():
    path = PROJDataFinder().search_wheel()
    log.debug("PROJ data found in package: path=%r.", path)
    set_proj_data_search_path(path)

# See https://github.com/rasterio/rasterio/issues/1631.
elif PROJDataFinder().has_data():
    log.debug("PROJ data files are available at built-in paths.")

else:
    path = PROJDataFinder().search()

    if path:
        log.debug("PROJ data found in other locations: path=%r.", path)
        set_proj_data_search_path(path)
