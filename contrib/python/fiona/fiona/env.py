"""Fiona's GDAL/AWS environment"""

from functools import wraps, total_ordering
from inspect import getfullargspec
import logging
import os
import re
import threading
import warnings

import attr

from fiona._env import (
    GDALDataFinder,
    GDALEnv,
    PROJDataFinder,
    calc_gdal_version_num,
    get_gdal_config,
    get_gdal_release_name,
    get_gdal_version_num,
    set_gdal_config,
    set_proj_data_search_path,
)
from fiona.errors import EnvError, FionaDeprecationWarning, GDALVersionError
from fiona.session import Session, DummySession


class ThreadEnv(threading.local):
    def __init__(self):
        self._env = None  # Initialises in each thread

        # When the outermost 'fiona.Env()' executes '__enter__' it
        # probes the GDAL environment to see if any of the supplied
        # config options already exist, the assumption being that they
        # were set with 'osgeo.gdal.SetConfigOption()' or possibly
        # 'fiona.env.set_gdal_config()'.  The discovered options are
        # reinstated when the outermost Fiona environment exits.
        # Without this check any environment options that are present in
        # the GDAL environment and are also passed to 'fiona.Env()'
        # will be unset when 'fiona.Env()' tears down, regardless of
        # their value.  For example:
        #
        #   from osgeo import gdal import fiona
        #
        #   gdal.SetConfigOption('key', 'value')
        #   with fiona.Env(key='something'):
        #       pass
        #
        # The config option 'key' would be unset when 'Env()' exits.
        # A more comprehensive solution would also leverage
        # https://trac.osgeo.org/gdal/changeset/37273 but this gets
        # Fiona + older versions of GDAL halfway there.  One major
        # assumption is that environment variables are not set directly
        # with 'osgeo.gdal.SetConfigOption()' OR
        # 'fiona.env.set_gdal_config()' inside of a 'fiona.Env()'.
        self._discovered_options = None


local = ThreadEnv()

log = logging.getLogger(__name__)


class Env:
    """Abstraction for GDAL and AWS configuration

    The GDAL library is stateful: it has a registry of format drivers,
    an error stack, and dozens of configuration options.

    Fiona's approach to working with GDAL is to wrap all the state
    up using a Python context manager (see PEP 343,
    https://www.python.org/dev/peps/pep-0343/). When the context is
    entered GDAL drivers are registered, error handlers are
    configured, and configuration options are set. When the context
    is exited, drivers are removed from the registry and other
    configurations are removed.

    Example:

        with fiona.Env(GDAL_CACHEMAX=512) as env:
            # All drivers are registered, GDAL's raster block cache
            # size is set to 512MB.
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
            "CHECK_WITH_INVERT_PROJ": True,
            "GTIFF_IMPLICIT_JPEG_OVR": False,
            "FIONA_ENV": True,
        }

    def __init__(
        self,
        session=None,
        aws_unsigned=False,
        profile_name=None,
        session_class=Session.aws_or_dummy,
        **options
    ):
        """Create a new GDAL/AWS environment.
        Note: this class is a context manager. GDAL isn't configured
        until the context is entered via `with fiona.Env():`

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
        ...     with fiona.open("zip+https://example.com/a.zip") as col:
        ...         print(col.profile)

        For access to secured cloud resources, a Fiona Session or a
        foreign session object may be passed to the constructor.

        >>> import boto3
        >>> from fiona.session import AWSSession
        >>> boto3_session = boto3.Session(...)
        >>> with Env(AWSSession(boto3_session)):
        ...     with fiona.open("zip+s3://example/a.zip") as col:
        ...         print(col.profile

        """
        aws_access_key_id = options.pop("aws_access_key_id", None)
        # Warn deprecation in 1.9, remove in 2.0.
        if aws_access_key_id:
            warnings.warn(
                "Passing abstract session keyword arguments is deprecated. "
                "Pass a Fiona AWSSession object instead.",
                FionaDeprecationWarning,
            )

        aws_secret_access_key = options.pop("aws_secret_access_key", None)
        aws_session_token = options.pop("aws_session_token", None)
        region_name = options.pop("region_name", None)

        if not {"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"}.isdisjoint(options):
            raise EnvError(
                "GDAL's AWS config options can not be directly set. "
                "AWS credentials are handled exclusively by boto3."
            )

        if session:
            # Passing a session via keyword argument is the canonical
            # way to configure access to secured cloud resources.
            # Warn deprecation in 1.9, remove in 2.0.
            if not isinstance(session, Session):
                warnings.warn(
                    "Passing a boto3 session is deprecated. Pass a Fiona AWSSession object instead.",
                    FionaDeprecationWarning,
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
                aws_unsigned=aws_unsigned,
            )

        elif {"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"}.issubset(os.environ.keys()):
            self.session = Session.from_environ()

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

    def drivers(self):
        """Return a mapping of registered drivers."""
        return local._env.drivers()

    def _dump_open_datasets(self):
        """Writes descriptions of open datasets to stderr

        For debugging and testing purposes.
        """
        return local._env._dump_open_datasets()

    def __enter__(self):
        if local._env is None:
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
        return self

    def __exit__(self, exc_type=None, exc_val=None, exc_tb=None):
        delenv()
        if self._has_parent_env:
            defenv()
            setenv(**self.context_options)
        else:
            # See note directly above where _discovered_options is globally
            # defined.
            while local._discovered_options:
                key, val = local._discovered_options.popitem()
                set_gdal_config(key, val, normalize=False)

            local._discovered_options = None


def defenv(**options):
    """Create a default environment if necessary."""
    if not local._env:
        local._env = GDALEnv()
        local._env.update_config_options(**options)

    local._env.start()


def getenv():
    """Get a mapping of current options."""
    if not local._env:
        raise EnvError("No GDAL environment exists")
    else:
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
    warnings.warn("Please use Env.session.hascreds() instead", FionaDeprecationWarning)
    return local._env is not None and all(
        key in local._env.get_config_options()
        for key in ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"]
    )


def delenv():
    """Delete options in the existing environment."""
    if not local._env:
        raise EnvError("No GDAL environment exists")
    else:
        local._env.clear_config_options()

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
    calls any GDAL C functions.

    Parameters
    ----------
    f : function
        A function.

    Returns
    -------
    A function wrapper.

    Notes
    -----
    If there is already an existing environment, the wrapper does
    nothing and immediately calls f with the given arguments.

    """

    @wraps(f)
    def wrapper(*args, **kwargs):
        if local._env:
            return f(*args, **kwargs)
        else:
            with Env.from_defaults():
                return f(*args, **kwargs)

    return wrapper


def ensure_env_with_credentials(f):
    """Ensures a config environment exists and has credentials.

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

    If there is already an existing environment, the wrapper does
    nothing and immediately calls f with the given arguments.

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
    """Convenience class for obtaining GDAL major and minor version
    components and comparing between versions.  This is highly
    simplistic and assumes a very normal numbering scheme for versions
    and ignores everything except the major and minor components.

    """

    major = attr.ib(default=0, validator=attr.validators.instance_of(int))
    minor = attr.ib(default=0, validator=attr.validators.instance_of(int))

    def __eq__(self, other):
        return (self.major, self.minor) == tuple(other.major, other.minor)

    def __lt__(self, other):
        return (self.major, self.minor) < tuple(other.major, other.minor)

    def __repr__(self):
        return f"GDALVersion(major={self.major}, minor={self.minor})"

    def __str__(self):
        return f"{self.major}.{self.minor}"

    @classmethod
    def parse(cls, input):
        """
        Parses input tuple or string to GDALVersion. If input is a GDALVersion
        instance, it is returned.

        Parameters
        ----------
        input: tuple of (major, minor), string, or instance of GDALVersion

        Returns
        -------
        GDALVersion instance

        """
        if isinstance(input, cls):
            return input
        if isinstance(input, tuple):
            return cls(*input)
        elif isinstance(input, str):
            # Extract major and minor version components.
            # alpha, beta, rc suffixes ignored
            match = re.search(r"^\d+\.\d+", input)
            if not match:
                raise ValueError(
                    "value does not appear to be a valid GDAL version "
                    f"number: {input}"
                )
            major, minor = (int(c) for c in match.group().split("."))
            return cls(major=major, minor=minor)

        raise TypeError("GDALVersion can only be parsed from a string or tuple")

    @classmethod
    def runtime(cls):
        """Return GDALVersion of current GDAL runtime"""
        return cls.parse(get_gdal_release_name())

    def at_least(self, other):
        other = self.__class__.parse(other)
        return self >= other


def require_gdal_version(
    version, param=None, values=None, is_max_version=False, reason=""
):
    """A decorator that ensures the called function or parameters are supported
    by the runtime version of GDAL.  Raises GDALVersionError if conditions
    are not met.

    Examples:
    \b
        @require_gdal_version('2.2')
        def some_func():

    calling `some_func` with a runtime version of GDAL that is < 2.2 raises a
    GDALVersionError.

    \b
        @require_gdal_version('2.2', param='foo')
        def some_func(foo='bar'):

    calling `some_func` with parameter `foo` of any value on GDAL < 2.2 raises
    a GDALVersionError.

    \b
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
            raise ValueError("require_gdal_version: param must be provided with values")

        if not isinstance(values, (tuple, list, set)):
            raise ValueError(
                "require_gdal_version: values must be a tuple, list, or set"
            )

    version = GDALVersion.parse(version)
    runtime = GDALVersion.runtime()
    inequality = ">=" if runtime < version else "<="
    reason = f"\n{reason}" if reason else reason

    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwds):
            if (runtime < version and not is_max_version) or (
                is_max_version and runtime > version
            ):

                if param is None:
                    raise GDALVersionError(
                        f"GDAL version must be {inequality} {version}{reason}"
                    )

                # normalize args and kwds to dict
                argspec = getfullargspec(f)
                full_kwds = kwds.copy()

                if argspec.args:
                    full_kwds.update(dict(zip(argspec.args[: len(args)], args)))

                if argspec.defaults:
                    defaults = dict(
                        zip(reversed(argspec.args), reversed(argspec.defaults))
                    )
                else:
                    defaults = {}

                if param in full_kwds:
                    if values is None:
                        if param not in defaults or (
                            full_kwds[param] != defaults[param]
                        ):
                            raise GDALVersionError(
                                f'usage of parameter "{param}" requires '
                                f"GDAL {inequality} {version}{reason}"
                            )

                    elif full_kwds[param] in values:
                        raise GDALVersionError(
                            f'parameter "{param}={full_kwds[param]}" requires '
                            f"GDAL {inequality} {version}{reason}"
                        )

            return f(*args, **kwds)

        return wrapper

    return decorator


# Patch the environment if needed, such as in the installed wheel case.

if "GDAL_DATA" not in os.environ:

    path = GDALDataFinder().search_wheel()

    if path:
        log.debug("GDAL data found in package: path=%r.", path)
        set_gdal_config("GDAL_DATA", path)

    # See https://github.com/mapbox/rasterio/issues/1631.
    elif GDALDataFinder().find_file("header.dxf"):
        log.debug("GDAL data files are available at built-in paths.")

    else:
        path = GDALDataFinder().search()

        if path:
            set_gdal_config("GDAL_DATA", path)
            log.debug("GDAL data found in other locations: path=%r.", path)

if 'PROJ_DATA' in os.environ:
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

# See https://github.com/mapbox/rasterio/issues/1631.
elif PROJDataFinder().has_data():
    log.debug("PROJ data files are available at built-in paths.")

else:
    path = PROJDataFinder().search()

    if path:
        log.debug("PROJ data found in other locations: path=%r.", path)
        set_proj_data_search_path(path)
