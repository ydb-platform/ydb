from tableauserverclient.helpers.logging import logger

import requests
import urllib3
import ssl

from defusedxml.ElementTree import fromstring, ParseError
from packaging.version import Version
from tableauserverclient.server.endpoint import (
    Sites,
    Views,
    Users,
    Groups,
    Workbooks,
    Datasources,
    Projects,
    Auth,
    Schedules,
    ServerInfo,
    Tasks,
    Subscriptions,
    Jobs,
    Metadata,
    Databases,
    Tables,
    Flows,
    FlowTasks,
    Webhooks,
    DataAccelerationReport,
    Favorites,
    DataAlerts,
    Fileuploads,
    FlowRuns,
    Metrics,
    Endpoint,
    CustomViews,
    LinkedTasks,
    GroupSets,
    Tags,
    VirtualConnections,
    OIDC,
    Extensions,
)
from tableauserverclient.server.exceptions import (
    ServerInfoEndpointNotFoundError,
    EndpointUnavailableError,
)
from tableauserverclient.server.endpoint.exceptions import NotSignedInError
from tableauserverclient.namespace import Namespace


_PRODUCT_TO_REST_VERSION = {
    "10.0": "2.3",
    "9.3": "2.2",
    "9.2": "2.1",
    "9.1": "2.0",
    "9.0": "2.0",
}

minimum_supported_server_version = "2.3"
default_server_version = "2.4"  # first version that dropped the legacy auth endpoint


class Server:
    """
    In the Tableau REST API, the server (https://MY-SERVER/) is the base or core
    of the URI that makes up the various endpoints or methods for accessing
    resources on the server (views, workbooks, sites, users, data sources, etc.)
    The TSC library provides a Server class that represents the server. You
    create a server instance to sign in to the server and to call the various
    methods for accessing resources.

    The Server class contains the attributes that represent the server on
    Tableau Server. After you create an instance of the Server class, you can
    sign in to the server and call methods to access all of the resources on the
    server.

    Parameters
    ----------
    server_address : str
        Specifies the address of the Tableau Server or Tableau Cloud (for
        example, https://MY-SERVER/).

    use_server_version : bool
        Specifies the version of the REST API to use (for example, '2.5'). When
        you use the TSC library to call methods that access Tableau Server, the
        version is passed to the endpoint as part of the URI
        (https://MY-SERVER/api/2.5/). Each release of Tableau Server supports
        specific versions of the REST API. New versions of the REST API are
        released with Tableau Server. By default, the value of version is set to
        '2.3', which corresponds to Tableau Server 10.0. You can view or set
        this value. You might need to set this to a different value, for
        example, if you want to access features that are supported by the server
        and a later version of the REST API. For more information, see REST API
        Versions.

    http_options : dict, optional
        Additional options to pass to the requests library when making HTTP requests.

    session_factory : callable, optional
        A factory function that returns a requests.Session object. If not provided,
        requests.session is used.

    Examples
    --------
    >>> import tableauserverclient as TSC

    >>> # create a instance of server
    >>> server = TSC.Server('https://MY-SERVER')

    >>> # sign in, etc.

    >>> # change the REST API version to match the server
    >>> server.use_server_version()

    >>> # or change the REST API version to match a specific version
    >>> # for example, 2.8
    >>> # server.version = '2.8'

    >>> # if connecting to an older Tableau Server with weak DH keys (Python 3.12+ only)
    >>> server.configure_ssl(allow_weak_dh=True)  # Note: reduces security

    Notes
    -----
    When using Python 3.12 or later with older versions of Tableau Server, you may encounter
    SSL errors related to weak Diffie-Hellman keys. This is because newer Python versions
    enforce stronger security requirements. You can temporarily work around this using
    configure_ssl(allow_weak_dh=True), but this reduces security and should only be used
    as a temporary measure until the server can be upgraded.
    """

    class PublishMode:
        """
        Enumerates the options that specify what happens when you publish a
        workbook or data source. The options are Overwrite, Append, or
        CreateNew.
        """

        Append = "Append"
        Overwrite = "Overwrite"
        CreateNew = "CreateNew"
        Replace = "Replace"

    def __init__(self, server_address, use_server_version=False, http_options=None, session_factory=None):
        self._auth_token = None
        self._site_id = None
        self._user_id = None
        self._ssl_context = None

        # TODO: this needs to change to default to https, but without breaking existing code
        if not server_address.startswith("http://") and not server_address.startswith("https://"):
            server_address = "http://" + server_address

        self._server_address: str = server_address
        self._session_factory = session_factory or requests.session

        self.auth = Auth(self)
        self.views = Views(self)
        self.users = Users(self)
        self.sites = Sites(self)
        self.groups = Groups(self)
        self.jobs = Jobs(self)
        self.workbooks = Workbooks(self)
        self.datasources = Datasources(self)
        self.favorites = Favorites(self)
        self.flows = Flows(self)
        self.flow_tasks = FlowTasks(self)
        self.projects = Projects(self)
        self.schedules = Schedules(self)
        self.server_info = ServerInfo(self)
        self.tasks = Tasks(self)
        self.subscriptions = Subscriptions(self)
        self.metadata = Metadata(self)
        self.databases = Databases(self)
        self.tables = Tables(self)
        self.webhooks = Webhooks(self)
        self.data_acceleration_report = DataAccelerationReport(self)
        self.data_alerts = DataAlerts(self)
        self.fileuploads = Fileuploads(self)
        self._namespace = Namespace()
        self.flow_runs = FlowRuns(self)
        self.metrics = Metrics(self)
        self.custom_views = CustomViews(self)
        self.linked_tasks = LinkedTasks(self)
        self.group_sets = GroupSets(self)
        self.tags = Tags(self)
        self.virtual_connections = VirtualConnections(self)
        self.oidc = OIDC(self)
        self.extensions = Extensions(self)

        self._session = self._session_factory()
        self._http_options = dict()  # must set this before making a server call
        if http_options:
            self.add_http_options(http_options)

        self.validate_connection_settings()  # does not make an actual outgoing request

        self.version = default_server_version
        if use_server_version:
            self.use_server_version()  # this makes a server call

    def validate_connection_settings(self):
        try:
            params = Endpoint(self).set_parameters(self._http_options, None, None, None, None)
            Endpoint.set_user_agent(params)
            if not self._server_address.startswith("http://") and not self._server_address.startswith("https://"):
                self._server_address = "http://" + self._server_address
            self._session.prepare_request(requests.Request("GET", url=self._server_address, params=self._http_options))
        except Exception as req_ex:
            raise ValueError("Server connection settings not valid", req_ex)

    def __repr__(self):
        return f"<TableauServerClient [Connection: {self.baseurl}, {self.server_info.serverInfo}]>"

    def add_http_options(self, options_dict: dict):
        try:
            self._http_options.update(options_dict)
            if "verify" in options_dict.keys() and self._http_options.get("verify") is False:
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                # would be nice if you could turn them back on
        except Exception as be:
            # expected errors on invalid input:
            # 'set' object has no attribute 'keys', 'list' object has no attribute 'keys'
            # TypeError: cannot convert dictionary update sequence element #0 to a sequence (input is a tuple)
            raise ValueError(f"Invalid http options given: {options_dict}")

    def clear_http_options(self):
        self._http_options = dict()

    def _clear_auth(self):
        self._site_id = None
        self._user_id = None
        self._auth_token = None
        self._site_url = None
        self._session = self._session_factory()

    def _set_auth(self, site_id, user_id, auth_token, site_url=None):
        self._site_id = site_id
        self._user_id = user_id
        self._auth_token = auth_token
        self._site_url = site_url

    def _get_legacy_version(self):
        # the serverInfo call was introduced in 2.4, earlier than that we have this different call
        response = self._session.get(self.server_address + "/auth?format=xml")
        try:
            info_xml = fromstring(response.content)
        except ParseError as parseError:
            logger.info(parseError)
            logger.info("Could not read server version info. The server may not be running or configured.")
            return self.version
        prod_version = info_xml.find(".//product_version").text
        version = _PRODUCT_TO_REST_VERSION.get(prod_version, minimum_supported_server_version)
        return version

    def _determine_highest_version(self):
        try:
            old_version = self.version
            version = self.server_info.get().rest_api_version
        except ServerInfoEndpointNotFoundError as e:
            logger.info(f"Could not get version info from server: {e.__class__}{e}")
            version = self._get_legacy_version()
        except EndpointUnavailableError as e:
            logger.info(f"Could not get version info from server: {e.__class__}{e}")
            version = self._get_legacy_version()
        except Exception as e:
            logger.info(f"Could not get version info from server: {e.__class__}{e}")
            version = None
        logger.info(f"versions: {version}, {old_version}")
        return version or old_version

    def use_server_version(self):
        self.version = self._determine_highest_version()

    def use_highest_version(self):
        self.use_server_version()
        logger.info("use use_server_version instead", DeprecationWarning)

    def check_at_least_version(self, target: str):
        server_version = Version(self.version or "2.4")
        target_version = Version(target)
        return server_version >= target_version

    def assert_at_least_version(self, comparison: str, reason: str):
        if not self.check_at_least_version(comparison):
            error = f"{reason} is not available in API version {self.version}. Requires {comparison}"
            raise EndpointUnavailableError(error)

    @property
    def baseurl(self):
        return f"{self._server_address}/api/{str(self.version)}"

    @property
    def namespace(self):
        return self._namespace()

    @property
    def auth_token(self):
        if self._auth_token is None:
            error = "Missing authentication token. You must sign in first."
            raise NotSignedInError(error)
        return self._auth_token

    @property
    def site_id(self):
        if self._site_id is None:
            error = "Missing site ID. You must sign in first."
            raise NotSignedInError(error)
        return self._site_id

    @property
    def site_url(self):
        if self._site_url is None:
            error = "Missing site URL. You must sign in first."
            raise NotSignedInError(error)
        return self._site_url

    @property
    def user_id(self):
        if self._user_id is None:
            error = "Missing user ID. You must sign in first."
            raise NotSignedInError(error)
        return self._user_id

    @property
    def server_address(self):
        return self._server_address

    @property
    def http_options(self):
        return self._http_options

    @property
    def session(self):
        return self._session

    def is_signed_in(self):
        return self._auth_token is not None

    def configure_ssl(self, *, allow_weak_dh=False):
        """Configure SSL/TLS settings for the server connection.

        Parameters
        ----------
        allow_weak_dh : bool, optional
            If True, allows connections to servers with DH keys that are considered too small by modern Python versions.
            WARNING: This reduces security and should only be used as a temporary workaround.
        """
        if allow_weak_dh:
            logger.warning(
                "WARNING: Allowing weak Diffie-Hellman keys. This reduces security and should only be used temporarily."
            )
            self._ssl_context = ssl.create_default_context()
            # Allow weak DH keys by setting minimum key size to 512 bits (default is 1024 in Python 3.12+)
            self._ssl_context.set_dh_parameters(min_key_bits=512)
            self.add_http_options({"verify": self._ssl_context})
        else:
            self._ssl_context = None
            # Remove any custom SSL context if we're reverting to default settings
            if "verify" in self._http_options:
                del self._http_options["verify"]
