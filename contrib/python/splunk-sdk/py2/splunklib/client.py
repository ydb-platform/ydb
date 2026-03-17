# Copyright 2011-2015 Splunk, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"): you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# The purpose of this module is to provide a friendlier domain interface to
# various Splunk endpoints. The approach here is to leverage the binding
# layer to capture endpoint context and provide objects and methods that
# offer simplified access their corresponding endpoints. The design avoids
# caching resource state. From the perspective of this module, the 'policy'
# for caching resource state belongs in the application or a higher level
# framework, and its the purpose of this module to provide simplified
# access to that resource state.
#
# A side note, the objects below that provide helper methods for updating eg:
# Entity state, are written so that they may be used in a fluent style.
#

"""The **splunklib.client** module provides a Pythonic interface to the
`Splunk REST API <http://docs.splunk.com/Documentation/Splunk/latest/RESTAPI/RESTcontents>`_,
allowing you programmatically access Splunk's resources.

**splunklib.client** wraps a Pythonic layer around the wire-level
binding of the **splunklib.binding** module. The core of the library is the
:class:`Service` class, which encapsulates a connection to the server, and
provides access to the various aspects of Splunk's functionality, which are
exposed via the REST API. Typically you connect to a running Splunk instance
with the :func:`connect` function::

    import splunklib.client as client
    service = client.connect(host='localhost', port=8089,
                       username='admin', password='...')
    assert isinstance(service, client.Service)

:class:`Service` objects have fields for the various Splunk resources (such as apps,
jobs, saved searches, inputs, and indexes). All of these fields are
:class:`Collection` objects::

    appcollection = service.apps
    my_app = appcollection.create('my_app')
    my_app = appcollection['my_app']
    appcollection.delete('my_app')

The individual elements of the collection, in this case *applications*,
are subclasses of :class:`Entity`. An ``Entity`` object has fields for its
attributes, and methods that are specific to each kind of entity. For example::

    print my_app['author']  # Or: print my_app.author
    my_app.package()  # Creates a compressed package of this application
"""

import contextlib
import datetime
import json
import logging
import re
import socket
from datetime import datetime, timedelta
from time import sleep

from splunklib import six
from splunklib.six.moves import urllib

from . import data
from .binding import (AuthenticationError, Context, HTTPError, UrlEncoded,
                      _encode, _make_cookie_header, _NoAuthenticationToken,
                      namespace)
from .data import record

logger = logging.getLogger(__name__)

__all__ = [
    "connect",
    "NotSupportedError",
    "OperationError",
    "IncomparableException",
    "Service",
    "namespace"
]

PATH_APPS = "apps/local/"
PATH_CAPABILITIES = "authorization/capabilities/"
PATH_CONF = "configs/conf-%s/"
PATH_PROPERTIES = "properties/"
PATH_DEPLOYMENT_CLIENTS = "deployment/client/"
PATH_DEPLOYMENT_TENANTS = "deployment/tenants/"
PATH_DEPLOYMENT_SERVERS = "deployment/server/"
PATH_DEPLOYMENT_SERVERCLASSES = "deployment/serverclass/"
PATH_EVENT_TYPES = "saved/eventtypes/"
PATH_FIRED_ALERTS = "alerts/fired_alerts/"
PATH_INDEXES = "data/indexes/"
PATH_INPUTS = "data/inputs/"
PATH_JOBS = "search/jobs/"
PATH_JOBS_V2 = "search/v2/jobs/"
PATH_LOGGER = "/services/server/logger/"
PATH_MESSAGES = "messages/"
PATH_MODULAR_INPUTS = "data/modular-inputs"
PATH_ROLES = "authorization/roles/"
PATH_SAVED_SEARCHES = "saved/searches/"
PATH_STANZA = "configs/conf-%s/%s" # (file, stanza)
PATH_USERS = "authentication/users/"
PATH_RECEIVERS_STREAM = "/services/receivers/stream"
PATH_RECEIVERS_SIMPLE = "/services/receivers/simple"
PATH_STORAGE_PASSWORDS = "storage/passwords"

XNAMEF_ATOM = "{http://www.w3.org/2005/Atom}%s"
XNAME_ENTRY = XNAMEF_ATOM % "entry"
XNAME_CONTENT = XNAMEF_ATOM % "content"

MATCH_ENTRY_CONTENT = "%s/%s/*" % (XNAME_ENTRY, XNAME_CONTENT)


class IllegalOperationException(Exception):
    """Thrown when an operation is not possible on the Splunk instance that a
    :class:`Service` object is connected to."""
    pass


class IncomparableException(Exception):
    """Thrown when trying to compare objects (using ``==``, ``<``, ``>``, and
    so on) of a type that doesn't support it."""
    pass


class AmbiguousReferenceException(ValueError):
    """Thrown when the name used to fetch an entity matches more than one entity."""
    pass


class InvalidNameException(Exception):
    """Thrown when the specified name contains characters that are not allowed
    in Splunk entity names."""
    pass


class NoSuchCapability(Exception):
    """Thrown when the capability that has been referred to doesn't exist."""
    pass


class OperationError(Exception):
    """Raised for a failed operation, such as a time out."""
    pass


class NotSupportedError(Exception):
    """Raised for operations that are not supported on a given object."""
    pass


def _trailing(template, *targets):
    """Substring of *template* following all *targets*.

    **Example**::

        template = "this is a test of the bunnies."
        _trailing(template, "is", "est", "the") == " bunnies"

    Each target is matched successively in the string, and the string
    remaining after the last target is returned. If one of the targets
    fails to match, a ValueError is raised.

    :param template: Template to extract a trailing string from.
    :type template: ``string``
    :param targets: Strings to successively match in *template*.
    :type targets: list of ``string``s
    :return: Trailing string after all targets are matched.
    :rtype: ``string``
    :raises ValueError: Raised when one of the targets does not match.
    """
    s = template
    for t in targets:
        n = s.find(t)
        if n == -1:
            raise ValueError("Target " + t + " not found in template.")
        s = s[n + len(t):]
    return s


# Filter the given state content record according to the given arg list.
def _filter_content(content, *args):
    if len(args) > 0:
        return record((k, content[k]) for k in args)
    return record((k, v) for k, v in six.iteritems(content)
        if k not in ['eai:acl', 'eai:attributes', 'type'])

# Construct a resource path from the given base path + resource name
def _path(base, name):
    if not base.endswith('/'): base = base + '/'
    return base + name


# Load an atom record from the body of the given response
# this will ultimately be sent to an xml ElementTree so we
# should use the xmlcharrefreplace option
def _load_atom(response, match=None):
    return data.load(response.body.read()
                     .decode('utf-8', 'xmlcharrefreplace'), match)


# Load an array of atom entries from the body of the given response
def _load_atom_entries(response):
    r = _load_atom(response)
    if 'feed' in r:
        # Need this to handle a random case in the REST API
        if r.feed.get('totalResults') in [0, '0']:
            return []
        entries = r.feed.get('entry', None)
        if entries is None: return None
        return entries if isinstance(entries, list) else [entries]
    # Unlike most other endpoints, the jobs endpoint does not return
    # its state wrapped in another element, but at the top level.
    # For example, in XML, it returns <entry>...</entry> instead of
    # <feed><entry>...</entry></feed>.
    else:
        entries = r.get('entry', None)
        if entries is None: return None
        return entries if isinstance(entries, list) else [entries]


# Load the sid from the body of the given response
def _load_sid(response, output_mode):
    if output_mode == "json":
        json_obj = json.loads(response.body.read())
        return json_obj.get('sid')
    return _load_atom(response).response.sid


# Parse the given atom entry record into a generic entity state record
def _parse_atom_entry(entry):
    title = entry.get('title', None)

    elink = entry.get('link', [])
    elink = elink if isinstance(elink, list) else [elink]
    links = record((link.rel, link.href) for link in elink)

    # Retrieve entity content values
    content = entry.get('content', {})

    # Host entry metadata
    metadata = _parse_atom_metadata(content)

    # Filter some of the noise out of the content record
    content = record((k, v) for k, v in six.iteritems(content)
                     if k not in ['eai:acl', 'eai:attributes'])

    if 'type' in content:
        if isinstance(content['type'], list):
            content['type'] = [t for t in content['type'] if t != 'text/xml']
            # Unset type if it was only 'text/xml'
            if len(content['type']) == 0:
                content.pop('type', None)
            # Flatten 1 element list
            if len(content['type']) == 1:
                content['type'] = content['type'][0]
        else:
            content.pop('type', None)

    return record({
        'title': title,
        'links': links,
        'access': metadata.access,
        'fields': metadata.fields,
        'content': content,
        'updated': entry.get("updated")
    })


# Parse the metadata fields out of the given atom entry content record
def _parse_atom_metadata(content):
    # Hoist access metadata
    access = content.get('eai:acl', None)

    # Hoist content metadata (and cleanup some naming)
    attributes = content.get('eai:attributes', {})
    fields = record({
        'required': attributes.get('requiredFields', []),
        'optional': attributes.get('optionalFields', []),
        'wildcard': attributes.get('wildcardFields', [])})

    return record({'access': access, 'fields': fields})

# kwargs: scheme, host, port, app, owner, username, password
def connect(**kwargs):
    """This function connects and logs in to a Splunk instance.

    This function is a shorthand for :meth:`Service.login`.
    The ``connect`` function makes one round trip to the server (for logging in).

    :param host: The host name (the default is "localhost").
    :type host: ``string``
    :param port: The port number (the default is 8089).
    :type port: ``integer``
    :param scheme: The scheme for accessing the service (the default is "https").
    :type scheme: "https" or "http"
    :param verify: Enable (True) or disable (False) SSL verification for
                   https connections. (optional, the default is True)
    :type verify: ``Boolean``
    :param `owner`: The owner context of the namespace (optional).
    :type owner: ``string``
    :param `app`: The app context of the namespace (optional).
    :type app: ``string``
    :param sharing: The sharing mode for the namespace (the default is "user").
    :type sharing: "global", "system", "app", or "user"
    :param `token`: The current session token (optional). Session tokens can be
                    shared across multiple service instances.
    :type token: ``string``
    :param cookie: A session cookie. When provided, you don't need to call :meth:`login`.
        This parameter is only supported for Splunk 6.2+.
    :type cookie: ``string``
    :param autologin: When ``True``, automatically tries to log in again if the
        session terminates.
    :type autologin: ``boolean``
    :param `username`: The Splunk account username, which is used to
                       authenticate the Splunk instance.
    :type username: ``string``
    :param `password`: The password for the Splunk account.
    :type password: ``string``
    :param retires: Number of retries for each HTTP connection (optional, the default is 0).
                    NOTE THAT THIS MAY INCREASE THE NUMBER OF ROUND TRIP CONNECTIONS TO THE SPLUNK SERVER.
    :type retries: ``int``
    :param retryDelay: How long to wait between connection attempts if `retries` > 0 (optional, defaults to 10s).
    :type retryDelay: ``int`` (in seconds)
    :param `context`: The SSLContext that can be used when setting verify=True (optional)
    :type context: ``SSLContext``
    :return: An initialized :class:`Service` connection.

    **Example**::

        import splunklib.client as client
        s = client.connect(...)
        a = s.apps["my_app"]
        ...
    """
    s = Service(**kwargs)
    s.login()
    return s


# In preparation for adding Storm support, we added an
# intermediary class between Service and Context. Storm's
# API is not going to be the same as enterprise Splunk's
# API, so we will derive both Service (for enterprise Splunk)
# and StormService for (Splunk Storm) from _BaseService, and
# put any shared behavior on it.
class _BaseService(Context):
    pass


class Service(_BaseService):
    """A Pythonic binding to Splunk instances.

    A :class:`Service` represents a binding to a Splunk instance on an
    HTTP or HTTPS port. It handles the details of authentication, wire
    formats, and wraps the REST API endpoints into something more
    Pythonic. All of the low-level operations on the instance from
    :class:`splunklib.binding.Context` are also available in case you need
    to do something beyond what is provided by this class.

    After creating a ``Service`` object, you must call its :meth:`login`
    method before you can issue requests to Splunk.
    Alternately, use the :func:`connect` function to create an already
    authenticated :class:`Service` object, or provide a session token
    when creating the :class:`Service` object explicitly (the same
    token may be shared by multiple :class:`Service` objects).

    :param host: The host name (the default is "localhost").
    :type host: ``string``
    :param port: The port number (the default is 8089).
    :type port: ``integer``
    :param scheme: The scheme for accessing the service (the default is "https").
    :type scheme: "https" or "http"
    :param verify: Enable (True) or disable (False) SSL verification for
                   https connections. (optional, the default is True)
    :type verify: ``Boolean``
    :param `owner`: The owner context of the namespace (optional; use "-" for wildcard).
    :type owner: ``string``
    :param `app`: The app context of the namespace (optional; use "-" for wildcard).
    :type app: ``string``
    :param `token`: The current session token (optional). Session tokens can be
                    shared across multiple service instances.
    :type token: ``string``
    :param cookie: A session cookie. When provided, you don't need to call :meth:`login`.
        This parameter is only supported for Splunk 6.2+.
    :type cookie: ``string``
    :param `username`: The Splunk account username, which is used to
                       authenticate the Splunk instance.
    :type username: ``string``
    :param `password`: The password, which is used to authenticate the Splunk
                       instance.
    :type password: ``string``
    :param retires: Number of retries for each HTTP connection (optional, the default is 0).
                    NOTE THAT THIS MAY INCREASE THE NUMBER OF ROUND TRIP CONNECTIONS TO THE SPLUNK SERVER.
    :type retries: ``int``
    :param retryDelay: How long to wait between connection attempts if `retries` > 0 (optional, defaults to 10s).
    :type retryDelay: ``int`` (in seconds)
    :return: A :class:`Service` instance.

    **Example**::

        import splunklib.client as client
        s = client.Service(username="boris", password="natasha", ...)
        s.login()
        # Or equivalently
        s = client.connect(username="boris", password="natasha")
        # Or if you already have a session token
        s = client.Service(token="atg232342aa34324a")
        # Or if you already have a valid cookie
        s = client.Service(cookie="splunkd_8089=...")
    """
    def __init__(self, **kwargs):
        super(Service, self).__init__(**kwargs)
        self._splunk_version = None
        self._kvstore_owner = None
        self._instance_type = None

    @property
    def apps(self):
        """Returns the collection of applications that are installed on this instance of Splunk.

        :return: A :class:`Collection` of :class:`Application` entities.
        """
        return Collection(self, PATH_APPS, item=Application)

    @property
    def confs(self):
        """Returns the collection of configuration files for this Splunk instance.

        :return: A :class:`Configurations` collection of
            :class:`ConfigurationFile` entities.
        """
        return Configurations(self)

    @property
    def capabilities(self):
        """Returns the list of system capabilities.

        :return: A ``list`` of capabilities.
        """
        response = self.get(PATH_CAPABILITIES)
        return _load_atom(response, MATCH_ENTRY_CONTENT).capabilities

    @property
    def event_types(self):
        """Returns the collection of event types defined in this Splunk instance.

        :return: An :class:`Entity` containing the event types.
        """
        return Collection(self, PATH_EVENT_TYPES)

    @property
    def fired_alerts(self):
        """Returns the collection of alerts that have been fired on the Splunk
        instance, grouped by saved search.

        :return: A :class:`Collection` of :class:`AlertGroup` entities.
        """
        return Collection(self, PATH_FIRED_ALERTS, item=AlertGroup)

    @property
    def indexes(self):
        """Returns the collection of indexes for this Splunk instance.

        :return: An :class:`Indexes` collection of :class:`Index` entities.
        """
        return Indexes(self, PATH_INDEXES, item=Index)

    @property
    def info(self):
        """Returns the information about this instance of Splunk.

        :return: The system information, as key-value pairs.
        :rtype: ``dict``
        """
        response = self.get("/services/server/info")
        return _filter_content(_load_atom(response, MATCH_ENTRY_CONTENT))

    def input(self, path, kind=None):
        """Retrieves an input by path, and optionally kind.

        :return: A :class:`Input` object.
        """
        return Input(self, path, kind=kind).refresh()

    @property
    def inputs(self):
        """Returns the collection of inputs configured on this Splunk instance.

        :return: An :class:`Inputs` collection of :class:`Input` entities.
        """
        return Inputs(self)

    def job(self, sid):
        """Retrieves a search job by sid.

        :return: A :class:`Job` object.
        """
        return Job(self, sid).refresh()

    @property
    def jobs(self):
        """Returns the collection of current search jobs.

        :return: A :class:`Jobs` collection of :class:`Job` entities.
        """
        return Jobs(self)

    @property
    def loggers(self):
        """Returns the collection of logging level categories and their status.

        :return: A :class:`Loggers` collection of logging levels.
        """
        return Loggers(self)

    @property
    def messages(self):
        """Returns the collection of service messages.

        :return: A :class:`Collection` of :class:`Message` entities.
        """
        return Collection(self, PATH_MESSAGES, item=Message)

    @property
    def modular_input_kinds(self):
        """Returns the collection of the modular input kinds on this Splunk instance.

        :return: A :class:`ReadOnlyCollection` of :class:`ModularInputKind` entities.
        """
        if self.splunk_version >= (5,):
            return ReadOnlyCollection(self, PATH_MODULAR_INPUTS, item=ModularInputKind)
        else:
            raise IllegalOperationException("Modular inputs are not supported before Splunk version 5.")

    @property
    def storage_passwords(self):
        """Returns the collection of the storage passwords on this Splunk instance.

        :return: A :class:`ReadOnlyCollection` of :class:`StoragePasswords` entities.
        """
        return StoragePasswords(self)

    # kwargs: enable_lookups, reload_macros, parse_only, output_mode
    def parse(self, query, **kwargs):
        """Parses a search query and returns a semantic map of the search.

        :param query: The search query to parse.
        :type query: ``string``
        :param kwargs: Arguments to pass to the ``search/parser`` endpoint
            (optional). Valid arguments are:

            * "enable_lookups" (``boolean``): If ``True``, performs reverse lookups
              to expand the search expression.

            * "output_mode" (``string``): The output format (XML or JSON).

            * "parse_only" (``boolean``): If ``True``, disables the expansion of
              search due to evaluation of subsearches, time term expansion,
              lookups, tags, eventtypes, and sourcetype alias.

            * "reload_macros" (``boolean``): If ``True``, reloads macro
              definitions from macros.conf.

        :type kwargs: ``dict``
        :return: A semantic map of the parsed search query.
        """
        if not self.disable_v2_api:
            return self.post("search/v2/parser", q=query, **kwargs)
        return self.get("search/parser", q=query, **kwargs)

    def restart(self, timeout=None):
        """Restarts this Splunk instance.

        The service is unavailable until it has successfully restarted.

        If a *timeout* value is specified, ``restart`` blocks until the service
        resumes or the timeout period has been exceeded. Otherwise, ``restart`` returns
        immediately.

        :param timeout: A timeout period, in seconds.
        :type timeout: ``integer``
        """
        msg = { "value": "Restart requested by " + self.username + "via the Splunk SDK for Python"}
        # This message will be deleted once the server actually restarts.
        self.messages.create(name="restart_required", **msg)
        result = self.post("/services/server/control/restart")
        if timeout is None:
            return result
        start = datetime.now()
        diff = timedelta(seconds=timeout)
        while datetime.now() - start < diff:
            try:
                self.login()
                if not self.restart_required:
                    return result
            except Exception as e:
                sleep(1)
        raise Exception("Operation time out.")

    @property
    def restart_required(self):
        """Indicates whether splunkd is in a state that requires a restart.

        :return: A ``boolean`` that indicates whether a restart is required.

        """
        response = self.get("messages").body.read()
        messages = data.load(response)['feed']
        if 'entry' not in messages:
            result = False
        else:
            if isinstance(messages['entry'], dict):
                titles = [messages['entry']['title']]
            else:
                titles = [x['title'] for x in messages['entry']]
            result = 'restart_required' in titles
        return result

    @property
    def roles(self):
        """Returns the collection of user roles.

        :return: A :class:`Roles` collection of :class:`Role` entities.
        """
        return Roles(self)

    def search(self, query, **kwargs):
        """Runs a search using a search query and any optional arguments you
        provide, and returns a `Job` object representing the search.

        :param query: A search query.
        :type query: ``string``
        :param kwargs: Arguments for the search (optional):

            * "output_mode" (``string``): Specifies the output format of the
              results.

            * "earliest_time" (``string``): Specifies the earliest time in the
              time range to
              search. The time string can be a UTC time (with fractional
              seconds), a relative time specifier (to now), or a formatted
              time string.

            * "latest_time" (``string``): Specifies the latest time in the time
              range to
              search. The time string can be a UTC time (with fractional
              seconds), a relative time specifier (to now), or a formatted
              time string.

            * "rf" (``string``): Specifies one or more fields to add to the
              search.

        :type kwargs: ``dict``
        :rtype: class:`Job`
        :returns: An object representing the created job.
        """
        return self.jobs.create(query, **kwargs)

    @property
    def saved_searches(self):
        """Returns the collection of saved searches.

        :return: A :class:`SavedSearches` collection of :class:`SavedSearch`
            entities.
        """
        return SavedSearches(self)

    @property
    def settings(self):
        """Returns the configuration settings for this instance of Splunk.

        :return: A :class:`Settings` object containing configuration settings.
        """
        return Settings(self)

    @property
    def splunk_version(self):
        """Returns the version of the splunkd instance this object is attached
        to.

        The version is returned as a tuple of the version components as
        integers (for example, `(4,3,3)` or `(5,)`).

        :return: A ``tuple`` of ``integers``.
        """
        if self._splunk_version is None:
            self._splunk_version = tuple([int(p) for p in self.info['version'].split('.')])
        return self._splunk_version

    @property
    def splunk_instance(self):
        if self._instance_type is None :
            splunk_info = self.info;
            if hasattr(splunk_info, 'instance_type') :
                self._instance_type = splunk_info['instance_type']
            else:
                self._instance_type = ''
        return self._instance_type

    @property
    def disable_v2_api(self):
        if self.splunk_instance.lower() == 'cloud':
            return self.splunk_version < (9,0,2209)
        return self.splunk_version < (9,0,2)

    @property
    def kvstore_owner(self):
        """Returns the KVStore owner for this instance of Splunk.

        By default is the kvstore owner is not set, it will return "nobody"
        :return: A string with the KVStore owner.
        """
        if self._kvstore_owner is None:
            self._kvstore_owner = "nobody"
        return self._kvstore_owner

    @kvstore_owner.setter
    def kvstore_owner(self, value):
        """
        kvstore is refreshed, when the owner value is changed
        """
        self._kvstore_owner = value
        self.kvstore

    @property
    def kvstore(self):
        """Returns the collection of KV Store collections.

        sets the owner for the namespace, before retrieving the KVStore Collection

        :return: A :class:`KVStoreCollections` collection of :class:`KVStoreCollection` entities.
        """
        self.namespace['owner'] = self.kvstore_owner
        return KVStoreCollections(self)

    @property
    def users(self):
        """Returns the collection of users.

        :return: A :class:`Users` collection of :class:`User` entities.
        """
        return Users(self)


class Endpoint(object):
    """This class represents individual Splunk resources in the Splunk REST API.

    An ``Endpoint`` object represents a URI, such as ``/services/saved/searches``.
    This class provides the common functionality of :class:`Collection` and
    :class:`Entity` (essentially HTTP GET and POST methods).
    """
    def __init__(self, service, path):
        self.service = service
        self.path = path

    def get_api_version(self, path):
        """Return the API version of the service used in the provided path.

        Args:
            path (str): A fully-qualified endpoint path (for example, "/services/search/jobs").

        Returns:
            int: Version of the API (for example, 1)
        """
        # Default to v1 if undefined in the path
        # For example, "/services/search/jobs" is using API v1
        api_version = 1
        
        versionSearch = re.search('(?:servicesNS\/[^/]+\/[^/]+|services)\/[^/]+\/v(\d+)\/', path)
        if versionSearch:
            api_version = int(versionSearch.group(1))
    
        return api_version

    def get(self, path_segment="", owner=None, app=None, sharing=None, **query):
        """Performs a GET operation on the path segment relative to this endpoint.

        This method is named to match the HTTP method. This method makes at least
        one roundtrip to the server, one additional round trip for
        each 303 status returned, plus at most two additional round
        trips if
        the ``autologin`` field of :func:`connect` is set to ``True``.

        If *owner*, *app*, and *sharing* are omitted, this method takes a
        default namespace from the :class:`Service` object for this :class:`Endpoint`.
        All other keyword arguments are included in the URL as query parameters.

        :raises AuthenticationError: Raised when the ``Service`` is not logged in.
        :raises HTTPError: Raised when an error in the request occurs.
        :param path_segment: A path segment relative to this endpoint.
        :type path_segment: ``string``
        :param owner: The owner context of the namespace (optional).
        :type owner: ``string``
        :param app: The app context of the namespace (optional).
        :type app: ``string``
        :param sharing: The sharing mode for the namespace (optional).
        :type sharing: "global", "system", "app", or "user"
        :param query: All other keyword arguments, which are used as query
            parameters.
        :type query: ``string``
        :return: The response from the server.
        :rtype: ``dict`` with keys ``body``, ``headers``, ``reason``,
                and ``status``

        **Example**::

            import splunklib.client
            s = client.service(...)
            apps = s.apps
            apps.get() == \\
                {'body': ...a response reader object...,
                 'headers': [('content-length', '26208'),
                             ('expires', 'Fri, 30 Oct 1998 00:00:00 GMT'),
                             ('server', 'Splunkd'),
                             ('connection', 'close'),
                             ('cache-control', 'no-store, max-age=0, must-revalidate, no-cache'),
                             ('date', 'Fri, 11 May 2012 16:30:35 GMT'),
                             ('content-type', 'text/xml; charset=utf-8')],
                 'reason': 'OK',
                 'status': 200}
            apps.get('nonexistant/path') # raises HTTPError
            s.logout()
            apps.get() # raises AuthenticationError
        """
        # self.path to the Endpoint is relative in the SDK, so passing
        # owner, app, sharing, etc. along will produce the correct
        # namespace in the final request.
        if path_segment.startswith('/'):
            path = path_segment
        else:
            if not self.path.endswith('/') and path_segment != "":
                self.path = self.path + '/'
            path = self.service._abspath(self.path + path_segment, owner=owner,
                                         app=app, sharing=sharing)
        # ^-- This was "%s%s" % (self.path, path_segment).
        # That doesn't work, because self.path may be UrlEncoded.

        # Get the API version from the path
        api_version = self.get_api_version(path)

        # Search API v2+ fallback to v1:
        #   - In v2+, /results_preview, /events and /results do not support search params.
        #   - Fallback from v2+ to v1 if Splunk Version is < 9.
        # if api_version >= 2 and ('search' in query and path.endswith(tuple(["results_preview", "events", "results"])) or self.service.splunk_version < (9,)):
        #     path = path.replace(PATH_JOBS_V2, PATH_JOBS)
        
        if api_version == 1:
            if isinstance(path, UrlEncoded):
                path = UrlEncoded(path.replace(PATH_JOBS_V2, PATH_JOBS), skip_encode=True)
            else:
                path = path.replace(PATH_JOBS_V2, PATH_JOBS)

        return self.service.get(path,
                                owner=owner, app=app, sharing=sharing,
                                **query)

    def post(self, path_segment="", owner=None, app=None, sharing=None, **query):
        """Performs a POST operation on the path segment relative to this endpoint.

        This method is named to match the HTTP method. This method makes at least
        one roundtrip to the server, one additional round trip for
        each 303 status returned, plus at most two additional round trips if
        the ``autologin`` field of :func:`connect` is set to ``True``.

        If *owner*, *app*, and *sharing* are omitted, this method takes a
        default namespace from the :class:`Service` object for this :class:`Endpoint`.
        All other keyword arguments are included in the URL as query parameters.

        :raises AuthenticationError: Raised when the ``Service`` is not logged in.
        :raises HTTPError: Raised when an error in the request occurs.
        :param path_segment: A path segment relative to this endpoint.
        :type path_segment: ``string``
        :param owner: The owner context of the namespace (optional).
        :type owner: ``string``
        :param app: The app context of the namespace (optional).
        :type app: ``string``
        :param sharing: The sharing mode of the namespace (optional).
        :type sharing: ``string``
        :param query: All other keyword arguments, which are used as query
            parameters.
        :type query: ``string``
        :return: The response from the server.
        :rtype: ``dict`` with keys ``body``, ``headers``, ``reason``,
                and ``status``

        **Example**::

            import splunklib.client
            s = client.service(...)
            apps = s.apps
            apps.post(name='boris') == \\
                {'body': ...a response reader object...,
                 'headers': [('content-length', '2908'),
                             ('expires', 'Fri, 30 Oct 1998 00:00:00 GMT'),
                             ('server', 'Splunkd'),
                             ('connection', 'close'),
                             ('cache-control', 'no-store, max-age=0, must-revalidate, no-cache'),
                             ('date', 'Fri, 11 May 2012 18:34:50 GMT'),
                             ('content-type', 'text/xml; charset=utf-8')],
                 'reason': 'Created',
                 'status': 201}
            apps.get('nonexistant/path') # raises HTTPError
            s.logout()
            apps.get() # raises AuthenticationError
        """       
        if path_segment.startswith('/'):
            path = path_segment
        else:
            if not self.path.endswith('/') and path_segment != "":
                self.path = self.path + '/'
            path = self.service._abspath(self.path + path_segment, owner=owner, app=app, sharing=sharing)
            
        # Get the API version from the path
        api_version = self.get_api_version(path)

        # Search API v2+ fallback to v1:
        #   - In v2+, /results_preview, /events and /results do not support search params.
        #   - Fallback from v2+ to v1 if Splunk Version is < 9.
        # if api_version >= 2 and ('search' in query and path.endswith(tuple(["results_preview", "events", "results"])) or self.service.splunk_version < (9,)):
        #     path = path.replace(PATH_JOBS_V2, PATH_JOBS)
        
        if api_version == 1:
            if isinstance(path, UrlEncoded):
                path = UrlEncoded(path.replace(PATH_JOBS_V2, PATH_JOBS), skip_encode=True)
            else:
                path = path.replace(PATH_JOBS_V2, PATH_JOBS)

        return self.service.post(path, owner=owner, app=app, sharing=sharing, **query)


# kwargs: path, app, owner, sharing, state
class Entity(Endpoint):
    """This class is a base class for Splunk entities in the REST API, such as
    saved searches, jobs, indexes, and inputs.

    ``Entity`` provides the majority of functionality required by entities.
    Subclasses only implement the special cases for individual entities.
    For example for saved searches, the subclass makes fields like ``action.email``,
    ``alert_type``, and ``search`` available.

    An ``Entity`` is addressed like a dictionary, with a few extensions,
    so the following all work, for example in saved searches::

        ent['action.email']
        ent['alert_type']
        ent['search']

    You can also access the fields as though they were the fields of a Python
    object, as in::

        ent.alert_type
        ent.search

    However, because some of the field names are not valid Python identifiers,
    the dictionary-like syntax is preferable.

    The state of an :class:`Entity` object is cached, so accessing a field
    does not contact the server. If you think the values on the
    server have changed, call the :meth:`Entity.refresh` method.
    """
    # Not every endpoint in the API is an Entity or a Collection. For
    # example, a saved search at saved/searches/{name} has an additional
    # method saved/searches/{name}/scheduled_times, but this isn't an
    # entity in its own right. In these cases, subclasses should
    # implement a method that uses the get and post methods inherited
    # from Endpoint, calls the _load_atom function (it's elsewhere in
    # client.py, but not a method of any object) to read the
    # information, and returns the extracted data in a Pythonesque form.
    #
    # The primary use of subclasses of Entity is to handle specially
    # named fields in the Entity. If you only need to provide a default
    # value for an optional field, subclass Entity and define a
    # dictionary ``defaults``. For instance,::
    #
    #     class Hypothetical(Entity):
    #         defaults = {'anOptionalField': 'foo',
    #                     'anotherField': 'bar'}
    #
    # If you have to do more than provide a default, such as rename or
    # actually process values, then define a new method with the
    # ``@property`` decorator.
    #
    #     class Hypothetical(Entity):
    #         @property
    #         def foobar(self):
    #             return self.content['foo'] + "-" + self.content["bar"]

    # Subclasses can override defaults the default values for
    # optional fields. See above.
    defaults = {}

    def __init__(self, service, path, **kwargs):
        Endpoint.__init__(self, service, path)
        self._state = None
        if not kwargs.get('skip_refresh', False):
            self.refresh(kwargs.get('state', None))  # "Prefresh"
        return

    def __contains__(self, item):
        try:
            self[item]
            return True
        except (KeyError, AttributeError):
            return False

    def __eq__(self, other):
        """Raises IncomparableException.

        Since Entity objects are snapshots of times on the server, no
        simple definition of equality will suffice beyond instance
        equality, and instance equality leads to strange situations
        such as::

            import splunklib.client as client
            c = client.connect(...)
            saved_searches = c.saved_searches
            x = saved_searches['asearch']

        but then ``x != saved_searches['asearch']``.

        whether or not there was a change on the server. Rather than
        try to do something fancy, we simple declare that equality is
        undefined for Entities.

        Makes no roundtrips to the server.
        """
        raise IncomparableException(
            "Equality is undefined for objects of class %s" % \
                self.__class__.__name__)

    def __getattr__(self, key):
        # Called when an attribute was not found by the normal method. In this
        # case we try to find it in self.content and then self.defaults.
        if key in self.state.content:
            return self.state.content[key]
        elif key in self.defaults:
            return self.defaults[key]
        else:
            raise AttributeError(key)

    def __getitem__(self, key):
        # getattr attempts to find a field on the object in the normal way,
        # then calls __getattr__ if it cannot.
        return getattr(self, key)

    # Load the Atom entry record from the given response - this is a method
    # because the "entry" record varies slightly by entity and this allows
    # for a subclass to override and handle any special cases.
    def _load_atom_entry(self, response):
        elem = _load_atom(response, XNAME_ENTRY)
        if isinstance(elem, list):
            apps = [ele.entry.content.get('eai:appName') for ele in elem]

            raise AmbiguousReferenceException(
                "Fetch from server returned multiple entries for name '%s' in apps %s." % (elem[0].entry.title, apps))
        else:
            return elem.entry

    # Load the entity state record from the given response
    def _load_state(self, response):
        entry = self._load_atom_entry(response)
        return _parse_atom_entry(entry)

    def _run_action(self, path_segment, **kwargs):
        """Run a method and return the content Record from the returned XML.

        A method is a relative path from an Entity that is not itself
        an Entity. _run_action assumes that the returned XML is an
        Atom field containing one Entry, and the contents of Entry is
        what should be the return value. This is right in enough cases
        to make this method useful.
        """
        response = self.get(path_segment, **kwargs)
        data = self._load_atom_entry(response)
        rec = _parse_atom_entry(data)
        return rec.content

    def _proper_namespace(self, owner=None, app=None, sharing=None):
        """Produce a namespace sans wildcards for use in entity requests.

        This method tries to fill in the fields of the namespace which are `None`
        or wildcard (`'-'`) from the entity's namespace. If that fails, it uses
        the service's namespace.

        :param owner:
        :param app:
        :param sharing:
        :return:
        """
        if owner is None and app is None and sharing is None: # No namespace provided
            if self._state is not None and 'access' in self._state:
                return (self._state.access.owner,
                        self._state.access.app,
                        self._state.access.sharing)
            else:
                return (self.service.namespace['owner'],
                        self.service.namespace['app'],
                        self.service.namespace['sharing'])
        else:
            return (owner,app,sharing)

    def delete(self):
        owner, app, sharing = self._proper_namespace()
        return self.service.delete(self.path, owner=owner, app=app, sharing=sharing)

    def get(self, path_segment="", owner=None, app=None, sharing=None, **query):
        owner, app, sharing = self._proper_namespace(owner, app, sharing)
        return super(Entity, self).get(path_segment, owner=owner, app=app, sharing=sharing, **query)

    def post(self, path_segment="", owner=None, app=None, sharing=None, **query):
        owner, app, sharing = self._proper_namespace(owner, app, sharing)
        return super(Entity, self).post(path_segment, owner=owner, app=app, sharing=sharing, **query)

    def refresh(self, state=None):
        """Refreshes the state of this entity.

        If *state* is provided, load it as the new state for this
        entity. Otherwise, make a roundtrip to the server (by calling
        the :meth:`read` method of ``self``) to fetch an updated state,
        plus at most two additional round trips if
        the ``autologin`` field of :func:`connect` is set to ``True``.

        :param state: Entity-specific arguments (optional).
        :type state: ``dict``
        :raises EntityDeletedException: Raised if the entity no longer exists on
            the server.

        **Example**::

            import splunklib.client as client
            s = client.connect(...)
            search = s.apps['search']
            search.refresh()
        """
        if state is not None:
            self._state = state
        else:
            self._state = self.read(self.get())
        return self

    @property
    def access(self):
        """Returns the access metadata for this entity.

        :return: A :class:`splunklib.data.Record` object with three keys:
            ``owner``, ``app``, and ``sharing``.
        """
        return self.state.access

    @property
    def content(self):
        """Returns the contents of the entity.

        :return: A ``dict`` containing values.
        """
        return self.state.content

    def disable(self):
        """Disables the entity at this endpoint."""
        self.post("disable")
        return self

    def enable(self):
        """Enables the entity at this endpoint."""
        self.post("enable")
        return self

    @property
    def fields(self):
        """Returns the content metadata for this entity.

        :return: A :class:`splunklib.data.Record` object with three keys:
            ``required``, ``optional``, and ``wildcard``.
        """
        return self.state.fields

    @property
    def links(self):
        """Returns a dictionary of related resources.

        :return: A ``dict`` with keys and corresponding URLs.
        """
        return self.state.links

    @property
    def name(self):
        """Returns the entity name.

        :return: The entity name.
        :rtype: ``string``
        """
        return self.state.title

    def read(self, response):
        """ Reads the current state of the entity from the server. """
        results = self._load_state(response)
        # In lower layers of the SDK, we end up trying to URL encode
        # text to be dispatched via HTTP. However, these links are already
        # URL encoded when they arrive, and we need to mark them as such.
        unquoted_links = dict([(k, UrlEncoded(v, skip_encode=True))
                               for k,v in six.iteritems(results['links'])])
        results['links'] = unquoted_links
        return results

    def reload(self):
        """Reloads the entity."""
        self.post("_reload")
        return self

    def acl_update(self, **kwargs):
        """To update Access Control List (ACL) properties for an endpoint.

        :param kwargs: Additional entity-specific arguments (required).

            - "owner" (``string``): The Splunk username, such as "admin". A value of "nobody" means no specific user (required).

            - "sharing" (``string``): A mode that indicates how the resource is shared. The sharing mode can be "user", "app", "global", or "system" (required).

        :type kwargs: ``dict``

        **Example**::

            import splunklib.client as client
            service = client.connect(...)
            saved_search = service.saved_searches["name"]
            saved_search.acl_update(sharing="app", owner="nobody", app="search", **{"perms.read": "admin, nobody"})
        """
        if "body" not in kwargs:
            kwargs = {"body": kwargs}

        if "sharing" not in kwargs["body"]:
            raise ValueError("Required argument 'sharing' is missing.")
        if "owner" not in kwargs["body"]:
            raise ValueError("Required argument 'owner' is missing.")

        self.post("acl", **kwargs)
        self.refresh()
        return self

    @property
    def state(self):
        """Returns the entity's state record.

        :return: A ``dict`` containing fields and metadata for the entity.
        """
        if self._state is None: self.refresh()
        return self._state

    def update(self, **kwargs):
        """Updates the server with any changes you've made to the current entity
        along with any additional arguments you specify.

            **Note**: You cannot update the ``name`` field of an entity.

        Many of the fields in the REST API are not valid Python
        identifiers, which means you cannot pass them as keyword
        arguments. That is, Python will fail to parse the following::

            # This fails
            x.update(check-new=False, email.to='boris@utopia.net')

        However, you can always explicitly use a dictionary to pass
        such keys::

            # This works
            x.update(**{'check-new': False, 'email.to': 'boris@utopia.net'})

        :param kwargs: Additional entity-specific arguments (optional).
        :type kwargs: ``dict``

        :return: The entity this method is called on.
        :rtype: class:`Entity`
        """
        # The peculiarity in question: the REST API creates a new
        # Entity if we pass name in the dictionary, instead of the
        # expected behavior of updating this Entity. Therefore we
        # check for 'name' in kwargs and throw an error if it is
        # there.
        if 'name' in kwargs:
            raise IllegalOperationException('Cannot update the name of an Entity via the REST API.')
        self.post(**kwargs)
        return self


class ReadOnlyCollection(Endpoint):
    """This class represents a read-only collection of entities in the Splunk
    instance.
    """
    def __init__(self, service, path, item=Entity):
        Endpoint.__init__(self, service, path)
        self.item = item # Item accessor
        self.null_count = -1

    def __contains__(self, name):
        """Is there at least one entry called *name* in this collection?

        Makes a single roundtrip to the server, plus at most two more
        if
        the ``autologin`` field of :func:`connect` is set to ``True``.
        """
        try:
            self[name]
            return True
        except KeyError:
            return False
        except AmbiguousReferenceException:
            return True

    def __getitem__(self, key):
        """Fetch an item named *key* from this collection.

        A name is not a unique identifier in a collection. The unique
        identifier is a name plus a namespace. For example, there can
        be a saved search named ``'mysearch'`` with sharing ``'app'``
        in application ``'search'``, and another with sharing
        ``'user'`` with owner ``'boris'`` and application
        ``'search'``. If the ``Collection`` is attached to a
        ``Service`` that has ``'-'`` (wildcard) as user and app in its
        namespace, then both of these may be visible under the same
        name.

        Where there is no conflict, ``__getitem__`` will fetch the
        entity given just the name. If there is a conflict and you
        pass just a name, it will raise a ``ValueError``. In that
        case, add the namespace as a second argument.

        This function makes a single roundtrip to the server, plus at
        most two additional round trips if
        the ``autologin`` field of :func:`connect` is set to ``True``.

        :param key: The name to fetch, or a tuple (name, namespace).
        :return: An :class:`Entity` object.
        :raises KeyError: Raised if *key* does not exist.
        :raises ValueError: Raised if no namespace is specified and *key*
                            does not refer to a unique name.

        **Example**::

            s = client.connect(...)
            saved_searches = s.saved_searches
            x1 = saved_searches.create(
                'mysearch', 'search * | head 1',
                owner='admin', app='search', sharing='app')
            x2 = saved_searches.create(
                'mysearch', 'search * | head 1',
                owner='admin', app='search', sharing='user')
            # Raises ValueError:
            saved_searches['mysearch']
            # Fetches x1
            saved_searches[
                'mysearch',
                client.namespace(sharing='app', app='search')]
            # Fetches x2
            saved_searches[
                'mysearch',
                client.namespace(sharing='user', owner='boris', app='search')]
        """
        try:
            if isinstance(key, tuple) and len(key) == 2:
                # x[a,b] is translated to x.__getitem__( (a,b) ), so we
                # have to extract values out.
                key, ns = key
                key = UrlEncoded(key, encode_slash=True)
                response = self.get(key, owner=ns.owner, app=ns.app)
            else:
                key = UrlEncoded(key, encode_slash=True)
                response = self.get(key)
            entries = self._load_list(response)
            if len(entries) > 1:
                raise AmbiguousReferenceException("Found multiple entities named '%s'; please specify a namespace." % key)
            elif len(entries) == 0:
                raise KeyError(key)
            else:
                return entries[0]
        except HTTPError as he:
            if he.status == 404: # No entity matching key and namespace.
                raise KeyError(key)
            else:
                raise

    def __iter__(self, **kwargs):
        """Iterate over the entities in the collection.

        :param kwargs: Additional arguments.
        :type kwargs: ``dict``
        :rtype: iterator over entities.

        Implemented to give Collection a listish interface. This
        function always makes a roundtrip to the server, plus at most
        two additional round trips if
        the ``autologin`` field of :func:`connect` is set to ``True``.

        **Example**::

            import splunklib.client as client
            c = client.connect(...)
            saved_searches = c.saved_searches
            for entity in saved_searches:
                print "Saved search named %s" % entity.name
        """

        for item in self.iter(**kwargs):
            yield item

    def __len__(self):
        """Enable ``len(...)`` for ``Collection`` objects.

        Implemented for consistency with a listish interface. No
        further failure modes beyond those possible for any method on
        an Endpoint.

        This function always makes a round trip to the server, plus at
        most two additional round trips if
        the ``autologin`` field of :func:`connect` is set to ``True``.

        **Example**::

            import splunklib.client as client
            c = client.connect(...)
            saved_searches = c.saved_searches
            n = len(saved_searches)
        """
        return len(self.list())

    def _entity_path(self, state):
        """Calculate the path to an entity to be returned.

        *state* should be the dictionary returned by
        :func:`_parse_atom_entry`. :func:`_entity_path` extracts the
        link to this entity from *state*, and strips all the namespace
        prefixes from it to leave only the relative path of the entity
        itself, sans namespace.

        :rtype: ``string``
        :return: an absolute path
        """
        # This has been factored out so that it can be easily
        # overloaded by Configurations, which has to switch its
        # entities' endpoints from its own properties/ to configs/.
        raw_path = urllib.parse.unquote(state.links.alternate)
        if 'servicesNS/' in raw_path:
            return _trailing(raw_path, 'servicesNS/', '/', '/')
        elif 'services/' in raw_path:
            return _trailing(raw_path, 'services/')
        else:
            return raw_path

    def _load_list(self, response):
        """Converts *response* to a list of entities.

        *response* is assumed to be a :class:`Record` containing an
        HTTP response, of the form::

            {'status': 200,
             'headers': [('content-length', '232642'),
                         ('expires', 'Fri, 30 Oct 1998 00:00:00 GMT'),
                         ('server', 'Splunkd'),
                         ('connection', 'close'),
                         ('cache-control', 'no-store, max-age=0, must-revalidate, no-cache'),
                         ('date', 'Tue, 29 May 2012 15:27:08 GMT'),
                         ('content-type', 'text/xml; charset=utf-8')],
             'reason': 'OK',
             'body': ...a stream implementing .read()...}

        The ``'body'`` key refers to a stream containing an Atom feed,
        that is, an XML document with a toplevel element ``<feed>``,
        and within that element one or more ``<entry>`` elements.
        """
        # Some subclasses of Collection have to override this because
        # splunkd returns something that doesn't match
        # <feed><entry></entry><feed>.
        entries = _load_atom_entries(response)
        if entries is None: return []
        entities = []
        for entry in entries:
            state = _parse_atom_entry(entry)
            entity = self.item(
                self.service,
                self._entity_path(state),
                state=state)
            entities.append(entity)

        return entities

    def itemmeta(self):
        """Returns metadata for members of the collection.

        Makes a single roundtrip to the server, plus two more at most if
        the ``autologin`` field of :func:`connect` is set to ``True``.

        :return: A :class:`splunklib.data.Record` object containing the metadata.

        **Example**::

            import splunklib.client as client
            import pprint
            s = client.connect(...)
            pprint.pprint(s.apps.itemmeta())
            {'access': {'app': 'search',
                                    'can_change_perms': '1',
                                    'can_list': '1',
                                    'can_share_app': '1',
                                    'can_share_global': '1',
                                    'can_share_user': '1',
                                    'can_write': '1',
                                    'modifiable': '1',
                                    'owner': 'admin',
                                    'perms': {'read': ['*'], 'write': ['admin']},
                                    'removable': '0',
                                    'sharing': 'user'},
             'fields': {'optional': ['author',
                                        'configured',
                                        'description',
                                        'label',
                                        'manageable',
                                        'template',
                                        'visible'],
                                        'required': ['name'], 'wildcard': []}}
        """
        response = self.get("_new")
        content = _load_atom(response, MATCH_ENTRY_CONTENT)
        return _parse_atom_metadata(content)

    def iter(self, offset=0, count=None, pagesize=None, **kwargs):
        """Iterates over the collection.

        This method is equivalent to the :meth:`list` method, but
        it returns an iterator and can load a certain number of entities at a
        time from the server.

        :param offset: The index of the first entity to return (optional).
        :type offset: ``integer``
        :param count: The maximum number of entities to return (optional).
        :type count: ``integer``
        :param pagesize: The number of entities to load (optional).
        :type pagesize: ``integer``
        :param kwargs: Additional arguments (optional):

            - "search" (``string``): The search query to filter responses.

            - "sort_dir" (``string``): The direction to sort returned items:
              "asc" or "desc".

            - "sort_key" (``string``): The field to use for sorting (optional).

            - "sort_mode" (``string``): The collating sequence for sorting
              returned items: "auto", "alpha", "alpha_case", or "num".

        :type kwargs: ``dict``

        **Example**::

            import splunklib.client as client
            s = client.connect(...)
            for saved_search in s.saved_searches.iter(pagesize=10):
                # Loads 10 saved searches at a time from the
                # server.
                ...
        """
        assert pagesize is None or pagesize > 0
        if count is None:
            count = self.null_count
        fetched = 0
        while count == self.null_count or fetched < count:
            response = self.get(count=pagesize or count, offset=offset, **kwargs)
            items = self._load_list(response)
            N = len(items)
            fetched += N
            for item in items:
                yield item
            if pagesize is None or N < pagesize:
                break
            offset += N
            logger.debug("pagesize=%d, fetched=%d, offset=%d, N=%d, kwargs=%s", pagesize, fetched, offset, N, kwargs)

    # kwargs: count, offset, search, sort_dir, sort_key, sort_mode
    def list(self, count=None, **kwargs):
        """Retrieves a list of entities in this collection.

        The entire collection is loaded at once and is returned as a list. This
        function makes a single roundtrip to the server, plus at most two more if
        the ``autologin`` field of :func:`connect` is set to ``True``.
        There is no caching--every call makes at least one round trip.

        :param count: The maximum number of entities to return (optional).
        :type count: ``integer``
        :param kwargs: Additional arguments (optional):

            - "offset" (``integer``): The offset of the first item to return.

            - "search" (``string``): The search query to filter responses.

            - "sort_dir" (``string``): The direction to sort returned items:
              "asc" or "desc".

            - "sort_key" (``string``): The field to use for sorting (optional).

            - "sort_mode" (``string``): The collating sequence for sorting
              returned items: "auto", "alpha", "alpha_case", or "num".

        :type kwargs: ``dict``
        :return: A ``list`` of entities.
        """
        # response = self.get(count=count, **kwargs)
        # return self._load_list(response)
        return list(self.iter(count=count, **kwargs))




class Collection(ReadOnlyCollection):
    """A collection of entities.

    Splunk provides a number of different collections of distinct
    entity types: applications, saved searches, fired alerts, and a
    number of others. Each particular type is available separately
    from the Splunk instance, and the entities of that type are
    returned in a :class:`Collection`.

    The interface for :class:`Collection` does not quite match either
    ``list`` or ``dict`` in Python, because there are enough semantic
    mismatches with either to make its behavior surprising. A unique
    element in a :class:`Collection` is defined by a string giving its
    name plus namespace (although the namespace is optional if the name is
    unique).

    **Example**::

        import splunklib.client as client
        service = client.connect(...)
        mycollection = service.saved_searches
        mysearch = mycollection['my_search', client.namespace(owner='boris', app='natasha', sharing='user')]
        # Or if there is only one search visible named 'my_search'
        mysearch = mycollection['my_search']

    Similarly, ``name`` in ``mycollection`` works as you might expect (though
    you cannot currently pass a namespace to the ``in`` operator), as does
    ``len(mycollection)``.

    However, as an aggregate, :class:`Collection` behaves more like a
    list. If you iterate over a :class:`Collection`, you get an
    iterator over the entities, not the names and namespaces.

    **Example**::

        for entity in mycollection:
            assert isinstance(entity, client.Entity)

    Use the :meth:`create` and :meth:`delete` methods to create and delete
    entities in this collection. To view the access control list and other
    metadata of the collection, use the :meth:`ReadOnlyCollection.itemmeta` method.

    :class:`Collection` does no caching. Each call makes at least one
    round trip to the server to fetch data.
    """

    def create(self, name, **params):
        """Creates a new entity in this collection.

        This function makes either one or two roundtrips to the
        server, depending on the type of entities in this
        collection, plus at most two more if
        the ``autologin`` field of :func:`connect` is set to ``True``.

        :param name: The name of the entity to create.
        :type name: ``string``
        :param namespace: A namespace, as created by the :func:`splunklib.binding.namespace`
            function (optional).  You can also set ``owner``, ``app``, and
            ``sharing`` in ``params``.
        :type namespace: A :class:`splunklib.data.Record` object with keys ``owner``, ``app``,
            and ``sharing``.
        :param params: Additional entity-specific arguments (optional).
        :type params: ``dict``
        :return: The new entity.
        :rtype: A subclass of :class:`Entity`, chosen by :meth:`Collection.self.item`.

        **Example**::

            import splunklib.client as client
            s = client.connect(...)
            applications = s.apps
            new_app = applications.create("my_fake_app")
        """
        if not isinstance(name, six.string_types):
            raise InvalidNameException("%s is not a valid name for an entity." % name)
        if 'namespace' in params:
            namespace = params.pop('namespace')
            params['owner'] = namespace.owner
            params['app'] = namespace.app
            params['sharing'] = namespace.sharing
        response = self.post(name=name, **params)
        atom = _load_atom(response, XNAME_ENTRY)
        if atom is None:
            # This endpoint doesn't return the content of the new
            # item. We have to go fetch it ourselves.
            return self[name]
        else:
            entry = atom.entry
            state = _parse_atom_entry(entry)
            entity = self.item(
                self.service,
                self._entity_path(state),
                state=state)
            return entity

    def delete(self, name, **params):
        """Deletes a specified entity from the collection.

        :param name: The name of the entity to delete.
        :type name: ``string``
        :return: The collection.
        :rtype: ``self``

        This method is implemented for consistency with the REST API's DELETE
        method.

        If there is no *name* entity on the server, a ``KeyError`` is
        thrown. This function always makes a roundtrip to the server.

        **Example**::

            import splunklib.client as client
            c = client.connect(...)
            saved_searches = c.saved_searches
            saved_searches.create('my_saved_search',
                                  'search * | head 1')
            assert 'my_saved_search' in saved_searches
            saved_searches.delete('my_saved_search')
            assert 'my_saved_search' not in saved_searches
        """
        name = UrlEncoded(name, encode_slash=True)
        if 'namespace' in params:
            namespace = params.pop('namespace')
            params['owner'] = namespace.owner
            params['app'] = namespace.app
            params['sharing'] = namespace.sharing
        try:
            self.service.delete(_path(self.path, name), **params)
        except HTTPError as he:
            # An HTTPError with status code 404 means that the entity
            # has already been deleted, and we reraise it as a
            # KeyError.
            if he.status == 404:
                raise KeyError("No such entity %s" % name)
            else:
                raise
        return self

    def get(self, name="", owner=None, app=None, sharing=None, **query):
        """Performs a GET request to the server on the collection.

        If *owner*, *app*, and *sharing* are omitted, this method takes a
        default namespace from the :class:`Service` object for this :class:`Endpoint`.
        All other keyword arguments are included in the URL as query parameters.

        :raises AuthenticationError: Raised when the ``Service`` is not logged in.
        :raises HTTPError: Raised when an error in the request occurs.
        :param path_segment: A path segment relative to this endpoint.
        :type path_segment: ``string``
        :param owner: The owner context of the namespace (optional).
        :type owner: ``string``
        :param app: The app context of the namespace (optional).
        :type app: ``string``
        :param sharing: The sharing mode for the namespace (optional).
        :type sharing: "global", "system", "app", or "user"
        :param query: All other keyword arguments, which are used as query
            parameters.
        :type query: ``string``
        :return: The response from the server.
        :rtype: ``dict`` with keys ``body``, ``headers``, ``reason``,
                and ``status``

        **Example**::

            import splunklib.client
            s = client.service(...)
            saved_searches = s.saved_searches
            saved_searches.get("my/saved/search") == \\
                {'body': ...a response reader object...,
                 'headers': [('content-length', '26208'),
                             ('expires', 'Fri, 30 Oct 1998 00:00:00 GMT'),
                             ('server', 'Splunkd'),
                             ('connection', 'close'),
                             ('cache-control', 'no-store, max-age=0, must-revalidate, no-cache'),
                             ('date', 'Fri, 11 May 2012 16:30:35 GMT'),
                             ('content-type', 'text/xml; charset=utf-8')],
                 'reason': 'OK',
                 'status': 200}
            saved_searches.get('nonexistant/search') # raises HTTPError
            s.logout()
            saved_searches.get() # raises AuthenticationError

        """
        name = UrlEncoded(name, encode_slash=True)
        return super(Collection, self).get(name, owner, app, sharing, **query)




class ConfigurationFile(Collection):
    """This class contains all of the stanzas from one configuration file.
    """
    # __init__'s arguments must match those of an Entity, not a
    # Collection, since it is being created as the elements of a
    # Configurations, which is a Collection subclass.
    def __init__(self, service, path, **kwargs):
        Collection.__init__(self, service, path, item=Stanza)
        self.name = kwargs['state']['title']


class Configurations(Collection):
    """This class provides access to the configuration files from this Splunk
    instance. Retrieve this collection using :meth:`Service.confs`.

    Splunk's configuration is divided into files, and each file into
    stanzas. This collection is unusual in that the values in it are
    themselves collections of :class:`ConfigurationFile` objects.
    """
    def __init__(self, service):
        Collection.__init__(self, service, PATH_PROPERTIES, item=ConfigurationFile)
        if self.service.namespace.owner == '-' or self.service.namespace.app == '-':
            raise ValueError("Configurations cannot have wildcards in namespace.")

    def __getitem__(self, key):
        # The superclass implementation is designed for collections that contain
        # entities. This collection (Configurations) contains collections
        # (ConfigurationFile).
        #
        # The configurations endpoint returns multiple entities when we ask for a single file.
        # This screws up the default implementation of __getitem__ from Collection, which thinks
        # that multiple entities means a name collision, so we have to override it here.
        try:
            response = self.get(key)
            return ConfigurationFile(self.service, PATH_CONF % key, state={'title': key})
        except HTTPError as he:
            if he.status == 404: # No entity matching key
                raise KeyError(key)
            else:
                raise

    def __contains__(self, key):
        # configs/conf-{name} never returns a 404. We have to post to properties/{name}
        # in order to find out if a configuration exists.
        try:
            response = self.get(key)
            return True
        except HTTPError as he:
            if he.status == 404: # No entity matching key
                return False
            else:
                raise

    def create(self, name):
        """ Creates a configuration file named *name*.

        If there is already a configuration file with that name,
        the existing file is returned.

        :param name: The name of the configuration file.
        :type name: ``string``

        :return: The :class:`ConfigurationFile` object.
        """
        # This has to be overridden to handle the plumbing of creating
        # a ConfigurationFile (which is a Collection) instead of some
        # Entity.
        if not isinstance(name, six.string_types):
            raise ValueError("Invalid name: %s" % repr(name))
        response = self.post(__conf=name)
        if response.status == 303:
            return self[name]
        elif response.status == 201:
            return ConfigurationFile(self.service, PATH_CONF % name, item=Stanza, state={'title': name})
        else:
            raise ValueError("Unexpected status code %s returned from creating a stanza" % response.status)

    def delete(self, key):
        """Raises `IllegalOperationException`."""
        raise IllegalOperationException("Cannot delete configuration files from the REST API.")

    def _entity_path(self, state):
        # Overridden to make all the ConfigurationFile objects
        # returned refer to the configs/ path instead of the
        # properties/ path used by Configrations.
        return PATH_CONF % state['title']


class Stanza(Entity):
    """This class contains a single configuration stanza."""

    def submit(self, stanza):
        """Adds keys to the current configuration stanza as a
        dictionary of key-value pairs.

        :param stanza: A dictionary of key-value pairs for the stanza.
        :type stanza: ``dict``
        :return: The :class:`Stanza` object.
        """
        body = _encode(**stanza)
        self.service.post(self.path, body=body)
        return self

    def __len__(self):
        # The stanza endpoint returns all the keys at the same level in the XML as the eai information
        # and 'disabled', so to get an accurate length, we have to filter those out and have just
        # the stanza keys.
        return len([x for x in self._state.content.keys()
                    if not x.startswith('eai') and x != 'disabled'])


class StoragePassword(Entity):
    """This class contains a storage password.
    """
    def __init__(self, service, path, **kwargs):
        state = kwargs.get('state', None)
        kwargs['skip_refresh'] = kwargs.get('skip_refresh', state is not None)
        super(StoragePassword, self).__init__(service, path, **kwargs)
        self._state = state

    @property
    def clear_password(self):
        return self.content.get('clear_password')

    @property
    def encrypted_password(self):
        return self.content.get('encr_password')

    @property
    def realm(self):
        return self.content.get('realm')

    @property
    def username(self):
        return self.content.get('username')


class StoragePasswords(Collection):
    """This class provides access to the storage passwords from this Splunk
    instance. Retrieve this collection using :meth:`Service.storage_passwords`.
    """
    def __init__(self, service):
        super(StoragePasswords, self).__init__(service, PATH_STORAGE_PASSWORDS, item=StoragePassword)

    def create(self, password, username, realm=None):
        """ Creates a storage password.

        A `StoragePassword` can be identified by <username>, or by <realm>:<username> if the
        optional realm parameter is also provided.

        :param password: The password for the credentials - this is the only part of the credentials that will be stored securely.
        :type name: ``string``
        :param username: The username for the credentials.
        :type name: ``string``
        :param realm: The credential realm. (optional)
        :type name: ``string``

        :return: The :class:`StoragePassword` object created.
        """
        if self.service.namespace.owner == '-' or self.service.namespace.app == '-':
            raise ValueError("While creating StoragePasswords, namespace cannot have wildcards.")

        if not isinstance(username, six.string_types):
            raise ValueError("Invalid name: %s" % repr(username))

        if realm is None:
            response = self.post(password=password, name=username)
        else:
            response = self.post(password=password, realm=realm, name=username)

        if response.status != 201:
            raise ValueError("Unexpected status code %s returned from creating a stanza" % response.status)

        entries = _load_atom_entries(response)
        state = _parse_atom_entry(entries[0])
        storage_password = StoragePassword(self.service, self._entity_path(state), state=state, skip_refresh=True)

        return storage_password

    def delete(self, username, realm=None):
        """Delete a storage password by username and/or realm.

        The identifier can be passed in through the username parameter as
        <username> or <realm>:<username>, but the preferred way is by
        passing in the username and realm parameters.

        :param username: The username for the credentials, or <realm>:<username> if the realm parameter is omitted.
        :type name: ``string``
        :param realm: The credential realm. (optional)
        :type name: ``string``
        :return: The `StoragePassword` collection.
        :rtype: ``self``
        """
        if self.service.namespace.owner == '-' or self.service.namespace.app == '-':
            raise ValueError("app context must be specified when removing a password.")

        if realm is None:
            # This case makes the username optional, so
            # the full name can be passed in as realm.
            # Assume it's already encoded.
            name = username
        else:
            # Encode each component separately
            name = UrlEncoded(realm, encode_slash=True) + ":" + UrlEncoded(username, encode_slash=True)

        # Append the : expected at the end of the name
        if name[-1] != ":":
            name = name + ":"
        return Collection.delete(self, name)


class AlertGroup(Entity):
    """This class represents a group of fired alerts for a saved search. Access
    it using the :meth:`alerts` property."""
    def __init__(self, service, path, **kwargs):
        Entity.__init__(self, service, path, **kwargs)

    def __len__(self):
        return self.count

    @property
    def alerts(self):
        """Returns a collection of triggered alerts.

        :return: A :class:`Collection` of triggered alerts.
        """
        return Collection(self.service, self.path)

    @property
    def count(self):
        """Returns the count of triggered alerts.

        :return: The triggered alert count.
        :rtype: ``integer``
        """
        return int(self.content.get('triggered_alert_count', 0))


class Indexes(Collection):
    """This class contains the collection of indexes in this Splunk instance.
    Retrieve this collection using :meth:`Service.indexes`.
    """
    def get_default(self):
        """ Returns the name of the default index.

        :return: The name of the default index.

        """
        index = self['_audit']
        return index['defaultDatabase']

    def delete(self, name):
        """ Deletes a given index.

        **Note**: This method is only supported in Splunk 5.0 and later.

        :param name: The name of the index to delete.
        :type name: ``string``
        """
        if self.service.splunk_version >= (5,):
            Collection.delete(self, name)
        else:
            raise IllegalOperationException("Deleting indexes via the REST API is "
                                            "not supported before Splunk version 5.")


class Index(Entity):
    """This class represents an index and provides different operations, such as
    cleaning the index, writing to the index, and so forth."""
    def __init__(self, service, path, **kwargs):
        Entity.__init__(self, service, path, **kwargs)

    def attach(self, host=None, source=None, sourcetype=None):
        """Opens a stream (a writable socket) for writing events to the index.

        :param host: The host value for events written to the stream.
        :type host: ``string``
        :param source: The source value for events written to the stream.
        :type source: ``string``
        :param sourcetype: The sourcetype value for events written to the
            stream.
        :type sourcetype: ``string``

        :return: A writable socket.
        """
        args = { 'index': self.name }
        if host is not None: args['host'] = host
        if source is not None: args['source'] = source
        if sourcetype is not None: args['sourcetype'] = sourcetype
        path = UrlEncoded(PATH_RECEIVERS_STREAM + "?" + urllib.parse.urlencode(args), skip_encode=True)

        cookie_or_auth_header = "Authorization: Splunk %s\r\n" % \
                                (self.service.token if self.service.token is _NoAuthenticationToken
                                 else self.service.token.replace("Splunk ", ""))

        # If we have cookie(s), use them instead of "Authorization: ..."
        if self.service.has_cookies():
            cookie_or_auth_header = "Cookie: %s\r\n" % _make_cookie_header(self.service.get_cookies().items())

        # Since we need to stream to the index connection, we have to keep
        # the connection open and use the Splunk extension headers to note
        # the input mode
        sock = self.service.connect()
        headers = [("POST %s HTTP/1.1\r\n" % str(self.service._abspath(path))).encode('utf-8'),
                   ("Host: %s:%s\r\n" % (self.service.host, int(self.service.port))).encode('utf-8'),
                   b"Accept-Encoding: identity\r\n",
                   cookie_or_auth_header.encode('utf-8'),
                   b"X-Splunk-Input-Mode: Streaming\r\n",
                   b"\r\n"]

        for h in headers:
            sock.write(h)
        return sock

    @contextlib.contextmanager
    def attached_socket(self, *args, **kwargs):
        """Opens a raw socket in a ``with`` block to write data to Splunk.

        The arguments are identical to those for :meth:`attach`. The socket is
        automatically closed at the end of the ``with`` block, even if an
        exception is raised in the block.

        :param host: The host value for events written to the stream.
        :type host: ``string``
        :param source: The source value for events written to the stream.
        :type source: ``string``
        :param sourcetype: The sourcetype value for events written to the
            stream.
        :type sourcetype: ``string``

        :returns: Nothing.

        **Example**::

            import splunklib.client as client
            s = client.connect(...)
            index = s.indexes['some_index']
            with index.attached_socket(sourcetype='test') as sock:
                sock.send('Test event\\r\\n')

        """
        try:
            sock = self.attach(*args, **kwargs)
            yield sock
        finally:
            sock.shutdown(socket.SHUT_RDWR)
            sock.close()

    def clean(self, timeout=60):
        """Deletes the contents of the index.

        This method blocks until the index is empty, because it needs to restore
        values at the end of the operation.

        :param timeout: The time-out period for the operation, in seconds (the
            default is 60).
        :type timeout: ``integer``

        :return: The :class:`Index`.
        """
        self.refresh()

        tds = self['maxTotalDataSizeMB']
        ftp = self['frozenTimePeriodInSecs']
        was_disabled_initially = self.disabled
        try:
            if (not was_disabled_initially and \
                self.service.splunk_version < (5,)):
                # Need to disable the index first on Splunk 4.x,
                # but it doesn't work to disable it on 5.0.
                self.disable()
            self.update(maxTotalDataSizeMB=1, frozenTimePeriodInSecs=1)
            self.roll_hot_buckets()

            # Wait until event count goes to 0.
            start = datetime.now()
            diff = timedelta(seconds=timeout)
            while self.content.totalEventCount != '0' and datetime.now() < start+diff:
                sleep(1)
                self.refresh()

            if self.content.totalEventCount != '0':
                raise OperationError("Cleaning index %s took longer than %s seconds; timing out." % (self.name, timeout))
        finally:
            # Restore original values
            self.update(maxTotalDataSizeMB=tds, frozenTimePeriodInSecs=ftp)
            if (not was_disabled_initially and \
                self.service.splunk_version < (5,)):
                # Re-enable the index if it was originally enabled and we messed with it.
                self.enable()

        return self

    def roll_hot_buckets(self):
        """Performs rolling hot buckets for this index.

        :return: The :class:`Index`.
        """
        self.post("roll-hot-buckets")
        return self

    def submit(self, event, host=None, source=None, sourcetype=None):
        """Submits a single event to the index using ``HTTP POST``.

        :param event: The event to submit.
        :type event: ``string``
        :param `host`: The host value of the event.
        :type host: ``string``
        :param `source`: The source value of the event.
        :type source: ``string``
        :param `sourcetype`: The sourcetype value of the event.
        :type sourcetype: ``string``

        :return: The :class:`Index`.
        """
        args = { 'index': self.name }
        if host is not None: args['host'] = host
        if source is not None: args['source'] = source
        if sourcetype is not None: args['sourcetype'] = sourcetype

        self.service.post(PATH_RECEIVERS_SIMPLE, body=event, **args)
        return self

    # kwargs: host, host_regex, host_segment, rename-source, sourcetype
    def upload(self, filename, **kwargs):
        """Uploads a file for immediate indexing.

        **Note**: The file must be locally accessible from the server.

        :param filename: The name of the file to upload. The file can be a
            plain, compressed, or archived file.
        :type filename: ``string``
        :param kwargs: Additional arguments (optional). For more about the
            available parameters, see `Index parameters <http://dev.splunk.com/view/SP-CAAAEE6#indexparams>`_ on Splunk Developer Portal.
        :type kwargs: ``dict``

        :return: The :class:`Index`.
        """
        kwargs['index'] = self.name
        path = 'data/inputs/oneshot'
        self.service.post(path, name=filename, **kwargs)
        return self


class Input(Entity):
    """This class represents a Splunk input. This class is the base for all
    typed input classes and is also used when the client does not recognize an
    input kind.
    """
    def __init__(self, service, path, kind=None, **kwargs):
        # kind can be omitted (in which case it is inferred from the path)
        # Otherwise, valid values are the paths from data/inputs ("udp",
        # "monitor", "tcp/raw"), or two special cases: "tcp" (which is "tcp/raw")
        # and "splunktcp" (which is "tcp/cooked").
        Entity.__init__(self, service, path, **kwargs)
        if kind is None:
            path_segments = path.split('/')
            i = path_segments.index('inputs') + 1
            if path_segments[i] == 'tcp':
                self.kind = path_segments[i] + '/' + path_segments[i+1]
            else:
                self.kind = path_segments[i]
        else:
            self.kind = kind

        # Handle old input kind names.
        if self.kind == 'tcp':
            self.kind = 'tcp/raw'
        if self.kind == 'splunktcp':
            self.kind = 'tcp/cooked'

    def update(self, **kwargs):
        """Updates the server with any changes you've made to the current input
        along with any additional arguments you specify.

        :param kwargs: Additional arguments (optional). For more about the
            available parameters, see `Input parameters <http://dev.splunk.com/view/SP-CAAAEE6#inputparams>`_ on Splunk Developer Portal.
        :type kwargs: ``dict``

        :return: The input this method was called on.
        :rtype: class:`Input`
        """
        # UDP and TCP inputs require special handling due to their restrictToHost
        # field. For all other inputs kinds, we can dispatch to the superclass method.
        if self.kind not in ['tcp', 'splunktcp', 'tcp/raw', 'tcp/cooked', 'udp']:
            return super(Input, self).update(**kwargs)
        else:
            # The behavior of restrictToHost is inconsistent across input kinds and versions of Splunk.
            # In Splunk 4.x, the name of the entity is only the port, independent of the value of
            # restrictToHost. In Splunk 5.0 this changed so the name will be of the form <restrictToHost>:<port>.
            # In 5.0 and 5.0.1, if you don't supply the restrictToHost value on every update, it will
            # remove the host restriction from the input. As of 5.0.2 you simply can't change restrictToHost
            # on an existing input.

            # The logic to handle all these cases:
            # - Throw an exception if the user tries to set restrictToHost on an existing input
            #   for *any* version of Splunk.
            # - Set the existing restrictToHost value on the update args internally so we don't
            #   cause it to change in Splunk 5.0 and 5.0.1.
            to_update = kwargs.copy()

            if 'restrictToHost' in kwargs:
                raise IllegalOperationException("Cannot set restrictToHost on an existing input with the SDK.")
            elif 'restrictToHost' in self._state.content and self.kind != 'udp':
                to_update['restrictToHost'] = self._state.content['restrictToHost']

            # Do the actual update operation.
            return super(Input, self).update(**to_update)


# Inputs is a "kinded" collection, which is a heterogenous collection where
# each item is tagged with a kind, that provides a single merged view of all
# input kinds.
class Inputs(Collection):
    """This class represents a collection of inputs. The collection is
    heterogeneous and each member of the collection contains a *kind* property
    that indicates the specific type of input.
    Retrieve this collection using :meth:`Service.inputs`."""

    def __init__(self, service, kindmap=None):
        Collection.__init__(self, service, PATH_INPUTS, item=Input)

    def __getitem__(self, key):
        # The key needed to retrieve the input needs it's parenthesis to be URL encoded
        # based on the REST API for input
        # <http://docs.splunk.com/Documentation/Splunk/latest/RESTAPI/RESTinput>
        if isinstance(key, tuple) and len(key) == 2:
            # Fetch a single kind
            key, kind = key
            key = UrlEncoded(key, encode_slash=True)
            try:
                response = self.get(self.kindpath(kind) + "/" + key)
                entries = self._load_list(response)
                if len(entries) > 1:
                    raise AmbiguousReferenceException("Found multiple inputs of kind %s named %s." % (kind, key))
                elif len(entries) == 0:
                    raise KeyError((key, kind))
                else:
                    return entries[0]
            except HTTPError as he:
                if he.status == 404: # No entity matching kind and key
                    raise KeyError((key, kind))
                else:
                    raise
        else:
            # Iterate over all the kinds looking for matches.
            kind = None
            candidate = None
            key = UrlEncoded(key, encode_slash=True)
            for kind in self.kinds:
                try:
                    response = self.get(kind + "/" + key)
                    entries = self._load_list(response)
                    if len(entries) > 1:
                        raise AmbiguousReferenceException("Found multiple inputs of kind %s named %s." % (kind, key))
                    elif len(entries) == 0:
                        pass
                    else:
                        if candidate is not None: # Already found at least one candidate
                            raise AmbiguousReferenceException("Found multiple inputs named %s, please specify a kind" % key)
                        candidate = entries[0]
                except HTTPError as he:
                    if he.status == 404:
                        pass # Just carry on to the next kind.
                    else:
                        raise
            if candidate is None:
                raise KeyError(key) # Never found a match.
            else:
                return candidate

    def __contains__(self, key):
        if isinstance(key, tuple) and len(key) == 2:
            # If we specify a kind, this will shortcut properly
            try:
                self.__getitem__(key)
                return True
            except KeyError:
                return False
        else:
            # Without a kind, we want to minimize the number of round trips to the server, so we
            # reimplement some of the behavior of __getitem__ in order to be able to stop searching
            # on the first hit.
            for kind in self.kinds:
                try:
                    response = self.get(self.kindpath(kind) + "/" + key)
                    entries = self._load_list(response)
                    if len(entries) > 0:
                        return True
                    else:
                        pass
                except HTTPError as he:
                    if he.status == 404:
                        pass # Just carry on to the next kind.
                    else:
                        raise
            return False

    def create(self, name, kind, **kwargs):
        """Creates an input of a specific kind in this collection, with any
        arguments you specify.

        :param `name`: The input name.
        :type name: ``string``
        :param `kind`: The kind of input:

            - "ad": Active Directory

            - "monitor": Files and directories

            - "registry": Windows Registry

            - "script": Scripts

            - "splunktcp": TCP, processed

            - "tcp": TCP, unprocessed

            - "udp": UDP

            - "win-event-log-collections": Windows event log

            - "win-perfmon": Performance monitoring

            - "win-wmi-collections": WMI

        :type kind: ``string``
        :param `kwargs`: Additional arguments (optional). For more about the
            available parameters, see `Input parameters <http://dev.splunk.com/view/SP-CAAAEE6#inputparams>`_ on Splunk Developer Portal.

        :type kwargs: ``dict``

        :return: The new :class:`Input`.
        """
        kindpath = self.kindpath(kind)
        self.post(kindpath, name=name, **kwargs)

        # If we created an input with restrictToHost set, then
        # its path will be <restrictToHost>:<name>, not just <name>,
        # and we have to adjust accordingly.

        # Url encodes the name of the entity.
        name = UrlEncoded(name, encode_slash=True)
        path = _path(
            self.path + kindpath,
            '%s:%s' % (kwargs['restrictToHost'], name) \
                if 'restrictToHost' in kwargs else name
                )
        return Input(self.service, path, kind)

    def delete(self, name, kind=None):
        """Removes an input from the collection.

        :param `kind`: The kind of input:

            - "ad": Active Directory

            - "monitor": Files and directories

            - "registry": Windows Registry

            - "script": Scripts

            - "splunktcp": TCP, processed

            - "tcp": TCP, unprocessed

            - "udp": UDP

            - "win-event-log-collections": Windows event log

            - "win-perfmon": Performance monitoring

            - "win-wmi-collections": WMI

        :type kind: ``string``
        :param name: The name of the input to remove.
        :type name: ``string``

        :return: The :class:`Inputs` collection.
        """
        if kind is None:
            self.service.delete(self[name].path)
        else:
            self.service.delete(self[name, kind].path)
        return self

    def itemmeta(self, kind):
        """Returns metadata for the members of a given kind.

        :param `kind`: The kind of input:

            - "ad": Active Directory

            - "monitor": Files and directories

            - "registry": Windows Registry

            - "script": Scripts

            - "splunktcp": TCP, processed

            - "tcp": TCP, unprocessed

            - "udp": UDP

            - "win-event-log-collections": Windows event log

            - "win-perfmon": Performance monitoring

            - "win-wmi-collections": WMI

        :type kind: ``string``

        :return: The metadata.
        :rtype: class:``splunklib.data.Record``
        """
        response = self.get("%s/_new" % self._kindmap[kind])
        content = _load_atom(response, MATCH_ENTRY_CONTENT)
        return _parse_atom_metadata(content)

    def _get_kind_list(self, subpath=None):
        if subpath is None:
            subpath = []

        kinds = []
        response = self.get('/'.join(subpath))
        content = _load_atom_entries(response)
        for entry in content:
            this_subpath = subpath + [entry.title]
            # The "all" endpoint doesn't work yet.
            # The "tcp/ssl" endpoint is not a real input collection.
            if entry.title == 'all' or this_subpath == ['tcp','ssl']:
                continue
            elif 'create' in [x.rel for x in entry.link]:
                path = '/'.join(subpath + [entry.title])
                kinds.append(path)
            else:
                subkinds = self._get_kind_list(subpath + [entry.title])
                kinds.extend(subkinds)
        return kinds

    @property
    def kinds(self):
        """Returns the input kinds on this Splunk instance.

        :return: The list of input kinds.
        :rtype: ``list``
        """
        return self._get_kind_list()

    def kindpath(self, kind):
        """Returns a path to the resources for a given input kind.

        :param `kind`: The kind of input:

            - "ad": Active Directory

            - "monitor": Files and directories

            - "registry": Windows Registry

            - "script": Scripts

            - "splunktcp": TCP, processed

            - "tcp": TCP, unprocessed

            - "udp": UDP

            - "win-event-log-collections": Windows event log

            - "win-perfmon": Performance monitoring

            - "win-wmi-collections": WMI

        :type kind: ``string``

        :return: The relative endpoint path.
        :rtype: ``string``
        """
        if kind == 'tcp':
            return UrlEncoded('tcp/raw', skip_encode=True)
        elif kind == 'splunktcp':
            return UrlEncoded('tcp/cooked', skip_encode=True)
        else:
            return UrlEncoded(kind, skip_encode=True)

    def list(self, *kinds, **kwargs):
        """Returns a list of inputs that are in the :class:`Inputs` collection.
        You can also filter by one or more input kinds.

        This function iterates over all possible inputs, regardless of any arguments you
        specify. Because the :class:`Inputs` collection is the union of all the inputs of each
        kind, this method implements parameters such as "count", "search", and so
        on at the Python level once all the data has been fetched. The exception
        is when you specify a single input kind, and then this method makes a single request
        with the usual semantics for parameters.

        :param kinds: The input kinds to return (optional).

            - "ad": Active Directory

            - "monitor": Files and directories

            - "registry": Windows Registry

            - "script": Scripts

            - "splunktcp": TCP, processed

            - "tcp": TCP, unprocessed

            - "udp": UDP

            - "win-event-log-collections": Windows event log

            - "win-perfmon": Performance monitoring

            - "win-wmi-collections": WMI

        :type kinds: ``string``
        :param kwargs: Additional arguments (optional):

            - "count" (``integer``): The maximum number of items to return.

            - "offset" (``integer``): The offset of the first item to return.

            - "search" (``string``): The search query to filter responses.

            - "sort_dir" (``string``): The direction to sort returned items:
              "asc" or "desc".

            - "sort_key" (``string``): The field to use for sorting (optional).

            - "sort_mode" (``string``): The collating sequence for sorting
              returned items: "auto", "alpha", "alpha_case", or "num".

        :type kwargs: ``dict``

        :return: A list of input kinds.
        :rtype: ``list``
        """
        if len(kinds) == 0:
            kinds = self.kinds
        if len(kinds) == 1:
            kind = kinds[0]
            logger.debug("Inputs.list taking short circuit branch for single kind.")
            path = self.kindpath(kind)
            logger.debug("Path for inputs: %s", path)
            try:
                path = UrlEncoded(path, skip_encode=True)
                response = self.get(path, **kwargs)
            except HTTPError as he:
                if he.status == 404: # No inputs of this kind
                    return []
            entities = []
            entries = _load_atom_entries(response)
            if entries is None:
                return [] # No inputs in a collection comes back with no feed or entry in the XML
            for entry in entries:
                state = _parse_atom_entry(entry)
                # Unquote the URL, since all URL encoded in the SDK
                # should be of type UrlEncoded, and all str should not
                # be URL encoded.
                path = urllib.parse.unquote(state.links.alternate)
                entity = Input(self.service, path, kind, state=state)
                entities.append(entity)
            return entities

        search = kwargs.get('search', '*')

        entities = []
        for kind in kinds:
            response = None
            try:
                kind = UrlEncoded(kind, skip_encode=True)
                response = self.get(self.kindpath(kind), search=search)
            except HTTPError as e:
                if e.status == 404:
                    continue # No inputs of this kind
                else:
                    raise

            entries = _load_atom_entries(response)
            if entries is None: continue # No inputs to process
            for entry in entries:
                state = _parse_atom_entry(entry)
                # Unquote the URL, since all URL encoded in the SDK
                # should be of type UrlEncoded, and all str should not
                # be URL encoded.
                path = urllib.parse.unquote(state.links.alternate)
                entity = Input(self.service, path, kind, state=state)
                entities.append(entity)
        if 'offset' in kwargs:
            entities = entities[kwargs['offset']:]
        if 'count' in kwargs:
            entities = entities[:kwargs['count']]
        if kwargs.get('sort_mode', None) == 'alpha':
            sort_field = kwargs.get('sort_field', 'name')
            if sort_field == 'name':
                f = lambda x: x.name.lower()
            else:
                f = lambda x: x[sort_field].lower()
            entities = sorted(entities, key=f)
        if kwargs.get('sort_mode', None) == 'alpha_case':
            sort_field = kwargs.get('sort_field', 'name')
            if sort_field == 'name':
                f = lambda x: x.name
            else:
                f = lambda x: x[sort_field]
            entities = sorted(entities, key=f)
        if kwargs.get('sort_dir', 'asc') == 'desc':
            entities = list(reversed(entities))
        return entities

    def __iter__(self, **kwargs):
        for item in self.iter(**kwargs):
            yield item

    def iter(self, **kwargs):
        """ Iterates over the collection of inputs.

        :param kwargs: Additional arguments (optional):

            - "count" (``integer``): The maximum number of items to return.

            - "offset" (``integer``): The offset of the first item to return.

            - "search" (``string``): The search query to filter responses.

            - "sort_dir" (``string``): The direction to sort returned items:
              "asc" or "desc".

            - "sort_key" (``string``): The field to use for sorting (optional).

            - "sort_mode" (``string``): The collating sequence for sorting
              returned items: "auto", "alpha", "alpha_case", or "num".

        :type kwargs: ``dict``
        """
        for item in self.list(**kwargs):
            yield item

    def oneshot(self, path, **kwargs):
        """ Creates a oneshot data input, which is an upload of a single file
        for one-time indexing.

        :param path: The path and filename.
        :type path: ``string``
        :param kwargs: Additional arguments (optional). For more about the
            available parameters, see `Input parameters <http://dev.splunk.com/view/SP-CAAAEE6#inputparams>`_ on Splunk Developer Portal.
        :type kwargs: ``dict``
        """
        self.post('oneshot', name=path, **kwargs)


class Job(Entity):
    """This class represents a search job."""
    def __init__(self, service, sid, **kwargs):
        # Default to v2 in Splunk Version 9+
        path = "{path}{sid}"
        # Formatting path based on the Splunk Version
        if service.disable_v2_api:
            path = path.format(path=PATH_JOBS, sid=sid)
        else:
            path = path.format(path=PATH_JOBS_V2, sid=sid)

        Entity.__init__(self, service, path, skip_refresh=True, **kwargs)
        self.sid = sid

    # The Job entry record is returned at the root of the response
    def _load_atom_entry(self, response):
        return _load_atom(response).entry

    def cancel(self):
        """Stops the current search and deletes the results cache.

        :return: The :class:`Job`.
        """
        try:
            self.post("control", action="cancel")
        except HTTPError as he:
            if he.status == 404:
                # The job has already been cancelled, so
                # cancelling it twice is a nop.
                pass
            else:
                raise
        return self

    def disable_preview(self):
        """Disables preview for this job.

        :return: The :class:`Job`.
        """
        self.post("control", action="disablepreview")
        return self

    def enable_preview(self):
        """Enables preview for this job.

        **Note**: Enabling preview might slow search considerably.

        :return: The :class:`Job`.
        """
        self.post("control", action="enablepreview")
        return self

    def events(self, **kwargs):
        """Returns a streaming handle to this job's events.

        :param kwargs: Additional parameters (optional). For a list of valid
            parameters, see `GET search/jobs/{search_id}/events
            <http://docs.splunk.com/Documentation/Splunk/latest/RESTAPI/RESTsearch#GET_search.2Fjobs.2F.7Bsearch_id.7D.2Fevents>`_
            in the REST API documentation.
        :type kwargs: ``dict``

        :return: The ``InputStream`` IO handle to this job's events.
        """
        kwargs['segmentation'] = kwargs.get('segmentation', 'none')
        
        # Search API v1(GET) and v2(POST)
        if self.service.disable_v2_api:
            return self.get("events", **kwargs).body
        return self.post("events", **kwargs).body

    def finalize(self):
        """Stops the job and provides intermediate results for retrieval.

        :return: The :class:`Job`.
        """
        self.post("control", action="finalize")
        return self

    def is_done(self):
        """Indicates whether this job finished running.

        :return: ``True`` if the job is done, ``False`` if not.
        :rtype: ``boolean``
        """
        if not self.is_ready():
            return False
        done = (self._state.content['isDone'] == '1')
        return done

    def is_ready(self):
        """Indicates whether this job is ready for querying.

        :return: ``True`` if the job is ready, ``False`` if not.
        :rtype: ``boolean``

        """
        response = self.get()
        if response.status == 204:
            return False
        self._state = self.read(response)
        ready = self._state.content['dispatchState'] not in ['QUEUED', 'PARSING']
        return ready

    @property
    def name(self):
        """Returns the name of the search job, which is the search ID (SID).

        :return: The search ID.
        :rtype: ``string``
        """
        return self.sid

    def pause(self):
        """Suspends the current search.

        :return: The :class:`Job`.
        """
        self.post("control", action="pause")
        return self

    def results(self, **query_params):
        """Returns a streaming handle to this job's search results. To get a nice, Pythonic iterator, pass the handle
        to :class:`splunklib.results.JSONResultsReader` along with the query param "output_mode='json'", as in::

            import splunklib.client as client
            import splunklib.results as results
            from time import sleep
            service = client.connect(...)
            job = service.jobs.create("search * | head 5")
            while not job.is_done():
                sleep(.2)
            rr = results.JSONResultsReader(job.results(output_mode='json'))
            for result in rr:
                if isinstance(result, results.Message):
                    # Diagnostic messages may be returned in the results
                    print '%s: %s' % (result.type, result.message)
                elif isinstance(result, dict):
                    # Normal events are returned as dicts
                    print result
            assert rr.is_preview == False

        Results are not available until the job has finished. If called on
        an unfinished job, the result is an empty event set.

        This method makes a single roundtrip
        to the server, plus at most two additional round trips if
        the ``autologin`` field of :func:`connect` is set to ``True``.

        :param query_params: Additional parameters (optional). For a list of valid
            parameters, see `GET search/jobs/{search_id}/results
            <http://docs.splunk.com/Documentation/Splunk/latest/RESTAPI/RESTsearch#GET_search.2Fjobs.2F.7Bsearch_id.7D.2Fresults>`_.
        :type query_params: ``dict``

        :return: The ``InputStream`` IO handle to this job's results.
        """
        query_params['segmentation'] = query_params.get('segmentation', 'none')
        
        # Search API v1(GET) and v2(POST)
        if self.service.disable_v2_api:
            return self.get("results", **query_params).body
        return self.post("results", **query_params).body

    def preview(self, **query_params):
        """Returns a streaming handle to this job's preview search results.

        Unlike :class:`splunklib.results.JSONResultsReader`along with the query param "output_mode='json'",
        which requires a job to be finished to return any results, the ``preview`` method returns any results that
        have been generated so far, whether the job is running or not. The returned search results are the raw data
        from the server. Pass the handle returned to :class:`splunklib.results.JSONResultsReader` to get a nice,
        Pythonic iterator over objects, as in::

            import splunklib.client as client
            import splunklib.results as results
            service = client.connect(...)
            job = service.jobs.create("search * | head 5")
            rr = results.JSONResultsReader(job.preview(output_mode='json'))
            for result in rr:
                if isinstance(result, results.Message):
                    # Diagnostic messages may be returned in the results
                    print '%s: %s' % (result.type, result.message)
                elif isinstance(result, dict):
                    # Normal events are returned as dicts
                    print result
            if rr.is_preview:
                print "Preview of a running search job."
            else:
                print "Job is finished. Results are final."

        This method makes one roundtrip to the server, plus at most
        two more if
        the ``autologin`` field of :func:`connect` is set to ``True``.

        :param query_params: Additional parameters (optional). For a list of valid
            parameters, see `GET search/jobs/{search_id}/results_preview
            <http://docs.splunk.com/Documentation/Splunk/latest/RESTAPI/RESTsearch#GET_search.2Fjobs.2F.7Bsearch_id.7D.2Fresults_preview>`_
            in the REST API documentation.
        :type query_params: ``dict``

        :return: The ``InputStream`` IO handle to this job's preview results.
        """
        query_params['segmentation'] = query_params.get('segmentation', 'none')
        
        # Search API v1(GET) and v2(POST)
        if self.service.disable_v2_api:
            return self.get("results_preview", **query_params).body
        return self.post("results_preview", **query_params).body

    def searchlog(self, **kwargs):
        """Returns a streaming handle to this job's search log.

        :param `kwargs`: Additional parameters (optional). For a list of valid
            parameters, see `GET search/jobs/{search_id}/search.log
            <http://docs.splunk.com/Documentation/Splunk/latest/RESTAPI/RESTsearch#GET_search.2Fjobs.2F.7Bsearch_id.7D.2Fsearch.log>`_
            in the REST API documentation.
        :type kwargs: ``dict``

        :return: The ``InputStream`` IO handle to this job's search log.
        """
        return self.get("search.log", **kwargs).body

    def set_priority(self, value):
        """Sets this job's search priority in the range of 0-10.

        Higher numbers indicate higher priority. Unless splunkd is
        running as *root*, you can only decrease the priority of a running job.

        :param `value`: The search priority.
        :type value: ``integer``

        :return: The :class:`Job`.
        """
        self.post('control', action="setpriority", priority=value)
        return self

    def summary(self, **kwargs):
        """Returns a streaming handle to this job's summary.

        :param `kwargs`: Additional parameters (optional). For a list of valid
            parameters, see `GET search/jobs/{search_id}/summary
            <http://docs.splunk.com/Documentation/Splunk/latest/RESTAPI/RESTsearch#GET_search.2Fjobs.2F.7Bsearch_id.7D.2Fsummary>`_
            in the REST API documentation.
        :type kwargs: ``dict``

        :return: The ``InputStream`` IO handle to this job's summary.
        """
        return self.get("summary", **kwargs).body

    def timeline(self, **kwargs):
        """Returns a streaming handle to this job's timeline results.

        :param `kwargs`: Additional timeline arguments (optional). For a list of valid
            parameters, see `GET search/jobs/{search_id}/timeline
            <http://docs.splunk.com/Documentation/Splunk/latest/RESTAPI/RESTsearch#GET_search.2Fjobs.2F.7Bsearch_id.7D.2Ftimeline>`_
            in the REST API documentation.
        :type kwargs: ``dict``

        :return: The ``InputStream`` IO handle to this job's timeline.
        """
        return self.get("timeline", **kwargs).body

    def touch(self):
        """Extends the expiration time of the search to the current time (now) plus
        the time-to-live (ttl) value.

        :return: The :class:`Job`.
        """
        self.post("control", action="touch")
        return self

    def set_ttl(self, value):
        """Set the job's time-to-live (ttl) value, which is the time before the
        search job expires and is still available.

        :param `value`: The ttl value, in seconds.
        :type value: ``integer``

        :return: The :class:`Job`.
        """
        self.post("control", action="setttl", ttl=value)
        return self

    def unpause(self):
        """Resumes the current search, if paused.

        :return: The :class:`Job`.
        """
        self.post("control", action="unpause")
        return self


class Jobs(Collection):
    """This class represents a collection of search jobs. Retrieve this
    collection using :meth:`Service.jobs`."""
    def __init__(self, service):
        # Splunk 9 introduces the v2 endpoint
        if not service.disable_v2_api:
            path = PATH_JOBS_V2
        else:
            path = PATH_JOBS
        Collection.__init__(self, service, path, item=Job)
        # The count value to say list all the contents of this
        # Collection is 0, not -1 as it is on most.
        self.null_count = 0

    def _load_list(self, response):
        # Overridden because Job takes a sid instead of a path.
        entries = _load_atom_entries(response)
        if entries is None: return []
        entities = []
        for entry in entries:
            state = _parse_atom_entry(entry)
            entity = self.item(
                self.service,
                entry['content']['sid'],
                state=state)
            entities.append(entity)
        return entities

    def create(self, query, **kwargs):
        """ Creates a search using a search query and any additional parameters
        you provide.

        :param query: The search query.
        :type query: ``string``
        :param kwargs: Additiona parameters (optional). For a list of available
            parameters, see `Search job parameters
            <http://dev.splunk.com/view/SP-CAAAEE5#searchjobparams>`_
            on Splunk Developer Portal.
        :type kwargs: ``dict``

        :return: The :class:`Job`.
        """
        if kwargs.get("exec_mode", None) == "oneshot":
            raise TypeError("Cannot specify exec_mode=oneshot; use the oneshot method instead.")
        response = self.post(search=query, **kwargs)
        sid = _load_sid(response, kwargs.get("output_mode", None))
        return Job(self.service, sid)

    def export(self, query, **params):
        """Runs a search and immediately starts streaming preview events. This method returns a streaming handle to
        this job's events as an XML document from the server. To parse this stream into usable Python objects,
        pass the handle to :class:`splunklib.results.JSONResultsReader` along with the query param
        "output_mode='json'"::

            import splunklib.client as client
            import splunklib.results as results
            service = client.connect(...)
            rr = results.JSONResultsReader(service.jobs.export("search * | head 5",output_mode='json'))
            for result in rr:
                if isinstance(result, results.Message):
                    # Diagnostic messages may be returned in the results
                    print '%s: %s' % (result.type, result.message)
                elif isinstance(result, dict):
                    # Normal events are returned as dicts
                    print result
            assert rr.is_preview == False

        Running an export search is more efficient as it streams the results
        directly to you, rather than having to write them out to disk and make
        them available later. As soon as results are ready, you will receive
        them.

        The ``export`` method makes a single roundtrip to the server (as opposed
        to two for :meth:`create` followed by :meth:`preview`), plus at most two
        more if the ``autologin`` field of :func:`connect` is set to ``True``.

        :raises `ValueError`: Raised for invalid queries.
        :param query: The search query.
        :type query: ``string``
        :param params: Additional arguments (optional). For a list of valid
            parameters, see `GET search/jobs/export
            <http://docs/Documentation/Splunk/latest/RESTAPI/RESTsearch#search.2Fjobs.2Fexport>`_
            in the REST API documentation.
        :type params: ``dict``

        :return: The ``InputStream`` IO handle to raw XML returned from the server.
        """
        if "exec_mode" in params:
            raise TypeError("Cannot specify an exec_mode to export.")
        params['segmentation'] = params.get('segmentation', 'none')
        return self.post(path_segment="export",
                         search=query,
                         **params).body

    def itemmeta(self):
        """There is no metadata available for class:``Jobs``.

        Any call to this method raises a class:``NotSupportedError``.

        :raises: class:``NotSupportedError``
        """
        raise NotSupportedError()

    def oneshot(self, query, **params):
        """Run a oneshot search and returns a streaming handle to the results.

        The ``InputStream`` object streams fragments from the server. To parse this stream into usable Python
        objects, pass the handle to :class:`splunklib.results.JSONResultsReader` along with the query param
        "output_mode='json'" ::

            import splunklib.client as client
            import splunklib.results as results
            service = client.connect(...)
            rr = results.JSONResultsReader(service.jobs.oneshot("search * | head 5",output_mode='json'))
            for result in rr:
                if isinstance(result, results.Message):
                    # Diagnostic messages may be returned in the results
                    print '%s: %s' % (result.type, result.message)
                elif isinstance(result, dict):
                    # Normal events are returned as dicts
                    print result
            assert rr.is_preview == False

        The ``oneshot`` method makes a single roundtrip to the server (as opposed
        to two for :meth:`create` followed by :meth:`results`), plus at most two more
        if the ``autologin`` field of :func:`connect` is set to ``True``.

        :raises ValueError: Raised for invalid queries.

        :param query: The search query.
        :type query: ``string``
        :param params: Additional arguments (optional):

            - "output_mode": Specifies the output format of the results (XML,
              JSON, or CSV).

            - "earliest_time": Specifies the earliest time in the time range to
              search. The time string can be a UTC time (with fractional seconds),
              a relative time specifier (to now), or a formatted time string.

            - "latest_time": Specifies the latest time in the time range to
              search. The time string can be a UTC time (with fractional seconds),
              a relative time specifier (to now), or a formatted time string.

            - "rf": Specifies one or more fields to add to the search.

        :type params: ``dict``

        :return: The ``InputStream`` IO handle to raw XML returned from the server.
        """
        if "exec_mode" in params:
            raise TypeError("Cannot specify an exec_mode to oneshot.")
        params['segmentation'] = params.get('segmentation', 'none')
        return self.post(search=query,
                         exec_mode="oneshot",
                         **params).body


class Loggers(Collection):
    """This class represents a collection of service logging categories.
    Retrieve this collection using :meth:`Service.loggers`."""
    def __init__(self, service):
        Collection.__init__(self, service, PATH_LOGGER)

    def itemmeta(self):
        """There is no metadata available for class:``Loggers``.

        Any call to this method raises a class:``NotSupportedError``.

        :raises: class:``NotSupportedError``
        """
        raise NotSupportedError()


class Message(Entity):
    def __init__(self, service, path, **kwargs):
        Entity.__init__(self, service, path, **kwargs)

    @property
    def value(self):
        """Returns the message value.

        :return: The message value.
        :rtype: ``string``
        """
        return self[self.name]


class ModularInputKind(Entity):
    """This class contains the different types of modular inputs. Retrieve this
    collection using :meth:`Service.modular_input_kinds`.
    """
    def __contains__(self, name):
        args = self.state.content['endpoints']['args']
        if name in args:
            return True
        else:
            return Entity.__contains__(self, name)

    def __getitem__(self, name):
        args = self.state.content['endpoint']['args']
        if name in args:
            return args['item']
        else:
            return Entity.__getitem__(self, name)

    @property
    def arguments(self):
        """A dictionary of all the arguments supported by this modular input kind.

        The keys in the dictionary are the names of the arguments. The values are
        another dictionary giving the metadata about that argument. The possible
        keys in that dictionary are ``"title"``, ``"description"``, ``"required_on_create``",
        ``"required_on_edit"``, ``"data_type"``. Each value is a string. It should be one
        of ``"true"`` or ``"false"`` for ``"required_on_create"`` and ``"required_on_edit"``,
        and one of ``"boolean"``, ``"string"``, or ``"number``" for ``"data_type"``.

        :return: A dictionary describing the arguments this modular input kind takes.
        :rtype: ``dict``
        """
        return self.state.content['endpoint']['args']

    def update(self, **kwargs):
        """Raises an error. Modular input kinds are read only."""
        raise IllegalOperationException("Modular input kinds cannot be updated via the REST API.")


class SavedSearch(Entity):
    """This class represents a saved search."""
    def __init__(self, service, path, **kwargs):
        Entity.__init__(self, service, path, **kwargs)

    def acknowledge(self):
        """Acknowledges the suppression of alerts from this saved search and
        resumes alerting.

        :return: The :class:`SavedSearch`.
        """
        self.post("acknowledge")
        return self

    @property
    def alert_count(self):
        """Returns the number of alerts fired by this saved search.

        :return: The number of alerts fired by this saved search.
        :rtype: ``integer``
        """
        return int(self._state.content.get('triggered_alert_count', 0))

    def dispatch(self, **kwargs):
        """Runs the saved search and returns the resulting search job.

        :param `kwargs`: Additional dispatch arguments (optional). For details,
                         see the `POST saved/searches/{name}/dispatch
                         <http://docs.splunk.com/Documentation/Splunk/latest/RESTAPI/RESTsearch#POST_saved.2Fsearches.2F.7Bname.7D.2Fdispatch>`_
                         endpoint in the REST API documentation.
        :type kwargs: ``dict``
        :return: The :class:`Job`.
        """
        response = self.post("dispatch", **kwargs)
        sid = _load_sid(response, kwargs.get("output_mode", None))
        return Job(self.service, sid)

    @property
    def fired_alerts(self):
        """Returns the collection of fired alerts (a fired alert group)
        corresponding to this saved search's alerts.

        :raises IllegalOperationException: Raised when the search is not scheduled.

        :return: A collection of fired alerts.
        :rtype: :class:`AlertGroup`
        """
        if self['is_scheduled'] == '0':
            raise IllegalOperationException('Unscheduled saved searches have no alerts.')
        c = Collection(
            self.service,
            self.service._abspath(PATH_FIRED_ALERTS + self.name,
                                  owner=self._state.access.owner,
                                  app=self._state.access.app,
                                  sharing=self._state.access.sharing),
            item=AlertGroup)
        return c

    def history(self, **kwargs):
        """Returns a list of search jobs corresponding to this saved search.

        :param `kwargs`: Additional arguments (optional).
        :type kwargs: ``dict``

        :return: A list of :class:`Job` objects.
        """
        response = self.get("history", **kwargs)
        entries = _load_atom_entries(response)
        if entries is None: return []
        jobs = []
        for entry in entries:
            job = Job(self.service, entry.title)
            jobs.append(job)
        return jobs

    def update(self, search=None, **kwargs):
        """Updates the server with any changes you've made to the current saved
        search along with any additional arguments you specify.

        :param `search`: The search query (optional).
        :type search: ``string``
        :param `kwargs`: Additional arguments (optional). For a list of available
            parameters, see `Saved search parameters
            <http://dev.splunk.com/view/SP-CAAAEE5#savedsearchparams>`_
            on Splunk Developer Portal.
        :type kwargs: ``dict``

        :return: The :class:`SavedSearch`.
        """
        # Updates to a saved search *require* that the search string be
        # passed, so we pass the current search string if a value wasn't
        # provided by the caller.
        if search is None: search = self.content.search
        Entity.update(self, search=search, **kwargs)
        return self

    def scheduled_times(self, earliest_time='now', latest_time='+1h'):
        """Returns the times when this search is scheduled to run.

        By default this method returns the times in the next hour. For different
        time ranges, set *earliest_time* and *latest_time*. For example,
        for all times in the last day use "earliest_time=-1d" and
        "latest_time=now".

        :param earliest_time: The earliest time.
        :type earliest_time: ``string``
        :param latest_time: The latest time.
        :type latest_time: ``string``

        :return: The list of search times.
        """
        response = self.get("scheduled_times",
                            earliest_time=earliest_time,
                            latest_time=latest_time)
        data = self._load_atom_entry(response)
        rec = _parse_atom_entry(data)
        times = [datetime.fromtimestamp(int(t))
                 for t in rec.content.scheduled_times]
        return times

    def suppress(self, expiration):
        """Skips any scheduled runs of this search in the next *expiration*
        number of seconds.

        :param expiration: The expiration period, in seconds.
        :type expiration: ``integer``

        :return: The :class:`SavedSearch`.
        """
        self.post("suppress", expiration=expiration)
        return self

    @property
    def suppressed(self):
        """Returns the number of seconds that this search is blocked from running
        (possibly 0).

        :return: The number of seconds.
        :rtype: ``integer``
        """
        r = self._run_action("suppress")
        if r.suppressed == "1":
            return int(r.expiration)
        else:
            return 0

    def unsuppress(self):
        """Cancels suppression and makes this search run as scheduled.

        :return: The :class:`SavedSearch`.
        """
        self.post("suppress", expiration="0")
        return self


class SavedSearches(Collection):
    """This class represents a collection of saved searches. Retrieve this
    collection using :meth:`Service.saved_searches`."""
    def __init__(self, service):
        Collection.__init__(
            self, service, PATH_SAVED_SEARCHES, item=SavedSearch)

    def create(self, name, search, **kwargs):
        """ Creates a saved search.

        :param name: The name for the saved search.
        :type name: ``string``
        :param search: The search query.
        :type search: ``string``
        :param kwargs: Additional arguments (optional). For a list of available
            parameters, see `Saved search parameters
            <http://dev.splunk.com/view/SP-CAAAEE5#savedsearchparams>`_
            on Splunk Developer Portal.
        :type kwargs: ``dict``
        :return: The :class:`SavedSearches` collection.
        """
        return Collection.create(self, name, search=search, **kwargs)


class Settings(Entity):
    """This class represents configuration settings for a Splunk service.
    Retrieve this collection using :meth:`Service.settings`."""
    def __init__(self, service, **kwargs):
        Entity.__init__(self, service, "/services/server/settings", **kwargs)

    # Updates on the settings endpoint are POSTed to server/settings/settings.
    def update(self, **kwargs):
        """Updates the settings on the server using the arguments you provide.

        :param kwargs: Additional arguments. For a list of valid arguments, see
            `POST server/settings/{name}
            <http://docs.splunk.com/Documentation/Splunk/latest/RESTAPI/RESTsystem#POST_server.2Fsettings.2F.7Bname.7D>`_
            in the REST API documentation.
        :type kwargs: ``dict``
        :return: The :class:`Settings` collection.
        """
        self.service.post("/services/server/settings/settings", **kwargs)
        return self


class User(Entity):
    """This class represents a Splunk user.
    """
    @property
    def role_entities(self):
        """Returns a list of roles assigned to this user.

        :return: The list of roles.
        :rtype: ``list``
        """
        return [self.service.roles[name] for name in self.content.roles]


# Splunk automatically lowercases new user names so we need to match that
# behavior here to ensure that the subsequent member lookup works correctly.
class Users(Collection):
    """This class represents the collection of Splunk users for this instance of
    Splunk. Retrieve this collection using :meth:`Service.users`.
    """
    def __init__(self, service):
        Collection.__init__(self, service, PATH_USERS, item=User)

    def __getitem__(self, key):
        return Collection.__getitem__(self, key.lower())

    def __contains__(self, name):
        return Collection.__contains__(self, name.lower())

    def create(self, username, password, roles, **params):
        """Creates a new user.

        This function makes two roundtrips to the server, plus at most
        two more if
        the ``autologin`` field of :func:`connect` is set to ``True``.

        :param username: The username.
        :type username: ``string``
        :param password: The password.
        :type password: ``string``
        :param roles: A single role or list of roles for the user.
        :type roles: ``string`` or  ``list``
        :param params: Additional arguments (optional). For a list of available
            parameters, see `User authentication parameters
            <http://dev.splunk.com/view/SP-CAAAEJ6#userauthparams>`_
            on Splunk Developer Portal.
        :type params: ``dict``

        :return: The new user.
        :rtype: :class:`User`

        **Example**::

            import splunklib.client as client
            c = client.connect(...)
            users = c.users
            boris = users.create("boris", "securepassword", roles="user")
            hilda = users.create("hilda", "anotherpassword", roles=["user","power"])
        """
        if not isinstance(username, six.string_types):
            raise ValueError("Invalid username: %s" % str(username))
        username = username.lower()
        self.post(name=username, password=password, roles=roles, **params)
        # splunkd doesn't return the user in the POST response body,
        # so we have to make a second round trip to fetch it.
        response = self.get(username)
        entry = _load_atom(response, XNAME_ENTRY).entry
        state = _parse_atom_entry(entry)
        entity = self.item(
            self.service,
            urllib.parse.unquote(state.links.alternate),
            state=state)
        return entity

    def delete(self, name):
        """ Deletes the user and returns the resulting collection of users.

        :param name: The name of the user to delete.
        :type name: ``string``

        :return:
        :rtype: :class:`Users`
        """
        return Collection.delete(self, name.lower())


class Role(Entity):
    """This class represents a user role.
    """
    def grant(self, *capabilities_to_grant):
        """Grants additional capabilities to this role.

        :param capabilities_to_grant: Zero or more capabilities to grant this
            role. For a list of capabilities, see
            `Capabilities <http://dev.splunk.com/view/SP-CAAAEJ6#capabilities>`_
            on Splunk Developer Portal.
        :type capabilities_to_grant: ``string`` or ``list``
        :return: The :class:`Role`.

        **Example**::

            service = client.connect(...)
            role = service.roles['somerole']
            role.grant('change_own_password', 'search')
        """
        possible_capabilities = self.service.capabilities
        for capability in capabilities_to_grant:
            if capability not in possible_capabilities:
                raise NoSuchCapability(capability)
        new_capabilities = self['capabilities'] + list(capabilities_to_grant)
        self.post(capabilities=new_capabilities)
        return self

    def revoke(self, *capabilities_to_revoke):
        """Revokes zero or more capabilities from this role.

        :param capabilities_to_revoke: Zero or more capabilities to grant this
            role. For a list of capabilities, see
            `Capabilities <http://dev.splunk.com/view/SP-CAAAEJ6#capabilities>`_
            on Splunk Developer Portal.
        :type capabilities_to_revoke: ``string`` or ``list``

        :return: The :class:`Role`.

        **Example**::

            service = client.connect(...)
            role = service.roles['somerole']
            role.revoke('change_own_password', 'search')
        """
        possible_capabilities = self.service.capabilities
        for capability in capabilities_to_revoke:
            if capability not in possible_capabilities:
                raise NoSuchCapability(capability)
        old_capabilities = self['capabilities']
        new_capabilities = []
        for c in old_capabilities:
            if c not in capabilities_to_revoke:
                new_capabilities.append(c)
        if new_capabilities == []:
            new_capabilities = '' # Empty lists don't get passed in the body, so we have to force an empty argument.
        self.post(capabilities=new_capabilities)
        return self


class Roles(Collection):
    """This class represents the collection of roles in the Splunk instance.
    Retrieve this collection using :meth:`Service.roles`."""
    def __init__(self, service):
        return Collection.__init__(self, service, PATH_ROLES, item=Role)

    def __getitem__(self, key):
        return Collection.__getitem__(self, key.lower())

    def __contains__(self, name):
        return Collection.__contains__(self, name.lower())

    def create(self, name, **params):
        """Creates a new role.

        This function makes two roundtrips to the server, plus at most
        two more if
        the ``autologin`` field of :func:`connect` is set to ``True``.

        :param name: Name for the role.
        :type name: ``string``
        :param params: Additional arguments (optional). For a list of available
            parameters, see `Roles parameters
            <http://dev.splunk.com/view/SP-CAAAEJ6#rolesparams>`_
            on Splunk Developer Portal.
        :type params: ``dict``

        :return: The new role.
        :rtype: :class:`Role`

        **Example**::

            import splunklib.client as client
            c = client.connect(...)
            roles = c.roles
            paltry = roles.create("paltry", imported_roles="user", defaultApp="search")
        """
        if not isinstance(name, six.string_types):
            raise ValueError("Invalid role name: %s" % str(name))
        name = name.lower()
        self.post(name=name, **params)
        # splunkd doesn't return the user in the POST response body,
        # so we have to make a second round trip to fetch it.
        response = self.get(name)
        entry = _load_atom(response, XNAME_ENTRY).entry
        state = _parse_atom_entry(entry)
        entity = self.item(
            self.service,
            urllib.parse.unquote(state.links.alternate),
            state=state)
        return entity

    def delete(self, name):
        """ Deletes the role and returns the resulting collection of roles.

        :param name: The name of the role to delete.
        :type name: ``string``

        :rtype: The :class:`Roles`
        """
        return Collection.delete(self, name.lower())


class Application(Entity):
    """Represents a locally-installed Splunk app."""
    @property
    def setupInfo(self):
        """Returns the setup information for the app.

        :return: The setup information.
        """
        return self.content.get('eai:setup', None)

    def package(self):
        """ Creates a compressed package of the app for archiving."""
        return self._run_action("package")

    def updateInfo(self):
        """Returns any update information that is available for the app."""
        return self._run_action("update")

class KVStoreCollections(Collection):
    def __init__(self, service):
        Collection.__init__(self, service, 'storage/collections/config', item=KVStoreCollection)

    def __getitem__(self, item):
        res = Collection.__getitem__(self, item)
        for k, v in res.content.items():
            if "accelerated_fields" in k:
                res.content[k] = json.loads(v)
        return res

    def create(self, name, accelerated_fields={}, fields={}, **kwargs):
        """Creates a KV Store Collection.

        :param name: name of collection to create
        :type name: ``string``
        :param accelerated_fields: dictionary of accelerated_fields definitions
        :type accelerated_fields: ``dict``
        :param fields: dictionary of field definitions
        :type fields: ``dict``
        :param kwargs: a dictionary of additional parameters specifying indexes and field definitions
        :type kwargs: ``dict``

        :return: Result of POST request
        """
        for k, v in six.iteritems(accelerated_fields):
            if isinstance(v, dict):
                v = json.dumps(v)
            kwargs['accelerated_fields.' + k] = v
        for k, v in six.iteritems(fields):
            kwargs['field.' + k] = v
        return self.post(name=name, **kwargs)

class KVStoreCollection(Entity):
    @property
    def data(self):
        """Returns data object for this Collection.

        :rtype: :class:`KVStoreCollectionData`
        """
        return KVStoreCollectionData(self)

    def update_accelerated_field(self, name, value):
        """Changes the definition of a KV Store accelerated_field.

        :param name: name of accelerated_fields to change
        :type name: ``string``
        :param value: new accelerated_fields definition
        :type value: ``dict``

        :return: Result of POST request
        """
        kwargs = {}
        if isinstance(value, dict):
            value = json.dumps(value)
        kwargs['accelerated_fields.' + name] = value
        return self.post(**kwargs)

    def update_field(self, name, value):
        """Changes the definition of a KV Store field.

        :param name: name of field to change
        :type name: ``string``
        :param value: new field definition
        :type value: ``string``

        :return: Result of POST request
        """
        kwargs = {}
        kwargs['field.' + name] = value
        return self.post(**kwargs)

class KVStoreCollectionData(object):
    """This class represents the data endpoint for a KVStoreCollection.

    Retrieve using :meth:`KVStoreCollection.data`
    """
    JSON_HEADER = [('Content-Type', 'application/json')]

    def __init__(self, collection):
        self.service = collection.service
        self.collection = collection
        self.owner, self.app, self.sharing = collection._proper_namespace()
        self.path = 'storage/collections/data/' + UrlEncoded(self.collection.name, encode_slash=True) + '/'

    def _get(self, url, **kwargs):
        return self.service.get(self.path + url, owner=self.owner, app=self.app, sharing=self.sharing, **kwargs)

    def _post(self, url, **kwargs):
        return self.service.post(self.path + url, owner=self.owner, app=self.app, sharing=self.sharing, **kwargs)

    def _delete(self, url, **kwargs):
        return self.service.delete(self.path + url, owner=self.owner, app=self.app, sharing=self.sharing, **kwargs)

    def query(self, **query):
        """
        Gets the results of query, with optional parameters sort, limit, skip, and fields.

        :param query: Optional parameters. Valid options are sort, limit, skip, and fields
        :type query: ``dict``

        :return: Array of documents retrieved by query.
        :rtype: ``array``
        """

        for key, value in query.items():
            if isinstance(query[key], dict):
                query[key] = json.dumps(value)

        return json.loads(self._get('', **query).body.read().decode('utf-8'))

    def query_by_id(self, id):
        """
        Returns object with _id = id.

        :param id: Value for ID. If not a string will be coerced to string.
        :type id: ``string``

        :return: Document with id
        :rtype: ``dict``
        """
        return json.loads(self._get(UrlEncoded(str(id), encode_slash=True)).body.read().decode('utf-8'))

    def insert(self, data):
        """
        Inserts item into this collection. An _id field will be generated if not assigned in the data.

        :param data: Document to insert
        :type data: ``string``

        :return: _id of inserted object
        :rtype: ``dict``
        """
        if isinstance(data, dict):
            data = json.dumps(data)
        return json.loads(self._post('', headers=KVStoreCollectionData.JSON_HEADER, body=data).body.read().decode('utf-8'))

    def delete(self, query=None):
        """
        Deletes all data in collection if query is absent. Otherwise, deletes all data matched by query.

        :param query: Query to select documents to delete
        :type query: ``string``

        :return: Result of DELETE request
        """
        return self._delete('', **({'query': query}) if query else {})

    def delete_by_id(self, id):
        """
        Deletes document that has _id = id.

        :param id: id of document to delete
        :type id: ``string``

        :return: Result of DELETE request
        """
        return self._delete(UrlEncoded(str(id), encode_slash=True))

    def update(self, id, data):
        """
        Replaces document with _id = id with data.

        :param id: _id of document to update
        :type id: ``string``
        :param data: the new document to insert
        :type data: ``string``

        :return: id of replaced document
        :rtype: ``dict``
        """
        if isinstance(data, dict):
            data = json.dumps(data)
        return json.loads(self._post(UrlEncoded(str(id), encode_slash=True), headers=KVStoreCollectionData.JSON_HEADER, body=data).body.read().decode('utf-8'))

    def batch_find(self, *dbqueries):
        """
        Returns array of results from queries dbqueries.

        :param dbqueries: Array of individual queries as dictionaries
        :type dbqueries: ``array`` of ``dict``

        :return: Results of each query
        :rtype: ``array`` of ``array``
        """
        if len(dbqueries) < 1:
            raise Exception('Must have at least one query.')

        data = json.dumps(dbqueries)

        return json.loads(self._post('batch_find', headers=KVStoreCollectionData.JSON_HEADER, body=data).body.read().decode('utf-8'))

    def batch_save(self, *documents):
        """
        Inserts or updates every document specified in documents.

        :param documents: Array of documents to save as dictionaries
        :type documents: ``array`` of ``dict``

        :return: Results of update operation as overall stats
        :rtype: ``dict``
        """
        if len(documents) < 1:
            raise Exception('Must have at least one document.')

        data = json.dumps(documents)

        return json.loads(self._post('batch_save', headers=KVStoreCollectionData.JSON_HEADER, body=data).body.read().decode('utf-8'))