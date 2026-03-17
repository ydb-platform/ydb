#!/usr/bin/env python
import logging
import os
import sys
import warnings
from types import TracebackType
from typing import Any
from typing import cast
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import TYPE_CHECKING
from typing import Union
from urllib.parse import unquote


try:
    import niquests as requests
    from niquests.auth import AuthBase
    from niquests.models import Response
    from niquests.structures import CaseInsensitiveDict
except ImportError:
    import requests
    from requests.auth import AuthBase
    from requests.models import Response
    from requests.structures import CaseInsensitiveDict

from lxml import etree
from lxml.etree import _Element

from .elements.base import BaseElement
from caldav import __version__
from caldav.collection import Calendar
from caldav.collection import CalendarSet
from caldav.collection import Principal
import caldav.compatibility_hints
from caldav.compatibility_hints import FeatureSet
from caldav.elements import cdav
from caldav.elements import dav
from caldav.lib import error
from caldav.lib.python_utilities import to_normal_str
from caldav.lib.python_utilities import to_wire
from caldav.lib.url import URL
from caldav.objects import log
from caldav.requests import HTTPBearerAuth

if TYPE_CHECKING:
    pass

if sys.version_info < (3, 9):
    from typing import Iterable, Mapping
else:
    from collections.abc import Iterable, Mapping

if sys.version_info < (3, 11):
    from typing_extensions import Self
else:
    from typing import Self

"""
The ``DAVClient`` class handles the basic communication with a
CalDAV server.  In 1.x the recommended usage of the library is to
start constructing a DAVClient object.  In 2.0 the function
``get_davclient`` was added as the new recommended way to get a
DAVClient object.  In later versions there may be a ``get_calendar``,
eliminating the need to deal with DAVClient for most use cases.

The ``DAVResponse`` class handles the data returned from the server.
In most use-cases library users will not interface with this class
directly.

``get_davclient`` will return a DAVClient object, based either on
environmental variables, a configuration file or test configuration.
"""

## TODO: this is also declared in davclient.DAVClient.__init__(...)
## TODO: it should be consolidated, duplication is a bad thing
## TODO: and it's almost certain that we'll forget to update this list
CONNKEYS = set(
    (
        "url",
        "proxy",
        "username",
        "password",
        "timeout",
        "headers",
        "huge_tree",
        "ssl_verify_cert",
        "ssl_cert",
        "auth",
        "auth_type",
        "features",
        "enable_rfc6764",
        "require_tls",
    )
)


def _auto_url(
    url,
    features,
    timeout=10,
    ssl_verify_cert=True,
    enable_rfc6764=True,
    username=None,
    require_tls=True,
):
    """
    Auto-construct URL from domain and features, with optional RFC6764 discovery.

    Args:
        url: User-provided URL, domain, or email address
        features: FeatureSet object or dict
        timeout: Timeout for RFC6764 well-known URI lookups
        ssl_verify_cert: SSL verification setting
        enable_rfc6764: Whether to attempt RFC6764 discovery
        username: Username to use for discovery if URL is not provided
        require_tls: Only accept TLS connections during discovery (default: True)

    Returns:
        A tuple of (url_string, discovered_username_or_None)
        The discovered_username will be extracted from email addresses like user@example.com
    """
    if isinstance(features, dict):
        features = FeatureSet(features)

    # If URL already has a path component, don't do discovery
    if url and "/" in str(url):
        return (url, None)

    # If no URL provided but username contains @, use username for discovery
    if not url and username and "@" in str(username) and enable_rfc6764:
        log.debug(f"No URL provided, using username for RFC6764 discovery: {username}")
        url = username

    # Try RFC6764 discovery first if enabled and we have a bare domain/email
    if enable_rfc6764 and url:
        from caldav.discovery import discover_caldav, DiscoveryError

        try:
            service_info = discover_caldav(
                identifier=url,
                timeout=timeout,
                ssl_verify_cert=ssl_verify_cert
                if isinstance(ssl_verify_cert, bool)
                else True,
                require_tls=require_tls,
            )
            if service_info:
                log.info(
                    f"RFC6764 discovered service: {service_info.url} (source: {service_info.source})"
                )
                if service_info.username:
                    log.debug(
                        f"Username discovered from email: {service_info.username}"
                    )
                return (service_info.url, service_info.username)
        except DiscoveryError as e:
            log.debug(f"RFC6764 discovery failed: {e}")
        except Exception as e:
            log.debug(f"RFC6764 discovery error: {e}")

    # Fall back to feature-based URL construction
    url_hints = features.is_supported("auto-connect.url", dict)
    # If URL is still empty or looks like an email (from failed discovery attempt),
    # replace it with the domain from hints
    if (not url or (url and "@" in str(url))) and "domain" in url_hints:
        url = url_hints["domain"]
    url = f"{url_hints.get('scheme', 'https')}://{url}{url_hints.get('basepath', '')}"
    return (url, None)


class DAVResponse:
    """
    This class is a response from a DAV request.  It is instantiated from
    the DAVClient class.  End users of the library should not need to
    know anything about this class.  Since we often get XML responses,
    it tries to parse it into `self.tree`
    """

    raw = ""
    reason: str = ""
    tree: Optional[_Element] = None
    headers: CaseInsensitiveDict = None
    status: int = 0
    davclient = None
    huge_tree: bool = False

    def __init__(
        self, response: Response, davclient: Optional["DAVClient"] = None
    ) -> None:
        self.headers = response.headers
        self.status = response.status_code
        log.debug("response headers: " + str(self.headers))
        log.debug("response status: " + str(self.status))

        self._raw = response.content
        self.davclient = davclient
        if davclient:
            self.huge_tree = davclient.huge_tree

        content_type = self.headers.get("Content-Type", "")
        xml = ["text/xml", "application/xml"]
        no_xml = ["text/plain", "text/calendar", "application/octet-stream"]
        expect_xml = any((content_type.startswith(x) for x in xml))
        expect_no_xml = any((content_type.startswith(x) for x in no_xml))
        if (
            content_type
            and not expect_xml
            and not expect_no_xml
            and response.status_code < 400
            and response.text
        ):
            error.weirdness(f"Unexpected content type: {content_type}")
        try:
            content_length = int(self.headers["Content-Length"])
        except:
            content_length = -1
        if content_length == 0 or not self._raw:
            self._raw = ""
            self.tree = None
            log.debug("No content delivered")
        else:
            ## For really huge objects we should pass the object as a stream to the
            ## XML parser, like this:
            # self.tree = etree.parse(response.raw, parser=etree.XMLParser(remove_blank_text=True))
            ## However, we would also need to decompress on the fly.  I won't bother now.
            try:
                ## https://github.com/python-caldav/caldav/issues/142
                ## We cannot trust the content=type (iCloud, OX and others).
                ## We'll try to parse the content as XML no matter
                ## the content type given.
                self.tree = etree.XML(
                    self._raw,
                    parser=etree.XMLParser(
                        remove_blank_text=True, huge_tree=self.huge_tree
                    ),
                )
            except:
                ## Content wasn't XML.  What does the content-type say?
                ## expect_no_xml means text/plain or text/calendar
                ## expect_no_xml -> ok, pass on, with debug logging
                ## expect_xml means text/xml or application/xml
                ## expect_xml -> raise an error
                ## anything else (text/plain, text/html, ''),
                ## log an info message and continue (some servers return HTML error pages)
                if not expect_no_xml or log.level <= logging.DEBUG:
                    if not expect_no_xml:
                        _log = logging.info
                    else:
                        _log = logging.debug
                        ## The statement below may not be true.
                        ## We may be expecting something else
                    _log(
                        "Expected some valid XML from the server, but got this: \n"
                        + str(self._raw),
                        exc_info=True,
                    )
                if expect_xml:
                    raise
            else:
                if log.level <= logging.DEBUG:
                    log.debug(etree.tostring(self.tree, pretty_print=True))

        ## this if will always be true as for now, see other comments on streaming.
        if hasattr(self, "_raw"):
            log.debug(self._raw)
            # ref https://github.com/python-caldav/caldav/issues/112 stray CRs may cause problems
            if isinstance(self._raw, bytes):
                self._raw = self._raw.replace(b"\r\n", b"\n")
            elif isinstance(self._raw, str):
                self._raw = self._raw.replace("\r\n", "\n")
        self.status = response.status_code
        ## ref https://github.com/python-caldav/caldav/issues/81,
        ## incidents with a response without a reason has been
        ## observed
        try:
            self.reason = response.reason
        except AttributeError:
            self.reason = ""

    @property
    def raw(self) -> str:
        ## TODO: this should not really be needed?
        if not hasattr(self, "_raw"):
            self._raw = etree.tostring(cast(_Element, self.tree), pretty_print=True)
        return to_normal_str(self._raw)

    def _strip_to_multistatus(self):
        """
        The general format of inbound data is something like this:

        <xml><multistatus>
            <response>(...)</response>
            <response>(...)</response>
            (...)
        </multistatus></xml>

        but sometimes the multistatus and/or xml element is missing in
        self.tree.  We don't want to bother with the multistatus and
        xml tags, we just want the response list.

        An "Element" in the lxml library is a list-like object, so we
        should typically return the element right above the responses.
        If there is nothing but a response, return it as a list with
        one element.

        (The equivalent of this method could probably be found with a
        simple XPath query, but I'm not much into XPath)
        """
        tree = self.tree
        if tree.tag == "xml" and tree[0].tag == dav.MultiStatus.tag:
            return tree[0]
        if tree.tag == dav.MultiStatus.tag:
            return self.tree
        return [self.tree]

    def validate_status(self, status: str) -> None:
        """
        status is a string like "HTTP/1.1 404 Not Found".  200, 207 and
        404 are considered good statuses.  The SOGo caldav server even
        returns "201 created" when doing a sync-report, to indicate
        that a resource was created after the last sync-token.  This
        makes sense to me, but I've only seen it from SOGo, and it's
        not in accordance with the examples in rfc6578.
        """
        if (
            " 200 " not in status
            and " 201 " not in status
            and " 207 " not in status
            and " 404 " not in status
        ):
            raise error.ResponseError(status)

    def _parse_response(self, response) -> Tuple[str, List[_Element], Optional[Any]]:
        """
        One response should contain one or zero status children, one
        href tag and zero or more propstats.  Find them, assert there
        isn't more in the response and return those three fields
        """
        status = None
        href: Optional[str] = None
        propstats: List[_Element] = []
        check_404 = False  ## special for purelymail
        error.assert_(response.tag == dav.Response.tag)
        for elem in response:
            if elem.tag == dav.Status.tag:
                error.assert_(not status)
                status = elem.text
                error.assert_(status)
                self.validate_status(status)
            elif elem.tag == dav.Href.tag:
                assert not href
                # Fix for https://github.com/python-caldav/caldav/issues/471
                # Confluence server quotes the user email twice. We unquote it manually.
                if "%2540" in elem.text:
                    elem.text = elem.text.replace("%2540", "%40")
                href = unquote(elem.text)
            elif elem.tag == dav.PropStat.tag:
                propstats.append(elem)
            elif elem.tag == "{DAV:}error":
                ## This happens with purelymail on a 404.
                ## This code is mostly moot, but in debug
                ## mode I want to be sure we do not toss away any data
                children = elem.getchildren()
                error.assert_(len(children) == 1)
                error.assert_(
                    children[0].tag == "{https://purelymail.com}does-not-exist"
                )
                check_404 = True
            else:
                ## i.e. purelymail may contain one more tag, <error>...</error>
                ## This is probably not a breach of the standard.  It may
                ## probably be ignored.  But it's something we may want to
                ## know.
                error.weirdness("unexpected element found in response", elem)
        error.assert_(href)
        if check_404:
            error.assert_("404" in status)
        ## TODO: is this safe/sane?
        ## Ref https://github.com/python-caldav/caldav/issues/435 the paths returned may be absolute URLs,
        ## but the caller expects them to be paths.  Could we have issues when a server has same path
        ## but different URLs for different elements?  Perhaps href should always be made into an URL-object?
        if ":" in href:
            href = unquote(URL(href).path)
        return (cast(str, href), propstats, status)

    def find_objects_and_props(self) -> Dict[str, Dict[str, _Element]]:
        """Check the response from the server, check that it is on an expected format,
        find hrefs and props from it and check statuses delivered.

        The parsed data will be put into self.objects, a dict {href:
        {proptag: prop_element}}.  Further parsing of the prop_element
        has to be done by the caller.

        self.sync_token will be populated if found, self.objects will be populated.
        """
        self.objects: Dict[str, Dict[str, _Element]] = {}
        self.statuses: Dict[str, str] = {}

        if "Schedule-Tag" in self.headers:
            self.schedule_tag = self.headers["Schedule-Tag"]

        responses = self._strip_to_multistatus()
        for r in responses:
            if r.tag == dav.SyncToken.tag:
                self.sync_token = r.text
                continue
            error.assert_(r.tag == dav.Response.tag)

            (href, propstats, status) = self._parse_response(r)
            ## I would like to do this assert here ...
            # error.assert_(not href in self.objects)
            ## but then there was https://github.com/python-caldav/caldav/issues/136
            if href not in self.objects:
                self.objects[href] = {}
                self.statuses[href] = status

            ## The properties may be delivered either in one
            ## propstat with multiple props or in multiple
            ## propstat
            for propstat in propstats:
                cnt = 0
                status = propstat.find(dav.Status.tag)
                error.assert_(status is not None)
                if status is not None and status.text is not None:
                    error.assert_(len(status) == 0)
                    cnt += 1
                    self.validate_status(status.text)
                    ## if a prop was not found, ignore it
                    if " 404 " in status.text:
                        continue
                for prop in propstat.iterfind(dav.Prop.tag):
                    cnt += 1
                    for theprop in prop:
                        self.objects[href][theprop.tag] = theprop

                ## there shouldn't be any more elements except for status and prop
                error.assert_(cnt == len(propstat))

        return self.objects

    def _expand_simple_prop(
        self, proptag, props_found, multi_value_allowed=False, xpath=None
    ):
        values = []
        if proptag in props_found:
            prop_xml = props_found[proptag]
            for item in prop_xml.items():
                if proptag == "{urn:ietf:params:xml:ns:caldav}calendar-data":
                    if (
                        item[0].lower().endswith("content-type")
                        and item[1].lower() == "text/calendar"
                    ):
                        continue
                    if item[0].lower().endswith("version") and item[1] in ("2", "2.0"):
                        continue
                log.error(
                    f"If you see this, please add a report at https://github.com/python-caldav/caldav/issues/209 - in _expand_simple_prop, dealing with {proptag}, extra item found: {'='.join(item)}."
                )
            if not xpath and len(prop_xml) == 0:
                if prop_xml.text:
                    values.append(prop_xml.text)
            else:
                _xpath = xpath if xpath else ".//*"
                leafs = prop_xml.findall(_xpath)
                values = []
                for leaf in leafs:
                    error.assert_(not leaf.items())
                    if leaf.text:
                        values.append(leaf.text)
                    else:
                        values.append(leaf.tag)
        if multi_value_allowed:
            return values
        else:
            if not values:
                return None
            error.assert_(len(values) == 1)
            return values[0]

    ## TODO: word "expand" does not feel quite right.
    def expand_simple_props(
        self,
        props: Iterable[BaseElement] = None,
        multi_value_props: Iterable[Any] = None,
        xpath: Optional[str] = None,
    ) -> Dict[str, Dict[str, str]]:
        """
        The find_objects_and_props() will stop at the xml element
        below the prop tag.  This method will expand those props into
        text.

        Executes find_objects_and_props if not run already, then
        modifies and returns self.objects.
        """
        props = props or []
        multi_value_props = multi_value_props or []

        if not hasattr(self, "objects"):
            self.find_objects_and_props()
        for href in self.objects:
            props_found = self.objects[href]
            for prop in props:
                if prop.tag is None:
                    continue

                props_found[prop.tag] = self._expand_simple_prop(
                    prop.tag, props_found, xpath=xpath
                )
            for prop in multi_value_props:
                if prop.tag is None:
                    continue

                props_found[prop.tag] = self._expand_simple_prop(
                    prop.tag, props_found, xpath=xpath, multi_value_allowed=True
                )
        # _Element objects in self.objects are parsed to str, thus the need to cast the return
        return cast(Dict[str, Dict[str, str]], self.objects)


class DAVClient:
    """
    Basic client for webdav, uses the niquests lib; gives access to
    low-level operations towards the caldav server.

    Unless you have special needs, you should probably care most about
    the constructor (__init__), the principal method and the calendar method.
    """

    proxy: Optional[str] = None
    url: URL = None
    huge_tree: bool = False

    def __init__(
        self,
        url: Optional[str] = "",
        proxy: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        auth: Optional[AuthBase] = None,
        auth_type: Optional[str] = None,
        timeout: Optional[int] = None,
        ssl_verify_cert: Union[bool, str] = True,
        ssl_cert: Union[str, Tuple[str, str], None] = None,
        headers: Mapping[str, str] = None,
        huge_tree: bool = False,
        features: Union[FeatureSet, dict, str] = None,
        enable_rfc6764: bool = True,
        require_tls: bool = True,
    ) -> None:
        """
        Sets up a HTTPConnection object towards the server in the url.

        Args:
          url: A fully qualified url, domain name, or email address. Can be omitted if username
               is an email address (RFC6764 discovery will use the username).
               Examples:
               - Full URL: `https://caldav.example.com/dav/`
               - Domain: `example.com` (will attempt RFC6764 discovery if enable_rfc6764=True)
               - Email: `user@example.com` (will attempt RFC6764 discovery if enable_rfc6764=True)
               - URL with auth: `scheme://user:pass@hostname:port`
               - Omit URL: Use `username='user@example.com'` for discovery
          username: Username for authentication. If url is omitted and username contains @,
                    RFC6764 discovery will be attempted using the username as email address.
          proxy: A string defining a proxy server: `scheme://hostname:port`. Scheme defaults to http, port defaults to 8080.
          auth: A niquests.auth.AuthBase or requests.auth.AuthBase object, may be passed instead of username/password.  username and password should be passed as arguments or in the URL
          timeout and ssl_verify_cert are passed to niquests.request.
          if auth_type is given, the auth-object will be auto-created. Auth_type can be ``bearer``, ``digest`` or ``basic``. Things are likely to work without ``auth_type`` set, but if nothing else the number of requests to the server will be reduced, and some servers may require this to squelch warnings of unexpected HTML delivered from the
           server etc.
          ssl_verify_cert can be the path of a CA-bundle or False.
          huge_tree: boolean, enable XMLParser huge_tree to handle big events, beware of security issues, see : https://lxml.de/api/lxml.etree.XMLParser-class.html
          features: The default, None, will in version 2.x enable all existing workarounds in the code for backward compability.  Otherwise it will expect a FeatureSet or a dict as defined in `caldav.compatibility_hints` and use that to figure out what workarounds are needed.
          enable_rfc6764: boolean, enable RFC6764 DNS-based service discovery for CalDAV/CardDAV.
                          Default: True. When enabled and a domain or email address is provided as url,
                          the library will attempt to discover the CalDAV service using:
                          1. DNS SRV records (_caldavs._tcp / _caldav._tcp)
                          2. DNS TXT records for path information
                          3. Well-Known URIs (/.well-known/caldav)
                          Set to False to disable automatic discovery and rely only on feature hints.
                          SECURITY: See require_tls parameter for security considerations.
          require_tls: boolean, require TLS (HTTPS) for discovered services. Default: True.
                       When True, RFC6764 discovery will ONLY accept HTTPS connections,
                       preventing DNS-based downgrade attacks where malicious DNS could
                       redirect to unencrypted HTTP. Set to False ONLY if you need to
                       support non-TLS servers and trust your DNS infrastructure.
                       This parameter has no effect if enable_rfc6764=False.

        The niquests library will honor a .netrc-file, if such a file exists
        username and password may be omitted.

        THe niquest library will honor standard proxy environmental variables like
        HTTP_PROXY, HTTPS_PROXY and ALL_PROXY.  See https://niquests.readthedocs.io/en/latest/user/advanced.html#proxies

        If the caldav server is behind a proxy or replies with html instead of xml
        when returning 401, warnings will be printed which might be unwanted.
        Check auth parameter for details.
        """
        headers = headers or {}

        ## Deprecation TODO: give a warning, user should use get_davclient or auto_calendar instead.  Probably.

        if isinstance(features, str):
            features = getattr(caldav.compatibility_hints, features)
        self.features = FeatureSet(features)
        self.huge_tree = huge_tree

        try:
            multiplexed = self.features.is_supported("http.multiplexing")
            self.session = requests.Session(multiplexed=multiplexed)
        except TypeError:
            self.session = requests.Session()

        url, discovered_username = _auto_url(
            url,
            self.features,
            timeout=timeout or 10,
            ssl_verify_cert=ssl_verify_cert,
            enable_rfc6764=enable_rfc6764,
            username=username,
            require_tls=require_tls,
        )

        log.debug("url: " + str(url))
        self.url = URL.objectify(url)
        # Prepare proxy info
        if proxy is not None:
            _proxy = proxy
            # niquests library expects the proxy url to have a scheme
            if "://" not in proxy:
                _proxy = self.url.scheme + "://" + proxy

            # add a port is one is not specified
            # TODO: this will break if using basic auth and embedding
            # username:password in the proxy URL
            p = _proxy.split(":")
            if len(p) == 2:
                _proxy += ":8080"
            log.debug("init - proxy: %s" % (_proxy))

            self.proxy = _proxy

        # Build global headers
        self.headers = CaseInsensitiveDict(
            {
                "User-Agent": "python-caldav/" + __version__,
                "Content-Type": "text/xml",
                "Accept": "text/xml, text/calendar",
            }
        )
        self.headers.update(headers or {})
        if self.url.username is not None:
            username = unquote(self.url.username)
            password = unquote(self.url.password)

        # Use discovered username if no explicit username was provided
        if username is None and discovered_username is not None:
            username = discovered_username
            log.debug(f"Using discovered username from RFC6764: {username}")

        self.username = username
        self.password = password
        self.auth = auth
        self.auth_type = auth_type

        ## I had problems with passwords with non-ascii letters in it ...
        if isinstance(self.password, str):
            self.password = self.password.encode("utf-8")
        if auth and self.auth_type:
            logging.error(
                "both auth object and auth_type sent to DAVClient.  The latter will be ignored."
            )
        elif self.auth_type:
            self.build_auth_object()

        # TODO: it's possible to force through a specific auth method here,
        # but no test code for this.
        self.timeout = timeout
        self.ssl_verify_cert = ssl_verify_cert
        self.ssl_cert = ssl_cert
        self.url = self.url.unauth()
        log.debug("self.url: " + str(url))

        self._principal = None

    def __enter__(self) -> Self:
        ## Used for tests, to set up a temporarily test server
        if hasattr(self, "setup"):
            try:
                self.setup()
            except:
                self.setup(self)
        return self

    def __exit__(
        self,
        exc_type: Optional[BaseException] = None,
        exc_value: Optional[BaseException] = None,
        traceback: Optional[TracebackType] = None,
    ) -> None:
        self.close()
        ## Used for tests, to tear down a temporarily test server
        if hasattr(self, "teardown"):
            try:
                self.teardown()
            except:
                self.teardown(self)

    def close(self) -> None:
        """
        Closes the DAVClient's session object
        """
        self.session.close()

    def principals(self, name=None):
        """
        Instead of returning the current logged-in principal, it attempts to query for all principals. This may or may not work dependent on the permissions and implementation of the calendar server.
        """
        if name:
            name_filter = [
                dav.PropertySearch()
                + [dav.Prop() + [dav.DisplayName()]]
                + dav.Match(value=name)
            ]
        else:
            name_filter = []

        query = (
            dav.PrincipalPropertySearch()
            + name_filter
            + [dav.Prop(), cdav.CalendarHomeSet(), dav.DisplayName()]
        )
        response = self.report(self.url, etree.tostring(query.xmlelement()))

        ## Possibly we should follow redirects (response status 3xx), but as
        ## for now we're just treating it in the same way as 4xx and 5xx -
        ## probably the server did not support the operation
        if response.status >= 300:
            raise error.ReportError(
                f"{response.status} {response.reason} - {response.raw}"
            )

        principal_dict = response.find_objects_and_props()
        ret = []
        for x in principal_dict:
            p = principal_dict[x]
            if not dav.DisplayName.tag in p:
                continue
            name = p[dav.DisplayName.tag].text
            error.assert_(not p[dav.DisplayName.tag].getchildren())
            error.assert_(not p[dav.DisplayName.tag].items())
            chs = p[cdav.CalendarHomeSet.tag]
            error.assert_(not chs.items())
            error.assert_(not chs.text)
            chs_href = chs.getchildren()
            error.assert_(len(chs_href) == 1)
            error.assert_(not chs_href[0].items())
            error.assert_(not chs_href[0].getchildren())
            chs_url = chs_href[0].text
            calendar_home_set = CalendarSet(client=self, url=chs_url)
            ret.append(
                Principal(
                    client=self, url=x, name=name, calendar_home_set=calendar_home_set
                )
            )
        return ret

    def principal(self, *largs, **kwargs):
        """
        Convenience method, it gives a bit more object-oriented feel to
        write client.principal() than Principal(client).

        This method returns a :class:`caldav.Principal` object, with
        higher-level methods for dealing with the principals
        calendars.
        """
        if not self._principal:
            self._principal = Principal(client=self, *largs, **kwargs)
        return self._principal

    def calendar(self, **kwargs):
        """Returns a calendar object.

        Typically, a URL should be given as a named parameter (url)

        No network traffic will be initiated by this method.

        If you don't know the URL of the calendar, use
        client.principal().calendar(...) instead, or
        client.principal().calendars()
        """
        return Calendar(client=self, **kwargs)

    def check_dav_support(self) -> Optional[str]:
        """
        Does a probe towards the server and returns True if it says it supports RFC4918 / DAV
        """
        try:
            ## SOGo does not return the full capability list on the caldav
            ## root URL, and that's OK according to the RFC ... so apparently
            ## we need to do an extra step here to fetch the URL of some
            ## element that should come with caldav extras.
            ## Anyway, packing this into a try-except in case it fails.
            response = self.options(self.principal().url)
        except:
            response = self.options(str(self.url))
        return response.headers.get("DAV", None)

    def check_cdav_support(self) -> bool:
        """
        Does a probe towards the server and returns True if it says it supports RFC4791 / CalDAV
        """
        support_list = self.check_dav_support()
        return support_list is not None and "calendar-access" in support_list

    def check_scheduling_support(self) -> bool:
        """
        Does a probe towards the server and returns True if it says it supports RFC6833 / CalDAV Scheduling
        """
        support_list = self.check_dav_support()
        return support_list is not None and "calendar-auto-schedule" in support_list

    def propfind(
        self, url: Optional[str] = None, props: str = "", depth: int = 0
    ) -> DAVResponse:
        """
        Send a propfind request.

        Parameters
        ----------
        url : URL
            url for the root of the propfind.
        props : xml
            properties we want
        depth : int
            maximum recursion depth

        Returns
        -------
        DAVResponse
        """
        return self.request(
            url or str(self.url), "PROPFIND", props, {"Depth": str(depth)}
        )

    def proppatch(self, url: str, body: str, dummy: None = None) -> DAVResponse:
        """
        Send a proppatch request.

        Args:
            url: url for the root of the propfind.
            body: XML propertyupdate request
            dummy: compatibility parameter

        Returns:
            DAVResponse
        """
        return self.request(url, "PROPPATCH", body)

    def report(self, url: str, query: str = "", depth: int = 0) -> DAVResponse:
        """
        Send a report request.

        Args:
            url: url for the root of the propfind.
            query: XML request
            depth: maximum recursion depth

        Returns
            DAVResponse
        """
        return self.request(
            url,
            "REPORT",
            query,
            {"Depth": str(depth), "Content-Type": 'application/xml; charset="utf-8"'},
        )

    def mkcol(self, url: str, body: str, dummy: None = None) -> DAVResponse:
        """
        Send a MKCOL request.

        MKCOL is basically not used with caldav, one should use
        MKCALENDAR instead.  However, some calendar servers MAY allow
        "subcollections" to be made in a calendar, by using the MKCOL
        query.  As for 2020-05, this method is not exercised by test
        code or referenced anywhere else in the caldav library, it's
        included just for the sake of completeness.  And, perhaps this
        DAVClient class can be used for vCards and other WebDAV
        purposes.

        Args:
            url: url for the root of the mkcol
            body: XML request
            dummy: compatibility parameter

        Returns:
            DAVResponse
        """
        return self.request(url, "MKCOL", body)

    def mkcalendar(self, url: str, body: str = "", dummy: None = None) -> DAVResponse:
        """
        Send a mkcalendar request.

        Args:
            url: url for the root of the mkcalendar
            body: XML request
            dummy: compatibility parameter

        Returns:
            DAVResponse
        """
        return self.request(url, "MKCALENDAR", body)

    def put(
        self, url: str, body: str, headers: Mapping[str, str] = None
    ) -> DAVResponse:
        """
        Send a put request.
        """
        return self.request(url, "PUT", body, headers or {})

    def post(
        self, url: str, body: str, headers: Mapping[str, str] = None
    ) -> DAVResponse:
        """
        Send a POST request.
        """
        return self.request(url, "POST", body, headers or {})

    def delete(self, url: str) -> DAVResponse:
        """
        Send a delete request.
        """
        return self.request(url, "DELETE")

    def options(self, url: str) -> DAVResponse:
        """
        Send an options request.
        """
        return self.request(url, "OPTIONS")

    def extract_auth_types(self, header: str):
        """This is probably meant for internal usage.  It takes the
        headers it got from the server and figures out what
        authentication types the server supports
        """
        # https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/WWW-Authenticate#syntax
        return {h.split()[0] for h in header.lower().split(",")}

    def build_auth_object(self, auth_types: Optional[List[str]] = None):
        """Fixes self.auth.  If ``self.auth_type`` is given, then
        insist on using this one.  If not, then assume auth_types to
        be a list of acceptable auth types and choose the most
        appropriate one (prefer digest or basic if username is given,
        and bearer if password is given).

        Args:
            auth_types - A list/tuple of acceptable auth_types
        """
        auth_type = self.auth_type
        if not auth_type and not auth_types:
            raise error.AuthorizationError(
                "No auth-type given.  This shouldn't happen.  Raise an issue at https://github.com/python-caldav/caldav/issues/ or by email noauthtype@plann.no"
            )
        if auth_types and auth_type and auth_type not in auth_types:
            raise error.AuthorizationError(
                reason=f"Configuration specifies to use {auth_type}, but server only accepts {auth_types}"
            )
        if not auth_type and auth_types:
            if self.username and "digest" in auth_types:
                auth_type = "digest"
            elif self.username and "basic" in auth_types:
                auth_type = "basic"
            elif self.password and "bearer" in auth_types:
                auth_type = "bearer"
            elif "bearer" in auth_types:
                raise error.AuthorizationError(
                    reason="Server provides bearer auth, but no password given.  The bearer token should be configured as password"
                )

        if auth_type == "digest":
            self.auth = requests.auth.HTTPDigestAuth(self.username, self.password)
        elif auth_type == "basic":
            self.auth = requests.auth.HTTPBasicAuth(self.username, self.password)
        elif auth_type == "bearer":
            self.auth = HTTPBearerAuth(self.password)

    def request(
        self,
        url: str,
        method: str = "GET",
        body: str = "",
        headers: Mapping[str, str] = None,
    ) -> DAVResponse:
        """
        Actually sends the request, and does the authentication
        """
        headers = headers or {}

        combined_headers = self.headers.copy()
        combined_headers.update(headers or {})
        if (body is None or body == "") and "Content-Type" in combined_headers:
            del combined_headers["Content-Type"]

        # objectify the url
        url_obj = URL.objectify(url)

        proxies = None
        if self.proxy is not None:
            proxies = {url_obj.scheme: self.proxy}
            log.debug("using proxy - %s" % (proxies))

        log.debug(
            "sending request - method={0}, url={1}, headers={2}\nbody:\n{3}".format(
                method, str(url_obj), combined_headers, to_normal_str(body)
            )
        )

        try:
            r = self.session.request(
                method,
                str(url_obj),
                data=to_wire(body),
                headers=combined_headers,
                proxies=proxies,
                auth=self.auth,
                timeout=self.timeout,
                verify=self.ssl_verify_cert,
                cert=self.ssl_cert,
            )
            log.debug("server responded with %i %s" % (r.status_code, r.reason))
            if (
                r.status_code == 401
                and "text/html" in self.headers.get("Content-Type", "")
                and not self.auth
            ):
                # The server can return HTML on 401 sometimes (ie. it's behind a proxy)
                # The user can avoid logging errors by setting the authentication type by themselves.
                msg = (
                    "No authentication object was provided. "
                    "HTML was returned when probing the server for supported authentication types. "
                    "To avoid logging errors, consider passing the auth_type connection parameter"
                )
                if r.headers.get("WWW-Authenticate"):
                    auth_types = [
                        t
                        for t in self.extract_auth_types(r.headers["WWW-Authenticate"])
                        if t in ["basic", "digest", "bearer"]
                    ]
                    if auth_types:
                        msg += "\nSupported authentication types: %s" % (
                            ", ".join(auth_types)
                        )
                log.warning(msg)
            response = DAVResponse(r, self)
        except:
            ## this is a workaround needed due to some weird server
            ## that would just abort the connection rather than send a
            ## 401 when an unauthenticated request with a body was
            ## sent to the server - ref https://github.com/python-caldav/caldav/issues/158
            if self.auth or not self.password:
                raise
            r = self.session.request(
                method="GET",
                url=str(url_obj),
                headers=combined_headers,
                proxies=proxies,
                timeout=self.timeout,
                verify=self.ssl_verify_cert,
                cert=self.ssl_cert,
            )
            if not r.status_code == 401:
                raise

        ## Returned headers
        r_headers = CaseInsensitiveDict(r.headers)
        if (
            r.status_code == 401
            and "WWW-Authenticate" in r_headers
            and not self.auth
            and (self.username or self.password)
        ):
            auth_types = self.extract_auth_types(r_headers["WWW-Authenticate"])
            self.build_auth_object(auth_types)

            if not self.auth:
                raise NotImplementedError(
                    "The server does not provide any of the currently "
                    "supported authentication methods: basic, digest, bearer"
                )

            return self.request(url, method, body, headers)

        elif (
            r.status_code == 401
            and "WWW-Authenticate" in r_headers
            and self.auth
            and self.password
            and isinstance(self.password, bytes)
        ):
            ## TODO: this has become a mess and should be refactored.
            ## (Arguably, this logic doesn't belong here at all.
            ## with niquests it's possible to just pass the username
            ## and password, maybe we should try that?)

            ## Most likely we're here due to wrong username/password
            ## combo, but it could also be a multiplexing problem.
            if (
                self.features.is_supported("http.multiplexing", return_defaults=False)
                is None
            ):
                self.session = requests.Session()
                self.features.set_feature("http.multiplexing", "unknown")
                ## If this one also fails, we give up
                ret = self.request(str(url_obj), method, body, headers)
                self.features.set_feature("http.multiplexing", False)
                return ret

            ## Most likely we're here due to wrong username/password
            ## combo, but it could also be charset problems.  Some
            ## (ancient) servers don't like UTF-8 binary auth with
            ## Digest authentication.  An example are old SabreDAV
            ## based servers.  Not sure about UTF-8 and Basic Auth,
            ## but likely the same.  so retry if password is a bytes
            ## sequence and not a string (see commit 13a4714, which
            ## introduced this regression)

            auth_types = self.extract_auth_types(r_headers["WWW-Authenticate"])
            self.password = self.password.decode()
            self.build_auth_object(auth_types)

            self.username = None
            self.password = None

            return self.request(str(url_obj), method, body, headers)

        if error.debug_dump_communication:
            import datetime
            from tempfile import NamedTemporaryFile

            with NamedTemporaryFile(prefix="caldavcomm", delete=False) as commlog:
                commlog.write(b"=" * 80 + b"\n")
                commlog.write(f"{datetime.datetime.now():%FT%H:%M:%S}".encode("utf-8"))
                commlog.write(b"\n====>\n")
                commlog.write(f"{method} {url}\n".encode("utf-8"))
                commlog.write(
                    b"\n".join(to_wire(f"{x}: {headers[x]}") for x in headers)
                )
                commlog.write(b"\n\n")
                commlog.write(to_wire(body))
                commlog.write(b"<====\n")
                commlog.write(f"{response.status} {response.reason}".encode("utf-8"))
                commlog.write(
                    b"\n".join(
                        to_wire(f"{x}: {response.headers[x]}") for x in response.headers
                    )
                )
                commlog.write(b"\n\n")
                ct = response.headers.get("Content-Type", "")
                if response.tree is not None:
                    commlog.write(
                        to_wire(etree.tostring(response.tree, pretty_print=True))
                    )
                else:
                    commlog.write(to_wire(response._raw))
                commlog.write(b"\n")

        # this is an error condition that should be raised to the application
        if (
            response.status == requests.codes.forbidden
            or response.status == requests.codes.unauthorized
        ):
            try:
                reason = response.reason
            except AttributeError:
                reason = "None given"
            raise error.AuthorizationError(url=str(url_obj), reason=reason)

        return response


def auto_calendars(
    config_file: str = None,
    config_section: str = "default",
    testconfig: bool = False,
    environment: bool = True,
    config_data: dict = None,
    config_name: str = None,
) -> Iterable["Calendar"]:
    """
    This will replace plann.lib.findcalendars()
    """
    raise NotImplementedError("auto_calendars not implemented yet")


def auto_calendar(*largs, **kwargs) -> Iterable["Calendar"]:
    """
    Alternative to auto_calendars - in most use cases, one calendar suffices
    """
    return next(auto_calendars(*largs, **kwargs), None)


def auto_conn(*largs, config_data: dict = None, **kwargs):
    """A quite stubbed verison of get_davclient was included in the
    v1.5-release as auto_conn, but renamed a few days later.  Probably
    nobody except my caldav tester project uses auto_conn, but as a
    thumb of rule anything released should stay "deprecated" for at
    least one major release before being removed.

    TODO: remove in version 3.0
    """
    warnings.warn(
        "auto_conn was renamed get_davclient",
        DeprecationWarning,
        stacklevel=2,
    )
    if config_data:
        kwargs.update(config_data)
    return get_davclient(*largs, **kwargs)


def get_davclient(
    check_config_file: bool = True,
    config_file: str = None,
    config_section: str = None,
    testconfig: bool = False,
    environment: bool = True,
    name: str = None,
    **config_data,
) -> "DAVClient":
    """
    This function will yield a DAVClient object.  It will not try to
    connect (see auto_calendars for that).  It will read configuration
    from various sources, dependent on the parameters given, in this
    order:

    * Data from the parameters given
    * Environment variables prepended with `CALDAV_`, like `CALDAV_URL`, `CALDAV_USERNAME`, `CALDAV_PASSWORD`.
    * Environment variables `PYTHON_CALDAV_USE_TEST_SERVER` and `CALDAV_CONFIG_FILE` will be honored if environment is set
    * Data from `./tests/conf.py` or `./conf.py` (this includes the possibility to spin up a test server)
    * Configuration file.  Documented in the plann project as for now.  (TODO - move it)
    """
    if config_data:
        return DAVClient(**config_data)

    if testconfig or (environment and os.environ.get("PYTHON_CALDAV_USE_TEST_SERVER")):
        sys.path.insert(0, "tests")
        sys.path.insert(1, ".")
        ## TODO: move the code from client into here
        try:
            from conf import client

            idx = os.environ.get("PYTHON_CALDAV_TEST_SERVER_IDX")
            try:
                idx = int(idx)
            except (ValueError, TypeError):
                idx = None
            name = name or os.environ.get("PYTHON_CALDAV_TEST_SERVER_NAME")
            if name and not idx:
                try:
                    idx = int(name)
                    name = None
                except ValueError:
                    pass
            conn = client(idx, name)
            if conn:
                return conn
        except ImportError:
            pass
        finally:
            sys.path = sys.path[2:]

    if environment:
        conf = {}
        for conf_key in (
            x
            for x in os.environ
            if x.startswith("CALDAV_") and not x.startswith("CALDAV_CONFIG")
        ):
            conf[conf_key[7:].lower()] = os.environ[conf_key]
        if conf:
            return DAVClient(**conf)
        if not config_file:
            config_file = os.environ.get("CALDAV_CONFIG_FILE")
        if not config_section:
            config_section = os.environ.get("CALDAV_CONFIG_SECTION")

    if check_config_file:
        ## late import in 2.0, as the config stuff isn't properly tested
        from . import config

        if not config_section:
            config_section = "default"

        cfg = config.read_config(config_file)
        if cfg:
            section = config.config_section(cfg, config_section)
            conn_params = {}
            for k in section:
                if k.startswith("caldav_") and section[k]:
                    key = k[7:]
                    if key == "pass":
                        key = "password"
                    if key == "user":
                        key = "username"
                    conn_params[key] = section[k]
            if conn_params:
                return DAVClient(**conn_params)
