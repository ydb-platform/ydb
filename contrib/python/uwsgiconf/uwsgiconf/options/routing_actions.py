from pathlib import Path
from typing import Union

from ..base import ParametrizedValue
from ..utils import KeyValue, filter_locals


class RouteAction(ParametrizedValue):

    pass


class ActionFlush(RouteAction):
    """Send the current contents of the transformation buffer
    to the client (without clearing the buffer).

    * http://uwsgi.readthedocs.io/en/latest/Transformations.html#flushing-magic

    """
    name = 'flush'

    def __init__(self):
        super().__init__()


class ActionGzip(RouteAction):
    """Encodes the response buffer to gzip."""

    name = 'gzip'
    plugin = 'transformation_gzip'

    def __init__(self):
        super().__init__()


class ActionToFile(RouteAction):
    """Used for caching a response buffer into a static file."""

    name = 'tofile'
    plugin = 'transformation_tofile'

    def __init__(self, filename, *, mode=None):
        arg = KeyValue(locals())
        super().__init__(arg)


class ActionUpper(RouteAction):
    """Transforms each character in uppercase.

    Mainly as an example of transformation plugin.

    """
    name = 'toupper'
    plugin = 'transformation_toupper'

    def __init__(self):
        super().__init__()


class ActionChunked(RouteAction):
    """Encodes the output in HTTP chunked."""

    name = 'chunked'

    def __init__(self):
        super().__init__()


class ActionTemplate(RouteAction):
    """Allows using a template file to expose everything
    from internal routing system into it.

    * http://uwsgi.readthedocs.io/en/latest/Changelog-1.9.19.html#the-template-transformation

    """
    name = 'template'

    def __init__(self):
        super().__init__()


class ActionFixContentLen(RouteAction):
    """Fixes Content-length header."""

    name = 'fixcl'

    def __init__(self, *, add_header=False):
        """
        :param bool add_header: Force header add instead of plain fix of existing header.
        """
        if add_header:
            self.name = 'forcexcl'

        super().__init__()


class ActionDoContinue(RouteAction):
    """Stop scanning the internal routing table
    and continue to the selected request handler.

    """
    name = 'continue'

    def __init__(self):
        super().__init__()


class ActionDoBreak(RouteAction):
    """Stop scanning the internal routing table and close the request."""

    name = 'break'

    def __init__(self, code, *, return_body=False):
        """
        :param int code: HTTP code

        :param return_body: Uses uWSGI's built-in status code and returns
            both status code and message body.

        """
        if return_body:
            self.name = 'return'

        super().__init__(code)


class ActionLog(RouteAction):
    """Print the specified message in the logs or
    do not log a request is ``message`` is ``None``.

    """
    name = 'log'

    def __init__(self, message):
        """

        :param str|None message: Message to add into log.
            If ``None`` logging will be disabled for this request.

        """
        if message is None:
            self.name = 'donotlog'

        super().__init__(message)


class ActionOffloadOff(RouteAction):
    """Do not use offloading."""

    name = 'donotoffload'

    def __init__(self):
        super().__init__()


class ActionAddVarLog(RouteAction):
    """Add the specified logvar."""

    name = 'logvar'

    def __init__(self, name, val):
        """
        :param str name: Variable name.

        :param val: Variable value.
        """
        super().__init__(name, val)


class ActionDoGoto(RouteAction):
    """Make a forward jump to the specified label or rule position."""

    name = 'goto'

    def __init__(self, where):
        """
        :param str|int where: Rule number of label to go to.
        """
        super().__init__(where)


class ActionAddVarCgi(RouteAction):
    """Add the specified CGI (environment) variable to the request."""

    name = 'addvar'

    def __init__(self, name, val):
        """
        :param str name: Variable name.

        :param val: Variable value.
        """
        super().__init__(name, val)


class ActionHeaderAdd(RouteAction):
    """Add the specified HTTP header to the response."""

    name = 'addheader'

    def __init__(self, name, val):
        """
        :param str name: Header name.

        :param val: Header value.
        """
        name += ':'

        super().__init__(name, val)


class ActionHeaderRemove(RouteAction):
    """Remove the specified HTTP header from the response."""

    name = 'delheader'

    def __init__(self, name):
        """
        :param str name: Header name.
        """
        super().__init__(name)


class ActionHeadersOff(RouteAction):
    """Disable headers."""

    name = 'disableheaders'

    def __init__(self):
        super().__init__()


class ActionHeadersReset(RouteAction):
    """Clear the response headers, setting a new HTTP status code,
    useful for resetting a response.

    """
    name = 'clearheaders'

    def __init__(self, code):
        """
        :param int code: HTTP code.
        """
        super().__init__(code)


class ActionSignal(RouteAction):
    """Raise the specified uwsgi signal."""

    name = 'signal'

    def __init__(self, num):
        """
        :param int num: Signal number.
        """
        super().__init__(num)


class ActionSend(RouteAction):
    """Extremely advanced (and dangerous) function allowing you
    to add raw data to the response.

    """
    name = 'send'

    def __init__(self, data, *, crnl: bool = False):
        """
        :param data: Data to add to response.
        :param crnl: Add carriage return and new line.

        """
        if crnl:
            self.name = 'send-crnl'
        super().__init__(data)


class ActionRedirect(RouteAction):
    """Return a HTTP 301/302 Redirect to the specified URL."""

    name = 'redirect-302'
    plugin = 'router_redirect'

    def __init__(self, url, *, permanent=False):
        """
        :param str url: URL to redirect to.
        :param bool permanent: If ``True`` use 301, otherwise 302.
        """
        if permanent:
            self.name = 'redirect-301'

        super().__init__(url)


class ActionRewrite(RouteAction):
    """A rewriting engine inspired by Apache mod_rewrite.

    Rebuild PATH_INFO and QUERY_STRING according to the specified rules
    before the request is dispatched to the request handler.

    """
    name = 'rewrite'
    plugin = 'router_rewrite'

    def __init__(self, rule, *, do_continue=False):
        """
        :param str rule: A rewrite rule.

        :param bool do_continue: Stop request processing
            and continue to the selected request handler.

        """
        if do_continue:
            self.name = 'rewrite-last'

        super().__init__(rule)


class ActionRouteUwsgi(RouteAction):
    """Rewrite the modifier1, modifier2 and optionally UWSGI_APPID values of a request
    or route the request to an external uwsgi server.

    """
    name = 'uwsgi'
    plugin = 'router_uwsgi'
    args_joiner = ','

    def __init__(self, external_address='', *, modifier='', app=''):
        """
        :param str external_address: External uWSGI server address (host:port).
        :param Modifier modifier: Set request modifier.
        :param str app: Set ``UWSGI_APPID``.

        """
        super().__init__(external_address, modifier, modifier.submod, app)


class ActionRouteExternal(RouteAction):
    """Route the request to an external HTTP server."""

    name = 'http'
    plugin = 'router_http'
    args_joiner = ','

    def __init__(self, address, *, host_header=None):
        """
        :param str address: External HTTP address (host:port)

        :param str host_header: HOST header value.

        """
        super().__init__(address, host_header)


class ActionAlarm(RouteAction):
    """Triggers an alarm.

    * http://uwsgi.readthedocs.io/en/latest/Changelog-1.9.6.html#the-alarm-routing-action

    """
    name = 'alarm'

    def __init__(self, name, message):
        """
        :param str name: Alarm name

        :param str message: Message to pass into alarm.
        """
        super().__init__(name, message)


class ActionServeStatic(RouteAction):
    """Serve a static file from the specified physical path."""

    name = 'static'
    plugin = 'router_static'

    def __init__(self, fpath: Union[str, Path]):
        """
        :param fpath: Static file path.

        """
        super().__init__(str(fpath))


class ActionAuthBasic(RouteAction):
    """Use Basic HTTP Auth."""

    name = 'basicauth'
    plugin = 'router_basicauth'
    args_joiner = ','

    def __init__(self, realm, *, user=None, password=None, do_next=False):
        """
        :param str realm:

        :param str user:

        :param str password: Password or htpasswd-like file.

        :param bool do_next: Allow next rule.
        """
        if do_next:
            self.name = 'basicauth-next'

        user_password = []

        if user:
            user += ':'
            user_password.append(user)

        if password:
            user_password.append(password)

        super().__init__(realm, ''.join(user_password) if user_password else None)


class AuthLdap(RouteAction):
    """Use Basic HTTP Auth."""

    name = 'ldapauth'
    plugin = 'ldap'
    args_joiner = ','

    def __init__(
            self, realm, address, *, base_dn=None, bind_dn=None, bind_password=None,
            filter=None, login_attr=None, log_level=None,
            do_next=False):
        """

        :param str realm:

        :param str address: LDAP server URI

        :param str base_dn: Base DN used when searching for users.

        :param str bind_dn: DN used for binding.
            Required if the LDAP server does not allow anonymous searches.

        :param str bind_password: Password for the ``bind_dn`` user.

        :param str filter: Filter used when searching for users. Default: ``(objectClass=*)``

        :param str login_attr: LDAP attribute that holds user login. Default: ``uid``.

        :param str log_level: Log level.

            Supported values:
                * 0 - don't log any binds
                * 1 - log authentication errors,
                * 2 - log both successful and failed binds

        :param bool do_next: Allow next rule.
        """
        arg = KeyValue(
            filter_locals(locals(), drop=['realm', 'do_next']),
            aliases={
                'address': 'url',
                'base_dn': 'basedn',
                'bind_dn': 'binddn',
                'bind_password': 'bindpw',
                'login_attr': 'attr',
                'log_level': 'loglevel',
            },
            items_separator=';'
        )

        if do_next:
            self.name = 'ldapauth-next'

        super().__init__(realm, arg)


class ActionSetHarakiri(RouteAction):
    """Set harakiri timeout for the current request."""

    name = 'harakiri'

    def __init__(self, timeout):
        """
        :param int timeout:
        """
        super().__init__(timeout)


class ActionDirChange(RouteAction):
    """Changes a directory."""

    name = 'chdir'

    def __init__(self, dir):
        """
        :param str dir: Directory to change into.
        """
        super().__init__(dir)


class ActionSetVarUwsgiAppid(RouteAction):
    """Set UWSGI_APPID.

    Bypass ``SCRIPT_NAME`` and ``VirtualHosting`` to let the user choose
    the mountpoint without limitations (or headaches).

    The concept is very generic: ``UWSGI_APPID`` is the identifier of an application.
    If it is not found in the internal list of apps, it will be loaded.

    """
    name = 'setapp'

    def __init__(self, app):
        """
        :param str app: Application ID.
        """
        super().__init__(app)


class ActionSetVarRemoteUser(RouteAction):
    """Set REMOTE_USER"""

    name = 'setuser'

    def __init__(self, user):
        """
        :param str user: Username.
        """
        super().__init__(user)


class ActionSetVarUwsgiHome(RouteAction):
    """Set UWSGI_HOME"""

    name = 'sethome'

    def __init__(self, dir):
        """
        :param str dir: Directory to make a new home.
        """
        super().__init__(dir)


class ActionSetVarUwsgiScheme(RouteAction):
    """Set UWSGI_SCHEME.

    Set the URL scheme when it cannot be reliably determined.
    This may be used to force HTTPS (with the value ``https``), for instance.

    * http://uwsgi.readthedocs.io/en/latest/Changelog-1.9.6.html#configuring-dynamic-apps-with-internal-routing

    """
    name = 'setscheme'

    def __init__(self, value):
        """
        :param str value:
        """
        super().__init__(value)


class ActionSetVarScriptName(RouteAction):
    """Set SCRIPT_NAME"""

    name = 'setscriptname'

    def __init__(self, name):
        """
        :param str name: Script name
        """
        super().__init__(name)


class ActionSetVarRequestMethod(RouteAction):
    """Set REQUEST_METHOD"""

    name = 'setmethod'

    def __init__(self, name):
        """
        :param str name: Method name.
        """
        super().__init__(name)


class ActionSetVarRequestUri(RouteAction):
    """Set REQUEST_URI"""

    name = 'seturi'

    def __init__(self, value):
        """
        :param str value: URI
        """
        super().__init__(value)


class ActionSetVarRemoteAddr(RouteAction):
    """Set REMOTE_ADDR"""

    name = 'setremoteaddr'

    def __init__(self, value):
        """
        :param str value: Address.
        """
        super().__init__(value)


class ActionSetVarPathInfo(RouteAction):
    """Set PATH_INFO"""

    name = 'setpathinfo'

    def __init__(self, value):
        """
        :param str value: New info.
        """
        super().__init__(value)


class ActionSetVarDocumentRoot(RouteAction):
    """Set DOCUMENT_ROOT"""

    name = 'setdocroot'

    def __init__(self, value):
        """
        :param str value:
        """
        super().__init__(value)


class ActionSetUwsgiProcessName(RouteAction):
    """Set uWSGI process name."""

    name = 'setprocname'

    def __init__(self, name):
        """
        :param str name: New process name.
        """
        super().__init__(name)


class ActionFixVarPathInfo(RouteAction):
    """Fixes PATH_INFO taking into account script name.

    This action allows you to set SCRIPT_NAME in nginx without bothering
    to rewrite the PATH_INFO (something nginx cannot afford).

    * http://uwsgi.readthedocs.io/en/latest/Changelog-2.0.11.html#fixpathinfo-routing-action

    """
    name = 'fixpathinfo'

    def __init__(self):
        super().__init__()


class ActionSetScriptFile(RouteAction):
    """Set script file.

    * http://uwsgi.readthedocs.io/en/latest/Changelog-1.9.6.html#configuring-dynamic-apps-with-internal-routing

    """
    name = 'setfile'

    def __init__(self, fpath: Union[str, Path]):
        """
        :param str fpath: File path.

        """
        super().__init__(str(fpath))
