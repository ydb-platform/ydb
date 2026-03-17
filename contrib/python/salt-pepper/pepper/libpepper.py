'''
A Python library for working with Salt's REST API

(Specifically the rest_cherrypy netapi module.)

'''
import json
import logging
import re
import ssl

from pepper.exceptions import PepperException

try:
    ssl._create_default_https_context = ssl._create_stdlib_context
except Exception:
    pass

try:
    from urllib.request import HTTPHandler, HTTPSHandler, Request, urlopen, \
        install_opener, build_opener
    from urllib.error import HTTPError, URLError
    import urllib.parse as urlparse
except ImportError:
    from urllib2 import HTTPHandler, HTTPSHandler, Request, urlopen, install_opener, build_opener, \
        HTTPError, URLError
    import urlparse

logger = logging.getLogger(__name__)


class Pepper(object):
    '''
    A thin wrapper for making HTTP calls to the salt-api rest_cherrpy REST
    interface

    >>> api = Pepper('https://localhost:8000')
    >>> api.login('saltdev', 'saltdev', 'pam')
    {"return": [
            {
                "eauth": "pam",
                "expire": 1370434219.714091,
                "perms": [
                    "test.*"
                ],
                "start": 1370391019.71409,
                "token": "c02a6f4397b5496ba06b70ae5fd1f2ab75de9237",
                "user": "saltdev"
            }
        ]
    }
    >>> api.low([{'client': 'local', 'tgt': '*', 'fun': 'test.ping'}])
    {u'return': [{u'ms-0': True,
              u'ms-1': True,
              u'ms-2': True,
              u'ms-3': True,
              u'ms-4': True}]}

    '''
    def __init__(self, api_url='https://localhost:8000', debug_http=False, ignore_ssl_errors=False):
        '''
        Initialize the class with the URL of the API

        :param api_url: Host or IP address of the salt-api URL;
            include the port number

        :param debug_http: Add a flag to urllib2 to output the HTTP exchange

        :param ignore_ssl_errors: Add a flag to urllib2 to ignore invalid SSL certificates

        :raises PepperException: if the api_url is misformed

        '''
        split = urlparse.urlsplit(api_url)
        if split.scheme not in ['http', 'https']:
            raise PepperException("salt-api URL missing HTTP(s) protocol: {0}"
                                  .format(api_url))

        self.api_url = api_url
        self.debug_http = int(debug_http)
        self._ssl_verify = not ignore_ssl_errors
        self.auth = {}
        self.salt_version = None

    def req_stream(self, path):
        '''
        A thin wrapper to get a response from saltstack api.
        The body of the response will not be downloaded immediately.
        Make sure to close the connection after use.
        api = Pepper('http://ipaddress/api/')
        print(api.login('salt','salt','pam'))
        response = api.req_stream('/events')

        :param path: The path to the salt api resource

        :return: :class:`Response <Response>` object

        :rtype: requests.Response
        '''
        import requests

        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'X-Requested-With': 'XMLHttpRequest',
        }
        if self.auth and 'token' in self.auth and self.auth['token']:
            headers.setdefault('X-Auth-Token', self.auth['token'])
        else:
            raise PepperException('Authentication required')
            return
        params = {'url': self._construct_url(path),
                  'headers': headers,
                  'verify': self._ssl_verify is True,
                  'stream': True
                  }
        try:
            resp = requests.get(**params)

            if resp.status_code == 401:
                raise PepperException(str(resp.status_code) + ':Authentication denied')
                return

            if resp.status_code == 500:
                raise PepperException(str(resp.status_code) + ':Server error.')
                return

            if resp.status_code == 404:
                raise PepperException(str(resp.status_code) + ' :This request returns nothing.')
                return
        except PepperException as e:
            print(e)
            return
        return resp

    def req_get(self, path):
        '''
        A thin wrapper from get http method of saltstack api
        api = Pepper('http://ipaddress/api/')
        print(api.login('salt','salt','pam'))
        print(api.req_get('/keys'))
        '''
        import requests

        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'X-Requested-With': 'XMLHttpRequest',
        }
        if self.auth and 'token' in self.auth and self.auth['token']:
            headers.setdefault('X-Auth-Token', self.auth['token'])
        else:
            raise PepperException('Authentication required')
            return
        params = {'url': self._construct_url(path),
                  'headers': headers,
                  'verify': self._ssl_verify is True,
                  }
        try:
            resp = requests.get(**params)

            if resp.status_code == 401:
                raise PepperException(str(resp.status_code) + ':Authentication denied')
                return

            if resp.status_code == 500:
                raise PepperException(str(resp.status_code) + ':Server error.')
                return

            if resp.status_code == 404:
                raise PepperException(str(resp.status_code) + ' :This request returns nothing.')
                return
        except PepperException as e:
            print(e)
            return
        return resp.json()

    def req(self, path, data=None):
        '''
        A thin wrapper around urllib2 to send requests and return the response

        If the current instance contains an authentication token it will be
        attached to the request as a custom header.

        :rtype: dictionary

        '''
        if ((hasattr(data, 'get') and data.get('eauth') == 'kerberos')
                or self.auth.get('eauth') == 'kerberos'):
            return self.req_requests(path, data)

        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'X-Requested-With': 'XMLHttpRequest',
        }

        opener = build_opener()
        for handler in opener.handlers:
            if isinstance(handler, HTTPHandler):
                handler.set_http_debuglevel(self.debug_http)
            if isinstance(handler, HTTPSHandler):
                handler.set_http_debuglevel(self.debug_http)
        install_opener(opener)

        # Build POST data
        if data is not None:
            postdata = json.dumps(data).encode()
            clen = len(postdata)
        else:
            postdata = None

        # Create request object
        url = self._construct_url(path)
        req = Request(url, postdata, headers)

        # Add POST data to request
        if data is not None:
            req.add_header('Content-Length', clen)

        # Add auth header to request
        if path != '/run' and self.auth and 'token' in self.auth and self.auth['token']:
            req.add_header('X-Auth-Token', self.auth['token'])

        # Send request
        try:
            if not (self._ssl_verify):
                con = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
                f = urlopen(req, context=con)
            else:
                f = urlopen(req)
            content = f.read().decode('utf-8')
            if (self.debug_http):
                logger.debug('Response: %s', content)
            ret = json.loads(content)

            if not self.salt_version and 'x-salt-version' in f.headers:
                self._parse_salt_version(f.headers['x-salt-version'])

        except (HTTPError, URLError) as exc:
            logger.debug('Error with request', exc_info=True)
            status = getattr(exc, 'code', None)

            if status == 401:
                raise PepperException('Authentication denied')

            if status == 500:
                raise PepperException('Server error.')

            logger.error('Error with request: {0}'.format(exc))
            raise
        except AttributeError:
            logger.debug('Error converting response from JSON', exc_info=True)
            raise PepperException('Unable to parse the server response.')

        return ret

    def req_requests(self, path, data=None):
        '''
        A thin wrapper around request and request_kerberos to send
        requests and return the response

        If the current instance contains an authentication token it will be
        attached to the request as a custom header.

        :rtype: dictionary

        '''
        import requests
        from requests_gssapi import HTTPSPNEGOAuth, OPTIONAL
        auth = HTTPSPNEGOAuth(mutual_authentication=OPTIONAL)
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'X-Requested-With': 'XMLHttpRequest',
        }
        if self.auth and 'token' in self.auth and self.auth['token']:
            headers.setdefault('X-Auth-Token', self.auth['token'])
        # Optionally toggle SSL verification
        params = {'url': self._construct_url(path),
                  'headers': headers,
                  'verify': self._ssl_verify is True,
                  'auth': auth,
                  'data': json.dumps(data),
                  }
        logger.debug('postdata {0}'.format(params))
        resp = requests.post(**params)
        if resp.status_code == 401:
            # TODO should be resp.raise_from_status
            raise PepperException('Authentication denied')
        if resp.status_code == 500:
            # TODO should be resp.raise_from_status
            raise PepperException('Server error.')

        if not self.salt_version and 'x-salt-version' in resp.headers:
            self._parse_salt_version(resp.headers['x-salt-version'])

        return resp.json()

    def low(self, lowstate, path='/'):
        '''
        Execute a command through salt-api and return the response

        :param string path: URL path to be joined with the API hostname

        :param list lowstate: a list of lowstate dictionaries
        '''
        return self.req(path, lowstate)

    def local(self, tgt, fun, arg=None, kwarg=None, expr_form='glob',
              timeout=None, ret=None):
        '''
        Run a single command using the ``local`` client

        Wraps :meth:`low`.
        '''
        low = {
            'client': 'local',
            'tgt': tgt,
            'fun': fun,
        }

        if arg:
            low['arg'] = arg

        if kwarg:
            low['kwarg'] = kwarg

        if expr_form:
            low['expr_form'] = expr_form

        if timeout:
            low['timeout'] = timeout

        if ret:
            low['ret'] = ret

        return self.low([low])

    def local_async(self, tgt, fun, arg=None, kwarg=None, expr_form='glob',
                    timeout=None, ret=None):
        '''
        Run a single command using the ``local_async`` client

        Wraps :meth:`low`.
        '''
        low = {
            'client': 'local_async',
            'tgt': tgt,
            'fun': fun,
        }

        if arg:
            low['arg'] = arg

        if kwarg:
            low['kwarg'] = kwarg

        if expr_form:
            low['expr_form'] = expr_form

        if timeout:
            low['timeout'] = timeout

        if ret:
            low['ret'] = ret

        return self.low([low])

    def local_batch(self, tgt, fun, arg=None, kwarg=None, expr_form='glob',
                    batch='50%', ret=None):
        '''
        Run a single command using the ``local_batch`` client

        Wraps :meth:`low`.
        '''
        low = {
            'client': 'local_batch',
            'tgt': tgt,
            'fun': fun,
        }

        if arg:
            low['arg'] = arg

        if kwarg:
            low['kwarg'] = kwarg

        if expr_form:
            low['expr_form'] = expr_form

        if batch:
            low['batch'] = batch

        if ret:
            low['ret'] = ret

        return self.low([low])

    def lookup_jid(self, jid):
        '''
        Get job results

        Wraps :meth:`runner`.
        '''

        return self.runner('jobs.lookup_jid', jid='{0}'.format(jid))

    def runner(self, fun, arg=None, **kwargs):
        '''
        Run a single command using the ``runner`` client

        Usage::
          runner('jobs.lookup_jid', jid=12345)
        '''
        low = {
            'client': 'runner',
            'fun': fun,
        }
        if arg:
            low['arg'] = arg

        low.update(kwargs)

        return self.low([low])

    def wheel(self, fun, arg=None, kwarg=None, **kwargs):
        '''
        Run a single command using the ``wheel`` client

        Usage::
          wheel('key.accept', match='myminion')
        '''
        low = {
            'client': 'wheel',
            'fun': fun,
        }

        if arg:
            low['arg'] = arg
        if kwarg:
            low['kwarg'] = kwarg

        low.update(kwargs)

        return self.low([low])

    def _send_auth(self, path, **kwargs):
        return self.req(path, kwargs)

    def login(self, username=None, password=None, eauth=None, **kwargs):
        '''
        Authenticate with salt-api and return the user permissions and
        authentication token or an empty dict

        '''
        local = locals()
        kwargs.update(
            dict(
                (key, local[key]) for key in (
                    'username',
                    'password',
                    'eauth'
                ) if local.get(key, None) is not None
            )
        )
        self.auth = self._send_auth('/login', **kwargs).get('return', [{}])[0]
        return self.auth

    def token(self, **kwargs):
        '''
        Get an eauth token from Salt for use with the /run URL

        '''
        self.auth = self._send_auth('/token', **kwargs)[0]
        return self.auth

    def _construct_url(self, path):
        '''
        Construct the url to salt-api for the given path

        Args:
            path: the path to the salt-api resource

        >>> api = Pepper('https://localhost:8000/salt-api/')
        >>> api._construct_url('/login')
        'https://localhost:8000/salt-api/login'
        '''

        relative_path = path.lstrip('/')
        return urlparse.urljoin(self.api_url, relative_path)

    def _parse_salt_version(self, version):
        # borrow from salt.version
        git_describe_regex = re.compile(
            r'(?:[^\d]+)?(?P<major>[\d]{1,4})'
            r'\.(?P<minor>[\d]{1,2})'
            r'(?:\.(?P<bugfix>[\d]{0,2}))?'
            r'(?:\.(?P<mbugfix>[\d]{0,2}))?'
            r'(?:(?P<pre_type>rc|a|b|alpha|beta|nb)(?P<pre_num>[\d]{1}))?'
            r'(?:(?:.*)-(?P<noc>(?:[\d]+|n/a))-(?P<sha>[a-z0-9]{8}))?'
        )
        match = git_describe_regex.match(version)
        if match:
            self.salt_version = match.groups()
