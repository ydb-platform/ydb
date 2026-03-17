"""Utilities using NDG HTTPS Client, including a main module that can be used to
fetch from a URL.
"""
__author__ = "R B Wilkinson"
__date__ = "09/12/11"
__copyright__ = "(C) 2011 Science and Technology Facilities Council"
__license__ = "BSD - see LICENSE file in top-level directory"
__contact__ = "Philip.Kershaw@stfc.ac.uk"
__revision__ = '$Id$'

import logging
from optparse import OptionParser
import os
import sys

if sys.version_info[0] > 2:
    import http.cookiejar as cookiejar_
    import http.client as http_client_
    from urllib.request import Request as Request_
    from urllib.request import HTTPHandler as HTTPHandler_
    from urllib.request import HTTPCookieProcessor as HTTPCookieProcessor_
    from urllib.request import HTTPBasicAuthHandler as HTTPBasicAuthHandler_
    from urllib.request import HTTPPasswordMgrWithDefaultRealm as \
                                            HTTPPasswordMgrWithDefaultRealm_
    from urllib.request import ProxyHandler as ProxyHandler_
    from urllib.error import HTTPError as HTTPError_
    import urllib.parse as urlparse_
else:
    import cookielib as cookiejar_
    import httplib as http_client_
    from urllib2 import Request as Request_
    from urllib2 import HTTPHandler as HTTPHandler_
    from urllib2 import HTTPCookieProcessor as HTTPCookieProcessor_
    from urllib2 import HTTPBasicAuthHandler as HTTPBasicAuthHandler_
    from urllib2 import HTTPPasswordMgrWithDefaultRealm as \
                                            HTTPPasswordMgrWithDefaultRealm_
    from urllib2 import ProxyHandler as ProxyHandler_
    from urllib2 import HTTPError as HTTPError_
    import urlparse as urlparse_

from ndg.httpsclient.urllib2_build_opener import build_opener
from ndg.httpsclient.https import HTTPSContextHandler
from ndg.httpsclient import ssl_context_util

log = logging.getLogger(__name__)

class AccumulatingHTTPCookieProcessor(HTTPCookieProcessor_):
    """Cookie processor that adds new cookies (instead of replacing the existing
    ones as HTTPCookieProcessor does)
    """
    def http_request(self, request):
        """Processes cookies for a HTTP request.
        @param request: request to process
        @type request: urllib2.Request
        @return: request
        @rtype: urllib2.Request
        """
        COOKIE_HEADER_NAME = "Cookie"
        tmp_request = Request_(request.get_full_url(), request.data, {},
                                      request.origin_req_host,
                                      request.unverifiable)
        self.cookiejar.add_cookie_header(tmp_request)
        # Combine existing and new cookies.
        new_cookies = tmp_request.get_header(COOKIE_HEADER_NAME)
        if new_cookies:
            if request.has_header(COOKIE_HEADER_NAME):
                # Merge new cookies with existing ones.
                old_cookies = request.get_header(COOKIE_HEADER_NAME)
                merged_cookies = '; '.join([old_cookies, new_cookies])
                request.add_unredirected_header(COOKIE_HEADER_NAME,
                                                merged_cookies)
            else:
                # No existing cookies so just set new ones.
                request.add_unredirected_header(COOKIE_HEADER_NAME, new_cookies)
        return request

    # Process cookies for HTTPS in the same way.
    https_request = http_request


class URLFetchError(Exception):
    """Error fetching content from URL"""
    

def fetch_from_url(url, config, data=None, handlers=None):
    """Returns data retrieved from a URL.
    @param url: URL to attempt to open
    @type url: basestring
    @param config: SSL context configuration
    @type config: Configuration
    @return data retrieved from URL or None
    """
    return_code, return_message, response = open_url(url, config, data=data,
                                                     handlers=handlers)
    if return_code and return_code == http_client_.OK:
        return_data = response.read()
        response.close()
        return return_data
    else:
        raise URLFetchError(return_message)

def fetch_from_url_to_file(url, config, output_file, data=None, handlers=None):
    """Writes data retrieved from a URL to a file.
    @param url: URL to attempt to open
    @type url: basestring
    @param config: SSL context configuration
    @type config: Configuration
    @param output_file: output file
    @type output_file: basestring
    @return: tuple (
        returned HTTP status code or 0 if an error occurred
        returned message
        boolean indicating whether access was successful)
    """
    return_code, return_message, response = open_url(url, config, data=data,
                                                     handlers=handlers)
    if return_code == http_client_.OK:
        return_data = response.read()
        response.close()
        outfile = open(output_file, "w")
        outfile.write(return_data)
        outfile.close()
        
    return return_code, return_message, return_code == http_client_.OK


def fetch_stream_from_url(url, config, data=None, handlers=None):
    """Returns data retrieved from a URL.
    @param url: URL to attempt to open
    @type url: basestring
    @param config: SSL context configuration
    @type config: Configuration
    @param data: HTTP POST data
    @type data: str
    @param handlers: list of custom urllib2 handlers to add to the request
    @type handlers: iterable
    @return: data retrieved from URL or None
    @rtype: file derived type
    """
    return_code, return_message, response = open_url(url, config, data=data,
                                                     handlers=handlers)
    if return_code and return_code == http_client_.OK:
        return response
    else:
        raise URLFetchError(return_message)


def open_url(url, config, data=None, handlers=None):
    """Attempts to open a connection to a specified URL.
    @param url: URL to attempt to open
    @param config: SSL context configuration
    @type config: Configuration
    @param data: HTTP POST data
    @type data: str
    @param handlers: list of custom urllib2 handlers to add to the request
    @type handlers: iterable
    @return: tuple (
        returned HTTP status code or 0 if an error occurred
        returned message or error description
        response object)
    """
    debuglevel = 1 if config.debug else 0

    # Set up handlers for URL opener.
    if config.cookie:
        cj = config.cookie
    else:
        cj = cookiejar_.CookieJar()
        
    # Use a cookie processor that accumulates cookies when redirects occur so
    # that an application can redirect for authentication and retain both any
    # cookies for the application and the security system (c.f.,
    # urllib2.HTTPCookieProcessor which replaces cookies).
    cookie_handler = AccumulatingHTTPCookieProcessor(cj)

    if not handlers:
        handlers = []
        
    handlers.append(cookie_handler)

    if config.debug:
        http_handler = HTTPHandler_(debuglevel=debuglevel)
        https_handler = HTTPSContextHandler(config.ssl_context, 
                                            debuglevel=debuglevel)
        handlers.extend([http_handler, https_handler])
        
    if config.http_basicauth:
        # currently only supports http basic auth
        auth_handler = HTTPBasicAuthHandler_(HTTPPasswordMgrWithDefaultRealm_())
        auth_handler.add_password(realm=None, uri=url,
                                  user=config.http_basicauth[0],
                                  passwd=config.http_basicauth[1])
        handlers.append(auth_handler)


    # Explicitly remove proxy handling if the host is one listed in the value of
    # the no_proxy environment variable because urllib2 does use proxy settings 
    # set via http_proxy and https_proxy, but does not take the no_proxy value 
    # into account.
    if not _should_use_proxy(url, config.no_proxy):
        handlers.append(ProxyHandler_({}))
        log.debug("Not using proxy")
    elif config.proxies:
        handlers.append(ProxyHandler_(config.proxies))
        log.debug("Configuring proxies: %s" % config.proxies)

    opener = build_opener(*handlers, ssl_context=config.ssl_context)
    
    headers = config.headers
    if headers is None: 
        headers = {}
        
    request = Request_(url, data, headers)

    # Open the URL and check the response.
    return_code = 0
    return_message = ''
    response = None
    
    try:
        response = opener.open(request)
        return_message = response.msg
        return_code = response.code
        if log.isEnabledFor(logging.DEBUG):
            for index, cookie in enumerate(cj):
                log.debug("%s  :  %s", index, cookie)
                
    except HTTPError_ as exc:
        return_code = exc.code
        return_message = "Error: %s" % exc.msg
        if log.isEnabledFor(logging.DEBUG):
            log.debug("%s %s", exc.code, exc.msg)
            
    except Exception as exc:
        return_message = "Error: %s" % exc.__str__()
        if log.isEnabledFor(logging.DEBUG):
            import traceback
            log.debug(traceback.format_exc())
            
    return (return_code, return_message, response)


def _should_use_proxy(url, no_proxy=None):
    """Determines whether a proxy should be used to open a connection to the 
    specified URL, based on the value of the no_proxy environment variable.
    @param url: URL
    @type url: basestring or urllib2.Request
    """
    if no_proxy is None:
        no_proxy_effective = os.environ.get('no_proxy', '')
    else:
        no_proxy_effective = no_proxy

    urlObj = urlparse_.urlparse(_url_as_string(url))
    for np in [h.strip() for h in no_proxy_effective.split(',')]:
        if urlObj.hostname == np:
            return False

    return True

def _url_as_string(url):
    """Returns the URL string from a URL value that is either a string or
    urllib2.Request..
    @param url: URL
    @type url: basestring or urllib2.Request
    @return: URL string
    @rtype: basestring
    """
    if isinstance(url, Request_):
        return url.get_full_url()
    elif isinstance(url, str):
        return url
    else:
        raise TypeError("Expected type %r or %r" %
                        (str, Request_))


class Configuration(object):
    """Connection configuration.
    """
    def __init__(self, ssl_context, debug=False, proxies=None, no_proxy=None,
                 cookie=None, http_basicauth=None, headers=None):
        """
        @param ssl_context: SSL context to use with this configuration
        @type ssl_context: OpenSSL.SSL.Context
        @param debug: if True, output debugging information
        @type debug: bool
        @param proxies: proxies to use for 
        @type proxies: dict with basestring keys and values
        @param no_proxy: hosts for which a proxy should not be used
        @type no_proxy: basestring
        @param cookie: cookies to set for request
        @type cookie: cookielib.CookieJar (python 3 - http.cookiejar)
        @param http_basicauth: http authentication, or None
        @type http_basicauth: tuple of (username,password)
        @param headers: http headers
        @type headers: dict
        """
        self.ssl_context = ssl_context
        self.debug = debug
        self.proxies = proxies
        self.no_proxy = no_proxy
        self.cookie = cookie
        self.http_basicauth = http_basicauth
        self.headers = headers


def main():
    '''Utility to fetch data using HTTP or HTTPS GET from a specified URL.
    '''
    parser = OptionParser(usage="%prog [options] url")
    parser.add_option("-c", "--certificate", dest="cert_file", metavar="FILE",
                      default=os.path.expanduser("~/credentials.pem"),
                      help="Certificate file - defaults to $HOME/credentials.pem")
    parser.add_option("-k", "--private-key", dest="key_file", metavar="FILE",
                      default=None,
                      help="Private key file - defaults to the certificate file")
    parser.add_option("-t", "--ca-certificate-dir", dest="ca_dir", 
                      metavar="PATH",
                      default=None,
                      help="Trusted CA certificate file directory")
    parser.add_option("-d", "--debug", action="store_true", dest="debug", 
                      default=False,
                      help="Print debug information.")
    parser.add_option("-p", "--post-data-file", dest="data_file",
                      metavar="FILE", default=None,
                      help="POST data file")
    parser.add_option("-f", "--fetch", dest="output_file", metavar="FILE",
                      default=None, help="Output file")
    parser.add_option("-n", "--no-verify-peer", action="store_true", 
                      dest="no_verify_peer", default=False,
                      help="Skip verification of peer certificate.")
    parser.add_option("-a", "--basicauth", dest="basicauth", 
                      metavar="USER:PASSWD",
                      default=None,
                      help="HTTP authentication credentials")
    parser.add_option("--header", action="append", dest="headers", 
                      metavar="HEADER: VALUE",
                      help="Add HTTP header to request")
    (options, args) = parser.parse_args()
    if len(args) != 1:
        parser.error("Incorrect number of arguments")

    url = args[0]

    if options.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    if options.key_file and os.path.exists(options.key_file):
        key_file = options.key_file
    else:
        key_file = None
    
    if options.cert_file and os.path.exists(options.cert_file):
        cert_file = options.cert_file
    else:
        cert_file = None
    
    if options.ca_dir and os.path.exists(options.ca_dir):
        ca_dir = options.ca_dir 
    else:
        ca_dir = None
        
    verify_peer = not options.no_verify_peer

    if options.data_file and os.path.exists(options.data_file):
        data_file = open(options.data_file)
        data = data_file.read()
        data_file.close()
    else:
        data = None
    
    if options.basicauth:
        http_basicauth = options.basicauth.split(':', 1)
    else:
        http_basicauth = None

    headers = {}
    if options.headers:
        for h in options.headers:
            key, val = h.split(':', 1)
            headers[key.strip()] = val.lstrip()
            
    # If a private key file is not specified, the key is assumed to be stored in 
    # the certificate file.
    ssl_context = ssl_context_util.make_ssl_context(key_file,
                                                    cert_file,
                                                    None,
                                                    ca_dir,
                                                    verify_peer, 
                                                    url)

    config = Configuration(ssl_context, 
                           options.debug,
                           http_basicauth=http_basicauth,
                           headers=headers)
    if options.output_file:
        return_code, return_message = fetch_from_url_to_file(
                                                      url, 
                                                      config,
                                                      options.output_file,
                                                      data)[:2]
        raise SystemExit(return_code, return_message)
    else:
        data = fetch_from_url(url, config)
        print(data)


if __name__=='__main__':
    logging.basicConfig()
    main()
