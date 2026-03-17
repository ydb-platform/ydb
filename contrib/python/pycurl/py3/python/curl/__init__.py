'''A high-level interface to the pycurl extension'''

# ** mfx NOTE: the CGI class uses "black magic" using COOKIEFILE in
#    combination with a non-existent file name. See the libcurl docs
#    for more info.

import sys, pycurl

py3 = sys.version_info[0] == 3

# python 2/3 compatibility
if py3:
    import urllib.parse as urllib_parse
    from urllib.parse import urljoin
    from io import BytesIO
else:
    import urllib as urllib_parse
    from urlparse import urljoin
    try:
        from cStringIO import StringIO as BytesIO
    except ImportError:
        from StringIO import StringIO as BytesIO

# We should ignore SIGPIPE when using pycurl.NOSIGNAL - see
# the libcurl tutorial for more info.
try:
    import signal
    from signal import SIGPIPE, SIG_IGN
except ImportError:
    pass
else:
    signal.signal(SIGPIPE, SIG_IGN)


class Curl:
    "High-level interface to pycurl functions."
    def __init__(self, base_url="", fakeheaders=None):
        self.handle = pycurl.Curl()
        # These members might be set.
        self.set_url(base_url)
        self.verbosity = 0
        self.fakeheaders = fakeheaders or []
        # Nothing past here should be modified by the caller.
        self.payload = None
        self.payload_io = BytesIO()
        self.hdr = ""
        # Verify that we've got the right site; harmless on a non-SSL connect.
        self.set_option(pycurl.SSL_VERIFYHOST, 2)
        # Follow redirects in case it wants to take us to a CGI...
        self.set_option(pycurl.FOLLOWLOCATION, 1)
        self.set_option(pycurl.MAXREDIRS, 5)
        self.set_option(pycurl.NOSIGNAL, 1)
        # Setting this option with even a nonexistent file makes libcurl
        # handle cookie capture and playback automatically.
        self.set_option(pycurl.COOKIEFILE, "/dev/null")
        # Set timeouts to avoid hanging too long
        self.set_timeout(30)
        # Use password identification from .netrc automatically
        self.set_option(pycurl.NETRC, 1)
        self.set_option(pycurl.WRITEFUNCTION, self.payload_io.write)
        def header_callback(x):
            self.hdr += x.decode('ascii')
        self.set_option(pycurl.HEADERFUNCTION, header_callback)

    def set_timeout(self, timeout):
        "Set timeout for a retrieving an object"
        self.set_option(pycurl.TIMEOUT, timeout)

    def set_url(self, url):
        "Set the base URL to be retrieved."
        self.base_url = url
        self.set_option(pycurl.URL, self.base_url)

    def set_option(self, *args):
        "Set an option on the retrieval."
        self.handle.setopt(*args)

    def set_verbosity(self, level):
        "Set verbosity to 1 to see transactions."
        self.set_option(pycurl.VERBOSE, level)

    def __request(self, relative_url=None):
        "Perform the pending request."
        if self.fakeheaders:
            self.set_option(pycurl.HTTPHEADER, self.fakeheaders)
        if relative_url:
            self.set_option(pycurl.URL, urljoin(self.base_url, relative_url))
        self.payload = None
        self.payload_io.seek(0)
        self.payload_io.truncate()
        self.hdr = ""
        self.handle.perform()
        self.payload = self.payload_io.getvalue()
        return self.payload

    def get(self, url="", params=None):
        "Ship a GET request for a specified URL, capture the response."
        if params:
            url += "?" + urllib_parse.urlencode(params)
        self.set_option(pycurl.HTTPGET, 1)
        return self.__request(url)

    def head(self, url="", params=None):
        "Ship a HEAD request for a specified URL, capture the response."
        if params:
            url += "?" + urllib_parse.urlencode(params)
        self.set_option(pycurl.NOBODY, 1)
        return self.__request(url)

    def post(self, cgi, params):
        "Ship a POST request to a specified CGI, capture the response."
        self.set_option(pycurl.POST, 1)
        self.set_option(pycurl.POSTFIELDS, urllib_parse.urlencode(params))
        return self.__request(cgi)

    def body(self):
        "Return the body from the last response."
        return self.payload

    def header(self):
        "Return the header from the last response."
        return self.hdr

    def get_info(self, *args):
        "Get information about retrieval."
        return self.handle.getinfo(*args)

    def info(self):
        "Return a dictionary with all info on the last response."
        m = {}
        m['effective-url'] = self.handle.getinfo(pycurl.EFFECTIVE_URL)
        m['http-code'] = self.handle.getinfo(pycurl.HTTP_CODE)
        m['total-time'] = self.handle.getinfo(pycurl.TOTAL_TIME)
        m['namelookup-time'] = self.handle.getinfo(pycurl.NAMELOOKUP_TIME)
        m['connect-time'] = self.handle.getinfo(pycurl.CONNECT_TIME)
        m['pretransfer-time'] = self.handle.getinfo(pycurl.PRETRANSFER_TIME)
        m['redirect-time'] = self.handle.getinfo(pycurl.REDIRECT_TIME)
        m['redirect-count'] = self.handle.getinfo(pycurl.REDIRECT_COUNT)
        m['size-upload'] = self.handle.getinfo(pycurl.SIZE_UPLOAD)
        m['size-download'] = self.handle.getinfo(pycurl.SIZE_DOWNLOAD)
        m['speed-upload'] = self.handle.getinfo(pycurl.SPEED_UPLOAD)
        m['header-size'] = self.handle.getinfo(pycurl.HEADER_SIZE)
        m['request-size'] = self.handle.getinfo(pycurl.REQUEST_SIZE)
        m['content-length-download'] = self.handle.getinfo(pycurl.CONTENT_LENGTH_DOWNLOAD)
        m['content-length-upload'] = self.handle.getinfo(pycurl.CONTENT_LENGTH_UPLOAD)
        m['content-type'] = self.handle.getinfo(pycurl.CONTENT_TYPE)
        m['response-code'] = self.handle.getinfo(pycurl.RESPONSE_CODE)
        m['speed-download'] = self.handle.getinfo(pycurl.SPEED_DOWNLOAD)
        m['ssl-verifyresult'] = self.handle.getinfo(pycurl.SSL_VERIFYRESULT)
        m['filetime'] = self.handle.getinfo(pycurl.INFO_FILETIME)
        m['starttransfer-time'] = self.handle.getinfo(pycurl.STARTTRANSFER_TIME)
        m['redirect-time'] = self.handle.getinfo(pycurl.REDIRECT_TIME)
        m['redirect-count'] = self.handle.getinfo(pycurl.REDIRECT_COUNT)
        m['http-connectcode'] = self.handle.getinfo(pycurl.HTTP_CONNECTCODE)
        m['httpauth-avail'] = self.handle.getinfo(pycurl.HTTPAUTH_AVAIL)
        m['proxyauth-avail'] = self.handle.getinfo(pycurl.PROXYAUTH_AVAIL)
        m['os-errno'] = self.handle.getinfo(pycurl.OS_ERRNO)
        m['num-connects'] = self.handle.getinfo(pycurl.NUM_CONNECTS)
        m['ssl-engines'] = self.handle.getinfo(pycurl.SSL_ENGINES)
        m['cookielist'] = self.handle.getinfo(pycurl.INFO_COOKIELIST)
        m['lastsocket'] = self.handle.getinfo(pycurl.LASTSOCKET)
        m['ftp-entry-path'] = self.handle.getinfo(pycurl.FTP_ENTRY_PATH)
        return m

    def answered(self, check):
        "Did a given check string occur in the last payload?"
        return self.payload.find(check) >= 0

    def close(self):
        "Close a session, freeing resources."
        if self.handle:
            self.handle.close()
        self.handle = None
        self.hdr = ""
        self.payload = ""

    def __del__(self):
        self.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        url = 'https://curl.haxx.se'
    else:
        url = sys.argv[1]
    c = Curl()
    c.get(url)
    print(c.body())
    print('='*74 + '\n')
    import pprint
    pprint.pprint(c.info())
    print(c.get_info(pycurl.OS_ERRNO))
    print(c.info()['os-errno'])
    c.close()
