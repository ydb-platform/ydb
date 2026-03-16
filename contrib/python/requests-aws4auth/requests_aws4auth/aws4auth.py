"""
Provides AWS4Auth class for handling Amazon Web Services version 4
authentication with the Requests module.

"""

# Licensed under the MIT License:
# http://opensource.org/licenses/MIT

import hmac
import hashlib
import posixpath
import re
import shlex
import datetime

try:
    import collections.abc as abc
except ImportError:
    import collections as abc

from urllib.parse import urlparse, parse_qs, quote, unquote

from requests.auth import AuthBase
from .aws4signingkey import AWS4SigningKey
from .exceptions import DateMismatchError, NoSecretKeyError, DateFormatError


class AWS4Auth(AuthBase):
    """
    Requests authentication class providing AWS version 4 authentication for
    HTTP requests. Implements header-based authentication only, GET URL
    parameter and POST parameter authentication are not supported.

    Provides authentication for regions and services listed at:
    http://docs.aws.amazon.com/general/latest/gr/rande.html

    The following services do not support AWS auth version 4 and are not usable
    with this package:
        * Simple Email Service (SES)' - AWS auth v3 only
        * Simple Workflow Service - AWS auth v3 only
        * Import/Export - AWS auth v2 only
        * SimpleDB - AWS auth V2 only
        * DevPay - AWS auth v1 only
        * Mechanical Turk - has own signing mechanism

    You can reuse AWS4Auth instances to sign as many requests as you need.

    Basic usage
    -----------
    >>> import requests
    >>> from requests_aws4auth import AWS4Auth
    >>> auth = AWS4Auth('<ACCESS ID>', '<ACCESS KEY>', 'eu-west-1', 's3')
    >>> endpoint = 'http://s3-eu-west-1.amazonaws.com'
    >>> response = requests.get(endpoint, auth=auth)
    >>> response.text
    <?xml version="1.0" encoding="UTF-8"?>
        <ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01">
            <Owner>
            <ID>bcaf1ffd86f461ca5fb16fd081034f</ID>
            <DisplayName>webfile</DisplayName>
            ...

    This example lists your buckets in the eu-west-1 region of the Amazon S3
    service.

    STS Temporary Credentials
    -------------------------
    >>> from requests_aws4auth import AWS4Auth
    >>> auth = AWS4Auth('<ACCESS ID>', '<ACCESS KEY>', 'eu-west-1', 's3',
                        session_token='<SESSION TOKEN>')
    ...

    This example shows how to construct an AWS4Auth object for use with STS
    temporary credentials. The ``x-amz-security-token`` header is added with
    the session token. Temporary credential timeouts are not managed -- in
    case the temporary credentials expire, they need to be re-generated and
    the AWS4Auth object re-constructed with the new credentials.

    Dynamic STS Credentials using botocore RefreshableCredentials
    -------------------------------------------------------------
    >>> from requests_aws4auth import AWS4Auth
    >>> from botocore.session import Session
    >>> credentials = Session().get_credentials()
    >>> auth = AWS4Auth(region='eu-west-1', service='es',
                        refreshable_credentials=credentials)
    ...

    This example shows how to construct an AWS4Auth instance with
    automatically refreshing credentials, suitable for long-running
    applications using AWS IAM assume-role.
    The RefreshableCredentials instance is used to generate valid static
    credentials per-request, eliminating the need to recreate the AWS4Auth
    instance when temporary credentials expire.

    Date handling
    -------------
    If an HTTP request to be authenticated contains a Date or X-Amz-Date
    header, AWS will only accept authorisation if the date in the header
    matches the scope date of the signing key (see
    http://docs.aws.amazon.com/general/latest/gr/sigv4-date-handling.html).

    From version 0.8 of requests-aws4auth, if the header date does not match
    the scope date, the AWS4Auth class will automatically regenerate its
    signing key, using the same scope parameters as the previous key except for
    the date, which will be changed to match the request date. (If a request
    does not include a date, the current date is added to the request in an
    X-Amz-Date header).

    The new behaviour from version 0.8 has implications for thread safety and
    secret key security, see the "Automatic key regeneration", "Secret key
    storage" and "Multithreading" sections below.

    This also means that AWS4Auth is now attempting to parse and extract dates
    from the values in X-Amz-Date and Date headers. Supported date formats are:

        * RFC 7231 (e.g. Mon, 09 Sep 2011 23:36:00 GMT)
        * RFC 850 (e.g. Sunday, 06-Nov-94 08:49:37 GMT)
        * C time (e.g. Wed Dec 4 00:00:00 2002)
        * Amz-Date format (e.g. 20090325T010101Z)
        * ISO 8601 / RFC 3339 (e.g. 2009-03-25T10:11:12.13-01:00)

    If either header is present but AWS4Auth cannot extract a date because all
    present date headers are in an unrecognisable format, AWS4Auth will delete
    any X-Amz-Date and Date headers present and replace with a single
    X-Amz-Date header containing the current date. This behaviour can be
    modified using the 'raise_invalid_date' keyword argument of the AWS4Auth
    constructor.

    Automatic key regeneration
    --------------------------
    If you do not want the signing key to be automatically regenerated when a
    mismatch between the request date and the scope date is encountered, use
    the alternative StrictAWS4Auth class, which is identical to AWS4Auth except
    that upon encountering a date mismatch it just raises a DateMismatchError.
    You can also use the PassiveAWS4Auth class, which mimics the AWS4Auth
    behaviour prior to version 0.8 and just signs and sends the request,
    whether the date matches or not. In this case it is up to the calling code
    to handle an authentication failure response from AWS caused by a date
    mismatch.

    Secret key storage
    ------------------
    To allow automatic key regeneration, the secret key is stored in the
    AWS4Auth instance, in the signing key object. If you do not want this to
    occur, instantiate the instance using an AWS4Signing key which was created
    with the store_secret_key parameter set to False:

    >>> sig_key = AWS4SigningKey(secret_key, region, service, date, False)
    >>> auth = StrictAWS4Auth(access_id, sig_key)

    The AWS4Auth class will then raise a NoSecretKeyError when it attempts to
    regenerate its key. A slightly more conceptually elegant way to handle this
    is to use the alternative StrictAWS4Auth class, again instantiating it with
    an AWS4SigningKey instance created with store_secret_key = False.

    Multithreading
    --------------
    If you share AWS4Auth (or even StrictAWS4Auth) instances between threads
    you are likely to encounter problems. Because AWS4Auth instances may
    unpredictably regenerate their signing key as part of signing a request,
    threads using the same instance may find the key changed by another thread
    halfway through the signing process, which may result in undefined
    behaviour.

    It may be possible to rig up a workable instance sharing mechanism using
    locking primitives and the StrictAWS4Auth class, however this poor author
    can't think of a scenario which works safely yet doesn't suffer from at
    some point blocking all threads for at least the duration of an HTTP
    request, which could be several seconds. If several requests come in in
    close succession which all require key regenerations then the system could
    be forced into serial operation for quite a length of time.

    In short, it's best to create a thread-local instance of AWS4Auth for each
    thread that needs to do authentication.

    Class attributes
    ----------------
    AWS4Auth.access_id   -- the access ID supplied to the instance
    AWS4Auth.region      -- the AWS region for the instance
    AWS4Auth.service     -- the endpoint code for the service for this instance
    AWS4Auth.date        -- the date the instance is valid for
    AWS4Auth.signing_key -- instance of AWS4SigningKey used for this instance,
                            either generated from the supplied parameters or
                            supplied directly on the command line

    """
    default_include_headers = {'host', 'content-type', 'date', 'x-amz-*'}

    def __init__(self, *args, **kwargs):
        """
        AWS4Auth instances can be created by supplying key scope parameters
        directly or by using an AWS4SigningKey instance:

        >>> auth = AWS4Auth(access_id, secret_key, region, service
        ...                 [, date][, raise_invalid_date=False][, session_token=None])

          or

        >>> auth = AWS4Auth(access_id, signing_key[, raise_invalid_date=False])

          or using auto-refreshed STS temporary creds via botocore RefreshableCredentials
          (useful for long-running processes):

        >>> auth = AWS4Auth(refreshable_credentials=botocore.session.Session().get_credentials(),
        ...                 region='eu-west-1', service='es')

        access_id   -- This is your AWS access ID
        secret_key  -- This is your AWS secret access key
        region      -- The region you're connecting to, as per the list at
                       http://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region
                       e.g. us-east-1. For services which don't require a region
                       (e.g. IAM), use us-east-1.
                       Must be supplied as a keyword argument iff refreshable_credentials
                       is set.
        service     -- The name of the service you're connecting to, as per
                       endpoints at:
                       http://docs.aws.amazon.com/general/latest/gr/rande.html
                       e.g. elasticbeanstalk.
                       Must be supplied as a keyword argument iff refreshable_credentials
                       is set.
        date        -- Date this instance is valid for. 8-digit date as str of the
                       form YYYYMMDD. Key is only valid for requests with a
                       Date or X-Amz-Date header matching this date. If date is
                       not supplied the current date is used.
        signing_key -- An AWS4SigningKey instance.
        raise_invalid_date
                    -- Must be supplied as keyword argument. AWS4Auth tries to
                       parse a date from the X-Amz-Date and Date headers of the
                       request, first trying X-Amz-Date, and then Date if
                       X-Amz-Date is not present or is in an unrecognised
                       format. If one or both of the two headers are present
                       yet neither are in a format which AWS4Auth recognises
                       then it will remove both headers and replace with a new
                       X-Amz-Date header using the current date.

                       If this behaviour is not wanted, set the
                       raise_invalid_date keyword argument to True, and
                       instead an InvalidDateError will be raised when neither
                       date is recognised. If neither header is present at all
                       then an X-Amz-Date header will still be added containing
                       the current date.

                       See the AWS4Auth class docstring for supported date
                       formats.
        session_token
                    -- Must be supplied as keyword argument. If session_token
                       is set, then it is used for the x-amz-security-token
                       header, for use with STS temporary credentials.
        refreshable_credentials
                    -- A botocore.credentials.RefreshableCredentials instance.
                       Must be supplied as keyword argument. This instance is
                       used to generate valid per-request static credentials,
                       without needing to re-generate the AWS4Auth instance.                       
                       If refreshable_credentials is set, the following arguments
                       are ignored: access_id, secret_key, signing_key,
                       session_token.

        """
        self.signing_key = None
        self.refreshable_credentials = kwargs.get('refreshable_credentials', None)
        if self.refreshable_credentials:
            # instantiate from refreshable_credentials
            self.service = kwargs.get('service', None)
            if not self.service:
                raise TypeError('service must be provided as keyword argument when using refreshable_credentials')
            self.region = kwargs.get('region', None)
            if not self.region:
                raise TypeError('region must be provided as keyword argument when using refreshable_credentials')
            self.date = kwargs.get('date', None)
            self.default_include_headers.add('x-amz-security-token')
        else:
            l = len(args)
            if l not in [2, 4, 5]:
                msg = 'AWS4Auth() takes 2, 4 or 5 arguments, {} given'.format(l)
                raise TypeError(msg)
            self.access_id = args[0]
            if isinstance(args[1], AWS4SigningKey) and l == 2:
                # instantiate from signing key
                self.signing_key = args[1]
                self.region = self.signing_key.region
                self.service = self.signing_key.service
                self.date = self.signing_key.date
            elif l in [4, 5]:
                # instantiate from args
                secret_key = args[1]
                self.region = args[2]
                self.service = args[3]
                self.date = args[4] if l == 5 else None
                self.regenerate_signing_key(secret_key=secret_key)
            else:
                raise TypeError()

            self.session_token = kwargs.get('session_token')
            if self.session_token:
                self.default_include_headers.add('x-amz-security-token')

        raise_invalid_date = kwargs.get('raise_invalid_date', False)
        if raise_invalid_date in [True, False]:
            self.raise_invalid_date = raise_invalid_date
        else:
            raise ValueError('raise_invalid_date must be True or False in AWS4Auth.__init__()')

        self.include_hdrs = set(self.default_include_headers)

        # if the key exists and it's some sort of listable object, use it.
        if 'include_hdrs' in kwargs and isinstance(kwargs['include_hdrs'], abc.Iterable):
            self.include_hdrs = set(kwargs['include_hdrs'])

        AuthBase.__init__(self)

    def regenerate_signing_key(self, secret_key=None, region=None,
                               service=None, date=None):
        """
        Regenerate the signing key for this instance. Store the new key in
        signing_key property.

        Take scope elements of the new key from the equivalent properties
        (region, service, date) of the current AWS4Auth instance. Scope
        elements can be overridden for the new key by supplying arguments to
        this function. If overrides are supplied update the current AWS4Auth
        instance's equivalent properties to match the new values.

        If secret_key is not specified use the value of the secret_key property
        of the current AWS4Auth instance's signing key. If the existing signing
        key is not storing its secret key (i.e. store_secret_key was set to
        False at instantiation) then raise a NoSecretKeyError and do not
        regenerate the key. In order to regenerate a key which is not storing
        its secret key, secret_key must be supplied to this function.

        Use the value of the existing key's store_secret_key property when
        generating the new key. If there is no existing key, then default
        to setting store_secret_key to True for new key.

        """
        if secret_key is None and (self.signing_key is None or self.signing_key.secret_key is None):

            raise NoSecretKeyError

        secret_key = secret_key or self.signing_key.secret_key
        region = region or self.region
        service = service or self.service
        date = date or self.date
        if self.signing_key is None:
            store_secret_key = True
        else:
            store_secret_key = self.signing_key.store_secret_key

        self.signing_key = AWS4SigningKey(secret_key, region, service, date,
                                          store_secret_key)

        self.region = region
        self.service = service
        self.date = self.signing_key.date

    def __call__(self, req):
        """
        Interface used by Requests module to apply authentication to HTTP
        requests.

        Add x-amz-content-sha256 and Authorization headers to the request. Add
        x-amz-date header to request if not already present and req does not
        contain a Date header.

        Check request date matches date in the current signing key. If not,
        regenerate signing key to match request date.

        If request body is not already encoded to bytes, encode to charset
        specified in Content-Type header, or UTF-8 if not specified.

        req -- Requests PreparedRequest object

        """
        if self.refreshable_credentials:
            # generate per-request static credentials
            self.refresh_credentials()
        # check request date matches scope date
        req_date = self.get_request_date(req)
        if req_date is None:
            # no date headers or none in recognisable format
            # replace them with x-amz-header with current date and time
            if 'date' in req.headers: del req.headers['date']
            if 'x-amz-date' in req.headers: del req.headers['x-amz-date']
            now = datetime.datetime.utcnow()
            req_date = now.date()
            req.headers['x-amz-date'] = now.strftime('%Y%m%dT%H%M%SZ')
        req_scope_date = req_date.strftime('%Y%m%d')
        if req_scope_date != self.date:
            self.handle_date_mismatch(req)

        # encode body and generate body hash
        if hasattr(req, 'body') and req.body is not None:
            if hasattr(req.body, 'read'):
                req.body = req.body.read()
            self.encode_body(req)
            content_hash = hashlib.sha256(req.body)
        elif hasattr(req, 'content') and req.content is not None:
            content_hash = hashlib.sha256(req.content)
        else:
            content_hash = hashlib.sha256(b'')
        req.headers['x-amz-content-sha256'] = content_hash.hexdigest()
        if self.session_token:
            req.headers['x-amz-security-token'] = self.session_token

        # generate signature
        result = self.get_canonical_headers(req, self.include_hdrs)
        cano_headers, signed_headers = result
        cano_req = self.get_canonical_request(req, cano_headers,
                                              signed_headers)
        sig_string = self.get_sig_string(req, cano_req, self.signing_key.scope)
        sig_string = sig_string.encode('utf-8')
        hsh = hmac.new(self.signing_key.key, sig_string, hashlib.sha256)
        sig = hsh.hexdigest()
        auth_str = 'AWS4-HMAC-SHA256 '
        auth_str += 'Credential={}/{}, '.format(self.access_id,
                                                self.signing_key.scope)
        auth_str += 'SignedHeaders={}, '.format(signed_headers)
        auth_str += 'Signature={}'.format(sig)
        req.headers['Authorization'] = auth_str
        return req

    def refresh_credentials(self):
        temporary_creds = self.refreshable_credentials.get_frozen_credentials()
        self.access_id = temporary_creds.access_key
        self.session_token = temporary_creds.token
        self.regenerate_signing_key(secret_key=temporary_creds.secret_key)

    @classmethod
    def get_request_date(cls, req):
        """
        Try to pull a date from the request by looking first at the
        x-amz-date header, and if that's not present then the Date header.

        Return a datetime.date object, or None if neither date header
        is found or is in a recognisable format.

        req -- a requests PreparedRequest object

        """
        date = None
        for header in ['x-amz-date', 'date']:
            if header not in req.headers:
                continue
            try:
                date_str = cls.parse_date(req.headers[header])
            except DateFormatError:
                continue
            try:
                date = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
            except ValueError:
                continue
            else:
                break

        return date

    @staticmethod
    def parse_date(date_str):
        """
        Check if date_str is in a recognised format and return an ISO
        yyyy-mm-dd format version if so. Raise DateFormatError if not.

        Recognised formats are:
        * RFC 7231 (e.g. Mon, 09 Sep 2011 23:36:00 GMT)
        * RFC 850 (e.g. Sunday, 06-Nov-94 08:49:37 GMT)
        * C time (e.g. Wed Dec 4 00:00:00 2002)
        * Amz-Date format (e.g. 20090325T010101Z)
        * ISO 8601 / RFC 3339 (e.g. 2009-03-25T10:11:12.13-01:00)

        date_str -- Str containing a date and optional time

        """
        months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug',
                  'sep', 'oct', 'nov', 'dec']
        formats = {
            # RFC 7231, e.g. 'Mon, 09 Sep 2011 23:36:00 GMT'
            r'^(?:\w{3}, )?(\d{2}) (\w{3}) (\d{4})\D.*$':
                lambda m: '{}-{:02d}-{}'.format(
                    m.group(3),
                    months.index(m.group(2).lower()) + 1,
                    m.group(1)),
            # RFC 850 (e.g. Sunday, 06-Nov-94 08:49:37 GMT)
            # assumes current century
            r'^\w+day, (\d{2})-(\w{3})-(\d{2})\D.*$':
                lambda m: '{}{}-{:02d}-{}'.format(
                    str(datetime.date.today().year)[:2],
                    m.group(3),
                    months.index(m.group(2).lower()) + 1,
                    m.group(1)),
            # C time, e.g. 'Wed Dec 4 00:00:00 2002'
            r'^\w{3} (\w{3}) (\d{1,2}) \d{2}:\d{2}:\d{2} (\d{4})$':
                lambda m: '{}-{:02d}-{:02d}'.format(
                    m.group(3),
                    months.index(m.group(1).lower()) + 1,
                    int(m.group(2))),
            # x-amz-date format dates, e.g. 20100325T010101Z
            r'^(\d{4})(\d{2})(\d{2})T\d{6}Z$':
                lambda m: '{}-{}-{}'.format(*m.groups()),
            # ISO 8601 / RFC 3339, e.g. '2009-03-25T10:11:12.13-01:00'
            r'^(\d{4}-\d{2}-\d{2})(?:[Tt].*)?$':
                lambda m: m.group(1),
        }

        out_date = None
        for regex, xform in formats.items():
            m = re.search(regex, date_str)
            if m:
                out_date = xform(m)
                break
        if out_date is None:
            raise DateFormatError
        else:
            return out_date

    def handle_date_mismatch(self, req):
        """
        Handle a request whose date doesn't match the signing key scope date.

        This AWS4Auth class implementation regenerates the signing key. See
        StrictAWS4Auth class if you would prefer an exception to be raised.

        req -- a requests prepared request object

        """
        req_datetime = self.get_request_date(req)
        new_key_date = req_datetime.strftime('%Y%m%d')
        self.regenerate_signing_key(date=new_key_date)

    @staticmethod
    def encode_body(req):
        """
        Encode body of request to bytes and update content-type if required.

        If the body of req is Unicode then encode to the charset found in
        content-type header if present, otherwise UTF-8, or ASCII if
        content-type is application/x-www-form-urlencoded. If encoding to UTF-8
        then add charset to content-type. Modifies req directly, does not
        return a modified copy.

        req -- Requests PreparedRequest object

        """
        if isinstance(req.body, str):
            split = req.headers.get('content-type', 'text/plain').split(';')
            if len(split) == 2:
                ct, cs = split
                cs = cs.split('=')[1]
                req.body = req.body.encode(cs)
            else:
                ct = split[0]
                if (ct == 'application/x-www-form-urlencoded' or 'x-amz-' in ct):
                    req.body = req.body.encode()
                else:
                    req.body = req.body.encode('utf-8')
                    req.headers['content-type'] = ct + '; charset=utf-8'

    def get_canonical_request(self, req, cano_headers, signed_headers):
        """
        Create the AWS authentication Canonical Request string.

        req            -- Requests/Httpx PreparedRequest object. Should already
                          include an x-amz-content-sha256 header
        cano_headers   -- Canonical Headers section of Canonical Request, as
                          returned by get_canonical_headers()
        signed_headers -- Signed Headers, as returned by
                          get_canonical_headers()

        """
        raw_url = str(req.url) # in case the url property is of type URL
        url = urlparse(raw_url)
        path = self.amz_cano_path(url.path)
        # AWS handles "extreme" querystrings differently to urlparse
        # (see post-vanilla-query-nonunreserved test in aws_testsuite)
        split = raw_url.split('?', 1)
        qs = split[1] if len(split) == 2 else ''
        qs = self.amz_cano_querystring(qs)
        payload_hash = req.headers['x-amz-content-sha256']
        req_parts = [req.method.upper(), path, qs, cano_headers,
                     signed_headers, payload_hash]
        cano_req = '\n'.join(req_parts)
        return cano_req

    @classmethod
    def get_canonical_headers(cls, req, include=None):
        """
        Generate the Canonical Headers section of the Canonical Request.

        Return the Canonical Headers and the Signed Headers strs as a tuple
        (canonical_headers, signed_headers).

        req     -- Requests PreparedRequest object
        include -- List of headers to include in the canonical and signed
                   headers. It's primarily included to allow testing against
                   specific examples from Amazon. If omitted or None it
                   includes host, content-type and any header starting 'x-amz-'
                   except for x-amz-client context, which appears to break
                   mobile analytics auth if included. Except for the
                   x-amz-client-context exclusion these defaults are per the
                   AWS documentation.

        """
        if include is None:
            include = cls.default_include_headers
        include = [x.lower() for x in include]
        headers = req.headers.copy()
        # Temporarily include the host header - AWS requires it to be included
        # in the signed headers, but Requests doesn't include it in a
        # PreparedRequest
        if 'host' not in headers:
            headers['host'] = urlparse(str(req.url)).netloc.split(':')[0]
        # Aggregate for upper/lowercase header name collisions in header names,
        # AMZ requires values of colliding headers be concatenated into a
        # single header with lowercase name.  Although this is not possible with
        # Requests, since it uses a case-insensitive dict to hold headers, this
        # is here just in case you duck type with a regular dict
        cano_headers_dict = {}
        for hdr, val in headers.items():
            hdr = hdr.strip().lower()
            val = cls.amz_norm_whitespace(val).strip()
            if (hdr in include or '*' in include
                or ('x-amz-*' in include and hdr.startswith('x-amz-')
                    and not hdr == 'x-amz-client-context')):
                vals = cano_headers_dict.setdefault(hdr, [])
                vals.append(val)
        # Flatten cano_headers dict to string and generate signed_headers
        cano_headers = ''
        signed_headers_list = []
        for hdr in sorted(cano_headers_dict):
            vals = cano_headers_dict[hdr]
            val = ','.join(sorted(vals))
            cano_headers += '{}:{}\n'.format(hdr, val)
            signed_headers_list.append(hdr)
        signed_headers = ';'.join(signed_headers_list)
        return (cano_headers, signed_headers)

    @staticmethod
    def get_sig_string(req, cano_req, scope):
        """
        Generate the AWS4 auth string to sign for the request.

        req      -- Requests PreparedRequest object. This should already
                    include an x-amz-date header.
        cano_req -- The Canonical Request, as returned by
                    get_canonical_request()

        """
        amz_date = req.headers['x-amz-date']
        hsh = hashlib.sha256(cano_req.encode())
        sig_items = ['AWS4-HMAC-SHA256', amz_date, scope, hsh.hexdigest()]
        sig_string = '\n'.join(sig_items)
        return sig_string

    def amz_cano_path(self, path):
        """
        Generate the canonical path as per AWS4 auth requirements.

        Not documented anywhere, determined from aws4_testsuite examples,
        problem reports and testing against the live services.

        path -- request path

        """
        safe_chars = '/~'
        qs = ''
        fixed_path = path
        if '?' in fixed_path:
            fixed_path, qs = fixed_path.split('?', 1)
        fixed_path = posixpath.normpath(fixed_path)
        fixed_path = re.sub('/+', '/', fixed_path)
        if path.endswith('/') and not fixed_path.endswith('/'):
            fixed_path += '/'
        full_path = fixed_path
        # S3 seems to require unquoting first. 'host' service is used in
        # amz_testsuite tests
        if self.service in ['s3', 'host']:
            full_path = unquote(full_path)
        full_path = quote(full_path, safe=safe_chars)
        if qs:
            full_path = '?'.join((full_path, qs))
        return full_path

    @staticmethod
    def amz_cano_querystring(qs):
        """
        Parse and format querystring as per AWS4 auth requirements.

        Perform percent quoting as needed.

        qs -- querystring

        """
        safe_qs_unresvd = '-_.~'
        qs = qs.split(' ')[0]
        # prevent parse_qs from interpreting semicolon as an alternative delimiter to ampersand
        qs = qs.replace(';', '%3B')
        qs_items = {}
        for name, vals in parse_qs(qs, keep_blank_values=True).items():
            name = quote(name, safe=safe_qs_unresvd)
            vals = [quote(val, safe=safe_qs_unresvd) for val in vals]
            qs_items[name] = vals
        qs_strings = []
        for name in sorted(qs_items):
            vals = qs_items[name]
            for val in sorted(vals):
                qs_strings.append('='.join([name, val]))
        qs = '&'.join(qs_strings)
        return qs

    @staticmethod
    def amz_norm_whitespace(text):
        """
        Replace runs of whitespace with a single space.

        Ignore text enclosed in quotes.

        """
        if re.search(r'\s', text):
            return ' '.join(shlex.split(text, posix=False))
        return text


class StrictAWS4Auth(AWS4Auth):
    """
    Instances of this subclass will not automatically regenerate their signing
    keys when asked to sign a request whose date does not match the scope date
    of the signing key. Instances will instead raise a DateMismatchError.

    Keys of StrictAWSAuth instances can be regenerated manually by calling the
    regenerate_signing_key() method.

    Keys will still store the secret key by default. If this is not desired
    then create the instance by passing an AWS4SigningKey created with
    store_secret_key set to False to the StrictAWS4AUth constructor:

    >>> sig_key = AWS4SigningKey(secret_key, region, service, date, False)
    >>> auth = StrictAWS4Auth(access_id, sig_key)

    """

    def handle_date_mismatch(self, req):
        """
        Handle a request whose date doesn't match the signing key process, by
        raising a DateMismatchError.

        Overrides the default behaviour of AWS4Auth where the signing key
        is automatically regenerated to match the request date

        To update the signing key if this is hit, call
        StrictAWS4Auth.regenerate_signing_key().

        """
        raise DateMismatchError


class PassiveAWS4Auth(AWS4Auth):
    """
    This subclass does not perform any special handling of a mismatched request
    and scope date, it signs the request and allows Requests to send it. It is
    up to the calling code to handle a failed authentication response from AWS.

    This behaviour mimics the behaviour of AWS4Auth for versions 0.7 and
    earlier.

    """

    def handle_date_mismatch(self, req):
        pass
