import base64
import hashlib
import hmac
import random
import struct
import sys
import platform

from puresasl import SASLError, SASLProtocolException, QOP

try:
    import kerberos
    have_kerberos = True
except ImportError:
    have_kerberos = False

if platform.system() == 'Windows':
    try:
        import winkerberos as kerberos
        # Fix for different capitalisation in winkerberos method name
        kerberos.authGSSClientUserName = kerberos.authGSSClientUsername
        have_kerberos = True
    except ImportError:
        have_kerberos = False

PY3 = sys.version_info[0] == 3
if PY3:
    def _b(s):
        return s.encode("utf-8")
else:
    def _b(s):
        return s


class Mechanism(object):
    """
    The base class for all mechanisms.
    """

    name = None
    """ The IANA registered name for the mechanism. """

    score = 0
    """ A relative security score where higher scores correspond
    to more secure mechanisms. """

    complete = False
    """ Set to True when SASL negotiation has completed succesfully. """

    has_initial_response = False

    allows_anonymous = True
    """ True if the mechanism allows for anonymous logins. """

    uses_plaintext = True
    """ True if the mechanism transmits sensitive information in plaintext. """

    active_safe = False
    """ True if the mechanism is safe against active attacks. """

    dictionary_safe = False
    """ True if the mechanism is safe against passive dictionary attacks. """

    qops = [QOP.AUTH]
    """ QOPs supported by the Mechanism """

    qop = QOP.AUTH
    """ Selected QOP """

    def __init__(self, sasl, **props):
        self.sasl = sasl

    def process(self, challenge=None):
        """
        Process a challenge request and return the response.

        :param challenge: A challenge issued by the server that
                          must be answered for authentication.
        """
        raise NotImplementedError()

    def wrap(self, outgoing):
        """
        Wrap an outgoing message intended for the SASL server. Depending
        on the negotiated quality of protection, this may result in the
        message being signed, encrypted, or left unaltered.
        """
        raise NotImplementedError()

    def unwrap(self, incoming):
        """
        Unwrap a message from the SASL server. Depending on the negotiated
        quality of protection, this may check a signature, decrypt the message,
        or leave the message unaltered.
        """
        raise NotImplementedError()

    def dispose(self):
        """
        Clear all sensitive data, such as passwords.
        """
        pass

    def _fetch_properties(self, *properties):
        """
        Ensure this mechanism has the needed properties. If they haven't
        been set yet, the registered callback function will be called for
        each property to retrieve a value.
        """
        needed = [p for p in properties if getattr(self, p, None) is None]
        if needed and not self.sasl.callback:
            raise SASLError('The following properties are required, but a '
                            'callback has not been set: %s' % ', '.join(needed))

        for prop in needed:
            setattr(self, prop, self.sasl.callback(prop))

    def _pick_qop(self, server_qop_set):
        """
        Choose a quality of protection based on the user's requirements,
        what the server supports, and what the mechanism supports.
        """
        user_qops = set(_b(qop) if isinstance(qop, str) else qop for qop in self.sasl.qops)  # normalize user-defined config
        supported_qops = set(self.qops)
        available_qops = user_qops & supported_qops & server_qop_set
        if not available_qops:
            user = b', '.join(user_qops).decode('ascii')
            supported = b', '.join(supported_qops).decode('ascii')
            offered = b', '.join(server_qop_set).decode('ascii')
            raise SASLProtocolException("Your requested quality of "
                                        "protection is one of (%s), the server is "
                                        "offering (%s), and %s supports (%s)" % (user, offered, self.name, supported))
        else:
            for qop in (QOP.AUTH_CONF, QOP.AUTH_INT, QOP.AUTH):
                if qop in available_qops:
                    self.qop = qop
                    break


class AnonymousMechanism(Mechanism):
    """
    An anonymous user login mechanism.
    """
    name = 'ANONYMOUS'
    score = 0

    uses_plaintext = False

    def process(self, challenge=None):
        self.complete = True
        return b'Anonymous, None'


class PlainMechanism(Mechanism):
    """
    A plaintext user/password based mechanism.
    """
    name = 'PLAIN'
    score = 1

    allows_anonymous = False

    def wrap(self, outgoing):
        return outgoing

    def unwrap(self, incoming):
        return incoming

    def __init__(self, sasl, username=None, password=None, identity='', **props):
        Mechanism.__init__(self, sasl)
        self.identity = identity
        self.username = username
        self.password = password

    def process(self, challenge=None):
        self._fetch_properties('username', 'password')
        self.complete = True
        auth_id = self.sasl.authorization_id or self.identity
        return b''.join((_b(auth_id), b'\x00', _b(self.username), b'\x00', _b(self.password)))

    def dispose(self):
        self.password = None


class ExternalMechanism(Mechanism):
    """
    The EXTERNAL mechanism allows a client to request the server to use
    credentials established by means external to the mechanism to
    authenticate the client.
    """
    name = 'EXTERNAL'
    score = 10

    def wrap(self, outgoing):
        return outgoing

    def unwrap(self, incoming):
        return incoming

    def process(self, challenge=None):
        return b''


class CramMD5Mechanism(PlainMechanism):
    name = "CRAM-MD5"
    score = 20

    allows_anonymous = False
    uses_plaintext = False

    def __init__(self, sasl, username=None, password=None, **props):
        Mechanism.__init__(self, sasl)
        self.username = username
        self.password = password

    def process(self, challenge=None):
        if challenge is None:
            return None

        self._fetch_properties('username', 'password')
        mac = hmac.HMAC(key=_b(self.password), digestmod=hashlib.md5)
        mac.update(challenge)
        self.complete = True
        return b''.join((_b(self.username), b' ', _b(mac.hexdigest())))

    def dispose(self):
        self.password = None


# functions used in DigestMD5 which were originally defined in the now-removed
# util module

def to_bytes(text):
    """
    Convert Unicode text to UTF-8 encoded bytes.

    Since Python 2.6+ and Python 3+ have similar but incompatible
    signatures, this function unifies the two to keep code sane.

    :param text: Unicode text to convert to bytes
    :rtype: bytes (Python3), str (Python2.6+)
    """
    if sys.version_info < (3, 0):
        import __builtin__
        return __builtin__.bytes(text)
    else:
        import builtins
        if isinstance(text, builtins.bytes):
            # We already have bytes, so do nothing
            return text
        if isinstance(text, list):
            # Convert a list of integers to bytes
            return builtins.bytes(text)
        else:
            # Convert UTF-8 text to bytes
            return builtins.bytes(str(text), encoding='utf-8')


def quote(text):
    """
    Enclose in quotes and escape internal slashes and double quotes.

    :param text: A Unicode or byte string.
    """
    text = to_bytes(text)
    return b'"' + text.replace(b'\\', b'\\\\').replace(b'"', b'\\"') + b'"'


class DigestMD5Mechanism(Mechanism):

    name = "DIGEST-MD5"
    score = 30

    allows_anonymous = False
    uses_plaintext = False

    def __init__(self, sasl, username=None, password=None, **props):
        Mechanism.__init__(self, sasl)
        self.username = username
        self.password = password

        self._digest_uri = None
        self._a1 = None

    def dispose(self):
        self._digest_uri = None
        self._a1 = None

        self.password = None
        self.key_hash = None
        self.realm = None
        self.nonce = None
        self.cnonce = None
        self.nc = 0

    def wrap(self, outgoing):
        return outgoing

    def unwrap(self, incoming):
        return incoming

    def response(self):
        required_props = ['username']
        if not getattr(self, 'key_hash', None):
            required_props.append('password')
        self._fetch_properties(*required_props)

        resp = {}
        resp['qop'] = self.qop

        if getattr(self, 'realm', None) is not None:
            resp['realm'] = quote(self.realm)

        resp['username'] = quote(to_bytes(self.username))
        resp['nonce'] = quote(self.nonce)
        if self.nc == 0:
            self.cnonce = to_bytes('%s' % random.random())[2:]
        resp['cnonce'] = quote(self.cnonce)
        self.nc += 1
        resp['nc'] = to_bytes('%08x' % self.nc)

        self._digest_uri = (
                to_bytes(self.sasl.service) + b'/' + to_bytes(self.sasl.host))
        resp['digest-uri'] = quote(self._digest_uri)

        a2 = b'AUTHENTICATE:' + self._digest_uri
        if self.qop != QOP.AUTH:
            a2 += b':00000000000000000000000000000000'
            resp['maxbuf'] = b'16777215'  # 2**24-1
        resp['response'] = self.gen_hash(a2)
        return b','.join(
            [
                to_bytes(k) + b'=' + to_bytes(v)
                for k, v in resp.items()
            ]
        )

    @staticmethod
    def parse_challenge(challenge):
        """Parse a digest challenge message.

        :param ``bytes`` challenge:
            Challenge message from the server, in bytes.
        :returns:
            ``dict`` of ``str`` keyword to ``bytes`` values.
        """
        ret = {}
        var = b''
        val = b''
        in_var = True
        in_quotes = False
        new = False
        escaped = False
        for c in challenge:
            if sys.version_info[0] == 3:
                c = to_bytes([c])
            if in_var:
                if c.isspace():
                    continue
                if c == b'=':
                    in_var = False
                    new = True
                else:
                    var += c
            else:
                if new:
                    if c == b'"':
                        in_quotes = True
                    else:
                        val += c
                    new = False
                elif in_quotes:
                    if escaped:
                        escaped = False
                        val += c
                    else:
                        if c == b'\\':
                            escaped = True
                        elif c == b'"':
                            in_quotes = False
                        else:
                            val += c
                else:
                    if c == b',':
                        if var:
                            ret[var.decode('ascii')] = val
                        var = b''
                        val = b''
                        in_var = True
                    else:
                        val += c
        if var:
            ret[var.decode('ascii')] = val
        return ret

    def gen_hash(self, a2):
        if not getattr(self, 'key_hash', None):
            key_hash = hashlib.md5()
            user = to_bytes(self.username)
            password = to_bytes(self.password)
            realm = to_bytes(self.realm)
            kh = user + b':' + realm + b':' + password
            key_hash.update(kh)
            self.key_hash = key_hash.digest()

        a1 = hashlib.md5(self.key_hash)
        a1h = b':' + self.nonce + b':' + self.cnonce
        a1.update(a1h)
        response = hashlib.md5()
        self._a1 = a1.digest()
        rv = to_bytes(a1.hexdigest().lower())
        rv += b':' + self.nonce
        rv += b':' + to_bytes('%08x' % self.nc)
        rv += b':' + self.cnonce
        rv += b':' + self.qop
        rv += b':' + to_bytes(hashlib.md5(a2).hexdigest().lower())
        response.update(rv)
        return to_bytes(response.hexdigest().lower())

    def authenticate_server(self, cmp_hash):
        a2 = b':' + self._digest_uri
        if self.qop != QOP.AUTH:
            a2 += b':00000000000000000000000000000000'
        if self.gen_hash(a2) != cmp_hash:
            raise SASLProtocolException('Invalid server auth response')

    def process(self, challenge=None):
        if challenge is None:
            needed = ['username', 'realm', 'nonce', 'key_hash',
                      'nc', 'cnonce', 'qops']
            if all(getattr(self, p, None) is not None for p in needed):
                return self.response()
            else:
                return None

        challenge_dict = DigestMD5Mechanism.parse_challenge(challenge)
        if 'rspauth' in challenge_dict:
            self.authenticate_server(challenge_dict['rspauth'])
            self.complete = True
            return None

        if 'realm' not in challenge_dict:
            self._fetch_properties('realm')
            challenge_dict['realm'] = self.realm

        for key in ('nonce', 'realm'):
            # TODO: rfc2831#section-2.1.1 realm: "Multiple realm directives are
            # allowed, in which case the user or client must choose one as the
            # realm for which to supply to username and password"
            # TODO: rfc2831#section-2.1.1 nonce: "This directive is required
            # and MUST appear exactly once; if not present, or if multiple
            # instances are present, the client should abort the authentication
            # exchange"
            if key in challenge_dict:
                setattr(self, key, challenge_dict[key])

        self.nc = 0
        if 'qop' in challenge_dict:
            server_offered_qops = [
                x.strip() for x in challenge_dict['qop'].split(b',')
            ]
        else:
            server_offered_qops = [QOP.AUTH]
        self._pick_qop(set(server_offered_qops))

        if 'maxbuf' in challenge_dict:
            self.max_buffer = min(
                self.sasl.max_buffer, int(challenge_dict['maxbuf']))

        # TODO: rfc2831#section-2.1.1 algorithm: This directive is required and
        # MUST appear exactly once; if not present, or if multiple instances
        # are present, the client should abort the authentication exchange.
        return self.response()


class GSSAPIMechanism(Mechanism):
    name = 'GSSAPI'
    score = 100
    qops = QOP.all

    allows_anonymous = False
    uses_plaintext = False
    active_safe = True

    def __init__(self, sasl, principal=None, **props):
        Mechanism.__init__(self, sasl)
        self.user = None
        self._have_negotiated_details = False
        self.host = self.sasl.host
        self.service = self.sasl.service
        self.principal = principal
        self._fetch_properties('host', 'service')

        krb_service = '@'.join((self.service, self.host))
        try:
            _, self.context = kerberos.authGSSClientInit(service=krb_service,
                                                         principal=self.principal)
        except TypeError:
            if self.principal is not None:
                raise Exception("Error: kerberos library does not support principal.")
            _, self.context = kerberos.authGSSClientInit(service=krb_service)

    def process(self, challenge=None):
        if not self._have_negotiated_details:
            kerberos.authGSSClientStep(self.context, '')
            _negotiated_details = kerberos.authGSSClientResponse(self.context)
            self._have_negotiated_details = True
            return base64.b64decode(_negotiated_details)

        challenge = base64.b64encode(challenge).decode('ascii')  # kerberos methods expect strings, not bytes
        if self.user is None:
            ret = kerberos.authGSSClientStep(self.context, challenge)
            if ret == kerberos.AUTH_GSS_COMPLETE:
                self.user = kerberos.authGSSClientUserName(self.context)
                return b''
            else:
                response = kerberos.authGSSClientResponse(self.context)
                if response:
                    response = base64.b64decode(response)
                else:
                    response = b''
            return response

        kerberos.authGSSClientUnwrap(self.context, challenge)
        data = kerberos.authGSSClientResponse(self.context)
        plaintext_data = base64.b64decode(data)
        if len(plaintext_data) != 4:
            raise SASLProtocolException("Bad response from server")  # todo: better message

        word, = struct.unpack('!I', plaintext_data)
        qop_bits = word >> 24
        max_length = word & 0xffffff
        server_offered_qops = QOP.names_from_bitmask(qop_bits)
        self._pick_qop(server_offered_qops)

        self.max_buffer = min(self.sasl.max_buffer, max_length)

        """
        byte 0: the selected qop. 1==auth, 2==auth-int, 4==auth-conf
        byte 1-3: the max length for any buffer sent back and forth on
            this connection. (big endian)
        the rest of the buffer: the authorization user name in UTF-8 -
            not null terminated.
        """
        auth_id = self.sasl.authorization_id or self.user
        l = len(auth_id)
        fmt = '!I' + str(l) + 's'
        word = QOP.flag_from_name(self.qop) << 24 | self.max_buffer
        out = struct.pack(fmt, word, _b(auth_id),)

        encoded = base64.b64encode(out).decode('ascii')

        kerberos.authGSSClientWrap(self.context, encoded)
        response = kerberos.authGSSClientResponse(self.context)
        self.complete = True
        return base64.b64decode(response)

    def wrap(self, outgoing):
        if self.qop != QOP.AUTH:
            outgoing = base64.b64encode(outgoing)
            if self.qop == QOP.AUTH_CONF:
                protect = 1
            else:
                protect = 0
            kerberos.authGSSClientWrap(self.context, outgoing.decode('utf8'), None, protect)
            return base64.b64decode(kerberos.authGSSClientResponse(self.context))
        else:
            return outgoing

    def unwrap(self, incoming):
        if self.qop != QOP.AUTH:
            incoming = base64.b64encode(incoming).decode('ascii')
            kerberos.authGSSClientUnwrap(self.context, incoming)
            conf = kerberos.authGSSClientResponseConf(self.context)
            if 0 == conf and self.qop == QOP.AUTH_CONF:
                raise Exception("Error: confidentiality requested, but not honored by the server.")
            return base64.b64decode(kerberos.authGSSClientResponse(self.context))
        else:
            return incoming

    def dispose(self):
        kerberos.authGSSClientClean(self.context)


#: Global registry mapping mechanism names to implementation classes.
mechanisms = dict((m.name, m) for m in (
    AnonymousMechanism,
    PlainMechanism,
    ExternalMechanism,
    CramMD5Mechanism,
    DigestMD5Mechanism))

if have_kerberos:
    mechanisms[GSSAPIMechanism.name] = GSSAPIMechanism
