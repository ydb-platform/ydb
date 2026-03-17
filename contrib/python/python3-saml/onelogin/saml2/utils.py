# -*- coding: utf-8 -*-

""" OneLogin_Saml2_Utils class


Auxiliary class of SAML Python Toolkit.

"""

import base64
import warnings
from copy import deepcopy
import calendar
from datetime import datetime
from hashlib import sha1, sha256, sha384, sha512
from isodate import parse_duration as duration_parser
import re
from textwrap import wrap
from functools import wraps
from uuid import uuid4
from xml.dom.minidom import Element
import zlib
import xmlsec

from onelogin.saml2 import compat
from onelogin.saml2.constants import OneLogin_Saml2_Constants
from onelogin.saml2.errors import OneLogin_Saml2_Error, OneLogin_Saml2_ValidationError
from onelogin.saml2.xml_utils import OneLogin_Saml2_XML


try:
    from urllib.parse import quote_plus, urlsplit, urlunsplit  # py3
except ImportError:
    from urlparse import urlsplit, urlunsplit
    from urllib import quote_plus  # py2


def return_false_on_exception(func):
    """
    Decorator. When applied to a function, it will, by default, suppress any exceptions
    raised by that function and return False. It may be overridden by passing a
    "raise_exceptions" keyword argument when calling the wrapped function.
    """
    @wraps(func)
    def exceptfalse(*args, **kwargs):
        if not kwargs.pop('raise_exceptions', False):
            try:
                return func(*args, **kwargs)
            except Exception:
                return False
        else:
            return func(*args, **kwargs)
    return exceptfalse


class OneLogin_Saml2_Utils(object):
    """

    Auxiliary class that contains several utility methods to parse time,
    urls, add sign, encrypt, decrypt, sign validation, handle xml ...

    """

    RESPONSE_SIGNATURE_XPATH = '/samlp:Response/ds:Signature'
    ASSERTION_SIGNATURE_XPATH = '/samlp:Response/saml:Assertion/ds:Signature'

    TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
    TIME_FORMAT_2 = "%Y-%m-%dT%H:%M:%S.%fZ"
    TIME_FORMAT_WITH_FRAGMENT = re.compile(r'^(\d{4,4}-\d{2,2}-\d{2,2}T\d{2,2}:\d{2,2}:\d{2,2})(\.\d*)?Z?$')

    @staticmethod
    def escape_url(url, lowercase_urlencoding=False):
        """
        escape the non-safe symbols in url
        The encoding used by ADFS 3.0 is not compatible with
        python's quote_plus (ADFS produces lower case hex numbers and quote_plus produces
        upper case hex numbers)
        :param url: the url to escape
        :type url: str

        :param lowercase_urlencoding: lowercase or no
        :type lowercase_urlencoding: boolean

        :return: the escaped url
        :rtype str
        """
        encoded = quote_plus(url)
        return re.sub(r"%[A-F0-9]{2}", lambda m: m.group(0).lower(), encoded) if lowercase_urlencoding else encoded

    @staticmethod
    def b64encode(data):
        """base64 encode"""
        return compat.to_string(base64.b64encode(compat.to_bytes(data)))

    @staticmethod
    def b64decode(data):
        """base64 decode"""
        return base64.b64decode(data)

    @staticmethod
    def decode_base64_and_inflate(value, ignore_zip=False):
        """
        base64 decodes and then inflates according to RFC1951
        :param value: a deflated and encoded string
        :type value: string
        :param ignore_zip: ignore zip errors
        :returns: the string after decoding and inflating
        :rtype: string
        """
        encoded = OneLogin_Saml2_Utils.b64decode(value)
        try:
            return zlib.decompress(encoded, -15)
        except zlib.error:
            if not ignore_zip:
                raise
        return encoded

    @staticmethod
    def deflate_and_base64_encode(value):
        """
        Deflates and then base64 encodes a string
        :param value: The string to deflate and encode
        :type value: string
        :returns: The deflated and encoded string
        :rtype: string
        """
        return OneLogin_Saml2_Utils.b64encode(zlib.compress(compat.to_bytes(value))[2:-4])

    @staticmethod
    def format_cert(cert, heads=True):
        """
        Returns a x509 cert (adding header & footer if required).

        :param cert: A x509 unformatted cert
        :type: string

        :param heads: True if we want to include head and footer
        :type: boolean

        :returns: Formatted cert
        :rtype: string
        """
        x509_cert = cert.replace('\x0D', '')
        x509_cert = x509_cert.replace('\r', '')
        x509_cert = x509_cert.replace('\n', '')
        if len(x509_cert) > 0:
            x509_cert = x509_cert.replace('-----BEGIN CERTIFICATE-----', '')
            x509_cert = x509_cert.replace('-----END CERTIFICATE-----', '')
            x509_cert = x509_cert.replace(' ', '')

            if heads:
                x509_cert = "-----BEGIN CERTIFICATE-----\n" + "\n".join(wrap(x509_cert, 64)) + "\n-----END CERTIFICATE-----\n"

        return x509_cert

    @staticmethod
    def format_private_key(key, heads=True):
        """
        Returns a private key (adding header & footer if required).

        :param key A private key
        :type: string

        :param heads: True if we want to include head and footer
        :type: boolean

        :returns: Formated private key
        :rtype: string
        """
        private_key = key.replace('\x0D', '')
        private_key = private_key.replace('\r', '')
        private_key = private_key.replace('\n', '')
        if len(private_key) > 0:
            if private_key.find('-----BEGIN PRIVATE KEY-----') != -1:
                private_key = private_key.replace('-----BEGIN PRIVATE KEY-----', '')
                private_key = private_key.replace('-----END PRIVATE KEY-----', '')
                private_key = private_key.replace(' ', '')
                if heads:
                    private_key = "-----BEGIN PRIVATE KEY-----\n" + "\n".join(wrap(private_key, 64)) + "\n-----END PRIVATE KEY-----\n"
            else:
                private_key = private_key.replace('-----BEGIN RSA PRIVATE KEY-----', '')
                private_key = private_key.replace('-----END RSA PRIVATE KEY-----', '')
                private_key = private_key.replace(' ', '')
                if heads:
                    private_key = "-----BEGIN RSA PRIVATE KEY-----\n" + "\n".join(wrap(private_key, 64)) + "\n-----END RSA PRIVATE KEY-----\n"
        return private_key

    @staticmethod
    def redirect(url, parameters={}, request_data={}):
        """
        Executes a redirection to the provided url (or return the target url).

        :param url: The target url
        :type: string

        :param parameters: Extra parameters to be passed as part of the url
        :type: dict

        :param request_data: The request as a dict
        :type: dict

        :returns: Url
        :rtype: string
        """
        assert isinstance(url, compat.str_type)
        assert isinstance(parameters, dict)

        if url.startswith('/'):
            url = '%s%s' % (OneLogin_Saml2_Utils.get_self_url_host(request_data), url)

        # Verify that the URL is to a http or https site.
        if re.search('^https?://', url, flags=re.IGNORECASE) is None:
            raise OneLogin_Saml2_Error(
                'Redirect to invalid URL: ' + url,
                OneLogin_Saml2_Error.REDIRECT_INVALID_URL
            )

        # Add encoded parameters
        if url.find('?') < 0:
            param_prefix = '?'
        else:
            param_prefix = '&'

        for name, value in parameters.items():

            if value is None:
                param = OneLogin_Saml2_Utils.escape_url(name)
            elif isinstance(value, list):
                param = ''
                for val in value:
                    param += OneLogin_Saml2_Utils.escape_url(name) + '[]=' + OneLogin_Saml2_Utils.escape_url(val) + '&'
                if len(param) > 0:
                    param = param[0:-1]
            else:
                param = OneLogin_Saml2_Utils.escape_url(name) + '=' + OneLogin_Saml2_Utils.escape_url(value)

            if param:
                url += param_prefix + param
                param_prefix = '&'

        return url

    @staticmethod
    def get_self_url_host(request_data):
        """
        Returns the protocol + the current host + the port (if different than
        common ports).

        :param request_data: The request as a dict
        :type: dict

        :return: Url
        :rtype: string
        """
        current_host = OneLogin_Saml2_Utils.get_self_host(request_data)
        protocol = 'https' if OneLogin_Saml2_Utils.is_https(request_data) else 'http'

        if request_data.get('server_port') is not None:
            warnings.warn(
                'The server_port key in request data is deprecated. '
                'The http_host key should include a port, if required.',
                category=DeprecationWarning,
            )
            port_suffix = ':%s' % request_data['server_port']
            if not current_host.endswith(port_suffix):
                if not ((protocol == 'https' and port_suffix == ':443') or (protocol == 'http' and port_suffix == ':80')):
                    current_host += port_suffix

        return '%s://%s' % (protocol, current_host)

    @staticmethod
    def get_self_host(request_data):
        """
        Returns the current host (which may include a port number part).

        :param request_data: The request as a dict
        :type: dict

        :return: The current host
        :rtype: string
        """
        if 'http_host' in request_data:
            return request_data['http_host']
        elif 'server_name' in request_data:
            warnings.warn("The server_name key in request data is undocumented & deprecated.", category=DeprecationWarning)
            return request_data['server_name']
        raise Exception('No hostname defined')

    @staticmethod
    def is_https(request_data):
        """
        Checks if https or http.

        :param request_data: The request as a dict
        :type: dict

        :return: False if https is not active
        :rtype: boolean
        """
        is_https = 'https' in request_data and request_data['https'] != 'off'
        # TODO: this use of server_port should be removed too
        is_https = is_https or ('server_port' in request_data and str(request_data['server_port']) == '443')
        return is_https

    @staticmethod
    def get_self_url_no_query(request_data):
        """
        Returns the URL of the current host + current view.

        :param request_data: The request as a dict
        :type: dict

        :return: The url of current host + current view
        :rtype: string
        """
        self_url_host = OneLogin_Saml2_Utils.get_self_url_host(request_data)
        script_name = request_data['script_name']
        if script_name:
            if script_name[0] != '/':
                script_name = '/' + script_name
        else:
            script_name = ''
        self_url_no_query = self_url_host + script_name
        if 'path_info' in request_data:
            self_url_no_query += request_data['path_info']

        return self_url_no_query

    @staticmethod
    def get_self_routed_url_no_query(request_data):
        """
        Returns the routed URL of the current host + current view.

        :param request_data: The request as a dict
        :type: dict

        :return: The url of current host + current view
        :rtype: string
        """
        self_url_host = OneLogin_Saml2_Utils.get_self_url_host(request_data)
        route = ''
        if 'request_uri' in request_data and request_data['request_uri']:
            route = request_data['request_uri']
            if 'query_string' in request_data and request_data['query_string']:
                route = route.replace(request_data['query_string'], '')

        return self_url_host + route

    @staticmethod
    def get_self_url(request_data):
        """
        Returns the URL of the current host + current view + query.

        :param request_data: The request as a dict
        :type: dict

        :return: The url of current host + current view + query
        :rtype: string
        """
        self_url_host = OneLogin_Saml2_Utils.get_self_url_host(request_data)

        request_uri = ''
        if 'request_uri' in request_data:
            request_uri = request_data['request_uri']
            if not request_uri.startswith('/'):
                match = re.search('^https?://[^/]*(/.*)', request_uri)
                if match is not None:
                    request_uri = match.groups()[0]

        return self_url_host + request_uri

    @staticmethod
    def generate_unique_id():
        """
        Generates an unique string (used for example as ID for assertions).

        :return: A unique string
        :rtype: string
        """
        return 'ONELOGIN_%s' % sha1(compat.to_bytes(uuid4().hex)).hexdigest()

    @staticmethod
    def parse_time_to_SAML(time):
        r"""
        Converts a UNIX timestamp to SAML2 timestamp on the form
        yyyy-mm-ddThh:mm:ss(\.s+)?Z.

        :param time: The time we should convert (DateTime).
        :type: string

        :return: SAML2 timestamp.
        :rtype: string
        """
        data = datetime.utcfromtimestamp(float(time))
        return data.strftime(OneLogin_Saml2_Utils.TIME_FORMAT)

    @staticmethod
    def parse_SAML_to_time(timestr):
        r"""
        Converts a SAML2 timestamp on the form yyyy-mm-ddThh:mm:ss(\.s+)?Z
        to a UNIX timestamp. The sub-second part is ignored.

        :param timestr: The time we should convert (SAML Timestamp).
        :type: string

        :return: Converted to a unix timestamp.
        :rtype: int
        """
        try:
            data = datetime.strptime(timestr, OneLogin_Saml2_Utils.TIME_FORMAT)
        except ValueError:
            try:
                data = datetime.strptime(timestr, OneLogin_Saml2_Utils.TIME_FORMAT_2)
            except ValueError:
                elem = OneLogin_Saml2_Utils.TIME_FORMAT_WITH_FRAGMENT.match(timestr)
                if not elem:
                    raise Exception("time data %s does not match format %s" % (timestr, r'yyyy-mm-ddThh:mm:ss(\.s+)?Z'))
                data = datetime.strptime(elem.groups()[0] + "Z", OneLogin_Saml2_Utils.TIME_FORMAT)

        return calendar.timegm(data.utctimetuple())

    @staticmethod
    def now():
        """
        :return: unix timestamp of actual time.
        :rtype: int
        """
        return calendar.timegm(datetime.utcnow().utctimetuple())

    @staticmethod
    def parse_duration(duration, timestamp=None):
        """
        Interprets a ISO8601 duration value relative to a given timestamp.

        :param duration: The duration, as a string.
        :type: string

        :param timestamp: The unix timestamp we should apply the duration to.
                          Optional, default to the current time.
        :type: string

        :return: The new timestamp, after the duration is applied.
        :rtype: int
        """
        assert isinstance(duration, compat.str_type)
        assert timestamp is None or isinstance(timestamp, int)

        timedelta = duration_parser(duration)
        if timestamp is None:
            data = datetime.utcnow() + timedelta
        else:
            data = datetime.utcfromtimestamp(timestamp) + timedelta
        return calendar.timegm(data.utctimetuple())

    @staticmethod
    def get_expire_time(cache_duration=None, valid_until=None):
        """
        Compares 2 dates and returns the earliest.

        :param cache_duration: The duration, as a string.
        :type: string

        :param valid_until: The valid until date, as a string or as a timestamp
        :type: string

        :return: The expiration time.
        :rtype: int
        """
        expire_time = None

        if cache_duration is not None:
            expire_time = OneLogin_Saml2_Utils.parse_duration(cache_duration)

        if valid_until is not None:
            if isinstance(valid_until, int):
                valid_until_time = valid_until
            else:
                valid_until_time = OneLogin_Saml2_Utils.parse_SAML_to_time(valid_until)
            if expire_time is None or expire_time > valid_until_time:
                expire_time = valid_until_time

        if expire_time is not None:
            return '%d' % expire_time
        return None

    @staticmethod
    def delete_local_session(callback=None):
        """
        Deletes the local session.
        """

        if callback is not None:
            callback()

    @staticmethod
    def calculate_x509_fingerprint(x509_cert, alg='sha1'):
        """
        Calculates the fingerprint of a formatted x509cert.

        :param x509_cert: x509 cert formatted
        :type: string

        :param alg: The algorithm to build the fingerprint
        :type: string

        :returns: fingerprint
        :rtype: string
        """
        assert isinstance(x509_cert, compat.str_type)

        lines = x509_cert.split('\n')
        data = ''
        inData = False

        for line in lines:
            # Remove '\r' from end of line if present.
            line = line.rstrip()
            if not inData:
                if line == '-----BEGIN CERTIFICATE-----':
                    inData = True
                elif line == '-----BEGIN PUBLIC KEY-----' or line == '-----BEGIN RSA PRIVATE KEY-----':
                    # This isn't an X509 certificate.
                    return None
            else:
                if line == '-----END CERTIFICATE-----':
                    break

                # Append the current line to the certificate data.
                data += line

        if not data:
            return None

        decoded_data = base64.b64decode(compat.to_bytes(data))

        if alg == 'sha512':
            fingerprint = sha512(decoded_data)
        elif alg == 'sha384':
            fingerprint = sha384(decoded_data)
        elif alg == 'sha256':
            fingerprint = sha256(decoded_data)
        else:
            fingerprint = sha1(decoded_data)

        return fingerprint.hexdigest().lower()

    @staticmethod
    def format_finger_print(fingerprint):
        """
        Formats a fingerprint.

        :param fingerprint: fingerprint
        :type: string

        :returns: Formatted fingerprint
        :rtype: string
        """
        formatted_fingerprint = fingerprint.replace(':', '')
        return formatted_fingerprint.lower()

    @staticmethod
    def generate_name_id(value, sp_nq, sp_format=None, cert=None, debug=False, nq=None):
        """
        Generates a nameID.

        :param value: fingerprint
        :type: string

        :param sp_nq: SP Name Qualifier
        :type: string

        :param sp_format: SP Format
        :type: string

        :param cert: IdP Public Cert to encrypt the nameID
        :type: string

        :param debug: Activate the xmlsec debug
        :type: bool

        :returns: DOMElement | XMLSec nameID
        :rtype: string

        :param nq: IDP Name Qualifier
        :type: string
        """

        root = OneLogin_Saml2_XML.make_root("{%s}container" % OneLogin_Saml2_Constants.NS_SAML)
        name_id = OneLogin_Saml2_XML.make_child(root, '{%s}NameID' % OneLogin_Saml2_Constants.NS_SAML)
        if sp_nq is not None:
            name_id.set('SPNameQualifier', sp_nq)
        if sp_format is not None:
            name_id.set('Format', sp_format)
        if nq is not None:
            name_id.set('NameQualifier', nq)
        name_id.text = value

        if cert is not None:
            xmlsec.enable_debug_trace(debug)

            # Load the public cert
            manager = xmlsec.KeysManager()
            manager.add_key(xmlsec.Key.from_memory(cert, xmlsec.KeyFormat.CERT_PEM, None))

            # Prepare for encryption
            enc_data = xmlsec.template.encrypted_data_create(
                root, xmlsec.Transform.AES128, type=xmlsec.EncryptionType.ELEMENT, ns="xenc")

            xmlsec.template.encrypted_data_ensure_cipher_value(enc_data)
            key_info = xmlsec.template.encrypted_data_ensure_key_info(enc_data, ns="dsig")
            enc_key = xmlsec.template.add_encrypted_key(key_info, xmlsec.Transform.RSA_OAEP)
            xmlsec.template.encrypted_data_ensure_cipher_value(enc_key)

            # Encrypt!
            enc_ctx = xmlsec.EncryptionContext(manager)
            enc_ctx.key = xmlsec.Key.generate(xmlsec.KeyData.AES, 128, xmlsec.KeyDataType.SESSION)
            enc_data = enc_ctx.encrypt_xml(enc_data, name_id)
            return '<saml:EncryptedID>' + compat.to_string(OneLogin_Saml2_XML.to_string(enc_data)) + '</saml:EncryptedID>'
        else:
            return OneLogin_Saml2_XML.extract_tag_text(root, "saml:NameID")

    @staticmethod
    def get_status(dom):
        """
        Gets Status from a Response.

        :param dom: The Response as XML
        :type: Document

        :returns: The Status, an array with the code and a message.
        :rtype: dict
        """
        status = {}

        status_entry = OneLogin_Saml2_XML.query(dom, '/samlp:Response/samlp:Status')
        if len(status_entry) != 1:
            raise OneLogin_Saml2_ValidationError(
                'Missing Status on response',
                OneLogin_Saml2_ValidationError.MISSING_STATUS
            )

        code_entry = OneLogin_Saml2_XML.query(dom, '/samlp:Response/samlp:Status/samlp:StatusCode', status_entry[0])
        if len(code_entry) != 1:
            raise OneLogin_Saml2_ValidationError(
                'Missing Status Code on response',
                OneLogin_Saml2_ValidationError.MISSING_STATUS_CODE
            )
        code = code_entry[0].values()[0]
        status['code'] = code

        status['msg'] = ''
        message_entry = OneLogin_Saml2_XML.query(dom, '/samlp:Response/samlp:Status/samlp:StatusMessage', status_entry[0])
        if len(message_entry) == 0:
            subcode_entry = OneLogin_Saml2_XML.query(dom, '/samlp:Response/samlp:Status/samlp:StatusCode/samlp:StatusCode', status_entry[0])
            if len(subcode_entry) == 1:
                status['msg'] = subcode_entry[0].values()[0]
        elif len(message_entry) == 1:
            status['msg'] = OneLogin_Saml2_XML.element_text(message_entry[0])

        return status

    @staticmethod
    def decrypt_element(encrypted_data, key, debug=False, inplace=False):
        """
        Decrypts an encrypted element.

        :param encrypted_data: The encrypted data.
        :type: lxml.etree.Element | DOMElement | basestring

        :param key: The key.
        :type: string

        :param debug: Activate the xmlsec debug
        :type: bool

        :param inplace: update passed data with decrypted result
        :type: bool

        :returns: The decrypted element.
        :rtype: lxml.etree.Element
        """

        if isinstance(encrypted_data, Element):
            encrypted_data = OneLogin_Saml2_XML.to_etree(str(encrypted_data.toxml()))
        if not inplace and isinstance(encrypted_data, OneLogin_Saml2_XML._element_class):
            encrypted_data = deepcopy(encrypted_data)
        elif isinstance(encrypted_data, OneLogin_Saml2_XML._text_class):
            encrypted_data = OneLogin_Saml2_XML._parse_etree(encrypted_data)

        xmlsec.enable_debug_trace(debug)
        manager = xmlsec.KeysManager()

        manager.add_key(xmlsec.Key.from_memory(key, xmlsec.KeyFormat.PEM, None))
        enc_ctx = xmlsec.EncryptionContext(manager)
        return enc_ctx.decrypt(encrypted_data)

    @staticmethod
    def add_sign(xml, key, cert, debug=False, sign_algorithm=OneLogin_Saml2_Constants.RSA_SHA256, digest_algorithm=OneLogin_Saml2_Constants.SHA256):
        """
        Adds signature key and senders certificate to an element (Message or
        Assertion).

        :param xml: The element we should sign
        :type: string | Document

        :param key: The private key
        :type: string

        :param cert: The public
        :type: string

        :param debug: Activate the xmlsec debug
        :type: bool

        :param sign_algorithm: Signature algorithm method
        :type sign_algorithm: string

        :param digest_algorithm: Digest algorithm method
        :type digest_algorithm: string

        :returns: Signed XML
        :rtype: string
        """
        if xml is None or xml == '':
            raise Exception('Empty string supplied as input')

        elem = OneLogin_Saml2_XML.to_etree(xml)

        sign_algorithm_transform_map = {
            OneLogin_Saml2_Constants.DSA_SHA1: xmlsec.Transform.DSA_SHA1,
            OneLogin_Saml2_Constants.RSA_SHA1: xmlsec.Transform.RSA_SHA1,
            OneLogin_Saml2_Constants.RSA_SHA256: xmlsec.Transform.RSA_SHA256,
            OneLogin_Saml2_Constants.RSA_SHA384: xmlsec.Transform.RSA_SHA384,
            OneLogin_Saml2_Constants.RSA_SHA512: xmlsec.Transform.RSA_SHA512
        }
        sign_algorithm_transform = sign_algorithm_transform_map.get(sign_algorithm, xmlsec.Transform.RSA_SHA256)

        signature = xmlsec.template.create(elem, xmlsec.Transform.EXCL_C14N, sign_algorithm_transform, ns='ds')

        issuer = OneLogin_Saml2_XML.query(elem, '//saml:Issuer')
        if len(issuer) > 0:
            issuer = issuer[0]
            issuer.addnext(signature)
            elem_to_sign = issuer.getparent()
        else:
            entity_descriptor = OneLogin_Saml2_XML.query(elem, '//md:EntityDescriptor')
            if len(entity_descriptor) > 0:
                elem.insert(0, signature)
            else:
                elem[0].insert(0, signature)
            elem_to_sign = elem

        elem_id = elem_to_sign.get('ID', None)
        if elem_id is not None:
            if elem_id:
                elem_id = '#' + elem_id
        else:
            generated_id = generated_id = OneLogin_Saml2_Utils.generate_unique_id()
            elem_id = '#' + generated_id
            elem_to_sign.attrib['ID'] = generated_id

        xmlsec.enable_debug_trace(debug)
        xmlsec.tree.add_ids(elem_to_sign, ["ID"])

        digest_algorithm_transform_map = {
            OneLogin_Saml2_Constants.SHA1: xmlsec.Transform.SHA1,
            OneLogin_Saml2_Constants.SHA256: xmlsec.Transform.SHA256,
            OneLogin_Saml2_Constants.SHA384: xmlsec.Transform.SHA384,
            OneLogin_Saml2_Constants.SHA512: xmlsec.Transform.SHA512
        }
        digest_algorithm_transform = digest_algorithm_transform_map.get(digest_algorithm, xmlsec.Transform.SHA256)

        ref = xmlsec.template.add_reference(signature, digest_algorithm_transform, uri=elem_id)
        xmlsec.template.add_transform(ref, xmlsec.Transform.ENVELOPED)
        xmlsec.template.add_transform(ref, xmlsec.Transform.EXCL_C14N)
        key_info = xmlsec.template.ensure_key_info(signature)
        xmlsec.template.add_x509_data(key_info)

        dsig_ctx = xmlsec.SignatureContext()
        sign_key = xmlsec.Key.from_memory(key, xmlsec.KeyFormat.PEM, None)
        sign_key.load_cert_from_memory(cert, xmlsec.KeyFormat.PEM)

        dsig_ctx.key = sign_key
        dsig_ctx.sign(signature)

        return OneLogin_Saml2_XML.to_string(elem)

    @staticmethod
    @return_false_on_exception
    def validate_sign(xml, cert=None, fingerprint=None, fingerprintalg='sha1', validatecert=False, debug=False, xpath=None, multicerts=None):
        """
        Validates a signature (Message or Assertion).

        :param xml: The element we should validate
        :type: string | Document

        :param cert: The public cert
        :type: string

        :param fingerprint: The fingerprint of the public cert
        :type: string

        :param fingerprintalg: The algorithm used to build the fingerprint
        :type: string

        :param validatecert: If true, will verify the signature and if the cert is valid.
        :type: bool

        :param debug: Activate the xmlsec debug
        :type: bool

        :param xpath: The xpath of the signed element
        :type: string

        :param multicerts: Multiple public certs
        :type: list

        :param raise_exceptions: Whether to return false on failure or raise an exception
        :type raise_exceptions: Boolean
        """
        if xml is None or xml == '':
            raise Exception('Empty string supplied as input')

        elem = OneLogin_Saml2_XML.to_etree(xml)
        xmlsec.enable_debug_trace(debug)
        xmlsec.tree.add_ids(elem, ["ID"])

        if xpath:
            signature_nodes = OneLogin_Saml2_XML.query(elem, xpath)
        else:
            signature_nodes = OneLogin_Saml2_XML.query(elem, OneLogin_Saml2_Utils.RESPONSE_SIGNATURE_XPATH)

            if len(signature_nodes) == 0:
                signature_nodes = OneLogin_Saml2_XML.query(elem, OneLogin_Saml2_Utils.ASSERTION_SIGNATURE_XPATH)

        if len(signature_nodes) == 1:
            signature_node = signature_nodes[0]

            if not multicerts:
                return OneLogin_Saml2_Utils.validate_node_sign(signature_node, elem, cert, fingerprint, fingerprintalg, validatecert, debug, raise_exceptions=True)
            else:
                # If multiple certs are provided, I may ignore cert and
                # fingerprint provided by the method and just check the
                # certs multicerts
                fingerprint = fingerprintalg = None
                for cert in multicerts:
                    if OneLogin_Saml2_Utils.validate_node_sign(signature_node, elem, cert, fingerprint, fingerprintalg, validatecert, False, raise_exceptions=False):
                        return True
                raise OneLogin_Saml2_ValidationError(
                    'Signature validation failed. SAML Response rejected.',
                    OneLogin_Saml2_ValidationError.INVALID_SIGNATURE
                )
        else:
            raise OneLogin_Saml2_ValidationError(
                'Expected exactly one signature node; got {}.'.format(len(signature_nodes)),
                OneLogin_Saml2_ValidationError.WRONG_NUMBER_OF_SIGNATURES
            )

    @staticmethod
    @return_false_on_exception
    def validate_metadata_sign(xml, cert=None, fingerprint=None, fingerprintalg='sha1', validatecert=False, debug=False):
        """
        Validates a signature of a EntityDescriptor.

        :param xml: The element we should validate
        :type: string | Document

        :param cert: The public cert
        :type: string

        :param fingerprint: The fingerprint of the public cert
        :type: string

        :param fingerprintalg: The algorithm used to build the fingerprint
        :type: string

        :param validatecert: If true, will verify the signature and if the cert is valid.
        :type: bool

        :param debug: Activate the xmlsec debug
        :type: bool

        :param raise_exceptions: Whether to return false on failure or raise an exception
        :type raise_exceptions: Boolean
        """
        if xml is None or xml == '':
            raise Exception('Empty string supplied as input')

        elem = OneLogin_Saml2_XML.to_etree(xml)
        xmlsec.enable_debug_trace(debug)
        xmlsec.tree.add_ids(elem, ["ID"])

        signature_nodes = OneLogin_Saml2_XML.query(elem, '/md:EntitiesDescriptor/ds:Signature')

        if len(signature_nodes) == 0:
            signature_nodes += OneLogin_Saml2_XML.query(elem, '/md:EntityDescriptor/ds:Signature')

            if len(signature_nodes) == 0:
                signature_nodes += OneLogin_Saml2_XML.query(elem, '/md:EntityDescriptor/md:SPSSODescriptor/ds:Signature')
                signature_nodes += OneLogin_Saml2_XML.query(elem, '/md:EntityDescriptor/md:IDPSSODescriptor/ds:Signature')

        if len(signature_nodes) > 0:
            for signature_node in signature_nodes:
                # Raises exception if invalid
                OneLogin_Saml2_Utils.validate_node_sign(signature_node, elem, cert, fingerprint, fingerprintalg, validatecert, debug, raise_exceptions=True)
            return True
        else:
            raise Exception('Could not validate metadata signature: No signature nodes found.')

    @staticmethod
    @return_false_on_exception
    def validate_node_sign(signature_node, elem, cert=None, fingerprint=None, fingerprintalg='sha1', validatecert=False, debug=False):
        """
        Validates a signature node.

        :param signature_node: The signature node
        :type: Node

        :param xml: The element we should validate
        :type: Document

        :param cert: The public cert
        :type: string

        :param fingerprint: The fingerprint of the public cert
        :type: string

        :param fingerprintalg: The algorithm used to build the fingerprint
        :type: string

        :param validatecert: If true, will verify the signature and if the cert is valid.
        :type: bool

        :param debug: Activate the xmlsec debug
        :type: bool

        :param raise_exceptions: Whether to return false on failure or raise an exception
        :type raise_exceptions: Boolean
        """
        if (cert is None or cert == '') and fingerprint:
            x509_certificate_nodes = OneLogin_Saml2_XML.query(signature_node, '//ds:Signature/ds:KeyInfo/ds:X509Data/ds:X509Certificate')
            if len(x509_certificate_nodes) > 0:
                x509_certificate_node = x509_certificate_nodes[0]
                x509_cert_value = OneLogin_Saml2_XML.element_text(x509_certificate_node)
                x509_cert_value_formatted = OneLogin_Saml2_Utils.format_cert(x509_cert_value)
                x509_fingerprint_value = OneLogin_Saml2_Utils.calculate_x509_fingerprint(x509_cert_value_formatted, fingerprintalg)
                if fingerprint == x509_fingerprint_value:
                    cert = x509_cert_value_formatted

        if cert is None or cert == '':
            raise OneLogin_Saml2_Error(
                'Could not validate node signature: No certificate provided.',
                OneLogin_Saml2_Error.CERT_NOT_FOUND
            )

        # Check if Reference URI is empty
        # reference_elem = OneLogin_Saml2_XML.query(signature_node, '//ds:Reference')
        # if len(reference_elem) > 0:
        #     if reference_elem[0].get('URI') == '':
        #         reference_elem[0].set('URI', '#%s' % signature_node.getparent().get('ID'))

        if validatecert:
            manager = xmlsec.KeysManager()
            manager.load_cert_from_memory(cert, xmlsec.KeyFormat.CERT_PEM, xmlsec.KeyDataType.TRUSTED)
            dsig_ctx = xmlsec.SignatureContext(manager)
        else:
            dsig_ctx = xmlsec.SignatureContext()
            dsig_ctx.key = xmlsec.Key.from_memory(cert, xmlsec.KeyFormat.CERT_PEM, None)

        dsig_ctx.set_enabled_key_data([xmlsec.KeyData.X509])

        try:
            dsig_ctx.verify(signature_node)
        except Exception as err:
            raise OneLogin_Saml2_ValidationError(
                'Signature validation failed. SAML Response rejected. %s',
                OneLogin_Saml2_ValidationError.INVALID_SIGNATURE,
                str(err)
            )

        return True

    @staticmethod
    def sign_binary(msg, key, algorithm=xmlsec.Transform.RSA_SHA256, debug=False):
        """
        Sign binary message

        :param msg: The element we should validate
        :type: bytes

        :param key: The private key
        :type: string

        :param debug: Activate the xmlsec debug
        :type: bool

        :return signed message
        :rtype str
        """

        if isinstance(msg, str):
            msg = msg.encode('utf8')

        xmlsec.enable_debug_trace(debug)
        dsig_ctx = xmlsec.SignatureContext()
        dsig_ctx.key = xmlsec.Key.from_memory(key, xmlsec.KeyFormat.PEM, None)
        return dsig_ctx.sign_binary(compat.to_bytes(msg), algorithm)

    @staticmethod
    def validate_binary_sign(signed_query, signature, cert=None, algorithm=OneLogin_Saml2_Constants.RSA_SHA256, debug=False):
        """
        Validates signed binary data (Used to validate GET Signature).

        :param signed_query: The element we should validate
        :type: string


        :param signature: The signature that will be validate
        :type: string

        :param cert: The public cert
        :type: string

        :param algorithm: Signature algorithm
        :type: string

        :param debug: Activate the xmlsec debug
        :type: bool
        """
        try:
            xmlsec.enable_debug_trace(debug)
            dsig_ctx = xmlsec.SignatureContext()
            dsig_ctx.key = xmlsec.Key.from_memory(cert, xmlsec.KeyFormat.CERT_PEM, None)

            sign_algorithm_transform_map = {
                OneLogin_Saml2_Constants.DSA_SHA1: xmlsec.Transform.DSA_SHA1,
                OneLogin_Saml2_Constants.RSA_SHA1: xmlsec.Transform.RSA_SHA1,
                OneLogin_Saml2_Constants.RSA_SHA256: xmlsec.Transform.RSA_SHA256,
                OneLogin_Saml2_Constants.RSA_SHA384: xmlsec.Transform.RSA_SHA384,
                OneLogin_Saml2_Constants.RSA_SHA512: xmlsec.Transform.RSA_SHA512
            }
            sign_algorithm_transform = sign_algorithm_transform_map.get(algorithm, xmlsec.Transform.RSA_SHA256)

            dsig_ctx.verify_binary(compat.to_bytes(signed_query),
                                   sign_algorithm_transform,
                                   compat.to_bytes(signature))
            return True
        except xmlsec.Error as e:
            if debug:
                print(e)
            return False

    @staticmethod
    def normalize_url(url):
        """
        Returns normalized URL for comparison.
        This method converts the netloc to lowercase, as it should be case-insensitive (per RFC 4343, RFC 7617)
        If standardization fails, the original URL is returned
        Python documentation indicates that URL split also normalizes query strings if empty query fields are present

        :param url: URL
        :type url: String

        :returns: A normalized URL, or the given URL string if parsing fails
        :rtype: String
        """
        try:
            scheme, netloc, path, query, fragment = urlsplit(url)
            normalized_url = urlunsplit((scheme.lower(), netloc.lower(), path, query, fragment))
            return normalized_url
        except Exception:
            return url
