"""ndg_httpsclient - module containing SSL peer verification class.
"""
__author__ = "P J Kershaw (STFC)"
__date__ = "09/12/11"
__copyright__ = "(C) 2012 Science and Technology Facilities Council"
__license__ = "BSD - see LICENSE file in top-level directory"
__contact__ = "Philip.Kershaw@stfc.ac.uk"
__revision__ = '$Id$'
import re
import logging
log = logging.getLogger(__name__)

try:
    from ndg.httpsclient.subj_alt_name import SubjectAltName
    from pyasn1.codec.der import decoder as der_decoder
    SUBJ_ALT_NAME_SUPPORT = True
    
except ImportError as e:
    SUBJ_ALT_NAME_SUPPORT = False
    SUBJ_ALT_NAME_SUPPORT_MSG = (
        'SubjectAltName support is disabled - check pyasn1 package '
        'installation to enable'
    )
    import warnings
    warnings.warn(SUBJ_ALT_NAME_SUPPORT_MSG)


class ServerSSLCertVerification(object):
    """Check server identity.  If hostname doesn't match, allow match of
    host's Distinguished Name against server DN setting"""
    DN_LUT = {
        'commonName':               'CN',
        'organisationalUnitName':   'OU',
        'organisation':             'O',
        'countryName':              'C',
        'emailAddress':             'EMAILADDRESS',
        'localityName':             'L',
        'stateOrProvinceName':      'ST',
        'streetAddress':            'STREET',
        'domainComponent':          'DC',
        'userid':                   'UID'
    }
    SUBJ_ALT_NAME_EXT_NAME = b'subjectAltName'
    PARSER_RE_STR = '/(%s)=' % '|'.join(list(DN_LUT.keys()) + \
                                        list(DN_LUT.values()))
    PARSER_RE = re.compile(PARSER_RE_STR)

    __slots__ = ('__hostname', '__certDN', '__subj_alt_name_match')

    def __init__(self, certDN=None, hostname=None, subj_alt_name_match=True):
        """Override parent class __init__ to enable setting of certDN
        setting

        @type certDN: string
        @param certDN: Set the expected Distinguished Name of the
        server to avoid errors matching hostnames.  This is useful
        where the hostname is not fully qualified
        @type hostname: string
        @param hostname: hostname to match against peer certificate 
        subjectAltNames or subject common name
        @type subj_alt_name_match: bool
        @param subj_alt_name_match: flag to enable/disable matching of hostname
        against peer certificate subjectAltNames.  Nb. A setting of True will 
        be ignored if the pyasn1 package is not installed
        """
        self.__certDN = None
        self.__hostname = None

        if certDN is not None:
            self.certDN = certDN

        if hostname is not None:
            self.hostname = hostname
        
        if subj_alt_name_match:
            if not SUBJ_ALT_NAME_SUPPORT:
                log.warning('Overriding "subj_alt_name_match" keyword setting: '
                            'peer verification with subjectAltNames is disabled')
                self.__subj_alt_name_match = False
            else:    
                self.__subj_alt_name_match = True
        else:
            log.debug('Disabling peer verification with subject '
                      'subjectAltNames!')
            self.__subj_alt_name_match = False

    def __call__(self, connection, peerCert, errorStatus, errorDepth,
                 preverifyOK):
        """Verify server certificate

        @type connection: OpenSSL.SSL.Connection
        @param connection: SSL connection object
        @type peerCert: basestring
        @param peerCert: server host certificate as OpenSSL.crypto.X509
        instance
        @type errorStatus: int
        @param errorStatus: error status passed from caller.  This is the value
        returned by the OpenSSL C function X509_STORE_CTX_get_error().  Look-up
        x509_vfy.h in the OpenSSL source to get the meanings of the different
        codes.  PyOpenSSL doesn't help you!
        @type errorDepth: int
        @param errorDepth: a non-negative integer representing where in the
        certificate chain the error occurred. If it is zero it occured in the
        end entity certificate, one if it is the certificate which signed the
        end entity certificate and so on.

        @type preverifyOK: int
        @param preverifyOK: the error status - 0 = Error, 1 = OK of the current
        SSL context irrespective of any verification checks done here.  If this
        function yields an OK status, it should enforce the preverifyOK value
        so that any error set upstream overrides and is honoured.
        @rtype: int
        @return: status code - 0/False = Error, 1/True = OK
        """
        if peerCert.has_expired():
            # Any expired certificate in the chain should result in an error
            log.error('Certificate %r in peer certificate chain has expired',
                      peerCert.get_subject())

            return False

        elif errorDepth == 0:
            # Only interested in DN of last certificate in the chain - this must
            # match the expected Server DN setting
            peerCertSubj = peerCert.get_subject()
            peerCertDN = peerCertSubj.get_components()
            peerCertDN.sort()

            if self.certDN is None:
                # Check hostname against peer certificate CN field instead:
                if self.hostname is None:
                    log.error('No "hostname" or "certDN" set to check peer '
                              'certificate against')
                    return False

                # Check for subject alternative names
                if self.__subj_alt_name_match:
                    dns_names = self._get_subj_alt_name(peerCert)
                    if self.hostname in dns_names:
                        return preverifyOK
                
                # If no subjectAltNames, default to check of subject Common Name   
                if peerCertSubj.commonName == self.hostname:
                    return preverifyOK
                else:
                    log.error('Peer certificate CN %r doesn\'t match the '
                              'expected CN %r', peerCertSubj.commonName,
                              self.hostname)
                    return False
            else:
                if peerCertDN == self.certDN:
                    return preverifyOK
                else:
                    log.error('Peer certificate DN %r doesn\'t match the '
                              'expected DN %r', peerCertDN, self.certDN)
                    return False
        else:
            return preverifyOK

    def get_verify_server_cert_func(self):
        def verify_server_cert(connection, peerCert, errorStatus, errorDepth,
                preverifyOK):
            return self.__call__(connection, peerCert, errorStatus,
                                 errorDepth, preverifyOK)
        
        return verify_server_cert

    @classmethod
    def _get_subj_alt_name(cls, peer_cert):
        '''Extract subjectAltName DNS name settings from certificate extensions
        
        @param peer_cert: peer certificate in SSL connection.  subjectAltName
        settings if any will be extracted from this
        @type peer_cert: OpenSSL.crypto.X509
        '''
        # Search through extensions
        dns_name = []
        general_names = SubjectAltName()
        for i in range(peer_cert.get_extension_count()):
            ext = peer_cert.get_extension(i)
            ext_name = ext.get_short_name()
            if ext_name == cls.SUBJ_ALT_NAME_EXT_NAME:
                # PyOpenSSL returns extension data in ASN.1 encoded form
                ext_dat = ext.get_data()
                decoded_dat = der_decoder.decode(ext_dat, 
                                                 asn1Spec=general_names)
                
                for name in decoded_dat:
                    if isinstance(name, SubjectAltName):
                        for entry in range(len(name)):
                            component = name.getComponentByPosition(entry)
                            dns_name.append(str(component.getComponent()))
                            
        return dns_name
        
    def _getCertDN(self):
        return self.__certDN

    def _setCertDN(self, val):
        if isinstance(val, str):
            # Allow for quoted DN
            certDN = val.strip('"')

            dnFields = self.__class__.PARSER_RE.split(certDN)
            if len(dnFields) < 2:
                raise TypeError('Error parsing DN string: "%s"' % certDN)

            self.__certDN = list(zip(dnFields[1::2], dnFields[2::2]))
            self.__certDN.sort()

        elif not isinstance(val, list):
            for i in val:
                if not len(i) == 2:
                    raise TypeError('Expecting list of two element DN field, '
                                    'DN field value pairs for "certDN" '
                                    'attribute')
            self.__certDN = val
        else:
            raise TypeError('Expecting list or string type for "certDN" '
                            'attribute')

    certDN = property(fget=_getCertDN,
                      fset=_setCertDN,
                      doc="Distinguished Name for Server Certificate")

    # Get/Set Property methods
    def _getHostname(self):
        return self.__hostname

    def _setHostname(self, val):
        if not isinstance(val, str):
            raise TypeError("Expecting string type for hostname "
                                 "attribute")
        self.__hostname = val

    hostname = property(fget=_getHostname,
                        fset=_setHostname,
                        doc="hostname of server")
