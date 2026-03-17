"""ndg_httpsclient SSL Context utilities module containing convenience routines
for setting SSL context configuration.

"""
__author__ = "P J Kershaw (STFC)"
__date__ = "09/12/11"
__copyright__ = "(C) 2012 Science and Technology Facilities Council"
__license__ = "BSD - see LICENSE file in top-level directory"
__contact__ = "Philip.Kershaw@stfc.ac.uk"
__revision__ = '$Id$'
import sys

if sys.version_info[0] > 2:
    import urllib.parse as urlparse_
else:
    import urlparse as urlparse_

from OpenSSL import SSL

from ndg.httpsclient.ssl_peer_verification import ServerSSLCertVerification


class SSlContextConfig(object):
    """
    Holds configuration options for creating a SSL context. This is used as a
    template to create the contexts with specific verification callbacks.
    """
    def __init__(self, key_file=None, cert_file=None, pem_file=None, ca_dir=None,
                 verify_peer=False):
        self.key_file = key_file
        self.cert_file = cert_file
        self.pem_file = pem_file
        self.ca_dir = ca_dir
        self.verify_peer = verify_peer


def make_ssl_context_from_config(ssl_config=False, url=None):
    return make_ssl_context(ssl_config.key_file, ssl_config.cert_file,
                            ssl_config.pem_file, ssl_config.ca_dir,
                            ssl_config.verify_peer, url)


def make_ssl_context(key_file=None, cert_file=None, pem_file=None, ca_dir=None,
                     verify_peer=False, url=None, method=SSL.TLSv1_2_METHOD,
                     key_file_passphrase=None):
    """
    Creates SSL context containing certificate and key file locations.
    """
    ssl_context = SSL.Context(method)
    
    # Key file defaults to certificate file if present.
    if cert_file:
        ssl_context.use_certificate_file(cert_file)
        
    if key_file_passphrase:
        passwd_cb = lambda max_passphrase_len, set_prompt, userdata: \
                           key_file_passphrase 
        ssl_context.set_passwd_cb(passwd_cb)
        
    if key_file:
        ssl_context.use_privatekey_file(key_file)
    elif cert_file:
        ssl_context.use_privatekey_file(cert_file)

    if pem_file or ca_dir:
        ssl_context.load_verify_locations(pem_file, ca_dir)
    else:
        ssl_context.set_default_verify_paths() # Use OS CA bundle

    def _callback(conn, x509, errnum, errdepth, preverify_ok):
        """Default certification verification callback.
        Performs no checks and returns the status passed in.
        """
        return preverify_ok
    
    verify_callback = _callback

    if verify_peer:
        ssl_context.set_verify_depth(9)
        if url:
            set_peer_verification_for_url_hostname(ssl_context, url)
        else:
            ssl_context.set_verify(SSL.VERIFY_PEER, verify_callback)
    else:
        ssl_context.set_verify(SSL.VERIFY_NONE, verify_callback)
        
    return ssl_context


def set_peer_verification_for_url_hostname(ssl_context, url, 
                                           if_verify_enabled=False):
    '''Convenience routine to set peer verification callback based on
    ServerSSLCertVerification class'''
    if not if_verify_enabled or (ssl_context.get_verify_mode() & SSL.VERIFY_PEER):
        urlObj = urlparse_.urlparse(url)
        hostname = urlObj.hostname
        server_ssl_cert_verif = ServerSSLCertVerification(hostname=hostname)
        verify_callback_ = server_ssl_cert_verif.get_verify_server_cert_func()
        ssl_context.set_verify(SSL.VERIFY_PEER, verify_callback_)

