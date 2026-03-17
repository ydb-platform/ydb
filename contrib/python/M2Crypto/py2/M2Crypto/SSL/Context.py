from __future__ import absolute_import

"""SSL Context

Copyright (c) 1999-2004 Ng Pheng Siong. All rights reserved."""

from M2Crypto import BIO, Err, RSA, X509, m2, util  # noqa
from M2Crypto.SSL import cb  # noqa
from M2Crypto.SSL.Session import Session  # noqa
from weakref import WeakValueDictionary
from typing import Any, AnyStr, Callable, Optional, Union  # noqa

__all__ = ['ctxmap', 'Context', 'map']


class _ctxmap(object):
    singleton = None  # type: Optional[_ctxmap]

    def __init__(self):
        # type: () -> None
        """Simple WeakReffed list.
        """
        self._ctxmap = WeakValueDictionary()

    def __getitem__(self, key):
        # type: (int) -> Any
        return self._ctxmap[key]

    def __setitem__(self, key, value):
        # type: (int, Any) -> None
        self._ctxmap[key] = value

    def __delitem__(self, key):
        # type: (int) -> None
        del self._ctxmap[key]


def ctxmap():
    # type: () -> _ctxmap
    if _ctxmap.singleton is None:
        _ctxmap.singleton = _ctxmap()
    return _ctxmap.singleton
# deprecated!!!
map = ctxmap


class Context(object):

    """'Context' for SSL connections."""

    m2_ssl_ctx_free = m2.ssl_ctx_free

    def __init__(self, protocol='tls', weak_crypto=None,
                 post_connection_check=None):
        # type: (str, Optional[int], Optional[Callable]) -> None
        proto = getattr(m2, protocol + '_method', None)
        if proto is None:
            # default is 'sslv23' for older versions of OpenSSL
            if protocol == 'tls':
                proto = getattr(m2, 'sslv23_method')
            else:
                raise ValueError("no such protocol '%s'" % protocol)
        self.ctx = m2.ssl_ctx_new(proto())
        self.allow_unknown_ca = 0  # type: Union[int, bool]
        self.post_connection_check = post_connection_check
        ctxmap()[int(self.ctx)] = self
        m2.ssl_ctx_set_cache_size(self.ctx, 128)
        if weak_crypto is None and protocol in ('sslv23', 'tls'):
            self.set_options(m2.SSL_OP_ALL | m2.SSL_OP_NO_SSLv2 |
                             m2.SSL_OP_NO_SSLv3)

    def __del__(self):
        # type: () -> None
        if getattr(self, 'ctx', None):
            self.m2_ssl_ctx_free(self.ctx)

    def close(self):
        # type: () -> None
        del ctxmap()[int(self.ctx)]

    def load_cert(self, certfile, keyfile=None,
                  callback=util.passphrase_callback):
        # type: (AnyStr, Optional[AnyStr], Callable) -> None
        """Load certificate and private key into the context.

        :param certfile: File that contains the PEM-encoded certificate.
        :param keyfile:  File that contains the PEM-encoded private key.
                         Default value of None indicates that the private key
                         is to be found in 'certfile'.
        :param callback: Callable object to be invoked if the private key is
                         passphrase-protected. Default callback provides a
                         simple terminal-style input for the passphrase.
        """
        m2.ssl_ctx_passphrase_callback(self.ctx, callback)
        m2.ssl_ctx_use_cert(self.ctx, certfile)
        if not keyfile:
            keyfile = certfile
        m2.ssl_ctx_use_privkey(self.ctx, keyfile)
        if not m2.ssl_ctx_check_privkey(self.ctx):
            raise ValueError('public/private key mismatch')

    def load_cert_chain(self, certchainfile, keyfile=None,
                        callback=util.passphrase_callback):
        # type: (AnyStr, Optional[AnyStr], Callable) -> None
        """Load certificate chain and private key into the context.

        :param certchainfile: File object containing the PEM-encoded
                              certificate chain.
        :param keyfile:       File object containing the PEM-encoded private
                              key. Default value of None indicates that the
                              private key is to be found in 'certchainfile'.
        :param callback:      Callable object to be invoked if the private key
                              is passphrase-protected. Default callback
                              provides a simple terminal-style input for the
                              passphrase.
        """
        m2.ssl_ctx_passphrase_callback(self.ctx, callback)
        m2.ssl_ctx_use_cert_chain(self.ctx, certchainfile)
        if not keyfile:
            keyfile = certchainfile
        m2.ssl_ctx_use_privkey(self.ctx, keyfile)
        if not m2.ssl_ctx_check_privkey(self.ctx):
            raise ValueError('public/private key mismatch')

    def set_client_CA_list_from_file(self, cafile):
        # type: (AnyStr) -> None
        """Load CA certs into the context. These CA certs are sent to the
        peer during *SSLv3 certificate request*.

        :param cafile: File object containing one or more PEM-encoded CA
                       certificates concatenated together.
        """
        m2.ssl_ctx_set_client_CA_list_from_file(self.ctx, cafile)

    # Deprecated.
    load_client_CA = load_client_ca = set_client_CA_list_from_file

    def load_verify_locations(self, cafile=None, capath=None):
        # type: (Optional[AnyStr], Optional[AnyStr]) -> int
        """Load CA certs into the context.

        These CA certs are used during verification of the peer's
        certificate.

        :param cafile: File containing one or more PEM-encoded CA
                       certificates concatenated together.

        :param capath: Directory containing PEM-encoded CA certificates
                       (one certificate per file).

        :return: 0 if the operation failed because CAfile and CApath are NULL
                  or the processing at one of the locations specified failed.
                  Check the error stack to find out the reason.

                1 The operation succeeded.
        """
        if cafile is None and capath is None:
            raise ValueError("cafile and capath can not both be None.")
        return m2.ssl_ctx_load_verify_locations(self.ctx, cafile, capath)

    # Deprecated.
    load_verify_info = load_verify_locations

    def set_session_id_ctx(self, id):
        # type: (bytes) -> None
        """Sets the session id for the SSL.Context w/in a session can be reused.

        :param id: Sessions are generated within a certain context. When
                   exporting/importing sessions with
                   i2d_SSL_SESSION/d2i_SSL_SESSION it would be possible,
                   to re-import a session generated from another context
                   (e.g. another application), which might lead to
                   malfunctions. Therefore each application must set its
                   own session id context sid_ctx which is used to
                   distinguish the contexts and is stored in exported
                   sessions. The sid_ctx can be any kind of binary data
                   with a given length, it is therefore possible to use
                   e.g. the name of the application and/or the hostname
                   and/or service name.
        """
        ret = m2.ssl_ctx_set_session_id_context(self.ctx, id)
        if not ret:
            raise Err.SSLError(Err.get_error_code(), '')

    def set_default_verify_paths(self):
        # type: () -> int
        """
        Specifies that the default locations from which CA certs are
        loaded should be used.

        There is one default directory and one default file. The default
        CA certificates directory is called "certs" in the default
        OpenSSL directory. Alternatively the SSL_CERT_DIR environment
        variable can be defined to override this location. The default
        CA certificates file is called "cert.pem" in the default OpenSSL
        directory. Alternatively the SSL_CERT_FILE environment variable
        can be defined to override this location.

        @return 0 if the operation failed. A missing default location is
                  still treated as a success. No error code is set.

                1 The operation succeeded.
        """
        ret = m2.ssl_ctx_set_default_verify_paths(self.ctx)
        if not ret:
            raise ValueError('Cannot use default SSL certificate store!')

    def set_allow_unknown_ca(self, ok):
        # type: (Union[int, bool]) -> None
        """Set the context to accept/reject a peer certificate if the
        certificate's CA is unknown.

        :param ok:       True to accept, False to reject.
        """
        self.allow_unknown_ca = ok

    def get_allow_unknown_ca(self):
        # type: () -> Union[int, bool]
        """Get the context's setting that accepts/rejects a peer
        certificate if the certificate's CA is unknown.

        FIXME 2Bconverted to bool
        """
        return self.allow_unknown_ca

    def set_verify(self, mode, depth, callback=None):
        # type: (int, int, Optional[Callable]) -> None
        """
        Set verify options. Most applications will need to call this
        method with the right options to make a secure SSL connection.

        :param mode:     The verification mode to use. Typically at least
                         SSL.verify_peer is used. Clients would also typically
                         add SSL.verify_fail_if_no_peer_cert.
        :param depth:    The maximum allowed depth of the certificate chain
                         returned by the peer.
        :param callback: Callable that can be used to specify custom
                         verification checks.
        """
        if callback is None:
            m2.ssl_ctx_set_verify_default(self.ctx, mode)
        else:
            m2.ssl_ctx_set_verify(self.ctx, mode, callback)
        m2.ssl_ctx_set_verify_depth(self.ctx, depth)

    def get_verify_mode(self):
        # type: () -> int
        return m2.ssl_ctx_get_verify_mode(self.ctx)

    def get_verify_depth(self):
        # type: () -> int
        """Returns the verification mode currently set in the SSL Context."""
        return m2.ssl_ctx_get_verify_depth(self.ctx)

    def set_tmp_dh(self, dhpfile):
        # type: (AnyStr) -> int
        """Load ephemeral DH parameters into the context.

        :param dhpfile: Filename of the file containing the PEM-encoded
                        DH parameters.
        """
        f = BIO.openfile(dhpfile)
        dhp = m2.dh_read_parameters(f.bio_ptr())
        return m2.ssl_ctx_set_tmp_dh(self.ctx, dhp)

    def set_tmp_dh_callback(self, callback=None):
        # type: (Optional[Callable]) -> None
        """Sets the callback function for SSL.Context.

        :param callback: Callable to be used when a DH parameters are required.
        """
        if callback is not None:
            m2.ssl_ctx_set_tmp_dh_callback(self.ctx, callback)

    def set_tmp_rsa(self, rsa):
        # type: (RSA.RSA) -> int
        """Load ephemeral RSA key into the context.

        :param rsa: RSA.RSA instance.
        """
        if isinstance(rsa, RSA.RSA):
            return m2.ssl_ctx_set_tmp_rsa(self.ctx, rsa.rsa)
        else:
            raise TypeError("Expected an instance of RSA.RSA, got %s." % rsa)

    def set_tmp_rsa_callback(self, callback=None):
        # type: (Optional[Callable]) -> None
        """Sets the callback function to be used when
        a temporary/ephemeral RSA key is required.
        """
        if callback is not None:
            m2.ssl_ctx_set_tmp_rsa_callback(self.ctx, callback)

    def set_info_callback(self, callback=cb.ssl_info_callback):
        # type: (Callable) -> None
        """Set a callback function to get state information.

        It can be used to get state information about the SSL
        connections that are created from this context.

        :param callback: Callback function. The default prints
                         information to stderr.
        """
        m2.ssl_ctx_set_info_callback(self.ctx, callback)

    def set_cipher_list(self, cipher_list):
        # type: (str) -> int
        """Sets the list of available ciphers.

        :param cipher_list: The format of the string is described in
                            ciphers(1).
        :return: 1 if any cipher could be selected and 0 on complete
                 failure.
        """
        return m2.ssl_ctx_set_cipher_list(self.ctx, cipher_list)

    def add_session(self, session):
        # type: (Session) -> int
        """Add the session to the context.

        :param session: the session to be added.

        :return: 0 The operation failed. It was tried to add the same
                   (identical) session twice.

                 1 The operation succeeded.
        """
        return m2.ssl_ctx_add_session(self.ctx, session._ptr())

    def remove_session(self, session):
        # type: (Session) -> int
        """Remove the session from the context.

        :param session: the session to be removed.

        :return: 0 The operation failed. The session was not found in
                   the cache.

                 1 The operation succeeded.
        """
        return m2.ssl_ctx_remove_session(self.ctx, session._ptr())

    def get_session_timeout(self):
        # type: () -> int
        """Get current session timeout.

        Whenever a new session is created, it is assigned a maximum
        lifetime.  This lifetime is specified by storing the creation
        time of the session and the timeout value valid at this time. If
        the actual time is later than creation time plus timeout, the
        session is not reused.

        Due to this realization, all sessions behave according to the
        timeout value valid at the time of the session negotiation.
        Changes of the timeout value do not affect already established
        sessions.

        Expired sessions are removed from the internal session cache,
        whenever SSL_CTX_flush_sessions(3) is called, either directly by
        the application or automatically (see
        SSL_CTX_set_session_cache_mode(3))

        The default value for session timeout is decided on a per
        protocol basis, see SSL_get_default_timeout(3).  All currently
        supported protocols have the same default timeout value of 300
        seconds.

        SSL_CTX_set_timeout() returns the previously set timeout value.

        :return: the currently set timeout value.
        """
        return m2.ssl_ctx_get_session_timeout(self.ctx)

    def set_session_timeout(self, timeout):
        # type: (int) -> int
        """Set new session timeout.

        See self.get_session_timeout() for explanation of the session
        timeouts.

        :param timeout: new timeout value.

        :return: the previously set timeout value.
        """
        return m2.ssl_ctx_set_session_timeout(self.ctx, timeout)

    def set_session_cache_mode(self, mode):
        # type: (int) -> int
        """Enables/disables session caching.

        The mode is set by using m2.SSL_SESS_CACHE_* constants.

        :param mode: new mode value.

        :return: the previously set cache mode value.
        """
        return m2.ssl_ctx_set_session_cache_mode(self.ctx, mode)

    def get_session_cache_mode(self):
        # type: () -> int
        """Gets the current session caching.

        The mode is set to m2.SSL_SESS_CACHE_* constants.

        :return: the previously set cache mode value.
        """
        return m2.ssl_ctx_get_session_cache_mode(self.ctx)

    def set_options(self, op):
        # type: (int) -> int
        """Adds the options set via bitmask in options to the Context.

        !!! Options already set before are not cleared!

        The behaviour of the SSL library can be changed by setting
        several options.  The options are coded as bitmasks and can be
        combined by a logical or operation (|).

        SSL.Context.set_options() and SSL.set_options() affect the
        (external) protocol behaviour of the SSL library. The (internal)
        behaviour of the API can be changed by using the similar
        SSL.Context.set_mode() and SSL.set_mode() functions.

        During a handshake, the option settings of the SSL object are
        used. When a new SSL object is created from a context using
        SSL(), the current option setting is copied. Changes to ctx
        do not affect already created SSL objects. SSL.clear() does not
        affect the settings.

        :param op: bitmask of additional options specified in
                   SSL_CTX_set_options(3) manpage.

        :return: the new options bitmask after adding options.
        """
        return m2.ssl_ctx_set_options(self.ctx, op)

    def get_cert_store(self):
        # type: () -> X509.X509
        """
        Get the certificate store associated with this context.

        :warning: The store is NOT refcounted, and as such can not be relied
                  to be valid once the context goes away or is changed.
        """
        return X509.X509_Store(m2.ssl_ctx_get_cert_store(self.ctx))
