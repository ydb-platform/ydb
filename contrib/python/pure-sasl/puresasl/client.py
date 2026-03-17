from functools import wraps
from warnings import warn

import puresasl.mechanisms as mech_mod
from puresasl import SASLError, QOP, SASLWarning


def _require_mech(f):
    """
    A utility decorator that ensures a mechanism has been chosen.
    """
    @wraps(f)
    def wrapped(self, *args, **kwargs):
        if not self._chosen_mech:
            raise SASLError("A mechanism has not been chosen yet")
        return f(self, *args, **kwargs)

    return wrapped


class SASLClient(object):
    """
    A SASL client for one negotiation with a SASL server.

    An new instance of this is typically created when establishing a connection
    with a SASL server. The SASL mechanism may be chosen at creation time or
    set later after a list of server-supported mechanisms has been discovered
    using `:method:choose_mechanism()`.

    Instances of this class do not directly communicate with the server (as
    the communication protocol differs from service to service), but rather
    relies on challenges from the server to be passed in and processed.  When
    initiating a new connection, this is done by passing challenges from the
    server to the :meth:`process()` method until the server indicates that
    SASL negotiation has completed and the `complete` attribute of the instance
    is set to ``True``.

    After the initial negotiation is complete, communication between the
    server and client may need to be processed, depending on the 'quality of
    service', which may call for signatures or encryption. To handle this,
    messages from the server should be passed through :meth:`unwrap()`, and
    messages outbound from the client to server should be passed through
    :meth:`wrap()`.

    Example usage::

        >>> from puresasl.client import SASLClient
        >>>
        >>> sasl = SASLClient('somehost2', 'customprotocol')
        >>> conn = get_connection_to('somehost2')
        >>> available_mechs = conn.get_mechanisms()
        >>> sasl.choose_mechanism(available_mechs, allow_anonymous=False)
        >>> while True:
        ...     status, challenge = conn.get_challenge()
        ...     if status == 'COMPLETE':
        ...         break
        ...     elif status == 'OK':
        ...         response = sasl.process(challenge)
        ...         conn.send_response(response)
        ...     else:
        ...         raise Exception(status)
        ...
        >>> if not sasl.complete:
        ...     raise Exception("SASL negotiation did not complete")
        >>>
        >>> # begin normal communication
        >>> encoded = conn.fetch_data()
        >>> decoded = sasl.unwrap(encoded)
        >>> response = process_data(decoded)
        >>> conn.send_data(sasl.wrap(response))
    """

    def __init__(self, host, service=None, mechanism=None, authorization_id=None,
                 callback=None, qops=QOP.all, mutual_auth=False, max_buffer=65536,
                 **mechanism_props):
        """
        `host` is the name of the SASL server, typically an FQDN, and `service` is
        usually the name of the protocol, such as `imap` or `http`.

        `mechanism` may be the string name of a mechanism to use, like
        'PLAIN' or 'GSSAPI'.  If left as ``None``, :meth:`choose_mechanism`
        must be used with a list of mechanisms that the server supports before
        `process()` can be used.

        Optionally, an `authorization_id` may be set if the mechanism and protocol
        support authorization.

        The allowed quality of protection (QoP) choices may be set with the `qops`
        parameter, which should be an iterable of allowed options. Valid options
        include 'auth' for no protection, 'auth-int' for integrity protection,
        and 'auth-conf' for confidentiality protection. The strongest of these
        that the server also supports will be chosen automatically.  If the
        server does not support any of these choices, a
        :exc:`SASLProtocolException` will be raised.

        If the chosen mechanism supports mutual authentication, which is
        authentication of the server by the client, this may be set to
        ``True`` to ensure that mutual authentication is performed.

        A max buffer size may be set with `max_buffer`.  If a max buffer size
        is also set during negotiation by the server, the min of these two
        values will be used.

        Any other mechanism-specific properties may be set with
        `**mechanism_props` and will automatically be passed in to the
        mechanism's constructor.  If any properties are required by the
        mechanism during the course of negotiation have not been passed in
        via `**mechanism_props`, the function passed in here as the `callback`
        argument will be called with one argument, the name of the required
        property.  The `callback` function should return a value for that
        property.
        """
        self.host = host
        self.service = service
        self.authorization_id = authorization_id
        self.mechanism = mechanism
        self.callback = callback
        self.qops = set(qops)
        self.mutual_auth = mutual_auth
        self.max_buffer = max_buffer

        self._mech_props = mechanism_props
        if self.mechanism is not None:
            try:
                mech_class = mech_mod.mechanisms[mechanism]
            except KeyError:
                gssapi = mech_mod.GSSAPIMechanism.name
                if mechanism == gssapi and not mech_mod.have_kerberos:
                    raise SASLError('kerberos module not installed, {0} '
                                    'unavailable'.format(gssapi))
                else:
                    raise SASLError('Unknown mechanism {0}'.format(mechanism))
            self._chosen_mech = mech_class(self, **self._mech_props)
        else:
            self._chosen_mech = None

    @_require_mech
    def process(self, challenge=None):
        """
        Process a challenge from the server during SASL negotiation.
        A response will be returned which should typically be sent to the
        server to answer the challenge.

        With some mechanisms and protocols, `process()` should be called
        with a `challenge` of ``None`` to generate the first message
        to be sent to the server.
        """
        return self._chosen_mech.process(challenge)

    @_require_mech
    def wrap(self, outgoing):
        """
        Wrap an outgoing message intended for the SASL server. Depending
        on the negotiated quality of protection, this may result in the
        message being signed, encrypted, or left unaltered.
        """
        return self._chosen_mech.wrap(outgoing)

    @_require_mech
    def unwrap(self, incoming):
        """
        Unwrap a message from the SASL server. Depending on the negotiated
        quality of protection, this may check a signature, decrypt the message,
        or leave the message unaltered.
        """
        return self._chosen_mech.unwrap(incoming)

    @property
    def complete(self):
        """
        Check to see if SASL negotiation has completed successfully, including
        a mutual authentication check if the chosen mechanism supports that and
        mutual authentication was requested via the `mutual_auth` parameter
        for the `SASLClient` constructor.
        """
        if not self._chosen_mech:
            raise SASLError("A mechanism has not been chosen yet")
        return self._chosen_mech.complete

    @_require_mech
    def dispose(self):
        """
        Clear all sensitive data, such as passwords.
        """
        self._chosen_mech.dispose()

    @property
    @_require_mech
    def qop(self):
        return self._chosen_mech.qop

    def choose_mechanism(self, mechanism_choices, allow_anonymous=True,
                         allow_plaintext=True, allow_active=True,
                         allow_dictionary=True):
        """
        Choose a mechanism from a list of mechanisms based on security
        scores for mechanisms and required properties of the mechanism.

        If `allow_anonymous` is ``False``, mechanisms that allow anonymous
        authentication will not be considered.

        If `allow_plaintext` is ``False``, mechanisms that transmit
        sensitive information in plaintext (and are thus susceptible to
        passive listening attacks) will not be considered.

        If `allow_active` is ``False``, mechanisms that are susceptible
        to active non-dictionary attacks (MITM, injection) will not be
        considered.

        If `allow_dictionary` is ``False, mechanisms that are susceptible
        to passive dictionary attacks will not be considered.
        """
        gssapi = mech_mod.GSSAPIMechanism.name
        if gssapi in mechanism_choices and not mech_mod.have_kerberos:
            warn('kerberos module not installed, {0} will be ignored'.format(
                 gssapi), SASLWarning)

        candidates = [mech_mod.mechanisms[choice]
                      for choice in mechanism_choices
                      if choice in mech_mod.mechanisms]

        if not allow_anonymous:
            candidates = [m for m in candidates if not m.allows_anonymous]
        if not allow_plaintext:
            candidates = [m for m in candidates if not m.uses_plaintext]
        if not allow_active:
            candidates = [m for m in candidates if m.active_safe]
        if not allow_dictionary:
            candidates = [m for m in candidates if m.dictionary_safe]

        if not candidates:
            raise SASLError("None of the mechanisms listed meet all "
                            "required properties")

        # Pick the best mechanism based on its security score
        mech_class = max(candidates, key=lambda mech: mech.score)
        self.mechanism = mech_class.name
        self._chosen_mech = mech_class(self, **self._mech_props)
