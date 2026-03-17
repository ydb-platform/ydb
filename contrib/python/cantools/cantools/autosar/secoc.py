# Utilities for dealing with AUTOSAR secure on-board communication.
# (SecOC, i.e., verification of the authenticity of the sender of
# messages.)

import bitstruct  # type: ignore

from ..database.can.message import Message
from ..errors import Error
from ..typechecking import (
    SecOCAuthenticatorFn,
)


class SecOCError(Error):
    """Exception that is raised if something SecOC related goes wrong.

    """


def compute_authenticator(raw_payload: bytes,
                          dbmsg: Message,
                          authenticator_fn: SecOCAuthenticatorFn,
                          freshness_value: int) \
                          -> bytes:
    """Given a byte-like object that contains the encoded signals to be
    send, compute the full authenticator SecOC value.
    """

    if dbmsg.autosar is None or dbmsg.autosar.secoc is None:
        raise SecOCError(f'Message "{dbmsg.name}" is not secured')

    secoc_props = dbmsg.autosar.secoc
    n_fresh = secoc_props.freshness_bit_length
    payload_len = secoc_props.payload_length

    # build the data that needs to be passed to authentificator function
    auth_data = bitstruct.pack(f'u16' # data ID
                               f'r{payload_len*8}' # payload to be secured
                               f'u{n_fresh}', # freshness value
                               secoc_props.data_id,
                               raw_payload[:payload_len],
                               freshness_value)

    # compute authenticator value
    return authenticator_fn(dbmsg, auth_data, freshness_value)

def apply_authenticator(raw_payload: bytes,
                        dbmsg: Message,
                        authenticator_fn: SecOCAuthenticatorFn,
                        freshness_value: int) \
                        -> bytearray:
    """Given a byte-like object that contains the encoded signals to be
    send, compute the full message which ought to be send.

    This is basically the concatenation of the raw payload, the
    truncated freshness value and the truncated authenticator for the
    message.
    """

    if dbmsg.autosar is None:
        raise RuntimeError(f'Message "{dbmsg.name}" does not have '
                           f'AUTOSAR specific properties.')
    elif dbmsg.autosar.secoc is None:
        raise RuntimeError(f'Message "{dbmsg.name}" does not have any'
                           f'SecOC properties (message is not secured).')

    result = bytearray(raw_payload)

    # compute authenticator value
    auth_value = compute_authenticator(raw_payload,
                                       dbmsg,
                                       authenticator_fn,
                                       freshness_value)

    # get the last N bits of the freshness value.
    secoc_props = dbmsg.autosar.secoc
    n_fresh_tx = secoc_props.freshness_tx_bit_length
    mask = (1 << n_fresh_tx) - 1
    truncated_freshness_value = freshness_value&mask
    payload_len = secoc_props.payload_length

    bitstruct.pack_into(f'u{n_fresh_tx}r{secoc_props.auth_tx_bit_length}',
                        result,
                        payload_len*8,
                        truncated_freshness_value,
                        auth_value)

    return result

def verify_authenticator(raw_payload: bytes,
                         dbmsg: Message,
                         authenticator_fn: SecOCAuthenticatorFn,
                         freshness_value: int) \
                    -> bool:
    """Verify that a message that is secured via SecOC is valid."""

    tmp_payload = apply_authenticator(raw_payload,
                                      dbmsg,
                                      authenticator_fn,
                                      freshness_value)

    return raw_payload == tmp_payload
