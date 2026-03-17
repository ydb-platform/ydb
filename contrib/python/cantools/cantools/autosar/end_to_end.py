# Utilities for calculating the CRC of the AUTOSAR end-to-end
# protection specification


import crccheck  # type: ignore

from ..database.can.message import Message


def compute_profile2_crc(payload: bytes,
                         msg_or_data_id: int | Message) -> int | None:
    """Compute the CRC checksum for profile 2 of the AUTOSAR end-to-end
    protection specification.

    data_id is the data ID to be used. If it is unspecified, it is
    determined from the message's ``autosar.e2e.data_ids`` attribute.
    """

    if len(payload) < 2:
        # Profile 2 E2E protection requires at least 2 bytes
        return None

    protected_len = None
    data_id = None

    if isinstance(msg_or_data_id, Message):
        msg = msg_or_data_id
        if msg.autosar is None or \
           msg.autosar.e2e is None or \
           msg.autosar.e2e.data_ids is None or \
           len(msg.autosar.e2e.data_ids) != 16:
            # message is not end-to-end protected using profile 2
            return None

        assert msg.autosar is not None
        assert msg.autosar.e2e is not None
        assert msg.autosar.e2e.data_ids is not None

        protected_len = msg.autosar.e2e.payload_length
        seq_counter = payload[1] & 0xf
        data_id = msg.autosar.e2e.data_ids[seq_counter]
    else:
        protected_len = len(payload)
        data_id = msg_or_data_id

    # create the data to be checksummed
    crc_data = bytearray(payload[1:protected_len])

    # append data id
    crc_data += bytearray([ data_id ])

    # do the actual work
    return int(crccheck.crc.Crc8Autosar().calc(crc_data))

def apply_profile2_crc(payload: bytes,
                       msg_or_data_id: int | Message) \
  -> bytearray | None:
    """Compute the CRC checksum for profile 2 of the AUTOSAR end-to-end
    protection specification and apply it to an encoded payload.

    If the message is passed, this function also takes care of special
    cases like the message not being end-to-end protected or being a
    secured frame.
    """

    crc = compute_profile2_crc(payload, msg_or_data_id)

    if crc is None:
        return None

    result = bytearray(payload)
    result[0] = crc
    return result


def check_profile2_crc(payload: bytes,
                       msg_or_data_id: int | Message) -> bool | None:
    """Check if the AUTOSAR E2E checksum for profile 2 of the AUTOSAR
    end-to-end protection specification is correct.

    If a message is not end-to-end protected by profile 2, ``False`` is
    returned.
    """

    crc = compute_profile2_crc(payload, msg_or_data_id)

    if crc is None:
        return None

    crc2 = payload[0]

    return crc == crc2

def compute_profile5_crc(payload: bytes,
                         msg_or_data_id: int | Message) -> int | None:
    """Compute the CRC checksum for profile 5 of the AUTOSAR end-to-end
    protection specification.

    data_id is the data ID to be used. If it is unspecified, it is
    determined from the message's ``autosar.e2e.data_ids`` attribute.
    """

    if len(payload) < 4:
        # Profile 5 E2E protection requires at least 4 bytes
        return None

    protected_len = None
    data_id = None

    if isinstance(msg_or_data_id, Message):
        msg = msg_or_data_id
        if msg_or_data_id.autosar is None or \
           msg_or_data_id.autosar.e2e is None or \
           msg_or_data_id.autosar.e2e.data_ids is None or \
           len(msg_or_data_id.autosar.e2e.data_ids) != 1:
            # message is not end-to-end protected using profile 5
            return None

        assert msg.autosar is not None
        assert msg.autosar.e2e is not None
        assert msg.autosar.e2e.data_ids is not None

        protected_len = msg.autosar.e2e.payload_length
        data_id = msg.autosar.e2e.data_ids[0]
    else:
        protected_len = len(payload)
        data_id = msg_or_data_id

    # we assume that the "offset" parameter given in the specification
    # is always 0...
    result = crccheck.crc.Crc16Autosar().calc(payload[2:protected_len],
                                              initvalue=0xffff)

    # deal with the data id
    result = crccheck.crc.Crc16Autosar().calc(bytearray([data_id&0xff]),
                                              initvalue=result)
    result = crccheck.crc.Crc16Autosar().calc(bytearray([(data_id>>8)&0xff]),
                                              initvalue=result)

    return int(result)

def apply_profile5_crc(payload: bytes,
                       msg_or_data_id: int | Message) \
  -> bytearray | None:
    """Compute the AUTOSAR E2E checksum for profile 5 of the AUTOSAR
    end-to-end protection specification and apply it to an encoded
    payload.

    If the message is passed, this function also takes care of special
    cases like the message not being end-to-end protected or being a
    secured frame.

    """

    crc = compute_profile5_crc(payload, msg_or_data_id)

    if crc is None:
        return None

    result = bytearray(payload)
    result[0] = crc&0xff
    result[1] = (crc>>8)&0xff

    return result

def check_profile5_crc(payload: bytes,
                       msg_or_data_id: int | Message) -> bool | None:
    """Check if the AUTOSAR E2E checksum for profile 5 of the AUTOSAR
    end-to-end protection specification is correct.

    If a message is not end-to-end protected by profile 5, ``False`` is
    returned.
    """

    crc = compute_profile5_crc(payload, msg_or_data_id)

    if crc is None:
        return None

    crc2 = payload[0] + (payload[1]<<8)

    return crc == crc2
