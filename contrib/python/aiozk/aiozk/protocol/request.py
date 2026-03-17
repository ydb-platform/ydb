import logging
import struct
from io import BytesIO

from .part import Part
from .primitives import Int


log = logging.getLogger(__name__)


class Request(Part):
    """
    Returns a bytesring representation of the request instance.

    # TODO(wglass): specify how xid and type preamble goes in

    Since this is a ``Part`` subclass the rest is a matter of
    appending the result of a ``render()`` call.
    """

    opcode = None
    special_xid = None
    writes_data = False

    def serialize(self, xid=None):
        buff = BytesIO()

        formats = []
        data = []
        if xid is not None:
            formats.append(Int.fmt)
            data.append(xid)
        if self.opcode:
            formats.append(Int.fmt)
            data.append(self.opcode)

        payload_format, payload_data = self.render()
        formats.append(payload_format)
        data.extend(payload_data)

        buff.write(struct.pack('!' + ''.join(formats), *data))

        return buff.getvalue()
