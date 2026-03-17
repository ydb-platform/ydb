# $Id: tftp.py 23 2006-11-08 15:45:33Z dugsong $
# -*- coding: utf-8 -*-
"""Trivial File Transfer Protocol."""
from __future__ import print_function
from __future__ import absolute_import

import struct

from . import dpkt

# Opcodes
OP_RRQ = 1  # read request
OP_WRQ = 2  # write request
OP_DATA = 3  # data packet
OP_ACK = 4  # acknowledgment
OP_ERR = 5  # error code

# Error codes
EUNDEF = 0  # not defined
ENOTFOUND = 1  # file not found
EACCESS = 2  # access violation
ENOSPACE = 3  # disk full or allocation exceeded
EBADOP = 4  # illegal TFTP operation
EBADID = 5  # unknown transfer ID
EEXISTS = 6  # file already exists
ENOUSER = 7  # no such user


class TFTP(dpkt.Packet):
    """Trivial File Transfer Protocol.

    Trivial File Transfer Protocol (TFTP) is a simple lockstep File Transfer Protocol which allows a client to get
    a file from or put a file onto a remote host. One of its primary uses is in the early stages of nodes booting
    from a local area network. TFTP has been used for this application because it is very simple to implement.

    Attributes:
        __hdr__: Header fields of TFTP.
            opcode: Operation Code (2 bytes)
    """

    __hdr__ = (('opcode', 'H', 1), )

    def unpack(self, buf):
        dpkt.Packet.unpack(self, buf)
        if self.opcode in (OP_RRQ, OP_WRQ):
            l_ = self.data.split(b'\x00')
            self.filename = l_[0]
            self.mode = l_[1]
            self.data = b''
        elif self.opcode in (OP_DATA, OP_ACK):
            self.block = struct.unpack('>H', self.data[:2])[0]
            self.data = self.data[2:]
        elif self.opcode == OP_ERR:
            self.errcode = struct.unpack('>H', self.data[:2])[0]
            self.errmsg = self.data[2:].split(b'\x00')[0]
            self.data = b''

    def __len__(self):
        return len(bytes(self))

    def __bytes__(self):
        if self.opcode in (OP_RRQ, OP_WRQ):
            s = self.filename + b'\x00' + self.mode + b'\x00'
        elif self.opcode in (OP_DATA, OP_ACK):
            s = struct.pack('>H', self.block)
        elif self.opcode == OP_ERR:
            s = struct.pack('>H', self.errcode) + (b'%s\x00' % self.errmsg)
        else:
            s = b''
        return self.pack_hdr() + s + self.data


def test_op_rrq():
    from binascii import unhexlify
    buf = unhexlify(
        '0001'    # opcode (OP_RRQ)
        '726663313335302e747874'  # filename (rfc1350.txt)
        '00'                      # null terminator
        '6f63746574'              # mode (octet)
        '00'                      # null terminator
    )
    tftp = TFTP(buf)
    assert tftp.filename == b'rfc1350.txt'
    assert tftp.mode == b'octet'
    assert bytes(tftp) == buf
    assert len(tftp) == len(buf)


def test_op_data():
    from binascii import unhexlify
    buf = unhexlify(
        '0003'    # opcode (OP_DATA)
        '0001'    # block
        '0a0a4e6574776f726b20576f726b696e672047726f7570'
    )
    tftp = TFTP(buf)
    assert tftp.block == 1
    assert tftp.data == b'\x0a\x0aNetwork Working Group'
    assert bytes(tftp) == buf
    assert len(tftp) == len(buf)


def test_op_err():
    from binascii import unhexlify
    buf = unhexlify(
        '0005'   # opcode (OP_ERR)
        '0007'   # errcode (ENOUSER)
        '0a0a4e6574776f726b20576f726b696e672047726f757000'
    )
    tftp = TFTP(buf)
    assert tftp.errcode == ENOUSER
    assert tftp.errmsg == b'\x0a\x0aNetwork Working Group'
    assert tftp.data == b''
    assert bytes(tftp) == buf


def test_op_other():
    from binascii import unhexlify
    buf = unhexlify(
        '0006'     # opcode (doesn't exist)
        'abcdef'   # trailing data
    )
    tftp = TFTP(buf)
    assert tftp.opcode == 6
    assert bytes(tftp) == buf
    assert tftp.data == unhexlify('abcdef')
