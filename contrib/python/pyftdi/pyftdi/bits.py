# Copyright (c) 2010-2024 Emmanuel Blot <emmanuel.blot@free.fr>
# Copyright (c) 2008-2016, Neotion
# All rights reserved.
#
# SPDX-License-Identifier: BSD-3-Clause

"""Bit field and sequence management."""

from typing import Iterable, List, Optional, Tuple, Union
from .misc import is_iterable, xor

# pylint: disable=invalid-name
# pylint: disable=unneeded-not
# pylint: disable=duplicate-key


class BitSequenceError(Exception):
    """Bit sequence error"""


class BitSequence:
    """Bit sequence.

       Support most of the common bit operations: or, and, shift, comparison,
       and conversion from and to integral values.

       Bit sequence objects are iterable.

       Can be initialized with another bit sequence, a integral value,
       a sequence of bytes or an iterable of common boolean values.

       :param value:  initial value
       :param msb:    most significant bit first or not
       :param length: count of signficant bits in the bit sequence
       :param bytes_: initial value specified as a sequence of bytes
       :param msby:   most significant byte first or not
    """

    def __init__(self, value: Union['BitSequence', str, int] = None,
                 msb: bool = False, length: int = 0,
                 bytes_: Optional[bytes] = None, msby: bool = True):
        """Instantiate a new bit sequence.
        """
        self._seq = bytearray()
        seq = self._seq
        if value and bytes_:
            raise BitSequenceError("Cannot inialize with both a value and "
                                   "bytes")
        if bytes_:
            provider = list(bytes_).__iter__() if msby else reversed(bytes_)
            for byte in provider:
                if isinstance(byte, str):
                    byte = ord(byte)
                elif byte > 0xff:
                    raise BitSequenceError("Invalid byte value")
                b = []
                for _ in range(8):
                    b.append(bool(byte & 0x1))
                    byte >>= 1
                if msb:
                    b.reverse()
                seq.extend(b)
        else:
            value = self._tomutable(value)
        if isinstance(value, int):
            self._init_from_integer(value, msb, length)
        elif isinstance(value, BitSequence):
            self._init_from_sibling(value, msb)
        elif is_iterable(value):
            self._init_from_iterable(value, msb)
        elif value is None:
            pass
        else:
            raise BitSequenceError(f"Cannot initialize from '{type(value)}'")
        self._update_length(length, msb)

    def sequence(self) -> bytearray:
        """Return the internal representation as a new mutable sequence"""
        return bytearray(self._seq)

    def reverse(self) -> 'BitSequence':
        """In-place reverse"""
        self._seq.reverse()
        return self

    def invert(self) -> 'BitSequence':
        """In-place invert sequence values"""
        self._seq = bytearray([x ^ 1 for x in self._seq])
        return self

    def append(self, seq) -> 'BitSequence':
        """Concatenate a new BitSequence"""
        if not isinstance(seq, BitSequence):
            seq = BitSequence(seq)
        self._seq.extend(seq.sequence())
        return self

    def lsr(self, count: int) -> None:
        """Left shift rotate"""
        count %= len(self)
        self._seq[:] = self._seq[count:] + self._seq[:count]

    def rsr(self, count: int) -> None:
        """Right shift rotate"""
        count %= len(self)
        self._seq[:] = self._seq[-count:] + self._seq[:-count]

    def tobit(self) -> bool:
        """Degenerate the sequence into a single bit, if possible"""
        if len(self) != 1:
            raise BitSequenceError("BitSequence should be a scalar")
        return bool(self._seq[0])

    def tobyte(self, msb: bool = False) -> int:
        """Convert the sequence into a single byte value, if possible"""
        if len(self) > 8:
            raise BitSequenceError("Cannot fit into a single byte")
        byte = 0
        pos = -1 if not msb else 0
        # copy the sequence
        seq = self._seq[:]
        while seq:
            byte <<= 1
            byte |= seq.pop(pos)
        return byte

    def tobytes(self, msb: bool = False, msby: bool = False) -> bytearray:
        """Convert the sequence into a sequence of byte values"""
        blength = (len(self)+7) & (~0x7)
        sequence = list(self._seq)
        if not msb:
            sequence.reverse()
        bytes_ = bytearray()
        for pos in range(0, blength, 8):
            seq = sequence[pos:pos+8]
            byte = 0
            while seq:
                byte <<= 1
                byte |= seq.pop(0)
            bytes_.append(byte)
        if msby:
            bytes_.reverse()
        return bytes_

    @staticmethod
    def _tomutable(value: Union[str, Tuple]) -> List:
        """Convert a immutable sequence into a mutable one"""
        if isinstance(value, tuple):
            # convert immutable sequence into a list so it can be popped out
            value = list(value)
        elif isinstance(value, str):
            # convert immutable sequence into a list so it can be popped out
            if value.startswith('0b'):
                value = list(value[2:])
            else:
                value = list(value)
        return value

    def _init_from_integer(self, value: int, msb: bool, length: int) -> None:
        """Initialize from any integer value"""
        bl = length or -1
        seq = self._seq
        while bl:
            seq.append(bool(value & 1))
            value >>= 1
            if not value:
                break
            bl -= 1
        if msb:
            seq.reverse()

    def _init_from_iterable(self, iterable: Iterable, msb: bool) -> None:
        """Initialize from an iterable"""
        smap = {'0': 0, '1': 1, False: 0, True: 1, 0: 0, 1: 1}
        seq = self._seq
        try:
            if msb:
                seq.extend([smap[bit] for bit in reversed(iterable)])
            else:
                seq.extend([smap[bit] for bit in iterable])
        except KeyError as exc:
            raise BitSequenceError('Invalid binary character in initializer') \
                    from exc

    def _init_from_sibling(self, value: 'BitSequence', msb: bool) -> None:
        """Initialize from a fellow object"""
        self._seq = value.sequence()
        if msb:
            self._seq.reverse()

    def _update_length(self, length, msb):
        """If a specific length is specified, extend the sequence as
           expected"""
        if length and (len(self) < length):
            extra = bytearray([False] * (length-len(self)))
            if msb:
                extra.extend(self._seq)
                self._seq = extra
            else:
                self._seq.extend(extra)

    def __iter__(self):
        return self._seq.__iter__()

    def __reversed__(self):
        return self._seq.__reversed__()

    def __getitem__(self, index):
        if isinstance(index, slice):
            return self.__class__(value=self._seq[index])
        return self._seq[index]

    def __setitem__(self, index, value):
        if isinstance(value, BitSequence):
            if issubclass(value.__class__, self.__class__) and \
               value.__class__ != self.__class__:
                raise BitSequenceError("Cannot set item with instance of a "
                                       "subclass")
        if isinstance(index, slice):
            value = self.__class__(value, length=len(self._seq[index]))
            self._seq[index] = value.sequence()
        else:
            if not isinstance(value, BitSequence):
                value = self.__class__(value)
            val = value.tobit()
            if index > len(self._seq):
                raise BitSequenceError("Cannot change the sequence size")
            self._seq[index] = val

    def __len__(self):
        return len(self._seq)

    def __eq__(self, other):
        return self._cmp(other) == 0

    def __ne__(self, other):
        return not self == other

    def __le__(self, other):
        return not self._cmp(other) <= 0

    def __lt__(self, other):
        return not self._cmp(other) < 0

    def __ge__(self, other):
        return not self._cmp(other) >= 0

    def __gt__(self, other):
        return not self._cmp(other) > 0

    def _cmp(self, other):
        # the bit sequence should be of the same length
        ld = len(self) - len(other)
        if ld:
            return ld
        for n, (x, y) in enumerate(zip(self._seq, other.sequence()), start=1):
            if xor(x, y):
                return n
        return 0

    def __repr__(self):
        # cannot use bin() as it truncates the MSB zero bits
        return ''.join([b and '1' or '0' for b in reversed(self._seq)])

    def __str__(self):
        chunks = []
        srepr = repr(self)
        length = len(self)
        for i in range(0, length, 8):
            if i:
                j = -i
            else:
                j = None
            chunks.append(srepr[-i-8:j])
        return f'{len(self)}: {" ".join(reversed(chunks))}'

    def __int__(self):
        value = 0
        for b in reversed(self._seq):
            value <<= 1
            value |= b and 1
        return value

    def __and__(self, other):
        if not isinstance(other, self.__class__):
            raise BitSequenceError('Need a BitSequence to combine')
        if len(self) != len(other):
            raise BitSequenceError('Sequences must be the same size')
        return self.__class__(value=list(map(lambda x, y: x and y,
                                             self._seq, other.sequence())))

    def __or__(self, other):
        if not isinstance(other, self.__class__):
            raise BitSequenceError('Need a BitSequence to combine')
        if len(self) != len(other):
            raise BitSequenceError('Sequences must be the same size')
        return self.__class__(value=list(map(lambda x, y: x or y,
                                             self._seq, other.sequence())))

    def __add__(self, other):
        return self.__class__(value=self._seq + other.sequence())

    def __ilshift__(self, count):
        count %= len(self)
        seq = bytearray([0]*count)
        seq.extend(self._seq[:-count])
        self._seq = seq
        return self

    def __irshift__(self, count):
        count %= len(self)
        seq = self._seq[count:]
        seq.extend([0]*count)
        self._seq = seq
        return self

    def inc(self) -> None:
        """Increment the sequence"""
        for p, b in enumerate(self._seq):
            b ^= True
            self._seq[p] = b
            if b:
                break

    def dec(self) -> None:
        """Decrement the sequence"""
        for p, b in enumerate(self._seq):
            b ^= True
            self._seq[p] = b
            if not b:
                break

    def invariant(self) -> bool:
        """Tells whether all bits of the sequence are of the same value.

           Return the value, or ValueError if the bits are not of the same
           value
        """
        try:
            ref = self._seq[0]
        except IndexError as exc:
            raise ValueError('Empty sequence') from exc
        if len(self._seq) == 1:
            return ref
        for b in self._seq[1:]:
            if b != ref:
                raise ValueError('Bits do no match')
        return ref


class BitZSequence(BitSequence):
    """Tri-state bit sequence manipulation.

       Support most of the BitSequence operations, with an extra high-Z state

       :param value:  initial value
       :param msb:    most significant bit first or not
       :param length: count of signficant bits in the bit sequence
    """

    __slots__ = ['_seq']

    Z = 0xff  # maximum byte value

    def __init__(self, value=None, msb=False, length=0):
        BitSequence.__init__(self, value=value, msb=msb, length=length)

    def invert(self):
        self._seq = [x in (None, BitZSequence.Z) and BitZSequence.Z or x ^ 1
                     for x in self._seq]
        return self

    def tobyte(self, msb=False):
        raise BitSequenceError(f'Type {type(self)} cannot be converted to '
                               f'byte')

    def tobytes(self, msb=False, msby=False):
        raise BitSequenceError(f'Type {type(self)} cannot be converted to '
                               f'bytes')

    def matches(self, other):
        # pylint: disable=missing-function-docstring
        if not isinstance(self, BitSequence):
            raise BitSequenceError('Not a BitSequence instance')
        # the bit sequence should be of the same length
        ld = len(self) - len(other)
        if ld:
            return ld
        for (x, y) in zip(self._seq, other.sequence()):
            if BitZSequence.Z in (x, y):
                continue
            if x is not y:
                return False
        return True

    def _init_from_iterable(self, iterable, msb):
        """Initialize from an iterable"""
        smap = {'0': 0, '1': 1, 'Z': BitZSequence.Z,
                False: 0, True: 1, None: BitZSequence.Z,
                0: 0, 1: 1, BitZSequence.Z: BitZSequence.Z}
        seq = self._seq
        try:
            if msb:
                seq.extend([smap[bit] for bit in reversed(iterable)])
            else:
                seq.extend([smap[bit] for bit in iterable])
        except KeyError as exc:
            raise BitSequenceError("Invalid binary character in initializer") \
                    from exc

    def __repr__(self):
        smap = {False: '0', True: '1', BitZSequence.Z: 'Z'}
        return ''.join([smap[b] for b in reversed(self._seq)])

    def __int__(self):
        if BitZSequence.Z in self._seq:
            raise BitSequenceError("High-Z BitSequence cannot be converted to "
                                   "an integral type")
        return BitSequence.__int__(self)

    def __cmp__(self, other):
        # the bit sequence should be of the same length
        ld = len(self) - len(other)
        if ld:
            return ld
        for n, (x, y) in enumerate(zip(self._seq, other.sequence()), start=1):
            if x is not y:
                return n
        return 0

    def __and__(self, other):
        if not isinstance(self, BitSequence):
            raise BitSequenceError('Need a BitSequence-compliant object to '
                                   'combine')
        if len(self) != len(other):
            raise BitSequenceError('Sequences must be the same size')

        def andz(x, y):
            """Compute the boolean AND operation for a tri-state boolean"""
            if BitZSequence.Z in (x, y):
                return BitZSequence.Z
            return x and y
        return self.__class__(
            value=list(map(andz, self._seq, other.sequence())))

    def __or__(self, other):
        if not isinstance(self, BitSequence):
            raise BitSequenceError('Need a BitSequence-compliant object to '
                                   'combine')
        if len(self) != len(other):
            raise BitSequenceError('Sequences must be the same size')

        def orz(x, y):
            """Compute the boolean OR operation for a tri-state boolean"""
            if BitZSequence.Z in (x, y):
                return BitZSequence.Z
            return x or y
        return self.__class__(value=list(map(orz, self._seq,
                                             other.sequence())))

    def __rand__(self, other):
        return self.__and__(other)

    def __ror__(self, other):
        return self.__or__(other)

    def __radd__(self, other):
        return self.__class__(value=other) + self


class BitField:
    """Bit field class to access and modify an integral value

       Beware the slices does not behave as regular Python slices:
       bitfield[3:5] means b3..b5, NOT b3..b4 as with regular slices
    """

    __slots__ = ['_val']

    def __init__(self, value=0):
        self._val = value

    def to_seq(self, msb=0, lsb=0):
        """Return the BitFiled as a sequence of boolean value"""
        seq = bytearray()
        count = 0
        value = self._val
        while value:
            count += 1
            value >>= 1
        for x in range(lsb, max(msb, count)):
            seq.append(bool((self._val >> x) & 1))
        return tuple(reversed(seq))

    def __getitem__(self, index):
        if isinstance(index, slice):
            if index.stop == index.start:
                return None
            if index.stop < index.start:
                offset = index.stop
                count = index.start-index.stop+1
            else:
                offset = index.start
                count = index.stop-index.start+1
            mask = (1 << count)-1
            return (self._val >> offset) & mask
        return (self._val >> index) & 1

    def __setitem__(self, index, value):
        if isinstance(index, slice):
            if index.stop == index.start:
                return
            if index.stop < index.start:
                offset = index.stop
                count = index.start-index.stop+1
            else:
                offset = index.start
                count = index.stop-index.start+1
            mask = (1 << count)-1
            value = (value & mask) << offset
            mask <<= offset
            self._val = (self._val & ~mask) | value
        else:
            if isinstance(value, bool):
                value = int(value)
            value = (value & int(1)) << index
            mask = int(1) << index
            self._val = (self._val & ~mask) | value

    def __int__(self):
        return self._val

    def __str__(self):
        return bin(self._val)
