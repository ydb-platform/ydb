# cython: language_level=3
# cython: overflowcheck=False
# cython: cdivision=True

"""
Cython implementation of Bob Jenkin's hashlittle from lookup3.c.
This code was adapted from HDF5 by Mark Kittisopikul.
"""

"""
lookup3.c, by Bob Jenkins, May 2006, Public Domain.

These are functions for producing 32-bit hashes for hash table lookup.
hashword(), hashlittle(), hashlittle2(), hashbig(), mix(), and final()
are externally useful functions.  Routines to test the hash are included
if SELF_TEST is defined.  You can use this free for any purpose.  It's in
the public domain.  It has no warranty.

You probably want to use hashlittle().  hashlittle() and hashbig()
hash byte arrays.  hashlittle() is is faster than hashbig() on
little-endian machines.  Intel and AMD are little-endian machines.
On second thought, you probably want hashlittle2(), which is identical to
hashlittle() except it returns two 32-bit hashes for the price of one.
You could implement hashbig2() if you wanted but I haven't bothered here.

If you want to find a hash of, say, exactly 7 integers, do
  a = i1;  b = i2;  c = i3;
  mix(a,b,c);
  a += i4; b += i5; c += i6;
  mix(a,b,c);
  a += i7;
  final(a,b,c);
then use c as the hash value.  If you have a variable length array of
4-byte integers to hash, use hashword().  If you have a byte array (like
a character string), use hashlittle().  If you have several byte arrays, or
a mix of things, see the comments above hashlittle().

Why is this so big?  I read 12 bytes at a time into 3 4-byte integers,
then mix those integers.  This is fast (you can do a lot more thorough
mixing with 12*3 instructions on 3 integers than you can with 3 instructions
on 1 byte), but shoehorning those bytes into integers efficiently is messy.
"""

"""
HDF5 (Hierarchical Data Format 5) Software Library and Utilities
Copyright 2006 by The HDF Group.

NCSA HDF5 (Hierarchical Data Format 5) Software Library and Utilities
Copyright 1998-2006 by The Board of Trustees of the University of Illinois.

All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted for any purpose (including commercial purposes)
provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice,
   this list of conditions, and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions, and the following disclaimer in the documentation
   and/or materials provided with the distribution.

3. Neither the name of The HDF Group, the name of the University, nor the
   name of any Contributor may be used to endorse or promote products derived
   from this software without specific prior written permission from
   The HDF Group, the University, or the Contributor, respectively.

DISCLAIMER:
THIS SOFTWARE IS PROVIDED BY THE HDF GROUP AND THE CONTRIBUTORS
"AS IS" WITH NO WARRANTY OF ANY KIND, EITHER EXPRESSED OR IMPLIED. IN NO
EVENT SHALL THE HDF GROUP OR THE CONTRIBUTORS BE LIABLE FOR ANY DAMAGES
SUFFERED BY THE USERS ARISING OUT OF THE USE OF THIS SOFTWARE, EVEN IF
ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

You are under no obligation whatsoever to provide any bug fixes, patches, or
upgrades to the features, functionality or performance of the source code
("Enhancements") to anyone; however, if you choose to make your Enhancements
available either publicly, or directly to The HDF Group, without imposing a
separate written license agreement for such Enhancements, then you hereby
grant the following license: a non-exclusive, royalty-free perpetual license
to install, use, modify, prepare derivative works, incorporate into other
computer software, distribute, and sublicense such enhancements or derivative
works thereof, in binary and source code form.
"""

import cython

from numcodecs.compat import ensure_contiguous_ndarray

from libc.stdint cimport uint8_t, uint16_t, uint32_t

cpdef uint32_t jenkins_lookup3(const uint8_t[::1] _data, uint32_t initval=0):
    """
    jenkins_lookup3(data: bytes, initval: uint32_t=0)
    hash a variable-length key into a 32-bit value

    data   : the key (unaligned variable-length array of bytes)
    initval : can be any 4-byte value, defaults to 0

    Returns a 32-bit value.  Every bit of the key affects every bit of
    the return value.  Two keys differing by one or two bits will have
    totally different hash values.

    The best hash table sizes are powers of 2.  There is no need to do
    mod a prime (mod is sooo slow!).  If you need less than 32 bits,
    use a bitmask.  For example, if you need only 10 bits, do
    h = (h & hashmask(10))
    In which case, the hash table should have hashsize(10) elements.

    If you are hashing strings, do it like this:
    ```
    h = 0
    for k in strings:
      h = jenkins_lookup3(k, h)
    ```

    By Bob Jenkins, 2006.  bob_jenkins@burtleburtle.net.  You may use this
    code any way you wish, private, educational, or commercial.  It's free.

    Use for hash table lookup, or anything where one collision in 2^^32 is
    acceptable.  Do NOT use for cryptographic purposes.

    Converted from H5_checksum_lookup3
    https://github.com/HDFGroup/hdf5/blob/577c192518598c7e2945683655feffcdbdf5a91b/src/H5checksum.c#L378-L472

    Originally hashlittle from https://www.burtleburtle.net/bob/c/lookup3.c
    Alternatively, consider the hashword implementation if we can assume little endian and alignment.
    """

    cdef:
        size_t length = _data.shape[0]
        # internal state
        uint32_t a, b, c = 0

    # Set up the internal state
    a = b = c = (<uint32_t>0xdeadbeef) + (<uint32_t>length) + initval

    # Return immediately for empty bytes
    if length == 0:
        return c

    cdef:
        const uint8_t *k = <const uint8_t *> &_data[0]

    # We are adding uint32_t values (words) byte by byte so we do not assume endianness or alignment
    # lookup3.c hashlittle checks for alignment

    # all but the last block: affect some 32 bits of (a,b,c)
    while length > 12:
        a += k[0]
        a += (<uint32_t>k[1]) << 8
        a += (<uint32_t>k[2]) << 16
        a += (<uint32_t>k[3]) << 24
        b += k[4]
        b += (<uint32_t>k[5]) << 8
        b += (<uint32_t>k[6]) << 16
        b += (<uint32_t>k[7]) << 24
        c += k[8]
        c += (<uint32_t>k[9]) << 8
        c += (<uint32_t>k[10]) << 16
        c += (<uint32_t>k[11]) << 24
        a, b, c = _jenkins_lookup3_mix(a, b, c)
        length -= 12
        k += 12

    # -------------------------------- last block: affect all 32 bits of (c)
    if length == 12:
        c += (<uint32_t>k[11]) << 24
        length -= 1

    if length == 11:
        c += (<uint32_t>k[10]) << 16
        length -= 1

    if length == 10:
        c += (<uint32_t>k[9]) << 8
        length -= 1

    if length == 9:
        c += k[8]
        length -= 1

    if length == 8:
        b += (<uint32_t>k[7]) << 24
        length -= 1

    if length == 7:
        b += (<uint32_t>k[6]) << 16
        length -= 1

    if length == 6:
        b += (<uint32_t>k[5]) << 8
        length -= 1

    if length == 5:
        b += k[4]
        length -= 1

    if length == 4:
        a += (<uint32_t>k[3]) << 24
        length -= 1

    if length == 3:
        a += (<uint32_t>k[2]) << 16
        length -= 1

    if length == 2:
        a += (<uint32_t>k[1]) << 8
        length -= 1

    if length == 1:
        a += k[0]
        length -= 1

    if length == 0:
        pass

    return _jenkins_lookup3_final(a, b, c)

cdef inline uint32_t _jenkins_lookup3_final(uint32_t a, uint32_t b, uint32_t c):
    """
    _jenkins_lookup3_final -- final mixing of 3 32-bit values (a,b,c) into c

    Pairs of (a,b,c) values differing in only a few bits will usually
    produce values of c that look totally different.  This was tested for
    * pairs that differed by one bit, by two bits, in any combination
      of top bits of (a,b,c), or in any combination of bottom bits of
      (a,b,c).
    * "differ" is defined as +, -, ^, or ~^.  For + and -, I transformed
      the output delta to a Gray code (a^(a>>1)) so a string of 1's (as
      is commonly produced by subtraction) look like a single 1-bit
      difference.
    * the base values were pseudorandom, all zero but one bit set, or
      all zero plus a counter that starts at zero.

    These constants passed:
     14 11 25 16 4 14 24
     12 14 25 16 4 14 24
    and these came close:
      4  8 15 26 3 22 24
     10  8 15 26 3 22 24
     11  8 15 26 3 22 24
    """
    c ^= b
    c -= _jenkins_lookup3_rot(b,14)
    a ^= c
    a -= _jenkins_lookup3_rot(c,11)
    b ^= a
    b -= _jenkins_lookup3_rot(a,25)
    c ^= b
    c -= _jenkins_lookup3_rot(b,16)
    a ^= c
    a -= _jenkins_lookup3_rot(c,4)
    b ^= a
    b -= _jenkins_lookup3_rot(a,14)
    c ^= b
    c -= _jenkins_lookup3_rot(b,24)
    return c

cdef inline uint32_t _jenkins_lookup3_rot(uint32_t x, uint8_t k):
    return (((x) << (k)) ^ ((x) >> (32 - (k))))

cdef inline (uint32_t, uint32_t, uint32_t) _jenkins_lookup3_mix(uint32_t a, uint32_t b, uint32_t c):
    """
    _jenkins_lookup3_mix -- mix 3 32-bit values reversibly.

    This is reversible, so any information in (a,b,c) before mix() is
    still in (a,b,c) after mix().

    If four pairs of (a,b,c) inputs are run through mix(), or through
    mix() in reverse, there are at least 32 bits of the output that
    are sometimes the same for one pair and different for another pair.
    This was tested for:
    * pairs that differed by one bit, by two bits, in any combination
      of top bits of (a,b,c), or in any combination of bottom bits of
      (a,b,c).
    * "differ" is defined as +, -, ^, or ~^.  For + and -, I transformed
      the output delta to a Gray code (a^(a>>1)) so a string of 1's (as
      is commonly produced by subtraction) look like a single 1-bit
      difference.
    * the base values were pseudorandom, all zero but one bit set, or
      all zero plus a counter that starts at zero.

    Some k values for my "a-=c; a^=rot(c,k); c+=b;" arrangement that
    satisfy this are
        4  6  8 16 19  4
        9 15  3 18 27 15
       14  9  3  7 17  3
    Well, "9 15 3 18 27 15" didn't quite get 32 bits diffing
    for "differ" defined as + with a one-bit base and a two-bit delta.  I
    used http://burtleburtle.net/bob/hash/avalanche.html to choose
    the operations, constants, and arrangements of the variables.

    This does not achieve avalanche.  There are input bits of (a,b,c)
    that fail to affect some output bits of (a,b,c), especially of a.  The
    most thoroughly mixed value is c, but it doesn't really even achieve
    avalanche in c.

    This allows some parallelism.  Read-after-writes are good at doubling
    the number of bits affected, so the goal of mixing pulls in the opposite
    direction as the goal of parallelism.  I did what I could.  Rotates
    seem to cost as much as shifts on every machine I could lay my hands
    on, and rotates are much kinder to the top and bottom bits, so I used
    rotates.
    """
    a -= c
    a ^= _jenkins_lookup3_rot(c, 4)
    c += b
    b -= a
    b ^= _jenkins_lookup3_rot(a, 6)
    a += c
    c -= b
    c ^= _jenkins_lookup3_rot(b, 8)
    b += a
    a -= c
    a ^= _jenkins_lookup3_rot(c, 16)
    c += b
    b -= a
    b ^= _jenkins_lookup3_rot(a, 19)
    a += c
    c -= b
    c ^= _jenkins_lookup3_rot(b, 4)
    b += a
    return a, b, c


