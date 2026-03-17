#
# Copyright (c) nexB Inc. and others. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/commoncode for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

import binascii
import hashlib

from commoncode.codec import bin_to_num
from commoncode.codec import urlsafe_b64encode
from commoncode import filetype

"""
Hashes and checksums.

Low level hash functions using standard crypto hashes used to construct hashes
of various lengths. Hashes that are smaller than 128 bits are based on a
truncated md5. Other length use SHA hashes.

Checksums are operating on files.
"""


def _hash_mod(bitsize, hmodule):
    """
    Return a hashing class returning hashes with a `bitsize` bit length. The
    interface of this class is similar to the hash module API.
    """

    class hasher(object):

        def __init__(self, msg=None):
            self.digest_size = bitsize // 8
            self.h = msg and hmodule(msg).digest()[:self.digest_size] or None

        def digest(self):
            return bytes(self.h)

        def hexdigest(self):
            return self.h and binascii.hexlify(self.h).decode('utf-8')

        def b64digest(self):
            return self.h and urlsafe_b64encode(self.h).decode('utf-8')

        def intdigest(self):
            return self.h and int(bin_to_num(self.h))

    return hasher


# Base hashers for each bit size
_hashmodules_by_bitsize = {
    # md5-based
    32: _hash_mod(32, hashlib.md5),
    64: _hash_mod(64, hashlib.md5),
    128: _hash_mod(128, hashlib.md5),
    # sha-based
    160: _hash_mod(160, hashlib.sha1),
    256: _hash_mod(256, hashlib.sha256),
    384: _hash_mod(384, hashlib.sha384),
    512: _hash_mod(512, hashlib.sha512)
}


def get_hasher(bitsize):
    """
    Return a hasher for a given size in bits of the resulting hash.
    """
    return _hashmodules_by_bitsize[bitsize]


class sha1_git_hasher(object):
    """
    Hash content using the git blob SHA1 convention.
    """

    def __init__(self, msg=None):
        self.digest_size = 160 // 8
        self.h = msg and self._compute(msg) or None

    def _compute(self, msg):
        # note: bytes interpolation is new in Python 3.5
        git_blob_msg = b'blob %d\0%s' % (len(msg), msg)
        return hashlib.sha1(git_blob_msg).digest()

    def digest(self):
        return bytes(self.h)

    def hexdigest(self):
        return self.h and binascii.hexlify(self.h).decode('utf-8')

    def b64digest(self):
        return self.h and urlsafe_b64encode(self.h).decode('utf-8')

    def intdigest(self):
        return self.h and int(bin_to_num(self.h))


_hashmodules_by_name = {
    'md5': get_hasher(128),
    'sha1': get_hasher(160),
    'sha1_git': sha1_git_hasher,
    'sha256': get_hasher(256),
    'sha384': get_hasher(384),
    'sha512': get_hasher(512)
}


def checksum(location, name, base64=False):
    """
    Return a checksum of `bitsize` length from the content of the file at
    `location`. The checksum is a hexdigest or base64-encoded is `base64` is
    True.
    """
    if not filetype.is_file(location):
        return
    hasher = _hashmodules_by_name[name]

    # fixme: we should read in chunks?
    with open(location, 'rb') as f:
        hashable = f.read()

    hashed = hasher(hashable)
    if base64:
        return hashed.b64digest()

    return hashed.hexdigest()


def md5(location):
    return checksum(location, name='md5', base64=False)


def sha1(location):
    return checksum(location, name='sha1', base64=False)


def b64sha1(location):
    return checksum(location, name='sha1', base64=True)


def sha256(location):
    return checksum(location, name='sha256', base64=False)


def sha512(location):
    return checksum(location, name='sha512', base64=False)


def sha1_git(location):
    return checksum(location, name='sha1_git', base64=False)


def multi_checksums(location, checksum_names=('md5', 'sha1', 'sha256', 'sha512', 'sha1_git')):
    """
    Return a mapping of hexdigest checksums keyed by checksum name from the content
    of the file at `location`. Use the `checksum_names` list of checksum names.
    The mapping is guaranted to contains all the requested names as keys.
    If the location is not a file, the values are None.
    """
    results = dict([(name, None) for name in checksum_names])
    if not filetype.is_file(location):
        return results

    # fixme: we should read in chunks?
    with open(location, 'rb') as f:
        hashable = f.read()

    for name in checksum_names:
        results[name] = _hashmodules_by_name[name](hashable).hexdigest()
    return results
