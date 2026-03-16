# The MIT License (MIT)
#
# Copyright (c) 2014 Richard Moore
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.


import sys
sys.path.append('../pyaes')

import os
import random

try:
    from StringIO import StringIO
except:
    import io
    StringIO = io.BytesIO

import pyaes
from pyaes.blockfeeder import Decrypter, Encrypter
from pyaes import decrypt_stream, encrypt_stream
from pyaes.util import to_bufferable


key = os.urandom(32)

plaintext = os.urandom(1000)

for mode_name in pyaes.AESModesOfOperation:
    mode = pyaes.AESModesOfOperation[mode_name]
    print(mode.name)

    kw = dict(key = key)
    if mode_name in ('cbc', 'cfb', 'ofb'):
        kw['iv'] = os.urandom(16)

    encrypter = Encrypter(mode(**kw))
    ciphertext = to_bufferable('')

    # Feed the encrypter random number of bytes at a time
    index = 0
    while index < len(plaintext):
        length = random.randint(1, 128)
        if index + length > len(plaintext): length = len(plaintext) - index
        ciphertext += encrypter.feed(plaintext[index: index + length])
        index += length
    ciphertext += encrypter.feed(None)

    decrypter = Decrypter(mode(**kw))
    decrypted = to_bufferable('')

    # Feed the decrypter random number of bytes at a time
    index = 0
    while index < len(ciphertext):
        length = random.randint(1, 128)
        if index + length > len(ciphertext): length = len(ciphertext) - index
        decrypted += decrypter.feed(ciphertext[index: index + length])
        index += length
    decrypted += decrypter.feed(None)

    passed = decrypted == plaintext
    cipher_length = len(ciphertext)
    print("  cipher-length=%(cipher_length)s passed=%(passed)s" % locals())

# Test block modes of operation with no padding
plaintext = os.urandom(1024)

for mode_name in ['ecb', 'cbc']:
    mode = pyaes.AESModesOfOperation[mode_name]
    print(mode.name + ' (no padding)')

    kw = dict(key = key)
    if mode_name == 'cbc':
        kw['iv'] = os.urandom(16)

    encrypter = Encrypter(mode(**kw), padding = pyaes.PADDING_NONE)
    ciphertext = to_bufferable('')

    # Feed the encrypter random number of bytes at a time
    index = 0
    while index < len(plaintext):
        length = random.randint(1, 128)
        if index + length > len(plaintext): length = len(plaintext) - index
        ciphertext += encrypter.feed(plaintext[index: index + length])
        index += length
    ciphertext += encrypter.feed(None)

    if len(ciphertext) != len(plaintext):
        print('  failed to encrypt with correct padding')

    decrypter = Decrypter(mode(**kw), padding = pyaes.PADDING_NONE)
    decrypted = to_bufferable('')

    # Feed the decrypter random number of bytes at a time
    index = 0
    while index < len(ciphertext):
        length = random.randint(1, 128)
        if index + length > len(ciphertext): length = len(ciphertext) - index
        decrypted += decrypter.feed(ciphertext[index: index + length])
        index += length
    decrypted += decrypter.feed(None)

    passed = decrypted == plaintext
    cipher_length = len(ciphertext)
    print("  cipher-length=%(cipher_length)s passed=%(passed)s" % locals())

plaintext = os.urandom(1000)

for mode_name in pyaes.AESModesOfOperation:
    mode = pyaes.AESModesOfOperation[mode_name]
    print(mode.name + ' (stream operations)')

    kw = dict(key = key)
    if mode_name in ('cbc', 'cfb', 'ofb'):
        kw['iv'] = os.urandom(16)

    moo = mode(**kw)
    output = StringIO()
    pyaes.encrypt_stream(moo, StringIO(plaintext), output)
    output.seek(0)
    ciphertext = output.read()

    moo = mode(**kw)
    output = StringIO()
    pyaes.decrypt_stream(moo, StringIO(ciphertext), output)
    output.seek(0)
    decrypted = output.read()

    passed = decrypted == plaintext
    cipher_length = len(ciphertext)
    print("  cipher-length=%(cipher_length)s passed=%(passed)s" % locals())

# Issue #15
# https://github.com/ricmoo/pyaes/issues/15
# @TODO: These tests need a severe overhaul; they should use deterministic input, keys and IVs...
def TestIssue15():
    print('Issue #15')

    key = b"abcdefghijklmnop"
    iv = b"0123456789012345"
    encrypter = pyaes.Encrypter(pyaes.AESModeOfOperationCBC(key, iv))

    plaintext = b"Hello World!!!!!"

    ciphertext = to_bufferable('')

    ciphertext += encrypter.feed(plaintext)
    ciphertext += encrypter.feed('')
    ciphertext += encrypter.feed(plaintext)
    ciphertext += encrypter.feed(None)
    expected = b'(Ob\xe5\xae"\xdc\xb0\x84\xc5\x04\x04GQ\xd8.\x0e4\xd2b\xc1\x15\xe5\x11M\xfc\x9a\xd2\xd5\xc8xP\x00[\xd57\x92\x01\xbb\xc42\x18\xbc\xbf\x1ay\x19P'

    decrypter = pyaes.Decrypter(pyaes.AESModeOfOperationCBC(key, iv))

    output = to_bufferable('')

    output += decrypter.feed('')
    output += decrypter.feed(ciphertext)
    output += decrypter.feed('')
    output += decrypter.feed(None)

    print("  passed=%(passed)s" % dict(passed = (ciphertext == expected and output == (plaintext + plaintext))))

TestIssue15()
