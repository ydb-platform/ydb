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

from pyaes import *

import os, time

# Python 3 doesn't have xrange and returns bytes from urandom
try:
    xrange
except NameError:
    xrange = range
else:
    pass

# compare against a known working implementation
from Crypto.Cipher import AES as KAES
from Crypto.Util import Counter as KCounter
for mode in [ 'CBC', 'CTR',  'CFB', 'ECB', 'OFB' ]:

    (tt_ksetup, tt_kencrypt, tt_kdecrypt) = (0.0, 0.0, 0.0)
    (tt_setup, tt_encrypt, tt_decrypt) = (0.0, 0.0, 0.0)
    count = 0

    for key_size in (128, 192, 256):

        for test in xrange(1, 8):
            key = os.urandom(key_size // 8)

            if mode == 'CBC':
                iv = os.urandom(16)
                plaintext = [ os.urandom(16) for x in xrange(0, test) ]

                t0 = time.time()
                kaes = KAES.new(key, KAES.MODE_CBC, IV = iv)
                kaes2 = KAES.new(key, KAES.MODE_CBC, IV = iv)
                tt_ksetup += time.time() - t0

                t0 = time.time()
                aes = AESModeOfOperationCBC(key, iv = iv)
                aes2 = AESModeOfOperationCBC(key, iv = iv)
                tt_setup += time.time() - t0

            elif mode == 'CFB':
                iv = os.urandom(16)
                plaintext = [ os.urandom(test * 5) for x in xrange(0, test) ]

                t0 = time.time()
                kaes = KAES.new(key, KAES.MODE_CFB, IV = iv, segment_size = test * 8)
                kaes2 = KAES.new(key, KAES.MODE_CFB, IV = iv, segment_size = test * 8)
                tt_ksetup += time.time() - t0

                t0 = time.time()
                aes = AESModeOfOperationCFB(key, iv = iv, segment_size = test)
                aes2 = AESModeOfOperationCFB(key, iv = iv, segment_size = test)
                tt_setup += time.time() - t0

            elif mode == 'ECB':
                plaintext = [ os.urandom(16) for x in xrange(0, test) ]

                t0 = time.time()
                kaes = KAES.new(key, KAES.MODE_ECB)
                kaes2 = KAES.new(key, KAES.MODE_ECB)
                tt_ksetup += time.time() - t0

                t0 = time.time()
                aes = AESModeOfOperationECB(key)
                aes2 = AESModeOfOperationECB(key)
                tt_setup += time.time() - t0

            elif mode == 'OFB':
                iv = os.urandom(16)
                plaintext = [ os.urandom(16) for x in xrange(0, test) ]

                t0 = time.time()
                kaes = KAES.new(key, KAES.MODE_OFB, IV = iv)
                kaes2 = KAES.new(key, KAES.MODE_OFB, IV = iv)
                tt_ksetup += time.time() - t0

                t0 = time.time()
                aes = AESModeOfOperationOFB(key, iv = iv)
                aes2 = AESModeOfOperationOFB(key, iv = iv)
                tt_setup += time.time() - t0

            elif mode == 'CTR':
                text_length = [None, 3, 16, 127, 128, 129, 1500, 10000, 100000, 10001, 10002, 10003, 10004, 10005, 10006, 10007, 10008][test]
                if test < 6:
                    plaintext = [ os.urandom(text_length) ]
                else:
                    plaintext = [ os.urandom(text_length) for x in xrange(0, test) ]

                t0 = time.time()
                kaes = KAES.new(key, KAES.MODE_CTR, counter = KCounter.new(128, initial_value = 0))
                kaes2 = KAES.new(key, KAES.MODE_CTR, counter = KCounter.new(128, initial_value = 0))
                tt_ksetup += time.time() - t0

                t0 = time.time()
                aes = AESModeOfOperationCTR(key, counter = Counter(initial_value = 0))
                aes2 = AESModeOfOperationCTR(key, counter = Counter(initial_value = 0))
                tt_setup += time.time() - t0

            count += 1

            t0 = time.time()
            kenc = [kaes.encrypt(p) for p in plaintext]
            tt_kencrypt += time.time() - t0

            t0 = time.time()
            enc = [aes.encrypt(p) for p in plaintext]
            tt_encrypt += time.time() - t0

            if kenc != enc:
                print("Test: mode=%s operation=encrypt key_size=%d text_length=%d trial=%d" % (mode, key_size, len(plaintext), test))
                raise Exception('Failed encypt test case (%s)' % mode)

            t0 = time.time()
            dt1 = [kaes2.decrypt(k) for k in kenc]
            tt_kdecrypt += time.time() - t0

            t0 = time.time()
            dt2 = [aes2.decrypt(k) for k in kenc]
            tt_decrypt += time.time() - t0

            if plaintext != dt2:
                print("Test: mode=%s operation=decrypt key_size=%d text_length=%d trial=%d" % (mode, key_size, len(plaintext), test))
                raise Exception('Failed decypt test case (%s)' % mode)

    better = (tt_setup + tt_encrypt + tt_decrypt) / (tt_ksetup + tt_kencrypt + tt_kdecrypt)
    print("Mode: %s" % mode)
    print("  Average time: PyCrypto: encrypt=%fs decrypt=%fs setup=%f" % (tt_kencrypt / count, tt_kdecrypt / count, tt_ksetup / count))
    print("  Average time: pyaes:    encrypt=%fs decrypt=%fs setup=%f" % (tt_encrypt / count, tt_decrypt / count, tt_setup / count))
    print("  Native better by: %dx" % better)

print("All test cases passes!")

