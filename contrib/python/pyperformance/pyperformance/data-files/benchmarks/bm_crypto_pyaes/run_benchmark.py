#!/usr/bin/env python
"""
Pure-Python Implementation of the AES block-cipher.

Benchmark AES in CTR mode using the pyaes module.
"""

import pyperf

import pyaes

# 23,000 bytes
CLEARTEXT = b"This is a test. What could possibly go wrong? " * 500

# 128-bit key (16 bytes)
KEY = b'\xa1\xf6%\x8c\x87}_\xcd\x89dHE8\xbf\xc9,'


def bench_pyaes(loops):
    range_it = range(loops)
    t0 = pyperf.perf_counter()

    for loops in range_it:
        aes = pyaes.AESModeOfOperationCTR(KEY)
        ciphertext = aes.encrypt(CLEARTEXT)

        # need to reset IV for decryption
        aes = pyaes.AESModeOfOperationCTR(KEY)
        plaintext = aes.decrypt(ciphertext)

        # explicitly destroy the pyaes object
        aes = None

    dt = pyperf.perf_counter() - t0
    if plaintext != CLEARTEXT:
        raise Exception("decrypt error!")

    return dt


if __name__ == "__main__":
    runner = pyperf.Runner()
    runner.metadata['description'] = ("Pure-Python Implementation "
                                      "of the AES block-cipher")
    runner.bench_time_func('crypto_pyaes', bench_pyaes)
