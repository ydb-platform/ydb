# coding: utf-8
from __future__ import unicode_literals
from __future__ import division

from random import random

from nanoid.resources import alphabet, size


def non_secure_generate(alphabet=alphabet, size=size):
    alphabet_len = len(alphabet)

    id = ''
    for _ in range(size):
        id += alphabet[int(random() * alphabet_len) | 0]
    return id


if __name__ == '__main__':
    print(non_secure_generate())
