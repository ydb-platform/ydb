import os
from .about import *
from .mrmr import hash, hash_unicode, hash_bytes


def get_include():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "include")
