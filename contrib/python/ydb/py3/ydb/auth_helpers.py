# -*- coding: utf-8 -*-
import os
from typing import Optional


def read_bytes(f):
    with open(f, "rb") as fr:
        return fr.read()


def load_ydb_root_certificate(path: Optional[str] = None):
    path = path if path is not None else os.getenv("YDB_SSL_ROOT_CERTIFICATES_FILE", None)
    if path is not None and os.path.exists(path):
        return read_bytes(path)
    return None
