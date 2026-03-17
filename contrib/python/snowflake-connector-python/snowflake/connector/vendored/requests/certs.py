#!/usr/bin/env python

"""
requests.certs
~~~~~~~~~~~~~~

This module returns the preferred default CA certificate bundle. There is
only one — the one from the certifi package.

If you are packaging Requests, e.g., for a Linux distribution or a managed
environment, you can change the definition of where() to return a separately
packaged CA bundle.
"""
import pathlib
import sys
import os
from certifi import where as certifi_where

def where():
    # since ya make tests are executed in the specific environment without usual file path's
    #   and sf vendored libs expects a file path in certifi.where() response this workaroud is created
    #   during test process cacert would be placed in the tmp location
    #   and projects using snowflake should set env var SNOWFLAKE_CABUNDLE_PATH where to place certs from arcadia
    # and packaged solution might provide some reasonable path to cacert.pm
    is_arcadia_python = hasattr(sys, "extra_modules")

    if is_arcadia_python:
        cacert_pem_path = os.environ.get("SNOWFLAKE_CABUNDLE_PATH")
        if cacert_pem_path:
            cacert_pem_path = pathlib.Path(cacert_pem_path)
            certs_path = cacert_pem_path.parent
        else:
            certs_path = pathlib.Path(os.environ.get("TMP")) / "certs"
            cacert_pem_path = certs_path / "cacert.pem"

        if not cacert_pem_path.exists():
            import library.python.resource
            certs_path.mkdir(parents=True, exist_ok=True)
            with open(cacert_pem_path, "wb") as fh:
                fh.write(library.python.resource.find("/builtin/cacert"))

        return str(cacert_pem_path.resolve())
    else:
        return certifi_where()


if __name__ == "__main__":
    print(where())
