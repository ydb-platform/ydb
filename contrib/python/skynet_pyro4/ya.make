PY23_LIBRARY(Pyro4)

LICENSE(MIT)

VERSION(4.8+dev)

NO_LINT()

RESOURCE_FILES(
    PREFIX contrib/python/skynet_pyro4/
    .dist-info/METADATA
    .dist-info/top_level.txt
)

PY_SRCS(
    TOP_LEVEL
    Pyro4/util.py
    Pyro4/socketutil.py
    Pyro4/constants.py
    Pyro4/nsc.py
    Pyro4/errors.py
    Pyro4/threadutil.py
    Pyro4/core.py
    Pyro4/naming.py
    Pyro4/socketserver/geventserver.py
    Pyro4/socketserver/threadpoolserver.py
    Pyro4/socketserver/multiplexserver.py
    Pyro4/socketserver/__init__.py
    Pyro4/configuration.py
    Pyro4/__init__.py
)

END()
