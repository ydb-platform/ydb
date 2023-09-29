PY2_LIBRARY()

LICENSE(MIT)

VERSION(5.4.1)

PEERDIR(
    contrib/libs/yaml
)

ADDINCL(
    contrib/python/PyYAML/py2/yaml
    FOR cython contrib/python/PyYAML/py2
)

PY_SRCS(
    TOP_LEVEL
    _yaml/__init__.py
    yaml/__init__.py
    yaml/composer.py
    yaml/constructor.py
    yaml/cyaml.py
    yaml/dumper.py
    yaml/emitter.py
    yaml/error.py
    yaml/events.py
    yaml/loader.py
    yaml/nodes.py
    yaml/parser.py
    yaml/reader.py
    yaml/representer.py
    yaml/resolver.py
    yaml/scanner.py
    yaml/serializer.py
    yaml/tokens.py
    CYTHON_C
    yaml/_yaml.pyx
)

RESOURCE_FILES(
    PREFIX contrib/python/PyYAML/
    .dist-info/METADATA
    .dist-info/top_level.txt
)

NO_LINT()

NO_COMPILER_WARNINGS()

END()
