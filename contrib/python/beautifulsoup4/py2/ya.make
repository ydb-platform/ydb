PY2_LIBRARY()

LICENSE(MIT)

VERSION(4.9.3)

PY_SRCS(
    TOP_LEVEL
    bs4/__init__.py
    bs4/builder/__init__.py
    bs4/builder/_html5lib.py
    bs4/builder/_htmlparser.py
    bs4/builder/_lxml.py
    bs4/dammit.py
    bs4/diagnose.py
    bs4/element.py
    bs4/formatter.py
)

PEERDIR(
    contrib/python/html5lib
    contrib/python/lxml
    contrib/python/soupsieve
)

NO_LINT()

RESOURCE_FILES(
    PREFIX contrib/python/beautifulsoup4/py2/
    .dist-info/METADATA
    .dist-info/top_level.txt
)

END()
