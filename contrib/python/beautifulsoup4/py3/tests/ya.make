PY3TEST()

PEERDIR(
    contrib/python/beautifulsoup4
    contrib/python/html5lib
    contrib/python/lxml
)

SRCDIR(contrib/python/beautifulsoup4/py3/bs4/tests)

TEST_SRCS(
    __init__.py
    test_builder.py
    test_builder_registry.py
    test_dammit.py
    test_element.py
    test_filter.py
    test_formatter.py
    test_html5lib.py
    test_htmlparser.py
    test_lxml.py
    test_navigablestring.py
    test_pageelement.py
    test_soup.py
    test_tag.py
    test_tree.py
)

NO_LINT()

END()
