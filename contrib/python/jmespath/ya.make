PY23_LIBRARY() 

OWNER(g:python-contrib)

VERSION(0.10.0)

LICENSE(MIT)

NO_LINT()

PY_SRCS(
    TOP_LEVEL
    jmespath/__init__.py
    jmespath/ast.py
    jmespath/compat.py
    jmespath/exceptions.py
    jmespath/functions.py
    jmespath/lexer.py
    jmespath/parser.py
    jmespath/visitor.py
)

RESOURCE_FILES(
    PREFIX contrib/python/jmespath/
    .dist-info/METADATA
    .dist-info/top_level.txt
)

END()

RECURSE_FOR_TESTS(
    tests
)
