PY23_LIBRARY()

PY_SRCS(
    __init__.py
)

RESOURCE_FILES(
    PREFIX library/python/coredump_filter/
    core_proc.js
    epilog.html
    prolog.html
    styles.css
)

IF(PYTHON2)
    PEERDIR(contrib/deprecated/python/enum34)
ENDIF()

END()

RECURSE(
    tests
)
