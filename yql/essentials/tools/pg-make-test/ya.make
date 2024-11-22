IF (NOT OPENSOURCE)

PY3_PROGRAM(pg-make-test)

PY_SRCS(
    __main__.py
)

PEERDIR(
    contrib/python/click
    contrib/python/PyYAML
    contrib/python/patch
    library/python/svn_version
    yql/essentials/tests/postgresql/common
)

END()

RECURSE(
  ../pgrun
  update-test-status
)

ENDIF()


