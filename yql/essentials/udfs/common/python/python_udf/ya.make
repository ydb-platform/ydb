PY23_NATIVE_LIBRARY()

YQL_ABI_VERSION(2 27 0)

SRCS(
    python_udf.cpp
)

PEERDIR(
    yql/essentials/public/udf
    yql/essentials/udfs/common/python/bindings
)

CFLAGS(
    -DDISABLE_PYDEBUG
)

NO_COMPILER_WARNINGS()

END()
