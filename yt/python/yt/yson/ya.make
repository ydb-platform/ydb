PY23_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

IF (PYTHON2)
    PEERDIR(yt/python_py2/yt/yson)
ELSE()
    PEERDIR(
        yt/python/yt

        contrib/python/six
    )

    PY_SRCS(
        NAMESPACE yt.yson

        __init__.py
        common.py
        convert.py
        lexer.py
        parser.py
        tokenizer.py
        writer.py
        yson_token.py
        yson_types.py
    )
ENDIF()

END()
