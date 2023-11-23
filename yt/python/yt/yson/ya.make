PY23_LIBRARY()

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

END()
