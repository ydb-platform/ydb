LIBRARY()

LICENSE(ISC)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

VERSION(2.1.1)

NO_COMPILER_WARNINGS()

SRCS(
    src/yajl.c
    src/yajl_buf.c
    src/yajl_gen.c
    src/yajl_parser.c
    src/yajl_version.c
    src/yajl_alloc.c
    src/yajl_encode.c
    src/yajl_lex.c
    src/yajl_tree.c
    yajl_parser.cpp
)

END()
