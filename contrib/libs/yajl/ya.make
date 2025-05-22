LIBRARY()

LICENSE(ISC)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

VERSION(2.1.1)

NO_COMPILER_WARNINGS()

SRCS(
    yajl.c
    yajl_buf.c
    yajl_gen.c
    yajl_parser.c
    yajl_version.c
    yajl_alloc.c
    yajl_encode.c
    yajl_lex.c
    yajl_tree.c
    yajl_parser.cpp
)

END()
