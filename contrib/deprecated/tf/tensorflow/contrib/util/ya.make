LIBRARY()

VERSION(1.10.1)

LICENSE(Apache-2.0)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

PEERDIR(
    contrib/deprecated/tf
)

SRCS(
    convert_graphdef_memmapped_format_lib.cc
)

END()
