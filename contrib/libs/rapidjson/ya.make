LIBRARY()

LICENSE(
    BSD-3-Clause AND
    ISC AND
    JSON AND
    MIT
)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

VERSION(1.1.0)

OWNER(
    g:contrib
    g:cpp-contrib
)

ADDINCL(
    contrib/libs/rapidjson/include
)

END()
