LIBRARY()

VERSION(1.0.0)

LICENSE(MIT)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

NO_UTIL()

ADDINCL(GLOBAL contrib/libs/fastops)

SRCS(
    FastIntrinsics.h
    SIMDFunctions.h
    avx_id.cpp
    avx_id.h
)

END()
