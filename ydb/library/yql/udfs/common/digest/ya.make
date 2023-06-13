YQL_UDF(digest_udf)

YQL_ABI_VERSION(
    2
    28
    0
)

SRCS(
    digest_udf.cpp
)

PEERDIR(
    contrib/libs/farmhash
    contrib/libs/highwayhash
    contrib/libs/openssl
    contrib/libs/xxhash
    library/cpp/digest/argonish
    library/cpp/digest/crc32c
    library/cpp/digest/md5
    library/cpp/digest/old_crc
    library/cpp/digest/sfh
)

ADDINCL(contrib/libs/highwayhash)

END()
