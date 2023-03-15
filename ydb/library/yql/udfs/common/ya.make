RECURSE(
    clickhouse/client
    datetime
    datetime2
    digest
    histogram
    hyperloglog
    ip_base
    json
    json2
    math
    pire
    re2
    set
    stat
    string
    top
    topfreq
    unicode_base
    url_base
    yson2
)

IF (ARCH_X86_64)
    RECURSE(
        hyperscan
    )
ENDIF()

