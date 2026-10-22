IF (NOT EXPORT_CMAKE)
    RECURSE(
        compress_base
        compress_base/impl
        datetime2
        datetime2/impl
        digest
        digest/impl
        file
        file/impl
        histogram
        histogram/impl
        hyperloglog
        hyperloglog/impl
        ip_base
        json
        json2
        json2/impl
        math
        math/impl
        pire
        pire/impl
        protobuf
        protobuf/impl
        python
        re2
        re2/impl
        reservoir_sampling
        reservoir_sampling/impl
        set
        set/impl
        stat
        stat/impl
        streaming
        streaming/impl
        string
        string/impl
        top
        top/impl
        topfreq
        topfreq/impl
        unicode_base
        unicode_base/impl
        url_base
        url_base/impl
        vector
        vector/impl
        yson2
        yson2/impl
    )
    IF (OS_LINUX AND ARCH_X86_64)
        IF (SANITIZER_TYPE != "undefined")
            RECURSE(string/fat)
        ENDIF()
        IF (NOT OPENSOURCE AND NOT BUILD_YQL_FROM_SOURCES)
            RECURSE(string/static_host)
        ELSEIF (SANITIZER_TYPE == "undefined")
            RECURSE(string/static_host)
        ENDIF()
    ENDIF()
    IF (ARCH_X86_64)
        RECURSE(
            hyperscan
            hyperscan/impl
        )
    ENDIF()
ENDIF()
