RECURSE(
    decimal
    fastcheck
    fastcheck/abi/impl
    fastcheck/impl
    issue
    langver
    purecalc
    result_format
    sql_format
    sql_format/abi/impl
    sql_format/impl
    types
    udf
    udf_meta
    utf8
)

IF (OS_LINUX AND ARCH_X86_64 AND SANITIZER_TYPE != "undefined")
    RECURSE(
        fastcheck/abi/fat
        sql_format/abi/fat
    )
ENDIF()
