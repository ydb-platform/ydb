RECURSE(
    decimal
    fastcheck
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
)

IF (OS_LINUX AND ARCH_X86_64 AND SANITIZER_TYPE != "undefined")
    RECURSE(
        sql_format/abi/fat
    )
ENDIF()
