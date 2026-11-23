# A udf whose implementation is compiled to bitcode as well, which only makes sense as a test: the
# ABI check in udf_version.h then travels into the module the JIT links at runtime, where there
# is no linker to resolve anything it refers to.
YQL_UDF_CONTRIB(bitcode_probe_udf)

YQL_ABI_VERSION(
    2
    28
    0
)

SRCS(
    bitcode_probe_udf.cpp
)

IF (NOT OS_LINUX OR CLANG_MCDC_COVERAGE == "yes")
    CFLAGS(-DDISABLE_IR)
ELSE()
    USE_LLVM_BC16()

    LLVM_BC(
        bitcode_probe_ir.cpp
        NAME BitcodeProbe
        SYMBOLS
        TwiceIR
    )
ENDIF()

END()

RECURSE_FOR_TESTS(
    udf_test
)
