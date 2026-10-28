YQL_ABI_VERSION(
    2
    46
    0
)

SRCS(
    yql/essentials/udfs/common/math/math_udf.cpp
)

# USE_LLVM_BC16 pins the bitcode compiler to Clang 16, which rejects
# -fcoverage-mcdc (Clang 18+ flag, added to BC_CXXFLAGS via $CXXFLAGS under
# MC/DC coverage). Fall back to the native DISABLE_IR path, same as on
# non-Linux. Further move to LLVM 18 will be done in scope of YQL-21388.
IF (NOT OS_LINUX OR CLANG_MCDC_COVERAGE == "yes")
    CFLAGS(
        -DDISABLE_IR
    )
ELSE()
    PEERDIR(
        yql/essentials/udfs/common/math/ir
    )
ENDIF()

PEERDIR(
    yql/essentials/core/langver
    yql/essentials/udfs/common/math/lib
)
