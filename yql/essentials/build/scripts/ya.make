BUILD_ONLY_IF(OS_LINUX)
BUILD_ONLY_IF(ARCH_X86_64)

EXECTEST()

DEPENDS(contrib/tools/python3/bin)

PEERDIR(build/platform/clang)

# The default CLANG resource uses LLVM 16, whose lld lacks --force-group-allocation.
ENV(YQL_UDF_TEST_TOOLCHAIN=$CLANG20_RESOURCE_GLOBAL)

DATA(
    arcadia/yql/essentials/build/scripts/internalize_udf.py
    arcadia/yql/essentials/build/scripts/test_internalize_udf.py
    arcadia/yql/essentials/build/scripts/test_internalize_udf.sh
)

# Exercise the declared toolchain without the runner's implicit compiler.
RUN(
    /bin/bash ${TEST_SOURCE_ROOT}/test_internalize_udf.sh
    NAME internalize_udf
    ENV YA_CXX=
    ENV YQL_UDF_TEST_PYTHON=${ARCADIA_BUILD_ROOT}/contrib/tools/python3/bin/python3
)

END()
