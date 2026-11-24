BUILD_ONLY_IF(OS_LINUX)

BUILD_ONLY_IF(ARCH_X86_64)

EXECTEST()

DEPENDS(
    contrib/tools/python3/bin
)

PEERDIR(
    build/platform/clang
)

# The default CLANG resource uses LLVM 16, whose lld lacks --force-group-allocation.
ENV(YQL_LIBRARY_TEST_TOOLCHAIN=$CLANG20_RESOURCE_GLOBAL)

DATA(
    arcadia/yql/essentials/build/scripts/internalize_library.py
    arcadia/yql/essentials/build/scripts/test_internalize_library.py
    arcadia/yql/essentials/build/scripts/test_internalize_library.sh
)

# Exercise the declared toolchain without the runner's implicit compiler.
RUN(
    /bin/bash
    ${TEST_SOURCE_ROOT}/test_internalize_library.sh
    NAME
    internalize_library
    ENV
    YA_CXX=
    ENV
    YQL_LIBRARY_TEST_PYTHON=${ARCADIA_BUILD_ROOT}/contrib/tools/python3/bin/python3
)

END()
