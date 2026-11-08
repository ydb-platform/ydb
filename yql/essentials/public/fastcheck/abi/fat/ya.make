BUILD_ONLY_IF(OS_LINUX)

BUILD_ONLY_IF(ARCH_X86_64)

YQL_INTERNALIZED_LIBRARY_FAT(fastcheck_abi)

YQL_EXPORT_SYMBOLS(
    YqlFastCheckV1
    YqlFastCheckFreeV1
    YqlFastCheckResultV1
    YqlFastCheckErrorV1
    YqlLinterV1
    YqlLinterLoadUdfMetaV1
    YqlUdfMetaFreeV1
    YqlUdfMetaErrorV1
    YqlUdfMetaGetAbiV1
    YqlLinterPreloadIssuesV1
    YqlLinterFreeV1
    YqlLinterResultV1
    YqlLinterErrorV1
)

PEERDIR(
    yql/essentials/public/fastcheck/abi/impl
)

END()

RECURSE_FOR_TESTS(
    ut
)
