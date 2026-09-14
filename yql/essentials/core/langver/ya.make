LIBRARY()

SRCS(
    feature.cpp
    feature.gen.cpp
    yql_core_langver.cpp
)

PEERDIR(
    yql/essentials/core/issue
    yql/essentials/public/langver
)

RUN_PYTHON3(
    feature.gen.py
    --input ${ARCADIA_ROOT}/yql/essentials/data/language/features.json
    --output ${ARCADIA_BUILD_ROOT}/${MODDIR}/feature.gen.h
    IN ${ARCADIA_ROOT}/yql/essentials/data/language/features.json
    OUT ${ARCADIA_BUILD_ROOT}/${MODDIR}/feature.gen.h
    OUTPUT_INCLUDES
    ${ARCADIA_ROOT}/${MODDIR}/feature.h
)

ADDINCL(
    GLOBAL ${ARCADIA_BUILD_ROOT}/${MODDIR}
    GLOBAL ${ARCADIA_ROOT}/${MODDIR}
)

END()

RECURSE_FOR_TESTS(ut)
