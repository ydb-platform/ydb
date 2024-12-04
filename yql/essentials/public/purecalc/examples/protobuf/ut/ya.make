IF (NOT SANITIZER_TYPE AND NOT OPENSOURCE)

EXECTEST()

RUN(protobuf ${ARCADIA_BUILD_ROOT}/yql/essentials/udfs STDOUT log.out CANONIZE_LOCALLY log.out)

DEPENDS(
    yql/essentials/public/purecalc/examples/protobuf
    yql/essentials/udfs/common/url_base
    yql/essentials/udfs/common/ip_base
)

END()

ENDIF()
