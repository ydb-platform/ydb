SET(UDF_PROBE_RESOURCE None)

IF (NOT OPENSOURCE AND NOT BUILD_YQL_FROM_SOURCES AND OS_LINUX AND ARCH_X86_64)
    INCLUDE(${ARCADIA_ROOT}/yql/resources.inc)
    IF (SANITIZER_TYPE == "address")
        SET(UDF_PROBE_RESOURCE ${UDF_PROBE_RESOURCE_ASAN})
    ELSEIF (SANITIZER_TYPE == "memory")
        SET(UDF_PROBE_RESOURCE ${UDF_PROBE_RESOURCE_MSAN})
    ELSEIF (SANITIZER_TYPE == "thread")
        SET(UDF_PROBE_RESOURCE ${UDF_PROBE_RESOURCE_TSAN})
    ELSEIF (SANITIZER_TYPE == "undefined")
        SET(UDF_PROBE_RESOURCE ${UDF_PROBE_RESOURCE_UBSAN})
    ELSE()
        SET(UDF_PROBE_RESOURCE ${UDF_PROBE_RESOURCE_DEFAULT})
    ENDIF()
ENDIF()

IF (UDF_PROBE_RESOURCE != "None")
    YQL_PREBUILT_TOOL()

    FROM_SANDBOX(FILE ${UDF_PROBE_RESOURCE} RENAME RESOURCE OUT_NOAUTO udf_probe.prebuilt EXECUTABLE)
    PRIMARY_OUTPUT(${BINDIR}/udf_probe.prebuilt)

    END()
ELSE()
    PROGRAM()

    SRCS(
        udf_probe.cpp
    )

    IF (OS_LINUX)
        # prevent external python extensions to lookup protobuf symbols (and maybe
        # other common stuff) in main binary
        EXPORTS_SCRIPT(${ARCADIA_ROOT}/yql/essentials/tools/exports.symlist)
    ENDIF()

    PEERDIR(
        yql/essentials/minikql
        yql/essentials/public/udf/service/terminate_policy
        yql/essentials/sql/pg_dummy
    )

    YQL_CURRENT_ABI_VERSION()

    END()
ENDIF()
