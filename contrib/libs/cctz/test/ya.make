GTEST()

VERSION(Service-proxy-version)

LICENSE(Apache-2.0)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

PEERDIR(
    contrib/libs/cctz
    contrib/libs/cctz/tzdata
)

ADDINCL(
    contrib/libs/cctz/include
)

IF (NOT AUTOCHECK)
    # We do not set TZDIR to a stable data source, so
    # LoadZone("libc:localtime") is inconsistent and makes
    # LocalTimeLibC test fail on distbuild.
    CFLAGS(
        -DCCTZ_TEST_LIBC_LOCALTIME
    )
ENDIF()

SRCS(
    civil_time_test.cc
    time_zone_format_test.cc
    time_zone_lookup_test.cc
)

EXPLICIT_DATA()

END()
