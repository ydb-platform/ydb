LIBRARY()

LICENSE(BSL-1.0)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt) 

OWNER( 
    antoshkka 
    g:cpp-committee 
    g:cpp-contrib 
) 
 
INCLUDE(${ARCADIA_ROOT}/contrib/restricted/boost/boost_common.inc)

PEERDIR(
    contrib/libs/icu
)

CFLAGS(
    -DBOOST_LOCALE_WITH_ICU
    -DBOOST_HAS_ICU=1
)

IF (OS_ANDROID)
    CFLAGS(
        -DBOOST_LOCALE_NO_WINAPI_BACKEND=1
        -DBOOST_LOCALE_NO_POSIX_BACKEND=1
    )
ELSEIF (OS_WINDOWS)
    CFLAGS( 
        -DBOOST_LOCALE_NO_POSIX_BACKEND=1 
    ) 
ELSE()
    CFLAGS(
        -DBOOST_LOCALE_NO_WINAPI_BACKEND=1
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        src/win32/collate.cpp
        src/win32/converter.cpp
        src/win32/lcid.cpp
        src/win32/numeric.cpp
        src/win32/win_backend.cpp
    )
ELSEIF (NOT OS_ANDROID) 
    SRCS(
        src/posix/codecvt.cpp
        src/posix/collate.cpp
        src/posix/converter.cpp
        src/posix/numeric.cpp
        src/posix/posix_backend.cpp
    )
ENDIF()

SRCS(
    src/encoding/codepage.cpp
    src/icu/boundary.cpp
    src/icu/codecvt.cpp
    src/icu/collator.cpp
    src/icu/conversion.cpp
    src/icu/date_time.cpp
    src/icu/formatter.cpp
    src/icu/icu_backend.cpp
    src/icu/numeric.cpp
    src/icu/time_zone.cpp
    src/shared/date_time.cpp
    src/shared/format.cpp
    src/shared/formatting.cpp
    src/shared/generator.cpp
    src/shared/ids.cpp
    src/shared/localization_backend.cpp
    src/shared/message.cpp
    src/shared/mo_lambda.cpp
    src/std/codecvt.cpp
    src/std/collate.cpp
    src/std/converter.cpp
    src/std/numeric.cpp
    src/std/std_backend.cpp
    src/util/codecvt_converter.cpp
    src/util/default_locale.cpp
    src/util/gregorian.cpp
    src/util/info.cpp
    src/util/locale_data.cpp
)

END()
