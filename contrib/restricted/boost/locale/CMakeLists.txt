
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(restricted-boost-locale)
target_compile_options(restricted-boost-locale PRIVATE
  -DBOOST_LOCALE_WITH_ICU
  -DBOOST_LOCALE_NO_WINAPI_BACKEND
  -Wno-everything
)
target_include_directories(restricted-boost-locale PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/locale/include
)
target_link_libraries(restricted-boost-locale PUBLIC
  contrib-libs-cxxsupp
  contrib-libs-icu
  restricted-boost-assert
  restricted-boost-config
  restricted-boost-function
  restricted-boost-iterator
  restricted-boost-smart_ptr
  restricted-boost-static_assert
  restricted-boost-thread
  restricted-boost-type_traits
)
target_sources(restricted-boost-locale PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/locale/src/posix/codecvt.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/locale/src/posix/collate.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/locale/src/posix/converter.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/locale/src/posix/numeric.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/locale/src/posix/posix_backend.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/locale/src/encoding/codepage.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/locale/src/icu/boundary.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/locale/src/icu/codecvt.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/locale/src/icu/collator.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/locale/src/icu/conversion.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/locale/src/icu/date_time.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/locale/src/icu/formatter.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/locale/src/icu/icu_backend.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/locale/src/icu/numeric.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/locale/src/icu/time_zone.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/locale/src/shared/date_time.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/locale/src/shared/format.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/locale/src/shared/formatting.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/locale/src/shared/generator.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/locale/src/shared/ids.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/locale/src/shared/localization_backend.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/locale/src/shared/message.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/locale/src/shared/mo_lambda.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/locale/src/std/codecvt.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/locale/src/std/collate.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/locale/src/std/converter.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/locale/src/std/numeric.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/locale/src/std/std_backend.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/locale/src/util/codecvt_converter.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/locale/src/util/default_locale.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/locale/src/util/gregorian.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/locale/src/util/info.cpp
  ${CMAKE_SOURCE_DIR}/contrib/restricted/boost/locale/src/util/locale_data.cpp
)
