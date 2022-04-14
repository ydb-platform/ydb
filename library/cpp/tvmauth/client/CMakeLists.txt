
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(cpp-tvmauth-client)
target_link_libraries(cpp-tvmauth-client PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-http-simple
  library-cpp-json
  cpp-openssl-crypto
  cpp-streams-brotli
  cpp-streams-zstd
  cpp-string_utils-quote
  library-cpp-tvmauth
  misc-retry_settings-v1
  tools-enum_parser-enum_serialization_runtime
)
target_sources(cpp-tvmauth-client PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/tvmauth/client/client_status.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/tvmauth/client/facade.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/tvmauth/client/logger.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/tvmauth/client/misc/api/roles_fetcher.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/tvmauth/client/misc/api/settings.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/tvmauth/client/misc/api/threaded_updater.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/tvmauth/client/misc/async_updater.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/tvmauth/client/misc/disk_cache.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/tvmauth/client/misc/last_error.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/tvmauth/client/misc/proc_info.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/tvmauth/client/misc/roles/decoder.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/tvmauth/client/misc/roles/entities_index.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/tvmauth/client/misc/roles/parser.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/tvmauth/client/misc/roles/roles.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/tvmauth/client/misc/threaded_updater.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/tvmauth/client/misc/tool/meta_info.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/tvmauth/client/misc/tool/roles_fetcher.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/tvmauth/client/misc/tool/settings.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/tvmauth/client/misc/tool/threaded_updater.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/tvmauth/client/misc/utils.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/tvmauth/client/mocked_updater.cpp
)
generate_enum_serilization(cpp-tvmauth-client
  ${CMAKE_SOURCE_DIR}/library/cpp/tvmauth/client/client_status.h
  INCLUDE_HEADERS
  library/cpp/tvmauth/client/client_status.h
)
generate_enum_serilization(cpp-tvmauth-client
  ${CMAKE_SOURCE_DIR}/library/cpp/tvmauth/client/misc/async_updater.h
  INCLUDE_HEADERS
  library/cpp/tvmauth/client/misc/async_updater.h
)
generate_enum_serilization(cpp-tvmauth-client
  ${CMAKE_SOURCE_DIR}/library/cpp/tvmauth/client/misc/last_error.h
  INCLUDE_HEADERS
  library/cpp/tvmauth/client/misc/last_error.h
)
