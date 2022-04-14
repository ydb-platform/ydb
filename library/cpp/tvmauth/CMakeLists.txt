
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(library-cpp-tvmauth)
target_link_libraries(library-cpp-tvmauth PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-string_utils-secret_string
  tvmauth-src-protos
  ticket_parser
  tools-enum_parser-enum_serialization_runtime
  library-cpp-resource
)
target_sources(library-cpp-tvmauth PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/tvmauth/deprecated/service_context.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/tvmauth/deprecated/user_context.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/tvmauth/src/parser.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/tvmauth/src/service_impl.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/tvmauth/src/service_ticket.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/tvmauth/src/status.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/tvmauth/src/unittest.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/tvmauth/src/user_impl.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/tvmauth/src/user_ticket.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/tvmauth/src/utils.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/tvmauth/src/version.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/tvmauth/utils.cpp
)
generate_enum_serilization(library-cpp-tvmauth
  ${CMAKE_SOURCE_DIR}/library/cpp/tvmauth/checked_user_ticket.h
  INCLUDE_HEADERS
  library/cpp/tvmauth/checked_user_ticket.h
)
generate_enum_serilization(library-cpp-tvmauth
  ${CMAKE_SOURCE_DIR}/library/cpp/tvmauth/ticket_status.h
  INCLUDE_HEADERS
  library/cpp/tvmauth/ticket_status.h
)

add_global_library_for(library-cpp-tvmauth.global library-cpp-tvmauth)
target_link_libraries(library-cpp-tvmauth.global PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-string_utils-secret_string
  tvmauth-src-protos
  ticket_parser
  tools-enum_parser-enum_serialization_runtime
  library-cpp-resource
)
target_sources(library-cpp-tvmauth.global PRIVATE
  ${CMAKE_BINARY_DIR}/library/cpp/tvmauth/0490e525f24df2be33c25916359673d0.cpp
)
resources(library-cpp-tvmauth.global
  ${CMAKE_BINARY_DIR}/library/cpp/tvmauth/0490e525f24df2be33c25916359673d0.cpp
  INPUTS
  ${CMAKE_SOURCE_DIR}/library/cpp/tvmauth/src/version
  KEYS
  /builtin/version
)
