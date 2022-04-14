
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.


find_package(OpenSSL REQUIRED)

add_library(ticket_parser)
target_link_libraries(ticket_parser PUBLIC
  contrib-libs-cxxsupp
  yutil
  OpenSSL::OpenSSL
  cpp-openssl-init
)
target_sources(ticket_parser PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/tvmauth/src/rw/keys.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/tvmauth/src/rw/rw_asn1.c
  ${CMAKE_SOURCE_DIR}/library/cpp/tvmauth/src/rw/rw_key.c
  ${CMAKE_SOURCE_DIR}/library/cpp/tvmauth/src/rw/rw_lib.c
  ${CMAKE_SOURCE_DIR}/library/cpp/tvmauth/src/rw/rw_ossl.c
  ${CMAKE_SOURCE_DIR}/library/cpp/tvmauth/src/rw/rw_pss.c
  ${CMAKE_SOURCE_DIR}/library/cpp/tvmauth/src/rw/rw_pss_sign.c
  ${CMAKE_SOURCE_DIR}/library/cpp/tvmauth/src/rw/rw_sign.c
)
