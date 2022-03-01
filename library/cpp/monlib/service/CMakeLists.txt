
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(cpp-monlib-service)
target_link_libraries(cpp-monlib-service PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-string_utils-base64
  contrib-libs-protobuf
  cpp-coroutine-engine
  cpp-coroutine-listener
  cpp-http-fetch
  cpp-http-server
  cpp-http-io
  library-cpp-logger
  cpp-malloc-api
  library-cpp-svnversion
  library-cpp-uri
  library-cpp-cgiparam
)
target_sources(cpp-monlib-service PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/monlib/service/monservice.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/monlib/service/mon_service_http_request.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/monlib/service/service.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/monlib/service/format.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/monlib/service/auth.cpp
)
