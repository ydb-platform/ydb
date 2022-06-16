
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(cpp-grpc-client)
target_link_libraries(cpp-grpc-client PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-grpc
  cpp-deprecated-atomic
)
target_sources(cpp-grpc-client PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/grpc/client/grpc_client_low.cpp
)
