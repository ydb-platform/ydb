
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(yndx-persqueue-msgbus_server)
target_link_libraries(yndx-persqueue-msgbus_server PUBLIC
  contrib-libs-cxxsupp
  yutil
  core-client-server
  yndx-grpc_services-persqueue
)
target_sources(yndx-persqueue-msgbus_server PRIVATE
  ${CMAKE_SOURCE_DIR}/kikimr/yndx/persqueue/msgbus_server/read_session_info.cpp
)
