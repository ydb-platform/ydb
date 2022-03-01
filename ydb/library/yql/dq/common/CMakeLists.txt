
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(yql-dq-common)
target_link_libraries(yql-dq-common PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  library-mkql_proto-protos
)
target_sources(yql-dq-common PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/dq/common/dq_common.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/dq/common/dq_value.cpp
)
