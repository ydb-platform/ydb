
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(dq-actors-transform)
target_compile_options(dq-actors-transform PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(dq-actors-transform PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  yql-dq-runtime
  yql-dq-proto
)
target_sources(dq-actors-transform PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/dq/actors/transform/dq_transform_actor_factory.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/dq/actors/transform/dq_transform_events.cpp
)
