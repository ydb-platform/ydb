add_library(core-sys_view-common)
target_link_libraries(core-sys_view-common PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  ydb-core-base
  ydb-core-protos
  ydb-core-tablet_flat
)
target_sources(core-sys_view-common PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/sys_view/common/schema.cpp
)
