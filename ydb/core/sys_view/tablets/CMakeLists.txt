add_library(core-sys_view-tablets)
target_compile_options(core-sys_view-tablets PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(core-sys_view-tablets PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  ydb-core-base
  core-kqp-runtime
  core-sys_view-common
)
target_sources(core-sys_view-tablets PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/sys_view/tablets/tablets.cpp
)
