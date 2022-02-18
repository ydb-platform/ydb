add_library(ydb-core-mon_alloc)
target_link_libraries(ydb-core-mon_alloc PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  cpp-actors-prof
  cpp-html-pcdata
  cpp-lfalloc-alloc_profiler
  cpp-lfalloc-dbg_info
  cpp-malloc-api
  monlib-service-pages
  cpp-ytalloc-api
  ydb-core-base
  ydb-core-control
)
target_sources(ydb-core-mon_alloc PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/mon_alloc/monitor.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/mon_alloc/profiler.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/mon_alloc/stats.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/mon_alloc/tcmalloc_null.cpp
)
