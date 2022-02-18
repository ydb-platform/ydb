add_library(core-sys_view-service)
target_link_libraries(core-sys_view-service PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  ydb-core-base
  ydb-core-protos
  library-aclib-protos
)
target_sources(core-sys_view-service PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/sys_view/service/db_counters.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/sys_view/service/ext_counters.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/sys_view/service/query_interval.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/sys_view/service/sysview_service.cpp
)
