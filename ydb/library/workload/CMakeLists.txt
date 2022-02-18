add_library(ydb-library-workload)
target_link_libraries(ydb-library-workload PUBLIC
  contrib-libs-cxxsupp
  yutil
  api-protos
)
target_sources(ydb-library-workload PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/workload/stock_workload.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/workload/workload_factory.cpp
)
