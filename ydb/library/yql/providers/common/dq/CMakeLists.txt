add_library(providers-common-dq)
target_link_libraries(providers-common-dq PUBLIC
  contrib-libs-cxxsupp
  yutil
  providers-dq-interface
)
target_sources(providers-common-dq PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/common/dq/yql_dq_integration_impl.cpp
)
