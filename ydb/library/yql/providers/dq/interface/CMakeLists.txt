add_library(providers-dq-interface)
target_compile_options(providers-dq-interface PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(providers-dq-interface PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
  library-cpp-yson
  library-yql-ast
  library-yql-core
)
target_sources(providers-dq-interface PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/dq/interface/yql_dq_integration.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/dq/interface/yql_dq_task_preprocessor.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/dq/interface/yql_dq_task_transform.cpp
)
