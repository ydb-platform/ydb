add_library(yql-dq-runtime)
target_compile_options(yql-dq-runtime PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(yql-dq-runtime PUBLIC
  contrib-libs-cxxsupp
  yutil
  libs-apache-arrow
  ydb-core-util
  ydb-library-mkql_proto
  yql-minikql-comp_nodes
  yql-minikql-computation
  yql-public-udf
  dq-actors-protos
  yql-dq-common
  yql-dq-expr_nodes
  yql-dq-type_ann
  tools-enum_parser-enum_serialization_runtime
)
target_sources(yql-dq-runtime PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/dq/runtime/dq_arrow_helpers.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/dq/runtime/dq_columns_resolve.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/dq/runtime/dq_compute.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/dq/runtime/dq_input_channel.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/dq/runtime/dq_input_producer.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/dq/runtime/dq_output_channel.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/dq/runtime/dq_output_consumer.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/dq/runtime/dq_source.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/dq/runtime/dq_sink.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/dq/runtime/dq_tasks_runner.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/dq/runtime/dq_transport.cpp
)
generate_enum_serilization(yql-dq-runtime
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/dq/runtime/dq_tasks_runner.h
  INCLUDE_HEADERS
  ydb/library/yql/dq/runtime/dq_tasks_runner.h
)
