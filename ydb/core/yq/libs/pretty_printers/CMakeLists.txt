add_library(yq-libs-pretty_printers)
target_compile_options(yq-libs-pretty_printers PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(yq-libs-pretty_printers PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
  libs-graph_params-proto
  ydb-library-protobuf_printer
  library-yql-minikql
  dq-api-protos
)
target_sources(yq-libs-pretty_printers PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/pretty_printers/graph_params_printer.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/pretty_printers/minikql_program_printer.cpp
)
