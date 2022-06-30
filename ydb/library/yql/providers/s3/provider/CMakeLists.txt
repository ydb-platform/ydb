
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(providers-s3-provider)
target_compile_options(providers-s3-provider PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(providers-s3-provider PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-re2
  library-cpp-json
  library-cpp-random_provider
  library-cpp-retry
  library-cpp-time_provider
  cpp-xml-document
  library-yql-ast
  library-yql-minikql
  yql-minikql-comp_nodes
  yql-minikql-computation
  providers-common-structured_token
  common-token_accessor-client
  library-yql-core
  yql-core-type_ann
  yql-dq-expr_nodes
  providers-common-config
  providers-common-dq
  providers-common-http_gateway
  providers-common-mkql
  providers-common-proto
  providers-common-provider
  common-schema-expr
  providers-common-transform
  providers-dq-common
  providers-dq-expr_nodes
  providers-dq-interface
  providers-result-expr_nodes
  providers-s3-expr_nodes
  providers-s3-proto
  library-yql-utils
)
target_sources(providers-s3-provider PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/s3/provider/yql_s3_datasink.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/s3/provider/yql_s3_datasink_execution.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/s3/provider/yql_s3_datasink_type_ann.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/s3/provider/yql_s3_datasource.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/s3/provider/yql_s3_datasource_type_ann.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/s3/provider/yql_s3_dq_integration.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/s3/provider/yql_s3_exec.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/s3/provider/yql_s3_io_discovery.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/s3/provider/yql_s3_logical_opt.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/s3/provider/yql_s3_mkql_compiler.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/s3/provider/yql_s3_phy_opt.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/s3/provider/yql_s3_provider.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/s3/provider/yql_s3_provider_impl.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/s3/provider/yql_s3_settings.cpp
)
