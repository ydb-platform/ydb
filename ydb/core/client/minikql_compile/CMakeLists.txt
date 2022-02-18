add_library(core-client-minikql_compile)
target_compile_options(core-client-minikql_compile PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(core-client-minikql_compile PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  cpp-threading-future
  ydb-core-base
  ydb-core-engine
  core-kqp-provider
  ydb-core-scheme
  library-yql-ast
  library-yql-core
  library-yql-minikql
  providers-common-mkql
)
target_sources(core-client-minikql_compile PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/client/minikql_compile/compile_context.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/client/minikql_compile/compile_result.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/client/minikql_compile/db_key_resolver.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/client/minikql_compile/mkql_compile_service.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/client/minikql_compile/yql_expr_minikql.cpp
)
