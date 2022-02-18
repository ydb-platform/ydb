add_library(core-kqp-compile)
target_compile_options(core-kqp-compile PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(core-kqp-compile PUBLIC
  contrib-libs-cxxsupp
  yutil
  ydb-core-formats
  core-kqp-common
  ydb-core-protos
  ydb-library-mkql_proto
)
target_sources(core-kqp-compile PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/compile/kqp_compile.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/compile/kqp_mkql_compiler.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/compile/kqp_olap_compiler.cpp
)
