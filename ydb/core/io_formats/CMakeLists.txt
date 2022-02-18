add_library(ydb-core-io_formats)
target_compile_options(ydb-core-io_formats PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(ydb-core-io_formats PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-double-conversion
  cpp-string_utils-quote
  ydb-core-formats
  ydb-core-scheme
  ydb-library-binary_json
  ydb-library-dynumber
  yql-minikql-dom
  yql-public-decimal
  yql-public-udf
  library-yql-utils
  public-lib-scheme_types
)
target_sources(ydb-core-io_formats PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/io_formats/csv_ydb_dump.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/io_formats/csv_arrow.cpp
)
