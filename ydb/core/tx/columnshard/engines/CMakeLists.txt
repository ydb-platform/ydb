add_library(tx-columnshard-engines)
target_compile_options(tx-columnshard-engines PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(tx-columnshard-engines PUBLIC
  contrib-libs-cxxsupp
  yutil
  libs-apache-arrow
  ydb-core-base
  ydb-core-formats
  ydb-core-protos
  ydb-core-scheme
  ydb-core-tablet
  ydb-core-tablet_flat
)
target_sources(tx-columnshard-engines PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/tx/columnshard/engines/column_engine_logs.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/tx/columnshard/engines/db_wrapper.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/tx/columnshard/engines/insert_table.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/tx/columnshard/engines/index_info.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/tx/columnshard/engines/indexed_read_data.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/tx/columnshard/engines/filter.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/tx/columnshard/engines/portion_info.cpp
)
