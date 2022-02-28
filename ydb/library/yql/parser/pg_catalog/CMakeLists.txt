add_library(yql-parser-pg_catalog)
target_link_libraries(yql-parser-pg_catalog PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-resource
)
target_sources(yql-parser-pg_catalog PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/parser/pg_catalog/catalog.cpp
)

add_global_library_for(yql-parser-pg_catalog.global yql-parser-pg_catalog)
target_link_libraries(yql-parser-pg_catalog.global PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-resource
)
target_sources(yql-parser-pg_catalog.global PRIVATE
  ${CMAKE_BINARY_DIR}/ydb/library/yql/parser/pg_catalog/9bfc0ed24fcfb8e096ec43a8430d7185.cpp
  ${CMAKE_BINARY_DIR}/ydb/library/yql/parser/pg_catalog/03528211297024b3dbd6ad7bd8089f55.cpp
  ${CMAKE_BINARY_DIR}/ydb/library/yql/parser/pg_catalog/4bdffcfe6e5c28b12f6f249d751c6cc5.cpp
  ${CMAKE_BINARY_DIR}/ydb/library/yql/parser/pg_catalog/13089390d5c6426a6f6417928fe4445c.cpp
)
resources(yql-parser-pg_catalog.global
  ${CMAKE_BINARY_DIR}/ydb/library/yql/parser/pg_catalog/9bfc0ed24fcfb8e096ec43a8430d7185.cpp
  INPUTS
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/parser/pg_catalog/pg_operator.dat
  KEYS
  pg_operator.dat
)
resources(yql-parser-pg_catalog.global
  ${CMAKE_BINARY_DIR}/ydb/library/yql/parser/pg_catalog/03528211297024b3dbd6ad7bd8089f55.cpp
  INPUTS
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/parser/pg_catalog/pg_proc.dat
  KEYS
  pg_proc.dat
)
resources(yql-parser-pg_catalog.global
  ${CMAKE_BINARY_DIR}/ydb/library/yql/parser/pg_catalog/4bdffcfe6e5c28b12f6f249d751c6cc5.cpp
  INPUTS
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/parser/pg_catalog/pg_type.dat
  KEYS
  pg_type.dat
)
resources(yql-parser-pg_catalog.global
  ${CMAKE_BINARY_DIR}/ydb/library/yql/parser/pg_catalog/13089390d5c6426a6f6417928fe4445c.cpp
  INPUTS
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/parser/pg_catalog/pg_cast.dat
  KEYS
  pg_cast.dat
)
