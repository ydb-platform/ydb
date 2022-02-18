add_library(yql-public-decimal)
target_link_libraries(yql-public-decimal PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(yql-public-decimal PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/public/decimal/yql_decimal.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/public/decimal/yql_decimal_serialize.cpp
)
