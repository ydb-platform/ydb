add_library(yql-parser-lexer_common)
target_link_libraries(yql-parser-lexer_common PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(yql-parser-lexer_common PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/parser/lexer_common/tokens.cpp
)
