add_library(sql-v0-lexer)
target_link_libraries(sql-v0-lexer PUBLIC
  contrib-libs-cxxsupp
  yutil
  core-issue-protos
  yql-parser-proto_ast
  proto_ast-gen-v0
)
target_sources(sql-v0-lexer PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/sql/v0/lexer/lexer.cpp
)
