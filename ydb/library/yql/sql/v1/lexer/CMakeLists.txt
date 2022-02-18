add_library(sql-v1-lexer)
target_link_libraries(sql-v1-lexer PUBLIC
  contrib-libs-cxxsupp
  yutil
  core-issue-protos
  yql-parser-proto_ast
  proto_ast-gen-v1
  proto_ast-gen-v1_ansi
  proto_ast-gen-v1_proto
)
target_sources(sql-v1-lexer PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/sql/v1/lexer/lexer.cpp
)
