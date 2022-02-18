add_library(yql-parser-proto_ast)
target_link_libraries(yql-parser-proto_ast PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-antlr3_cpp_runtime
  contrib-libs-protobuf
)
target_sources(yql-parser-proto_ast PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/parser/proto_ast/proto_ast.cpp
)
