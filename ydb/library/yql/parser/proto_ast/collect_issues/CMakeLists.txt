add_library(parser-proto_ast-collect_issues INTERFACE)
target_link_libraries(parser-proto_ast-collect_issues INTERFACE
  contrib-libs-cxxsupp
  yutil
  yql-public-issue
  yql-parser-proto_ast
)
