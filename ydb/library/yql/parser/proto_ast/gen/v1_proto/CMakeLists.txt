
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(proto_ast-gen-v1_proto)
target_link_libraries(proto_ast-gen-v1_proto PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
)
target_proto_messages(proto_ast-gen-v1_proto PRIVATE
  ${CMAKE_BINARY_DIR}/ydb/library/yql/parser/proto_ast/gen/v1_proto/SQLv1Parser.proto
)
run_antlr(
  OUTPUT
  ${CMAKE_BINARY_DIR}/ydb/library/yql/parser/proto_ast/gen/v1_proto/SQLv1Parser.proto
  DEPENDS
  ${CMAKE_BINARY_DIR}/ydb/library/yql/parser/proto_ast/gen/v1_proto/SQLv1.g
  ${CMAKE_BINARY_DIR}/ydb/library/yql/parser/proto_ast/gen/v1_proto/org/antlr/codegen/templates/protobuf/protobuf.stg
  WORKING_DIRECTORY
  ${CMAKE_BINARY_DIR}/ydb/library/yql/parser/proto_ast/gen/v1_proto
  ANTLER_ARGS
  ${CMAKE_BINARY_DIR}/ydb/library/yql/parser/proto_ast/gen/v1_proto/SQLv1.g
  -lib
  .
  -fo
  ${CMAKE_BINARY_DIR}/ydb/library/yql/parser/proto_ast/gen/v1_proto
  -language
  protobuf
)
set(GRAMMAR_STRING_CORE_SINGLE "~(QUOTE_SINGLE | BACKSLASH) | (BACKSLASH .)")
set(GRAMMAR_STRING_CORE_DOUBLE "~(QUOTE_DOUBLE | BACKSLASH) | (BACKSLASH .)")
set(GRAMMAR_MULTILINE_COMMENT_CORE       ".")

configure_file(
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/sql/v1/SQLv1.g.in
  ${CMAKE_BINARY_DIR}/ydb/library/yql/parser/proto_ast/gen/v1_proto/SQLv1.g
)


