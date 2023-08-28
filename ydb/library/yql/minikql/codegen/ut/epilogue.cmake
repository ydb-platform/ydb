llvm_compile_cxx(ydb-library-yql-minikql-codegen-ut
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/minikql/codegen/ut/128_bit.ll
  ${CMAKE_BINARY_DIR}/ydb/library/yql/minikql/codegen/ut/128_bit.ll.bc
  ${CLANGPLUSPLUS}
  -Wno-unknown-warning-option
  -emit-llvm
)
