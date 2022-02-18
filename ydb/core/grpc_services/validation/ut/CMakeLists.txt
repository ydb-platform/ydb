add_executable(ydb-core-grpc_services-validation-ut)
target_include_directories(ydb-core-grpc_services-validation-ut PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/grpc_services/validation
)
target_link_libraries(ydb-core-grpc_services-validation-ut PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-cpuid_check
  cpp-testing-unittest_main
  cpp-testing-unittest
  validation-ut-protos
)
target_sources(ydb-core-grpc_services-validation-ut PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/grpc_services/validation/ut.cpp
)
add_test(
  NAME
  ydb-core-grpc_services-validation-ut
  COMMAND
  ydb-core-grpc_services-validation-ut
)
target_link_flags(ydb-core-grpc_services-validation-ut
  PUBLIC
  -lpthread
  -lrt
  -ldl
)
vcs_info(ydb-core-grpc_services-validation-ut)
