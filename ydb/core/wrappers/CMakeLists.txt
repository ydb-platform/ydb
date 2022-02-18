add_library(ydb-core-wrappers)
target_link_libraries(ydb-core-wrappers PUBLIC
  contrib-libs-cxxsupp
  yutil
  libs-aws-sdk-cpp-aws-cpp-sdk-s3
  contrib-libs-curl
  cpp-actors-core
  ydb-core-base
  ydb-core-protos
  core-wrappers-ut_helpers
)
target_sources(ydb-core-wrappers PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/wrappers/s3_out.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/wrappers/s3_wrapper.cpp
)
