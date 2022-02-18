add_library(ydb-core-health_check)
target_link_libraries(ydb-core-health_check PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  ydb-core-base
  core-blobstorage-base
  ydb-library-aclib
  api-protos
  public-issue-protos
)
target_sources(ydb-core-health_check PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/health_check/health_check.cpp
)
