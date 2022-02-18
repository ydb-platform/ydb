add_library(core-blobstorage-lwtrace_probes)
target_link_libraries(core-blobstorage-lwtrace_probes PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-grpc
  cpp-lwtrace-protos
  ydb-core-base
  ydb-core-protos
)
target_sources(core-blobstorage-lwtrace_probes PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/lwtrace_probes/blobstorage_probes.cpp
)
