add_library(impl-ydb_internal-make_request)
target_link_libraries(impl-ydb_internal-make_request PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
  api-protos
)
target_sources(impl-ydb_internal-make_request PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/impl/ydb_internal/make_request/make.cpp
)
