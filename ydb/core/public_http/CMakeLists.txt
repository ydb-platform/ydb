
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(ydb-core-public_http)
target_compile_options(ydb-core-public_http PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(ydb-core-public_http PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  cpp-actors-http
  cpp-protobuf-json
  ydb-core-base
  ydb-core-http_proxy
  core-grpc_services-local_rpc
  ydb-core-protos
  core-viewer-json
  core-viewer-protos
  yq-libs-result_formatter
  yql-public-issue
)
target_sources(ydb-core-public_http PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/public_http/http_req.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/public_http/http_router.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/public_http/http_service.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/public_http/grpc_request_context_wrapper.cpp
)
