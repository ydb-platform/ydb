
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.


add_subdirectory(common)
add_subdirectory(compile)
add_subdirectory(compute_actor)
add_subdirectory(counters)
add_subdirectory(executer)
add_subdirectory(expr_nodes)
add_subdirectory(host)
add_subdirectory(node)
add_subdirectory(opt)
add_subdirectory(prepare)
add_subdirectory(provider)
add_subdirectory(proxy)
add_subdirectory(rm)
add_subdirectory(runtime)
add_subdirectory(ut)

add_library(ydb-core-kqp)
target_compile_options(ydb-core-kqp PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(ydb-core-kqp PUBLIC
  contrib-libs-cxxsupp
  yutil
  libs-apache-arrow
  cpp-actors-core
  cpp-actors-helpers
  cpp-digest-md5
  cpp-string_utils-base64
  ydb-core-actorlib_impl
  ydb-core-base
  core-client-minikql_compile
  ydb-core-engine
  ydb-core-formats
  core-grpc_services-local_rpc
  core-kqp-common
  core-kqp-compute_actor
  core-kqp-counters
  core-kqp-executer
  core-kqp-host
  core-kqp-node
  core-kqp-proxy
  core-kqp-rm
  core-kqp-runtime
  ydb-core-protos
  core-sys_view-service
  ydb-core-util
  ydb-core-ydb_convert
  ydb-library-aclib
  core-services-mounts
  yql-public-issue
  yql-utils-actor_log
  yql-utils-log
  api-protos
  public-lib-base
  public-lib-operation_id
  library-cpp-resource
)
target_sources(ydb-core-kqp PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/kqp_compile_actor.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/kqp_compile_request.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/kqp_compile_service.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/kqp_shutdown_controller.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/kqp_ic_gateway.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/kqp_metadata_loader.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/kqp_response.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/kqp_worker_actor.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/kqp_session_actor.cpp
)

add_global_library_for(ydb-core-kqp.global ydb-core-kqp)
target_compile_options(ydb-core-kqp.global PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(ydb-core-kqp.global PUBLIC
  contrib-libs-cxxsupp
  yutil
  libs-apache-arrow
  cpp-actors-core
  cpp-actors-helpers
  cpp-digest-md5
  cpp-string_utils-base64
  ydb-core-actorlib_impl
  ydb-core-base
  core-client-minikql_compile
  ydb-core-engine
  ydb-core-formats
  core-grpc_services-local_rpc
  core-kqp-common
  core-kqp-compute_actor
  core-kqp-counters
  core-kqp-executer
  core-kqp-host
  core-kqp-node
  core-kqp-proxy
  core-kqp-rm
  core-kqp-runtime
  ydb-core-protos
  core-sys_view-service
  ydb-core-util
  ydb-core-ydb_convert
  ydb-library-aclib
  core-services-mounts
  yql-public-issue
  yql-utils-actor_log
  yql-utils-log
  api-protos
  public-lib-base
  public-lib-operation_id
  library-cpp-resource
)
target_sources(ydb-core-kqp.global PRIVATE
  ${CMAKE_BINARY_DIR}/ydb/core/kqp/da10233abc65c3f178c7da0ce0d7a4ea.cpp
)
resources(ydb-core-kqp.global
  ${CMAKE_BINARY_DIR}/ydb/core/kqp/da10233abc65c3f178c7da0ce0d7a4ea.cpp
  INPUTS
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/kqp_default_settings.txt
  KEYS
  kqp_default_settings.txt
)
