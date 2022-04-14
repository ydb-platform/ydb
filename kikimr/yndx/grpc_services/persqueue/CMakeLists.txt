
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(yndx-grpc_services-persqueue)
target_link_libraries(yndx-grpc_services-persqueue PUBLIC
  contrib-libs-cxxsupp
  yutil
  api-grpc-yndx
  api-protos-yndx
  yndx-persqueue-read_batch_converter
  ydb-core-base
  core-client-server
  ydb-core-grpc_services
  core-mind-address_classification
  ydb-core-persqueue
  core-persqueue-events
  core-persqueue-writer
  ydb-core-protos
  ydb-library-aclib
  library-persqueue-topic_parser
  services-lib-actors
  services-lib-sharding
  ydb-services-persqueue_v1
)
target_sources(yndx-grpc_services-persqueue PRIVATE
  ${CMAKE_SOURCE_DIR}/kikimr/yndx/grpc_services/persqueue/grpc_pq_clusters_updater_actor.cpp
  ${CMAKE_SOURCE_DIR}/kikimr/yndx/grpc_services/persqueue/grpc_pq_read.cpp
  ${CMAKE_SOURCE_DIR}/kikimr/yndx/grpc_services/persqueue/grpc_pq_read_actor.cpp
  ${CMAKE_SOURCE_DIR}/kikimr/yndx/grpc_services/persqueue/grpc_pq_write.cpp
  ${CMAKE_SOURCE_DIR}/kikimr/yndx/grpc_services/persqueue/grpc_pq_write_actor.cpp
  ${CMAKE_SOURCE_DIR}/kikimr/yndx/grpc_services/persqueue/persqueue.cpp
)
