add_library(ydb-services-persqueue_cluster_discovery)
target_link_libraries(ydb-services-persqueue_cluster_discovery PUBLIC
  contrib-libs-cxxsupp
  yutil
  ydb-core-base
  core-client-server
  ydb-core-grpc_services
  core-mind-address_classification
  ydb-core-mon
  ydb-core-persqueue
  ydb-core-protos
  ydb-core-util
  api-grpc
  api-grpc-draft
  api-protos
  services-persqueue_cluster_discovery-cluster_ordering
)
target_sources(ydb-services-persqueue_cluster_discovery PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/services/persqueue_cluster_discovery/cluster_discovery_service.cpp
  ${CMAKE_SOURCE_DIR}/ydb/services/persqueue_cluster_discovery/cluster_discovery_worker.cpp
  ${CMAKE_SOURCE_DIR}/ydb/services/persqueue_cluster_discovery/counters.cpp
  ${CMAKE_SOURCE_DIR}/ydb/services/persqueue_cluster_discovery/grpc_service.cpp
)
