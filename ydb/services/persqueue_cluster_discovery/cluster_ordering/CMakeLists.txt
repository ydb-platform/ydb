add_library(services-persqueue_cluster_discovery-cluster_ordering)
target_link_libraries(services-persqueue_cluster_discovery-cluster_ordering PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(services-persqueue_cluster_discovery-cluster_ordering PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/services/persqueue_cluster_discovery/cluster_ordering/weighed_ordering.cpp
)
