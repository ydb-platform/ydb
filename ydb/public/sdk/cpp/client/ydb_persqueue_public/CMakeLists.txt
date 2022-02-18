add_library(cpp-client-ydb_persqueue_public INTERFACE)
target_link_libraries(cpp-client-ydb_persqueue_public INTERFACE
  contrib-libs-cxxsupp
  yutil
  cpp-client-ydb_persqueue_core
  client-ydb_persqueue_public-codecs
)
