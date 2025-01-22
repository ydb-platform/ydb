_ydb_sdk_add_library(client-ydb_persqueue_public-codecs INTERFACE)

target_link_libraries(client-ydb_persqueue_public-codecs INTERFACE
  client-ydb_topic-codecs
)
