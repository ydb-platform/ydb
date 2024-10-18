add_subdirectory(impl)

_ydb_sdk_add_library(client-ydb_federated_topic)

target_link_libraries(client-ydb_federated_topic PUBLIC
  yutil
  enum_serialization_runtime
  client-ydb_topic
  client-ydb_federated_topic-impl
)

generate_enum_serilization(client-ydb_federated_topic
  ${YDB_SDK_SOURCE_DIR}/include/ydb-cpp-sdk/client/federated_topic/federated_topic.h
  INCLUDE_HEADERS
  include/ydb-cpp-sdk/client/federated_topic/federated_topic.h
)

_ydb_sdk_make_client_component(FederatedTopic client-ydb_federated_topic)
