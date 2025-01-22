_ydb_sdk_add_library(client-ydb_topic-codecs)

target_link_libraries(client-ydb_topic-codecs PUBLIC
  yutil
  streams-zstd
  api-grpc-draft
  api-grpc
  api-protos
)

target_sources(client-ydb_topic-codecs PRIVATE
  codecs.cpp
)

generate_enum_serilization(client-ydb_topic-codecs
  ${YDB_SDK_SOURCE_DIR}/include/ydb-cpp-sdk/client/topic/codecs.h
  INCLUDE_HEADERS
  include/ydb-cpp-sdk/client/topic/codecs.h
)

_ydb_sdk_install_targets(TARGETS client-ydb_topic-codecs)
