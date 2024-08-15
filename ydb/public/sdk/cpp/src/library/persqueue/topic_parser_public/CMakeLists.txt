_ydb_sdk_add_library(persqueue-topic_parser_public)

target_link_libraries(persqueue-topic_parser_public PUBLIC
  yutil
)

target_sources(persqueue-topic_parser_public PRIVATE
  topic_parser.cpp
)

_ydb_sdk_install_targets(TARGETS persqueue-topic_parser_public)
