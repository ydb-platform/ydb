add_library(restricted-aws-aws-c-event-stream)
target_include_directories(restricted-aws-aws-c-event-stream PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/aws/aws-c-event-stream/include
)
target_include_directories(restricted-aws-aws-c-event-stream PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/aws/aws-c-event-stream/include
)
target_link_libraries(restricted-aws-aws-c-event-stream PUBLIC
  restricted-aws-aws-c-common
  restricted-aws-aws-c-io
  restricted-aws-aws-checksums
)
target_sources(restricted-aws-aws-c-event-stream PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/aws/aws-c-event-stream/source/event_stream.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/aws/aws-c-event-stream/source/event_stream_channel_handler.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/aws/aws-c-event-stream/source/event_stream_rpc.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/aws/aws-c-event-stream/source/event_stream_rpc_client.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/aws/aws-c-event-stream/source/event_stream_rpc_server.c
)
