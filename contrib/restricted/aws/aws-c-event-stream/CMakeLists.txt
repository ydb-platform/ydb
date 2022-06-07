
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(restricted-aws-aws-c-event-stream)
target_include_directories(restricted-aws-aws-c-event-stream PUBLIC
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
