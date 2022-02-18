add_library(grpc-third_party-upb)
target_compile_options(grpc-third_party-upb PRIVATE
  -DGRPC_POSIX_FORK_ALLOW_PTHREAD_ATFORK=1
)
target_include_directories(grpc-third_party-upb PRIVATE
  ${CMAKE_BINARY_DIR}/contrib/libs/grpc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/third_party/upb
)
target_sources(grpc-third_party-upb PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/third_party/upb/upb/decode.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/third_party/upb/upb/encode.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/third_party/upb/upb/msg.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/third_party/upb/upb/port.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/third_party/upb/upb/table.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/third_party/upb/upb/upb.c
)
