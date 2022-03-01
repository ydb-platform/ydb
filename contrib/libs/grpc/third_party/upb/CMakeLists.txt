
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



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
