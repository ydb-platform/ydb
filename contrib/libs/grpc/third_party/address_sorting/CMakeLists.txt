add_library(grpc-third_party-address_sorting)
target_compile_options(grpc-third_party-address_sorting PRIVATE
  -DGRPC_POSIX_FORK_ALLOW_PTHREAD_ATFORK=1
)
target_include_directories(grpc-third_party-address_sorting PRIVATE
  ${CMAKE_BINARY_DIR}/contrib/libs/grpc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/third_party/address_sorting/include
)
target_sources(grpc-third_party-address_sorting PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/third_party/address_sorting/address_sorting.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/third_party/address_sorting/address_sorting_posix.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/third_party/address_sorting/address_sorting_windows.c
)
