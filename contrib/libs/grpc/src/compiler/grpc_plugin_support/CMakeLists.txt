add_library(src-compiler-grpc_plugin_support)
target_compile_options(src-compiler-grpc_plugin_support PRIVATE
  -DGRPC_POSIX_FORK_ALLOW_PTHREAD_ATFORK=1
)
target_include_directories(src-compiler-grpc_plugin_support PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/include
)
target_include_directories(src-compiler-grpc_plugin_support PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/include
  ${CMAKE_BINARY_DIR}/contrib/libs/grpc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc
)
target_link_libraries(src-compiler-grpc_plugin_support PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
  contrib-libs-protoc
)
target_sources(src-compiler-grpc_plugin_support PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/compiler/cpp_generator.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/compiler/python_generator.cc
)
