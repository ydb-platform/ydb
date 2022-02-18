add_executable(grpc_cpp)
target_compile_options(grpc_cpp PRIVATE
  -DGRPC_USE_ABSL=0
)
target_include_directories(grpc_cpp PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/include
)
target_link_libraries(grpc_cpp PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-cpuid_check
  src-compiler-grpc_plugin_support
)
target_sources(grpc_cpp PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/compiler/cpp_plugin.cc
)
target_link_flags(grpc_cpp
  PUBLIC
  -ldl
  -lrt
  -Wl,--no-as-needed
  -fPIC
  -Wl,--gdb-index
  -fPIC
  -fuse-ld=$(LLD_ROOT-sbr:2283360772)/ld
  -Wl,--no-rosegment
  -lpthread
  -lrt
  -ldl
)
vcs_info(grpc_cpp)
