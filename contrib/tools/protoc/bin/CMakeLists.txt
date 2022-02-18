add_executable(protoc)
target_link_libraries(protoc PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-cpuid_check
  contrib-libs-protoc
)
target_sources(protoc PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/protoc/src/google/protobuf/compiler/main.cc
)
target_link_flags(protoc
  PUBLIC
  -lpthread
  -lrt
  -ldl
)
vcs_info(protoc)
