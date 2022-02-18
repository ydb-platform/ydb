add_executable(validation)
target_link_libraries(validation PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-cpuid_check
  contrib-libs-protoc
  api-protos-annotations
)
target_sources(validation PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/grpc_services/validation/helpers.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/grpc_services/validation/main.cpp
)
target_link_flags(validation
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
vcs_info(validation)
