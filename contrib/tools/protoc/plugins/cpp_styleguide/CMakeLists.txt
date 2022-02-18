add_executable(cpp_styleguide)
target_link_libraries(cpp_styleguide PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-cpuid_check
  contrib-libs-protoc
)
target_sources(cpp_styleguide PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/tools/protoc/plugins/cpp_styleguide/cpp_styleguide.cpp
)
target_link_flags(cpp_styleguide
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
vcs_info(cpp_styleguide)
