add_executable(gencrc)
target_link_libraries(gencrc PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-cpuid_check
)
target_sources(gencrc PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/digest/old_crc/gencrc/main.cpp
)
target_link_flags(gencrc
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
vcs_info(gencrc)
