add_executable(enum_parser)
target_link_libraries(enum_parser PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-cpuid_check
  cpp-getopt-small
  tools-enum_parser-parse_enum
)
target_sources(enum_parser PRIVATE
  ${CMAKE_SOURCE_DIR}/tools/enum_parser/enum_parser/main.cpp
)
target_link_flags(enum_parser
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
vcs_info(enum_parser)
