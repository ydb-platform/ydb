add_executable(ydb)
target_link_libraries(ydb PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-cpuid_check
  commands
)
target_sources(ydb PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/apps/ydb/main.cpp
)
target_link_flags(ydb
  PUBLIC
  -lpthread
  -lrt
  -ldl
)
vcs_info(ydb)
