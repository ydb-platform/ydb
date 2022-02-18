add_executable(sqs)
target_link_libraries(sqs PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-cpuid_check
  library-cpp-getopt
  ymq-client-cpp
  core-ymq-proto
)
target_sources(sqs PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/ymq/client/bin/main.cpp
)
target_link_flags(sqs
  PUBLIC
  -lpthread
  -lrt
  -ldl
)
vcs_info(sqs)
