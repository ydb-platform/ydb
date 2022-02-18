add_library(library-persqueue-tests)
target_link_libraries(library-persqueue-tests PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-testing-unittest
  cpp-http-io
  library-cpp-json
)
target_sources(library-persqueue-tests PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/persqueue/tests/counters.cpp
)
