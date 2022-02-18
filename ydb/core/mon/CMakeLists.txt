add_library(ydb-core-mon)
target_link_libraries(ydb-core-mon PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  cpp-lwtrace-mon
  cpp-string_utils-url
  ydb-core-base
  ydb-library-aclib
)
target_sources(ydb-core-mon PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/mon/mon.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/mon/crossref.cpp
)
