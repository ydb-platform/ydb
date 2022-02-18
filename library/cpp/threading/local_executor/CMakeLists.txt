add_library(cpp-threading-local_executor)
target_link_libraries(cpp-threading-local_executor PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-tbb
  cpp-threading-future
)
target_sources(cpp-threading-local_executor PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/threading/local_executor/local_executor.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/threading/local_executor/tbb_local_executor.cpp
)
