add_library(cpp-threading-poor_man_openmp)
target_link_libraries(cpp-threading-poor_man_openmp PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(cpp-threading-poor_man_openmp PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/threading/poor_man_openmp/thread_helper.cpp
)
