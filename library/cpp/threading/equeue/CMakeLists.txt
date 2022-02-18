add_library(cpp-threading-equeue)
target_link_libraries(cpp-threading-equeue PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(cpp-threading-equeue PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/threading/equeue/equeue.cpp
)
