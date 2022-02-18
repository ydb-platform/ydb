add_library(cpp-containers-top_keeper)
target_link_libraries(cpp-containers-top_keeper PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(cpp-containers-top_keeper PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/containers/top_keeper/top_keeper.cpp
)
