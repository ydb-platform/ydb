add_library(cpp-containers-str_map)
target_link_libraries(cpp-containers-str_map PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(cpp-containers-str_map PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/containers/str_map/str_map.cpp
)
