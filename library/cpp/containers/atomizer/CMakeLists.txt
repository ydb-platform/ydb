add_library(cpp-containers-atomizer)
target_link_libraries(cpp-containers-atomizer PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-containers-str_map
)
target_sources(cpp-containers-atomizer PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/containers/atomizer/atomizer.cpp
)
