add_library(cpp-deprecated-accessors)
target_link_libraries(cpp-deprecated-accessors PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(cpp-deprecated-accessors PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/deprecated/accessors/accessors.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/deprecated/accessors/accessors_impl.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/deprecated/accessors/memory_traits.cpp
)
