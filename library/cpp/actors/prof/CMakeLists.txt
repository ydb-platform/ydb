add_library(cpp-actors-prof)
target_link_libraries(cpp-actors-prof PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-charset
  cpp-containers-atomizer
)
target_sources(cpp-actors-prof PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/actors/prof/tag.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/actors/prof/tcmalloc_null.cpp
)
