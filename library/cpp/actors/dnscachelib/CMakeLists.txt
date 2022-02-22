add_library(cpp-actors-dnscachelib)
target_link_libraries(cpp-actors-dnscachelib PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-lwtrace
)
target_sources(cpp-actors-dnscachelib PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/actors/dnscachelib/dnscache.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/actors/dnscachelib/probes.cpp
)
