add_library(actors-interconnect-mock)
target_link_libraries(actors-interconnect-mock PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-interconnect
)
target_sources(actors-interconnect-mock PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/actors/interconnect/mock/ic_mock.cpp
)
