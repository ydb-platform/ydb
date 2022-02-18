add_library(cpp-digest-sfh)
target_link_libraries(cpp-digest-sfh PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(cpp-digest-sfh PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/digest/sfh/sfh.cpp
)
