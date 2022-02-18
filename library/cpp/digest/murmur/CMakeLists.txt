add_library(cpp-digest-murmur)
target_link_libraries(cpp-digest-murmur PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(cpp-digest-murmur PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/digest/murmur/murmur.cpp
)
