add_library(cpp-digest-crc32c)
target_link_libraries(cpp-digest-crc32c PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-crcutil
)
target_sources(cpp-digest-crc32c PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/digest/crc32c/crc32c.cpp
)
