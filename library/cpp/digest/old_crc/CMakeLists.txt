add_library(cpp-digest-old_crc)
target_link_libraries(cpp-digest-old_crc PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(cpp-digest-old_crc INTERFACE
  ${CMAKE_BINARY_DIR}/library/cpp/digest/old_crc/crc.inc
)
target_sources(cpp-digest-old_crc PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/digest/old_crc/crc.cpp
)
add_custom_command(
  OUTPUT
  ${CMAKE_BINARY_DIR}/library/cpp/digest/old_crc/crc.inc
  DEPENDS
  ${CMAKE_BINARY_DIR}/library/cpp/digest/old_crc/gencrc/gencrc
  COMMAND
  ${CMAKE_BINARY_DIR}/library/cpp/digest/old_crc/gencrc/gencrc
  >
  crc.inc
)
