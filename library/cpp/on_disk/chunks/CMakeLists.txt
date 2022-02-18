add_library(cpp-on_disk-chunks)
target_link_libraries(cpp-on_disk-chunks PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(cpp-on_disk-chunks PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/on_disk/chunks/chunked_helpers.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/on_disk/chunks/reader.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/on_disk/chunks/writer.cpp
)
