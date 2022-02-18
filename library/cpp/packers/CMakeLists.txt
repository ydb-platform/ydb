add_library(library-cpp-packers)
target_link_libraries(library-cpp-packers PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(library-cpp-packers PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/packers/packers.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/packers/proto_packer.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/packers/region_packer.cpp
)
