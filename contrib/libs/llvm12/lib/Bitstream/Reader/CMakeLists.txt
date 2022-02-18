add_library(lib-Bitstream-Reader)
target_include_directories(lib-Bitstream-Reader PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Bitstream/Reader
)
target_link_libraries(lib-Bitstream-Reader PUBLIC
  contrib-libs-cxxsupp
  contrib-libs-llvm12
  llvm12-lib-Support
)
target_sources(lib-Bitstream-Reader PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Bitstream/Reader/BitstreamReader.cpp
)
