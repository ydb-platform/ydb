add_library(lib-Bitcode-Reader)
target_include_directories(lib-Bitcode-Reader PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Bitcode/Reader
)
target_link_libraries(lib-Bitcode-Reader PUBLIC
  contrib-libs-cxxsupp
  contrib-libs-llvm12
  libs-llvm12-include
  lib-Bitstream-Reader
  llvm12-lib-IR
  llvm12-lib-Support
)
target_sources(lib-Bitcode-Reader PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Bitcode/Reader/BitReader.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Bitcode/Reader/BitcodeAnalyzer.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Bitcode/Reader/BitcodeReader.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Bitcode/Reader/MetadataLoader.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Bitcode/Reader/ValueList.cpp
)
