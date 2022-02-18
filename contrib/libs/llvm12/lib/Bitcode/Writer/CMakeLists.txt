add_library(lib-Bitcode-Writer)
target_include_directories(lib-Bitcode-Writer PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Bitcode/Writer
)
target_link_libraries(lib-Bitcode-Writer PUBLIC
  contrib-libs-cxxsupp
  contrib-libs-llvm12
  libs-llvm12-include
  llvm12-lib-Analysis
  llvm12-lib-IR
  llvm12-lib-MC
  llvm12-lib-Object
  llvm12-lib-Support
)
target_sources(lib-Bitcode-Writer PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Bitcode/Writer/BitWriter.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Bitcode/Writer/BitcodeWriter.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Bitcode/Writer/BitcodeWriterPass.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Bitcode/Writer/ValueEnumerator.cpp
)
