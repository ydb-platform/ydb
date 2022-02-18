add_library(llvm12-lib-AsmParser)
target_include_directories(llvm12-lib-AsmParser PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/AsmParser
)
target_link_libraries(llvm12-lib-AsmParser PUBLIC
  contrib-libs-cxxsupp
  contrib-libs-llvm12
  libs-llvm12-include
  llvm12-lib-BinaryFormat
  llvm12-lib-IR
  llvm12-lib-Support
)
target_sources(llvm12-lib-AsmParser PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/AsmParser/LLLexer.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/AsmParser/LLParser.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/AsmParser/Parser.cpp
)
