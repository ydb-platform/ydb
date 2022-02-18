add_library(lib-MC-MCParser)
target_include_directories(lib-MC-MCParser PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/MC/MCParser
)
target_link_libraries(lib-MC-MCParser PUBLIC
  contrib-libs-cxxsupp
  contrib-libs-llvm12
  llvm12-lib-MC
  llvm12-lib-Support
)
target_sources(lib-MC-MCParser PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/MC/MCParser/AsmLexer.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/MC/MCParser/AsmParser.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/MC/MCParser/COFFAsmParser.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/MC/MCParser/COFFMasmParser.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/MC/MCParser/DarwinAsmParser.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/MC/MCParser/ELFAsmParser.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/MC/MCParser/MCAsmLexer.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/MC/MCParser/MCAsmParser.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/MC/MCParser/MCAsmParserExtension.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/MC/MCParser/MCTargetAsmParser.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/MC/MCParser/MasmParser.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/MC/MCParser/WasmAsmParser.cpp
)
