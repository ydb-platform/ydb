add_library(lib-MC-MCDisassembler)
target_include_directories(lib-MC-MCDisassembler PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/MC/MCDisassembler
)
target_link_libraries(lib-MC-MCDisassembler PUBLIC
  contrib-libs-cxxsupp
  contrib-libs-llvm12
  llvm12-lib-MC
  llvm12-lib-Support
)
target_sources(lib-MC-MCDisassembler PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/MC/MCDisassembler/Disassembler.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/MC/MCDisassembler/MCDisassembler.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/MC/MCDisassembler/MCExternalSymbolizer.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/MC/MCDisassembler/MCRelocationInfo.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/MC/MCDisassembler/MCSymbolizer.cpp
)
