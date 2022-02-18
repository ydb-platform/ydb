add_library(lib-DebugInfo-MSF)
target_include_directories(lib-DebugInfo-MSF PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/DebugInfo/MSF
)
target_link_libraries(lib-DebugInfo-MSF PUBLIC
  contrib-libs-cxxsupp
  contrib-libs-llvm12
  llvm12-lib-Support
)
target_sources(lib-DebugInfo-MSF PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/DebugInfo/MSF/MSFBuilder.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/DebugInfo/MSF/MSFCommon.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/DebugInfo/MSF/MSFError.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/DebugInfo/MSF/MappedBlockStream.cpp
)
