add_library(Target-X86-TargetInfo)
target_include_directories(Target-X86-TargetInfo PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Target/X86
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Target/X86/TargetInfo
)
target_link_libraries(Target-X86-TargetInfo PUBLIC
  contrib-libs-cxxsupp
  contrib-libs-llvm12
  llvm12-lib-Support
)
target_sources(Target-X86-TargetInfo PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Target/X86/TargetInfo/X86TargetInfo.cpp
)
