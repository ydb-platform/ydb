add_library(lib-Transforms-InstCombine)
target_include_directories(lib-Transforms-InstCombine PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Transforms/InstCombine
)
target_link_libraries(lib-Transforms-InstCombine PUBLIC
  contrib-libs-cxxsupp
  contrib-libs-llvm12
  libs-llvm12-include
  llvm12-lib-Analysis
  llvm12-lib-IR
  llvm12-lib-Support
  lib-Transforms-Utils
)
target_sources(lib-Transforms-InstCombine PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Transforms/InstCombine/InstCombineAddSub.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Transforms/InstCombine/InstCombineAndOrXor.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Transforms/InstCombine/InstCombineAtomicRMW.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Transforms/InstCombine/InstCombineCalls.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Transforms/InstCombine/InstCombineCasts.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Transforms/InstCombine/InstCombineCompares.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Transforms/InstCombine/InstCombineLoadStoreAlloca.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Transforms/InstCombine/InstCombineMulDivRem.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Transforms/InstCombine/InstCombineNegator.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Transforms/InstCombine/InstCombinePHI.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Transforms/InstCombine/InstCombineSelect.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Transforms/InstCombine/InstCombineShifts.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Transforms/InstCombine/InstCombineSimplifyDemanded.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Transforms/InstCombine/InstCombineVectorOps.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Transforms/InstCombine/InstructionCombining.cpp
)
