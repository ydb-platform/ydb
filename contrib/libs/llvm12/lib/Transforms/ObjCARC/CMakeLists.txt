add_library(lib-Transforms-ObjCARC)
target_include_directories(lib-Transforms-ObjCARC PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Transforms/ObjCARC
)
target_link_libraries(lib-Transforms-ObjCARC PUBLIC
  contrib-libs-cxxsupp
  contrib-libs-llvm12
  libs-llvm12-include
  llvm12-lib-Analysis
  llvm12-lib-IR
  llvm12-lib-Support
  lib-Transforms-Utils
)
target_sources(lib-Transforms-ObjCARC PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Transforms/ObjCARC/DependencyAnalysis.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Transforms/ObjCARC/ObjCARC.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Transforms/ObjCARC/ObjCARCAPElim.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Transforms/ObjCARC/ObjCARCContract.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Transforms/ObjCARC/ObjCARCExpand.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Transforms/ObjCARC/ObjCARCOpts.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Transforms/ObjCARC/ProvenanceAnalysis.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Transforms/ObjCARC/ProvenanceAnalysisEvaluator.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Transforms/ObjCARC/PtrState.cpp
)
