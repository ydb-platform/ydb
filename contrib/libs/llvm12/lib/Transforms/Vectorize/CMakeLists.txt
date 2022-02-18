add_library(lib-Transforms-Vectorize)
target_include_directories(lib-Transforms-Vectorize PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Transforms/Vectorize
)
target_link_libraries(lib-Transforms-Vectorize PUBLIC
  contrib-libs-cxxsupp
  contrib-libs-llvm12
  libs-llvm12-include
  llvm12-lib-Analysis
  llvm12-lib-IR
  llvm12-lib-Support
  lib-Transforms-Utils
)
target_sources(lib-Transforms-Vectorize PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Transforms/Vectorize/LoadStoreVectorizer.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Transforms/Vectorize/LoopVectorizationLegality.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Transforms/Vectorize/LoopVectorize.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Transforms/Vectorize/SLPVectorizer.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Transforms/Vectorize/VPlan.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Transforms/Vectorize/VPlanHCFGBuilder.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Transforms/Vectorize/VPlanPredicator.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Transforms/Vectorize/VPlanSLP.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Transforms/Vectorize/VPlanTransforms.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Transforms/Vectorize/VPlanVerifier.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Transforms/Vectorize/VectorCombine.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Transforms/Vectorize/Vectorize.cpp
)
