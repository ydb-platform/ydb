add_library(lib-Frontend-OpenMP)
target_include_directories(lib-Frontend-OpenMP PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Frontend/OpenMP
)
target_link_libraries(lib-Frontend-OpenMP PUBLIC
  contrib-libs-cxxsupp
  contrib-libs-llvm12
  libs-llvm12-include
  llvm12-lib-IR
  llvm12-lib-Support
  lib-Transforms-Utils
)
target_sources(lib-Frontend-OpenMP PRIVATE
  ${CMAKE_BINARY_DIR}/contrib/libs/llvm12/lib/Frontend/OpenMP/OMP.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Frontend/OpenMP/OMPContext.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/llvm12/lib/Frontend/OpenMP/OMPIRBuilder.cpp
)
