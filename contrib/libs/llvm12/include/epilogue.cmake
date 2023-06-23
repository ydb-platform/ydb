add_custom_target(OpenMP-gen-srcs-stealing
  DEPENDS ${CMAKE_BINARY_DIR}/contrib/libs/llvm12/lib/Frontend/OpenMP/OMP.cpp
)
