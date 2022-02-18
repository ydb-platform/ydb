add_library(abseil-cpp-absl-debugging)
target_compile_options(abseil-cpp-absl-debugging PRIVATE
  -DNOMINMAX
)
target_include_directories(abseil-cpp-absl-debugging PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_include_directories(abseil-cpp-absl-debugging PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_link_libraries(abseil-cpp-absl-debugging PUBLIC
  contrib-libs-cxxsupp
  base-internal-raw_logging
  absl-base-log_severity
)
target_sources(abseil-cpp-absl-debugging PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp/absl/debugging/internal/address_is_readable.cc
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp/absl/debugging/internal/elf_mem_image.cc
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp/absl/debugging/internal/vdso_support.cc
)
