add_library(base-internal-spinlock_wait)
target_compile_options(base-internal-spinlock_wait PRIVATE
  -DNOMINMAX
)
target_include_directories(base-internal-spinlock_wait PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_include_directories(base-internal-spinlock_wait PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_link_libraries(base-internal-spinlock_wait PUBLIC
  contrib-libs-cxxsupp
)
target_sources(base-internal-spinlock_wait PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp/absl/base/internal/spinlock_wait.cc
)
