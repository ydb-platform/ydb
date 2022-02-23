add_library(ybase-internal-spinlock_wait)
target_include_directories(ybase-internal-spinlock_wait PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_include_directories(ybase-internal-spinlock_wait PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_link_libraries(ybase-internal-spinlock_wait PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(ybase-internal-spinlock_wait PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/base/internal/spinlock_wait.cc
)
