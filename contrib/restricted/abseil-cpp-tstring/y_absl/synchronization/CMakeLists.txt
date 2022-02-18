add_library(abseil-cpp-tstring-y_absl-synchronization)
target_include_directories(abseil-cpp-tstring-y_absl-synchronization PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_include_directories(abseil-cpp-tstring-y_absl-synchronization PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_link_libraries(abseil-cpp-tstring-y_absl-synchronization PUBLIC
  contrib-libs-cxxsupp
  yutil
  abseil-cpp-tstring-y_absl-base
  base-internal-low_level_alloc
  base-internal-raw_logging
  base-internal-spinlock_wait
  base-internal-throw_delegate
  y_absl-base-log_severity
  abseil-cpp-tstring-y_absl-debugging
  y_absl-debugging-stacktrace
  y_absl-debugging-symbolize
  abseil-cpp-tstring-y_absl-demangle
  abseil-cpp-tstring-y_absl-numeric
  abseil-cpp-tstring-y_absl-strings
  strings-internal-absl_strings_internal
  y_absl-synchronization-internal
  abseil-cpp-tstring-y_absl-time
  y_absl-time-civil_time
  y_absl-time-time_zone
)
target_sources(abseil-cpp-tstring-y_absl-synchronization PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/synchronization/barrier.cc
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/synchronization/blocking_counter.cc
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/synchronization/internal/create_thread_identity.cc
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/synchronization/internal/per_thread_sem.cc
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/synchronization/internal/waiter.cc
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/synchronization/mutex.cc
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/synchronization/notification.cc
)
