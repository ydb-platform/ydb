
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(src-core-lib)
target_compile_options(src-core-lib PRIVATE
  -DGRPC_POSIX_FORK_ALLOW_PTHREAD_ATFORK=1
)
target_include_directories(src-core-lib PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/include
)
target_include_directories(src-core-lib PRIVATE
  ${CMAKE_BINARY_DIR}/contrib/libs/grpc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/ext/upb-generated
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/third_party/upb
)
target_link_libraries(src-core-lib PUBLIC
  contrib-libs-cxxsupp
  yutil
  grpc-third_party-upb
  base-internal-spinlock_wait
  abseil-cpp-tstring-y_absl-container
  abseil-cpp-tstring-y_absl-memory
  abseil-cpp-tstring-y_absl-status
  abseil-cpp-tstring-y_absl-strings
  y_absl-strings-cord
  strings-internal-str_format
  abseil-cpp-tstring-y_absl-synchronization
  abseil-cpp-tstring-y_absl-time
)
target_sources(src-core-lib PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/ext/upb-generated/google/protobuf/any.upb.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/ext/upb-generated/google/rpc/status.upb.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/lib/gpr/alloc.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/lib/gpr/atm.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/lib/gpr/cpu_iphone.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/lib/gpr/cpu_linux.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/lib/gpr/cpu_posix.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/lib/gpr/cpu_windows.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/lib/gpr/env_linux.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/lib/gpr/env_posix.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/lib/gpr/env_windows.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/lib/gpr/log.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/lib/gpr/log_android.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/lib/gpr/log_linux.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/lib/gpr/log_posix.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/lib/gpr/log_windows.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/lib/gpr/murmur_hash.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/lib/gpr/string.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/lib/gpr/string_posix.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/lib/gpr/string_util_windows.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/lib/gpr/string_windows.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/lib/gpr/sync.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/lib/gpr/sync_abseil.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/lib/gpr/sync_posix.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/lib/gpr/sync_windows.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/lib/gpr/time.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/lib/gpr/time_posix.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/lib/gpr/time_precise.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/lib/gpr/time_windows.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/lib/gpr/tmpfile_msys.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/lib/gpr/tmpfile_posix.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/lib/gpr/tmpfile_windows.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/lib/gpr/wrap_memcpy.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/lib/gprpp/arena.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/lib/gprpp/examine_stack.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/lib/gprpp/fork.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/lib/gprpp/global_config_env.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/lib/gprpp/host_port.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/lib/gprpp/mpscq.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/lib/gprpp/stat_posix.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/lib/gprpp/stat_windows.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/lib/gprpp/status_helper.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/lib/gprpp/thd_posix.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/lib/gprpp/thd_windows.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/lib/gprpp/time_util.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/lib/profiling/basic_timers.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/src/core/lib/profiling/stap_timers.cc
)
