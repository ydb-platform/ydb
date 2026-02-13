cmake_minimum_required(VERSION 3.15)

project(yc-libvhost-server C)

if(UNIX AND NOT APPLE)
  set(LINUX TRUE)
endif()

if(NOT LINUX)
  message(FATAL_ERROR "Unsupported platform")
endif()

set(LIBVHOST_LOG_VERBOSITY "LOG_INFO" CACHE STRING "Libvhost log verbosity")
message("Compiler ${CMAKE_C_COMPILER}")
message("Libvhost log verbosity: ${LIBVHOST_LOG_VERBOSITY}")

add_library(vhost-server)
add_compile_definitions(_GNU_SOURCE LOG_VERBOSITY=${LIBVHOST_LOG_VERBOSITY})
target_compile_options(vhost-server PRIVATE
  -Wall
  -Werror
  -Wextra
  -Wno-unused-parameter
  -g
  -O2

  # make these warnings non-fatal in gcc
  $<$<C_COMPILER_ID:GNU>:
    -Wno-error=unused-value
    -Wno-error=unused-result
    -Wno-error=strict-aliasing
  >

  # enable additional warnings to enforce coding standards
  -Wmissing-prototypes
  -Wmissing-declarations
  $<$<C_COMPILER_ID:Clang>:
    -Wmissing-variable-declarations
    -Wzero-length-array
  >
  $<$<C_COMPILER_ID:GNU>:
    -Wzero-length-bounds
  >
)
target_include_directories(vhost-server PUBLIC
  include
)
target_include_directories(vhost-server PRIVATE
  ./
)
target_sources(vhost-server PRIVATE
  blockdev.c
  event.c
  fs.c
  logging.c
  memlog.c
  memmap.c
  server.c
  vdev.c
  platform.c
  virtio/virt_queue.c
  virtio/virtio_blk.c
  virtio/virtio_fs.c
)
