if (NOT CCACHE_PATH)
  find_program(CCACHE_PATH ccache)
endif()
if (NOT CCACHE_PATH)
  message(AUTHOR_WARNING
    "Ccache is not found, that will increase the re-compilation time; "
    "pass -DCCACHE_PATH=path/to/bin to specify the path to the `ccache` binary file."
  )
else()
  set(CMAKE_C_COMPILER_LAUNCHER "${CCACHE_PATH}" CACHE STRING "C compiler launcher")
  set(CMAKE_CXX_COMPILER_LAUNCHER "${CCACHE_PATH}" CACHE STRING "C++ compiler launcher")
endif()
