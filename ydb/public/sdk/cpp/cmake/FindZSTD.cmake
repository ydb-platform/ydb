# - Find ZSTD
#
# ZSTD_INCLUDE - Where to find LibZSTD public headers
# ZSTD_LIBS - List of libraries when using LibZSTD.
# ZSTD_FOUND - True if LibZSTD found.

find_path(ZSTD_INCLUDE_DIR
  zstd.h
  HINTS $ENV{ZSTD_ROOT}/include
)

find_library(ZSTD_LIBRARIES
  zstd
  HINTS $ENV{ZSTD_ROOT}/lib
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(ZSTD DEFAULT_MSG ZSTD_LIBRARIES ZSTD_INCLUDE_DIR)

mark_as_advanced(ZSTD_INCLUDE_DIR ZSTD_LIBRARIES)

if (ZSTD_FOUND AND NOT TARGET ZSTD::ZSTD)
  add_library(ZSTD::ZSTD UNKNOWN IMPORTED)
  set_target_properties(ZSTD::ZSTD PROPERTIES
    IMPORTED_LOCATION ${ZSTD_LIBRARIES}
    INTERFACE_INCLUDE_DIRECTORIES ${ZSTD_INCLUDE_DIR}
  )
endif()
