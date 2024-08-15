# - Find xxHash
#
# xxHash_INCLUDE - Where to find LibxxHash public headers
# xxHash_LIBS - List of libraries when using LibxxHash.
# xxHash_FOUND - True if LibxxHash found.

find_path(xxHash_INCLUDE_DIR
  xxhash.h
  HINTS $ENV{xxHash_ROOT}/include
)

find_library(xxHash_LIBRARIES
  xxhash
  HINTS $ENV{xxHash_ROOT}/lib
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(xxHash DEFAULT_MSG xxHash_LIBRARIES xxHash_INCLUDE_DIR)

mark_as_advanced(xxHash_INCLUDE_DIR xxHash_LIBRARIES)

if (xxHash_FOUND AND NOT TARGET xxHash::xxHash)
  add_library(xxHash::xxHash UNKNOWN IMPORTED)
  set_target_properties(xxHash::xxHash PROPERTIES
    IMPORTED_LOCATION ${xxHash_LIBRARIES}
    INTERFACE_INCLUDE_DIRECTORIES ${xxHash_INCLUDE_DIR}
  )
endif()
