# - Find IDN
#
# IDN_INCLUDE - Where to find LibIDN public headers
# IDN_LIBS - List of libraries when using LibIDN.
# IDN_FOUND - True if LibIDN found.

find_path(IDN_INCLUDE_DIR
  idna.h
  HINTS $ENV{IDN_ROOT}/include)

find_library(IDN_LIBRARIES
  idn
  HINTS $ENV{IDN_ROOT}/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(IDN DEFAULT_MSG IDN_LIBRARIES IDN_INCLUDE_DIR)

mark_as_advanced(IDN_INCLUDE_DIR IDN_LIBRARIES)

if (IDN_FOUND AND NOT TARGET IDN::IDN)
  add_library(IDN::IDN UNKNOWN IMPORTED)
  set_target_properties(IDN::IDN PROPERTIES
    IMPORTED_LOCATION ${IDN_LIBRARIES}
    INTERFACE_INCLUDE_DIRECTORIES ${IDN_INCLUDE_DIR}
  )
endif()
