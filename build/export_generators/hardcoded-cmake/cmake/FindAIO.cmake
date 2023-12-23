# - Find AIO
#
# AIO_INCLUDE - Where to find libaio.h
# AIO_LIBS - List of libraries when using AIO.
# AIO_FOUND - True if AIO found.

find_path(AIO_INCLUDE_DIR
  libaio.h
  HINTS $ENV{AIO_ROOT}/include)

find_library(AIO_LIBRARIES
  aio
  HINTS $ENV{AIO_ROOT}/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(AIO DEFAULT_MSG AIO_LIBRARIES AIO_INCLUDE_DIR)

mark_as_advanced(AIO_INCLUDE_DIR AIO_LIBRARIES)

if (AIO_FOUND AND NOT TARGET AIO::aio)
  add_library(AIO::aio UNKNOWN IMPORTED)
  set_target_properties(AIO::aio PROPERTIES
    IMPORTED_LOCATION ${AIO_LIBRARIES}
    INTERFACE_INCLUDE_DIRECTORIES ${AIO_INCLUDE_DIR}
  )
endif()
