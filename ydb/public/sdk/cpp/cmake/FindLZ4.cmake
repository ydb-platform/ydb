find_library(LZ4_LIBRARIES
    NAMES lz4 liblz4
    HINTS $ENV{LZ4_ROOT}/lib
)

find_path(LZ4_INCLUDE_DIR
    NAMES lz4.h lz4hc.h
    HINTS $ENV{LZ4_ROOT}/include
)

set(LZ4_INCLUDE_DIRS
  ${LZ4_INCLUDE_DIR}
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(LZ4 DEFAULT_MSG LZ4_LIBRARIES LZ4_INCLUDE_DIRS)

mark_as_advanced(LZ4_LIBRARIES LZ4_INCLUDE_DIRS)

if(LZ4_FOUND AND NOT LZ4::LZ4)
    add_library(LZ4::LZ4 UNKNOWN IMPORTED)
    set_target_properties( LZ4::LZ4 PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES "${LZ4_INCLUDE_DIRS}"
        IMPORTED_LOCATION ${LZ4_LIBRARIES})
endif()

