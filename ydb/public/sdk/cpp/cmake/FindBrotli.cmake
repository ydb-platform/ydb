# - Find IDN
#
# IDN_INCLUDE - Where to find LibIDN public headers
# IDN_LIBS - List of libraries when using LibIDN.
# IDN_FOUND - True if LibIDN found.

find_path(Brotli_INCLUDE_DIR
  "brotli/decode.h"
  HINTS $ENV{Brotli_ROOT}/include
)

find_library(Brotli_common_LIBRARY
  brotlicommon
  HINTS $ENV{Brotli_ROOT}/lib
)

find_library(Brotli_enc_LIBRARY
  brotlienc
  HINTS $ENV{Brotli_ROOT}/lib
)

find_library(Brotli_dec_LIBRARY
  brotlidec
  HINTS $ENV{Brotli_ROOT}/lib
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Brotli
  DEFAULT_MSG
  Brotli_common_LIBRARY
  Brotli_enc_LIBRARY
  Brotli_dec_LIBRARY
  Brotli_INCLUDE_DIR
)

mark_as_advanced(Brotli_INCLUDE_DIR Brotli_common_LIBRARY Brotli_enc_LIBRARY Brotli_dec_LIBRARY)

if (Brotli_FOUND)
  if (NOT TARGET Brotli::common)
    add_library(Brotli::common UNKNOWN IMPORTED)
    set_target_properties(Brotli::common PROPERTIES
      IMPORTED_LOCATION ${Brotli_common_LIBRARY}
      INTERFACE_INCLUDE_DIRECTORIES ${Brotli_INCLUDE_DIR}
    )
  endif()
  if (NOT TARGET Brotli::dec)
    add_library(Brotli::dec UNKNOWN IMPORTED)
    set_target_properties(Brotli::dec PROPERTIES
      IMPORTED_LOCATION ${Brotli_dec_LIBRARY}
      INTERFACE_INCLUDE_DIRECTORIES ${Brotli_INCLUDE_DIR}
    )
    set_property(TARGET Brotli::dec APPEND PROPERTY INTERFACE_LINK_LIBRARIES 
      Brotli::common
    )
  endif()
  if (NOT TARGET Brotli::enc)
    add_library(Brotli::enc UNKNOWN IMPORTED)
    set_target_properties(Brotli::enc PROPERTIES
      IMPORTED_LOCATION ${Brotli_enc_LIBRARY}
      INTERFACE_INCLUDE_DIRECTORIES ${Brotli_INCLUDE_DIR}
    )
    set_property(TARGET Brotli::enc APPEND PROPERTY INTERFACE_LINK_LIBRARIES 
      Brotli::common
      Brotli::dec
    )
  endif()
endif()
