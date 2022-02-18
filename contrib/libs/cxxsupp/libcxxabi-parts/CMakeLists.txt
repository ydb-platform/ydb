add_library(libs-cxxsupp-libcxxabi-parts)
target_compile_options(libs-cxxsupp-libcxxabi-parts PRIVATE
  -D_LIBCXXABI_BUILDING_LIBRARY
)
target_include_directories(libs-cxxsupp-libcxxabi-parts PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/cxxsupp/libcxxabi/include
  ${CMAKE_SOURCE_DIR}/contrib/libs/cxxsupp/libcxx/include
  ${CMAKE_SOURCE_DIR}/contrib/libs/cxxsupp/libcxx
)
target_sources(libs-cxxsupp-libcxxabi-parts PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/cxxsupp/libcxxabi/src/cxa_thread_atexit.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/cxxsupp/libcxxabi/src/abort_message.cpp
  ${CMAKE_SOURCE_DIR}/contrib/libs/cxxsupp/libcxxabi/src/cxa_demangle.cpp
)
set_property(
  SOURCE
  ${CMAKE_SOURCE_DIR}/contrib/libs/cxxsupp/libcxxabi/src/cxa_thread_atexit.cpp
  APPEND
  PROPERTY
  COMPILE_OPTIONS
  -fPIC
  -fno-lto
)
