add_library(libs-cxxsupp-libcxxrt)
target_compile_options(libs-cxxsupp-libcxxrt PRIVATE
  -nostdinc++
)
target_include_directories(libs-cxxsupp-libcxxrt PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/cxxsupp/libcxxrt
)
target_link_libraries(libs-cxxsupp-libcxxrt PUBLIC
  contrib-libs-libunwind
)
target_sources(libs-cxxsupp-libcxxrt PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/cxxsupp/libcxxrt/auxhelper.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/cxxsupp/libcxxrt/dynamic_cast.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/cxxsupp/libcxxrt/exception.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/cxxsupp/libcxxrt/guard.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/cxxsupp/libcxxrt/memory.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/cxxsupp/libcxxrt/stdexcept.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/cxxsupp/libcxxrt/typeinfo.cc
)
