add_library(library-cpp-cpuid_check INTERFACE)
target_link_libraries(library-cpp-cpuid_check INTERFACE
  contrib-libs-cxxsupp
  yutil
)

add_global_library_for(library-cpp-cpuid_check.global library-cpp-cpuid_check)
target_link_libraries(library-cpp-cpuid_check.global INTERFACE
  contrib-libs-cxxsupp
  yutil
)
target_sources(library-cpp-cpuid_check.global PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/cpuid_check/cpu_id_check.cpp
)
