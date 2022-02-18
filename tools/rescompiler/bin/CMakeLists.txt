add_executable(rescompiler)
target_link_libraries(rescompiler PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-cpuid_check
  library-cpp-resource
)
target_sources(rescompiler PRIVATE
  ${CMAKE_SOURCE_DIR}/tools/rescompiler/main.cpp
)
target_link_flags(rescompiler
  PUBLIC
  -lpthread
  -lrt
  -ldl
)
vcs_info(rescompiler)
