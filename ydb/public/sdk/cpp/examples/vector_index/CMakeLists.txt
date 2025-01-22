add_executable(vector_index)

target_link_libraries(vector_index PUBLIC
  yutil
  getopt
  YDB-CPP-SDK::Table
)

target_sources(vector_index PRIVATE
  ${YDB_SDK_SOURCE_DIR}/examples/vector_index/main.cpp
  ${YDB_SDK_SOURCE_DIR}/examples/vector_index/vector_index.cpp
)

vcs_info(vector_index)

if (CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64" OR CMAKE_SYSTEM_PROCESSOR STREQUAL "AMD64")
  target_link_libraries(vector_index PUBLIC
    cpuid_check
  )
endif()

if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
  target_link_options(vector_index PRIVATE
    -ldl
    -lrt
    -Wl,--no-as-needed
    -lpthread
  )
elseif (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
  target_link_options(vector_index PRIVATE
    -Wl,-platform_version,macos,11.0,11.0
    -framework
    CoreFoundation
  )
endif()
