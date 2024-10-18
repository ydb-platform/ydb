add_executable(ttl)

target_link_libraries(ttl PUBLIC
  yutil
  getopt
  YDB-CPP-SDK::Table
)

target_sources(ttl PRIVATE
  ${YDB_SDK_SOURCE_DIR}/examples/ttl/main.cpp
  ${YDB_SDK_SOURCE_DIR}/examples/ttl/ttl.cpp
)

vcs_info(ttl)

if (CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64" OR CMAKE_SYSTEM_PROCESSOR STREQUAL "AMD64")
  target_link_libraries(ttl PUBLIC
    cpuid_check
  )
endif()

if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
  target_link_options(ttl PRIVATE
    -ldl
    -lrt
    -Wl,--no-as-needed
    -lpthread
  )
elseif (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
  target_link_options(ttl PRIVATE
    -Wl,-platform_version,macos,11.0,11.0
    -framework
    CoreFoundation
  )
endif()
