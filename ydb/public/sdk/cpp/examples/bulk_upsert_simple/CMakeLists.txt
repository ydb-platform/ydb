add_executable(bulk_upsert_simple)

target_link_libraries(bulk_upsert_simple PUBLIC
  yutil
  getopt
  YDB-CPP-SDK::Table
)

target_sources(bulk_upsert_simple PRIVATE
  ${YDB_SDK_SOURCE_DIR}/examples/bulk_upsert_simple/main.cpp
)

vcs_info(bulk_upsert_simple)

if (CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64" OR CMAKE_SYSTEM_PROCESSOR STREQUAL "AMD64")
  target_link_libraries(bulk_upsert_simple PUBLIC
    cpuid_check
  )
endif()

if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
  target_link_options(bulk_upsert_simple PRIVATE
    -ldl
    -lrt
    -Wl,--no-as-needed
    -lpthread
  )
elseif (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
  target_link_options(bulk_upsert_simple PRIVATE
    -Wl,-platform_version,macos,11.0,11.0
    -framework
    CoreFoundation
  )
endif()
