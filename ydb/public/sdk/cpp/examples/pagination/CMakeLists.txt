add_executable(pagination)

target_link_libraries(pagination PUBLIC
  yutil
  getopt
  YDB-CPP-SDK::Table
)

target_sources(pagination PRIVATE
  ${YDB_SDK_SOURCE_DIR}/examples/pagination/main.cpp
  ${YDB_SDK_SOURCE_DIR}/examples/pagination/pagination_data.cpp
  ${YDB_SDK_SOURCE_DIR}/examples/pagination/pagination.cpp
)

vcs_info(pagination)

if (CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64" OR CMAKE_SYSTEM_PROCESSOR STREQUAL "AMD64")
  target_link_libraries(pagination PUBLIC
    cpuid_check
  )
endif()

if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
  target_link_options(pagination PRIVATE
    -ldl
    -lrt
    -Wl,--no-as-needed
    -lpthread
  )
elseif (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
  target_link_options(pagination PRIVATE
    -Wl,-platform_version,macos,11.0,11.0
    -framework
    CoreFoundation
  )
endif()