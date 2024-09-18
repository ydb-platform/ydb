add_executable(basic_example)

target_link_libraries(basic_example PUBLIC
  yutil
  getopt
  YDB-CPP-SDK::Query
  YDB-CPP-SDK::Params
  YDB-CPP-SDK::Driver
)

target_sources(basic_example PRIVATE
  main.cpp
  basic_example_data.cpp
  basic_example.cpp
)

vcs_info(basic_example)

if (CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64" OR CMAKE_SYSTEM_PROCESSOR STREQUAL "AMD64")
  target_link_libraries(basic_example PUBLIC
    cpuid_check
  )
endif()

if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
  target_link_options(basic_example PRIVATE
    -ldl
    -lrt
    -Wl,--no-as-needed
    -lpthread
  )
elseif (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
  target_link_options(basic_example PRIVATE
    -Wl,-platform_version,macos,11.0,11.0
    -framework
    CoreFoundation
  )
endif()
