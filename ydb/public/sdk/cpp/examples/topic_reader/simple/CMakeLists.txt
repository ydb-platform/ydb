add_executable(simple_persqueue_reader)

target_link_libraries(simple_persqueue_reader PUBLIC
  yutil
  YDB-CPP-SDK::Topic
  getopt
)

target_sources(simple_persqueue_reader PRIVATE
  ${YDB_SDK_SOURCE_DIR}/examples/topic_reader/simple/main.cpp
)

vcs_info(simple_persqueue_reader)

if (CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64" OR CMAKE_SYSTEM_PROCESSOR STREQUAL "AMD64")
  target_link_libraries(simple_persqueue_reader PUBLIC
    cpuid_check
  )
endif()

if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
  target_link_options(simple_persqueue_reader PRIVATE
    -ldl
    -lrt
    -Wl,--no-as-needed
    -lpthread
  )
elseif (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
  target_link_options(simple_persqueue_reader PRIVATE
    -Wl,-platform_version,macos,11.0,11.0
    -framework
    CoreFoundation
  )
endif()
