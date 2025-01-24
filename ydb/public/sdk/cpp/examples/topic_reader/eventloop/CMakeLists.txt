add_executable(persqueue_reader_eventloop)

target_link_libraries(persqueue_reader_eventloop PUBLIC
  yutil
  YDB-CPP-SDK::Topic
  getopt
)

target_sources(persqueue_reader_eventloop PRIVATE
  ${YDB_SDK_SOURCE_DIR}/examples/topic_reader/eventloop/main.cpp
)

vcs_info(persqueue_reader_eventloop)

if (CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64" OR CMAKE_SYSTEM_PROCESSOR STREQUAL "AMD64")
  target_link_libraries(persqueue_reader_eventloop PUBLIC
    cpuid_check
  )
endif()

if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
  target_link_options(persqueue_reader_eventloop PRIVATE
    -ldl
    -lrt
    -Wl,--no-as-needed
    -lpthread
  )
elseif (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
  target_link_options(persqueue_reader_eventloop PRIVATE
    -Wl,-platform_version,macos,11.0,11.0
    -framework
    CoreFoundation
  )
endif()
