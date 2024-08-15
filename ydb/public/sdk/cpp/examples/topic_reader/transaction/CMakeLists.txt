add_executable(read_from_topic_in_transaction)

target_link_libraries(read_from_topic_in_transaction PUBLIC
  yutil
  YDB-CPP-SDK::Topic
  getopt
)

target_sources(read_from_topic_in_transaction PRIVATE
  ${YDB_SDK_SOURCE_DIR}/examples/topic_reader/transaction/application.cpp
  ${YDB_SDK_SOURCE_DIR}/examples/topic_reader/transaction/main.cpp
  ${YDB_SDK_SOURCE_DIR}/examples/topic_reader/transaction/options.cpp
)

vcs_info(read_from_topic_in_transaction)

if (CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64" OR CMAKE_SYSTEM_PROCESSOR STREQUAL "AMD64")
  target_link_libraries(read_from_topic_in_transaction PUBLIC
    cpuid_check
  )
endif()

if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
  target_link_options(read_from_topic_in_transaction PRIVATE
    -ldl
    -lrt
    -Wl,--no-as-needed
    -lpthread
  )
elseif (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
  target_link_options(read_from_topic_in_transaction PRIVATE
    -Wl,-platform_version,macos,11.0,11.0
    -framework
    CoreFoundation
  )
endif()
