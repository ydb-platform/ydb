add_executable(secondary_index_builtin)

target_link_libraries(secondary_index_builtin PUBLIC
  yutil
  getopt
  YDB-CPP-SDK::Table
)

target_sources(secondary_index_builtin PRIVATE
  ${YDB_SDK_SOURCE_DIR}/examples/secondary_index_builtin/main.cpp
  ${YDB_SDK_SOURCE_DIR}/examples/secondary_index_builtin/secondary_index.cpp
  ${YDB_SDK_SOURCE_DIR}/examples/secondary_index_builtin/secondary_index_create.cpp
  ${YDB_SDK_SOURCE_DIR}/examples/secondary_index_builtin/secondary_index_fill.cpp
  ${YDB_SDK_SOURCE_DIR}/examples/secondary_index_builtin/secondary_index_select.cpp
  ${YDB_SDK_SOURCE_DIR}/examples/secondary_index_builtin/secondary_index_drop.cpp
  ${YDB_SDK_SOURCE_DIR}/examples/secondary_index_builtin/secondary_index_select_join.cpp
)

vcs_info(secondary_index_builtin)

if (CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64" OR CMAKE_SYSTEM_PROCESSOR STREQUAL "AMD64")
  target_link_libraries(secondary_index_builtin PUBLIC
    cpuid_check
  )
endif()

if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
  target_link_options(secondary_index_builtin PRIVATE
    -ldl
    -lrt
    -Wl,--no-as-needed
    -lpthread
  )
elseif (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
  target_link_options(secondary_index_builtin PRIVATE
    -Wl,-platform_version,macos,11.0,11.0
    -framework
    CoreFoundation
  )
endif()
