option(YDB_EMBEDDED_UI_DEV_SNAPSHOT "Use latest dev version of embedded UI git repo" Off)
include(${CMAKE_CURRENT_SOURCE_DIR}/recursive_resources.cmake)

if (YDB_EMBEDDED_UI_DEV_SNAPSHOT)
  find_program(NPM_PATH NAMES npm REQUIRED)

  include(ExternalProject)
  ExternalProject_Add(ydb-embedded-ui-git
    GIT_REPOSITORY https://github.com/ydb-platform/ydb-embedded-ui.git
    GIT_TAG main
    USES_TERMINAL_DOWNLOAD On
    USES_TERMINAL_UPDATE On
    USES_TERMINAL_BUILD On
    CONFIGURE_COMMAND ""
    TEST_COMMAND ""
    INSTALL_COMMAND ""
    BUILD_IN_SOURCE On
    BUILD_COMMAND ${NPM_PATH} ci
    COMMAND       ${NPM_PATH} run build:embedded
  )
  ExternalProject_Get_property(ydb-embedded-ui-git SOURCE_DIR)
  add_gen_resources(ydb-embedded-ui-git ${CMAKE_BINARY_DIR}/ydb/core/viewer/ydb_embedded_ui_monitoring.cpp
    IN_DIR  ${SOURCE_DIR}/build
    PREFIX monitoring
  )

else()
  add_dir_resources(${CMAKE_BINARY_DIR}/ydb/core/viewer/ydb_embedded_ui_monitoring.cpp
    IN_DIR  ${CMAKE_CURRENT_SOURCE_DIR}/monitoring
    PREFIX monitoring
  )
endif()
target_sources(ydb-core-viewer.global PRIVATE
  ${CMAKE_BINARY_DIR}/ydb/core/viewer/ydb_embedded_ui_monitoring.cpp
)
