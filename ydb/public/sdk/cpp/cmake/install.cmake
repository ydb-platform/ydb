function(_ydb_sdk_install_targets)
  if (NOT YDB_SDK_INSTALL)
    return()
  endif()
  set(multiValueArgs TARGETS)
  cmake_parse_arguments(
    ARG "${option}" "${oneValueArgs}" "${multiValueArgs}" "${ARGN}"
  )
  if (NOT ARG_TARGETS)
    message(FATAL_ERROR "No TARGETS given for install")
    return()
  endif()
  foreach (ITEM_TARGET IN LISTS ARG_TARGETS)
    if(NOT TARGET ${ITEM_TARGET})
      message(FATAL_ERROR "${ITEM_TARGET} is not target. You should use only targets")
      return()
    endif()
  endforeach()
  install(TARGETS ${ARG_TARGETS}
    EXPORT ydb-cpp-sdk-targets
    CONFIGURATIONS RELEASE
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    INCLUDES DESTINATION
      ${CMAKE_INSTALL_INCLUDEDIR}
      ${CMAKE_INSTALL_INCLUDEDIR}/__ydb_sdk_special_headers
  )
endfunction()

function(_ydb_sdk_directory_install)
  if (NOT ${YDB_SDK_INSTALL})
    return()
  endif()
  set(oneValueArgs DESTINATION PATTERN)
  set(multiValueArgs FILES DIRECTORY)
  cmake_parse_arguments(
    ARG "${option}" "${oneValueArgs}" "${multiValueArgs}" "${ARGN}"
  )
  if (NOT ARG_DESTINATION)
    message(FATAL_ERROR "No DESTINATION for install")
  endif()
  if (NOT ARG_FILES AND NOT ARG_DIRECTORY)
    message(FATAL_ERROR "No FILES or DIRECTORY provided to install")
  endif()
  if (ARG_FILES AND ARG_DIRECTORY)
    message(FATAL_ERROR "FILES and DIRECTORY are mutually exclusive install arguments")
  endif()
  if (ARG_FILES)
    install(FILES ${ARG_FILES} DESTINATION ${ARG_DESTINATION})
  else()
    if (DEFINED ARG_PATTERN)
      install(DIRECTORY ${ARG_DIRECTORY} DESTINATION ${ARG_DESTINATION} USE_SOURCE_PERMISSIONS FILES_MATCHING PATTERN ${ARG_PATTERN})
    else()
      install(DIRECTORY ${ARG_DIRECTORY} DESTINATION ${ARG_DESTINATION} USE_SOURCE_PERMISSIONS)
    endif()
  endif()
endfunction()

function(_ydb_sdk_install_headers ArgIncludeDir)
  if (NOT ${YDB_SDK_INSTALL})
    return()
  endif()
  _ydb_sdk_directory_install(DIRECTORY ${YDB_SDK_SOURCE_DIR}/include/ydb-cpp-sdk
    DESTINATION ${ArgIncludeDir}
  )

  file(STRINGS ${YDB_SDK_SOURCE_DIR}/cmake/public_headers.txt PublicHeaders)
  file(STRINGS ${YDB_SDK_SOURCE_DIR}/cmake/protos_public_headers.txt ProtosPublicHeaders)
  if (NOT MSVC)
    list(REMOVE_ITEM PublicHeaders library/cpp/deprecated/atomic/atomic_win.h)
  endif()
  foreach(HeaderPath ${PublicHeaders})
    get_filename_component(RelInstallPath ${HeaderPath} DIRECTORY)
    _ydb_sdk_directory_install(FILES
        ${YDB_SDK_SOURCE_DIR}/${HeaderPath}
      DESTINATION ${ArgIncludeDir}/__ydb_sdk_special_headers/${RelInstallPath}
    )
  endforeach()
  foreach(HeaderPath ${ProtosPublicHeaders})
    get_filename_component(RelInstallPath ${HeaderPath} DIRECTORY)
    _ydb_sdk_directory_install(FILES
        ${YDB_SDK_BINARY_DIR}/${HeaderPath}
      DESTINATION ${ArgIncludeDir}/__ydb_sdk_special_headers/${RelInstallPath}
    )
  endforeach()
endfunction()
