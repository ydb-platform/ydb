function(target_bison_parser Tgt Scope)
  foreach(arg ${ARGN})
    get_filename_component(argPath ${arg} REALPATH)
    if (argPath MATCHES "${PROJECT_SOURCE_DIR}/.*")
      file(RELATIVE_PATH argRel ${CMAKE_CURRENT_SOURCE_DIR} ${argPath})
      set(ArgInBindir ${CMAKE_CURRENT_BINARY_DIR}/${argRel})
      if (ArgInBindir MATCHES ".*/\\.\\./.*")
        string(REPLACE "/.." "/__" ArgInBindir ${ArgInBindir})
      elseif (argRel MATCHES ".*/.*")
        # Add /_/ only if relative path has some subpath
        string(REPLACE ${CMAKE_CURRENT_BINARY_DIR}/ ${CMAKE_CURRENT_BINARY_DIR}/_/ ArgInBindir ${ArgInBindir})
      endif()
    else()
      set(ArgInBindir ${argPath})
    endif()
    get_filename_component(OutputBase ${arg} NAME_WLE)
    get_filename_component(OutputDir ${ArgInBindir} DIRECTORY)
    add_custom_command(
      OUTPUT ${OutputDir}/${OutputBase}.cpp ${OutputDir}/${OutputBase}.h
      COMMAND ${CMAKE_COMMAND} -E make_directory ${OutputDir}
      COMMAND ${CMAKE_COMMAND} -E env M4=${PROJECT_BINARY_DIR}/bin/m4/bin/m4 ${PROJECT_BINARY_DIR}/bin/bison/bin/bison ${BISON_FLAGS} -v --defines=${OutputDir}/${OutputBase}.h -o ${OutputDir}/${OutputBase}.cpp ${arg}
      DEPENDS ${arg}
    )
    target_sources(${Tgt} ${Scope} ${OutputDir}/${OutputBase}.cpp ${OutputDir}/${OutputBase}.h)
  endforeach()
endfunction()

function(target_flex_lexers Tgt)
endfunction()
