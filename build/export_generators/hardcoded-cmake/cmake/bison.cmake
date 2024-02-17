function(target_bison_parser Tgt Scope)
  foreach(arg ${ARGN})
    get_filename_component(argPath ${arg} REALPATH)
    if (argPath MATCHES "${PROJECT_SOURCE_DIR}/.*")
      file(RELATIVE_PATH argRel ${CMAKE_CURRENT_SOURCE_DIR} ${argPath})
      string(REPLACE ".." "__" ArgInBindir ${argRel})
      set(ArgInBindir ${CMAKE_CURRENT_BINARY_DIR}/${ArgInBindir})
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
