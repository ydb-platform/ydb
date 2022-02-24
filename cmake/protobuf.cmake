function(target_proto_plugin Tgt Name PluginTarget)
  set_property(TARGET ${Tgt} APPEND PROPERTY
    PROTOC_OPTS --${Name}_out=${CMAKE_BINARY_DIR} --plugin=protoc-gen-${Name}=$<TARGET_FILE:${PluginTarget}>
  )
  set_property(TARGET ${Tgt} APPEND PROPERTY
    PROTOC_DEPS ${PluginTarget}
  )
endfunction()

function(target_proto_messages Tgt Scope)
  get_property(ProtocExtraOutsSuf TARGET ${Tgt} PROPERTY PROTOC_EXTRA_OUTS)
  foreach(proto ${ARGN})
    if (proto MATCHES ${CMAKE_SOURCE_DIR})
      file(RELATIVE_PATH protoRel ${CMAKE_SOURCE_DIR} ${proto})
    elseif(proto MATCHES ${CMAKE_BINARY_DIR})
      file(RELATIVE_PATH protoRel ${CMAKE_BINARY_DIR} ${proto})
    else()
      set(protoRel ${proto})
    endif()
    get_filename_component(OutputBase ${protoRel} NAME_WLE)
    get_filename_component(OutputDir ${CMAKE_BINARY_DIR}/${protoRel} DIRECTORY)
    list(TRANSFORM ProtocExtraOutsSuf PREPEND ${OutputDir}/${OutputBase} OUTPUT_VARIABLE ProtocExtraOuts)
    add_custom_command(
        OUTPUT
          ${OutputDir}/${OutputBase}.pb.cc
          ${OutputDir}/${OutputBase}.pb.h
          ${ProtocExtraOuts}
        COMMAND protoc
          ${COMMON_PROTOC_FLAGS}
          -I=${CMAKE_SOURCE_DIR}/contrib/libs/protobuf/src
          --cpp_out=${CMAKE_BINARY_DIR}
          "$<JOIN:$<TARGET_GENEX_EVAL:${Tgt},$<TARGET_PROPERTY:${Tgt},PROTOC_OPTS>>,;>"
          ${protoRel}
        DEPENDS ${proto} $<TARGET_PROPERTY:${Tgt},PROTOC_DEPS>
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        COMMAND_EXPAND_LISTS
    )
    target_sources(${Tgt} ${Scope}
      ${OutputDir}/${OutputBase}.pb.cc ${OutputDir}/${OutputBase}.pb.h
      ${ProtocExtraOuts}
    )
  endforeach()
endfunction()
