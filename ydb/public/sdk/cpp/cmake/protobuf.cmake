function(_ydb_sdk_gen_proto_messages Tgt Scope Grpc)
  get_property(PublicProtos TARGET ${Tgt} PROPERTY PUBLIC_PROTOS)
  foreach(ProtoPath ${ARGN})
    get_filename_component(OutputBase ${ProtoPath} NAME_WLE)
    file(RELATIVE_PATH ProtoRelPath ${YDB_SDK_SOURCE_DIR} ${ProtoPath})
    set(OutDir "")
    if (${OutputBase} IN_LIST PublicProtos)
      get_property(OutDir TARGET ${Tgt} PROPERTY PROTOS_OUT_DIR)
      get_filename_component(GenDir ${YDB_SDK_BINARY_DIR}/${OutDir}/${ProtoRelPath} DIRECTORY)
    else()
      get_filename_component(GenDir ${YDB_SDK_BINARY_DIR}/${ProtoRelPath} DIRECTORY)
    endif()

    set(ProtocOuts
      ${GenDir}/${OutputBase}.pb.cc
      ${GenDir}/${OutputBase}.pb.h
    )
    set(Opts "")
    if (Grpc)
      list(APPEND ProtocOuts
        ${GenDir}/${OutputBase}.grpc.pb.cc
        ${GenDir}/${OutputBase}.grpc.pb.h
      )
      list(APPEND Opts
        "--grpc_cpp_out=${YDB_SDK_BINARY_DIR}/${OutDir}/$<TARGET_PROPERTY:${Tgt},PROTO_NAMESPACE>" 
        "--plugin=protoc-gen-grpc_cpp=$<TARGET_GENEX_EVAL:${Tgt},$<TARGET_FILE:gRPC::grpc_cpp_plugin>>"
      )
    endif()

    add_custom_command(
      OUTPUT
        ${ProtocOuts}
      COMMAND protobuf::protoc
        ${COMMON_PROTOC_FLAGS}
        "-I$<JOIN:$<TARGET_GENEX_EVAL:${Tgt},$<TARGET_PROPERTY:${Tgt},PROTO_ADDINCL>>,;-I>"
        "--cpp_out=${YDB_SDK_BINARY_DIR}/${OutDir}/$<TARGET_PROPERTY:${Tgt},PROTO_NAMESPACE>"
        "${Opts}"
        "--experimental_allow_proto3_optional"
        ${ProtoRelPath}
      DEPENDS
        ${ProtoPath}
      WORKING_DIRECTORY ${YDB_SDK_SOURCE_DIR}
      COMMAND_EXPAND_LISTS
    )
    target_sources(${Tgt} ${Scope}
      ${ProtocOuts}
    )
  endforeach()
endfunction()

function(_ydb_sdk_init_proto_library_impl Tgt USE_API_COMMON_PROTOS)
  _ydb_sdk_add_library(${Tgt})
  target_link_libraries(${Tgt} PUBLIC
    protobuf::libprotobuf
  )

  set(proto_incls ${YDB_SDK_SOURCE_DIR})

  if (USE_API_COMMON_PROTOS)
    target_link_libraries(${Tgt} PUBLIC
      api-common-protos
    )
    list(APPEND proto_incls ${YDB_SDK_SOURCE_DIR}/third_party/api-common-protos)
  endif()

  set_property(TARGET ${Tgt} PROPERTY 
    PROTO_ADDINCL ${proto_incls}
  )

  foreach(ProtoPath ${ARGN})
    get_filename_component(OutputBase ${ProtoPath} NAME_WLE)
    set_property(TARGET ${Tgt} APPEND PROPERTY
      PUBLIC_PROTOS ${OutputBase}
    )
  endforeach()
endfunction()

function(_ydb_sdk_add_proto_library Tgt)
  set(opts "USE_API_COMMON_PROTOS" "GRPC")
  set(oneValueArgs "OUT_DIR")
  set(multiValueArgs "SOURCES" "PUBLIC_PROTOS")
  cmake_parse_arguments(
    ARG "${opts}" "${oneValueArgs}" "${multiValueArgs}" "${ARGN}"
  )

  _ydb_sdk_init_proto_library_impl(${Tgt} ${ARG_USE_API_COMMON_PROTOS} ${ARG_PUBLIC_PROTOS})

  if (ARG_GRPC)
    target_link_libraries(${Tgt} PUBLIC
      gRPC::grpc++
    )
  endif()

  set_property(TARGET ${Tgt} PROPERTY
    PROTOS_OUT_DIR ${ARG_OUT_DIR}
  )

  _ydb_sdk_gen_proto_messages(${Tgt} PRIVATE ${ARG_GRPC} ${ARG_SOURCES})
endfunction()
