LIBRARY()

LICENSE(Apache-2.0)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

OWNER(orivej)

PEERDIR(
    contrib/libs/gflags
    contrib/libs/protoc 
    contrib/libs/grpc/src/proto/grpc/reflection/v1alpha
    contrib/restricted/googletest/googlemock
    contrib/restricted/googletest/googletest
)

ADDINCL(
    ${ARCADIA_BUILD_ROOT}/contrib/libs/grpc
    contrib/libs/grpc
)

NO_COMPILER_WARNINGS()

SRCS(
    byte_buffer_proto_helper.cc
    # grpc_cli_libs:
    cli_call.cc
    cli_credentials.cc
    grpc_tool.cc
    proto_file_parser.cc
    service_describer.cc
    string_ref_helper.cc
    # grpc++_proto_reflection_desc_db:
    proto_reflection_descriptor_database.cc
    # grpc++_test_config:
    test_config_cc.cc
)

END()
