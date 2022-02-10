PROTO_LIBRARY()

LICENSE(Apache-2.0)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt) 
 
OWNER(
    akastornov
    g:contrib
    g:cpp-contrib
)

EXCLUDE_TAGS(
    GO_PROTO
    PY_PROTO
    PY3_PROTO
)

PROTO_NAMESPACE( 
    GLOBAL 
    contrib/libs/grpc 
) 

PEERDIR(
    contrib/libs/grpc/src/proto/grpc/testing
)

GRPC()

SRCS(
    ads_for_test.proto
    cds_for_test.proto 
    eds_for_test.proto
    lds_rds_for_test.proto 
    lrs_for_test.proto
    orca_load_report_for_test.proto
)

END()
