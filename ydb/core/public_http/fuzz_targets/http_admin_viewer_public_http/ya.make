FUZZ()

FUZZ_DICTS(
    ydb/core/public_http/fuzz_targets/http_admin_viewer_public_http/http_admin_viewer_public_http.dict
)

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/monlib/service
    library/cpp/protobuf/json
    ydb/core/public_http
    ydb/core/viewer
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
