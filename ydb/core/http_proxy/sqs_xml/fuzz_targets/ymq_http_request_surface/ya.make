FUZZ()

FUZZ_DICTS(
    ydb/core/http_proxy/sqs_xml/fuzz_targets/ymq_http_request_surface/ymq_http_request_surface.dict
)

SRCS(
    main.cpp
)

PEERDIR(
    ydb/core/http_proxy/sqs_xml
    library/cpp/cgiparam
    library/cpp/http/misc
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
    ydb/core/ymq/base
)

END()
