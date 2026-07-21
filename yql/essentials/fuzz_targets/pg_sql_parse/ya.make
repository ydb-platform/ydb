FUZZ()

SRCS(
    main.cpp
)

PEERDIR(
    yql/essentials/parser/pg_wrapper
    yql/essentials/parser/pg_wrapper/interface
    yql/essentials/public/udf/service/stub
)

YQL_LAST_ABI_VERSION()

END()
