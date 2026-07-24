FUZZ()

SRCS(
    main.cpp
)

PEERDIR(
    contrib/libs/libfuzzer
    yql/essentials/minikql/computation/llvm16
    yql/essentials/minikql/dom
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
    yql/essentials/types/binary_json
)

YQL_LAST_ABI_VERSION()

END()
