YQL_LIBRARY()

PEERDIR_FROM_POLICY(
    ALLOW
    yql/udfs/antirobot/decrypt_hypocrisy_fingerprint$
    yql/udfs/dj/fs_module/dj_fs$
    yql/udfs/dj/fs_module/dj_fs_test$
    yql/udfs/modadvert/bigmod
)

PEERDIR(
    library/cpp/deprecated/enum_codegen
    library/cpp/containers/stack_vector
    library/cpp/json
    library/cpp/yson_pull
    yql/essentials/public/udf
    yql/essentials/utils
)

SRCS(
    node.cpp
    json.cpp
    yson.cpp
    make.cpp
    peel.cpp
    hash.cpp
    convert.cpp
)

END()

RECURSE_FOR_TESTS(
    benchmark
    ut
)
