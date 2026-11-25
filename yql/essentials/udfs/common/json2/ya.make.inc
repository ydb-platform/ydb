YQL_ABI_VERSION(
    2
    28
    0
)

SRCS(
    yql/essentials/udfs/common/json2/json2_udf.cpp
    yql/essentials/udfs/common/json2/as_json_node.cpp
    yql/essentials/udfs/common/json2/compile_path.cpp
    yql/essentials/udfs/common/json2/parse.cpp
    yql/essentials/udfs/common/json2/resource.cpp
    yql/essentials/udfs/common/json2/serialize.cpp
    yql/essentials/udfs/common/json2/sql_exists.cpp
    yql/essentials/udfs/common/json2/sql_query.cpp
    yql/essentials/udfs/common/json2/sql_value.cpp
)

PEERDIR(
    yql/essentials/core/sql_types
    yql/essentials/types/binary_json/dom
    yql/essentials/minikql/dom
    yql/essentials/minikql/jsonpath
)
