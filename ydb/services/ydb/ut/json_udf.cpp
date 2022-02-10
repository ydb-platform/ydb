#include <ydb/library/yql/udfs/common/json2/json2_udf.cpp>

NYql::NUdf::TUniquePtr<NYql::NUdf::IUdfModule> CreateJson2Module() {
    return new NJson2Udf::TJson2Module();
}
