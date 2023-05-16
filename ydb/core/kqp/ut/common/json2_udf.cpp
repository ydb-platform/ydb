#include <ydb/library/yql/udfs/common/json2/json2_udf.cpp>

namespace NKikimr::NKqp {

NYql::NUdf::TUniquePtr<NYql::NUdf::IUdfModule> CreateJson2Module() {
    return new NJson2Udf::TJson2Module();
}

} // namespace NKikimr::NKqp