#include <ydb/library/yql/udfs/common/math/math_udf.cpp>

namespace NKikimr::NKqp {

NYql::NUdf::TUniquePtr<NYql::NUdf::IUdfModule> CreateMathModule() {
    return new ::TMathModule();
}

} // namespace NKikimr::NKqp
