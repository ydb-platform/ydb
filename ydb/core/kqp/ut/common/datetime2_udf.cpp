#include <ydb/library/yql/udfs/common/datetime2/datetime_udf.cpp>

namespace NKikimr::NKqp {

NYql::NUdf::TUniquePtr<NYql::NUdf::IUdfModule> CreateDateTime2Module() {
    return new ::TDateTime2Module();
}

} // namespace NKikimr::NKqp