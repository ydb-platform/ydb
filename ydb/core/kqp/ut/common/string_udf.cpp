#include <ydb/library/yql/udfs/common/string/string_udf.cpp>

namespace NKikimr::NKqp {

NYql::NUdf::TUniquePtr<NYql::NUdf::IUdfModule> CreateStringModule() {
    return new ::TStringModule();
}


} // namespace NKikimr::NKqp
