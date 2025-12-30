#include <ydb/library/yql/udfs/common/fulltext/fulltext_udf.cpp>

namespace NKikimr::NKqp {

NYql::NUdf::TUniquePtr<NYql::NUdf::IUdfModule> CreateFullTextModule() {
    return new ::TFullTextModule();
}

} // namespace NKikimr::NKqp
