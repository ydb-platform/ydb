#include <yql/essentials/udfs/common/digest/digest_udf.cpp>

namespace NKikimr::NKqp {

NYql::NUdf::TUniquePtr<NYql::NUdf::IUdfModule> CreateDigestModule() {
    return new ::TDigestModule();
}

} // namespace NKikimr::NKqp
