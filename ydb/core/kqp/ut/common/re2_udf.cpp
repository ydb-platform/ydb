// HACK: the TRe2Module class is in an anonymous namespace
// so including the source cpp is the only way to access it
#include <yql/essentials/udfs/common/re2/re2_udf.cpp>

namespace NKikimr::NKqp {

NYql::NUdf::TUniquePtr<NYql::NUdf::IUdfModule> CreateRe2Module() {
    return new TRe2Module<true>();
}

} // namespace NKikimr::NKqp
