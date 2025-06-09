#include <yql/essentials/udfs/common/unicode_base/unicode_base.cpp>

namespace NKikimr::NKqp {

NYql::NUdf::TUniquePtr<NYql::NUdf::IUdfModule> CreateUnicodeModule() {
    return new ::TUnicodeModule();
}

} // namespace NKikimr::NKqp
