#include "scheme_types_defs.h"
#include "scheme_tablecell.h"


namespace NKikimr {
namespace NScheme {
namespace NNames {

    DECLARE_TYPED_TYPE_NAME(ActorID);
    DECLARE_TYPED_TYPE_NAME(StepOrderId);

} // namespace NNames

    ::TString HasUnexpectedValueSize(const ::NKikimr::TRawTypeValue& value) {
        ::TString result;

        if (value) {
            const ui32 fixedSize = GetFixedSize(value.Type());
            if (fixedSize > 0 && value.Size() != fixedSize) {
                result = ::TStringBuilder()
                    << "Value with declared type " << NScheme::TypeName(value.Type())
                    << " has unexpected size " << value.Size()
                    << " (expected " << fixedSize << ")";
            }
        }

        return result;
    }

    ::TString HasUnexpectedValueSize(const ::NKikimr::TCell& value, const TTypeInfo& typeInfo) {
        ::TString result;

        if (value) {
            const ui32 fixedSize = GetFixedSize(typeInfo);
            if (fixedSize > 0 && value.Size() != fixedSize) {
                result = ::TStringBuilder()
                    << "Cell with declared type " << NScheme::TypeName(typeInfo)
                    << " has unexpected size " << value.Size()
                    << " (expected " << fixedSize << ")";
            }
        }

        return result;
    }

} // namespace NScheme
} // namespace NKikimr
