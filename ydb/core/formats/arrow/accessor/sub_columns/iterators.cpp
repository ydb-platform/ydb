#include "iterators.h"
#include "native_scalars.h"

namespace NKikimr::NArrow::NAccessor::NSubColumns {

NJson::TJsonValue TGeneralIterator::GetValue() const {
    AFL_VERIFY(IsValidFlag);
    if (RawValue.empty()) {
        return NJson::TJsonValue(NJson::JSON_UNDEFINED);
    }
    return NativeScalarToJsonValue(TStringBuf(RawValue.data(), RawValue.size()), ValueType);
}

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
