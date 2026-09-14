#include "iterators.h"
#include "types.h"

namespace NKikimr::NArrow::NAccessor::NSubColumns {

NJson::TJsonValue TGeneralIterator::GetValue() const {
    AFL_VERIFY(IsValidFlag);
    return ArrayElementToJsonValueView(*CurrentArray, LocalIndex, ValueType).ToJsonValue();
}

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
