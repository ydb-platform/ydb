#include "iterators.h"
#include "types.h"

namespace NKikimr::NArrow::NAccessor::NSubColumns {

NJson::TJsonValue TGeneralIterator::GetValue() const {
    AFL_VERIFY(IsValidFlag);
    return ArrayElementToJsonValue(*CurrentArray, LocalIndex, ValueType);
}

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
