#include "iterators.h"
#include "types.h"

namespace NKikimr::NArrow::NAccessor::NSubColumns {

NJson::TJsonValue TGeneralIterator::GetValue() const {
    AFL_VERIFY(IsValidFlag);
    return Codec->ReadValueView(*CurrentArray, LocalIndex).ToJsonValue();
}

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
