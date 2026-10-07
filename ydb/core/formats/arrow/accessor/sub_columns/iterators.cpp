#include "iterators.h"
#include <ydb/core/formats/arrow/accessor/common/types.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

NJson::TJsonValue TGeneralIterator::GetValue() const {
    AFL_VERIFY(IsValidFlag);
    AFL_VERIFY(CurrentValue);
    return CurrentValue->ToJsonValue();
}

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
