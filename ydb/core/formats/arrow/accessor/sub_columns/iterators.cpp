#include "iterators.h"
#include <ydb/core/formats/arrow/accessor/common/types.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

NJson::TJsonValue TGeneralIterator::GetValue() const {
    YDB_LOG_VERIFY(IsValidFlag);
    YDB_LOG_VERIFY(CurrentValue);
    return CurrentValue->ToJsonValue();
}

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
