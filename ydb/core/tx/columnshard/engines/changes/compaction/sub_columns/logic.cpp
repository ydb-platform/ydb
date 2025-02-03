#include "logic.h"

#include <ydb/core/formats/arrow/accessor/sub_columns/constructor.h>

namespace NKikimr::NOlap::NCompaction {

const TSubColumnsMerger::TSettings& TSubColumnsMerger::GetSettings() const {
    return Context.GetLoader()->GetAccessorConstructor().GetObjectPtrVerifiedAs<NArrow::NAccessor::NSubColumns::TConstructor>()->GetSettings();
}

}   // namespace NKikimr::NOlap::NCompaction
