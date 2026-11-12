#include "metadata.h"

#include <ydb/core/tx/columnshard/engines/column_engine.h>

namespace NKikimr::NOlap::NReader::NTrivial::NSysView::NAbstract {

bool TAccessor::ArePortionsLoaded(const IColumnEngine& engine, const TPKRangesFilter& /*ranges*/) const {
    return engine.AreAllPortionsLoaded(GetTableFilterPathId());
}

}   // namespace NKikimr::NOlap::NReader::NTrivial::NSysView::NAbstract
