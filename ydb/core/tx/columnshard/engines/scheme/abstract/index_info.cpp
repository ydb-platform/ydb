#include "index_info.h"
#include <ydb/core/sys_view/common/path.h>
#include <ydb/core/sys_view/common/schema.h>

namespace NKikimr::NOlap {

const TString IIndexInfo::STORE_INDEX_STATS_TABLE = TString("/") + NSysView::SysPathName + "/" + NSysView::StorePrimaryIndexStatsName;
const TString IIndexInfo::TABLE_INDEX_STATS_TABLE = TString("/") + NSysView::SysPathName + "/" + NSysView::TablePrimaryIndexStatsName;

std::shared_ptr<NKikimr::NOlap::TColumnLoader> IIndexInfo::GetColumnLoaderVerified(const ui32 columnId) const {
    auto result = GetColumnLoaderOptional(columnId);
    AFL_VERIFY(result);
    return result;
}

} // namespace NKikimr::NOlap
