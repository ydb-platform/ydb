#include "index_info.h"
#include <ydb/core/sys_view/common/path.h>
#include <ydb/core/sys_view/common/schema.h>

namespace NKikimr::NOlap {

std::shared_ptr<NKikimr::NOlap::TColumnLoader> IIndexInfo::GetColumnLoaderVerified(const ui32 columnId) const {
    auto result = GetColumnLoaderOptional(columnId);
    AFL_VERIFY(result);
    return result;
}

} // namespace NKikimr::NOlap
