#pragma once
#include <ydb/core/tx/columnshard/common/path_id.h>

namespace NKikimr::NColumnShard {

class TInternalPathIdMapper {
public:
    NColumnShard::TInternalPathId GetLocalPathIId(const NColumnShard::TLocalPathId) {
        //TODO implement me
        return {};
    }
    NColumnShard::TLocalPathId GetLocalPathIdByInernalPathId(const NColumnShard::TInternalPathId) {
        return {};
    }
};

} //namespace NKikimr::NColumnShard