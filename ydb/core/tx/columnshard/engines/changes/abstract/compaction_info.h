#pragma once
#include <ydb/library/accessor/positive_integer.h>
#include <ydb/library/actors/core/monotonic.h>
#include <ydb/core/tx/columnshard/common/path_id.h>

#include <util/generic/string.h>
#include <util/stream/output.h>
#include <util/system/yassert.h>

#include <memory>

namespace NKikimr::NOlap {
class TGranuleMeta;

class TPlanCompactionInfo {
private:
    TUnifiedPathId PathId;
    TMonotonic StartTime = TMonotonic::Now();
    TPositiveControlInteger Count;

public:
    void Start() {
        StartTime = TMonotonic::Now();
        ++Count;
    }

    bool Finish();

    TMonotonic GetStartTime() const {
        return StartTime;
    }

    TPlanCompactionInfo(const TUnifiedPathId& pathId)
        : PathId(pathId) {
    }

    TUnifiedPathId GetPathId() const {
        return PathId;
    }
};

}   // namespace NKikimr::NOlap
