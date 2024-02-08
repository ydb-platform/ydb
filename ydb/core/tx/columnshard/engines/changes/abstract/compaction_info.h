#pragma once
#include <util/generic/string.h>
#include <util/system/yassert.h>
#include <util/stream/output.h>
#include <ydb/library/actors/core/monotonic.h>
#include <memory>

namespace NKikimr::NOlap {
class TGranuleMeta;

class TPlanCompactionInfo {
private:
    ui64 PathId = 0;
    const TMonotonic StartTime = TMonotonic::Now();
public:
    TMonotonic GetStartTime() const {
        return StartTime;
    }

    TPlanCompactionInfo(const ui64 pathId)
        : PathId(pathId) {

    }

    ui64 GetPathId() const {
        return PathId;
    }
};

}
