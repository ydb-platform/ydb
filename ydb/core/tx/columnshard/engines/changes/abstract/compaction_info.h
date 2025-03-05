#pragma once
#include <ydb/library/accessor/positive_integer.h>
#include <ydb/library/actors/core/monotonic.h>

#include <util/generic/string.h>
#include <util/stream/output.h>
#include <util/system/yassert.h>

#include <memory>

namespace NKikimr::NOlap {
class TGranuleMeta;

class TPlanCompactionInfo {
private:
    ui64 PathId = 0;
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

    TPlanCompactionInfo(const ui64 pathId)
        : PathId(pathId) {
    }

    ui64 GetPathId() const {
        return PathId;
    }
};

}   // namespace NKikimr::NOlap
