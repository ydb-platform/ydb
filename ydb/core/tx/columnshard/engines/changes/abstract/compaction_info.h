#pragma once
#include "mark.h"
#include <util/generic/string.h>
#include <util/system/yassert.h>
#include <util/stream/output.h>
#include <library/cpp/actors/core/monotonic.h>
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

struct TCompactionInfo {
private:
    std::shared_ptr<TGranuleMeta> GranuleMeta;
public:
    TCompactionInfo(std::shared_ptr<TGranuleMeta> granule)
        : GranuleMeta(granule)
    {
        Y_VERIFY(granule);
    }

    std::shared_ptr<TGranuleMeta> GetGranule() const {
        return GranuleMeta;
    }

};

struct TCompactionSrcGranule {
    TMark Mark;

    TCompactionSrcGranule(const TMark& mark)
        : Mark(mark)
    {
    }
};

}
