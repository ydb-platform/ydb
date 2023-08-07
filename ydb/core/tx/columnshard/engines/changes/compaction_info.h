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
    bool InternalFlag = false;
    const TMonotonic StartTime = TMonotonic::Now();
public:
    TMonotonic GetStartTime() const {
        return StartTime;
    }

    TPlanCompactionInfo(const ui64 pathId, const bool internalFlag)
        : PathId(pathId)
        , InternalFlag(internalFlag) {

    }

    ui64 GetPathId() const {
        return PathId;
    }

    bool IsInternal() const {
        return InternalFlag;
    }
};

struct TCompactionInfo {
private:
    std::shared_ptr<TGranuleMeta> GranuleMeta;
    const bool InGranuleFlag = false;
public:
    TCompactionInfo(std::shared_ptr<TGranuleMeta> granule, const bool inGranule)
        : GranuleMeta(granule)
        , InGranuleFlag(inGranule)
    {
        Y_VERIFY(granule);
    }

    bool InGranule() const {
        return InGranuleFlag;
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
