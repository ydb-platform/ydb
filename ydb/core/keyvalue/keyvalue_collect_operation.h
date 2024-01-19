#pragma once
#include "defs.h"
#include "keyvalue_data_header.h"
#include <ydb/core/base/logoblob.h>
#include <util/system/unaligned_mem.h>

namespace NKikimr {
namespace NKeyValue {

struct TCollectOperationHeader {
    ui32 CollectGeneration;
    ui32 CollectStep;

    ui32 GetCollectGeneration() const { return CollectGeneration; }
    ui32 GetCollectStep() const { return CollectStep; }

    TCollectOperationHeader(ui32 collectGeneration, ui32 collectStep);
};

struct TCollectOperation : public TThrRefBase {
    TCollectOperationHeader Header;
    TVector<TLogoBlobID> Keep;
    TVector<TLogoBlobID> DoNotKeep;
    TVector<TLogoBlobID> TrashGoingToCollect;
    const bool AdvanceBarrier;

    TCollectOperation(ui32 collectGeneration, ui32 collectStep,
            TVector<TLogoBlobID> &&keep, TVector<TLogoBlobID> &&doNotKeep, TVector<TLogoBlobID>&& trashGoingToCollect,
            bool advanceBarrier)
        : Header(collectGeneration, collectStep)
        , Keep(std::move(keep))
        , DoNotKeep(std::move(doNotKeep))
        , TrashGoingToCollect(std::move(trashGoingToCollect))
        , AdvanceBarrier(advanceBarrier)
    {}
};

} // NKeyValue
} // NKikimr
