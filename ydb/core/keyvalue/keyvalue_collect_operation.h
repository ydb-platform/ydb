#pragma once
#include "defs.h"
#include "keyvalue_data_header.h"
#include <ydb/core/base/logoblob.h>
#include <util/system/unaligned_mem.h>

namespace NKikimr {
namespace NKeyValue {

#pragma pack(push, 1)
struct TCollectOperationHeader {
    TDataHeader DataHeader;
    ui64 CollectGeneration;
    ui64 CollectStep;
    ui64 KeepCount;
    ui64 DoNotKeepCount;

    ui64 GetCollectGeneration() const {
        return ReadUnaligned<ui64>(&CollectGeneration);
    }
    ui64 GetCollectStep() const {
        return ReadUnaligned<ui64>(&CollectStep);
    }
    ui64 GetKeepCount() const {
        return ReadUnaligned<ui64>(&KeepCount);
    }
    ui64 GetDoNotKeepCount() const {
        return ReadUnaligned<ui64>(&DoNotKeepCount);
    }
    void SetCollectGeneration(ui64 value) {
        WriteUnaligned<ui64>(&CollectGeneration, value);
    }
    void SetCollectStep(ui64 value) {
        WriteUnaligned<ui64>(&CollectStep, value);
    }
    void SetKeepCount(ui64 value) {
        WriteUnaligned<ui64>(&KeepCount, value);
    }
    void SetDoNotKeepCount(ui64 value) {
        WriteUnaligned<ui64>(&DoNotKeepCount, value);
    }


    TCollectOperationHeader(ui64 collectGeneration, ui64 collectStep,
        TVector<TLogoBlobID> &keep, TVector<TLogoBlobID> &doNotKeep);
};
#pragma pack(pop)

struct TCollectOperation : public TThrRefBase {
    TCollectOperationHeader Header;
    TVector<TLogoBlobID> Keep;
    TVector<TLogoBlobID> DoNotKeep;
    TVector<TLogoBlobID> TrashGoingToCollect;

    TCollectOperation(ui64 collectGeneration, ui64 collectStep,
            TVector<TLogoBlobID> &&keep, TVector<TLogoBlobID> &&doNotKeep, TVector<TLogoBlobID>&& trashGoingToCollect)
        : Header(collectGeneration, collectStep, keep, doNotKeep)
        , Keep(std::move(keep))
        , DoNotKeep(std::move(doNotKeep))
        , TrashGoingToCollect(std::move(trashGoingToCollect))
    {}
};

} // NKeyValue
} // NKikimr
