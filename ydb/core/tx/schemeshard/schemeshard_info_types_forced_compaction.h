#pragma once

#include "schemeshard_info_types_base.h"

namespace NKikimr {
namespace NSchemeShard {

// namespace NForcedCompaction {
struct TForcedCompactionInfo : TSimpleRefCount<TForcedCompactionInfo> {
    using TPtr = TIntrusivePtr<TForcedCompactionInfo>;

    enum class EState: ui8 {
        Invalid = 0,
        InProgress = 1,
        Done = 2,
        Cancelled = 3,
        Cancelling = 4,
    };

    ui64 Id;  // TxId from the original TEvCreateRequest
    EState State = EState::Invalid;
    TPathId TablePathId;
    TPathId SubdomainPathId;
    bool Cascade;
    ui32 MaxShardsInFlight;

    TInstant StartTime = TInstant::Zero();
    TInstant EndTime = TInstant::Zero();

    TMaybe<TString> UserSID;

    THashSet<TPathId> TablesToCompact;
    ui32 TotalShardCount = 0;
    ui32 DoneShardCount = 0; // updates only when persisting

    THashSet<TShardIdx> ShardsInFlight;

    TSet<TActorId> Subscribers;

    bool IsFinished() const;
    void AddNotifySubscriber(const TActorId& actorId);
    float CalcProgress() const;
};
// } // NForcedCompaction

}
}
