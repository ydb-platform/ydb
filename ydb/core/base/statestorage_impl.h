#pragma once
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/protos/base.pb.h>
#include "statestorage.h"

namespace NKikimr {

inline TActorId MakeStateStorageReplicaID(ui32 node, ui32 replicaIndex) {
    char x[12] = { 's', 't', 's' };
    x[3] = (char)1; // stateStorageGroup
    memcpy(x + 5, &replicaIndex, sizeof(ui32));
    return TActorId(node, TStringBuf(x, 12));
}

struct TEvStateStorage::TEvReplicaInfo : public TEventPB<TEvStateStorage::TEvReplicaInfo, NKikimrStateStorage::TEvInfo, TEvStateStorage::EvReplicaInfo> {
    TEvReplicaInfo()
    {}

    TEvReplicaInfo(ui64 tabletId, NKikimrProto::EReplyStatus status)
    {
        Record.SetTabletID(tabletId);
        Record.SetStatus(status);
    }

    TEvReplicaInfo(ui64 tabletId, const TActorId &currentLeader, const TActorId &currentLeaderTablet, ui32 currentGeneration, ui32 currentStep, bool locked, ui64 lockedFor)
    {
        Record.SetStatus(NKikimrProto::OK);

        Record.SetTabletID(tabletId);
        ActorIdToProto(currentLeader, Record.MutableCurrentLeader());
        ActorIdToProto(currentLeaderTablet, Record.MutableCurrentLeaderTablet());
        Record.SetCurrentGeneration(currentGeneration);
        Record.SetCurrentStep(currentStep);
        if (locked) {
            Record.SetLocked(locked);
            Record.SetLockedFor(lockedFor);
        }
    }

    TString ToString() const {
        TStringStream str;
        str << "{EvReplicaInfo Status: " << (ui32)Record.GetStatus();
        str << " TabletID: " << Record.GetTabletID();
        if (Record.HasCurrentLeader()) {
            str << " CurrentLeader: " << ActorIdFromProto(Record.GetCurrentLeader()).ToString();
        }
        if (Record.HasCurrentLeaderTablet()) {
            str << " CurrentLeaderTablet: " << ActorIdFromProto(Record.GetCurrentLeaderTablet()).ToString();
        }
        if (Record.HasCurrentGeneration()) {
            str << " CurrentGeneration: " << Record.GetCurrentGeneration();
        }
        if (Record.HasCurrentStep()) {
            str << " CurrentStep: " << Record.GetCurrentStep();
        }
        if (Record.HasLocked()) {
            str << " Locked: " << (Record.GetLocked() ? "true" : "false");
        }
        if (Record.HasLockedFor()) {
            str << " LockedFor: " << Record.GetLockedFor();
        }
        str << "}";
        return str.Str();
    }

};

struct TEvStateStorage::TEvUpdateGroupConfig : public TEventLocal<TEvUpdateGroupConfig, EvUpdateGroupConfig> {
    TIntrusivePtr<TStateStorageInfo> GroupConfig;
    TIntrusivePtr<TStateStorageInfo> BoardConfig;
    TIntrusivePtr<TStateStorageInfo> SchemeBoardConfig;

    TEvUpdateGroupConfig(
                const TIntrusivePtr<TStateStorageInfo> &info,
                const TIntrusivePtr<TStateStorageInfo> &board,
                const TIntrusivePtr<TStateStorageInfo> &scheme)
        : GroupConfig(info)
        , BoardConfig(board)
        , SchemeBoardConfig(scheme)
    {}
};

struct TEvStateStorage::TEvResolveReplicas : public TEventLocal<TEvResolveReplicas, EvResolveReplicas> {
    const ui64 TabletID;
    const bool Subscribe;

    TEvResolveReplicas(ui64 tabletId, bool subscribe = false)
        : TabletID(tabletId)
        , Subscribe(subscribe)
    {}
};

struct TEvStateStorage::TEvResolveBoard : public TEventLocal<TEvResolveBoard, EvResolveBoard> {
    const TString Path;
    const bool Subscribe;

    TEvResolveBoard(const TString &path, bool subscribe = false)
        : Path(path)
        , Subscribe(subscribe)
    {}
};

struct TEvStateStorage::TEvResolveSchemeBoard : public TEventLocal<TEvResolveSchemeBoard, EvResolveSchemeBoard> {
    enum EKeyType {
        KeyTypePath,
        KeyTypePathId,
    };

    const TString Path;
    const TPathId PathId;

    const EKeyType KeyType;
    const bool Subscribe;

    TEvResolveSchemeBoard(const TString &path, bool subscribe = false)
        : Path(path)
        , KeyType(KeyTypePath)
        , Subscribe(subscribe)
    {}

    TEvResolveSchemeBoard(const TPathId& pathId, bool subscribe = false)
        : PathId(pathId)
        , KeyType(KeyTypePathId)
        , Subscribe(subscribe)
    {}
};

struct TEvStateStorage::TEvResolveReplicasList : public TEventLocal<TEvResolveReplicasList, EvResolveReplicasList> {
    TVector<TActorId> Replicas;
    ui32 ConfigContentHash = Max<ui32>();
};

struct TEvStateStorage::TEvListSchemeBoard : public TEventLocal<TEvListSchemeBoard, EvListSchemeBoard> {
    const bool Subscribe = false;

    TEvListSchemeBoard(bool subscribe)
        : Subscribe(subscribe)
    {}
};

struct TEvStateStorage::TEvListSchemeBoardResult : public TEventLocal<TEvListSchemeBoardResult, EvListSchemeBoardResult> {
    TIntrusiveConstPtr<TStateStorageInfo> Info;

    TEvListSchemeBoardResult(const TIntrusiveConstPtr<TStateStorageInfo> &info)
        : Info(info)
    {}
};

struct TEvStateStorage::TEvListStateStorage : public TEventLocal<TEvListStateStorage, EvListStateStorage> {
};

struct TEvStateStorage::TEvListStateStorageResult : public TEventLocal<TEvListStateStorageResult, EvListStateStorageResult> {
    TIntrusiveConstPtr<TStateStorageInfo> Info;

    TEvListStateStorageResult(const TIntrusiveConstPtr<TStateStorageInfo> &info)
        : Info(info)
    {}
};

struct TEvStateStorage::TEvPublishActorGone : public TEventLocal<TEvPublishActorGone, EvPublishActorGone> {
    TActorId Replica;

    TEvPublishActorGone(const TActorId& replica)
        : Replica(replica)
    {}
};

struct TEvStateStorage::TEvReplicaLookup : public TEventPB<TEvStateStorage::TEvReplicaLookup, NKikimrStateStorage::TEvLookup, TEvStateStorage::EvReplicaLookup>{
    struct TActualityCounter : public TRefCounted<TActualityCounter, TAtomicCounter> {};
    using TActualityCounterPtr = TIntrusivePtr<TActualityCounter>;
    TActualityCounterPtr ActualityRefCounter;

    TEvReplicaLookup()
    {}

    TEvReplicaLookup(ui64 tabletId, ui64 cookie)
    {
        Record.SetTabletID(tabletId);
        Record.SetCookie(cookie);
    }

    TEvReplicaLookup(ui64 tabletId, ui64 cookie, TActualityCounterPtr &actualityRefCounter)
        : ActualityRefCounter(actualityRefCounter)
    {
        Record.SetTabletID(tabletId);
        Record.SetCookie(cookie);
    }

    TString ToString() const {
        TStringStream str;
        str << "{EvReplicaLookup TabletID: " << Record.GetTabletID();
        str << " Cookie: " << Record.GetCookie();
        str << "}";
        return str.Str();
    }
};

struct TEvStateStorage::TEvReplicaUpdate : public TEventPB<TEvStateStorage::TEvReplicaUpdate, NKikimrStateStorage::TEvUpdate, TEvStateStorage::EvReplicaUpdate> {
    TEvReplicaUpdate()
    {}

    TEvReplicaUpdate(ui64 tabletId, ui32 proposedGeneration, ui32 proposedStep)
    {
        Record.SetTabletID(tabletId);
        Record.SetProposedGeneration(proposedGeneration);
        Record.SetProposedStep(proposedStep);
    }

    TString ToString() const {
        TStringStream str;
        str << "{EvReplicaUpdate TabletID: " << Record.GetTabletID();
        str << " ProposedGeneration: " << Record.GetProposedGeneration();
        str << " ProposedStep: " << Record.GetProposedStep();
        str << "}";
        return str.Str();
    }
};

struct TEvStateStorage::TEvReplicaDelete : public TEventPB<TEvStateStorage::TEvReplicaDelete, NKikimrStateStorage::TEvDelete, TEvStateStorage::EvReplicaDelete> {
    TEvReplicaDelete()
    {}

    TEvReplicaDelete(ui64 tabletId)
    {
        Record.SetTabletID(tabletId);
    }

    TString ToString() const {
        TStringStream str;
        str << "{EvReplicaUpdate TabletID: " << Record.GetTabletID();
        str << "}";
        return str.Str();
    }
};

struct TEvStateStorage::TEvReplicaCleanup : public TEventPB<TEvStateStorage::TEvReplicaCleanup, NKikimrStateStorage::TEvCleanup, TEvStateStorage::EvReplicaCleanup> {
    TEvReplicaCleanup()
    {}

    TEvReplicaCleanup(ui64 tabletId, TActorId proposedLeader)
    {
        Record.SetTabletID(tabletId);
        ActorIdToProto(proposedLeader, Record.MutableProposedLeader());
    }
};

struct TEvStateStorage::TEvReplicaLock : public TEventPB<TEvStateStorage::TEvReplicaLock, NKikimrStateStorage::TEvLock, TEvStateStorage::EvReplicaLock> {
    TEvReplicaLock()
    {}

    TEvReplicaLock(ui64 tabletId, ui32 proposedGeneration)
    {
        Record.SetTabletID(tabletId);
        Record.SetProposedGeneration(proposedGeneration);
    }

    TString ToString() const {
        TStringStream str;
        str << "{EvReplicaLock TabletID: " << Record.GetTabletID();
        str << " ProposedGeneration: " << Record.GetProposedGeneration();
        str << "}";
        return str.Str();
    }
};

struct TEvStateStorage::TEvReplicaBoardPublish : public TEventPB<TEvStateStorage::TEvReplicaBoardPublish, NKikimrStateStorage::TEvReplicaBoardPublish, TEvStateStorage::EvReplicaBoardPublish> {
    TEvReplicaBoardPublish()
    {}

    TEvReplicaBoardPublish(const TString &path, const TString &payload, ui64 ttlMs, bool reg, TActorId owner)
    {
        Record.SetPath(path);
        Record.SetPayload(payload);
        Record.SetTtlMs(ttlMs);
        Record.SetRegister(reg);
        ActorIdToProto(owner, Record.MutableOwner());
    }
};

struct TEvStateStorage::TEvReplicaBoardLookup : public TEventPB<TEvStateStorage::TEvReplicaBoardLookup, NKikimrStateStorage::TEvReplicaBoardLookup, TEvStateStorage::EvReplicaBoardLookup> {
    TEvReplicaBoardLookup()
    {}

    TEvReplicaBoardLookup(const TString &path, bool sub)
    {
        Record.SetPath(path);
        Record.SetSubscribe(sub);
    }
};

struct TEvStateStorage::TEvReplicaBoardCleanup : public TEventPB<TEvStateStorage::TEvReplicaBoardCleanup, NKikimrStateStorage::TEvReplicaBoardCleanup, TEvStateStorage::EvReplicaBoardCleanup> {
    TEvReplicaBoardCleanup()
    {}
};

struct TEvStateStorage::TEvReplicaBoardUnsubscribe : public TEventPB<TEvStateStorage::TEvReplicaBoardUnsubscribe, NKikimrStateStorage::TEvReplicaBoardUnsubscribe, TEvStateStorage::EvReplicaBoardUnsubscribe> {
    TEvReplicaBoardUnsubscribe()
    {}
};

struct TEvStateStorage::TEvReplicaBoardPublishAck : public TEventPB<TEvStateStorage::TEvReplicaBoardPublishAck, NKikimrStateStorage::TEvReplicaBoardPublishAck, TEvStateStorage::EvReplicaBoardPublishAck> {
    TEvReplicaBoardPublishAck()
    {}
};

struct TEvStateStorage::TEvReplicaBoardInfo : public TEventPB<TEvStateStorage::TEvReplicaBoardInfo, NKikimrStateStorage::TEvReplicaBoardInfo, TEvStateStorage::EvReplicaBoardInfo> {

    TEvReplicaBoardInfo()
    {}

    TEvReplicaBoardInfo(const TString &path, bool dropped)
    {
        Record.SetPath(path);
        Record.SetDropped(dropped);
    }
};

struct TEvStateStorage::TEvReplicaBoardInfoUpdate : public TEventPB<TEvStateStorage::TEvReplicaBoardInfoUpdate, NKikimrStateStorage::TEvReplicaBoardInfoUpdate, TEvStateStorage::EvReplicaBoardInfoUpdate> {

    TEvReplicaBoardInfoUpdate()
    {}

    TEvReplicaBoardInfoUpdate(const TString &path)
    {
        Record.SetPath(path);
    }
};

}
