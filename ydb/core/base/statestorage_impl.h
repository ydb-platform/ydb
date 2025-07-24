#pragma once

#include "statestorage.h"

#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/protos/base.pb.h>

#include <util/string/join.h>

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

    TEvReplicaInfo(ui64 tabletId, NKikimrProto::EReplyStatus status, ui64 clusterStateGeneration, ui64 clusterStateGuid)
    {
        Record.SetTabletID(tabletId);
        Record.SetStatus(status);
        Record.SetClusterStateGeneration(clusterStateGeneration);
        Record.SetClusterStateGuid(clusterStateGuid);
    }

    TEvReplicaInfo(ui64 tabletId, const TActorId &currentLeader, const TActorId &currentLeaderTablet, ui32 currentGeneration
        , ui32 currentStep, bool locked, ui64 lockedFor, ui64 clusterStateGeneration, ui64 clusterStateGuid)
    {
        Record.SetStatus(NKikimrProto::OK);
        Record.SetClusterStateGeneration(clusterStateGeneration);
        Record.SetClusterStateGuid(clusterStateGuid);
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
        str << " ClusterStateGeneration: " << Record.GetClusterStateGeneration();
        str << " ClusterStateGuid: " << Record.GetClusterStateGuid();
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

struct TEvStateStorage::TEvRingGroupPassAway : public TEventLocal<TEvRingGroupPassAway, EvRingGroupPassAway> {
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

    TString ToString() const override {
        TStringStream str;
        str << "{EvUpdateGroupConfig"
            << " GroupConfig: " << (GroupConfig ? GroupConfig->ToString() : "empty")
            << " BoardConfig: " << (BoardConfig ? BoardConfig->ToString() : "empty")
            << " SchemeBoardConfig: " << (SchemeBoardConfig ? SchemeBoardConfig->ToString() : "empty")
            << "}";
        return str.Str();
    }
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
    struct TReplicaGroup {
        TVector<TActorId> Replicas;
        bool WriteOnly;
        ERingGroupState State;

        TString ToString() const {
            TStringStream str;
            str << "{Replicas: [" << JoinSeq(", ", Replicas) << "]"
                << " WriteOnly: " << WriteOnly
                << " State: " << static_cast<int>(State)
                << "}";
            return str.Str();
        }
    };

    TVector<TReplicaGroup> ReplicaGroups;
    ui32 ConfigContentHash = Max<ui32>();
    ui64 ClusterStateGeneration;
    ui64 ClusterStateGuid;

    TVector<TActorId> GetPlainReplicas() const {
        TVector<TActorId> result;
        ui32 size = 0;
        for (const auto& rg : ReplicaGroups) {
            size += rg.Replicas.size();
        }
        result.reserve(size);
        for (const auto& r : ReplicaGroups) {
            result.insert(result.end(), r.Replicas.begin(), r.Replicas.end());
        }
        return result;
    }

    TString ToString() const override {
        TStringStream str;
        str << "{EvResolveReplicasList"
            << " ReplicaGroups: [" << JoinSeq(", ", ReplicaGroups) << "]"
            << "}";
        return str.Str();
    }
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

    TEvReplicaLookup(ui64 tabletId, ui64 cookie, ui64 clusterStateGeneration, ui64 clusterStateGuid)
    {
        Record.SetTabletID(tabletId);
        Record.SetCookie(cookie);
        Record.SetClusterStateGeneration(clusterStateGeneration);
        Record.SetClusterStateGuid(clusterStateGuid);
    }

    TEvReplicaLookup(ui64 tabletId, ui64 cookie, ui64 clusterStateGeneration, ui64 clusterStateGuid, TActualityCounterPtr &actualityRefCounter)
        : ActualityRefCounter(actualityRefCounter)
    {
        Record.SetTabletID(tabletId);
        Record.SetCookie(cookie);
        Record.SetClusterStateGeneration(clusterStateGeneration);
        Record.SetClusterStateGuid(clusterStateGuid);
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

    TEvReplicaUpdate(ui64 tabletId, ui32 proposedGeneration, ui32 proposedStep, ui64 clusterStateGeneration, ui64 clusterStateGuid)
    {
        Record.SetTabletID(tabletId);
        Record.SetProposedGeneration(proposedGeneration);
        Record.SetProposedStep(proposedStep);
        Record.SetClusterStateGeneration(clusterStateGeneration);
        Record.SetClusterStateGuid(clusterStateGuid);
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

    TEvReplicaDelete(ui64 tabletId, ui64 clusterStateGeneration, ui64 clusterStateGuid)
    {
        Record.SetTabletID(tabletId);
        Record.SetClusterStateGeneration(clusterStateGeneration);
        Record.SetClusterStateGuid(clusterStateGuid);
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

    TEvReplicaCleanup(ui64 tabletId, TActorId proposedLeader, ui64 clusterStateGeneration, ui64 clusterStateGuid)
    {
        Record.SetTabletID(tabletId);
        Record.SetClusterStateGeneration(clusterStateGeneration);
        Record.SetClusterStateGuid(clusterStateGuid);
        ActorIdToProto(proposedLeader, Record.MutableProposedLeader());
    }
};

struct TEvStateStorage::TEvReplicaLock : public TEventPB<TEvStateStorage::TEvReplicaLock, NKikimrStateStorage::TEvLock, TEvStateStorage::EvReplicaLock> {
    TEvReplicaLock()
    {}

    TEvReplicaLock(ui64 tabletId, ui32 proposedGeneration, ui64 clusterStateGeneration, ui64 clusterStateGuid)
    {
        Record.SetTabletID(tabletId);
        Record.SetProposedGeneration(proposedGeneration);
        Record.SetClusterStateGeneration(clusterStateGeneration);
        Record.SetClusterStateGuid(clusterStateGuid);
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

    TEvReplicaBoardPublish(const TString &path, const TString &payload, ui64 ttlMs, bool reg, TActorId owner, ui64 clusterStateGeneration, ui64 clusterStateGuid)
    {
        Record.SetPath(path);
        Record.SetPayload(payload);
        Record.SetTtlMs(ttlMs);
        Record.SetRegister(reg);
        Record.SetClusterStateGeneration(clusterStateGeneration);
        Record.SetClusterStateGuid(clusterStateGuid);
        ActorIdToProto(owner, Record.MutableOwner());
    }
};

struct TEvStateStorage::TEvReplicaBoardLookup : public TEventPB<TEvStateStorage::TEvReplicaBoardLookup, NKikimrStateStorage::TEvReplicaBoardLookup, TEvStateStorage::EvReplicaBoardLookup> {
    TEvReplicaBoardLookup()
    {}

    TEvReplicaBoardLookup(const TString &path, bool sub, ui64 clusterStateGeneration, ui64 clusterStateGuid)
    {
        Record.SetPath(path);
        Record.SetSubscribe(sub);
        Record.SetClusterStateGeneration(clusterStateGeneration);
        Record.SetClusterStateGuid(clusterStateGuid);
    }
};

struct TEvStateStorage::TEvReplicaBoardCleanup : public TEventPB<TEvStateStorage::TEvReplicaBoardCleanup, NKikimrStateStorage::TEvReplicaBoardCleanup, TEvStateStorage::EvReplicaBoardCleanup> {
    TEvReplicaBoardCleanup()
    {}

    TEvReplicaBoardCleanup(ui64 clusterStateGeneration, ui64 clusterStateGuid)
    {
        Record.SetClusterStateGeneration(clusterStateGeneration);
        Record.SetClusterStateGuid(clusterStateGuid);
    }
};

struct TEvStateStorage::TEvReplicaBoardUnsubscribe : public TEventPB<TEvStateStorage::TEvReplicaBoardUnsubscribe, NKikimrStateStorage::TEvReplicaBoardUnsubscribe, TEvStateStorage::EvReplicaBoardUnsubscribe> {
    TEvReplicaBoardUnsubscribe()
    {}

    TEvReplicaBoardUnsubscribe(ui64 clusterStateGeneration, ui64 clusterStateGuid)
    {
        Record.SetClusterStateGeneration(clusterStateGeneration);
        Record.SetClusterStateGuid(clusterStateGuid);
    }
};

struct TEvStateStorage::TEvReplicaBoardPublishAck : public TEventPB<TEvStateStorage::TEvReplicaBoardPublishAck, NKikimrStateStorage::TEvReplicaBoardPublishAck, TEvStateStorage::EvReplicaBoardPublishAck> {
    TEvReplicaBoardPublishAck()
    {}
};

struct TEvStateStorage::TEvReplicaBoardInfo : public TEventPB<TEvStateStorage::TEvReplicaBoardInfo, NKikimrStateStorage::TEvReplicaBoardInfo, TEvStateStorage::EvReplicaBoardInfo> {

    TEvReplicaBoardInfo()
    {}

    TEvReplicaBoardInfo(const TString &path, bool dropped, ui64 clusterStateGeneration, ui64 clusterStateGuid)
    {
        Record.SetPath(path);
        Record.SetDropped(dropped);
        Record.SetClusterStateGeneration(clusterStateGeneration);
        Record.SetClusterStateGuid(clusterStateGuid);
    }
};

struct TEvStateStorage::TEvReplicaBoardInfoUpdate : public TEventPB<TEvStateStorage::TEvReplicaBoardInfoUpdate, NKikimrStateStorage::TEvReplicaBoardInfoUpdate, TEvStateStorage::EvReplicaBoardInfoUpdate> {

    TEvReplicaBoardInfoUpdate()
    {}

    TEvReplicaBoardInfoUpdate(const TString &path, ui64 clusterStateGeneration, ui64 clusterStateGuid)
    {
        Record.SetPath(path);
        Record.SetClusterStateGeneration(clusterStateGeneration);
        Record.SetClusterStateGuid(clusterStateGuid);
    }
};

}

Y_DECLARE_OUT_SPEC(inline, NKikimr::TEvStateStorage::TEvResolveReplicasList::TReplicaGroup, o, x) {
    o << x.ToString();
}
