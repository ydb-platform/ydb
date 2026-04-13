#pragma once

#include <ydb/core/base/row_version.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/library/actors/core/actorid.h>
#include <util/datetime/base.h>
#include <util/generic/vector.h>
#include <util/generic/set.h>

namespace NKikimr {
namespace NLongTxService {

struct TLocalSnapshotInfo {
    TRowVersion Snapshot;
    TInstant CreationTime;
    NActors::TActorId SessionActorId;
    TVector<NKikimr::TTableId> TableIds;
    std::shared_ptr<std::atomic<bool>> AliveFlag = nullptr;

    TLocalSnapshotInfo() = delete;

    TLocalSnapshotInfo(
        const TRowVersion& snapshot,
        const NActors::TActorId& sessionActorId,
        TVector<NKikimr::TTableId> tableIds,
        const TInstant creationTime)
        : Snapshot(snapshot)
        , CreationTime(creationTime)
        , SessionActorId(sessionActorId)
        , TableIds(std::move(tableIds))
        , AliveFlag(std::make_shared<std::atomic<bool>>(true))
    {}

    struct TComparatorByCreateTimeFirst {
        bool operator()(const TLocalSnapshotInfo& lhs, const TLocalSnapshotInfo& rhs) const {
            return std::tie(lhs.CreationTime, lhs.Snapshot, lhs.SessionActorId) < std::tie(rhs.CreationTime, rhs.Snapshot, rhs.SessionActorId);
        }
    };
};

struct TRemoteSnapshotInfo {
    TRowVersion Snapshot;
    NActors::TActorId SessionActorId;
    TVector<NKikimr::TTableId> TableIds;

    TRemoteSnapshotInfo(
        const TRowVersion& snapshot,
        const NActors::TActorId& sessionActorId,
        TVector<NKikimr::TTableId> tableIds)
        : Snapshot(snapshot)
        , SessionActorId(sessionActorId)
        , TableIds(std::move(tableIds))
    {}

    struct TComparatorBySnapshotAndSessionId {
        bool operator()(const TRemoteSnapshotInfo& lhs, const TRemoteSnapshotInfo& rhs) const {
            return std::tie(lhs.Snapshot, lhs.SessionActorId) < std::tie(rhs.Snapshot, rhs.SessionActorId);
        }
    };
};

class TLocalSnapshotsStorage : public TThrRefBase {
public:
    void Insert(TLocalSnapshotInfo snapshot);
    void CleanExpired();
    void Clear();

    class TView : public TInputRangeAdaptor<TView> {
    private:
        friend class TInputRangeAdaptor<TView>;
        friend class TLocalSnapshotsStorage;

        using TConstIter = TSet<TLocalSnapshotInfo, TLocalSnapshotInfo::TComparatorByCreateTimeFirst>::const_iterator;

        TView(
                TConstIter begin,
                TConstIter end,
                TInstant maxCreationTime)
            : Iter(begin)
            , End(end)
            , MaxCreationTime(maxCreationTime)
        {}

        const TLocalSnapshotInfo* Next();

        TConstIter Iter;
        const TConstIter End;
        const TInstant MaxCreationTime;
    };

    TView View() const;

private:
    TSet<TLocalSnapshotInfo, TLocalSnapshotInfo::TComparatorByCreateTimeFirst> LocalSnapshots;
};

using TLocalSnapshotsStoragePtr = TIntrusivePtr<TLocalSnapshotsStorage>;
using TConstLocalSnapshotsStoragePtr = TIntrusiveConstPtr<TLocalSnapshotsStorage>;

class TRemoteSnapshotsStorage : public TThrRefBase {
    struct TNodeSnapshotsState {
        TInstant CollectionTime;
        TVector<TRemoteSnapshotInfo> RemoteSnapshots;
    };

public:
    void UpdateAndCleanExpired(const TVector<TRemoteSnapshotInfo>& snapshots, const THashMap<ui32, TInstant>& updatedNodeIdToCollectionTime);
    void UpdateBorder(const TRowVersion& border);
    void Clear();

    class TView : public TInputRangeAdaptor<TView> {
    private:
        friend class TInputRangeAdaptor<TView>;
        friend class TRemoteSnapshotsStorage;

        using TConstNodesIter = THashMap<ui32, TNodeSnapshotsState>::const_iterator;
        using TConstSnapshotsIter = TVector<TRemoteSnapshotInfo>::const_iterator;

        TView(
            TConstNodesIter begin,
            TConstNodesIter end);

        const TRemoteSnapshotInfo* Next();

        TConstSnapshotsIter SnapshotsIter;
        TConstNodesIter NodesIter;
        const TConstNodesIter NodesEnd;
    };

    TView View() const;
    TRowVersion GetBorder() const;

    bool IsReady() const;

private:
    THashMap<ui32, TNodeSnapshotsState> NodeIdToState;
    TRowVersion SnapshotBorder = TRowVersion::Max();
    bool Ready = false;
};

using TRemoteSnapshotsStoragePtr = TIntrusivePtr<TRemoteSnapshotsStorage>;

}
}
