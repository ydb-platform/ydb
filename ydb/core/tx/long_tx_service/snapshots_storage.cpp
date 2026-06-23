#include "snapshots_storage.h"

#include <ydb/core/base/appdata.h>
#include <library/cpp/time_provider/time_provider.h>
#include <ydb/core/protos/long_tx_service_config.pb.h>

namespace NKikimr {
namespace NLongTxService {

void TLocalSnapshotsStorage::Insert(TLocalSnapshotInfo snapshot) {
    LocalSnapshots.emplace(std::move(snapshot));
}

void TLocalSnapshotsStorage::CleanExpired() {
    for (auto it = LocalSnapshots.begin(); it != LocalSnapshots.end(); ) {
        if (!it->AliveFlag->load()) {
            it = LocalSnapshots.erase(it);
        } else {
            ++it;
        }
    }
}

void TLocalSnapshotsStorage::Clear() {
    LocalSnapshots.clear();
}

const TLocalSnapshotInfo* TLocalSnapshotsStorage::TView::Next() {
    while (Iter != End && (Iter->Snapshot.Step > MaxSnapshotStep || !Iter->AliveFlag->load())) {
        ++Iter;
    }

    if (Iter != End) {
        return &*(Iter++);
    }
    return nullptr;
}

TLocalSnapshotsStorage::TView TLocalSnapshotsStorage::View() const {
    return View(AppData()->TimeProvider->Now());
}

TLocalSnapshotsStorage::TView TLocalSnapshotsStorage::View(TInstant now) const {
    const ui64 promotionMs =
        TDuration::Seconds(AppData()->LongTxServiceConfig.GetLocalSnapshotPromotionTimeSeconds()).MilliSeconds();
    const ui64 nowMs = now.MilliSeconds();
    const ui64 maxSnapshotStep = nowMs > promotionMs ? nowMs - promotionMs : 0;
    return TLocalSnapshotsStorage::TView{
        LocalSnapshots.begin(),
        LocalSnapshots.end(),
        maxSnapshotStep};
}

void TRemoteSnapshotsStorage::Init(const TVector<TRemoteSnapshotInfo>& snapshots, const THashMap<ui32, TInstant>& nodeIdToCollectionTime) {
    AFL_ENSURE(NodeIdToState.empty());
    // Seed collection times from the peer first, so freshness (GetOldestCollectionTime) is meaningful
    // right after prefill instead of collapsing to Zero (which disables age-based cleanup).
    for (const auto& [nodeId, collectionTime] : nodeIdToCollectionTime) {
        NodeIdToState[nodeId].CollectionTime = collectionTime;
    }
    for (const auto& snapshot : snapshots) {
        const auto nodeId = snapshot.SessionActorId.NodeId();
        NodeIdToState[nodeId].RemoteSnapshots.push_back(snapshot);
    }
    Ready = true;
}


void TRemoteSnapshotsStorage::UpdateAndCleanExpired(const TVector<TRemoteSnapshotInfo>& snapshots, const THashMap<ui32, TInstant>& updatedNodeIdToCollectionTime) {
    const auto now = AppData()->TimeProvider->Now();

    THashSet<ui32> nodeIdsToUpdateSnapshots;
    THashSet<ui32> nodeIdsToDeleteSnapshots;

    for (const auto& [nodeId, state] : NodeIdToState) {
        if (!updatedNodeIdToCollectionTime.contains(nodeId)
                && state.CollectionTime + TDuration::Seconds(AppData()->LongTxServiceConfig.GetUnavailableNodeSnapshotsExpirationTimeSeconds()) < now) {
            nodeIdsToDeleteSnapshots.insert(nodeId);
        }
    }

    for (const auto& [nodeId, collectionTime] : updatedNodeIdToCollectionTime) {
        if (NodeIdToState[nodeId].CollectionTime < collectionTime) {
            nodeIdsToUpdateSnapshots.insert(nodeId);
            NodeIdToState.at(nodeId).CollectionTime = collectionTime;
        }
    }

    for (const auto& nodeId : nodeIdsToDeleteSnapshots) {
        NodeIdToState.erase(nodeId);
    }

    for (const auto& nodeId : nodeIdsToUpdateSnapshots) {
        NodeIdToState.at(nodeId).RemoteSnapshots.clear();
    }

    for (const auto& snapshot : snapshots) {
        const auto nodeId = snapshot.SessionActorId.NodeId();
        if (nodeIdsToUpdateSnapshots.contains(nodeId)) {
            NodeIdToState.at(nodeId).RemoteSnapshots.push_back(snapshot);
        }
    }

    Ready = true;
}

TRemoteSnapshotsStorage::TView::TView(
        TRemoteSnapshotsStorage::TView::TConstNodesIter begin,
        TRemoteSnapshotsStorage::TView::TConstNodesIter end)
    : NodesIter(begin)
    , NodesEnd(end)
{
    if (NodesIter != NodesEnd) {
        SnapshotsIter = NodesIter->second.RemoteSnapshots.begin();
    }
}

const TRemoteSnapshotInfo* TRemoteSnapshotsStorage::TView::Next() {
    if (NodesIter == NodesEnd) {
        return nullptr;
    }

    while (NodesIter != NodesEnd && SnapshotsIter == NodesIter->second.RemoteSnapshots.end()) {
        ++NodesIter;
        if (NodesIter != NodesEnd) {
            SnapshotsIter = NodesIter->second.RemoteSnapshots.begin();
        }
    }

    if (NodesIter == NodesEnd || SnapshotsIter == NodesIter->second.RemoteSnapshots.end()) {
        return nullptr;
    }

    return &*(SnapshotsIter++);
}

TRemoteSnapshotsStorage::TView TRemoteSnapshotsStorage::View() const {
    return TRemoteSnapshotsStorage::TView{NodeIdToState.begin(), NodeIdToState.end()};
}

void TRemoteSnapshotsStorage::UpdateBorder(const TRowVersion& border) {
    SnapshotBorder = border;
}

void TRemoteSnapshotsStorage::Clear() {
    NodeIdToState.clear();
    SnapshotBorder = TRowVersion::Max();
    Ready = false;
}

TRowVersion TRemoteSnapshotsStorage::GetBorder() const {
    return SnapshotBorder;
}

TInstant TRemoteSnapshotsStorage::GetOldestCollectionTime() const {
    TInstant oldest = AppData()->TimeProvider->Now();
    for (const auto& [nodeId, state] : NodeIdToState) {
        oldest = std::min(oldest, state.CollectionTime);
    }
    return oldest;
}

THashMap<ui32, TInstant> TRemoteSnapshotsStorage::GetNodeIdToCollectionTime() const {
    THashMap<ui32, TInstant> result;
    result.reserve(NodeIdToState.size());
    for (const auto& [nodeId, state] : NodeIdToState) {
        result[nodeId] = state.CollectionTime;
    }
    return result;
}

bool TRemoteSnapshotsStorage::IsReady() const {
    return Ready;
}

}
}
