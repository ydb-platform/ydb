#include "snapshots_storage.h"

#include "public/snapshot_registry.h"

#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/time_provider/time_provider.h>
#include <util/stream/str.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/long_tx_service_config.pb.h>

namespace NKikimr {
namespace NLongTxService {
namespace {

void RenderTableIds(IOutputStream& str, const TVector<NKikimr::TTableId>& tableIds) {
    str << " Tables:";
    for (const auto& tableId : tableIds) {
        str << " " << tableId;
    }
}

ui64 SnapshotAgeSeconds(const ui64 nowMs, const TRowVersion& snapshot) {
    return nowMs > snapshot.Step ? (nowMs - snapshot.Step) / 1000 : 0;
}

// IImmutableSnapshotRegistryBuilder::AddSnapshot drops snapshots at or above the
// border: the registry returns true for them anyway (covered by the border).
const char* RegistryInputMarker(const TRowVersion& border, const TRowVersion& snapshot) {
    return snapshot < border ? " [registry input]" : " [covered by border]";
}

void RenderSnapshotLine(
        IOutputStream& str,
        const TStringBuf source,
        const TRowVersion& snapshot,
        const NActors::TActorId& sessionActorId,
        const TVector<NKikimr::TTableId>& tableIds,
        const TRowVersion& border,
        const ui64 nowMs) {
    str << "    ";
    if (source) {
        str << source << " ";
    }
    str << "Snapshot [Step " << snapshot.Step << ", TxId " << snapshot.TxId << "]"
        << " Age " << SnapshotAgeSeconds(nowMs, snapshot) << "s"
        << " SessionActorId " << sessionActorId
        << RegistryInputMarker(border, snapshot);
    RenderTableIds(str, tableIds);
    str << Endl;
}

TDuration Age(const TInstant now, const TInstant since) {
    return now > since ? now - since : TDuration::Zero();
}

ui64 PromotionCutoffStep(const TInstant now, const TDuration promotionTime) {
    const ui64 nowMs = now.MilliSeconds();
    const ui64 promotionMs = promotionTime.MilliSeconds();
    return nowMs > promotionMs ? nowMs - promotionMs : 0;
}

} // namespace

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
    while (Iter != End) {
        if (Iter->Snapshot.Step > MaxSnapshotStep) {
            // LocalSnapshots is ordered by Snapshot (Step primary)
            Iter = End;
            break;
        }
        if (Iter->AliveFlag->load()) {
            return &*(Iter++);
        }
        ++Iter;
    }
    return nullptr;
}

TLocalSnapshotsStorage::TView TLocalSnapshotsStorage::View() const {
    return View(AppData()->TimeProvider->Now());
}

TLocalSnapshotsStorage::TView TLocalSnapshotsStorage::View(TInstant now) const {
    const TDuration promotionTime = TDuration::Seconds(AppData()->LongTxServiceConfig.GetLocalSnapshotPromotionTimeSeconds());
    return ViewPromotedUpToStep(PromotionCutoffStep(now, promotionTime));
}

TLocalSnapshotsStorage::TView TLocalSnapshotsStorage::ViewPromotedUpToStep(ui64 maxSnapshotStep) const {
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
    return GetOldestCollectionTime(AppData()->TimeProvider->Now());
}

TInstant TRemoteSnapshotsStorage::GetOldestCollectionTime(TInstant now) const {
    TInstant oldest = now;
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

TString RenderSnapshotsMonPage(
    const TLocalSnapshotsStorage& localSnapshots,
    const TDuration localPromotionTime,
    const TRemoteSnapshotsStorage& remoteSnapshots,
    const TInstant now,
    const TInstant lastRegistryBuildTime,
    const IImmutableSnapshotRegistry* currentRegistry) {
    const TRowVersion border = remoteSnapshots.GetBorder();
    const ui64 nowMs = now.MilliSeconds();
    const ui64 localCutoffStep = PromotionCutoffStep(now, localPromotionTime);
    const THashMap<ui32, TInstant> nodeIdToCollectionTime = remoteSnapshots.GetNodeIdToCollectionTime();
    const TInstant oldestCollectionTime = remoteSnapshots.GetOldestCollectionTime(now);

    TStringStream str;
    HTML(str) {
        PRE() {
            str << "Now: " << now << Endl;
            str << "Remote snapshots storage ready: " << (remoteSnapshots.IsReady() ? "true" : "false") << Endl;
            str << "Snapshots border: [Step " << border.Step << ", TxId " << border.TxId << "]" << Endl;
            str << "Oldest snapshots collection time: " << oldestCollectionTime
                << " (" << Age(now, oldestCollectionTime) << " ago)" << Endl;
            if (lastRegistryBuildTime) {
                str << "Last registry build time: " << lastRegistryBuildTime
                    << " (" << Age(now, lastRegistryBuildTime) << " ago)" << Endl;
            } else {
                str << "Last registry build time: (registry has not been built)" << Endl;
            }
        }
        PRE() {
            str << "Local snapshots (promoted and alive):" << Endl;
            size_t localCount = 0;
            for (const auto& snapshotInfo : localSnapshots.ViewPromotedUpToStep(localCutoffStep)) {
                ++localCount;
                RenderSnapshotLine(str, "", snapshotInfo.Snapshot, snapshotInfo.SessionActorId, snapshotInfo.TableIds, border, nowMs);
            }
            str << "Total: " << localCount << Endl;
        }
        PRE() {
            str << "Remote snapshots:" << Endl;
            for (const auto& [nodeId, collectionTime] : nodeIdToCollectionTime) {
                str << "    Node " << nodeId
                    << " CollectionTime " << collectionTime
                    << " (" << Age(now, collectionTime) << " ago)" << Endl;
            }
            for (const auto& snapshotInfo : remoteSnapshots.View()) {
                const TString source = TStringBuilder() << "Node " << snapshotInfo.SessionActorId.NodeId();
                RenderSnapshotLine(str, source, snapshotInfo.Snapshot, snapshotInfo.SessionActorId, snapshotInfo.TableIds, border, nowMs);
            }
        }
        PRE() {
            str << "Registry input on the next maintenance (promoted local + all remote snapshots below border):" << Endl;
            size_t registryInputCount = 0;
            for (const auto& snapshotInfo : localSnapshots.ViewPromotedUpToStep(localCutoffStep)) {
                if (snapshotInfo.Snapshot < border) {
                    ++registryInputCount;
                    RenderSnapshotLine(str, "[local]", snapshotInfo.Snapshot, snapshotInfo.SessionActorId, snapshotInfo.TableIds, border, nowMs);
                }
            }
            for (const auto& snapshotInfo : remoteSnapshots.View()) {
                if (snapshotInfo.Snapshot < border) {
                    ++registryInputCount;
                    RenderSnapshotLine(str, "[remote]", snapshotInfo.Snapshot, snapshotInfo.SessionActorId, snapshotInfo.TableIds, border, nowMs);
                }
            }
            str << "Total: " << registryInputCount << Endl;
        }
        PRE() {
            str << "Current registry:" << Endl;
            if (currentRegistry) {
                const TRowVersion registryBorder = currentRegistry->GetBorder();
                str << "    Border [Step " << registryBorder.Step << ", TxId " << registryBorder.TxId << "]"
                    << " OldestCollectionTime " << currentRegistry->GetOldestCollectionTime() << Endl;
            } else {
                str << "    (not built)" << Endl;
            }
        }
    }
    return str.Str();
}

}
}
