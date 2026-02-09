#include "node_broker_impl.h"
#include "node_broker__scheme.h"

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/base/nameservice.h>
#include <ydb/core/base/path.h>
#include <ydb/core/cms/console/config_helpers.h>
#include <ydb/core/protos/counters_node_broker.pb.h>
#include <ydb/core/protos/feature_flags.pb.h>
#include <ydb/core/protos/node_broker.pb.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/set.h>

Y_DECLARE_OUT_SPEC(, NKikimr::NNodeBroker::Schema::EMainNodesTable, out, value) {
    switch (value) {
        case NKikimr::NNodeBroker::Schema::EMainNodesTable::Nodes:
            out << "Nodes";
            return;
        case NKikimr::NNodeBroker::Schema::EMainNodesTable::NodesV2:
            out << "NodesV2";
            return;
    }
    out << "Unknown";
}

Y_DECLARE_OUT_SPEC(, NKikimr::NNodeBroker::ENodeState, out, value) {
    switch (value) {
        case NKikimr::NNodeBroker::ENodeState::Removed:
            out << "Removed";
            return;
        case NKikimr::NNodeBroker::ENodeState::Active:
            out << "Active";
            return;
        case NKikimr::NNodeBroker::ENodeState::Expired:
            out << "Expired";
            return;
        }
    out << "Unknown";
}

namespace NKikimr {
namespace NNodeBroker {

using namespace NKikimrNodeBroker;

namespace {

template <typename T>
bool IsReady(T &t)
{
    return t.IsReady();
}

template <typename T, typename ...Ts>
bool IsReady(T &t, Ts &...args)
{
    return t.IsReady() && IsReady(args...);
}

std::atomic<INodeBrokerHooks*> NodeBrokerHooks{ nullptr };

struct TVersionedNodeID {
    struct TCmpByVersion {
        bool operator()(TVersionedNodeID a, TVersionedNodeID b) const {
            return a.Version < b.Version;
        }
    };

    ui32 NodeId;
    ui64 Version;
};

} // anonymous namespace

void INodeBrokerHooks::OnActivateExecutor(ui64 tabletId) {
    Y_UNUSED(tabletId);
}

INodeBrokerHooks* INodeBrokerHooks::Get() {
    return NodeBrokerHooks.load(std::memory_order_acquire);
}

void INodeBrokerHooks::Set(INodeBrokerHooks* hooks) {
    NodeBrokerHooks.store(hooks, std::memory_order_release);
}

void TNodeBroker::OnActivateExecutor(const TActorContext &ctx)
{
    if (auto* hooks = INodeBrokerHooks::Get()) {
        hooks->OnActivateExecutor(TabletID());
    }

    const auto *appData = AppData(ctx);

    MaxStaticId = Min(appData->DynamicNameserviceConfig->MaxStaticNodeId, TActorId::MaxNodeId);
    MinDynamicId = Max(MaxStaticId + 1, (ui64)Min(appData->DynamicNameserviceConfig->MinDynamicNodeId, TActorId::MaxNodeId));
    MaxDynamicId = Max(MinDynamicId, (ui64)Min(appData->DynamicNameserviceConfig->MaxDynamicNodeId, TActorId::MaxNodeId));

    EnableStableNodeNames = appData->FeatureFlags.GetEnableStableNodeNames();

    Executor()->RegisterExternalTabletCounters(TabletCountersPtr);
    Committed.ClearState();

    Execute(CreateTxInitScheme(), ctx);
}

void TNodeBroker::OnDetach(const TActorContext &ctx)
{
    LOG_DEBUG(ctx, NKikimrServices::NODE_BROKER, "TNodeBroker::OnDetach");

    Die(ctx);
}

void TNodeBroker::OnTabletDead(TEvTablet::TEvTabletDead::TPtr &ev,
                               const TActorContext &ctx)
{
    Y_UNUSED(ev);

    LOG_INFO(ctx, NKikimrServices::NODE_BROKER, "OnTabletDead: %" PRIu64, TabletID());

    Die(ctx);
}

void TNodeBroker::DefaultSignalTabletActive(const TActorContext &ctx)
{
    Y_UNUSED(ctx);
}

bool TNodeBroker::OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev,
                                      const TActorContext &ctx)
{
    if (!ev)
        return true;

    TStringStream str;
    HTML(str) {
        PRE() {
            str << "Served domain: " << AppData(ctx)->DomainsInfo->GetDomain()->Name << Endl
                << "DynamicNameserviceConfig:" << Endl
                << "  MaxStaticNodeId: " << MaxStaticId << Endl
                << "  MaxDynamicNodeId: " << MaxDynamicId << Endl
                << "  EpochDuration: " << Committed.EpochDuration << Endl
                << "  StableNodeNamePrefix: " << Committed.StableNodeNamePrefix << Endl
                << "  BannedIds:";
            for (auto &pr : Committed.BannedIds)
                str << " [" << pr.first << ", " << pr.second << "]";
            str << Endl << Endl;
            str << "Registered nodes:" << Endl;

            TSet<ui32> ids;
            for (auto &pr : Committed.Nodes)
                ids.insert(pr.first);
            for (auto id : ids) {
                auto &node = Committed.Nodes.at(id);
                str << " - " << id << Endl
                    << "   Address: " << node.Address << Endl
                    << "   Host: " << node.Host << Endl
                    << "   ResolveHost: " << node.ResolveHost << Endl
                    << "   Port: " << node.Port << Endl
                    << "   DataCenter: " << node.Location.GetDataCenterId() << Endl
                    << "   Location: " << node.Location.ToString() << Endl
                    << "   Lease: " << node.Lease << Endl
                    << "   Expire: " << node.ExpirationString() << Endl
                    << "   AuthorizedByCertificate: " << (node.AuthorizedByCertificate ? "true" : "false") << Endl
                    << "   ServicedSubDomain: " << node.ServicedSubDomain << Endl
                    << "   SlotIndex: " << node.SlotIndex << Endl;
            }
            str << Endl;

            str << "Free Node IDs count: " << Committed.FreeIds.Count() << Endl;

            str << Endl;
            str << "Slot Indexes Pools usage: " << Endl;
            size_t totalSize = 0;
            size_t totalCapacity = 0;
            for (const auto &[subdomainKey, slotIndexesPool] : Committed.SlotIndexesPools) {
                const size_t size = slotIndexesPool.Size();
                totalSize += size;
                const size_t capacity = slotIndexesPool.Capacity();
                totalCapacity += capacity;
                const double usagePercent = floor(size * 100.0 / capacity);
                str << "   " << subdomainKey
                    << " = " << usagePercent << "% (" << size << " of " << capacity << ")"
                    << Endl;
            }
            str << Endl;

            if (totalCapacity > 0) {
                const double totalUsagePercent = floor(totalSize * 100.0 / totalCapacity);
                str << "   Total"
                    << " = " << totalUsagePercent << "% (" << totalSize << " of " << totalCapacity << ")"
                    << Endl;
            } else {
                str << "   No Slot Indexes Pools" << Endl;
            }
        }
    }

    ctx.Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes(str.Str()));
    return true;
}

void TNodeBroker::Cleanup(const TActorContext &ctx)
{
    LOG_DEBUG(ctx, NKikimrServices::NODE_BROKER, "TNodeBroker::Cleanup");

    NConsole::UnsubscribeViaConfigDispatcher(ctx, ctx.SelfID);
}

void TNodeBroker::Die(const TActorContext &ctx)
{
    Cleanup(ctx);
    TActorBase::Die(ctx);
}

void TNodeBroker::TState::ClearState()
{
    Nodes.clear();
    ExpiredNodes.clear();
    RemovedNodes.clear();
    Hosts.clear();

    RecomputeFreeIds();
    RecomputeSlotIndexesPools();
}

void TNodeBroker::TState::UpdateLocation(TNodeInfo &node, const TNodeLocation &location)
{
    node.Version = Epoch.Version + 1;
    node.Location = location;

    LOG_DEBUG_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                LogPrefix() << " Updated location of " << node.IdString()
                << " to " << node.Location.ToString());
}

TNodeBroker::TNodeInfo* TNodeBroker::TState::FindNode(ui32 nodeId)
{
    if (auto it = Nodes.find(nodeId); it != Nodes.end()) {
        return &it->second;
    }

    if (auto it = ExpiredNodes.find(nodeId); it != ExpiredNodes.end()) {
        return &it->second;
    }

    if (auto it = RemovedNodes.find(nodeId); it != RemovedNodes.end()) {
        return &it->second;
    }

    return nullptr;
}

void TNodeBroker::TState::RegisterNewNode(const TNodeInfo &info)
{
    LOG_DEBUG_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                LogPrefix() << " Register new active node " << info.IdString());

    FreeIds.Reset(info.NodeId);
    if (info.SlotIndex.has_value()) {
        SlotIndexesPools[info.ServicedSubDomain].Acquire(info.SlotIndex.value());
    }

    Hosts.emplace(std::make_tuple(info.Host, info.Address, info.Port), info.NodeId);
    Nodes.emplace(info.NodeId, info);
    RemovedNodes.erase(info.NodeId);
}

void TNodeBroker::TState::AddNode(const TNodeInfo &info)
{
    switch (info.State) {
        case ENodeState::Active:
            LOG_DEBUG_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                        LogPrefix() << " Added node " << info.IdString());
            FreeIds.Reset(info.NodeId);
            if (info.SlotIndex.has_value()) {
                SlotIndexesPools[info.ServicedSubDomain].Acquire(info.SlotIndex.value());
            }
            Hosts.emplace(std::make_tuple(info.Host, info.Address, info.Port), info.NodeId);
            Nodes.emplace(info.NodeId, info);
            break;
        case ENodeState::Expired:
            LOG_DEBUG_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                        LogPrefix() << " Added expired node " << info.IdString());
            FreeIds.Reset(info.NodeId);
            if (info.SlotIndex.has_value()) {
                SlotIndexesPools[info.ServicedSubDomain].Acquire(info.SlotIndex.value());
            }
            ExpiredNodes.emplace(info.NodeId, info);
            break;
        case ENodeState::Removed:
            LOG_DEBUG_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                        LogPrefix() << " Added removed node " << info.IdShortString());
            RemovedNodes.emplace(info.NodeId, info);
            break;
    }
}

void TNodeBroker::TState::ExtendLease(TNodeInfo &node)
{
    node.Version = Epoch.Version + 1;
    ++node.Lease;
    node.Expire = Epoch.NextEnd;

    LOG_DEBUG_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                LogPrefix() << " Extended lease of " << node.IdString() << " up to "
                << node.ExpirationString() << " (lease " << node.Lease << ")");
}

void TNodeBroker::TState::FixNodeId(TNodeInfo &node)
{
    node.Version = Epoch.Version + 1;
    ++node.Lease;
    node.Expire = TInstant::Max();

    LOG_DEBUG_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                LogPrefix() << " Fix ID for node " << node.IdString());
}

void TNodeBroker::TState::RecomputeFreeIds()
{
    FreeIds.Clear();
    FreeIds.Set(Self->MinDynamicId, Self->MaxDynamicId + 1);

    // Remove all allocated IDs from the set.
    for (auto &pr : Nodes)
        FreeIds.Reset(pr.first);
    for (auto &pr : ExpiredNodes)
        FreeIds.Reset(pr.first);

    // Remove banned intervals from the set.
    for (auto &pr : BannedIds) {
        FreeIds.Reset(pr.first, pr.second + 1);
    }
}

void TNodeBroker::TState::RecomputeSlotIndexesPools()
{
    for (auto &[_, slotIndexesPool] : SlotIndexesPools) {
        slotIndexesPool.ReleaseAll();
    }

    for (const auto &[_, node] : Nodes) {
        if (node.SlotIndex.has_value()) {
            SlotIndexesPools[node.ServicedSubDomain].Acquire(node.SlotIndex.value());
        }
    }
    for (const auto &[_, node] : ExpiredNodes) {
        if (node.SlotIndex.has_value()) {
            SlotIndexesPools[node.ServicedSubDomain].Acquire(node.SlotIndex.value());
        }
    }
}

bool TNodeBroker::TState::IsBannedId(ui32 id) const
{
    for (auto &pr : BannedIds)
        if (id >= pr.first && id <= pr.second)
            return true;
    return false;
}

void TNodeBroker::AddDelayedListNodesRequest(ui64 epoch,
                                             TEvNodeBroker::TEvListNodes::TPtr &ev)
{
    Y_ABORT_UNLESS(epoch > Committed.Epoch.Id);
    LOG_DEBUG_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                "Delaying list nodes request for epoch #" << epoch);

    DelayedListNodesRequests.emplace(epoch, ev);
}

void TNodeBroker::ProcessListNodesRequest(TEvNodeBroker::TEvListNodes::TPtr &ev)
{
    auto *msg = ev->Get();

    NKikimrNodeBroker::TNodesInfo info;
    Committed.Epoch.Serialize(*info.MutableEpoch());
    info.SetDomain(AppData()->DomainsInfo->GetDomain()->DomainUid);
    TAutoPtr<TEvNodeBroker::TEvNodesInfo> resp = new TEvNodeBroker::TEvNodesInfo(info);

    bool optimized = false;

    if (msg->Record.HasCachedVersion()) {
        if (msg->Record.GetCachedVersion() == Committed.Epoch.Version) {
            // Client has an up-to-date list already
            optimized = true;
        } else {
            // We may be able to only send added or updated nodes in the same epoch when
            // all deltas are cached up to the current epoch inclusive.
            ui64 neededFirstVersion = msg->Record.GetCachedVersion() + 1;
            if (!EpochDeltasVersions.empty()
                && neededFirstVersion > Committed.ApproxEpochStart.Version
                && neededFirstVersion <= Committed.Epoch.Version)
            {
                auto it = std::lower_bound(EpochDeltasVersions.begin(), EpochDeltasVersions.end(), neededFirstVersion);
                if (it != EpochDeltasVersions.begin()) {
                    // Note: usually there is a small number of nodes added
                    // between subsequent requests, so this substr should be
                    // very cheap.
                    resp->PreSerializedData = EpochDeltasCache.substr(std::prev(it)->CacheEndOffset);
                } else {
                    resp->PreSerializedData = EpochDeltasCache;
                }
                optimized = true;
            }
        }
    }

    if (!optimized) {
        resp->PreSerializedData = EpochCache;
    }

    TabletCounters->Percentile()[COUNTER_LIST_NODES_BYTES].IncrementFor(resp->GetCachedByteSize());
    LOG_TRACE_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                "Send TEvNodesInfo for epoch " << Committed.Epoch.ToString());

    Send(ev->Sender, resp.Release());
}

void TNodeBroker::ProcessDelayedListNodesRequests()
{
    THashSet<TActorId> processed;
    while (!DelayedListNodesRequests.empty()) {
        auto it = DelayedListNodesRequests.begin();
        if (it->first > Committed.Epoch.Id)
            break;

        // Avoid processing more than one request from the same sender
        if (processed.insert(it->second->Sender).second) {
            ProcessListNodesRequest(it->second);
        }
        DelayedListNodesRequests.erase(it);
    }
}

void TNodeBroker::ScheduleEpochUpdate(const TActorContext &ctx)
{
    auto now = ctx.Now();
    if (now >= Committed.Epoch.End) {
        ctx.Schedule(TDuration::Zero(), new TEvPrivate::TEvUpdateEpoch);
    } else {
        auto *ev = new IEventHandle(SelfId(), SelfId(), new TEvPrivate::TEvUpdateEpoch);
        EpochTimerCookieHolder.Reset(ISchedulerCookie::Make2Way());
        CreateLongTimer(ctx, Committed.Epoch.End - now, ev, AppData(ctx)->SystemPoolId,
                        EpochTimerCookieHolder.Get());

        LOG_TRACE_S(ctx, NKikimrServices::NODE_BROKER,
                    "Scheduled epoch update at " << Committed.Epoch.End);
    }
}

void TNodeBroker::ScheduleProcessSubscribersQueue(const TActorContext &ctx)
{
    if (!ScheduledProcessSubscribersQueue && !SubscribersQueue.Empty()) {
        ctx.Schedule(TDuration::MilliSeconds(1), new TEvPrivate::TEvProcessSubscribersQueue);
        ScheduledProcessSubscribersQueue = true;
    }
}

void TNodeBroker::FillNodeInfo(const TNodeInfo &node,
                               NKikimrNodeBroker::TNodeInfo &info) const
{
    info.SetNodeId(node.NodeId);
    info.SetHost(node.Host);
    info.SetPort(node.Port);
    info.SetResolveHost(node.ResolveHost);
    info.SetAddress(node.Address);
    info.SetExpire(node.Expire.GetValue());
    node.Location.Serialize(info.MutableLocation(), false);
    FillNodeName(node.SlotIndex, info);
}

void TNodeBroker::FillNodeName(const std::optional<ui32> &slotIndex,
                               NKikimrNodeBroker::TNodeInfo &info) const
{
    if (EnableStableNodeNames && slotIndex.has_value()) {
        const TString name = TStringBuilder() << Committed.StableNodeNamePrefix << slotIndex.value();
        info.SetName(name);
    }
}

void TNodeBroker::TState::ComputeNextEpochDiff(TStateDiff &diff)
{
    for (auto &pr : Nodes) {
        if (pr.second.Expire <= Epoch.End)
            diff.NodesToExpire.push_back(pr.first);
    }

    for (auto &pr : ExpiredNodes)
        diff.NodesToRemove.push_back(pr.first);

    diff.NewEpoch.Id = Epoch.Id + 1;
    diff.NewEpoch.Version = Epoch.Version + 1;
    diff.NewEpoch.Start = Epoch.End;
    diff.NewEpoch.End = Epoch.NextEnd;
    diff.NewEpoch.NextEnd = diff.NewEpoch.End + EpochDuration;
    diff.NewApproxEpochStart.Id = diff.NewEpoch.Id;
    diff.NewApproxEpochStart.Version = diff.NewEpoch.Version;
}

void TNodeBroker::TState::ApplyStateDiff(const TStateDiff &diff)
{
    for (auto id : diff.NodesToExpire) {
        auto it = Nodes.find(id);
        Y_ABORT_UNLESS(it != Nodes.end());

        LOG_DEBUG_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                    LogPrefix() << " Node " << it->second.IdString() << " has expired");

        Hosts.erase(std::make_tuple(it->second.Host, it->second.Address, it->second.Port));
        it->second.State = ENodeState::Expired;
        it->second.Version = diff.NewEpoch.Version;
        ExpiredNodes.emplace(id, std::move(it->second));
        Nodes.erase(it);
    }

    for (auto id : diff.NodesToRemove) {
        auto it = ExpiredNodes.find(id);
        Y_ABORT_UNLESS(it != ExpiredNodes.end());

        LOG_DEBUG_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                    LogPrefix() << " Remove node " << it->second.IdString());

        if (!IsBannedId(id) && id >= Self->MinDynamicId && id <= Self->MaxDynamicId) {
            FreeIds.Set(id);
        }
        ReleaseSlotIndex(it->second);
        RemovedNodes.emplace(id, TNodeInfo(id, ENodeState::Removed, diff.NewEpoch.Version));
        ExpiredNodes.erase(it);
    }

    LOG_DEBUG_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                LogPrefix() << " Move to new epoch " << diff.NewEpoch.ToString()
                << ", approximate epoch start " << diff.NewApproxEpochStart.ToString());

    Epoch = diff.NewEpoch;
    ApproxEpochStart = diff.NewApproxEpochStart;
}

void TNodeBroker::TState::UpdateEpochVersion()
{
    LOG_DEBUG_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                LogPrefix() << " Update current epoch version from " << Epoch.Version
                << " to " << Epoch.Version + 1);

    ++Epoch.Version;
}

void TNodeBroker::PrepareEpochCache()
{
    LOG_DEBUG_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                "Preparing nodes list cache for epoch " << Committed.Epoch.ToString()
                << ", approximate epoch start " << Committed.ApproxEpochStart.ToString()
                << " nodes=" << Committed.Nodes.size() << " expired=" << Committed.ExpiredNodes.size());

    NKikimrNodeBroker::TNodesInfo info;
    for (auto &entry : Committed.Nodes)
        FillNodeInfo(entry.second, *info.AddNodes());
    for (auto &entry : Committed.ExpiredNodes)
        FillNodeInfo(entry.second, *info.AddExpiredNodes());

    Y_PROTOBUF_SUPPRESS_NODISCARD info.SerializeToString(&EpochCache);
    TabletCounters->Simple()[COUNTER_EPOCH_SIZE_BYTES].Set(EpochCache.size());

    EpochDeltasCache.clear();
    EpochDeltasVersions.clear();

    TVector<TVersionedNodeID> updatedAfterEpochStart;
    for (auto &entry : Committed.Nodes) {
        if (entry.second.Version > Committed.ApproxEpochStart.Version) {
            updatedAfterEpochStart.emplace_back(entry.second.NodeId, entry.second.Version);
        }
    }
    std::sort(updatedAfterEpochStart.begin(), updatedAfterEpochStart.end(), TVersionedNodeID::TCmpByVersion());

    NKikimrNodeBroker::TNodesInfo deltaInfo;
    TString delta;
    for (const auto &[id, v] : updatedAfterEpochStart) {
        FillNodeInfo(Committed.Nodes.at(id), *deltaInfo.AddNodes());

        Y_PROTOBUF_SUPPRESS_NODISCARD deltaInfo.SerializeToString(&delta);
        AddDeltaToEpochDeltasCache(delta, v);

        deltaInfo.ClearNodes();
    }
    TabletCounters->Simple()[COUNTER_EPOCH_DELTAS_SIZE_BYTES].Set(EpochDeltasCache.size());
}

void TNodeBroker::PrepareUpdateNodesLog()
{
    LOG_DEBUG_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                "Preparing update nodes log for epoch #" << Committed.Epoch.ToString()
                << " nodes=" << Committed.Nodes.size()
                << " expired=" << Committed.ExpiredNodes.size()
                << " removed=" << Committed.RemovedNodes.size());

    UpdateNodesLog.clear();
    UpdateNodesLogVersions.clear();

    TVector<TVersionedNodeID> nodeIdsSortedByVersion;
    for (auto &entry : Committed.Nodes) {
        nodeIdsSortedByVersion.emplace_back(entry.second.NodeId, entry.second.Version);
    }
    for (auto &entry : Committed.ExpiredNodes) {
        nodeIdsSortedByVersion.emplace_back(entry.second.NodeId, entry.second.Version);
    }
    for (auto &entry : Committed.RemovedNodes) {
        nodeIdsSortedByVersion.emplace_back(entry.second.NodeId, entry.second.Version);
    }
    std::sort(nodeIdsSortedByVersion.begin(), nodeIdsSortedByVersion.end(), TVersionedNodeID::TCmpByVersion());

    for (const auto &id : nodeIdsSortedByVersion) {
        const auto& node = *Committed.FindNode(id.NodeId);
        AddNodeToUpdateNodesLog(node);
    }
    TabletCounters->Simple()[COUNTER_UPDATE_NODES_LOG_SIZE_BYTES].Set(UpdateNodesLog.size());
}

void TNodeBroker::AddNodeToEpochCache(const TNodeInfo &node)
{
    LOG_DEBUG_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                "Add node " << node.IdString() << " to epoch cache");

    NKikimrNodeBroker::TNodesInfo info;
    FillNodeInfo(node, *info.AddNodes());

    TString delta;
    Y_PROTOBUF_SUPPRESS_NODISCARD info.SerializeToString(&delta);

    EpochCache += delta;
    TabletCounters->Simple()[COUNTER_EPOCH_SIZE_BYTES].Set(EpochCache.size());

    AddDeltaToEpochDeltasCache(delta, node.Version);
}

void TNodeBroker::AddDeltaToEpochDeltasCache(const TString &delta, ui64 version) {
    Y_ENSURE(EpochDeltasVersions.empty() || EpochDeltasVersions.back().Version <= version);
    if (!EpochDeltasVersions.empty() && EpochDeltasVersions.back().Version == version) {
        EpochDeltasCache += delta;
        EpochDeltasVersions.back().CacheEndOffset = EpochDeltasCache.size();
    } else {
        EpochDeltasCache += delta;
        EpochDeltasVersions.emplace_back(version, EpochDeltasCache.size());
    }
    TabletCounters->Simple()[COUNTER_EPOCH_DELTAS_SIZE_BYTES].Set(EpochDeltasCache.size());
}

void TNodeBroker::AddNodeToUpdateNodesLog(const TNodeInfo &node)
{
    LOG_DEBUG_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                "Add node " << node.IdShortString() << " to update nodes log");

    NKikimrNodeBroker::TUpdateNodes updateNodes;

    switch (node.State) {
        case ENodeState::Active:
            FillNodeInfo(node, *updateNodes.AddUpdates()->MutableNode());
            break;
        case ENodeState::Expired:
            updateNodes.AddUpdates()->SetExpiredNode(node.NodeId);
            break;
        case ENodeState::Removed:
            updateNodes.AddUpdates()->SetRemovedNode(node.NodeId);
            break;
    }

    TString delta;
    Y_PROTOBUF_SUPPRESS_NODISCARD updateNodes.SerializeToString(&delta);

    Y_ENSURE(UpdateNodesLogVersions.empty() || UpdateNodesLogVersions.back().Version <= node.Version);
    if (!UpdateNodesLogVersions.empty() && UpdateNodesLogVersions.back().Version == node.Version) {
        UpdateNodesLog += delta;
        UpdateNodesLogVersions.back().CacheEndOffset = UpdateNodesLog.size();
    } else {
        UpdateNodesLog += delta;
        UpdateNodesLogVersions.emplace_back(node.Version, UpdateNodesLog.size());
    }
    TabletCounters->Simple()[COUNTER_UPDATE_NODES_LOG_SIZE_BYTES].Set(UpdateNodesLog.size());
}

void TNodeBroker::SubscribeForConfigUpdates(const TActorContext &ctx)
{
    ui32 nodeBrokerItem = (ui32)NKikimrConsole::TConfigItem::NodeBrokerConfigItem;
    ui32 featureFlagsItem = (ui32)NKikimrConsole::TConfigItem::FeatureFlagsItem;
    NConsole::SubscribeViaConfigDispatcher(ctx, {nodeBrokerItem, featureFlagsItem}, ctx.SelfID);
}

void TNodeBroker::SendToSubscriber(const TSubscriberInfo &subscriber, IEventBase* event, const TActorContext &ctx) const
{
    SendToSubscriber(subscriber, event, 0, ctx);
}

void TNodeBroker::SendToSubscriber(const TSubscriberInfo &subscriber, IEventBase* event, ui64 cookie, const TActorContext &ctx) const
{
    THolder<IEventHandle> ev = MakeHolder<IEventHandle>(subscriber.Id, ctx.SelfID, event, 0, cookie);
    if (subscriber.PipeServerInfo->IcSession) {
        ev->Rewrite(TEvInterconnect::EvForward, subscriber.PipeServerInfo->IcSession);
    }
    ctx.Send(ev.Release());
}


void TNodeBroker::SendUpdateNodes(TSubscriberInfo &subscriber, const TActorContext &ctx)
{
    SubscribersQueue.Remove(&subscriber);

    NKikimrNodeBroker::TUpdateNodes record;
    record.SetSeqNo(subscriber.SeqNo);
    Committed.Epoch.Serialize(*record.MutableEpoch());
    auto response = MakeHolder<TEvNodeBroker::TEvUpdateNodes>(record);

    auto it = std::lower_bound(UpdateNodesLogVersions.begin(), UpdateNodesLogVersions.end(), subscriber.SentVersion + 1);
    if (it != UpdateNodesLogVersions.begin()) {
        response->PreSerializedData = UpdateNodesLog.substr(std::prev(it)->CacheEndOffset);
    } else {
        response->PreSerializedData = UpdateNodesLog;
    }

    TabletCounters->Percentile()[COUNTER_UPDATE_NODES_BYTES].IncrementFor(response->GetCachedByteSize());
    LOG_TRACE_S(ctx, NKikimrServices::NODE_BROKER,
                "Send TEvUpdateNodes v" << subscriber.SentVersion << " -> v" << Committed.Epoch.Version
                << " to " << subscriber.Id);
    SendToSubscriber(subscriber, response.Release(), ctx);

    subscriber.SentVersion = Committed.Epoch.Version;
    SubscribersQueue.PushBack(&subscriber);
}

TNodeBroker::TSubscriberInfo& TNodeBroker::AddSubscriber(TActorId subscriberId,
                                                         TActorId pipeServerId,
                                                         ui64 seqNo,
                                                         ui64 version,
                                                         const TActorContext &ctx)
{
    LOG_DEBUG_S(ctx, NKikimrServices::NODE_BROKER,
                "New subscriber " << subscriberId
                << ", seqNo: " << seqNo
                << ", version: " << version
                << ", server pipe id: " << pipeServerId);

    auto& pipeServer = PipeServers.at(pipeServerId);
    auto res = Subscribers.emplace(
        std::piecewise_construct,
        std::forward_as_tuple(subscriberId),
        std::forward_as_tuple(subscriberId, seqNo, version, &pipeServer)
    );
    Y_ENSURE(res.second, "Subscription already exists for " << subscriberId);
    pipeServer.Subscribers.insert(subscriberId);
    return res.first->second;
}

void TNodeBroker::RemoveSubscriber(TActorId subscriber, const TActorContext &ctx)
{
    auto it = Subscribers.find(subscriber);
    Y_ENSURE(it != Subscribers.end(), "No subscription for " << subscriber);

    LOG_DEBUG_S(ctx, NKikimrServices::NODE_BROKER,
                "Unsubscribed " << subscriber
                << ", seqNo: " << it->second.SeqNo
                << ", server pipe id: " << it->second.PipeServerInfo->Id);

    it->second.PipeServerInfo->Subscribers.erase(subscriber);
    SubscribersQueue.Remove(&it->second);
    Subscribers.erase(it);
}

bool TNodeBroker::HasOutdatedSubscription(TActorId subscriber, ui64 newSeqNo) const
{
    if (auto it = Subscribers.find(subscriber); it != Subscribers.end()) {
        return it->second.SeqNo < newSeqNo;
    }
    return false;
}

void TNodeBroker::UpdateCommittedStateCounters() {
    TabletCounters->Simple()[COUNTER_ACTIVE_NODES].Set(Committed.Nodes.size());
    TabletCounters->Simple()[COUNTER_EXPIRED_NODES].Set(Committed.ExpiredNodes.size());
    TabletCounters->Simple()[COUNTER_REMOVED_NODES].Set(Committed.RemovedNodes.size());
    TabletCounters->Simple()[COUNTER_EPOCH_VERSION].Set(Committed.Epoch.Version);
}

void TNodeBroker::TState::LoadConfigFromProto(const NKikimrNodeBroker::TConfig &config)
{
    Config = config;

    EpochDuration = TDuration::MicroSeconds(config.GetEpochDuration());
    if (EpochDuration < MIN_LEASE_DURATION) {
        LOG_ERROR_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                    LogPrefix() << " Configured lease duration (" << EpochDuration << ") is too"
                    " small. Using min. value: " << MIN_LEASE_DURATION);
        EpochDuration = MIN_LEASE_DURATION;
    }

    StableNodeNamePrefix = config.GetStableNodeNamePrefix();

    BannedIds.clear();
    for (auto &banned : config.GetBannedNodeIds())
        BannedIds.emplace_back(banned.GetFrom(), banned.GetTo());
    RecomputeFreeIds();
}

void TNodeBroker::TState::ReleaseSlotIndex(TNodeInfo &node)
{
    if (node.SlotIndex.has_value()) {
        SlotIndexesPools[node.ServicedSubDomain].Release(node.SlotIndex.value());
        node.SlotIndex.reset();
    }
}

void TNodeBroker::TDirtyState::DbUpdateNode(ui32 nodeId, TTransactionContext &txc)
{
    const auto* node = FindNode(nodeId);
    if (node != nullptr) {
        switch (node->State) {
            case ENodeState::Active:
            case ENodeState::Expired:
                DbAddNode(*node, txc);
                break;
            case ENodeState::Removed:
                DbRemoveNode(*node, txc);
                break;
        }
    }
}

void TNodeBroker::TDirtyState::DbRemoveNode(const TNodeInfo &node, TTransactionContext &txc)
{
    LOG_DEBUG_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                DbLogPrefix() << " Removing node " << node.IdShortString() << " from database");

    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::NodesV2>().Key(node.NodeId)
        .Update<Schema::NodesV2::NodeInfo>(NKikimrNodeBroker::TNodeInfoSchema())
        .Update<Schema::NodesV2::State>(ENodeState::Removed)
        .Update<Schema::NodesV2::Version>(node.Version)
        .Update<Schema::NodesV2::SchemaVersion>(1);

    db.Table<Schema::Nodes>().Key(node.NodeId).Delete();
}

void TNodeBroker::TDirtyState::DbAddNode(const TNodeInfo &node,
                            TTransactionContext &txc)
{
    LOG_DEBUG_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                DbLogPrefix() << " Adding node " << node.IdString() << " to database"
                << " state=" << node.State
                << " resolvehost=" << node.ResolveHost
                << " address=" << node.Address
                << " dc=" << node.Location.GetDataCenterId()
                << " location=" << node.Location.ToString()
                << " lease=" << node.Lease
                << " expire=" << node.ExpirationString()
                << " servicedsubdomain=" << node.ServicedSubDomain
                << " slotindex=" << node.SlotIndex
                << " authorizedbycertificate=" << (node.AuthorizedByCertificate ? "true" : "false"));

    NIceDb::TNiceDb db(txc.DB);

    db.Table<Schema::NodesV2>().Key(node.NodeId)
        .Update<Schema::NodesV2::NodeInfo>(node.SerializeToSchema())
        .Update<Schema::NodesV2::State>(node.State)
        .Update<Schema::NodesV2::Version>(node.Version)
        .Update<Schema::NodesV2::SchemaVersion>(1);

    using T = Schema::Nodes;
    db.Table<T>().Key(node.NodeId)
        .Update<T::Host>(node.Host)
        .Update<T::Port>(node.Port)
        .Update<T::ResolveHost>(node.ResolveHost)
        .Update<T::Address>(node.Address)
        .Update<T::Lease>(node.Lease)
        .Update<T::Expire>(node.Expire.GetValue())
        .Update<T::Location>(node.Location.GetSerializedLocation())
        .Update<T::ServicedSubDomain>(node.ServicedSubDomain)
        .Update<T::AuthorizedByCertificate>(node.AuthorizedByCertificate);

    if (node.SlotIndex.has_value()) {
        db.Table<T>().Key(node.NodeId)
            .Update<T::SlotIndex>(node.SlotIndex.value());
    } else {
        db.Table<T>().Key(node.NodeId)
            .UpdateToNull<T::SlotIndex>();
    }
}


void TNodeBroker::TDirtyState::DbApplyStateDiff(const TStateDiff &diff,
                                   TTransactionContext &txc)
{
    DbUpdateNodes(diff.NodesToExpire, txc);
    DbUpdateNodes(diff.NodesToRemove, txc);
    DbUpdateEpoch(diff.NewEpoch, txc);
    DbUpdateApproxEpochStart(diff.NewApproxEpochStart, txc);
}

void TNodeBroker::TDirtyState::DbFixNodeId(const TNodeInfo &node,
                              TTransactionContext &txc)
{
    LOG_DEBUG_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                DbLogPrefix() << " Fix ID in database for node: " <<  node.IdString());

    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::Nodes>().Key(node.NodeId)
        .Update<Schema::Nodes::Lease>(node.Lease + 1)
        .Update<Schema::Nodes::Expire>(TInstant::Max().GetValue());
}

TNodeBroker::TDbChanges TNodeBroker::TDirtyState::DbLoadState(TTransactionContext &txc,
                              const TActorContext &ctx)
{
    NIceDb::TNiceDb db(txc.DB);

    if (!db.Precharge<Schema>())
        return { .Ready = false };

    auto configRow = db.Table<Schema::Config>()
        .Key(Schema::ConfigKeyConfig).Select<Schema::Config::Value>();
    auto subscriptionRow = db.Table<Schema::Params>()
        .Key(Schema::ParamKeyConfigSubscription).Select<Schema::Params::Value>();
    auto currentEpochIdRow = db.Table<Schema::Params>()
        .Key(Schema::ParamKeyCurrentEpochId).Select<Schema::Params::Value>();
    auto currentEpochVersionRow = db.Table<Schema::Params>()
        .Key(Schema::ParamKeyCurrentEpochVersion).Select<Schema::Params::Value>();
    auto currentEpochStartRow = db.Table<Schema::Params>()
        .Key(Schema::ParamKeyCurrentEpochStart).Select<Schema::Params::Value>();
    auto currentEpochEndRow = db.Table<Schema::Params>()
        .Key(Schema::ParamKeyCurrentEpochEnd).Select<Schema::Params::Value>();
    auto nextEpochEndRow = db.Table<Schema::Params>()
        .Key(Schema::ParamKeyNextEpochEnd).Select<Schema::Params::Value>();
    auto approxEpochStartIdRow = db.Table<Schema::Params>()
        .Key(Schema::ParamKeyApproximateEpochStartId).Select<Schema::Params::Value>();
    auto approxEpochStartVersionRow = db.Table<Schema::Params>()
        .Key(Schema::ParamKeyApproximateEpochStartVersion).Select<Schema::Params::Value>();
    auto mainNodesTableRow = db.Table<Schema::Params>()
        .Key(Schema::ParamKeyMainNodesTable).Select<Schema::Params::Value>();
    auto nodesRowset = db.Table<Schema::Nodes>()
        .Range().Select<Schema::Nodes::TColumns>();
    auto nodesV2Rowset = db.Table<Schema::NodesV2>()
        .Range().Select<Schema::NodesV2::TColumns>();

    if (!IsReady(configRow, subscriptionRow, currentEpochIdRow,
                 currentEpochVersionRow, currentEpochStartRow,
                 currentEpochEndRow, nextEpochEndRow, approxEpochStartIdRow,
                 approxEpochStartVersionRow, mainNodesTableRow, nodesRowset,
                 nodesV2Rowset))
        return { .Ready = false };

    ClearState();

    if (configRow.IsValid()) {
        auto configString = configRow.GetValue<Schema::Config::Value>();
        NKikimrNodeBroker::TConfig config;
        Y_PROTOBUF_SUPPRESS_NODISCARD config.ParseFromArray(configString.data(), configString.size());
        LoadConfigFromProto(config);

        LOG_DEBUG_S(ctx, NKikimrServices::NODE_BROKER,
                    DbLogPrefix() << " Loaded config:" << Endl << config.DebugString());
    } else {
        LOG_DEBUG_S(ctx, NKikimrServices::NODE_BROKER,
                    DbLogPrefix() << " Using default config.");

        LoadConfigFromProto(NKikimrNodeBroker::TConfig());
    }

    if (subscriptionRow.IsValid()) {
        ConfigSubscriptionId = subscriptionRow.GetValue<Schema::Params::Value>();

        LOG_DEBUG_S(ctx, NKikimrServices::NODE_BROKER,
                    DbLogPrefix() << " Loaded config subscription: " << ConfigSubscriptionId);
    }

    TDbChanges dbChanges;
    if (currentEpochIdRow.IsValid()) {
        Y_ABORT_UNLESS(currentEpochVersionRow.IsValid());
        Y_ABORT_UNLESS(currentEpochStartRow.IsValid());
        Y_ABORT_UNLESS(currentEpochEndRow.IsValid());
        Y_ABORT_UNLESS(nextEpochEndRow.IsValid());
        TString val;

        Epoch.Id = currentEpochIdRow.GetValue<Schema::Params::Value>();
        Epoch.Version = currentEpochVersionRow.GetValue<Schema::Params::Value>();
        Epoch.Start = TInstant::FromValue(currentEpochStartRow.GetValue<Schema::Params::Value>());
        Epoch.End = TInstant::FromValue(currentEpochEndRow.GetValue<Schema::Params::Value>());
        Epoch.NextEnd = TInstant::FromValue(nextEpochEndRow.GetValue<Schema::Params::Value>());

        LOG_DEBUG_S(ctx, NKikimrServices::NODE_BROKER,
                    DbLogPrefix() << " Loaded current epoch: " << Epoch.ToString());
    } else {
        // If there is no epoch start the first one.
        Epoch.Id = 1;
        Epoch.Version = 1;
        Epoch.Start = ctx.Now();
        Epoch.End = Epoch.Start + EpochDuration;
        Epoch.NextEnd = Epoch.End + EpochDuration;

        LOG_DEBUG_S(ctx, NKikimrServices::NODE_BROKER,
                    DbLogPrefix() << " Starting the first epoch: " << Epoch.ToString());

        dbChanges.UpdateEpoch = true;
    }

    if (approxEpochStartIdRow.IsValid() && approxEpochStartVersionRow.IsValid()) {
        ApproxEpochStart.Id = approxEpochStartIdRow.GetValue<Schema::Params::Value>();
        ApproxEpochStart.Version = approxEpochStartVersionRow.GetValue<Schema::Params::Value>();

        if (ApproxEpochStart.Id != Epoch.Id) {
            ApproxEpochStart.Id = Epoch.Id;
            ApproxEpochStart.Version = Epoch.Version;

            LOG_DEBUG_S(ctx, NKikimrServices::NODE_BROKER,
                        DbLogPrefix() << " Approximate epoch start is changed: " << ApproxEpochStart.ToString());

            dbChanges.UpdateApproxEpochStart = true;
        } else {
            LOG_DEBUG_S(ctx, NKikimrServices::NODE_BROKER,
                        DbLogPrefix() << " Loaded approximate epoch start: " << ApproxEpochStart.ToString());
        }
    } else {
        ApproxEpochStart.Id = Epoch.Id;
        ApproxEpochStart.Version = Epoch.Version;

        LOG_DEBUG_S(ctx, NKikimrServices::NODE_BROKER,
                    DbLogPrefix() << " Loaded the first approximate epoch start: " << ApproxEpochStart.ToString());

        dbChanges.UpdateApproxEpochStart = true;
    }

    Schema::EMainNodesTable mainNodesTable = Schema::EMainNodesTable::Nodes;
    if (mainNodesTableRow.IsValid()) {
        mainNodesTable = static_cast<Schema::EMainNodesTable>(mainNodesTableRow.GetValue<Schema::Params::Value>());

        LOG_NOTICE_S(ctx, NKikimrServices::NODE_BROKER,
                     DbLogPrefix() << " Loaded main nodes table: " << mainNodesTable);
    }

    if (!mainNodesTableRow.IsValid() || mainNodesTable != Schema::EMainNodesTable::Nodes) {
        dbChanges.UpdateMainNodesTable = true;
    }

    if (mainNodesTable == Schema::EMainNodesTable::Nodes) {
        if (dbChanges.Merge(DbLoadNodes(nodesRowset, ctx)); !dbChanges.Ready) {
            return dbChanges;
        }
        if (dbChanges.Merge(DbMigrateNodes(nodesV2Rowset, ctx)); !dbChanges.Ready) {
            return dbChanges;
        }
    } else if (mainNodesTable == Schema::EMainNodesTable::NodesV2) {
        if (dbChanges.Merge(DbLoadNodesV2(nodesV2Rowset, ctx)); !dbChanges.Ready) {
            return dbChanges;
        }
        if (dbChanges.Merge(DbMigrateNodesV2()); !dbChanges.Ready) {
            return dbChanges;
        }
    }

    if (!dbChanges.NewVersionUpdateNodes.empty()) {
        UpdateEpochVersion();
        dbChanges.UpdateEpoch = true;
    }

    return dbChanges;
}

TNodeBroker::TDbChanges TNodeBroker::TDirtyState::DbLoadNodes(auto &nodesRowset, const TActorContext &ctx)
{
    TVector<ui32> toRemove;
    while (!nodesRowset.EndOfSet()) {
        auto id = nodesRowset.template GetValue<Schema::Nodes::ID>();
        // We don't remove nodes with a different domain id when there's a
        // single domain. We may have been running in a single domain allocation
        // mode, and now temporarily restarted without this mode enabled. We
        // should still support nodes that have been registered before we
        // restarted, even though it's not available for allocation.
        if (id <= Self->MaxStaticId || id > Self->MaxDynamicId) {
            LOG_ERROR_S(ctx, NKikimrServices::NODE_BROKER,
                        DbLogPrefix() << " Removing node with wrong ID " << id << " not in range ("
                        << Self->MaxStaticId << ", " << Self->MaxDynamicId << "]");
            toRemove.push_back(id);
            TNodeInfo info{id, ENodeState::Removed, Epoch.Version + 1};
            AddNode(info);
        } else {
            auto expire = TInstant::FromValue(nodesRowset.template GetValue<Schema::Nodes::Expire>());
            std::optional<TNodeLocation> modernLocation;
            if (nodesRowset.template HaveValue<Schema::Nodes::Location>()) {
                modernLocation.emplace(TNodeLocation::FromSerialized, nodesRowset.template GetValue<Schema::Nodes::Location>());
            }

            TNodeLocation location;

            // only modern value found in database
            Y_ABORT_UNLESS(modernLocation);
            location = std::move(*modernLocation);

            TNodeInfo info{id,
                nodesRowset.template GetValue<Schema::Nodes::Address>(),
                nodesRowset.template GetValue<Schema::Nodes::Host>(),
                nodesRowset.template GetValue<Schema::Nodes::ResolveHost>(),
                (ui16)nodesRowset.template GetValue<Schema::Nodes::Port>(),
                location}; // format update pending

            info.Lease = nodesRowset.template GetValue<Schema::Nodes::Lease>();
            info.Expire = expire;
            info.ServicedSubDomain = TSubDomainKey(nodesRowset.template GetValueOrDefault<Schema::Nodes::ServicedSubDomain>());
            if (nodesRowset.template HaveValue<Schema::Nodes::SlotIndex>()) {
                info.SlotIndex = nodesRowset.template GetValue<Schema::Nodes::SlotIndex>();
            }
            info.AuthorizedByCertificate = nodesRowset.template GetValue<Schema::Nodes::AuthorizedByCertificate>();
            info.State = expire > Epoch.Start ? ENodeState::Active : ENodeState::Expired;
            AddNode(info);

            LOG_DEBUG_S(ctx, NKikimrServices::NODE_BROKER,
                        DbLogPrefix() << " Loaded node " << info.ToString());
        }

        if (!nodesRowset.Next())
            return { .Ready = false };
    }

    return {
        .Ready = true,
        .NewVersionUpdateNodes = std::move(toRemove)
    };
}

TNodeBroker::TDbChanges TNodeBroker::TDirtyState::DbLoadNodesV2(auto &nodesV2Rowset, const TActorContext &ctx)
{
    TVector<ui32> toRemove;
    while (!nodesV2Rowset.EndOfSet()) {
        ui32 id = nodesV2Rowset.template GetValue<Schema::NodesV2::NodeId>();
        ENodeState state = nodesV2Rowset.template GetValue<Schema::NodesV2::State>();
        if (state != ENodeState::Removed && (id <= Self->MaxStaticId || id > Self->MaxDynamicId)) {
            LOG_ERROR_S(ctx, NKikimrServices::NODE_BROKER,
                        DbLogPrefix() << " Removing node with wrong ID " << id << " not in range ("
                        << Self->MaxStaticId << ", " << Self->MaxDynamicId << "]");
            toRemove.push_back(id);
            TNodeInfo node(id, ENodeState::Removed, Epoch.Version + 1);
            AddNode(node);
        } else {
            auto info = nodesV2Rowset.template GetValue<Schema::NodesV2::NodeInfo>();
            ui64 version = nodesV2Rowset.template GetValue<Schema::NodesV2::Version>();
            TNodeInfo node(id, state, version, info);
            AddNode(node);
            LOG_DEBUG_S(ctx, NKikimrServices::NODE_BROKER,
                        DbLogPrefix() << " Loaded nodeV2 " << node.ToString());
        }

        if (!nodesV2Rowset.Next()) {
            return { .Ready = false };
        }
    }

    return {
        .Ready = true,
        .NewVersionUpdateNodes = std::move(toRemove)
    };
}

TNodeBroker::TDbChanges TNodeBroker::TDirtyState::DbMigrateNodesV2() {
    // Assume that Nodes table is fully cleared by future version,
    // so just need to fill it with active & expired nodes
    TVector<ui32> updateNodes;
    for (const auto &[id, _] : Nodes) {
        updateNodes.push_back(id);
    }
    for (const auto &[id, _] : ExpiredNodes) {
        updateNodes.push_back(id);
    }
    return {
        .Ready = true,
        .UpdateNodes = std::move(updateNodes)
    };
}

TNodeBroker::TDbChanges TNodeBroker::TDirtyState::DbMigrateNodes(auto &nodesV2Rowset, const TActorContext &ctx) {
    TVector<ui32> newVersionUpdateNodes;
    TVector<ui32> updateNodes;

    THashSet<ui32> nodesV2;
    while (!nodesV2Rowset.EndOfSet()) {
        ui32 id = nodesV2Rowset.template GetValue<Schema::NodesV2::NodeId>();
        nodesV2.insert(id);

        auto info = nodesV2Rowset.template GetValue<Schema::NodesV2::NodeInfo>();
        auto state = nodesV2Rowset.template GetValue<Schema::NodesV2::State>();
        auto version = nodesV2Rowset.template GetValue<Schema::NodesV2::Version>();
        TNodeInfo nodeV2(id, state, version, info);

        LOG_DEBUG_S(ctx, NKikimrServices::NODE_BROKER,
                    DbLogPrefix() << " Loaded nodeV2 " << nodeV2.ToString());

        auto* node = FindNode(id);
        bool nodeRemoved = node == nullptr || node->State == ENodeState::Removed;
        bool nodeChanged = !nodeRemoved && !node->EqualExceptVersion(nodeV2);

        if (nodeChanged) {
            if (node->State == ENodeState::Active && !node->EqualCachedData(nodeV2)) {
                // Old version can change active nodes without version bump.
                // Don't know if any node is aware of this change, so send it to all nodes.
                node->Version = Epoch.Version + 1;
                newVersionUpdateNodes.push_back(id);
            } else if (node->State == ENodeState::Expired && node->State != nodeV2.State) {
                // Node is expired only with version bump. Don't know exactly version, so send it
                // to all nodes that don't have the most recent version.
                node->Version = Epoch.Version;
                updateNodes.push_back(id);
            } else {
                // Don't need to send anywhere, ABA is not possible.
                node->Version = nodeV2.Version;
                updateNodes.push_back(id);
            }

            LOG_NOTICE_S(ctx, NKikimrServices::NODE_BROKER,
                         DbLogPrefix() << " Migrating changed node " << node->ToString());
        } else if (nodeRemoved) {
            if (node != nullptr) {
                // Remove was made by new version, migration already in progress
                LOG_NOTICE_S(ctx, NKikimrServices::NODE_BROKER,
                             DbLogPrefix() << " Migrating removed node " << node->IdShortString());
            } else if (nodeV2.State != ENodeState::Removed) {
                // Assume that old version removes nodes only with version bump. It is not always
                // true, so it is possible that client never receive this remove until the restart.
                // Don't know exactly version, so send it to all nodes that don't have the most
                // recent version.
                TNodeInfo removedNode(id, ENodeState::Removed, Epoch.Version);
                AddNode(removedNode);
                updateNodes.push_back(id);

                LOG_NOTICE_S(ctx, NKikimrServices::NODE_BROKER,
                             DbLogPrefix() << " Migrating removed node " << removedNode.IdShortString());
            } else {
                AddNode(nodeV2);
                LOG_DEBUG_S(ctx, NKikimrServices::NODE_BROKER,
                            DbLogPrefix() << " Removed node " << nodeV2.IdShortString() << " is already migrated");
            }
        } else {
            node->Version = nodeV2.Version;
            LOG_DEBUG_S(ctx, NKikimrServices::NODE_BROKER,
                        DbLogPrefix() << " Node " << node->IdShortString() << " is already migrated");
        }

        if (!nodesV2Rowset.Next()) {
            return { .Ready = false };
        }
    }

    for (auto &[id, node] : Nodes) {
        if (!nodesV2.contains(id)) {
            node.Version = Epoch.Version + 1;
            newVersionUpdateNodes.push_back(id);

            LOG_NOTICE_S(ctx, NKikimrServices::NODE_BROKER,
                         DbLogPrefix() << " Migrating new active node " << node.ToString());
        }
    }

    for (auto& [id, node] : ExpiredNodes) {
         if (!nodesV2.contains(id)) {
            node.Version = Epoch.Version;
            updateNodes.push_back(id);

            LOG_NOTICE_S(ctx, NKikimrServices::NODE_BROKER,
                         DbLogPrefix() << " Migrating new expired node " << node.ToString());
        }
    }

    return {
        .Ready = true,
        .NewVersionUpdateNodes = std::move(newVersionUpdateNodes),
        .UpdateNodes = std::move(updateNodes)
    };
}

void TNodeBroker::TDirtyState::DbUpdateNodes(const TVector<ui32> &nodes, TTransactionContext &txc)
{
    for (auto id : nodes) {
        DbUpdateNode(id, txc);
    }
}

void TNodeBroker::TDirtyState::DbUpdateConfig(const NKikimrNodeBroker::TConfig &config,
                                 TTransactionContext &txc)
{
    LOG_DEBUG_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                DbLogPrefix() << " Update config in database"
                << " config=" << config.ShortDebugString());

    TString value;
    Y_PROTOBUF_SUPPRESS_NODISCARD config.SerializeToString(&value);
    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::Config>().Key(Schema::ConfigKeyConfig)
        .Update<Schema::Config::Value>(value);
}

void TNodeBroker::TDirtyState::DbUpdateConfigSubscription(ui64 subscriptionId,
                                             TTransactionContext &txc)
{
    LOG_DEBUG_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                DbLogPrefix() << " Update config subscription in database"
                << " id=" << subscriptionId);

    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::Params>().Key(Schema::ParamKeyConfigSubscription)
        .Update<Schema::Params::Value>(subscriptionId);
}

void TNodeBroker::TDirtyState::DbUpdateEpoch(const TEpochInfo &epoch,
                                TTransactionContext &txc)
{
    LOG_DEBUG_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                DbLogPrefix() << " Update epoch in database: " << epoch.ToString());

    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::Params>().Key(Schema::ParamKeyCurrentEpochId)
        .Update<Schema::Params::Value>(epoch.Id);
    db.Table<Schema::Params>().Key(Schema::ParamKeyCurrentEpochVersion)
        .Update<Schema::Params::Value>(epoch.Version);
    db.Table<Schema::Params>().Key(Schema::ParamKeyCurrentEpochStart)
        .Update<Schema::Params::Value>(epoch.Start.GetValue());
    db.Table<Schema::Params>().Key(Schema::ParamKeyCurrentEpochEnd)
        .Update<Schema::Params::Value>(epoch.End.GetValue());
    db.Table<Schema::Params>().Key(Schema::ParamKeyNextEpochEnd)
        .Update<Schema::Params::Value>(epoch.NextEnd.GetValue());
}

void TNodeBroker::TDirtyState::DbUpdateApproxEpochStart(const TApproximateEpochStartInfo &epochStart,
                                    TTransactionContext &txc)
{
    LOG_DEBUG_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                DbLogPrefix() << " Update approx epoch start in database: " << epochStart.ToString());

    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::Params>().Key(Schema::ParamKeyApproximateEpochStartId)
        .Update<Schema::Params::Value>(epochStart.Id);
    db.Table<Schema::Params>().Key(Schema::ParamKeyApproximateEpochStartVersion)
        .Update<Schema::Params::Value>(epochStart.Version);
}

void TNodeBroker::TDirtyState::DbUpdateMainNodesTable(TTransactionContext &txc)
{
    Schema::EMainNodesTable newMainNodesTable = Schema::EMainNodesTable::Nodes;
    LOG_NOTICE_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                DbLogPrefix() << " Update main nodes table to: " << newMainNodesTable);

    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::Params>().Key(Schema::ParamKeyMainNodesTable)
        .Update<Schema::Params::Value>(static_cast<ui64>(newMainNodesTable));
}


void TNodeBroker::TDirtyState::DbUpdateEpochVersion(ui64 version,
                                       TTransactionContext &txc)
{
    LOG_DEBUG_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                DbLogPrefix() << " Update epoch version in database"
                << " version=" << version);

    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::Params>().Key(Schema::ParamKeyCurrentEpochVersion)
        .Update<Schema::Params::Value>(version);
}

void TNodeBroker::TDirtyState::DbUpdateNodeLease(const TNodeInfo &node,
                                    TTransactionContext &txc)
{
    LOG_DEBUG_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                DbLogPrefix() << " Update node " << node.IdString() << " lease in database"
                << " lease=" << node.Lease + 1
                << " expire=" << Epoch.NextEnd);

    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::Nodes>().Key(node.NodeId)
        .Update<Schema::Nodes::Lease>(node.Lease + 1)
        .Update<Schema::Nodes::Expire>(Epoch.NextEnd.GetValue());
}

void TNodeBroker::TDirtyState::DbUpdateNodeLocation(const TNodeInfo &node,
                                       TTransactionContext &txc)
{
    LOG_DEBUG_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                DbLogPrefix() << " Update node " << node.IdString() << " location in database"
                << " location=" << node.Location.ToString());

    NIceDb::TNiceDb db(txc.DB);
    using T = Schema::Nodes;
    db.Table<T>().Key(node.NodeId).Update<T::Location>(node.Location.GetSerializedLocation());
}

void TNodeBroker::TDirtyState::DbReleaseSlotIndex(const TNodeInfo &node,
                                       TTransactionContext &txc)
{

    LOG_DEBUG_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                DbLogPrefix() << " Release slot index (" << node.SlotIndex << ") node "
                << node.IdString() << " in database");
    NIceDb::TNiceDb db(txc.DB);
    using T = Schema::Nodes;
    db.Table<T>().Key(node.NodeId)
        .UpdateToNull<T::SlotIndex>();
}

void TNodeBroker::TDirtyState::DbUpdateNodeAuthorizedByCertificate(const TNodeInfo &node,
                                       TTransactionContext &txc)
{
    LOG_DEBUG_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                DbLogPrefix() << " Update node " << node.IdString() << " authorizedbycertificate in database"
                << " authorizedbycertificate=" << (node.AuthorizedByCertificate ? "true" : "false"));

    NIceDb::TNiceDb db(txc.DB);
    using T = Schema::Nodes;
    db.Table<T>().Key(node.NodeId).Update<T::AuthorizedByCertificate>(node.AuthorizedByCertificate);
}

void TNodeBroker::Handle(TEvConsole::TEvConfigNotificationRequest::TPtr &ev,
                         const TActorContext &ctx)
{
    const auto& appConfig = ev->Get()->Record.GetConfig();
    if (appConfig.HasFeatureFlags()) {
        EnableStableNodeNames = appConfig.GetFeatureFlags().GetEnableStableNodeNames();
    }

    if (ev->Get()->Record.HasLocal() && ev->Get()->Record.GetLocal()) {
        Execute(CreateTxUpdateConfig(ev), ctx);
    } else {
        // ignore and immediately ack messages from old persistent console subscriptions
        auto response = MakeHolder<TEvConsole::TEvConfigNotificationResponse>();
        response->Record.MutableConfigId()->CopyFrom(ev->Get()->Record.GetConfigId());
        ctx.Send(ev->Sender, response.Release(), 0, ev->Cookie);
    }
}

void TNodeBroker::Handle(TEvConsole::TEvReplaceConfigSubscriptionsResponse::TPtr &ev,
                         const TActorContext &ctx)
{
    auto &rec = ev->Get()->Record;
    if (rec.GetStatus().GetCode() != Ydb::StatusIds::SUCCESS) {
        LOG_ERROR_S(ctx, NKikimrServices::NODE_BROKER,
                    "Cannot subscribe for config updates: " << rec.GetStatus().GetCode()
                    << " " << rec.GetStatus().GetReason());
        return;
    }

    Execute(CreateTxUpdateConfigSubscription(ev), ctx);
}

void TNodeBroker::Handle(TEvNodeBroker::TEvListNodes::TPtr &ev,
                         const TActorContext &)
{
    TabletCounters->Cumulative()[COUNTER_LIST_NODES_REQUESTS].Increment(1);
    auto &rec = ev->Get()->Record;

    ui64 epoch = rec.GetMinEpoch();
    if (epoch > Committed.Epoch.Id) {
        AddDelayedListNodesRequest(epoch, ev);
        return;
    }

    ProcessListNodesRequest(ev);
}

void TNodeBroker::Handle(TEvNodeBroker::TEvResolveNode::TPtr &ev,
                         const TActorContext &ctx)
{
    TabletCounters->Cumulative()[COUNTER_RESOLVE_NODE_REQUESTS].Increment(1);
    ui32 nodeId = ev->Get()->Record.GetNodeId();
    TAutoPtr<TEvNodeBroker::TEvResolvedNode> resp = new TEvNodeBroker::TEvResolvedNode;

    auto it = Committed.Nodes.find(nodeId);
    if (it != Committed.Nodes.end() && it->second.Expire > ctx.Now()) {
        resp->Record.MutableStatus()->SetCode(TStatus::OK);
        FillNodeInfo(it->second, *resp->Record.MutableNode());
    } else {
        resp->Record.MutableStatus()->SetCode(TStatus::WRONG_REQUEST);
        resp->Record.MutableStatus()->SetReason("Unknown node");
    }

    LOG_TRACE_S(ctx, NKikimrServices::NODE_BROKER,
                "Send TEvResolvedNode: " << resp->ToString());

    ctx.Send(ev->Sender, resp.Release());
}

void TNodeBroker::Handle(TEvNodeBroker::TEvRegistrationRequest::TPtr &ev,
                         const TActorContext &ctx)
{
    LOG_TRACE_S(ctx, NKikimrServices::NODE_BROKER, "Handle TEvNodeBroker::TEvRegistrationRequest"
        << ": request# " << ev->Get()->Record.ShortDebugString());
    TabletCounters->Cumulative()[COUNTER_REGISTRATION_REQUESTS].Increment(1);

    class TResolveTenantActor : public TActorBootstrapped<TResolveTenantActor> {
        TEvNodeBroker::TEvRegistrationRequest::TPtr Ev;
        TActorId ReplyTo;
        NActors::TScopeId ScopeId;
        TSubDomainKey ServicedSubDomain;
        TString Error;

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::NODE_BROKER_ACTOR;
        }

        TResolveTenantActor(TEvNodeBroker::TEvRegistrationRequest::TPtr& ev, TActorId replyTo)
            : Ev(ev)
            , ReplyTo(replyTo)
        {}

        void Bootstrap(const TActorContext& ctx) {
            Become(&TThis::StateFunc);

            auto& record = Ev->Get()->Record;

            if (const auto& bridgePileName = TNodeLocation(record.GetLocation()).GetBridgePileName()) {
                if (AppData()->BridgeModeEnabled) {
                    const auto& bridge = AppData()->BridgeConfig;
                    const auto& piles = bridge.GetPiles();
                    bool found = false;
                    for (int i = 0; i < piles.size(); ++i) {
                        if (piles[i].GetName() == *bridgePileName) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        Error = TStringBuilder() << "Incorrect bridge pile name " << *bridgePileName;
                    }
                } else {
                    Error = "Bridge pile specified while bridge mode is disabled";
                }
            } else if (AppData()->BridgeModeEnabled) {
                Error = "Bridge pile not specified while bridge mode is enabled";
            }

            if (record.HasPath()) {
                auto req = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
                req->DatabaseName = AppData()->DomainsInfo->GetDomain()->Name;

                auto& rset = req->ResultSet;
                rset.emplace_back();
                auto& item = rset.back();
                item.Path = NKikimr::SplitPath(record.GetPath());
                item.RedirectRequired = false;
                item.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
                ctx.Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(req), IEventHandle::FlagTrackDelivery, 0);
            } else {
                Finish(ctx);
            }
        }

        void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) {
            const auto& navigate = ev->Get()->Request;
            auto& rset = navigate->ResultSet;
            Y_ABORT_UNLESS(rset.size() == 1);
            auto& response = rset.front();

            LOG_TRACE_S(ctx, NKikimrServices::NODE_BROKER, "Handle TEvTxProxySchemeCache::TEvNavigateKeySetResult"
                << ": response# " << response.ToString(*AppData()->TypeRegistry));

            if (response.Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok && response.DomainInfo) {
                if (response.DomainInfo->IsServerless()) {
                    ScopeId = {response.DomainInfo->ResourcesDomainKey.OwnerId, response.DomainInfo->ResourcesDomainKey.LocalPathId};
                } else {
                    ScopeId = {response.DomainInfo->DomainKey.OwnerId, response.DomainInfo->DomainKey.LocalPathId};
                }
                ServicedSubDomain = TSubDomainKey(response.DomainInfo->DomainKey.OwnerId, response.DomainInfo->DomainKey.LocalPathId);
            } else {
                LOG_WARN_S(ctx, NKikimrServices::NODE_BROKER, "Cannot resolve tenant"
                    << ": request# " << Ev->Get()->Record.ShortDebugString()
                    << ", response# " << response.ToString(*AppData()->TypeRegistry));
            }

            Finish(ctx);
        }

        void HandleUndelivered(const TActorContext& ctx) {
            Finish(ctx);
        }

        void Finish(const TActorContext& ctx) {
            LOG_TRACE_S(ctx, NKikimrServices::NODE_BROKER, "Finished resolving tenant"
                << ": request# " << Ev->Get()->Record.ShortDebugString()
                << ": scope id# " << ScopeIdToString(ScopeId)
                << ": serviced subdomain# " << ServicedSubDomain);

            Send(ReplyTo, new TEvPrivate::TEvResolvedRegistrationRequest(Ev, ScopeId, ServicedSubDomain, std::move(Error)));
            Die(ctx);
        }

        STRICT_STFUNC(StateFunc, {
            HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle)
            CFunc(TEvents::TSystem::Undelivered, HandleUndelivered)
        })
    };
    ctx.RegisterWithSameMailbox(new TResolveTenantActor(ev, SelfId()));
}

void TNodeBroker::Handle(TEvNodeBroker::TEvGracefulShutdownRequest::TPtr &ev,
                         const TActorContext &ctx) {
    LOG_TRACE_S(ctx, NKikimrServices::NODE_BROKER, "Handle TEvNodeBroker::TEvGracefulShutdownRequest"
        << ": request# " << ev->Get()->Record.ShortDebugString());
    TabletCounters->Cumulative()[COUNTER_GRACEFUL_SHUTDOWN_REQUESTS].Increment(1);
    Execute(CreateTxGracefulShutdown(ev), ctx);
}

void TNodeBroker::Handle(TEvNodeBroker::TEvExtendLeaseRequest::TPtr &ev,
                         const TActorContext &ctx)
{
    TabletCounters->Cumulative()[COUNTER_EXTEND_LEASE_REQUESTS].Increment(1);
    Execute(CreateTxExtendLease(ev), ctx);
}

void TNodeBroker::Handle(TEvNodeBroker::TEvCompactTables::TPtr &ev,
                         const TActorContext &ctx)
{
    Y_UNUSED(ev);
    Y_UNUSED(ctx);
    Executor()->CompactTables();
}

void TNodeBroker::Handle(TEvNodeBroker::TEvGetConfigRequest::TPtr &ev,
                         const TActorContext &ctx)
{
    auto resp = MakeHolder<TEvNodeBroker::TEvGetConfigResponse>();
    resp->Record.MutableConfig()->CopyFrom(Committed.Config);

    LOG_TRACE_S(ctx, NKikimrServices::NODE_BROKER,
                "Send TEvGetConfigResponse: " << resp->ToString());

    ctx.Send(ev->Sender, resp.Release());
}

void TNodeBroker::Handle(TEvNodeBroker::TEvSetConfigRequest::TPtr &ev,
                         const TActorContext &ctx)
{
    Execute(CreateTxUpdateConfig(ev), ctx);
}

void TNodeBroker::Handle(TEvNodeBroker::TEvSubscribeNodesRequest::TPtr &ev,
                         const TActorContext &ctx)
{
    TabletCounters->Cumulative()[COUNTER_SUBSCRIBE_NODES_REQUESTS].Increment(1);

    auto seqNo = ev->Get()->Record.GetSeqNo();
    auto version = ev->Get()->Record.GetCachedVersion();

    if (HasOutdatedSubscription(ev->Sender, seqNo)) {
        RemoveSubscriber(ev->Sender, ctx);
    }

    if (!Subscribers.contains(ev->Sender)) {
        auto& subscriber = AddSubscriber(ev->Sender, ev->Recipient, seqNo, version, ctx);
        SendUpdateNodes(subscriber, ctx);
    }
}

void TNodeBroker::Handle(TEvNodeBroker::TEvSyncNodesRequest::TPtr &ev,
                         const TActorContext &ctx)
{
    TabletCounters->Cumulative()[COUNTER_SYNC_NODES_REQUESTS].Increment(1);

    auto it = Subscribers.find(ev->Sender);
    if (it == Subscribers.end()) {
        return;
    }

    if (it->second.SeqNo != ev->Get()->Record.GetSeqNo()) {
        return;
    }

    if (it->second.SentVersion < Committed.Epoch.Version) {
        SendUpdateNodes(it->second, ctx);
    }

    auto response = MakeHolder<TEvNodeBroker::TEvSyncNodesResponse>();
    response->Record.SetSeqNo(it->second.SeqNo);
    SendToSubscriber(it->second, response.Release(), ev->Cookie, ctx);
}

void TNodeBroker::Handle(TEvTabletPipe::TEvServerConnected::TPtr &ev)
{
    auto res = PipeServers.emplace(ev->Get()->ServerId, TPipeServerInfo(ev->Get()->ServerId, ev->Get()->InterconnectSession));
    Y_ENSURE(res.second, "Unexpected TEvServerConnected for " << ev->Get()->ServerId);
}

void TNodeBroker::Handle(TEvTabletPipe::TEvServerDisconnected::TPtr &ev,
                         const TActorContext &ctx)
{
    auto it = PipeServers.find(ev->Get()->ServerId);
    Y_ENSURE(it != PipeServers.end(), "Unexpected TEvServerDisconnected for " << ev->Get()->ServerId);
    while (!it->second.Subscribers.empty()) {
        RemoveSubscriber(*it->second.Subscribers.begin(), ctx);
    }
    PipeServers.erase(it);
}

void TNodeBroker::Handle(TEvPrivate::TEvUpdateEpoch::TPtr &ev,
                         const TActorContext &ctx)
{
    Y_UNUSED(ev);
    if (Committed.Epoch.End > ctx.Now()) {
        LOG_INFO_S(ctx, NKikimrServices::NODE_BROKER,
                   "Epoch update event is too early");
        ScheduleEpochUpdate(ctx);
        return;
    }

    Execute(CreateTxUpdateEpoch(), ctx);
}

void TNodeBroker::Handle(TEvPrivate::TEvResolvedRegistrationRequest::TPtr &ev,
                         const TActorContext &ctx)
{
    Execute(CreateTxRegisterNode(ev), ctx);
}

void TNodeBroker::Handle(TEvPrivate::TEvProcessSubscribersQueue::TPtr &, const TActorContext &ctx)
{
    ScheduledProcessSubscribersQueue = false;
    if (!SubscribersQueue.Empty()) {
        auto& subscriber = *SubscribersQueue.Front();
        if (subscriber.SentVersion < Committed.Epoch.Version) {
            SendUpdateNodes(subscriber, ctx);
            ScheduleProcessSubscribersQueue(ctx);
        }
    }
}

TNodeBroker::TState::TState(TNodeBroker* self)
        : Self(self)
{}

TStringBuf TNodeBroker::TState::LogPrefix() const {
    return "[Committed]";
}

TNodeBroker::TDirtyState::TDirtyState(TNodeBroker* self)
        : TState(self)
{}

TStringBuf TNodeBroker::TDirtyState::LogPrefix() const {
    return "[Dirty]";
}

TStringBuf TNodeBroker::TDirtyState::DbLogPrefix() const {
    return "[DB]";
}

TNodeBroker::TNodeInfo::TNodeInfo(ui32 nodeId, ENodeState state, ui64 version, const TNodeInfoSchema& schema)
    : TEvInterconnect::TNodeInfo(nodeId, schema.GetAddress(), schema.GetHost(),
                                 schema.GetResolveHost(), schema.GetPort(),
                                 TNodeLocation(schema.GetLocation()))
    , Lease(schema.GetLease())
    , Expire(TInstant::MicroSeconds(schema.GetExpire()))
    , AuthorizedByCertificate(schema.GetAuthorizedByCertificate())
    , SlotIndex(schema.GetSlotIndex())
    , ServicedSubDomain(schema.GetServicedSubDomain())
    , State(state)
    , Version(version)
{}

TNodeBroker::TNodeInfo::TNodeInfo(ui32 nodeId, ENodeState state, ui64 version)
    : TNodeInfo(nodeId, state, version, TNodeInfoSchema())
{}

bool TNodeBroker::TNodeInfo::EqualCachedData(const TNodeInfo &other) const
{
    return Host == other.Host
        && Port == other.Port
        && ResolveHost == other.ResolveHost
        && Address == other.Address
        && Location == other.Location
        && Expire == other.Expire;
}

bool TNodeBroker::TNodeInfo::EqualExceptVersion(const TNodeInfo &other) const
{
    return EqualCachedData(other)
        && Lease == other.Lease
        && AuthorizedByCertificate == other.AuthorizedByCertificate
        && SlotIndex == other.SlotIndex
        && ServicedSubDomain == other.ServicedSubDomain
        && State == other.State;
}

TString TNodeBroker::TNodeInfo::IdString() const
{
    return TStringBuilder() << IdShortString() << " " << Host << ":" << Port;
}

TString TNodeBroker::TNodeInfo::IdShortString() const
{
    return TStringBuilder() << "#" << NodeId << ".v" << Version;
}

TString TNodeBroker::TNodeInfo::ToString() const
{
    TStringBuilder builder;
    builder << IdShortString() << " { "
        << "NodeId: " << NodeId
        << ", State: " << State
        << ", Version: " << Version
        << ", Host: " << Host
        << ", Port: " << Port
        << ", ResolveHost: " << ResolveHost
        << ", Address: " << Address
        << ", Lease: " << Lease
        << ", Expire: " << ExpirationString()
        << ", Location: " << Location.ToString()
        << ", AuthorizedByCertificate: " << AuthorizedByCertificate
        << ", SlotIndex: " << SlotIndex
        << ", ServicedSubDomain: " << ServicedSubDomain
    << " }";
    return builder;
}

TNodeInfoSchema TNodeBroker::TNodeInfo::SerializeToSchema() const {
    TNodeInfoSchema serialized;
    serialized.SetHost(Host);
    serialized.SetPort(Port);
    serialized.SetResolveHost(ResolveHost);
    serialized.SetAddress(Address);
    serialized.SetLease(Lease);
    serialized.SetExpire(Expire.MicroSeconds());
    Location.Serialize(serialized.MutableLocation(), false);
    serialized.MutableServicedSubDomain()->CopyFrom(ServicedSubDomain);
    if (SlotIndex.has_value()) {
        serialized.SetSlotIndex(*SlotIndex);
    }
    serialized.SetAuthorizedByCertificate(AuthorizedByCertificate);
    return serialized;
}

void TNodeBroker::TDbChanges::Merge(const TDbChanges &other) {
    Ready = Ready && other.Ready;
    UpdateEpoch = UpdateEpoch || other.UpdateEpoch;
    UpdateApproxEpochStart = UpdateApproxEpochStart || other.UpdateApproxEpochStart;
    UpdateMainNodesTable = UpdateMainNodesTable || other.UpdateMainNodesTable;
    UpdateNodes.insert(UpdateNodes.end(), other.UpdateNodes.begin(), other.UpdateNodes.end());
    NewVersionUpdateNodes.insert(NewVersionUpdateNodes.end(), other.NewVersionUpdateNodes.begin(),
        other.NewVersionUpdateNodes.end());
}

bool TNodeBroker::TDbChanges::HasNodeUpdates() const {
    return !UpdateNodes.empty() || !NewVersionUpdateNodes.empty();
}

TNodeBroker::TNodeBroker(const TActorId &tablet, TTabletStorageInfo *info)
        : TActor(&TThis::StateInit)
        , TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
        , Dirty(this)
        , Committed(this)
{
    TabletCountersPtr.Reset(new TProtobufTabletCounters<
        ESimpleCounters_descriptor,
        ECumulativeCounters_descriptor,
        EPercentileCounters_descriptor,
        ETxTypes_descriptor
    >());
    TabletCounters = TabletCountersPtr.Get();
}

IActor *CreateNodeBroker(const TActorId &tablet,
                         TTabletStorageInfo *info)
{
    return new TNodeBroker(tablet, info);
}

} // NNodeBroker
} // NKikimr
