#include "node_broker_impl.h"
#include "node_broker__scheme.h"

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/base/nameservice.h>
#include <ydb/core/base/path.h>
#include <ydb/core/cms/console/config_helpers.h>
#include <ydb/core/protos/node_broker.pb.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/set.h>

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

    DomainId = appData->DomainsInfo->GetDomainUidByTabletId(TabletID());
    Y_ABORT_UNLESS(DomainId < DOMAINS_COUNT);
    SingleDomain = appData->DomainsInfo->Domains.size() == 1;
    SingleDomainAlloc = SingleDomain && appData->FeatureFlags.GetEnableNodeBrokerSingleDomainMode();

    MaxStaticId = Min(appData->DynamicNameserviceConfig->MaxStaticNodeId, TActorId::MaxNodeId);
    MinDynamicId = Max(MaxStaticId + 1, (ui64)Min(appData->DynamicNameserviceConfig->MinDynamicNodeId, TActorId::MaxNodeId));
    MaxDynamicId = Max(MinDynamicId, (ui64)Min(appData->DynamicNameserviceConfig->MaxDynamicNodeId, TActorId::MaxNodeId));

    EnableStableNodeNames = appData->FeatureFlags.GetEnableStableNodeNames();

    ClearState();

    ProcessTx(CreateTxInitScheme(), ctx);
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
            str << "Served domain: " << AppData(ctx)->DomainsInfo->Domains.at(DomainId)->Name << Endl
                << "DynamicNameserviceConfig:" << Endl
                << "  MaxStaticNodeId: " << AppData(ctx)->DynamicNameserviceConfig->MaxStaticNodeId << Endl
                << "  MaxDynamicNodeId: " << AppData(ctx)->DynamicNameserviceConfig->MaxDynamicNodeId << Endl
                << "  EpochDuration: " << EpochDuration << Endl
                << "  StableNodeNamePrefix: " << StableNodeNamePrefix << Endl
                << "  BannedIds:";
            for (auto &pr : BannedIds)
                str << " [" << pr.first << ", " << pr.second << "]";
            str << Endl << Endl;
            str << "Registered nodes:" << Endl;

            TSet<ui32> ids;
            for (auto &pr : Nodes)
                ids.insert(pr.first);
            for (auto id : ids) {
                auto &node = Nodes.at(id);
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

            str << "Free Node IDs count: " << FreeIds.Count() << Endl;

            str << Endl;
            str << "Slot Indexes Pools usage: " << Endl;
            size_t totalSize = 0;
            size_t totalCapacity = 0;
            for (const auto &[subdomainKey, slotIndexesPool] : SlotIndexesPools) {
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

    TxProcessor->Clear();
}

void TNodeBroker::Die(const TActorContext &ctx)
{
    Cleanup(ctx);
    TActorBase::Die(ctx);
}

void TNodeBroker::ClearState()
{
    Nodes.clear();
    ExpiredNodes.clear();
    Hosts.clear();

    RecomputeFreeIds();
    RecomputeSlotIndexesPools();
}

void TNodeBroker::AddNode(const TNodeInfo &info)
{
    FreeIds.Reset(info.NodeId);
    if (info.SlotIndex.has_value()) {
        SlotIndexesPools[info.ServicedSubDomain].Acquire(info.SlotIndex.value());
    }    

    if (info.Expire > Epoch.Start) {
        LOG_DEBUG_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                    "Added node " << info.IdString());

        Hosts.emplace(std::make_tuple(info.Host, info.Address, info.Port), info.NodeId);
        Nodes.emplace(info.NodeId, info);
    } else {
        LOG_DEBUG_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                    "Added expired node " << info.IdString());

        ExpiredNodes.emplace(info.NodeId, info);
    }
}

void TNodeBroker::ExtendLease(TNodeInfo &node)
{
    ++node.Lease;
    node.Expire = Epoch.NextEnd;

    LOG_DEBUG_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                "Extended lease of " << node.IdString() << " up to "
                << node.ExpirationString() << " (lease " << node.Lease << ")");
}

void TNodeBroker::FixNodeId(TNodeInfo &node)
{
    ++node.Lease;
    node.Expire = TInstant::Max();

    LOG_DEBUG_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                "Fix ID for node " << node.IdString());
}

void TNodeBroker::RecomputeFreeIds()
{
    FreeIds.Clear();

    if (SingleDomainAlloc) {
        FreeIds.Set(MinDynamicId, MaxDynamicId + 1);
    } else {
        auto firstId = RewriteNodeId(MinDynamicId);
        if (firstId < MinDynamicId)
            firstId += NodeIdStep();

        auto lastId = RewriteNodeId(MaxDynamicId);
        if (lastId > MaxDynamicId)
            lastId -= NodeIdStep();

        // Only ids marked with our domain id are available
        FreeIds.Reserve(lastId + 1);
        for (ui32 id = firstId; id <= lastId; id += NodeIdStep()) {
            FreeIds.Set(id);
        }
    }

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

void TNodeBroker::RecomputeSlotIndexesPools()
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

bool TNodeBroker::IsBannedId(ui32 id) const
{
    for (auto &pr : BannedIds)
        if (id >= pr.first && id <= pr.second)
            return true;
    return false;
}

void TNodeBroker::AddDelayedListNodesRequest(ui64 epoch,
                                             TEvNodeBroker::TEvListNodes::TPtr &ev)
{
    Y_ABORT_UNLESS(epoch > Epoch.Id);
    LOG_DEBUG_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                "Delaying list nodes request for epoch #" << epoch);

    DelayedListNodesRequests.emplace(epoch, ev);
}

void TNodeBroker::ProcessListNodesRequest(TEvNodeBroker::TEvListNodes::TPtr &ev)
{
    ui64 version = ev->Get()->Record.GetCachedVersion();

    NKikimrNodeBroker::TNodesInfo info;
    Epoch.Serialize(*info.MutableEpoch());
    info.SetDomain(DomainId);
    TAutoPtr<TEvNodeBroker::TEvNodesInfo> resp = new TEvNodeBroker::TEvNodesInfo(info);
    if (version != Epoch.Version)
        resp->PreSerializedData = EpochCache;

    LOG_TRACE_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                "Send TEvNodesInfo for epoch " << Epoch.ToString());

    Send(ev->Sender, resp.Release());
}

void TNodeBroker::ProcessDelayedListNodesRequests()
{
    while (!DelayedListNodesRequests.empty()) {
        auto it = DelayedListNodesRequests.begin();
        if (it->first > Epoch.Id)
            break;

        ProcessListNodesRequest(it->second);
        DelayedListNodesRequests.erase(it);
    }
}

void TNodeBroker::ScheduleEpochUpdate(const TActorContext &ctx)
{
    auto now = ctx.Now();
    if (now >= Epoch.End) {
        ctx.Schedule(TDuration::Zero(), new TEvPrivate::TEvUpdateEpoch);
    } else {
        auto *ev = new IEventHandle(SelfId(), SelfId(), new TEvPrivate::TEvUpdateEpoch);
        EpochTimerCookieHolder.Reset(ISchedulerCookie::Make2Way());
        CreateLongTimer(ctx, Epoch.End - now, ev, AppData(ctx)->SystemPoolId,
                        EpochTimerCookieHolder.Get());

        LOG_TRACE_S(ctx, NKikimrServices::NODE_BROKER,
                    "Scheduled epoch update at " << Epoch.End);
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
        const TString name = TStringBuilder() << StableNodeNamePrefix << slotIndex.value();
        info.SetName(name);
    }
}

void TNodeBroker::ComputeNextEpochDiff(TStateDiff &diff)
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
}

void TNodeBroker::ApplyStateDiff(const TStateDiff &diff)
{
    for (auto id : diff.NodesToExpire) {
        auto it = Nodes.find(id);
        Y_ABORT_UNLESS(it != Nodes.end());

        LOG_DEBUG_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                    "Node " << it->second.IdString() << " has expired");

        Hosts.erase(std::make_tuple(it->second.Host, it->second.Address, it->second.Port));
        ExpiredNodes.emplace(id, std::move(it->second));
        Nodes.erase(it);
    }

    for (auto id : diff.NodesToRemove) {
        auto it = ExpiredNodes.find(id);
        Y_ABORT_UNLESS(it != ExpiredNodes.end());

        LOG_DEBUG_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                    "Remove node " << it->second.IdString());

        if (!IsBannedId(id) && NodeIdDomain(id) == DomainId && id >= MinDynamicId && id <= MaxDynamicId) {
            FreeIds.Set(id);
        }
        if (it->second.SlotIndex.has_value()) {
            SlotIndexesPools[it->second.ServicedSubDomain].Release(it->second.SlotIndex.value());
        }
        ExpiredNodes.erase(it);
    }

    LOG_DEBUG_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                "Move to new epoch " << diff.NewEpoch.ToString());

    Epoch = diff.NewEpoch;
}

void TNodeBroker::UpdateEpochVersion()
{
    LOG_DEBUG_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                "Update current epoch version from " << Epoch.Version
                << " to " << Epoch.Version + 1);

    ++Epoch.Version;
}

void TNodeBroker::PrepareEpochCache()
{
    LOG_DEBUG_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                "Preparing nodes list cache for epoch #" << Epoch.Id
                << " nodes=" << Nodes.size() << " expired=" << ExpiredNodes.size());

    NKikimrNodeBroker::TNodesInfo info;
    for (auto &entry : Nodes)
        FillNodeInfo(entry.second, *info.AddNodes());
    for (auto &entry : ExpiredNodes)
        FillNodeInfo(entry.second, *info.AddExpiredNodes());

    Y_PROTOBUF_SUPPRESS_NODISCARD info.SerializeToString(&EpochCache);
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
}

void TNodeBroker::SubscribeForConfigUpdates(const TActorContext &ctx)
{
    ui32 nodeBrokerItem = (ui32)NKikimrConsole::TConfigItem::NodeBrokerConfigItem;
    ui32 featureFlagsItem = (ui32)NKikimrConsole::TConfigItem::FeatureFlagsItem;
    NConsole::SubscribeViaConfigDispatcher(ctx, {nodeBrokerItem, featureFlagsItem}, ctx.SelfID);
}

void TNodeBroker::ProcessTx(ITransaction *tx,
                            const TActorContext &ctx)
{
    TxProcessor->ProcessTx(tx, ctx);
}

void TNodeBroker::ProcessTx(ui32 nodeId,
                            ITransaction *tx,
                            const TActorContext &ctx)
{
    TxProcessor->GetSubProcessor(ToString(nodeId), ctx)->ProcessTx(tx, ctx);
}

void TNodeBroker::TxCompleted(ITransaction *tx,
                              const TActorContext &ctx)
{
    TxProcessor->TxCompleted(tx, ctx);
}

void TNodeBroker::TxCompleted(ui32 nodeId,
                              ITransaction *tx,
                              const TActorContext &ctx)
{
    TxProcessor->GetSubProcessor(ToString(nodeId), ctx)->TxCompleted(tx, ctx);
}

void TNodeBroker::LoadConfigFromProto(const NKikimrNodeBroker::TConfig &config)
{
    Config = config;

    EpochDuration = TDuration::MicroSeconds(config.GetEpochDuration());
    if (EpochDuration < MIN_LEASE_DURATION) {
        LOG_ERROR_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                    "Configured lease duration (" << EpochDuration << ") is too"
                    " small. Using min. value: " << MIN_LEASE_DURATION);
        EpochDuration = MIN_LEASE_DURATION;
    }

    StableNodeNamePrefix = config.GetStableNodeNamePrefix();

    BannedIds.clear();
    for (auto &banned : config.GetBannedNodeIds())
        BannedIds.emplace_back(banned.GetFrom(), banned.GetTo());
    RecomputeFreeIds();
}

void TNodeBroker::DbAddNode(const TNodeInfo &node,
                            TTransactionContext &txc)
{
    LOG_DEBUG_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                "Adding node " << node.IdString() << " to database"
                << " resolvehost=" << node.ResolveHost
                << " address=" << node.Address
                << " dc=" << node.Location.GetDataCenterId()
                << " location=" << node.Location.ToString()
                << " lease=" << node.Lease
                << " expire=" << node.ExpirationString()
                << " servicedsubdomain=" << node.ServicedSubDomain
                << " slotindex= " << node.SlotIndex);

    NIceDb::TNiceDb db(txc.DB);
    using T = Schema::Nodes;
    db.Table<T>().Key(node.NodeId)
        .Update<T::Host>(node.Host)
        .Update<T::Port>(node.Port)
        .Update<T::ResolveHost>(node.ResolveHost)
        .Update<T::Address>(node.Address)
        .Update<T::Lease>(node.Lease)
        .Update<T::Expire>(node.Expire.GetValue())
        .Update<T::Location>(node.Location.GetSerializedLocation())
        .Update<T::ServicedSubDomain>(node.ServicedSubDomain);

    if (node.SlotIndex.has_value()) {
        db.Table<T>().Key(node.NodeId)
            .Update<T::SlotIndex>(node.SlotIndex.value());
    } else {
        db.Table<T>().Key(node.NodeId)
            .UpdateToNull<T::SlotIndex>();
    }
}

void TNodeBroker::DbApplyStateDiff(const TStateDiff &diff,
                                   TTransactionContext &txc)
{
    DbRemoveNodes(diff.NodesToRemove, txc);
    DbUpdateEpoch(diff.NewEpoch, txc);
}

void TNodeBroker::DbFixNodeId(const TNodeInfo &node,
                              TTransactionContext &txc)
{
    LOG_DEBUG_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                "Fix ID for node: " <<  node.IdString());

    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::Nodes>().Key(node.NodeId)
        .Update<Schema::Nodes::Lease>(node.Lease + 1)
        .Update<Schema::Nodes::Expire>(TInstant::Max().GetValue());
}

bool TNodeBroker::DbLoadState(TTransactionContext &txc,
                              const TActorContext &ctx)
{
    NIceDb::TNiceDb db(txc.DB);
    bool updateEpoch = false;

    if (!db.Precharge<Schema>())
        return false;

    auto configRow = db.Table<Schema::Config>()
        .Key(ConfigKeyConfig).Select<Schema::Config::Value>();
    auto subscriptionRow = db.Table<Schema::Params>()
        .Key(ParamKeyConfigSubscription).Select<Schema::Params::Value>();
    auto currentEpochIdRow = db.Table<Schema::Params>()
        .Key(ParamKeyCurrentEpochId).Select<Schema::Params::Value>();
    auto currentEpochVersionRow = db.Table<Schema::Params>()
        .Key(ParamKeyCurrentEpochVersion).Select<Schema::Params::Value>();
    auto currentEpochStartRow = db.Table<Schema::Params>()
        .Key(ParamKeyCurrentEpochStart).Select<Schema::Params::Value>();
    auto currentEpochEndRow = db.Table<Schema::Params>()
        .Key(ParamKeyCurrentEpochEnd).Select<Schema::Params::Value>();
    auto nextEpochEndRow = db.Table<Schema::Params>()
        .Key(ParamKeyNextEpochEnd).Select<Schema::Params::Value>();
    auto nodesRowset = db.Table<Schema::Nodes>()
        .Range().Select<Schema::Nodes::TColumns>();

    if (!IsReady(configRow, subscriptionRow, currentEpochIdRow,
                 currentEpochVersionRow, currentEpochStartRow,
                 currentEpochEndRow, nextEpochEndRow, nodesRowset))
        return false;

    ClearState();

    if (configRow.IsValid()) {
        auto configString = configRow.GetValue<Schema::Config::Value>();
        NKikimrNodeBroker::TConfig config;
        Y_PROTOBUF_SUPPRESS_NODISCARD config.ParseFromArray(configString.data(), configString.size());
        LoadConfigFromProto(config);

        LOG_DEBUG_S(ctx, NKikimrServices::NODE_BROKER,
                    "Loaded config:" << Endl << config.DebugString());
    } else {
        LOG_DEBUG_S(ctx, NKikimrServices::NODE_BROKER,
                    "Using default config.");

        LoadConfigFromProto(NKikimrNodeBroker::TConfig());
    }

    if (subscriptionRow.IsValid()) {
        ConfigSubscriptionId = subscriptionRow.GetValue<Schema::Params::Value>();

        LOG_DEBUG_S(ctx, NKikimrServices::NODE_BROKER,
                    "Loaded config subscription: " << ConfigSubscriptionId);
    }

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
                    "Loaded current epoch: " << Epoch.ToString());
    } else {
        // If there is no epoch start the first one.
        Epoch.Id = 1;
        Epoch.Version = 1;
        Epoch.Start = ctx.Now();
        Epoch.End = Epoch.Start + EpochDuration;
        Epoch.NextEnd = Epoch.End + EpochDuration;

        LOG_DEBUG_S(ctx, NKikimrServices::NODE_BROKER,
                    "Starting the first epoch: " << Epoch.ToString());

        updateEpoch = true;
    }

    TVector<ui32> toRemove;
    while (!nodesRowset.EndOfSet()) {
        using T = Schema::Nodes;
        auto id = nodesRowset.GetValue<T::ID>();
        // We don't remove nodes with a different domain id when there's a
        // single domain. We may have been running in a single domain allocation
        // mode, and now temporarily restarted without this mode enabled. We
        // should still support nodes that have been registered before we
        // restarted, even though it's not available for allocation.
        if (!SingleDomain && NodeIdDomain(id) != DomainId) {
            LOG_ERROR_S(ctx, NKikimrServices::NODE_BROKER,
                        "Ignoring node with wrong ID " << id << " from domain "
                        << NodeIdDomain(id) << " (expected " << DomainId <<  ")");
            toRemove.push_back(id);
        } else if (id <= MaxStaticId || id > MaxDynamicId) {
            LOG_ERROR_S(ctx, NKikimrServices::NODE_BROKER,
                        "Ignoring node with wrong ID " << id << " not in range ("
                        << MaxStaticId << ", " << MaxDynamicId << "]");
            toRemove.push_back(id);
        } else {
            auto expire = TInstant::FromValue(nodesRowset.GetValue<T::Expire>());
            std::optional<TNodeLocation> modernLocation;
            if (nodesRowset.HaveValue<T::Location>()) {
                modernLocation.emplace(TNodeLocation::FromSerialized, nodesRowset.GetValue<T::Location>());
            }

            TNodeLocation location;

            // only modern value found in database
            Y_ABORT_UNLESS(modernLocation);
            location = std::move(*modernLocation);

            TNodeInfo info{id,
                nodesRowset.GetValue<T::Address>(),
                nodesRowset.GetValue<T::Host>(),
                nodesRowset.GetValue<T::ResolveHost>(),
                (ui16)nodesRowset.GetValue<T::Port>(),
                location}; // format update pending

            info.Lease = nodesRowset.GetValue<T::Lease>();
            info.Expire = expire;
            info.ServicedSubDomain = TSubDomainKey(nodesRowset.GetValueOrDefault<T::ServicedSubDomain>());
            if (nodesRowset.HaveValue<T::SlotIndex>()) {
                info.SlotIndex = nodesRowset.GetValue<T::SlotIndex>();
            } 
            AddNode(info);

            LOG_DEBUG_S(ctx, NKikimrServices::NODE_BROKER,
                        "Loaded node " << info.IdString()
                        << " expiring " << info.ExpirationString());
        }

        if (!nodesRowset.Next())
            return false;
    }

    DbRemoveNodes(toRemove, txc);
    if (updateEpoch)
        DbUpdateEpoch(Epoch, txc);

    return true;
}

void TNodeBroker::DbRemoveNodes(const TVector<ui32> &nodes,
                                TTransactionContext &txc)
{
    NIceDb::TNiceDb db(txc.DB);
    for (auto id : nodes) {
        LOG_DEBUG_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                    "Removing node #" << id << " from database");

        db.Table<Schema::Nodes>().Key(id).Delete();
    }
}

void TNodeBroker::DbUpdateConfig(const NKikimrNodeBroker::TConfig &config,
                                 TTransactionContext &txc)
{
    LOG_DEBUG_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                "Update config in database"
                << " config=" << config.ShortDebugString());

    TString value;
    Y_PROTOBUF_SUPPRESS_NODISCARD config.SerializeToString(&value);
    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::Config>().Key(ConfigKeyConfig)
        .Update<Schema::Config::Value>(value);
}

void TNodeBroker::DbUpdateConfigSubscription(ui64 subscriptionId,
                                             TTransactionContext &txc)
{
    LOG_DEBUG_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                "Update config subscription in database"
                << " id=" << subscriptionId);

    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::Params>().Key(ParamKeyConfigSubscription)
        .Update<Schema::Params::Value>(subscriptionId);
}

void TNodeBroker::DbUpdateEpoch(const TEpochInfo &epoch,
                                TTransactionContext &txc)
{
    LOG_DEBUG_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                "Update epoch in database: " << epoch.ToString());

    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::Params>().Key(ParamKeyCurrentEpochId)
        .Update<Schema::Params::Value>(epoch.Id);
    db.Table<Schema::Params>().Key(ParamKeyCurrentEpochVersion)
        .Update<Schema::Params::Value>(epoch.Version);
    db.Table<Schema::Params>().Key(ParamKeyCurrentEpochStart)
        .Update<Schema::Params::Value>(epoch.Start.GetValue());
    db.Table<Schema::Params>().Key(ParamKeyCurrentEpochEnd)
        .Update<Schema::Params::Value>(epoch.End.GetValue());
    db.Table<Schema::Params>().Key(ParamKeyNextEpochEnd)
        .Update<Schema::Params::Value>(epoch.NextEnd.GetValue());
}

void TNodeBroker::DbUpdateEpochVersion(ui64 version,
                                       TTransactionContext &txc)
{
    LOG_DEBUG_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                "Update epoch version in database"
                << " version=" << version);

    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::Params>().Key(ParamKeyCurrentEpochVersion)
        .Update<Schema::Params::Value>(version);
}

void TNodeBroker::DbUpdateNodeLease(const TNodeInfo &node,
                                    TTransactionContext &txc)
{
    LOG_DEBUG_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                "Update node " << node.IdString() << " lease in database"
                << " lease=" << node.Lease + 1
                << " expire=" << Epoch.NextEnd);

    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::Nodes>().Key(node.NodeId)
        .Update<Schema::Nodes::Lease>(node.Lease + 1)
        .Update<Schema::Nodes::Expire>(Epoch.NextEnd.GetValue());
}

void TNodeBroker::DbUpdateNodeLocation(const TNodeInfo &node,
                                       TTransactionContext &txc)
{
    LOG_DEBUG_S(TActorContext::AsActorContext(), NKikimrServices::NODE_BROKER,
                "Update node " << node.IdString() << " location in database"
                << " location=" << node.Location.ToString());

    NIceDb::TNiceDb db(txc.DB);
    using T = Schema::Nodes;
    db.Table<T>().Key(node.NodeId).Update<T::Location>(node.Location.GetSerializedLocation());
}

void TNodeBroker::Handle(TEvConsole::TEvConfigNotificationRequest::TPtr &ev,
                         const TActorContext &ctx)
{   
    const auto& appConfig = ev->Get()->Record.GetConfig();
    if (appConfig.HasFeatureFlags()) {
        EnableStableNodeNames = appConfig.GetFeatureFlags().GetEnableStableNodeNames();
    }

    if (ev->Get()->Record.HasLocal() && ev->Get()->Record.GetLocal()) {
        ProcessTx(CreateTxUpdateConfig(ev), ctx);
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

    ProcessTx(0, CreateTxUpdateConfigSubscription(ev), ctx);
}

void TNodeBroker::Handle(TEvNodeBroker::TEvListNodes::TPtr &ev,
                         const TActorContext &)
{
    auto &rec = ev->Get()->Record;

    ui64 epoch = rec.GetMinEpoch();
    if (epoch > Epoch.Id) {
        AddDelayedListNodesRequest(epoch, ev);
        return;
    }

    ProcessListNodesRequest(ev);
}

void TNodeBroker::Handle(TEvNodeBroker::TEvResolveNode::TPtr &ev,
                         const TActorContext &ctx)
{
    ui32 nodeId = ev->Get()->Record.GetNodeId();
    TAutoPtr<TEvNodeBroker::TEvResolvedNode> resp = new TEvNodeBroker::TEvResolvedNode;

    auto it = Nodes.find(nodeId);
    if (it != Nodes.end() && it->second.Expire > ctx.Now()) {
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

    class TRegisterNodeActor : public TActorBootstrapped<TRegisterNodeActor> {
        TEvNodeBroker::TEvRegistrationRequest::TPtr Ev;
        TNodeBroker *Self;
        NActors::TScopeId ScopeId;
        TSubDomainKey ServicedSubDomain;

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::NODE_BROKER_ACTOR;
        }

        TRegisterNodeActor(TEvNodeBroker::TEvRegistrationRequest::TPtr& ev, TNodeBroker *self)
            : Ev(ev)
            , Self(self)
        {}

        void Bootstrap(const TActorContext& ctx) {
            Become(&TThis::StateFunc);

            auto& record = Ev->Get()->Record;

            if (record.HasPath()) {
                auto req = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
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

            Self->ProcessTx(Self->CreateTxRegisterNode(Ev, ScopeId, ServicedSubDomain), ctx);
            Die(ctx);
        }

        STRICT_STFUNC(StateFunc, {
            HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle)
            CFunc(TEvents::TSystem::Undelivered, HandleUndelivered)
        })
    };
    ctx.RegisterWithSameMailbox(new TRegisterNodeActor(ev, this));
}

void TNodeBroker::Handle(TEvNodeBroker::TEvExtendLeaseRequest::TPtr &ev,
                         const TActorContext &ctx)
{
    ui32 nodeId = ev->Get()->Record.GetNodeId();
    ProcessTx(nodeId, CreateTxExtendLease(ev), ctx);
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
    resp->Record.MutableConfig()->CopyFrom(Config);

    LOG_TRACE_S(ctx, NKikimrServices::NODE_BROKER,
                "Send TEvGetConfigResponse: " << resp->ToString());

    ctx.Send(ev->Sender, resp.Release());
}

void TNodeBroker::Handle(TEvNodeBroker::TEvSetConfigRequest::TPtr &ev,
                         const TActorContext &ctx)
{
    ProcessTx(CreateTxUpdateConfig(ev), ctx);
}

void TNodeBroker::Handle(TEvPrivate::TEvUpdateEpoch::TPtr &ev,
                         const TActorContext &ctx)
{
    Y_UNUSED(ev);
    if (Epoch.End > ctx.Now()) {
        LOG_INFO_S(ctx, NKikimrServices::NODE_BROKER,
                   "Epoch update event is too early");
        ScheduleEpochUpdate(ctx);
        return;
    }

    ProcessTx(CreateTxUpdateEpoch(), ctx);
}

IActor *CreateNodeBroker(const TActorId &tablet,
                         TTabletStorageInfo *info)
{
    return new TNodeBroker(tablet, info);
}

} // NNodeBroker
} // NKikimr
