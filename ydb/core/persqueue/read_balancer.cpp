#include "read_balancer.h"
#include "read_balancer__balancing.h"
#include "read_balancer__txpreinit.h"
#include "read_balancer__txwrite.h"

#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/protos/counters_pq.pb.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/tablet/tablet_exception.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/random_provider/random_provider.h>

namespace NKikimr {
namespace NPQ {

using namespace NBalancing;


static constexpr TDuration ACL_SUCCESS_RETRY_TIMEOUT = TDuration::Seconds(30);
static constexpr TDuration ACL_ERROR_RETRY_TIMEOUT = TDuration::Seconds(5);
static constexpr TDuration ACL_EXPIRATION_TIMEOUT = TDuration::Minutes(5);

TString EncodeAnchor(const TString& v) {
    auto r = Base64Encode(v);
    while (r.EndsWith('=')) {
        r.resize(r.size() - 1);
    }
    return r;
}

TPersQueueReadBalancer::TPersQueueReadBalancer(const TActorId &tablet, TTabletStorageInfo *info)
        : TActor(&TThis::StateInit)
        , TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
        , Inited(false)
        , PathId(0)
        , Generation(0)
        , Version(-1)
        , MaxPartsPerTablet(0)
        , SchemeShardId(0)
        , LastACLUpdate(TInstant::Zero())
        , TxId(0)
        , NumActiveParts(0)
        , MaxIdx(0)
        , NextPartitionId(0)
        , NextPartitionIdForWrite(0)
        , StartPartitionIdForWrite(0)
        , TotalGroups(0)
        , ResourceMetrics(nullptr)
        , WaitingForACL(false)
        , StatsReportRound(0)
    {
        Balancer = std::make_unique<TBalancer>(*this);
    }

struct TPersQueueReadBalancer::TTxWritePartitionStats : public ITransaction {
    TPersQueueReadBalancer * const Self;

    TTxWritePartitionStats(TPersQueueReadBalancer *self)
        : Self(self)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        Self->TTxWritePartitionStatsScheduled = false;

        NIceDb::TNiceDb db(txc.DB);
        for (auto& s : Self->AggregatedStats.Stats) {
            auto partition = s.first;
            auto& stats = s.second;

            auto it = Self->PartitionsInfo.find(partition);
            if (it == Self->PartitionsInfo.end()) {
                continue;
            }

            db.Table<Schema::Partitions>().Key(partition).Update(
                NIceDb::TUpdate<Schema::Partitions::DataSize>(stats.DataSize),
                NIceDb::TUpdate<Schema::Partitions::UsedReserveSize>(stats.UsedReserveSize)
            );
        }

        return true;
    }

    void Complete(const TActorContext&) override {};
};

void TPersQueueReadBalancer::Die(const TActorContext& ctx) {
    StopFindSubDomainPathId();
    StopWatchingSubDomainPathId();

    for (auto& pipe : TabletPipes) {
        NTabletPipe::CloseClient(ctx, pipe.second.PipeActor);
    }
    TabletPipes.clear();
    if (PartitionsScaleManager) {
        PartitionsScaleManager->Die(ctx);
    }
    TActor<TPersQueueReadBalancer>::Die(ctx);
}

void TPersQueueReadBalancer::OnActivateExecutor(const TActorContext &ctx) {
    ResourceMetrics = Executor()->GetResourceMetrics();
    Become(&TThis::StateWork);
    if (Executor()->GetStats().IsFollower)
        Y_ABORT("is follower works well with Balancer?");
    else
        Execute(new TTxPreInit(this), ctx);
}

void TPersQueueReadBalancer::OnDetach(const TActorContext &ctx) {
    Die(ctx);
}

void TPersQueueReadBalancer::OnTabletDead(TEvTablet::TEvTabletDead::TPtr&, const TActorContext &ctx) {
    Die(ctx);
}

void TPersQueueReadBalancer::DefaultSignalTabletActive(const TActorContext &) {
    // must be empty
}

void TPersQueueReadBalancer::InitDone(const TActorContext &ctx) {
    if (SubDomainPathId) {
        StartWatchingSubDomainPathId();
    } else {
        StartFindSubDomainPathId(true);
    }

    StartPartitionIdForWrite = NextPartitionIdForWrite = rand() % TotalGroups;

    TStringBuilder s;
    s << "BALANCER INIT DONE for " << Topic << ": ";
    for (auto& p : PartitionsInfo) {
        s << "(" << p.first << ", " << p.second.TabletId << ") ";
    }
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER, s);

    for (auto &ev : UpdateEvents) {
        ctx.Send(ctx.SelfID, ev.Release());
    }
    UpdateEvents.clear();

    for (auto &ev : RegisterEvents) {
        ctx.Send(ctx.SelfID, ev.Release());
    }
    RegisterEvents.clear();

    auto wakeupInterval = std::max<ui64>(AppData(ctx)->PQConfig.GetBalancerWakeupIntervalSec(), 1);
    ctx.Schedule(TDuration::Seconds(wakeupInterval), new TEvents::TEvWakeup());

    ctx.Send(ctx.SelfID, new TEvPersQueue::TEvUpdateACL());
}

void TPersQueueReadBalancer::HandleWakeup(TEvents::TEvWakeup::TPtr& ev, const TActorContext &ctx) {
    LOG_DEBUG(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER, TStringBuilder() << "TPersQueueReadBalancer::HandleWakeup");

    switch (ev->Get()->Tag) {
        case TPartitionScaleManager::TRY_SCALE_REQUEST_WAKE_UP_TAG: {
            if (PartitionsScaleManager && SplitMergeEnabled(TabletConfig)) {
                PartitionsScaleManager->TrySendScaleRequest(ctx);
            }
            break;
        }
        default: {
            GetStat(ctx); //TODO: do it only on signals from outerspace right now
            auto wakeupInterval = std::max<ui64>(AppData(ctx)->PQConfig.GetBalancerWakeupIntervalSec(), 1);
            ctx.Schedule(TDuration::Seconds(wakeupInterval), new TEvents::TEvWakeup());
        }
    }
}

void TPersQueueReadBalancer::HandleUpdateACL(TEvPersQueue::TEvUpdateACL::TPtr&, const TActorContext &ctx) {
    GetACL(ctx);
}


bool TPersQueueReadBalancer::OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext& ctx) {
    if (!ev) {
        return true;
    }

    TString str = GenerateStat();
    ctx.Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes(str));
    return true;
}

TString TPersQueueReadBalancer::GenerateStat() {
    auto& metrics = AggregatedStats.Metrics;
    auto balancerStatistcs = Balancer->GetStatistics();

    TStringStream str;
    HTML(str) {
        str << "<style>"
            << " .properties { border-bottom-style: solid; border-top-style: solid; border-width: 1px; border-color: darkgrey; padding-bottom: 10px; } "
            << " .properties>tbody>tr>td { padding-left: 10px; padding-right: 10px; } "
            << " .tgrid { width: 100%; border: 0; }"
            << " .tgrid>tbody>tr>td { vertical-align: top; }"
            << "</style>";

        TAG(TH3) {str << "PersQueueReadBalancer " << TabletID() << " (" << Path << ")";}

        auto property = [&](const TString& name, const auto value) {
            TABLER() {
                TABLED() { str << name;}
                TABLED() { str << value; }
            }
        };

        UL_CLASS("nav nav-tabs") {
            LI_CLASS("active") {
                str << "<a href=\"#generic\" data-toggle=\"tab\">Generic Info</a>";
            }
            LI() {
                str << "<a href=\"#partitions\" data-toggle=\"tab\">Partitions</a>";
            }
            for (auto& consumer : balancerStatistcs.Consumers) {
                LI() {
                    str << "<a href=\"#c_" << EncodeAnchor(consumer.ConsumerName) << "\" data-toggle=\"tab\">" << NPersQueue::ConvertOldConsumerName(consumer.ConsumerName) << "</a>";
                }
            }
        }

        DIV_CLASS("tab-content") {
            DIV_CLASS_ID("tab-pane fade in active", "generic") {
                TABLE_CLASS("tgrid") {
                    TABLEBODY() {
                        TABLER() {
                            TABLED() {
                                TABLE_CLASS("properties") {
                                    CAPTION() { str << "Tablet info"; }
                                    TABLEBODY() {
                                        property("Topic", Topic);
                                        property("Path", Path);
                                        property("Initialized", Inited ? "yes" : "no");
                                        property("SchemeShard", TStringBuilder() << "<a href=\"?TabletID=" << SchemeShardId << "\">" << SchemeShardId << "</a>");
                                        property("PathId", PathId);
                                        property("Version", Version);
                                        property("Generation", Generation);
                                    }
                                }
                            }
                            TABLED() {
                                if (Inited) {
                                    TABLE_CLASS("properties") {
                                        CAPTION() { str << "Statistics"; }
                                        TABLEBODY() {
                                            property("Active pipes", balancerStatistcs.Sessions.size());
                                            property("Active partitions", NumActiveParts);
                                            property("Total data size", AggregatedStats.TotalDataSize);
                                            property("Reserve size", PartitionReserveSize());
                                            property("Used reserve size", AggregatedStats.TotalUsedReserveSize);
                                            property("[Total/Max/Avg]WriteSpeedSec", TStringBuilder() << metrics.TotalAvgWriteSpeedPerSec << "/" << metrics.MaxAvgWriteSpeedPerSec << "/" << metrics.TotalAvgWriteSpeedPerSec / NumActiveParts);
                                            property("[Total/Max/Avg]WriteSpeedMin", TStringBuilder() << metrics.TotalAvgWriteSpeedPerMin << "/" << metrics.MaxAvgWriteSpeedPerMin << "/" << metrics.TotalAvgWriteSpeedPerMin / NumActiveParts);
                                            property("[Total/Max/Avg]WriteSpeedHour", TStringBuilder() << metrics.TotalAvgWriteSpeedPerHour << "/" << metrics.MaxAvgWriteSpeedPerHour << "/" << metrics.TotalAvgWriteSpeedPerHour / NumActiveParts);
                                            property("[Total/Max/Avg]WriteSpeedDay", TStringBuilder() << metrics.TotalAvgWriteSpeedPerDay << "/" << metrics.MaxAvgWriteSpeedPerDay << "/" << metrics.TotalAvgWriteSpeedPerDay / NumActiveParts);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            DIV_CLASS_ID("tab-pane fade", "partitions") {
                TABLE_CLASS("table") {
                    TABLEHEAD() {
                        TABLER() {
                            TABLEH() {str << "partition";}
                            TABLEH() { str << "tabletId";}
                            TABLEH() { str << "Size";}
                        }
                    }
                    TABLEBODY() {
                        for (auto& [partitionId, partitionInfo] : PartitionsInfo) {
                            const auto& stats = AggregatedStats.Stats[partitionId];

                            TABLER() {
                                TABLED() { str << partitionId;}
                                TABLED() { HREF(TStringBuilder() << "?TabletID=" << partitionInfo.TabletId) { str << partitionInfo.TabletId; } }
                                TABLED() { str << stats.DataSize;}
                            }
                        }
                    }
                }
            }

            Balancer->RenderApp(str);
        }
    }
    return str.Str();
}


void TPersQueueReadBalancer::HandleOnInit(TEvPersQueue::TEvUpdateBalancerConfig::TPtr &ev, const TActorContext&) {

    UpdateEvents.push_back(ev->Release().Release());
}

void TPersQueueReadBalancer::Handle(TEvPersQueue::TEvGetPartitionIdForWrite::TPtr &ev, const TActorContext &ctx) {
    NextPartitionIdForWrite = (NextPartitionIdForWrite + 1) % TotalGroups; //TODO: change here when there will be more than 1 partition in partition_group.
    THolder<TEvPersQueue::TEvGetPartitionIdForWriteResponse> response = MakeHolder<TEvPersQueue::TEvGetPartitionIdForWriteResponse>();
    response->Record.SetPartitionId(NextPartitionIdForWrite);
    ctx.Send(ev->Sender, response.Release());
    if (NextPartitionIdForWrite == StartPartitionIdForWrite) { // randomize next cycle
        StartPartitionIdForWrite = NextPartitionIdForWrite = rand() % TotalGroups;
    }
}


void TPersQueueReadBalancer::Handle(TEvPersQueue::TEvCheckACL::TPtr &ev, const TActorContext &ctx) {

    if (!AppData(ctx)->PQConfig.GetCheckACL()) {
        RespondWithACL(ev, NKikimrPQ::EAccess::ALLOWED, "", ctx);
        return;
    }

    if (ctx.Now() > LastACLUpdate + ACL_EXPIRATION_TIMEOUT || Topic.empty()) { //Topic.empty is only for tests
        WaitingACLRequests.push_back(ev);
        return;
    }

    auto& record = ev->Get()->Record;

    if (record.GetToken().empty()) {
        if (record.GetOperation() == NKikimrPQ::EOperation::WRITE_OP && TabletConfig.GetRequireAuthWrite() ||
                 record.GetOperation() == NKikimrPQ::EOperation::READ_OP && TabletConfig.GetRequireAuthRead()) {
            RespondWithACL(ev, NKikimrPQ::EAccess::DENIED, TStringBuilder() << "topic " << Topic << " requires authentication", ctx);
        } else {
            RespondWithACL(ev, NKikimrPQ::EAccess::ALLOWED, "", ctx);
        }
        return;
    }

    NACLib::TUserToken token(record.GetToken());
    CheckACL(ev, token, ctx);
}


void TPersQueueReadBalancer::RespondWithACL(
        const TEvPersQueue::TEvCheckACL::TPtr &request,
        const NKikimrPQ::EAccess &access,
        const TString &error,
        const TActorContext &ctx) {
    THolder<TEvPersQueue::TEvCheckACLResponse> res{new TEvPersQueue::TEvCheckACLResponse};
    res->Record.SetTopic(Topic);
    res->Record.SetPath(Path);
    res->Record.SetAccess(access);
    res->Record.SetError(error);
    ctx.Send(request->Sender, res.Release());
}

void TPersQueueReadBalancer::CheckACL(const TEvPersQueue::TEvCheckACL::TPtr &request, const NACLib::TUserToken& token, const TActorContext &ctx) {
    NACLib::EAccessRights rights = NACLib::EAccessRights::UpdateRow;
    const auto& record = request->Get()->Record;
    switch(record.GetOperation()) {
        case NKikimrPQ::EOperation::READ_OP:
            rights = NACLib::EAccessRights::SelectRow;
            break;
        case NKikimrPQ::EOperation::WRITE_OP:
            rights = NACLib::EAccessRights::UpdateRow;
            break;
    };

    TString user = record.HasUser() ? record.GetUser() : "";

    if (record.GetOperation() == NKikimrPQ::EOperation::READ_OP) {
        if (!Consumers.contains(user)) {
            RespondWithACL(request, NKikimrPQ::EAccess::DENIED, TStringBuilder() << "no read rule provided for consumer '"
                    << NPersQueue::ConvertOldConsumerName(user, ctx)
                    << "' that allows to read topic from original cluster '" << NPersQueue::GetDC(Topic)
                    << "'; may be there is read rule with mode all-original only and you are reading with mirrored topics. Change read-rule to mirror-to-<cluster> or options of reading process.", ctx);
            return;
        }
    }
    if (ACL.CheckAccess(rights, token)) {
        RespondWithACL(request, NKikimrPQ::EAccess::ALLOWED, "", ctx);
    } else {
        RespondWithACL(request, NKikimrPQ::EAccess::DENIED, TStringBuilder() << "access denied for consumer '" << NPersQueue::ConvertOldConsumerName(user, ctx) << "' : no " << (rights == NACLib::EAccessRights::SelectRow ? "ReadTopic" : "WriteTopic") << " permission" , ctx);
    }
}

void TPersQueueReadBalancer::Handle(TEvPersQueue::TEvDescribe::TPtr &ev, const TActorContext& ctx) {
    if (ctx.Now() > LastACLUpdate + ACL_EXPIRATION_TIMEOUT || Topic.empty()) { //Topic.empty is only for tests
        WaitingDescribeRequests.push_back(ev);
        return;
    } else {
        THolder<TEvPersQueue::TEvDescribeResponse> res{new TEvPersQueue::TEvDescribeResponse};
        res->Record.MutableConfig()->CopyFrom(TabletConfig);
        res->Record.MutableConfig()->ClearAllPartitions();
        res->Record.SetVersion(Version);
        res->Record.SetTopicName(Topic);
        res->Record.SetPartitionPerTablet(MaxPartsPerTablet);
        res->Record.SetSchemeShardId(SchemeShardId);
        res->Record.SetBalancerTabletId(TabletID());
        res->Record.SetSecurityObject(ACL.SerializeAsString());
        for (auto& parts : PartitionsInfo) {
            auto p = res->Record.AddPartitions();
            p->SetPartition(parts.first);
            p->SetTabletId(parts.second.TabletId);
        }
        ctx.Send(ev->Sender, res.Release());
    }
}


void TPersQueueReadBalancer::Handle(TEvPersQueue::TEvUpdateBalancerConfig::TPtr &ev, const TActorContext& ctx) {
    auto& record = ev->Get()->Record;
    if ((int)record.GetVersion() < Version && Inited) {
        THolder<TEvPersQueue::TEvUpdateConfigResponse> res{new TEvPersQueue::TEvUpdateConfigResponse};
        res->Record.SetStatus(NKikimrPQ::ERROR_BAD_VERSION);
        res->Record.SetTxId(record.GetTxId());
        res->Record.SetOrigin(TabletID());
        ctx.Send(ev->Sender, res.Release());
        return;
    }

    if ((int)record.GetVersion() == Version) {
        if (!WaitingResponse.empty()) { //got transaction infly
            WaitingResponse.push_back(ev->Sender);
        } else { //version already applied
            LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER, "BALANCER Topic " << Topic << "Tablet " << TabletID()
                            << " Config already applied version " << record.GetVersion() << " actor " << ev->Sender
                            << " txId " << record.GetTxId());
            THolder<TEvPersQueue::TEvUpdateConfigResponse> res{new TEvPersQueue::TEvUpdateConfigResponse};
            res->Record.SetStatus(NKikimrPQ::OK);
            res->Record.SetTxId(record.GetTxId());
            res->Record.SetOrigin(TabletID());
            ctx.Send(ev->Sender, res.Release());
        }
        return;
    }

    if ((int)record.GetVersion() > Version && !WaitingResponse.empty()) { //old transaction is not done yet
        THolder<TEvPersQueue::TEvUpdateConfigResponse> res{new TEvPersQueue::TEvUpdateConfigResponse};
        res->Record.SetStatus(NKikimrPQ::ERROR_UPDATE_IN_PROGRESS);
        res->Record.SetTxId(ev->Get()->Record.GetTxId());
        res->Record.SetOrigin(TabletID());
        ctx.Send(ev->Sender, res.Release());
        return;
    }
    WaitingResponse.push_back(ev->Sender);

    Version = record.GetVersion();
    MaxPartsPerTablet = record.GetPartitionPerTablet();
    PathId = record.GetPathId();
    Topic = std::move(record.GetTopicName());
    Path = std::move(record.GetPath());
    TxId = record.GetTxId();
    TabletConfig = std::move(record.GetTabletConfig());

    if (!TabletConfig.GetAllPartitions().size()) {
        for (auto& p : record.GetPartitions()) {
            auto* ap = TabletConfig.AddAllPartitions();
            ap->SetPartitionId(p.GetPartition());
            ap->SetTabletId(p.GetTabletId());
            ap->SetCreateVersion(p.GetCreateVersion());
            if (p.HasKeyRange()) {
                ap->MutableKeyRange()->CopyFrom(p.GetKeyRange());
            }
            ap->SetStatus(p.GetStatus());
            ap->MutableParentPartitionIds()->Reserve(p.GetParentPartitionIds().size());
            for (const auto parent : p.GetParentPartitionIds()) {
                ap->MutableParentPartitionIds()->AddAlreadyReserved(parent);
            }
            ap->MutableChildPartitionIds()->Reserve(p.GetChildPartitionIds().size());
            for (const auto children : p.GetChildPartitionIds()) {
                ap->MutableChildPartitionIds()->AddAlreadyReserved(children);
            }
        }
    }

    Migrate(TabletConfig);

    SchemeShardId = record.GetSchemeShardId();
    TotalGroups = record.HasTotalGroupCount() ? record.GetTotalGroupCount() : 0;

    ui32 prevNextPartitionId = NextPartitionId;
    NextPartitionId = record.HasNextPartitionId() ? record.GetNextPartitionId() : 0;

    if (record.HasSubDomainPathId()) {
        SubDomainPathId.emplace(record.GetSchemeShardId(), record.GetSubDomainPathId());
    }

    PartitionGraph = MakePartitionGraph(record);

    auto oldConsumers = std::move(Consumers);
    Consumers.clear();
    for (auto& consumer : TabletConfig.GetConsumers()) {
        auto it = oldConsumers.find(consumer.GetName());
        if (it != oldConsumers.end()) {
            Consumers[consumer.GetName()] = std::move(it->second);
        }
    }


    std::vector<std::pair<ui64, TTabletInfo>> newTablets;
    std::vector<std::pair<ui32, ui32>> newGroups;
    std::vector<std::pair<ui64, TTabletInfo>> reallocatedTablets;

    if (SplitMergeEnabled(TabletConfig)) {
        if (!PartitionsScaleManager) {
            PartitionsScaleManager = std::make_unique<TPartitionScaleManager>(Topic, DatabasePath, PathId, Version, TabletConfig);
        } else {
            PartitionsScaleManager->UpdateBalancerConfig(PathId, Version, TabletConfig);
        }
    }

    for (auto& p : record.GetTablets()) {
        auto it = TabletsInfo.find(p.GetTabletId());
        if (it == TabletsInfo.end()) {
            TTabletInfo info{p.GetOwner(), p.GetIdx()};
            TabletsInfo[p.GetTabletId()] = info;
            newTablets.push_back(std::make_pair(p.GetTabletId(), info));
        } else {
            if (it->second.Owner != p.GetOwner() || it->second.Idx != p.GetIdx()) {
                TTabletInfo info{p.GetOwner(), p.GetIdx()};
                TabletsInfo[p.GetTabletId()] = info;
                reallocatedTablets.push_back(std::make_pair(p.GetTabletId(), info));
            }
        }

    }

    std::map<ui32, TPartitionInfo> partitionsInfo;
    std::vector<TPartInfo> newPartitions;
    std::vector<ui32> newPartitionsIds;
    for (auto& p : record.GetPartitions()) {
        auto it = PartitionsInfo.find(p.GetPartition());
        if (it == PartitionsInfo.end()) {
            Y_ABORT_UNLESS(p.GetPartition() >= prevNextPartitionId && p.GetPartition() < NextPartitionId || NextPartitionId == 0);

            partitionsInfo[p.GetPartition()] = {p.GetTabletId()};

            newPartitionsIds.push_back(p.GetPartition());
            newPartitions.push_back(TPartInfo{p.GetPartition(), p.GetTabletId(), 0, p.GetKeyRange()});

            ++NumActiveParts;

            // for back compatibility. Remove it after 24-3
            newGroups.push_back({p.GetGroup(), p.GetPartition()});
        } else { //group is already defined
            partitionsInfo[p.GetPartition()] = it->second;
        }
    }

    if (TotalGroups == 0) {
        NextPartitionId = TotalGroups = partitionsInfo.size(); // this will not work when we support the deletion of the partition
    }

    std::vector<ui32> deletedPartitions;
    for (auto& p : PartitionsInfo) {
        if (partitionsInfo.find(p.first) == partitionsInfo.end()) {
            Y_ABORT("deleting of partitions is not fully supported yet");
            deletedPartitions.push_back(p.first);
        }
    }
    PartitionsInfo = std::unordered_map<ui32, TPartitionInfo>(partitionsInfo.rbegin(), partitionsInfo.rend());

    Balancer->UpdateConfig(newPartitionsIds, deletedPartitions, ctx);

    Execute(new TTxWrite(this, std::move(deletedPartitions), std::move(newPartitions), std::move(newTablets), std::move(newGroups), std::move(reallocatedTablets)), ctx);

    if (SubDomainPathId && (!WatchingSubDomainPathId || *WatchingSubDomainPathId != *SubDomainPathId)) {
        StartWatchingSubDomainPathId();
    }

    UpdateConfigCounters();
}


TStringBuilder TPersQueueReadBalancer::GetPrefix() const {
    return TStringBuilder() << "tablet " << TabletID() << " topic " << Topic << " ";
}


void TPersQueueReadBalancer::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx)
{
    auto tabletId = ev->Get()->TabletId;
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER, GetPrefix() << "TEvClientDestroyed " << tabletId);

    ClosePipe(tabletId, ctx);
    RequestTabletIfNeeded(tabletId, ctx, true);
}


void TPersQueueReadBalancer::Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx)
{
    auto tabletId = ev->Get()->TabletId;

    PipesRequested.erase(tabletId);

    if (ev->Get()->Status != NKikimrProto::OK) {
        ClosePipe(ev->Get()->TabletId, ctx);
        RequestTabletIfNeeded(ev->Get()->TabletId, ctx, true);

        LOG_ERROR_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER, GetPrefix() << "TEvClientConnected Status " << ev->Get()->Status << ", TabletId " << tabletId);
        return;
    }

    Y_VERIFY_DEBUG_S(ev->Get()->Generation, "Tablet generation should be greater than 0");

    auto it = TabletPipes.find(tabletId);
    if (it != TabletPipes.end()) {
        it->second.Generation = ev->Get()->Generation;
        it->second.NodeId = ev->Get()->ServerId.NodeId();

        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER, GetPrefix() << "TEvClientConnected TabletId " << tabletId << ", NodeId " << ev->Get()->ServerId.NodeId() << ", Generation " << ev->Get()->Generation);
    }
    else
        LOG_INFO_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER, GetPrefix() << "TEvClientConnected Pipe is not found, TabletId " << tabletId);
}

void TPersQueueReadBalancer::ClosePipe(const ui64 tabletId, const TActorContext& ctx)
{
    auto it = TabletPipes.find(tabletId);
    if (it != TabletPipes.end()) {
        NTabletPipe::CloseClient(ctx, it->second.PipeActor);
        TabletPipes.erase(it);
        PipesRequested.erase(tabletId);
    }
}

TActorId TPersQueueReadBalancer::GetPipeClient(const ui64 tabletId, const TActorContext& ctx) {
    TActorId pipeClient;

    auto it = TabletPipes.find(tabletId);
    if (it == TabletPipes.end()) {
        NTabletPipe::TClientConfig clientConfig(NTabletPipe::TClientRetryPolicy::WithRetries());
        pipeClient = ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, tabletId, clientConfig));
        TabletPipes[tabletId].PipeActor = pipeClient;
        auto res = PipesRequested.insert(tabletId);
        Y_ABORT_UNLESS(res.second);
    } else {
        pipeClient = it->second.PipeActor;
    }

    return pipeClient;
}

void TPersQueueReadBalancer::RequestTabletIfNeeded(const ui64 tabletId, const TActorContext& ctx, bool pipeReconnected)
{
    if (tabletId == SchemeShardId) {
        if (!WaitingForACL) {
            return;
        }
        TActorId pipeClient = GetPipeClient(tabletId, ctx);
        NTabletPipe::SendData(ctx, pipeClient, new NSchemeShard::TEvSchemeShard::TEvDescribeScheme(tabletId, PathId));
    } else {
        TActorId pipeClient = GetPipeClient(tabletId, ctx);

        auto it = AggregatedStats.Cookies.find(tabletId);
        if (!pipeReconnected || it != AggregatedStats.Cookies.end()) {
            ui64 cookie;
            if (pipeReconnected) {
                cookie = it->second;
            } else {
                cookie = ++AggregatedStats.NextCookie;
                AggregatedStats.Cookies[tabletId] = cookie;
            }

            LOG_DEBUG(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                TStringBuilder() << "Send TEvPersQueue::TEvStatus TabletId: " << tabletId << " Cookie: " << cookie);
            NTabletPipe::SendData(ctx, pipeClient, new TEvPersQueue::TEvStatus("", true), cookie);
        }

        NTabletPipe::SendData(ctx, pipeClient, new TEvPQ::TEvSubDomainStatus(SubDomainOutOfSpace));
    }
}


void TPersQueueReadBalancer::Handle(TEvPersQueue::TEvStatusResponse::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;
    ui64 tabletId = record.GetTabletId();
    ui64 cookie = ev->Cookie;

    if ((0 != cookie && cookie != AggregatedStats.Cookies[tabletId]) || (0 == cookie && !AggregatedStats.Cookies.contains(tabletId))) {
        return;
    }

    AggregatedStats.Cookies.erase(tabletId);

    for (const auto& partRes : record.GetPartResult()) {
        ui32 partitionId = partRes.GetPartition();
        if (!PartitionsInfo.contains(partitionId)) {
            continue;
        }

        if (SplitMergeEnabled(TabletConfig) && PartitionsScaleManager) {
            PartitionsScaleManager->HandleScaleStatusChange(partitionId, partRes.GetScaleStatus(), ctx);
        }

        AggregatedStats.AggrStats(partitionId, partRes.GetPartitionSize(), partRes.GetUsedReserveSize());
        AggregatedStats.AggrStats(partRes.GetAvgWriteSpeedPerSec(), partRes.GetAvgWriteSpeedPerMin(),
            partRes.GetAvgWriteSpeedPerHour(), partRes.GetAvgWriteSpeedPerDay());
        AggregatedStats.Stats[partitionId].Counters = partRes.GetAggregatedCounters();
        AggregatedStats.Stats[partitionId].HasCounters = true;
    }

    Balancer->Handle(ev, ctx);

    if (AggregatedStats.Cookies.empty()) {
        CheckStat(ctx);
        Balancer->ProcessPendingStats(ctx);
    }
}

void TPersQueueReadBalancer::Handle(TEvPQ::TEvStatsWakeup::TPtr& ev, const TActorContext& ctx) {
    if (AggregatedStats.Round != ev->Get()->Round) {
        // old message
        return;
    }

    if (AggregatedStats.Cookies.empty()) {
        return;
    }

    CheckStat(ctx);
}

void TPersQueueReadBalancer::Handle(TEvPersQueue::TEvStatus::TPtr& ev, const TActorContext&) {
    Send(ev.Get()->Sender, GetStatsEvent());
}

void TPersQueueReadBalancer::TAggregatedStats::AggrStats(ui32 partition, ui64 dataSize, ui64 usedReserveSize) {
    Y_ABORT_UNLESS(dataSize >= usedReserveSize);

    auto& oldValue = Stats[partition];

    TPartitionStats newValue;
    newValue.DataSize = dataSize;
    newValue.UsedReserveSize = usedReserveSize;

    TotalDataSize += (newValue.DataSize - oldValue.DataSize);
    TotalUsedReserveSize += (newValue.UsedReserveSize - oldValue.UsedReserveSize);

    Y_ABORT_UNLESS(TotalDataSize >= TotalUsedReserveSize);

    oldValue = newValue;
}

void TPersQueueReadBalancer::TAggregatedStats::AggrStats(ui64 avgWriteSpeedPerSec, ui64 avgWriteSpeedPerMin, ui64 avgWriteSpeedPerHour, ui64 avgWriteSpeedPerDay) {
        NewMetrics.TotalAvgWriteSpeedPerSec += avgWriteSpeedPerSec;
        NewMetrics.MaxAvgWriteSpeedPerSec = Max<ui64>(NewMetrics.MaxAvgWriteSpeedPerSec, avgWriteSpeedPerSec);
        NewMetrics.TotalAvgWriteSpeedPerMin += avgWriteSpeedPerMin;
        NewMetrics.MaxAvgWriteSpeedPerMin = Max<ui64>(NewMetrics.MaxAvgWriteSpeedPerMin, avgWriteSpeedPerMin);
        NewMetrics.TotalAvgWriteSpeedPerHour += avgWriteSpeedPerHour;
        NewMetrics.MaxAvgWriteSpeedPerHour = Max<ui64>(NewMetrics.MaxAvgWriteSpeedPerHour, avgWriteSpeedPerHour);
        NewMetrics.TotalAvgWriteSpeedPerDay += avgWriteSpeedPerDay;
        NewMetrics.MaxAvgWriteSpeedPerDay = Max<ui64>(NewMetrics.MaxAvgWriteSpeedPerDay, avgWriteSpeedPerDay);
}

void TPersQueueReadBalancer::AnswerWaitingRequests(const TActorContext& ctx) {
    std::vector<TEvPersQueue::TEvCheckACL::TPtr> ww;
    ww.swap(WaitingACLRequests);
    for (auto& r : ww) {
        Handle(r, ctx);
    }

    std::vector<TEvPersQueue::TEvDescribe::TPtr> dr;
    dr.swap(WaitingDescribeRequests);
    for (auto& r : dr) {
        Handle(r, ctx);
    }
}

void TPersQueueReadBalancer::Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev, const TActorContext& ctx) {
    Y_UNUSED(ctx);
    if (!WaitingForACL) //ignore if already processed
        return;
    WaitingForACL = false;
    const auto& record = ev->Get()->GetRecord();
    if (record.GetStatus() == NKikimrScheme::EStatus::StatusSuccess) {
        ACL.Clear();
        Y_PROTOBUF_SUPPRESS_NODISCARD ACL.MutableACL()->ParseFromString(record.GetPathDescription().GetSelf().GetEffectiveACL());
        LastACLUpdate = ctx.Now();
        ctx.Schedule(TDuration::Seconds(AppData(ctx)->PQConfig.GetBalancerMetadataRetryTimeoutSec()), new TEvPersQueue::TEvUpdateACL());

        AnswerWaitingRequests(ctx);
    } else {
        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER, GetPrefix() << "couldn't receive ACL due to " << record.GetStatus());
        ctx.Schedule(ACL_ERROR_RETRY_TIMEOUT, new TEvPersQueue::TEvUpdateACL());
    }
}

void TPersQueueReadBalancer::CheckStat(const TActorContext& ctx) {
    Y_UNUSED(ctx);
    //TODO: Deside about changing number of partitions and send request to SchemeShard
    //TODO: make AlterTopic request via TX_PROXY

    if (!TTxWritePartitionStatsScheduled) {
        TTxWritePartitionStatsScheduled = true;
        Execute(new TTxWritePartitionStats(this));
    }

    AggregatedStats.Metrics = AggregatedStats.NewMetrics;

    TEvPersQueue::TEvPeriodicTopicStats* ev = GetStatsEvent();
    LOG_DEBUG(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
            TStringBuilder() << "Send TEvPeriodicTopicStats PathId: " << PathId
                             << " Generation: " << Generation
                             << " StatsReportRound: " << StatsReportRound
                             << " DataSize: " << AggregatedStats.TotalDataSize
                             << " UsedReserveSize: " << AggregatedStats.TotalUsedReserveSize);

    NTabletPipe::SendData(ctx, GetPipeClient(SchemeShardId, ctx), ev);

    UpdateCounters(ctx);
}

void TPersQueueReadBalancer::InitCounters(const TActorContext& ctx) {
    if (!DatabasePath) {
        return;
    }

    if (DynamicCounters) {
        return;
    }

    TStringBuf name = TStringBuf(Path);
    name.SkipPrefix(DatabasePath);
    name.SkipPrefix("/");

    bool isServerless = AppData(ctx)->FeatureFlags.GetEnableDbCounters(); //TODO: find out it via describe
    DynamicCounters = AppData(ctx)->Counters->GetSubgroup("counters", isServerless ? "topics_serverless" : "topics")
                ->GetSubgroup("host", "")
                ->GetSubgroup("database", DatabasePath)
                ->GetSubgroup("cloud_id", CloudId)
                ->GetSubgroup("folder_id", FolderId)
                ->GetSubgroup("database_id", DatabaseId)
                ->GetSubgroup("topic", TString(name));

    ActivePartitionCountCounter = DynamicCounters->GetExpiringNamedCounter("name", "topic.partition.active_count", false);
    InactivePartitionCountCounter = DynamicCounters->GetExpiringNamedCounter("name", "topic.partition.inactive_count", false);
}

void TPersQueueReadBalancer::UpdateConfigCounters() {
    if (!DynamicCounters) {
        return;
    }

    size_t inactiveCount = std::count_if(TabletConfig.GetAllPartitions().begin(), TabletConfig.GetAllPartitions().end(), [](auto& p) {
        return p.GetStatus() == NKikimrPQ::ETopicPartitionStatus::Inactive;
    });

    ActivePartitionCountCounter->Set(PartitionsInfo.size() - inactiveCount);
    InactivePartitionCountCounter->Set(inactiveCount);
}

void TPersQueueReadBalancer::UpdateCounters(const TActorContext& ctx) {
    if (!AggregatedStats.Stats.size())
        return;

    if (!DynamicCounters)
        return;

    using TPartitionLabeledCounters = TProtobufTabletLabeledCounters<EPartitionLabeledCounters_descriptor>;
    THolder<TPartitionLabeledCounters> labeledCounters;
    using TConsumerLabeledCounters = TProtobufTabletLabeledCounters<EClientLabeledCounters_descriptor>;
    THolder<TConsumerLabeledCounters> labeledConsumerCounters;

    labeledCounters.Reset(new TPartitionLabeledCounters("topic", 0, DatabasePath));
    labeledConsumerCounters.Reset(new TConsumerLabeledCounters("topic|x|consumer", 0, DatabasePath));

    if (AggregatedCounters.empty()) {
        for (ui32 i = 0; i < labeledCounters->GetCounters().Size(); ++i) {
            TString name = labeledCounters->GetNames()[i];
            TStringBuf nameBuf = name;
            nameBuf.SkipPrefix("PQ/");
            name = nameBuf;
            AggregatedCounters.push_back(name.empty() ? nullptr : DynamicCounters->GetExpiringNamedCounter("name", name, false));
        }
    }

    for (auto& [consumer, info]: Consumers) {
        info.Aggr.Reset(new TTabletLabeledCountersBase{});
        if (info.AggregatedCounters.empty()) {
            auto clientCounters = DynamicCounters->GetSubgroup("consumer", NPersQueue::ConvertOldConsumerName(consumer, ctx));
            for (ui32 i = 0; i < labeledConsumerCounters->GetCounters().Size(); ++i) {
                TString name = labeledConsumerCounters->GetNames()[i];
                TStringBuf nameBuf = name;
                nameBuf.SkipPrefix("PQ/");
                name = nameBuf;
                info.AggregatedCounters.push_back(name.empty() ? nullptr : clientCounters->GetExpiringNamedCounter("name", name, false));
            }
        }
    }

    /*** apply counters ****/

    ui64 milliSeconds = TAppData::TimeProvider->Now().MilliSeconds();

    THolder<TTabletLabeledCountersBase> aggr(new TTabletLabeledCountersBase);

    for (auto it = AggregatedStats.Stats.begin(); it != AggregatedStats.Stats.end(); ++it) {
        if (!it->second.HasCounters)
            continue;
        for (ui32 i = 0; i < it->second.Counters.ValuesSize() && i < labeledCounters->GetCounters().Size(); ++i) {
            labeledCounters->GetCounters()[i] = it->second.Counters.GetValues(i);
        }
        aggr->AggregateWith(*labeledCounters);

        for (const auto& consumerStats : it->second.Counters.GetConsumerAggregatedCounters()) {
            auto jt = Consumers.find(consumerStats.GetConsumer());
            if (jt == Consumers.end())
                continue;
            for (ui32 i = 0; i < consumerStats.ValuesSize() && i < labeledCounters->GetCounters().Size(); ++i) {
                labeledConsumerCounters->GetCounters()[i] = consumerStats.GetValues(i);
            }
            jt->second.Aggr->AggregateWith(*labeledConsumerCounters);
        }

    }

    /*** show counters ***/
    for (ui32 i = 0; aggr->HasCounters() && i < aggr->GetCounters().Size(); ++i) {
        if (!AggregatedCounters[i])
            continue;
        const auto& type = aggr->GetCounterType(i);
        auto val = aggr->GetCounters()[i].Get();
        if (type == TLabeledCounterOptions::CT_TIMELAG) {
            val = val <= milliSeconds ? milliSeconds - val : 0;
        }
        AggregatedCounters[i]->Set(val);
    }

    for (auto& [consumer, info] : Consumers) {
        for (ui32 i = 0; info.Aggr->HasCounters() && i < info.Aggr->GetCounters().Size(); ++i) {
            if (!info.AggregatedCounters[i])
                continue;
            const auto& type = info.Aggr->GetCounterType(i);
            auto val = info.Aggr->GetCounters()[i].Get();
            if (type == TLabeledCounterOptions::CT_TIMELAG) {
                val = val <= milliSeconds ? milliSeconds - val : 0;
            }
            info.AggregatedCounters[i]->Set(val);
        }
    }
}

TEvPersQueue::TEvPeriodicTopicStats* TPersQueueReadBalancer::GetStatsEvent() {
    TEvPersQueue::TEvPeriodicTopicStats* ev = new TEvPersQueue::TEvPeriodicTopicStats();
    auto& rec = ev->Record;
    rec.SetPathId(PathId);
    rec.SetGeneration(Generation);

    rec.SetRound(++StatsReportRound);
    rec.SetDataSize(AggregatedStats.TotalDataSize);
    rec.SetUsedReserveSize(AggregatedStats.TotalUsedReserveSize);
    rec.SetSubDomainOutOfSpace(SubDomainOutOfSpace);

    return ev;
}

void TPersQueueReadBalancer::GetStat(const TActorContext& ctx) {
    if (!AggregatedStats.Cookies.empty()) {
        AggregatedStats.Cookies.clear();
        CheckStat(ctx);
    }

    TPartitionMetrics newMetrics;
    AggregatedStats.NewMetrics = newMetrics;

    for (auto& p : PartitionsInfo) {
        AggregatedStats.Stats[p.first].HasCounters = false;

        const ui64& tabletId = p.second.TabletId;
        if (AggregatedStats.Cookies.contains(tabletId)) { //already asked stat
            continue;
        }
        RequestTabletIfNeeded(tabletId, ctx);
    }

    // TEvStatsWakeup must processed before next TEvWakeup, which send next status request to TPersQueue
    const auto& config = AppData(ctx)->PQConfig;
    auto wakeupInterval = std::max<ui64>(config.GetBalancerWakeupIntervalSec(), 1);
    auto stateWakeupInterval = std::max<ui64>(config.GetBalancerWakeupIntervalSec(), 1);
    ui64 delayMs = std::min(stateWakeupInterval * 1000, wakeupInterval * 500);
    if (0 < delayMs) {
        Schedule(TDuration::MilliSeconds(delayMs), new TEvPQ::TEvStatsWakeup(++AggregatedStats.Round));
    }
}

void TPersQueueReadBalancer::GetACL(const TActorContext& ctx) {
    if (WaitingForACL) // if there is request infly
        return;
    if (SchemeShardId == 0) {
        ctx.Schedule(ACL_SUCCESS_RETRY_TIMEOUT, new TEvPersQueue::TEvUpdateACL());
    } else {
        WaitingForACL = true;
        RequestTabletIfNeeded(SchemeShardId, ctx);
    }
}


void TPersQueueReadBalancer::HandleOnInit(TEvPersQueue::TEvGetPartitionsLocation::TPtr& ev, const TActorContext& ctx) {
    auto* evResponse = new TEvPersQueue::TEvGetPartitionsLocationResponse();
    evResponse->Record.SetStatus(false);
    ctx.Send(ev->Sender, evResponse);
}

void TPersQueueReadBalancer::Handle(TEvPersQueue::TEvGetPartitionsLocation::TPtr& ev, const TActorContext& ctx) {
    auto* evResponse = new TEvPersQueue::TEvGetPartitionsLocationResponse();
    const auto& request = ev->Get()->Record;
    auto addPartitionToResponse = [&](ui64 partitionId, ui64 tabletId) {
        auto* pResponse = evResponse->Record.AddLocations();
        pResponse->SetPartitionId(partitionId);
        if (PipesRequested.contains(tabletId)) {
            return false;
        }
        auto iter = TabletPipes.find(tabletId);
        if (iter == TabletPipes.end()) {
            GetPipeClient(tabletId, ctx);
            return false;
        }
        pResponse->SetNodeId(iter->second.NodeId.GetRef());
        pResponse->SetGeneration(iter->second.Generation.GetRef());

        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
            GetPrefix() << "addPartitionToResponse tabletId " << tabletId << ", partitionId " << partitionId
                        << ", NodeId " << pResponse->GetNodeId() << ", Generation " << pResponse->GetGeneration());
        return true;
    };
    auto sendResponse = [&](bool status) {
        evResponse->Record.SetStatus(status);
        ctx.Send(ev->Sender, evResponse);
    };
    bool ok = true;
    if (request.PartitionsSize() == 0) {
        if (!PipesRequested.empty() || TabletPipes.size() < TabletsInfo.size()) {
            // Do not have all pipes connected.
            return sendResponse(false);
        }
        for (const auto& [partitionId, partitionInfo] : PartitionsInfo) {
            ok = addPartitionToResponse(partitionId, partitionInfo.TabletId) && ok;
        }
    } else {
        for (const auto& partitionInRequest : request.GetPartitions()) {
            auto partitionInfoIter = PartitionsInfo.find(partitionInRequest);
            if (partitionInfoIter == PartitionsInfo.end()) {
                return sendResponse(false);
            }
            ok = addPartitionToResponse(partitionInRequest, partitionInfoIter->second.TabletId) && ok;
        }
    }
    return sendResponse(ok);
}




//
// Watching PQConfig
//

struct TTxWriteSubDomainPathId : public ITransaction {
    TPersQueueReadBalancer* const Self;

    TTxWriteSubDomainPathId(TPersQueueReadBalancer* self)
        : Self(self)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext&) {
        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::Data>().Key(1).Update(
            NIceDb::TUpdate<Schema::Data::SubDomainPathId>(Self->SubDomainPathId->LocalPathId));
        return true;
    }

    void Complete(const TActorContext&) {
    }
};

static constexpr TDuration MaxFindSubDomainPathIdDelay = TDuration::Minutes(1);

void TPersQueueReadBalancer::StopFindSubDomainPathId() {
    if (FindSubDomainPathIdActor) {
        Send(FindSubDomainPathIdActor, new TEvents::TEvPoison);
        FindSubDomainPathIdActor = { };
    }
}

void TPersQueueReadBalancer::StartFindSubDomainPathId(bool delayFirstRequest) {
    if (!FindSubDomainPathIdActor &&
        SchemeShardId != 0 &&
        (!SubDomainPathId || SubDomainPathId->OwnerId != SchemeShardId))
    {
        FindSubDomainPathIdActor = Register(CreateFindSubDomainPathIdActor(SelfId(), TabletID(), SchemeShardId, delayFirstRequest, MaxFindSubDomainPathIdDelay));
    }
}

void TPersQueueReadBalancer::Handle(NSchemeShard::TEvSchemeShard::TEvSubDomainPathIdFound::TPtr& ev, const TActorContext& ctx) {
    const auto* msg = ev->Get();

    if (FindSubDomainPathIdActor == ev->Sender) {
        FindSubDomainPathIdActor = { };
    }

    if (SchemeShardId == msg->SchemeShardId &&
       !SubDomainPathId || SubDomainPathId->OwnerId != msg->SchemeShardId)
    {
        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
            "Discovered subdomain " << msg->LocalPathId << " at RB " << TabletID());

        SubDomainPathId.emplace(msg->SchemeShardId, msg->LocalPathId);
        Execute(new TTxWriteSubDomainPathId(this), ctx);
        StartWatchingSubDomainPathId();
    }
}

void TPersQueueReadBalancer::StopWatchingSubDomainPathId() {
    if (WatchingSubDomainPathId) {
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchRemove());
        WatchingSubDomainPathId.reset();
    }
}

void TPersQueueReadBalancer::StartWatchingSubDomainPathId() {
    if (!SubDomainPathId || SubDomainPathId->OwnerId != SchemeShardId) {
        return;
    }

    if (WatchingSubDomainPathId && *WatchingSubDomainPathId != *SubDomainPathId) {
        StopWatchingSubDomainPathId();
    }

    if (!WatchingSubDomainPathId) {
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchPathId(*SubDomainPathId));
        WatchingSubDomainPathId = *SubDomainPathId;
    }
}

void TPersQueueReadBalancer::Handle(TEvTxProxySchemeCache::TEvWatchNotifyUpdated::TPtr& ev, const TActorContext& ctx) {
    const auto* msg = ev->Get();
    if (DatabasePath.empty()) {
        DatabasePath = msg->Result->GetPath();
        for (const auto& attr : msg->Result->GetPathDescription().GetUserAttributes()) {
            if (attr.GetKey() == "folder_id") FolderId = attr.GetValue();
            if (attr.GetKey() == "cloud_id") CloudId = attr.GetValue();
            if (attr.GetKey() == "database_id") DatabaseId = attr.GetValue();
        }

        InitCounters(ctx);
        UpdateConfigCounters();
    }

    if (PartitionsScaleManager) {
        PartitionsScaleManager->UpdateDatabasePath(DatabasePath);
    }

    if (SubDomainPathId && msg->PathId == *SubDomainPathId) {
        const bool outOfSpace = msg->Result->GetPathDescription()
            .GetDomainDescription()
            .GetDomainState()
            .GetDiskQuotaExceeded();

        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
            "Discovered subdomain " << msg->PathId << " state, outOfSpace = " << outOfSpace
            << " at RB " << TabletID());

        SubDomainOutOfSpace = outOfSpace;

        for (auto& p : PartitionsInfo) {
            const ui64& tabletId = p.second.TabletId;
            TActorId pipeClient = GetPipeClient(tabletId, ctx);
            NTabletPipe::SendData(ctx, pipeClient, new TEvPQ::TEvSubDomainStatus(outOfSpace));
        }
    }
}


//
// Balancing
//

void TPersQueueReadBalancer::Handle(TEvPQ::TEvReadingPartitionStatusRequest::TPtr& ev, const TActorContext& ctx) {
    Balancer->Handle(ev, ctx);
}

void TPersQueueReadBalancer::Handle(TEvPersQueue::TEvReadingPartitionStartedRequest::TPtr& ev, const TActorContext& ctx) {
    Balancer->Handle(ev, ctx);
}

void TPersQueueReadBalancer::Handle(TEvPersQueue::TEvReadingPartitionFinishedRequest::TPtr& ev, const TActorContext& ctx) {
    Balancer->Handle(ev, ctx);
}

void TPersQueueReadBalancer::Handle(TEvPersQueue::TEvPartitionReleased::TPtr& ev, const TActorContext& ctx) {
    Balancer->Handle(ev, ctx);
}

void TPersQueueReadBalancer::Handle(TEvPQ::TEvWakeupReleasePartition::TPtr &ev, const TActorContext& ctx) {
    Balancer->Handle(ev, ctx);
}

void TPersQueueReadBalancer::Handle(TEvPQ::TEvBalanceConsumer::TPtr& ev, const TActorContext& ctx) {
    Balancer->Handle(ev, ctx);
}

void TPersQueueReadBalancer::Handle(TEvTabletPipe::TEvServerConnected::TPtr& ev, const TActorContext& ctx)
{
    Balancer->Handle(ev, ctx);
}

void TPersQueueReadBalancer::Handle(TEvTabletPipe::TEvServerDisconnected::TPtr& ev, const TActorContext& ctx)
{
    Balancer->Handle(ev, ctx);
}

void TPersQueueReadBalancer::HandleOnInit(TEvPersQueue::TEvRegisterReadSession::TPtr& ev, const TActorContext&)
{
    RegisterEvents.push_back(ev->Release().Release());
}

void TPersQueueReadBalancer::Handle(TEvPersQueue::TEvRegisterReadSession::TPtr& ev, const TActorContext& ctx)
{
    Balancer->Handle(ev, ctx);
}

void TPersQueueReadBalancer::Handle(TEvPersQueue::TEvGetReadSessionsInfo::TPtr& ev, const TActorContext& ctx)
{
    Balancer->Handle(ev, ctx);
}


//
// Autoscaling
//

void TPersQueueReadBalancer::Handle(TEvPQ::TEvPartitionScaleStatusChanged::TPtr& ev, const TActorContext& ctx) {
    if (!SplitMergeEnabled(TabletConfig)) {
        return;
    }
    auto& record = ev->Get()->Record;
    auto* node = PartitionGraph.GetPartition(record.GetPartitionId());
    if (!node) {
        return;
    }

    if (PartitionsScaleManager) {
        PartitionsScaleManager->HandleScaleStatusChange(record.GetPartitionId(), record.GetScaleStatus(), ctx);
    }
}

void TPersQueueReadBalancer::Handle(TPartitionScaleRequest::TEvPartitionScaleRequestDone::TPtr& ev, const TActorContext& ctx) {
    if (!SplitMergeEnabled(TabletConfig)) {
        return;
    }
    if (PartitionsScaleManager) {
        PartitionsScaleManager->HandleScaleRequestResult(ev, ctx);
    }
}

} // NPQ
} // NKikimr
