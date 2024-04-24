#include "read_balancer.h"
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


static constexpr TDuration ACL_SUCCESS_RETRY_TIMEOUT = TDuration::Seconds(30);
static constexpr TDuration ACL_ERROR_RETRY_TIMEOUT = TDuration::Seconds(5);
static constexpr TDuration ACL_EXPIRATION_TIMEOUT = TDuration::Minutes(5);

NKikimrPQ::EConsumerScalingSupport DefaultScalingSupport() {
    // TODO fix me after support of paremeter ConsumerScalingSupport
    return AppData()->FeatureFlags.GetEnableTopicSplitMerge() ? NKikimrPQ::EConsumerScalingSupport::FULL_SUPPORT
                                                              : NKikimrPQ::EConsumerScalingSupport::NOT_SUPPORT;
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
        , NoGroupsInBase(true)
        , ResourceMetrics(nullptr)
        , WaitingForACL(false)
        , StatsReportRound(0)
    {
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
    for (auto& [_, clientInfo] : ClientsInfo) {
        for (auto& [_, groupInfo] : clientInfo.ClientGroupsInfo) {
            groupInfo.Balance(ctx);
        }
    }

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
    TStringStream str;
    HTML(str) {
        TAG(TH2) {str << "PersQueueReadBalancer Tablet";}
        TAG(TH3) {str << "Topic: " << Topic;}
        TAG(TH3) {str << "Generation: " << Generation;}
        TAG(TH3) {str << "Inited: " << Inited;}
        TAG(TH3) {str << "ActivePipes: " << PipesInfo.size();}
        if (Inited) {
            TAG(TH3) {str << "Active partitions: " << NumActiveParts;}
            TAG(TH3) {str << "[Total/Max/Avg]WriteSpeedSec: " << metrics.TotalAvgWriteSpeedPerSec << "/" << metrics.MaxAvgWriteSpeedPerSec << "/" << metrics.TotalAvgWriteSpeedPerSec / NumActiveParts;}
            TAG(TH3) {str << "[Total/Max/Avg]WriteSpeedMin: " << metrics.TotalAvgWriteSpeedPerMin << "/" << metrics.MaxAvgWriteSpeedPerMin << "/" << metrics.TotalAvgWriteSpeedPerMin / NumActiveParts;}
            TAG(TH3) {str << "[Total/Max/Avg]WriteSpeedHour: " << metrics.TotalAvgWriteSpeedPerHour << "/" << metrics.MaxAvgWriteSpeedPerHour << "/" << metrics.TotalAvgWriteSpeedPerHour / NumActiveParts;}
            TAG(TH3) {str << "[Total/Max/Avg]WriteSpeedDay: " << metrics.TotalAvgWriteSpeedPerDay << "/" << metrics.MaxAvgWriteSpeedPerDay << "/" << metrics.TotalAvgWriteSpeedPerDay / NumActiveParts;}
            TAG(TH3) {str << "TotalDataSize: " << AggregatedStats.TotalDataSize;}
            TAG(TH3) {str << "ReserveSize: " << PartitionReserveSize();}
            TAG(TH3) {str << "TotalUsedReserveSize: " << AggregatedStats.TotalUsedReserveSize;}
        }

        UL_CLASS("nav nav-tabs") {
            LI_CLASS("active") {
                str << "<a href=\"#main\" data-toggle=\"tab\">partitions</a>";
            }
            for (auto& pp : ClientsInfo) {
                LI() {
                    str << "<a href=\"#client_" << Base64Encode(pp.first) << "\" data-toggle=\"tab\">" << NPersQueue::ConvertOldConsumerName(pp.first) << "</a>";
                }
            }
        }
        DIV_CLASS("tab-content") {
            DIV_CLASS_ID("tab-pane fade in active", "main") {
                TABLE_SORTABLE_CLASS("table") {
                    TABLEHEAD() {
                        TABLER() {
                            TABLEH() {str << "partition";}
                            TABLEH() {str << "group";}
                            TABLEH() {str << "tabletId";}
                        }
                    }
                    TABLEBODY() {
                        for (auto& p : PartitionsInfo) {
                            TABLER() {
                                TABLED() { str << p.first;}
                                TABLED() { str << p.second.GroupId;}
                                TABLED() { str << p.second.TabletId;}
                            }
                        }
                    }
                }
            }
            for (auto& p : ClientsInfo) {
                DIV_CLASS_ID("tab-pane fade", "client_" + Base64Encode(p.first)) {
                    TABLE_SORTABLE_CLASS("table") {
                        TABLEHEAD() {
                            TABLER() {
                                TABLEH() {str << "partition";}
                                TABLEH() {str << "group";}
                                TABLEH() {str << "tabletId";}
                                TABLEH() {str << "state";}
                                TABLEH() {str << "session";}
                            }
                        }
                        TABLEBODY() {
                            for (auto& ci : p.second.ClientGroupsInfo) {
                                for (auto& pp : ci.second.PartitionsInfo) {
                                    TABLER() {
                                        TABLED() { str << pp.first;}
                                        TABLED() { str << ci.second.Group;}
                                        TABLED() { str << pp.second.TabletId;}
                                        TABLED() { str << (ui32)pp.second.State;}
                                        auto* session = ci.second.FindSession(pp.second.Session);
                                        Y_ABORT_UNLESS((session == nullptr) == (pp.second.State == EPS_FREE));
                                        TABLED() { str << (pp.second.State != EPS_FREE ? session->Session : "");}
                                    }
                                }
                            }
                        }
                    }

                    TABLE_SORTABLE_CLASS("table") {
                        TABLEHEAD() {
                            TABLER() {
                                TABLEH() {str << "session";}
                                TABLEH() {str << "group";}
                                TABLEH() {str << "suspended partitions";}
                                TABLEH() {str << "active partitions";}
                                TABLEH() {str << "total partitions";}
                            }
                        }
                        TABLEBODY() {

                            for (auto& ci : p.second.ClientGroupsInfo) {
                                for (auto& pp : ci.second.SessionsInfo) {
                                    TABLER() {
                                        TABLED() { str << pp.second.Session;}
                                        TABLED() { str << ci.second.Group;}
                                        TABLED() { str << pp.second.NumSuspended;}
                                        TABLED() { str << pp.second.NumActive - pp.second.NumSuspended;}
                                        TABLED() { str << (pp.second.NumActive);}
                                    }
                                }
                                TABLER() {
                                    TABLED() { str << "FREE";}
                                    TABLED() { str << ci.second.Group;}
                                    TABLED() { str << 0;}
                                    TABLED() { str << ci.second.FreePartitions.size();}
                                    TABLED() { str << ci.second.FreePartitions.size();}
                                }
                            }
                        }
                    }
                }
            }
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

void TPersQueueReadBalancer::Handle(TEvPersQueue::TEvWakeupClient::TPtr &ev, const TActorContext& ctx) {
    auto jt = ClientsInfo.find(ev->Get()->Client);
    if (jt == ClientsInfo.end())
        return;

    auto& clientInfo = jt->second;
    auto it = clientInfo.ClientGroupsInfo.find(ev->Get()->Group);
    if (it != clientInfo.ClientGroupsInfo.end()) {
        auto& groupInfo = it->second;
        groupInfo.WakeupScheduled = false;
        groupInfo.Balance(ctx);
    }
}

void TPersQueueReadBalancer::Handle(TEvPersQueue::TEvDescribe::TPtr &ev, const TActorContext& ctx) {
    if (ctx.Now() > LastACLUpdate + ACL_EXPIRATION_TIMEOUT || Topic.empty()) { //Topic.empty is only for tests
        WaitingDescribeRequests.push_back(ev);
        return;
    } else {
        THolder<TEvPersQueue::TEvDescribeResponse> res{new TEvPersQueue::TEvDescribeResponse};
        res->Record.MutableConfig()->CopyFrom(TabletConfig);
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
    Migrate(TabletConfig);

    SchemeShardId = record.GetSchemeShardId();
    TotalGroups = record.HasTotalGroupCount() ? record.GetTotalGroupCount() : 0;
    ui32 prevNextPartitionId = NextPartitionId;
    NextPartitionId = record.HasNextPartitionId() ? record.GetNextPartitionId() : 0;
    std::map<ui32, TPartitionInfo> partitionsInfo;
    if (record.HasSubDomainPathId()) {
        SubDomainPathId.emplace(record.GetSchemeShardId(), record.GetSubDomainPathId());
    }

    auto oldConsumers = std::move(Consumers);
    Consumers.clear();
    for (auto& consumer : TabletConfig.GetConsumers()) {
        auto scalingSupport = consumer.HasScalingSupport() ? consumer.GetScalingSupport() : DefaultScalingSupport();

        auto it = oldConsumers.find(consumer.GetName());
        if (it != oldConsumers.end()) {
            auto& c = Consumers[consumer.GetName()] = std::move(it->second);
            c.ScalingSupport = scalingSupport;
        } else {
            Consumers[consumer.GetName()].ScalingSupport = scalingSupport;
        }
    }

    PartitionGraph = MakePartitionGraph(record);

    std::vector<TPartInfo> newPartitions;
    std::vector<ui32> deletedPartitions;
    std::vector<std::pair<ui64, TTabletInfo>> newTablets;
    std::vector<std::pair<ui32, ui32>> newGroups;
    std::vector<std::pair<ui64, TTabletInfo>> reallocatedTablets;
    
    if (SplitMergeEnabled(TabletConfig)) {
        if (!PartitionsScaleManager) {
            PartitionsScaleManager = std::make_unique<TPartitionScaleManager>(Topic, DatabasePath, record);
        } else {
            PartitionsScaleManager->UpdateBalancerConfig(record);
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

    ui32 prevGroups = GroupsInfo.size();

    for (auto& p : record.GetPartitions()) {
        auto it = PartitionsInfo.find(p.GetPartition());
        ui32 group = p.HasGroup() ? p.GetGroup() : p.GetPartition() + 1;
        Y_ABORT_UNLESS(group > 0);

        if (NoGroupsInBase) {
            Y_ABORT_UNLESS(group <= TotalGroups || TotalGroups == 0);
            newGroups.push_back(std::make_pair(group, p.GetPartition()));
        }
        if (it == PartitionsInfo.end()) {
            Y_ABORT_UNLESS(group <= TotalGroups && group > prevGroups || TotalGroups == 0);
            Y_ABORT_UNLESS(p.GetPartition() >= prevNextPartitionId && p.GetPartition() < NextPartitionId || NextPartitionId == 0);
            partitionsInfo[p.GetPartition()] = {p.GetTabletId(), EPS_FREE, TActorId(), group, {}};
            if (SplitMergeEnabled(TabletConfig)) {
                partitionsInfo[p.GetPartition()].KeyRange.DeserializeFromProto(p.GetKeyRange());
            }

            newPartitions.push_back(TPartInfo{p.GetPartition(), p.GetTabletId(), group, partitionsInfo[p.GetPartition()].KeyRange});

            if (!NoGroupsInBase)
                newGroups.push_back(std::make_pair(group, p.GetPartition()));
            GroupsInfo[group].push_back(p.GetPartition());
            ++NumActiveParts;
        } else { //group is already defined
            Y_ABORT_UNLESS(it->second.GroupId == group);
            partitionsInfo[p.GetPartition()] = it->second;
        }
    }

    if (TotalGroups == 0) {
        NextPartitionId = TotalGroups = GroupsInfo.size();
    }

    Y_ABORT_UNLESS(GroupsInfo.size() == TotalGroups);

    for (auto& p : PartitionsInfo) {
        if (partitionsInfo.find(p.first) == partitionsInfo.end()) {
            Y_ABORT("deleting of partitions is not fully supported yet");
            deletedPartitions.push_back(p.first);
        }
    }
    PartitionsInfo = std::unordered_map<ui32, TPartitionInfo>(partitionsInfo.rbegin(), partitionsInfo.rend());

    for (auto& [_, clientInfo] : ClientsInfo) {
        auto mainGroup = clientInfo.ClientGroupsInfo.find(TClientInfo::MAIN_GROUP);
        for (auto& newPartition : newPartitions) {
            ui32 groupId = newPartition.Group;
            auto it = clientInfo.SessionsWithGroup ? clientInfo.ClientGroupsInfo.find(groupId) : mainGroup;
            if (it == clientInfo.ClientGroupsInfo.end()) {
                Y_ABORT_UNLESS(clientInfo.SessionsWithGroup);
                clientInfo.AddGroup(groupId);
                it = clientInfo.ClientGroupsInfo.find(groupId);
            }
            auto& group = it->second;
            group.FreePartition(newPartition.PartitionId);
            group.PartitionsInfo[newPartition.PartitionId] = {newPartition.TabletId, EPS_FREE, TActorId(), groupId, newPartition.KeyRange};
            group.ScheduleBalance(ctx);
        }
    }
    RebuildStructs();

    Execute(new TTxWrite(this, std::move(deletedPartitions), std::move(newPartitions), std::move(newTablets), std::move(newGroups), std::move(reallocatedTablets)), ctx);

    if (SubDomainPathId && (!WatchingSubDomainPathId || *WatchingSubDomainPathId != *SubDomainPathId)) {
        StartWatchingSubDomainPathId();
    }
}


TStringBuilder TPersQueueReadBalancer::GetPrefix() const {
    return TStringBuilder() << "tablet " << TabletID() << " topic " << Topic << " ";
}

TStringBuilder TPersQueueReadBalancer::TClientGroupInfo::GetPrefix() const {
    return TStringBuilder() << "tablet " << TabletId << " topic " << Topic << " ";
}

TStringBuilder TPersQueueReadBalancer::TClientInfo::GetPrefix() const {
    return TStringBuilder() << "tablet " << TabletId << " topic " << Topic << " ";
}

void TPersQueueReadBalancer::Handle(TEvTabletPipe::TEvServerDisconnected::TPtr& ev, const TActorContext& ctx)
{
    auto it = PipesInfo.find(ev->Get()->ClientId);

    LOG_INFO_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER, GetPrefix() << "pipe " << ev->Get()->ClientId << " disconnected; active server actors: "
                        << (it != PipesInfo.end() ? it->second.ServerActors : -1));

    if (it != PipesInfo.end()) {
        if (--(it->second.ServerActors) > 0)
            return;
        if (!it->second.Session.empty()) {
            LOG_NOTICE_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER, GetPrefix() << "pipe " << ev->Get()->ClientId << " client " << it->second.ClientId << " disconnected session " << it->second.Session);
            UnregisterSession(it->first, ctx);
        } else {
            LOG_INFO_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER, GetPrefix() << "pipe " << ev->Get()->ClientId << " disconnected no session");
            PipesInfo.erase(it);
        }
    }
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
        auto generation = partRes.GetGeneration();
        auto cookie = partRes.GetCookie();
        for (const auto& consumer : partRes.GetConsumerResult()) {
            if (consumer.GetReadingFinished()) {
                auto it = ClientsInfo.find(consumer.GetConsumer());
                if (it != ClientsInfo.end()) {
                    auto& clientInfo = it->second;
                    if (clientInfo.IsReadeable(partitionId) && clientInfo.SetCommittedState(partitionId, generation, cookie)) {
                        clientInfo.ProccessReadingFinished(partRes.GetPartition(), ctx);
                    }
                }
            }
        }

        if (!PartitionsInfo.contains(partRes.GetPartition())) {
            continue;
        }
        if (SplitMergeEnabled(TabletConfig) && PartitionsScaleManager) {
            TPartitionScaleManager::TPartitionInfo scalePartitionInfo = {
                .Id = partitionId,
                .KeyRange = PartitionsInfo[partRes.GetPartition()].KeyRange
            };
            PartitionsScaleManager->HandleScaleStatusChange(scalePartitionInfo, partRes.GetScaleStatus(), ctx);
        }
        partRes.GetScaleStatus();

        AggregatedStats.AggrStats(partitionId, partRes.GetPartitionSize(), partRes.GetUsedReserveSize());
        AggregatedStats.AggrStats(partRes.GetAvgWriteSpeedPerSec(), partRes.GetAvgWriteSpeedPerMin(), 
            partRes.GetAvgWriteSpeedPerHour(), partRes.GetAvgWriteSpeedPerDay());
        AggregatedStats.Stats[partitionId].Counters = partRes.GetAggregatedCounters();
        AggregatedStats.Stats[partitionId].HasCounters = true;
    }

    if (AggregatedStats.Cookies.empty()) {
        CheckStat(ctx);
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

void TPersQueueReadBalancer::UpdateCounters(const TActorContext& ctx) {
    if (!AggregatedStats.Stats.size())
        return;

    if (!DatabasePath)
        return;

    using TPartitionLabeledCounters = TProtobufTabletLabeledCounters<EPartitionLabeledCounters_descriptor>;
    THolder<TPartitionLabeledCounters> labeledCounters;
    using TConsumerLabeledCounters = TProtobufTabletLabeledCounters<EClientLabeledCounters_descriptor>;
    THolder<TConsumerLabeledCounters> labeledConsumerCounters;


    labeledCounters.Reset(new TPartitionLabeledCounters("topic", 0, DatabasePath));
    labeledConsumerCounters.Reset(new TConsumerLabeledCounters("topic|x|consumer", 0, DatabasePath));

    auto counters = AppData(ctx)->Counters;
    bool isServerless = AppData(ctx)->FeatureFlags.GetEnableDbCounters(); //TODO: find out it via describe

    TStringBuf name = TStringBuf(Path);
    name.SkipPrefix(DatabasePath);
    name.SkipPrefix("/");
    counters = counters->GetSubgroup("counters", isServerless ? "topics_serverless" : "topics")
                ->GetSubgroup("host", "")
                ->GetSubgroup("database", DatabasePath)
                ->GetSubgroup("cloud_id", CloudId)
                ->GetSubgroup("folder_id", FolderId)
                ->GetSubgroup("database_id", DatabaseId)
                ->GetSubgroup("topic", TString(name));

    if (AggregatedCounters.empty()) {
        for (ui32 i = 0; i < labeledCounters->GetCounters().Size(); ++i) {
            TString name = labeledCounters->GetNames()[i];
            TStringBuf nameBuf = name;
            nameBuf.SkipPrefix("PQ/");
            name = nameBuf;
            AggregatedCounters.push_back(name.empty() ? nullptr : counters->GetExpiringNamedCounter("name", name, false));
        }
    }

    for (auto& [consumer, info]: Consumers) {
        info.Aggr.Reset(new TTabletLabeledCountersBase{});
        if (info.AggregatedCounters.empty()) {
            auto clientCounters = counters->GetSubgroup("consumer", NPersQueue::ConvertOldConsumerName(consumer, ctx));
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

void TPersQueueReadBalancer::Handle(TEvTabletPipe::TEvServerConnected::TPtr& ev, const TActorContext& ctx)
{
    const TActorId& sender = ev->Get()->ClientId;
    auto& pipe = PipesInfo[sender];
    ++pipe.ServerActors;

    LOG_INFO_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
            GetPrefix() << "pipe " << sender << " connected; active server actors: " << pipe.ServerActors);
}

TPersQueueReadBalancer::TClientGroupInfo& TPersQueueReadBalancer::TClientInfo::AddGroup(const ui32 group) {
    auto r = ClientGroupsInfo.insert({group, TClientGroupInfo{ *this }});

    TClientGroupInfo& clientInfo = r.first->second;
    clientInfo.Group = group;
    clientInfo.ClientId = ClientId;
    clientInfo.Topic = Topic;
    clientInfo.TabletId = TabletId;
    clientInfo.Path = Path;
    clientInfo.Generation = Generation;
    clientInfo.Step = &Step;

    clientInfo.SessionKeySalt = TAppData::RandomProvider->GenRand64();
    return clientInfo;
}

void TPersQueueReadBalancer::TClientGroupInfo::ActivatePartition(ui32 partitionId) {
    auto* session = FindSession(partitionId);
    if (session) {
        --session->NumInactive;
    }
}

void TPersQueueReadBalancer::TClientGroupInfo::InactivatePartition(ui32 partitionId) {
    auto* session = FindSession(partitionId);
    if (session) {
        ++session->NumInactive;
    }
}

void TPersQueueReadBalancer::TClientGroupInfo::FreePartition(ui32 partitionId) {
    if (Group != TClientInfo::MAIN_GROUP || ClientInfo.IsReadeable(partitionId)) {
        FreePartitions.push_back(partitionId);
    }
}

void TPersQueueReadBalancer::TClientInfo::FillEmptyGroup(const ui32 group, const std::unordered_map<ui32, TPartitionInfo>& partitionsInfo) {
    auto& groupInfo = AddGroup(group);

    for (auto& [partitionId, partitionInfo] : partitionsInfo) {
        if (partitionInfo.GroupId == group || group == MAIN_GROUP) { //check group
            groupInfo.PartitionsInfo.insert({partitionId, partitionInfo});
            groupInfo.FreePartition(partitionId);
        }
    }
}

void TPersQueueReadBalancer::TClientInfo::AddSession(const ui32 groupId, const std::unordered_map<ui32, TPartitionInfo>& partitionsInfo,
                                                    const TActorId& sender, const NKikimrPQ::TRegisterReadSession& record) {

    TActorId pipe = ActorIdFromProto(record.GetPipeClient());

    Y_ABORT_UNLESS(pipe);

    if (ClientGroupsInfo.find(groupId) == ClientGroupsInfo.end()) {
        FillEmptyGroup(groupId, partitionsInfo);
    }

    auto it = ClientGroupsInfo.find(groupId);
    auto& group = it->second;
    group.SessionsInfo.insert({
        group.SessionKey(pipe),
        TSessionInfo(
            record.GetSession(),
            sender,
            record.HasClientNode() ? record.GetClientNode() : "none",
            sender.NodeId(),
            TAppData::TimeProvider->Now()
        )
    });
}

TPersQueueReadBalancer::TReadingPartitionStatus& TPersQueueReadBalancer::TClientInfo::GetPartitionReadingStatus(ui32 partitionId) {
    return ReadingPartitionStatus[partitionId];
}

bool TPersQueueReadBalancer::TClientInfo::IsReadeable(ui32 partitionId) const {
    if (!ScalingSupport()) {
        return true;
    }

    auto* node = Balancer.PartitionGraph.GetPartition(partitionId);
    if (!node) {
        return false;
    }

    if (ReadingPartitionStatus.empty()) {
        return node->Parents.empty();
    }

    for(auto* parent : node->HierarhicalParents) {
        if (!IsFinished(parent->Id)) {
            return false;
        }
    }

    return true;
}

bool TPersQueueReadBalancer::TClientInfo::IsFinished(ui32 partitionId) const {
    auto it = ReadingPartitionStatus.find(partitionId);
    if (it == ReadingPartitionStatus.end()) {
        return false;
    }
    return it->second.IsFinished();
}

bool TPersQueueReadBalancer::TClientInfo::SetCommittedState(ui32 partitionId, ui32 generation, ui64 cookie) {
    return ReadingPartitionStatus[partitionId].SetCommittedState(generation, cookie);
}

TPersQueueReadBalancer::TClientGroupInfo* TPersQueueReadBalancer::TClientInfo::FindGroup(ui32 partitionId) {
    auto it = ClientGroupsInfo.find(partitionId + 1);
    if (it != ClientGroupsInfo.end()) {
        return &it->second;
    }

    it = ClientGroupsInfo.find(MAIN_GROUP);
    if (it == ClientGroupsInfo.end()) {
        return nullptr;
    }

    auto& group = it->second;
    if (group.PartitionsInfo.contains(partitionId)) {
        return &group;
    }

    return nullptr;
}

bool TPersQueueReadBalancer::TClientInfo::ProccessReadingFinished(ui32 partitionId, const TActorContext& ctx) {
    if (!ScalingSupport()) {
        return false;
    }

    auto* groupInfo = FindGroup(partitionId);
    if (!groupInfo) {
        return false; // TODO is it correct?
    }
    groupInfo->InactivatePartition(partitionId);

    bool hasChanges = false;

    Balancer.PartitionGraph.Travers(partitionId, [&](ui32 id) {
        if (IsReadeable(id)) {
            auto* groupInfo = FindGroup(id);
            if (!groupInfo) {
                return false; // TODO is it correct?
            }
            auto it = groupInfo->PartitionsInfo.find(id);
            if (it == groupInfo->PartitionsInfo.end()) {
                return false; // TODO is it correct?
            }
            auto& partitionInfo = it->second;

            if (partitionInfo.State == EPS_FREE) {
                groupInfo->FreePartitions.push_back(id);
                groupInfo->ScheduleBalance(ctx);
                hasChanges = true;
            }
            return true;
        }
        return false;
    });

    return hasChanges;
}

void TPersQueueReadBalancer::HandleOnInit(TEvPersQueue::TEvRegisterReadSession::TPtr& ev, const TActorContext&)
{
    Y_ABORT(""); // TODO why?
    RegisterEvents.push_back(ev->Release().Release());
}


void TPersQueueReadBalancer::Handle(TEvPersQueue::TEvRegisterReadSession::TPtr& ev, const TActorContext& ctx)
{
    const auto& record = ev->Get()->Record;

    TActorId pipe = ActorIdFromProto(record.GetPipeClient());
    LOG_NOTICE_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
            "client " << record.GetClientId() << " register session for pipe " << pipe << " session " << record.GetSession());

    Y_ABORT_UNLESS(!record.GetSession().empty());
    Y_ABORT_UNLESS(!record.GetClientId().empty());

    Y_ABORT_UNLESS(pipe);

    //TODO: check here that pipe with clientPipe=sender is still connected

    auto jt = PipesInfo.find(pipe);
    if (jt == PipesInfo.end()) {
        LOG_CRIT_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                GetPrefix() << "client " << record.GetClientId() << " pipe " << pipe
                        << " is not connected and got register session request for session " << record.GetSession());
        return;
    }

    std::vector<ui32> groups;
    groups.reserve(record.GroupsSize());
    for (auto& group : record.GetGroups()) {
        if (group == 0 || group > TotalGroups) {
            THolder<TEvPersQueue::TEvError> response(new TEvPersQueue::TEvError);
            response->Record.SetCode(NPersQueue::NErrorCode::BAD_REQUEST);
            response->Record.SetDescription(TStringBuilder() << "no group " << group << " in topic " << Topic);
            ctx.Send(ev->Sender, response.Release());
            return;
        }
        groups.push_back(group);
    }

    auto& pipeInfo = jt->second;
    pipeInfo.Init(record.GetClientId(), record.GetSession(), ev->Sender, groups);

    auto cit = Consumers.find(record.GetClientId());
    NKikimrPQ::EConsumerScalingSupport scalingSupport = cit == Consumers.end() ? DefaultScalingSupport() : cit->second.ScalingSupport;

    auto it = ClientsInfo.find(record.GetClientId());
    if (it == ClientsInfo.end()) {
        auto p = ClientsInfo.insert({record.GetClientId(), TClientInfo{ *this,  scalingSupport }});
        Y_ABORT_UNLESS(p.second);
        it = p.first;
        it->second.ClientId = record.GetClientId();
        it->second.Topic = Topic;
        it->second.TabletId = TabletID();
        it->second.Path = Path;
        it->second.Generation = Generation;
        it->second.Step = 0;
    }

    auto& clientInfo = it->second;
    if (!groups.empty()) {
        ++clientInfo.SessionsWithGroup;
    }

    if (clientInfo.SessionsWithGroup > 0 && groups.empty()) {
        groups.reserve(TotalGroups);
        for (ui32 i = 1; i <= TotalGroups; ++i) {
            groups.push_back(i);
        }
    }

    if (!groups.empty()) {
        auto jt = clientInfo.ClientGroupsInfo.find(0);
        if (jt != clientInfo.ClientGroupsInfo.end()) {
            clientInfo.KillSessionsWithoutGroup(ctx);
        }
        for (auto g : groups) {
            clientInfo.AddSession(g, PartitionsInfo, ev->Sender, record);
        }
        for (ui32 group = 1; group <= TotalGroups; ++group) {
            if (clientInfo.ClientGroupsInfo.find(group) == clientInfo.ClientGroupsInfo.end()) {
                clientInfo.FillEmptyGroup(group, PartitionsInfo);
            }
        }
    } else {
        clientInfo.AddSession(0, PartitionsInfo, ev->Sender, record);
        Y_ABORT_UNLESS(clientInfo.ClientGroupsInfo.size() == 1);
    }

    RegisterSession(pipe, ctx);
}


void TPersQueueReadBalancer::Handle(TEvPersQueue::TEvGetReadSessionsInfo::TPtr& ev, const TActorContext& ctx)
{
    const auto& record = ev->Get()->Record;
    auto it = ClientsInfo.find(record.GetClientId());
    THolder<TEvPersQueue::TEvReadSessionsInfoResponse> response(new TEvPersQueue::TEvReadSessionsInfoResponse());

    std::unordered_set<ui32> partitionsRequested;
    for (auto p : record.GetPartitions()) {
        partitionsRequested.insert(p);
    }
    response->Record.SetTabletId(TabletID());

    if (it != ClientsInfo.end()) {
        for (auto& c : it->second.ClientGroupsInfo) {
            for (auto& p : c.second.PartitionsInfo) {
                if (!partitionsRequested.empty() && !partitionsRequested.contains(p.first)) {
                    continue;
                }
                auto pi = response->Record.AddPartitionInfo();
                pi->SetPartition(p.first);
                if (p.second.State == EPS_ACTIVE) {
                    auto* session = c.second.FindSession(p.second.Session);
                    Y_ABORT_UNLESS(session != nullptr);
                    pi->SetClientNode(session->ClientNode);
                    pi->SetProxyNodeId(session->ProxyNodeId);
                    pi->SetSession(session->Session);
                    pi->SetTimestamp(session->Timestamp.Seconds());
                    pi->SetTimestampMs(session->Timestamp.MilliSeconds());
                } else {
                    pi->SetClientNode("");
                    pi->SetProxyNodeId(0);
                    pi->SetSession("");
                    pi->SetTimestamp(0);
                    pi->SetTimestampMs(0);
                }
            }
            for (auto& s : c.second.SessionsInfo) {
                auto si = response->Record.AddReadSessions();
                si->SetSession(s.second.Session);

                ActorIdToProto(s.second.Sender, si->MutableSessionActor());
            }
        }
    }
    ctx.Send(ev->Sender, response.Release());
}


bool TPersQueueReadBalancer::TClientInfo::ScalingSupport() const {
    return NKikimrPQ::EConsumerScalingSupport::FULL_SUPPORT == ScalingSupport_;
}

void TPersQueueReadBalancer::TClientInfo::KillSessionsWithoutGroup(const TActorContext& ctx) {
    auto it = ClientGroupsInfo.find(MAIN_GROUP);
    Y_ABORT_UNLESS(it != ClientGroupsInfo.end());
    for (auto& s : it->second.SessionsInfo) {
        THolder<TEvPersQueue::TEvError> response(new TEvPersQueue::TEvError);
        response->Record.SetCode(NPersQueue::NErrorCode::ERROR);
        response->Record.SetDescription(TStringBuilder() << "there are new sessions with group, old session without group will be killed - recreate it, please");
        ctx.Send(s.second.Sender, response.Release());
        LOG_NOTICE_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER, GetPrefix() <<"client " << ClientId << " kill session pipe " << s.first.first << " session " << s.second.Session);
    }
    ClientGroupsInfo.erase(it);
}

void TPersQueueReadBalancer::TClientInfo::MergeGroups(const TActorContext& ctx) {
    Y_ABORT_UNLESS(ClientGroupsInfo.find(0) == ClientGroupsInfo.end());

    LOG_INFO_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER, GetPrefix() << "client " << ClientId << " merge groups");

    auto& mainGroupInfo = AddGroup(MAIN_GROUP);

    ui32 numSessions = 0;
    ui32 numGroups = 0;

    for (auto it = ClientGroupsInfo.begin(); it != ClientGroupsInfo.end();) {
        auto jt = it++;
        if (jt->first == MAIN_GROUP) {
            continue;
        }
        ++numGroups;

        auto& groupInfo = jt->second;
        for (auto& pi : groupInfo.PartitionsInfo) {
            bool res = mainGroupInfo.PartitionsInfo.insert(pi).second;
            Y_ABORT_UNLESS(res);
        }

        for (auto& si : groupInfo.SessionsInfo) {
            auto key = si.first;
            key.second = mainGroupInfo.SessionKeySalt;
            auto it = mainGroupInfo.SessionsInfo.find(key);
            if (it == mainGroupInfo.SessionsInfo.end()) {
                mainGroupInfo.SessionsInfo.insert(std::make_pair(key, si.second)); //there must be all sessions in all groups
            } else {
                auto& session = it->second;
                session.NumActive += si.second.NumActive;
                session.NumSuspended += si.second.NumSuspended;
                session.NumInactive += si.second.NumInactive;
            }
            ++numSessions;
        }

        for (auto& fp : groupInfo.FreePartitions) {
            mainGroupInfo.FreePartition(fp);
        }

        ClientGroupsInfo.erase(jt);
    }
    Y_ABORT_UNLESS(mainGroupInfo.SessionsInfo.size() * numGroups == numSessions);
    Y_ABORT_UNLESS(ClientGroupsInfo.size() == 1);
    mainGroupInfo.ScheduleBalance(ctx);

}

void TPersQueueReadBalancer::Handle(TEvPersQueue::TEvPartitionReleased::TPtr& ev, const TActorContext& ctx)
{
    const auto& record = ev->Get()->Record;
    auto partitionId = record.GetPartition();
    TActorId sender = ActorIdFromProto(record.GetPipeClient());
    const TString& clientId = record.GetClientId();

    auto pit = PartitionsInfo.find(partitionId);
    if (pit == PartitionsInfo.end()) {
        LOG_CRIT_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER, GetPrefix() << "client " << record.GetClientId() << " pipe " << sender << " got deleted partition " << record);
        return;
    }

    ui32 group = pit->second.GroupId;
    Y_ABORT_UNLESS(group > 0);

    LOG_INFO_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER, GetPrefix() << "client " << record.GetClientId() << " released partition from pipe " << sender
                                                << " session " << record.GetSession() << " partition " << partitionId << " group " << group);

    auto it = ClientsInfo.find(clientId);
    if (it == ClientsInfo.end()) {
        LOG_CRIT_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER, GetPrefix() << "client " << record.GetClientId() << " pipe " << sender
                            << " is not connected adn got release partitions request for session " << record.GetSession());
        return;
    }

    auto& clientInfo = it->second;
    if (!clientInfo.SessionsWithGroup) {
        group = TClientInfo::MAIN_GROUP;
    }
    auto cit = clientInfo.ClientGroupsInfo.find(group);
    if (cit == clientInfo.ClientGroupsInfo.end()) {
        LOG_CRIT_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER, GetPrefix() << "client " << record.GetClientId() << " pipe " << sender
                            << " is not connected and got release partitions request for session " << record.GetSession());
        return;
    }

    auto& clientGroupsInfo = cit->second;
    auto jt = clientGroupsInfo.PartitionsInfo.find(partitionId);

    auto* session = clientGroupsInfo.FindSession(sender);
    if (session == nullptr) { //already dead session
        return;
    }
    Y_ABORT_UNLESS(jt != clientGroupsInfo.PartitionsInfo.end());
    auto& partitionInfo = jt->second;
    partitionInfo.Unlock();

    clientGroupsInfo.FreePartition(partitionId);

    session->Unlock(!clientInfo.IsReadeable(partitionId)); // TODO тут точно должно быть IsReadable без условия что прочитана?
    clientInfo.UnlockPartition(partitionId, ctx);

    clientGroupsInfo.ScheduleBalance(ctx);
}

void TPersQueueReadBalancer::TClientInfo::UnlockPartition(ui32 partitionId, const TActorContext& ctx) {
    if (GetPartitionReadingStatus(partitionId).StopReading()) {
        // Release all children partitions if required

        auto* n = Balancer.PartitionGraph.GetPartition(partitionId);
        if (!n) {
            return;
        }

        std::deque<TPartitionGraph::Node*> queue;
        queue.insert(queue.end(), n->Children.begin(), n->Children.end());

        while (!queue.empty()) {
            auto* node = queue.front();
            queue.pop_front();
            queue.insert(queue.end(), node->Children.begin(), node->Children.end());

            auto* group = FindGroup(node->Id);
            if (!group) {
                continue;
            }
            group->ReleasePartition(node->Id, ctx);
        }
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


void TPersQueueReadBalancer::RebuildStructs() {
    //TODO : bug here in case of deleting number of partitions
    //TODO : track session with smallest and biggest number of (active but not suspended partitions
}

void TPersQueueReadBalancer::RegisterSession(const TActorId& pipe, const TActorContext& ctx)
{
    //TODO : change structs for only this session, not all client
    auto it = PipesInfo.find(pipe);
    Y_ABORT_UNLESS(it != PipesInfo.end());
    auto jt = ClientsInfo.find(it->second.ClientId);
    Y_ABORT_UNLESS(jt != ClientsInfo.end());
    for (auto& c : jt->second.ClientGroupsInfo) {
        c.second.ScheduleBalance(ctx);
    }
}

void TPersQueueReadBalancer::UnregisterSession(const TActorId& pipe, const TActorContext& ctx)
{
    //TODO : change structs for only this session
    auto it = PipesInfo.find(pipe);
    Y_ABORT_UNLESS(it != PipesInfo.end());
    auto& pipeInfo = it->second;

    auto jt = ClientsInfo.find(pipeInfo.ClientId);
    Y_ABORT_UNLESS(jt != ClientsInfo.end());
    TClientInfo& clientInfo = jt->second;

    for (auto& [groupKey, groupInfo] : clientInfo.ClientGroupsInfo) {
        for (auto& [partitionId, partitionInfo] : groupInfo.PartitionsInfo) { //TODO: reverse map
            if (partitionInfo.Session == pipe) {
                partitionInfo.Unlock();
                groupInfo.FreePartition(partitionId);
            }
        }

        if (groupInfo.EraseSession(pipe)) {
            groupInfo.ScheduleBalance(ctx);
        }
    }
    if (pipeInfo.WithGroups() && --clientInfo.SessionsWithGroup == 0) {
        clientInfo.MergeGroups(ctx);
    }

    PipesInfo.erase(it);
}


std::pair<TActorId, ui64> TPersQueueReadBalancer::TClientGroupInfo::SessionKey(const TActorId pipe) const {
    return std::make_pair(pipe, SessionKeySalt);
}

bool TPersQueueReadBalancer::TClientGroupInfo::EraseSession(const TActorId pipe) {
    return SessionsInfo.erase(SessionKey(pipe));
}

TPersQueueReadBalancer::TSessionInfo* TPersQueueReadBalancer::TClientGroupInfo::FindSession(const TActorId pipe) {
    auto it = SessionsInfo.find(SessionKey(pipe));
    if (it == SessionsInfo.end()) {
        return nullptr;
    }
    return &(it->second);
}

TPersQueueReadBalancer::TSessionInfo* TPersQueueReadBalancer::TClientGroupInfo::FindSession(ui32 partitionId) {
    auto partitionIt = PartitionsInfo.find(partitionId);
    if (partitionIt != PartitionsInfo.end()) {
        auto& partitionInfo = partitionIt->second;
        if (partitionInfo.Session) {
            return FindSession(partitionInfo.Session);
        }
    }

    return nullptr;
}

void TPersQueueReadBalancer::TClientGroupInfo::ScheduleBalance(const TActorContext& ctx) {
    if (WakeupScheduled) {
        return;
    }
    WakeupScheduled = true;
    ctx.Send(ctx.SelfID, new TEvPersQueue::TEvWakeupClient(ClientId, Group));
}

std::tuple<ui32, ui32, ui32> TPersQueueReadBalancer::TClientGroupInfo::TotalPartitions() const {
    ui32 totalActive = 0;
    ui32 totalInactive = 0;
    ui32 totalUnreadable = 0;

    if (ClientInfo.ReadingPartitionStatus.empty()) {
        totalActive = FreePartitions.size();
    } else {
        for (auto p : FreePartitions) {
            if (ClientInfo.IsReadeable(p)) {
                if (ClientInfo.IsFinished(p)) {
                    ++totalInactive;
                } else {
                    ++totalActive;
                }
            } else {
                ++totalUnreadable;
            }
        }
    }
    for(auto& [_, session] : SessionsInfo) {
        totalActive += session.NumActive - session.NumInactive;
        totalInactive += session.NumInactive;
    }

    return {totalActive, totalInactive, totalUnreadable};
}

void TPersQueueReadBalancer::TClientGroupInfo::ReleaseExtraPartitions(ui32 desired, ui32 allowPlusOne, const TActorContext& ctx) {
    // request partitions from sessions if needed
    for (auto& [sessionKey, sessionInfo] : SessionsInfo) {
        ui32 realDesired = (allowPlusOne > 0) ? desired + 1 : desired;
        if (allowPlusOne > 0) {
            --allowPlusOne;
        }

        i64 canRequest = ((i64)sessionInfo.NumActive) - sessionInfo.NumInactive - sessionInfo.NumSuspended - realDesired;
        if (canRequest > 0) {
            ReleasePartition(sessionKey.first, sessionInfo, canRequest, ctx);
        }
    }
}

void TPersQueueReadBalancer::TClientGroupInfo::LockMissingPartitions(
            ui32 desired,
            ui32 allowPlusOne,
            const std::function<bool (ui32 partitionId)> partitionPredicate,
            const std::function<ssize_t (const TSessionInfo& sessionInfo)> actualExtractor,
            const TActorContext& ctx) {

    std::deque<ui32> freePartitions = std::move(FreePartitions);
    std::deque<ui32> toOtherPartitions;

    for (auto& [sessionKey, sessionInfo] : SessionsInfo) {
        auto& pipe = sessionKey.first;

        ui32 realDesired = (allowPlusOne > 0) ? desired + 1 : desired;
        if (allowPlusOne > 0) {
            --allowPlusOne;
        }

        ssize_t actual = actualExtractor(sessionInfo);
        if (actual >= realDesired) {
            continue;
        }

        i64 req = ((i64)realDesired) - actual;
        while (req > 0 && !freePartitions.empty()) {
            auto partitionId = freePartitions.front();
            if (partitionPredicate(partitionId)) {
                auto& status = ClientInfo.GetPartitionReadingStatus(partitionId);
                if (status.BalanceToOtherPipe() && status.LastPipe != pipe || SessionsInfo.size() == 1) {
                    --req;
                    LockPartition(pipe, sessionInfo, partitionId, ctx);
                } else {
                    toOtherPartitions.push_back(partitionId);
                }
            } else {
                FreePartitions.push_back(partitionId);
            }
            freePartitions.pop_front();
        }

        if (!freePartitions.empty()) {
            Y_ABORT_UNLESS(actualExtractor(sessionInfo) >= desired && actualExtractor(sessionInfo) <= desired + 1);
        }
    }

    if (!toOtherPartitions.empty()) {
        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                GetPrefix() << "client: "<< ClientId << " balance group " << Group << " partitions " << JoinRange(", ", toOtherPartitions.begin(), toOtherPartitions.end()) << " to other sessions");

        for (auto& [sessionKey, sessionInfo] : SessionsInfo) {
            auto& pipe = sessionKey.first;
            ui32 realDesired = desired + 1;

            ssize_t actual = actualExtractor(sessionInfo);
            if (actual >= realDesired) {
                continue;
            }

            ssize_t req = ((ssize_t)realDesired) - actual;
            size_t possibleIterations = toOtherPartitions.size();
            while (req > 0 && !toOtherPartitions.empty() && possibleIterations) {
                auto partitionId = toOtherPartitions.front();
                toOtherPartitions.pop_front();

                auto& status = ClientInfo.GetPartitionReadingStatus(partitionId);
                if (status.LastPipe != pipe) {
                    --req;
                    --possibleIterations;
                    LockPartition(pipe, sessionInfo, partitionId, ctx);
                } else {
                    --possibleIterations;
                    toOtherPartitions.push_back(partitionId);
                }
            }
        }
    }

    FreePartitions.insert(FreePartitions.end(), freePartitions.begin(), freePartitions.end());
}

void TPersQueueReadBalancer::TClientGroupInfo::Balance(const TActorContext& ctx) {
    ui32 sessionsCount = SessionsInfo.size();

    if (!sessionsCount) {
        return;
    }

    auto [totalActive, totalInactive, totalUnreadable] = TotalPartitions();

    LOG_INFO_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER, GetPrefix() << "client " << ClientId << " balance group " << Group << ": "
                            << " TotalActive=" << totalActive << ", TotalInactive=" << totalInactive << ", TotalUnreadable=" << totalUnreadable);


    //FreePartitions and PipeInfo[].NumActive are consistent
    ui32 desiredActive = totalActive / sessionsCount;
    ui32 allowPlusOne = totalActive % sessionsCount;
    ui32 desiredInactive = totalInactive / sessionsCount + 1;

    ReleaseExtraPartitions(desiredActive, allowPlusOne, ctx);

    //give free partitions to starving sessions
    if (FreePartitions.empty()) {
        return;
    }

    LockMissingPartitions(desiredActive, allowPlusOne,
        [&](ui32 partitionId) { return !ClientInfo.IsFinished(partitionId) && ClientInfo.IsReadeable(partitionId); },
        [](const TSessionInfo& sessionInfo) {return ((ssize_t)sessionInfo.NumActive) - sessionInfo.NumInactive; },
        ctx);

    LockMissingPartitions(desiredInactive, 0,
        [&](ui32 partitionId) { return ClientInfo.IsFinished(partitionId) && ClientInfo.IsReadeable(partitionId); },
        [](const TSessionInfo& sessionInfo) {return (ssize_t)sessionInfo.NumInactive; },
        ctx);

    Y_ABORT_UNLESS(FreePartitions.size() == totalUnreadable);
    FreePartitions.clear();
}

void TPersQueueReadBalancer::TClientGroupInfo::LockPartition(const TActorId pipe, TSessionInfo& sessionInfo, ui32 partition, const TActorContext& ctx) {
    auto it = PartitionsInfo.find(partition);
    Y_ABORT_UNLESS(it != PartitionsInfo.end());

    auto& partitionInfo = it->second;
    partitionInfo.Lock(pipe);

    ++sessionInfo.NumActive;
    if (ClientInfo.IsFinished(partition)) {
        ++sessionInfo.NumInactive;
    }
    //TODO:rebuild structs

    THolder<TEvPersQueue::TEvLockPartition> res{new TEvPersQueue::TEvLockPartition};
    res->Record.SetSession(sessionInfo.Session);
    res->Record.SetPartition(partition);
    res->Record.SetTopic(Topic);
    res->Record.SetPath(Path);
    res->Record.SetGeneration(Generation);
    res->Record.SetStep(++(*Step));
    res->Record.SetClientId(ClientId);
    ActorIdToProto(pipe, res->Record.MutablePipeClient());
    res->Record.SetTabletId(PartitionsInfo[partition].TabletId);

    LOG_INFO_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER, GetPrefix() << "client " << ClientId << " lock partition for pipe "
                            << pipe << " session " << sessionInfo.Session << " partition " << partition << " generation " << Generation << " step " << *Step);

    ctx.Send(sessionInfo.Sender, res.Release());
}

THolder<TEvPersQueue::TEvReleasePartition> TPersQueueReadBalancer::TClientGroupInfo::MakeEvReleasePartition(
                                                                const TActorId pipe,
                                                                const TSessionInfo& sessionInfo,
                                                                const ui32 count,
                                                                const std::set<ui32>& partitions) {
    THolder<TEvPersQueue::TEvReleasePartition> res{new TEvPersQueue::TEvReleasePartition};
    auto& r = res->Record;

    r.SetSession(sessionInfo.Session);
    r.SetTopic(Topic);
    r.SetPath(Path);
    r.SetGeneration(Generation);
    if (count) {
        r.SetCount(count);
    }
    for (auto& p : partitions) {
        r.AddPartition(p);
    }
    r.SetClientId(ClientId);
    r.SetGroup(Group);
    ActorIdToProto(pipe, r.MutablePipeClient());

    return res;
}

void TPersQueueReadBalancer::TClientGroupInfo::ReleasePartition(const ui32 partitionId, const TActorContext& ctx) {
    auto it = PartitionsInfo.find(partitionId);
    if (it == PartitionsInfo.end()) {
        // TODO inconsistent status?
        return;
    }

    auto& partitionInfo = it->second;

    if (partitionInfo.Session) {
        auto* session = FindSession(partitionInfo.Session);
        if (session) {
            ReleasePartition(partitionInfo.Session, *session, std::set{partitionId}, ctx);
        }
    }
}

void TPersQueueReadBalancer::TClientGroupInfo::ReleasePartition(const TActorId pipe, TSessionInfo& sessionInfo, const ui32 count, const TActorContext& ctx) {
    sessionInfo.NumSuspended += count;

    LOG_INFO_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER, GetPrefix() << "client " << ClientId << " release partition group " << Group
                                << " for pipe " << pipe << " session " << sessionInfo.Session << " count " << count);

    ctx.Send(sessionInfo.Sender, MakeEvReleasePartition(pipe, sessionInfo, count, {}).Release());
}

void TPersQueueReadBalancer::TClientGroupInfo::ReleasePartition(const TActorId pipe, TSessionInfo& sessionInfo, const std::set<ui32>& partitions, const TActorContext& ctx) {
    sessionInfo.NumSuspended += partitions.size();

    LOG_INFO_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER, GetPrefix() << "client " << ClientId << " release partition group " << Group
                                << " for pipe " << pipe << " session " << sessionInfo.Session);

    ctx.Send(sessionInfo.Sender, MakeEvReleasePartition(pipe, sessionInfo, 0, partitions).Release());
}


static constexpr TDuration MaxFindSubDomainPathIdDelay = TDuration::Minutes(1);


void TPersQueueReadBalancer::StopFindSubDomainPathId() {
    if (FindSubDomainPathIdActor) {
        Send(FindSubDomainPathIdActor, new TEvents::TEvPoison);
        FindSubDomainPathIdActor = { };
    }
}


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

void TPersQueueReadBalancer::Handle(TEvPQ::TEvReadingPartitionStatusRequest::TPtr& ev, const TActorContext& ctx) {
    auto& r = ev->Get()->Record;
    auto partitionId = r.GetPartitionId();

    auto it = ClientsInfo.find(r.GetConsumer());
    if (it != ClientsInfo.end()) {
        auto& clientInfo = it->second;

        if (!clientInfo.IsReadeable(partitionId)) {
            LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                        "The offset of the partition " << partitionId << " was commited by " << r.GetConsumer()
                        << " but the partition isn't readable");
            return;
        }

        if (clientInfo.SetCommittedState(partitionId, r.GetGeneration(), r.GetCookie())) {
            LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                "The offset of the partition " << partitionId << " was commited by " << r.GetConsumer());

            clientInfo.ProccessReadingFinished(partitionId, ctx);
        }
    }
}

void TPersQueueReadBalancer::Handle(TEvPersQueue::TEvReadingPartitionStartedRequest::TPtr& ev, const TActorContext& ctx) {
    auto& r = ev->Get()->Record;
    auto partitionId = r.GetPartitionId();

    auto it = ClientsInfo.find(r.GetConsumer());
    if (it == ClientsInfo.end()) {
        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                "Received TEvReadingPartitionStartedRequest from unknown consumer " << r.GetConsumer());
    }

    auto& clientInfo = it->second;
    auto& status = clientInfo.GetPartitionReadingStatus(partitionId);

    if (status.StartReading()) {
        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                "Reading of the partition " << partitionId << " was started by " << r.GetConsumer() << ". We stop reading from child partitions.");

        auto* groupInfo = clientInfo.FindGroup(partitionId);
        if (groupInfo) {
            groupInfo->ActivatePartition(partitionId);
        }

        // We releasing all children's partitions because we don't start reading the partition from EndOffset
        PartitionGraph.Travers(partitionId, [&](ui32 partitionId) {
            auto& status = clientInfo.GetPartitionReadingStatus(partitionId);
            auto* group = clientInfo.FindGroup(partitionId);

            if (group) {
                if (status.Reset()) {
                    group->ActivatePartition(partitionId);
                }
                group->ReleasePartition(partitionId, ctx);
            }

            return true;
        });
    } else {
        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                "Reading of the partition " << partitionId << " was started by " << r.GetConsumer() << ".");

    }
}

TString GetSdkDebugString(bool scaleAwareSDK) {
    return scaleAwareSDK ? "ScaleAwareSDK" : "old SDK";
}

void TPersQueueReadBalancer::Handle(TEvPersQueue::TEvReadingPartitionFinishedRequest::TPtr& ev, const TActorContext& ctx) {
    auto& r = ev->Get()->Record;
    auto partitionId = r.GetPartitionId();

    auto it = ClientsInfo.find(r.GetConsumer());
    if (it == ClientsInfo.end()) {
        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                "Received TEvReadingPartitionFinishedRequest from unknown consumer " << r.GetConsumer());
    }

    auto& clientInfo = it->second;
    auto& status = clientInfo.GetPartitionReadingStatus(partitionId);

    if (!clientInfo.IsReadeable(partitionId)) {
        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                    "Reading of the partition " << partitionId << " was finished by " << r.GetConsumer()
                    << " but the partition isn't readable");
        return;
    }

    if (status.SetFinishedState(r.GetScaleAwareSDK(), r.GetStartedReadingFromEndOffset())) {
        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                    "Reading of the partition " << partitionId << " was finished by " << r.GetConsumer()
                    << ", firstMessage=" << r.GetStartedReadingFromEndOffset() << ", " << GetSdkDebugString(r.GetScaleAwareSDK()));

        clientInfo.ProccessReadingFinished(partitionId, ctx);
    } else if (!status.IsFinished()) {
        auto delay = std::min<size_t>(1ul << status.Iteration, TabletConfig.GetPartitionConfig().GetLifetimeSeconds()); // TODO Учесть время закрытия партиции на запись

        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER,
                    "Reading of the partition " << partitionId << " was finished by " << r.GetConsumer()
                    << ". Scheduled release of the partition for re-reading. Delay=" << delay << " seconds,"
                    << " firstMessage=" << r.GetStartedReadingFromEndOffset() << ", " << GetSdkDebugString(r.GetScaleAwareSDK()));

        status.LastPipe = ev->Sender;
        ctx.Schedule(TDuration::Seconds(delay), new TEvPQ::TEvWakeupReleasePartition(r.GetConsumer(), partitionId, status.Cookie));
    }
}

void TPersQueueReadBalancer::Handle(TEvPQ::TEvWakeupReleasePartition::TPtr &ev, const TActorContext& ctx) {
    auto* msg = ev->Get();
    auto it = ClientsInfo.find(msg->Consumer);
    if (it == ClientsInfo.end()) {
        return;
    }

    auto& clientInfo = it->second;
    auto& readingStatus = clientInfo.GetPartitionReadingStatus(msg->PartitionId);
    if (readingStatus.Cookie != msg->Cookie) {
        return;
    }

    auto* group = clientInfo.FindGroup(msg->PartitionId);
    if (!group) {
        // TODO inconsistent status? must be filtered by cookie?
        return;
    }

    group->ReleasePartition(msg->PartitionId, ctx);
}

void TPersQueueReadBalancer::Handle(TEvPQ::TEvPartitionScaleStatusChanged::TPtr& ev, const TActorContext& ctx) {
    if (!SplitMergeEnabled(TabletConfig)) {
        return;
    }
    auto& record = ev->Get()->Record;
    auto partitionInfoIt = PartitionsInfo.find(record.GetPartitionId());
    if (partitionInfoIt == PartitionsInfo.end()) {
        return;
    }

    if (PartitionsScaleManager) {
        TPartitionScaleManager::TPartitionInfo scalePartitionInfo = {
            .Id = record.GetPartitionId(),
            .KeyRange = partitionInfoIt->second.KeyRange
        };
        PartitionsScaleManager->HandleScaleStatusChange(scalePartitionInfo, record.GetScaleStatus(), ctx);
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
