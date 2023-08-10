#include "read_balancer.h"

#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/protos/counters_pq.pb.h>
#include <ydb/core/tablet/tablet_exception.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/string_utils/base64/base64.h>


namespace NKikimr {
namespace NPQ {


using namespace NTabletFlatExecutor;

static constexpr TDuration ACL_SUCCESS_RETRY_TIMEOUT = TDuration::Seconds(30);
static constexpr TDuration ACL_ERROR_RETRY_TIMEOUT = TDuration::Seconds(5);
static constexpr TDuration ACL_EXPIRATION_TIMEOUT = TDuration::Minutes(5);

bool TPersQueueReadBalancer::TTxPreInit::Execute(TTransactionContext& txc, const TActorContext& ctx) {
    Y_UNUSED(ctx);
    NIceDb::TNiceDb(txc.DB).Materialize<Schema>();
    return true;
}

void TPersQueueReadBalancer::TTxPreInit::Complete(const TActorContext& ctx) {
    Self->Execute(new TTxInit(Self), ctx);
}


bool TPersQueueReadBalancer::TTxInit::Execute(TTransactionContext& txc, const TActorContext& ctx) {
    try {
        Y_UNUSED(ctx); //read config
        NIceDb::TNiceDb db(txc.DB);

        auto dataRowset = db.Table<Schema::Data>().Range().Select();
        auto partsRowset = db.Table<Schema::Partitions>().Range().Select();
        auto groupsRowset = db.Table<Schema::Groups>().Range().Select();
        auto tabletsRowset = db.Table<Schema::Tablets>().Range().Select();

        if (!dataRowset.IsReady() || !partsRowset.IsReady() || !groupsRowset.IsReady() || !tabletsRowset.IsReady())
            return false;

        while (!dataRowset.EndOfSet()) { //found out topic info
            Y_VERIFY(!Self->Inited);
            Self->PathId  = dataRowset.GetValue<Schema::Data::PathId>();
            Self->Topic   = dataRowset.GetValue<Schema::Data::Topic>();
            Self->Path    = dataRowset.GetValue<Schema::Data::Path>();
            Self->Version = dataRowset.GetValue<Schema::Data::Version>();
            Self->MaxPartsPerTablet = dataRowset.GetValueOrDefault<Schema::Data::MaxPartsPerTablet>(0);
            Self->SchemeShardId = dataRowset.GetValueOrDefault<Schema::Data::SchemeShardId>(0);
            Self->NextPartitionId = dataRowset.GetValueOrDefault<Schema::Data::NextPartitionId>(0);

            ui64 subDomainPathId = dataRowset.GetValueOrDefault<Schema::Data::SubDomainPathId>(0);
            if (subDomainPathId) {
                Self->SubDomainPathId.emplace(Self->SchemeShardId, subDomainPathId);
            }

            TString config = dataRowset.GetValueOrDefault<Schema::Data::Config>("");
            if (!config.empty()) {
                bool res = Self->TabletConfig.ParseFromString(config);
                Y_VERIFY(res);
                Self->Consumers.clear();
                for (const auto& rr : Self->TabletConfig.GetReadRules()) {
                    Self->Consumers[rr];
                }
            }
            Self->Inited = true;
            if (!dataRowset.Next())
                return false;
        }

        while (!partsRowset.EndOfSet()) { //found out tablets for partitions
            ++Self->NumActiveParts;
            ui32 part = partsRowset.GetValue<Schema::Partitions::Partition>();
            ui64 tabletId = partsRowset.GetValue<Schema::Partitions::TabletId>();
            Self->PartitionsInfo[part] = {tabletId, EPartitionState::EPS_FREE, TActorId(), part + 1};
            Self->AggregatedStats.AggrStats(part, partsRowset.GetValue<Schema::Partitions::DataSize>(), 
                                            partsRowset.GetValue<Schema::Partitions::UsedReserveSize>());

            if (!partsRowset.Next())
                return false;
        }

        while (!groupsRowset.EndOfSet()) { //found out tablets for partitions
            ui32 groupId = groupsRowset.GetValue<Schema::Groups::GroupId>();
            ui32 partition = groupsRowset.GetValue<Schema::Groups::Partition>();
            Y_VERIFY(groupId > 0);
            auto jt = Self->PartitionsInfo.find(partition);
            Y_VERIFY(jt != Self->PartitionsInfo.end());
            jt->second.GroupId = groupId;

            Self->NoGroupsInBase = false;

            if (!groupsRowset.Next())
                return false;
        }

        Y_VERIFY(Self->ClientsInfo.empty());

        for (auto& p : Self->PartitionsInfo) {
            ui32 groupId = p.second.GroupId;
            Self->GroupsInfo[groupId].push_back(p.first);

        }
        Self->TotalGroups = Self->GroupsInfo.size();


        while (!tabletsRowset.EndOfSet()) { //found out tablets for partitions
            ui64 tabletId = tabletsRowset.GetValue<Schema::Tablets::TabletId>();
            TTabletInfo info;
            info.Owner = tabletsRowset.GetValue<Schema::Tablets::Owner>();
            info.Idx = tabletsRowset.GetValue<Schema::Tablets::Idx>();
            Self->MaxIdx = Max(Self->MaxIdx, info.Idx);

            Self->TabletsInfo[tabletId] = info;
            if (!tabletsRowset.Next())
                return false;
        }

        Self->Generation = txc.Generation;
    } catch (const TNotReadyTabletException&) {
        return false;
    } catch (...) {
       Y_FAIL("there must be no leaked exceptions");
    }
    return true;
}

void TPersQueueReadBalancer::TTxInit::Complete(const TActorContext& ctx) {
    Self->SignalTabletActive(ctx);
    if (Self->Inited)
        Self->InitDone(ctx);
}

bool TPersQueueReadBalancer::TTxWrite::Execute(TTransactionContext& txc, const TActorContext&) {
    NIceDb::TNiceDb db(txc.DB);
    TString config;
    bool res = Self->TabletConfig.SerializeToString(&config);
    Y_VERIFY(res);
    db.Table<Schema::Data>().Key(1).Update(
        NIceDb::TUpdate<Schema::Data::PathId>(Self->PathId),
        NIceDb::TUpdate<Schema::Data::Topic>(Self->Topic),
        NIceDb::TUpdate<Schema::Data::Path>(Self->Path),
        NIceDb::TUpdate<Schema::Data::Version>(Self->Version),
        NIceDb::TUpdate<Schema::Data::MaxPartsPerTablet>(Self->MaxPartsPerTablet),
        NIceDb::TUpdate<Schema::Data::SchemeShardId>(Self->SchemeShardId),
        NIceDb::TUpdate<Schema::Data::NextPartitionId>(Self->NextPartitionId),
        NIceDb::TUpdate<Schema::Data::Config>(config),
        NIceDb::TUpdate<Schema::Data::SubDomainPathId>(Self->SubDomainPathId ? Self->SubDomainPathId->LocalPathId : 0));
    for (auto& p : DeletedPartitions) {
        db.Table<Schema::Partitions>().Key(p).Delete();
    }
    for (auto& p : NewPartitions) {
        db.Table<Schema::Partitions>().Key(p.first).Update(
            NIceDb::TUpdate<Schema::Partitions::TabletId>(p.second.TabletId));
    }
    for (auto & p : NewGroups) {
        db.Table<Schema::Groups>().Key(p.first, p.second).Update();
    }
    for (auto& p : NewTablets) {
        db.Table<Schema::Tablets>().Key(p.first).Update(
            NIceDb::TUpdate<Schema::Tablets::Owner>(p.second.Owner),
            NIceDb::TUpdate<Schema::Tablets::Idx>(p.second.Idx));
    }
    for (auto& p : ReallocatedTablets) {
        db.Table<Schema::Tablets>().Key(p.first).Update(
            NIceDb::TUpdate<Schema::Tablets::Owner>(p.second.Owner),
            NIceDb::TUpdate<Schema::Tablets::Idx>(p.second.Idx));
    }
    return true;
}

void TPersQueueReadBalancer::TTxWrite::Complete(const TActorContext &ctx) {
    for (auto& actor : Self->WaitingResponse) {
        THolder<TEvPersQueue::TEvUpdateConfigResponse> res{new TEvPersQueue::TEvUpdateConfigResponse};
        res->Record.SetStatus(NKikimrPQ::OK);
        res->Record.SetTxId(Self->TxId);
        res->Record.SetOrigin(Self->TabletID());
        ctx.Send(actor, res.Release());
    }
    Self->WaitingResponse.clear();

    Self->NoGroupsInBase = false;

    Self->Inited = true;
    Self->InitDone(ctx);
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
                                        Y_VERIFY((session == nullptr) == (pp.second.State == EPS_FREE));
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
    auto it = jt->second.ClientGroupsInfo.find(ev->Get()->Group);
    if (it != jt->second.ClientGroupsInfo.end()) {
        it->second.WakeupScheduled = false;
        it->second.Balance(ctx);
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
    }
    WaitingResponse.push_back(ev->Sender);

    Version = record.GetVersion();
    MaxPartsPerTablet = record.GetPartitionPerTablet();
    PathId = record.GetPathId();
    Topic = record.GetTopicName();
    Path = record.GetPath();
    TxId = record.GetTxId();
    TabletConfig = record.GetTabletConfig();
    SchemeShardId = record.GetSchemeShardId();
    TotalGroups = record.HasTotalGroupCount() ? record.GetTotalGroupCount() : 0;
    ui32 prevNextPartitionId = NextPartitionId;
    NextPartitionId = record.HasNextPartitionId() ? record.GetNextPartitionId() : 0;
    THashMap<ui32, TPartitionInfo> partitionsInfo;
    if (record.HasSubDomainPathId()) {
        SubDomainPathId.emplace(record.GetSchemeShardId(), record.GetSubDomainPathId());
    }

    auto oldConsumers = std::move(Consumers);
    Consumers.clear();
    for (const auto& rr : TabletConfig.GetReadRules()) {
        auto it = oldConsumers.find(rr);
        if (it != oldConsumers.end()) {
            Consumers[rr] = std::move(it->second);
        } else {
            Consumers[rr];
        }
    }

    TVector<std::pair<ui32, TPartInfo>> newPartitions;
    TVector<ui32> deletedPartitions;
    TVector<std::pair<ui64, TTabletInfo>> newTablets;
    TVector<std::pair<ui32, ui32>> newGroups;
    TVector<std::pair<ui64, TTabletInfo>> reallocatedTablets;

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
        Y_VERIFY(group > 0);

        if (NoGroupsInBase) {
            Y_VERIFY(group <= TotalGroups || TotalGroups == 0);
            newGroups.push_back(std::make_pair(group, p.GetPartition()));
        }
        if (it == PartitionsInfo.end()) {
            Y_VERIFY(group <= TotalGroups && group > prevGroups || TotalGroups == 0);
            Y_VERIFY(p.GetPartition() >= prevNextPartitionId && p.GetPartition() < NextPartitionId || NextPartitionId == 0);
            partitionsInfo[p.GetPartition()] = {p.GetTabletId(), EPS_FREE, TActorId(), group};
            newPartitions.push_back(std::make_pair(p.GetPartition(), TPartInfo{p.GetTabletId(), group}));
            if (!NoGroupsInBase)
                newGroups.push_back(std::make_pair(group, p.GetPartition()));
            GroupsInfo[group].push_back(p.GetPartition());
            ++NumActiveParts;
        } else { //group is already defined
            Y_VERIFY(it->second.GroupId == group);
            partitionsInfo[p.GetPartition()] = it->second;
        }
    }

    if (TotalGroups == 0) {
        NextPartitionId = TotalGroups = GroupsInfo.size();
    }

    Y_VERIFY(GroupsInfo.size() == TotalGroups);

    for (auto& p : PartitionsInfo) {
        if (partitionsInfo.find(p.first) == partitionsInfo.end()) {
            Y_FAIL("deleting of partitions is not fully supported yet");
            deletedPartitions.push_back(p.first);
        }
    }
    PartitionsInfo = partitionsInfo;

    for (auto& p : ClientsInfo) {
        auto mainGroup = p.second.ClientGroupsInfo.find(TClientInfo::MAIN_GROUP);
        for (auto& part : newPartitions) {
            ui32 group = part.second.Group;
            auto it = p.second.SessionsWithGroup ? p.second.ClientGroupsInfo.find(group) : mainGroup;
            if (it == p.second.ClientGroupsInfo.end()) {
                Y_VERIFY(p.second.SessionsWithGroup);
                p.second.AddGroup(group);
                it = p.second.ClientGroupsInfo.find(group);
            }
            it->second.FreePartitions.push_back(part.first);
            it->second.PartitionsInfo[part.first] = {part.second.TabletId, EPS_FREE, TActorId(), group};
            it->second.ScheduleBalance(ctx);
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
    RequestTabletIfNeeded(tabletId, ctx);
}


void TPersQueueReadBalancer::Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx)
{
    auto tabletId = ev->Get()->TabletId;

    PipesRequested.erase(tabletId);

    if (ev->Get()->Status != NKikimrProto::OK) {
        ClosePipe(ev->Get()->TabletId, ctx);
        RequestTabletIfNeeded(ev->Get()->TabletId, ctx);

        LOG_ERROR_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER, GetPrefix() << "TEvClientConnected Status " << ev->Get()->Status << ", TabletId " << tabletId);
        return;
    }

    Y_VERIFY_DEBUG_S(ev->Get()->Generation, "Tablet generation should be greater than 0");

    auto it = TabletPipes.find(tabletId);
    if (!it.IsEnd()) {
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
        NTabletPipe::TClientConfig clientConfig;
        pipeClient = ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, tabletId, clientConfig));
        TabletPipes[tabletId].PipeActor = pipeClient;
        auto res = PipesRequested.insert(tabletId);
        Y_VERIFY(res.second);
    } else {
        pipeClient = it->second.PipeActor;
    }

    return pipeClient;
}

void TPersQueueReadBalancer::RequestTabletIfNeeded(const ui64 tabletId, const TActorContext& ctx)
{
    if ((tabletId == SchemeShardId && !WaitingForACL) ||
        (tabletId != SchemeShardId && AggregatedStats.Cookies.contains(tabletId))) {
        return;
    }

    TActorId pipeClient = GetPipeClient(tabletId, ctx);
    if (tabletId == SchemeShardId) {
        NTabletPipe::SendData(ctx, pipeClient, new NSchemeShard::TEvSchemeShard::TEvDescribeScheme(tabletId, PathId));
    } else {
        ui64 cookie = ++AggregatedStats.NextCookie;
        AggregatedStats.Cookies[tabletId] = cookie;
        NTabletPipe::SendData(ctx, pipeClient, new TEvPersQueue::TEvStatus(), cookie);
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

        if (!PartitionsInfo.contains(partRes.GetPartition())) {
            continue;
        }

        AggregatedStats.AggrStats(partRes.GetPartition(), partRes.GetPartitionSize(), partRes.GetUsedReserveSize());
        AggregatedStats.AggrStats(partRes.GetAvgWriteSpeedPerSec(), partRes.GetAvgWriteSpeedPerMin(), 
            partRes.GetAvgWriteSpeedPerHour(), partRes.GetAvgWriteSpeedPerDay());
        AggregatedStats.Stats[partRes.GetPartition()].Counters = partRes.GetAggregatedCounters();
        AggregatedStats.Stats[partRes.GetPartition()].HasCounters = true;
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
    Y_VERIFY(dataSize >= usedReserveSize);

    auto& oldValue = Stats[partition];

    TPartitionStats newValue;
    newValue.DataSize = dataSize;
    newValue.UsedReserveSize = usedReserveSize;

    TotalDataSize += (newValue.DataSize - oldValue.DataSize);
    TotalUsedReserveSize += (newValue.UsedReserveSize - oldValue.UsedReserveSize);

    Y_VERIFY(TotalDataSize >= TotalUsedReserveSize);

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
    TVector<TEvPersQueue::TEvCheckACL::TPtr> ww;
    ww.swap(WaitingACLRequests);
    for (auto& r : ww) {
        Handle(r, ctx);
    }

    TVector<TEvPersQueue::TEvDescribe::TPtr> dr;
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
    ui64 delayMs = std::min(config.GetBalancerStatsWakeupIntervalSec() * 1000, config.GetBalancerWakeupIntervalSec() * 500);
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
    auto it = PipesInfo.find(sender);
    if (it == PipesInfo.end()) {
        PipesInfo.insert({sender, {"", "", TActorId(), false, 1}});
    } else {
        it->second.ServerActors++;
    }
    LOG_INFO_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER, GetPrefix() << "pipe " << sender << " connected; active server actors: " << PipesInfo[sender].ServerActors);
}

TPersQueueReadBalancer::TClientGroupInfo& TPersQueueReadBalancer::TClientInfo::AddGroup(const ui32 group) {
    TClientGroupInfo& clientInfo = ClientGroupsInfo[group];
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

void TPersQueueReadBalancer::TClientInfo::FillEmptyGroup(const ui32 group, const THashMap<ui32, TPartitionInfo>& partitionsInfo) {
    auto& clientInfo = AddGroup(group);

    for (auto& p : partitionsInfo) {
        if (p.second.GroupId == group || group == 0) { //check group
            clientInfo.PartitionsInfo.insert(p);
            clientInfo.FreePartitions.push_back(p.first);
        }
    }
}

void TPersQueueReadBalancer::TClientInfo::AddSession(const ui32 group, const THashMap<ui32, TPartitionInfo>& partitionsInfo,
                                                    const TActorId& sender, const NKikimrPQ::TRegisterReadSession& record) {

    TActorId pipe = ActorIdFromProto(record.GetPipeClient());

    Y_VERIFY(pipe);

    if (ClientGroupsInfo.find(group) == ClientGroupsInfo.end()) {
        FillEmptyGroup(group, partitionsInfo);
    }

    auto it = ClientGroupsInfo.find(group);
    it->second.SessionsInfo.insert({
        it->second.SessionKey(pipe),
        TClientGroupInfo::TSessionInfo(
            record.GetSession(), sender,
            record.HasClientNode() ? record.GetClientNode() : "none",
            sender.NodeId(), TAppData::TimeProvider->Now()
        )
    });
}


void TPersQueueReadBalancer::HandleOnInit(TEvPersQueue::TEvRegisterReadSession::TPtr& ev, const TActorContext&)
{
    Y_FAIL("");
    RegisterEvents.push_back(ev->Release().Release());
}


void TPersQueueReadBalancer::Handle(TEvPersQueue::TEvRegisterReadSession::TPtr& ev, const TActorContext& ctx)
{
    const auto& record = ev->Get()->Record;

    TActorId pipe = ActorIdFromProto(record.GetPipeClient());
    LOG_NOTICE_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER, "client " << record.GetClientId() << " register session for pipe " << pipe << " session " << record.GetSession());

    Y_VERIFY(!record.GetSession().empty());
    Y_VERIFY(!record.GetClientId().empty());

    Y_VERIFY(pipe);

    //TODO: check here that pipe with clientPipe=sender is still connected

    auto jt = PipesInfo.find(pipe);
    if (jt == PipesInfo.end()) {
        LOG_CRIT_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER, GetPrefix() << "client " << record.GetClientId() << " pipe " << pipe
                        << " is not connected and got register session request for session " << record.GetSession());
        return;
    }

    TVector<ui32> groups;
    groups.reserve(record.GroupsSize());
    for (auto& group : record.GetGroups()) {
        groups.push_back(group);
        if (groups.back() == 0 || groups.back() > TotalGroups) {
            THolder<TEvPersQueue::TEvError> response(new TEvPersQueue::TEvError);
            response->Record.SetCode(NPersQueue::NErrorCode::BAD_REQUEST);
            response->Record.SetDescription(TStringBuilder() << "no group " << groups.back() << " in topic " << Topic);
            ctx.Send(ev->Sender, response.Release());
            return;
        }
    }

    jt->second = {record.GetClientId(), record.GetSession(), ev->Sender, !groups.empty(), jt->second.ServerActors};

    auto it = ClientsInfo.find(record.GetClientId());
    if (it == ClientsInfo.end()) {
        auto p = ClientsInfo.insert({record.GetClientId(), TClientInfo{}});
        Y_VERIFY(p.second);
        it = p.first;
        it->second.ClientId = record.GetClientId();
        it->second.Topic = Topic;
        it->second.TabletId = TabletID();
        it->second.Path = Path;
        it->second.Generation = Generation;
        it->second.Step = 0;
    }
    if (!groups.empty()) {
        ++it->second.SessionsWithGroup;
    }

    if (it->second.SessionsWithGroup > 0 && groups.empty()) {
        groups.reserve(TotalGroups);
        for (ui32 i = 1; i <= TotalGroups; ++i) {
            groups.push_back(i);
        }
    }

    if (!groups.empty()) {
        auto jt = it->second.ClientGroupsInfo.find(0);
        if (jt != it->second.ClientGroupsInfo.end()) {
            it->second.KillSessionsWithoutGroup(ctx);
        }
        for (auto g : groups) {
            it->second.AddSession(g, PartitionsInfo, ev->Sender, record);
        }
        for (ui32 group = 1; group <= TotalGroups; ++group) {
            if (it->second.ClientGroupsInfo.find(group) == it->second.ClientGroupsInfo.end()) {
                it->second.FillEmptyGroup(group, PartitionsInfo);
            }
        }
    } else {
        it->second.AddSession(0, PartitionsInfo, ev->Sender, record);
        Y_VERIFY(it->second.ClientGroupsInfo.size() == 1);
    }
    RegisterSession(pipe, ctx);
}


void TPersQueueReadBalancer::Handle(TEvPersQueue::TEvGetReadSessionsInfo::TPtr& ev, const TActorContext& ctx)
{
    const auto& record = ev->Get()->Record;
    auto it = ClientsInfo.find(record.GetClientId());
    THolder<TEvPersQueue::TEvReadSessionsInfoResponse> response(new TEvPersQueue::TEvReadSessionsInfoResponse());

    THashSet<ui32> partitionsRequested;
    for (auto p : record.GetPartitions()) {
        partitionsRequested.insert(p);
    }
    response->Record.SetTabletId(TabletID());

    if (it != ClientsInfo.end()) {
        for (auto& c : it->second.ClientGroupsInfo) {
            for (auto& p : c.second.PartitionsInfo) {
                if (partitionsRequested && !partitionsRequested.contains(p.first))
                    continue;
                auto pi = response->Record.AddPartitionInfo();
                pi->SetPartition(p.first);
                if (p.second.State == EPS_ACTIVE) {
                    auto* session = c.second.FindSession(p.second.Session);
                    Y_VERIFY(session != nullptr);
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


void TPersQueueReadBalancer::TClientInfo::KillSessionsWithoutGroup(const TActorContext& ctx) {
    auto it = ClientGroupsInfo.find(MAIN_GROUP);
    Y_VERIFY(it != ClientGroupsInfo.end());
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
    Y_VERIFY(ClientGroupsInfo.find(0) == ClientGroupsInfo.end());

    LOG_INFO_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER, GetPrefix() << "client " << ClientId << " merge groups");

    auto& clientInfo = AddGroup(MAIN_GROUP);

    ui32 numSessions = 0;
    ui32 numGroups = 0;

    for (auto it = ClientGroupsInfo.begin(); it != ClientGroupsInfo.end();) {
        auto jt = it++;
        if (jt->first == MAIN_GROUP) {
            continue;
        }
        ++numGroups;
        for (auto& pi : jt->second.PartitionsInfo) {
            bool res = clientInfo.PartitionsInfo.insert(pi).second;
            Y_VERIFY(res);
        }
        for (auto& si : jt->second.SessionsInfo) {
            auto key = si.first;
            key.second = clientInfo.SessionKeySalt;
            auto it = clientInfo.SessionsInfo.find(key);
            if (it == clientInfo.SessionsInfo.end()) {
                clientInfo.SessionsInfo.insert(std::make_pair(key, si.second)); //there must be all sessions in all groups
            } else {
                it->second.NumActive += si.second.NumActive;
                it->second.NumSuspended += si.second.NumSuspended;
            }
            ++numSessions;
        }
        for (auto& fp : jt->second.FreePartitions) {
            clientInfo.FreePartitions.push_back(fp);
        }
        ClientGroupsInfo.erase(jt);
    }
    Y_VERIFY(clientInfo.SessionsInfo.size() * numGroups == numSessions);
    Y_VERIFY(ClientGroupsInfo.size() == 1);
    clientInfo.ScheduleBalance(ctx);

}

void TPersQueueReadBalancer::Handle(TEvPersQueue::TEvPartitionReleased::TPtr& ev, const TActorContext& ctx)
{
    const auto& record = ev->Get()->Record;
    TActorId sender = ActorIdFromProto(record.GetPipeClient());
    const TString& clientId = record.GetClientId();

    auto pit = PartitionsInfo.find(record.GetPartition());
    if (pit == PartitionsInfo.end()) {
        LOG_CRIT_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER, GetPrefix() << "client " << record.GetClientId() << " pipe " << sender << " got deleted partition " << record);
        return;
    }

    ui32 group = pit->second.GroupId;
    Y_VERIFY(group > 0);

    LOG_INFO_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER, GetPrefix() << "client " << record.GetClientId() << " released partition from pipe " << sender
                                                << " session " << record.GetSession() << " partition " << record.GetPartition() << " group " << group);

    auto it = ClientsInfo.find(clientId);
    if (it == ClientsInfo.end()) {
        LOG_CRIT_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER, GetPrefix() << "client " << record.GetClientId() << " pipe " << sender
                            << " is not connected adn got release partitions request for session " << record.GetSession());
        return;
    }
    if (!it->second.SessionsWithGroup) {
        group = 0;
    }
    auto cit = it->second.ClientGroupsInfo.find(group);
    if (cit == it->second.ClientGroupsInfo.end()) {
        LOG_CRIT_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER, GetPrefix() << "client " << record.GetClientId() << " pipe " << sender
                            << " is not connected and got release partitions request for session " << record.GetSession());
        return;
    }

    auto jt = cit->second.PartitionsInfo.find(record.GetPartition());

    auto* session = cit->second.FindSession(sender);
    if (session == nullptr) { //already dead session
        return;
    }
    Y_VERIFY(jt != cit->second.PartitionsInfo.end());
    jt->second.Session = TActorId();
    jt->second.State = EPS_FREE;
    cit->second.FreePartitions.push_back(jt->first);

    --session->NumActive;
    --session->NumSuspended;

    cit->second.ScheduleBalance(ctx);
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
        if (iter.IsEnd()) {
            GetPipeClient(tabletId, ctx);
            return false;
        }
        pResponse->SetNodeId(iter->second.NodeId.GetRef());
        pResponse->SetGeneration(iter->second.Generation.GetRef());

        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER, GetPrefix() << "addPartitionToResponse tabletId " << tabletId << ", partitionId " << partitionId << ", NodeId " << pResponse->GetNodeId() << ", Generation " << pResponse->GetGeneration());
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
            if (partitionInfoIter.IsEnd()) {
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
    Y_VERIFY(it != PipesInfo.end());
    auto jt = ClientsInfo.find(it->second.ClientId);
    Y_VERIFY(jt != ClientsInfo.end());
    for (auto& c : jt->second.ClientGroupsInfo) {
        c.second.ScheduleBalance(ctx);
    }
}

void TPersQueueReadBalancer::UnregisterSession(const TActorId& pipe, const TActorContext& ctx)
{
    //TODO : change structs for only this session
    auto it = PipesInfo.find(pipe);
    Y_VERIFY(it != PipesInfo.end());
    const TString& clientId = it->second.ClientId;
    auto jt = ClientsInfo.find(clientId);
    Y_VERIFY(jt != ClientsInfo.end());
    TClientInfo& clientInfo = jt->second;
    for (auto& [groupKey, groupInfo] : clientInfo.ClientGroupsInfo) {
        for (auto& [partitionNumber, partitionInfo] : groupInfo.PartitionsInfo) { //TODO: reverse map
            if (partitionInfo.Session == pipe) {
                partitionInfo.Session = TActorId();
                partitionInfo.State = EPS_FREE;
                groupInfo.FreePartitions.push_back(partitionNumber);
            }
        }

        bool res = groupInfo.EraseSession(pipe);
        if (res)
            groupInfo.ScheduleBalance(ctx);
    }
    if (it->second.WithGroups && --clientInfo.SessionsWithGroup == 0) {
        clientInfo.MergeGroups(ctx);
    }

    PipesInfo.erase(pipe);
}


std::pair<TActorId, ui64> TPersQueueReadBalancer::TClientGroupInfo::SessionKey(const TActorId pipe) const {
    return std::make_pair(pipe, SessionKeySalt);
}

bool TPersQueueReadBalancer::TClientGroupInfo::EraseSession(const TActorId pipe) {
    return SessionsInfo.erase(SessionKey(pipe));
}

TPersQueueReadBalancer::TClientGroupInfo::TSessionInfo* TPersQueueReadBalancer::TClientGroupInfo::FindSession(const TActorId pipe) {
    auto it = SessionsInfo.find(SessionKey(pipe));
    if (it == SessionsInfo.end()) {
        return nullptr;
    }
    return &(it->second);
}

void TPersQueueReadBalancer::TClientGroupInfo::ScheduleBalance(const TActorContext& ctx) {
    if (WakeupScheduled)
        return;
    WakeupScheduled = true;
    ctx.Send(ctx.SelfID, new TEvPersQueue::TEvWakeupClient(ClientId, Group));
}


void TPersQueueReadBalancer::TClientGroupInfo::Balance(const TActorContext& ctx) {

    //TODO: use filled structs
    ui32 total = PartitionsInfo.size();
    ui32 sessionsCount = SessionsInfo.size();
    if (sessionsCount == 0)
        return; //no sessions, no problems

    //FreePartitions and PipeInfo[].NumActive are consistent
    ui32 desired = total / sessionsCount;

    ui32 allowPlusOne = total % sessionsCount;
    ui32 cur = allowPlusOne;
    //request partitions from sessions if needed
    for (auto& [sessionKey, sessionInfo] : SessionsInfo) {
        ui32 realDesired = (cur > 0) ? desired + 1 : desired;
        if (cur > 0)
            --cur;

        i64 canRequest = ((i64)sessionInfo.NumActive) - sessionInfo.NumSuspended - realDesired;
        if (canRequest > 0) {
            ReleasePartition(sessionKey.first, sessionInfo, Group, canRequest, ctx);
        }
    }

    //give free partitions to starving sessions
    if (FreePartitions.empty())
        return;

    cur = allowPlusOne;
    for (auto& [sessionKey, sessionInfo] : SessionsInfo) {
        ui32 realDesired = (cur > 0) ? desired + 1 : desired;
        if (cur > 0)
            --cur;

        if(sessionInfo.NumActive >= realDesired)
            continue;

        i64 req = ((i64)realDesired) - sessionInfo.NumActive;
        while (req > 0 && !FreePartitions.empty()) {
            --req;
            LockPartition(sessionKey.first, sessionInfo, FreePartitions.front(), ctx);
            FreePartitions.pop_front();
            if (FreePartitions.empty())
                return;
        }
        Y_VERIFY(sessionInfo.NumActive >= desired && sessionInfo.NumActive <= desired + 1);
    }
    Y_VERIFY(FreePartitions.empty());
}


void TPersQueueReadBalancer::TClientGroupInfo::LockPartition(const TActorId pipe, TSessionInfo& sessionInfo, ui32 partition, const TActorContext& ctx) {
    auto it = PartitionsInfo.find(partition);
    Y_VERIFY(it != PartitionsInfo.end());
    it->second.Session = pipe;
    it->second.State = EPS_ACTIVE;
    ++sessionInfo.NumActive;
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

void TPersQueueReadBalancer::TClientGroupInfo::ReleasePartition(const TActorId pipe, TSessionInfo& sessionInfo, const ui32 group, const ui32 count, const TActorContext& ctx) {
    sessionInfo.NumSuspended += count;

    THolder<TEvPersQueue::TEvReleasePartition> res{new TEvPersQueue::TEvReleasePartition};
    res->Record.SetSession(sessionInfo.Session);
    res->Record.SetTopic(Topic);
    res->Record.SetPath(Path);
    res->Record.SetGeneration(Generation);
    res->Record.SetClientId(ClientId);
    res->Record.SetCount(count);
    res->Record.SetGroup(group);
    ActorIdToProto(pipe, res->Record.MutablePipeClient());

    LOG_INFO_S(ctx, NKikimrServices::PERSQUEUE_READ_BALANCER, GetPrefix() << "client " << ClientId << " release partition group " << group
                                << " for pipe " << pipe << " session " << sessionInfo.Session);

    ctx.Send(sessionInfo.Sender, res.Release());
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
        db.Table<TPersQueueReadBalancer::Schema::Data>().Key(1).Update(
            NIceDb::TUpdate<TPersQueueReadBalancer::Schema::Data::SubDomainPathId>(Self->SubDomainPathId->LocalPathId));
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


} // NPQ
} // NKikimr
