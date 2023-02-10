#include "actors.h"
#include "info_collector.h"
#include "test_load_actor.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/load_test/events.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/public/lib/base/msgbus.h>

#include <google/protobuf/text_format.h>
#include <library/cpp/monlib/service/pages/templates.h>

// DataShard load is associated with full path to table: only one load per path can be started.
//
// TLoad is used to prepare database (drop, create, configure, etc) and then start load.

namespace NKikimr::NDataShardLoad {

namespace {

struct TFinishedTestInfo {
    ui64 Tag;
    TString ErrorReason;
    TInstant FinishTime;
    NKikimrDataShardLoad::TLoadReport Report;
};

// TLoad

class TLoad : public TActorBootstrapped<TLoad> {
public:
    enum class EState {
        Init,
        DropTable,
        CreateTable,
        DescribePath,
        Warmup,
        RunLoad,
    };

private:
    const TActorId Parent;
    const ui64 Tag;
    NKikimrDataShardLoad::TEvYCSBTestLoadRequest Request;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;

    EState State = EState::Init;
    ui64 LastTag = 0;
    TInstant StartTs;
    THashSet<TActorId> LoadActors;

    TString Session;

    TActorId HttpInfoCollector;
    THashMap<TActorId, ui64> HttpInfoWaiters;

    // info about finished actors
    TVector<TFinishedTestInfo> FinishedTests;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::DS_LOAD_ACTOR;
    }

    TLoad(TActorId parent,
          ui64 tag,
          const NKikimrDataShardLoad::TEvYCSBTestLoadRequest& request,
          const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters)
        : Parent(parent)
        , Tag(tag)
        , Request(std::move(request))
        , Counters(counters)
    {}

    void Bootstrap(const TActorContext& ctx) {
        Become(&TLoad::StateFunc);

        // There are three cases:
        // 1. Drop/create or create table.
        // 2. Just resolve the table
        // 3. Request contains all the data and we can run the load

        if (!Request.HasTableSetup()) {
            State = EState::RunLoad;
            RunLoad(ctx);
            return;
        }

        const auto& setup = Request.GetTableSetup();
        if (!setup.HasWorkingDir()) {
            StopWithError(ctx, "Missing working directory param");
            return;
        }
        if (!setup.HasTableName()) {
            StopWithError(ctx, "Missing table name param");
            return;
        }

        CreateSession(ctx);
    }

    void CreateSession(const TActorContext& ctx) {
        auto kqpProxy = NKqp::MakeKqpProxyID(ctx.SelfID.NodeId());
        LOG_TRACE_S(ctx, NKikimrServices::DS_LOAD_TEST, "TLoad# " << Tag
            << " sends event for session creation to proxy# " << kqpProxy.ToString());

        auto ev = MakeHolder<NKqp::TEvKqp::TEvCreateSessionRequest>();
        ev->Record.MutableRequest()->SetDatabase(Request.GetTableSetup().GetWorkingDir());
        Send(kqpProxy, ev.Release());
    }

    void CloseSession(const TActorContext& ctx) {
        if (!Session)
            return;

        auto kqpProxy = NKqp::MakeKqpProxyID(ctx.SelfID.NodeId());
        LOG_TRACE_S(ctx, NKikimrServices::DS_LOAD_TEST, "TLoad# " << Tag
            << " sends session close query to proxy: " << kqpProxy);

        auto ev = MakeHolder<NKqp::TEvKqp::TEvCloseSessionRequest>();
        ev->Record.MutableRequest()->SetSessionId(Session);
        ctx.Send(kqpProxy, ev.Release());
    }

    void Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const TActorContext& ctx) {
        auto& response = ev->Get()->Record;

        if (response.GetYdbStatus() == Ydb::StatusIds_StatusCode_SUCCESS) {
            Session = response.GetResponse().GetSessionId();
            LOG_TRACE_S(ctx, NKikimrServices::DS_LOAD_TEST, "TLoad# " << Tag << " session: " << Session);
            PrepareTable(ctx);
        } else {
            StopWithError(ctx, "failed to create session: " + ev->Get()->ToString());
        }
    }

    void PrepareTable(const TActorContext& ctx) {
        const auto& setup = Request.GetTableSetup();
        if (State == EState::Init) {
            if (setup.GetDropTable()) {
                State = EState::DropTable;
            } else if (setup.GetCreateTable()) {
                State = EState::CreateTable;
            } else {
                State = EState::DescribePath;
            }
        }

        switch (State) {
        case EState::DropTable:
            return DropTable(ctx);
        case EState::CreateTable:
            return CreateTable(ctx);
        case EState::DescribePath:
            return DescribePath(ctx);
        case EState::Warmup:
            return Warmup(ctx);
        case EState::RunLoad:
            return RunLoad(ctx);
        default:
            StopWithError(ctx, TStringBuilder() << "PrepareTable: wrong state " << State);
            return;
        }
    }

    void DropTable(const TActorContext& ctx) {
        const auto& setup = Request.GetTableSetup();
        LOG_NOTICE_S(ctx, NKikimrServices::DS_LOAD_TEST, "TLoad# " << Tag
            << " drops table# " << setup.GetTableName()
            << " in dir# " << setup.GetWorkingDir());

        TString queryBase = R"__(
            --!syntax_v1
            DROP TABLE `%s/%s`;
        )__";

        TString query = Sprintf(
            queryBase.c_str(),
            setup.GetWorkingDir().c_str(),
            setup.GetTableName().c_str());

        SendQuery(ctx, std::move(query));
    }

    void CreateTable(const TActorContext& ctx) {
        const auto& setup = Request.GetTableSetup();

        LOG_NOTICE_S(ctx, NKikimrServices::DS_LOAD_TEST, "TLoad# " << Tag
            << " creates table# " << setup.GetTableName()
            << " in dir# " << setup.GetWorkingDir());

        TString queryBase = R"__(
            --!syntax_v1
            CREATE TABLE `%s/%s` (
                id Text NOT NULL,
                field0 Bytes,
                field1 Bytes,
                field2 Bytes,
                field3 Bytes,
                field4 Bytes,
                field5 Bytes,
                field6 Bytes,
                field7 Bytes,
                field8 Bytes,
                field9 Bytes,
                PRIMARY KEY(id)
            ) WITH (AUTO_PARTITIONING_BY_LOAD = DISABLED,
                    AUTO_PARTITIONING_BY_SIZE = ENABLED,
                    AUTO_PARTITIONING_PARTITION_SIZE_MB = %)__" PRIu64 R"__(,
                    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = %)__" PRIu64 R"__(,
                    AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = %)__" PRIu64 ");";

        TString query = Sprintf(
            queryBase.c_str(),
            setup.GetWorkingDir().c_str(),
            setup.GetTableName().c_str(),
            setup.GetMaxPartSizeMb(),
            setup.GetMinParts(),
            setup.GetMaxParts());

        SendQuery(ctx, std::move(query));
    }

    void SendQuery(const TActorContext& ctx, TString&& query) {
        const auto& setup = Request.GetTableSetup();
        auto ev = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
        ev->Record.MutableRequest()->SetDatabase(setup.GetWorkingDir());
        ev->Record.MutableRequest()->SetSessionId(Session);
        ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_DDL);
        ev->Record.MutableRequest()->SetQuery(std::move(query));

        auto kqpProxy = NKqp::MakeKqpProxyID(ctx.SelfID.NodeId());
        ctx.Send(kqpProxy, ev.Release());
    }

    void Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
        LOG_TRACE_S(ctx, NKikimrServices::DS_LOAD_TEST, "TLoad# " << Tag
            << " received from " << ev->Sender << ": " << ev->Get()->Record.DebugString());

        auto& response = ev->Get()->Record.GetRef();
        if (response.GetYdbStatus() != Ydb::StatusIds_StatusCode_SUCCESS) {
            TStringStream ss;
            ss << "KQP query failed in state# " << State << ": " << response.GetYdbStatus();
            StopWithError(ctx, ss.Str());
            return;
        }

        switch (State) {
        case EState::DropTable:
            State = EState::CreateTable;
            break;
        case EState::CreateTable:
            State = EState::DescribePath;
            break;
        default:
            StopWithError(ctx, TStringBuilder() << "Handle Query response: unknown state# " << State);
            return;
        }

        PrepareTable(ctx);
    }

    void DescribePath(const TActorContext& ctx) {
        const auto& setup = Request.GetTableSetup();
        TString path = setup.GetWorkingDir() + "/" + setup.GetTableName();

        auto request = std::make_unique<TEvTxUserProxy::TEvNavigate>();
        request->Record.MutableDescribePath()->SetPath(path);

        ctx.Send(MakeTxProxyID(), request.release());
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->GetRecord();

        TVector<ui64> shards;
        for (auto &part : record.GetPathDescription().GetTablePartitions())
            shards.push_back(part.GetDatashardId());

        if (shards.empty()) {
            StopWithError(ctx, "No shards");
            return;
        }

        const auto& setup = Request.GetTableSetup();
        auto& target = *Request.MutableTargetShard();
        target.SetTabletId(shards[0]); // for now first shard
        target.SetTableId(record.GetPathId());
        target.SetWorkingDir(setup.GetWorkingDir());
        target.SetTableName(setup.GetTableName());

        State = EState::Warmup;
        PrepareTable(ctx);
    }

    void Warmup(const TActorContext& ctx) {
        // load initial rows, so that later we write *same* rows to non-empty shard,
        // i.e. shard which calculates stats, compacts, etc

        if (!Request.HasTableSetup() || Request.GetTableSetup().GetSkipWarmup()) {
            LOG_INFO_S(ctx, NKikimrServices::DS_LOAD_TEST, "TLoad# " << Tag
                << " skipped warmup");

            State = EState::RunLoad;
            PrepareTable(ctx);
            return;
        }

        NKikimrDataShardLoad::TEvYCSBTestLoadRequest::TUpdateStart cmd;
        switch (Request.Command_case()) {
        case NKikimrDataShardLoad::TEvYCSBTestLoadRequest::CommandCase::kUpsertBulkStart:
            cmd = Request.GetUpsertBulkStart();
            break;
        case NKikimrDataShardLoad::TEvYCSBTestLoadRequest::CommandCase::kUpsertLocalMkqlStart:
            cmd = Request.GetUpsertLocalMkqlStart();
            break;
        case NKikimrDataShardLoad::TEvYCSBTestLoadRequest::CommandCase::kUpsertKqpStart:
            cmd = Request.GetUpsertKqpStart();
            break;
        case NKikimrDataShardLoad::TEvYCSBTestLoadRequest::CommandCase::kUpsertProposeStart:
            cmd = Request.GetUpsertProposeStart();
            break;
        case NKikimrDataShardLoad::TEvYCSBTestLoadRequest::CommandCase::kReadIteratorStart:
            cmd.SetRowCount(Request.GetReadIteratorStart().GetRowCount());
            break;
        case NKikimrDataShardLoad::TEvYCSBTestLoadRequest::CommandCase::kReadKqpStart:
            cmd.SetRowCount(Request.GetReadKqpStart().GetRowCount());
            break;
        default: {
            LOG_INFO_S(ctx, NKikimrServices::DS_LOAD_TEST, "TLoad# " << Tag
                << " skipped warmup");
            State = EState::RunLoad;
            return PrepareTable(ctx);
        }
        }

        const auto& target = Request.GetTargetShard();
        LOG_INFO_S(ctx, NKikimrServices::DS_LOAD_TEST, "TLoad# " << Tag
            << " warmups table# " << target.GetTableName()
            << " in dir# " << target.GetWorkingDir()
            << " with rows# " << cmd.GetRowCount());

        cmd.SetInflight(100);
        cmd.SetBatchSize(100);

        LoadActors.insert(ctx.Register(
            CreateUpsertBulkActor(
               cmd,
               target,
               ctx.SelfID,
               GetServiceCounters(Counters, "load_actor"),
               TSubLoadId(Tag, ctx.SelfID, ++LastTag))));
    }

    void RunLoad(const TActorContext& ctx) {
        StartTs = TAppData::TimeProvider->Now();
        const ui64 tag = ++LastTag;
        std::unique_ptr<IActor> actor;
        auto counters = GetServiceCounters(Counters, "load_actor");

        switch (Request.Command_case()) {
        case NKikimrDataShardLoad::TEvYCSBTestLoadRequest::CommandCase::kUpsertBulkStart:
            actor.reset(CreateUpsertBulkActor(
                Request.GetUpsertBulkStart(),
                Request.GetTargetShard(),
                ctx.SelfID,
                counters,
                TSubLoadId(Tag, ctx.SelfID, ++LastTag)));
            break;
        case NKikimrDataShardLoad::TEvYCSBTestLoadRequest::CommandCase::kUpsertLocalMkqlStart:
            actor.reset(CreateLocalMkqlUpsertActor(
                Request.GetUpsertLocalMkqlStart(),
                Request.GetTargetShard(),
                ctx.SelfID,
                counters,
                TSubLoadId(Tag, ctx.SelfID, ++LastTag)));
            break;
        case NKikimrDataShardLoad::TEvYCSBTestLoadRequest::CommandCase::kUpsertKqpStart:
            actor.reset(CreateKqpUpsertActor(
                Request.GetUpsertKqpStart(),
                Request.GetTargetShard(),
                ctx.SelfID,
                counters,
                TSubLoadId(Tag, ctx.SelfID, ++LastTag)));
            break;
        case NKikimrDataShardLoad::TEvYCSBTestLoadRequest::CommandCase::kUpsertProposeStart:
            actor.reset(CreateProposeUpsertActor(
                Request.GetUpsertProposeStart(),
                Request.GetTargetShard(),
                ctx.SelfID,
                counters,
                TSubLoadId(Tag, ctx.SelfID, ++LastTag)));
            break;
        case NKikimrDataShardLoad::TEvYCSBTestLoadRequest::CommandCase::kReadIteratorStart:
            actor.reset(CreateReadIteratorActor(
                Request.GetReadIteratorStart(),
                Request.GetTargetShard(),
                ctx.SelfID,
                counters,
                TSubLoadId(Tag, ctx.SelfID, ++LastTag)));
            break;
        case NKikimrDataShardLoad::TEvYCSBTestLoadRequest::CommandCase::kReadKqpStart:
            actor.reset(CreateKqpSelectActor(
                Request.GetReadKqpStart(),
                Request.GetTargetShard(),
                ctx.SelfID,
                counters,
                TSubLoadId(Tag, ctx.SelfID, ++LastTag)));
            break;
        default: {
            TStringStream ss;
            ss << "TLoad: unexpected command case# " << Request.Command_case()
               << ", proto# " << Request.DebugString();
            StopWithError(ctx, ss.Str());
            return;
        }
        }

        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "TLoad# " << Tag << " created load actor of type# "
            << Request.Command_case() <<  " with tag# " << tag << ", proto# " << Request.DebugString());

        LoadActors.insert(ctx.Register(actor.release()));
    }

    void Handle(TEvDataShardLoad::TEvTestLoadFinished::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record;
        LoadActors.erase(ev->Sender);

        if (record.HasErrorReason() || !record.HasReport()) {
            TStringStream ss;
            ss <<  "error from actor# " << ev->Sender << " with tag# " << record.GetTag();
            StopWithError(ctx, ss.Str());
            return;
        }

        if (State == EState::Warmup) {
            State = EState::RunLoad;
            return PrepareTable(ctx);
        }

        LOG_INFO_S(ctx, NKikimrServices::DS_LOAD_TEST, "TLoad# " << Tag
            << " received finished from actor# " << ev->Sender << " with tag# " << record.GetTag());

        FinishedTests.push_back({record.GetTag(), record.GetErrorReason(), TAppData::TimeProvider->Now(), record.GetReport()});

        if (LoadActors.empty()) {
            Finish(ctx);
            return;
        }
    }

    void Finish(const TActorContext& ctx) {
        auto endTs = TAppData::TimeProvider->Now();
        auto duration = endTs - StartTs;

        ui64 oks = 0;
        ui64 errors = 0;
        ui64 subtestCount = 0;
        ui64 actualDurationMs = 0; // i.e. time for RPS calculation
        for (const auto& test: FinishedTests) {
            oks += test.Report.GetOperationsOK();
            errors += test.Report.GetOperationsError();
            subtestCount += test.Report.GetSubtestCount();
            actualDurationMs += test.Report.GetDurationMs();
        }

        size_t rps = oks * 1000 / actualDurationMs ? actualDurationMs : 1;

        TIntrusivePtr<TEvLoad::TLoadReport> report(new TEvLoad::TLoadReport());
        report->Duration = duration;

        NJson::TJsonValue value;
        value["duration_s"] = duration.Seconds();
        value["oks"] = oks;
        value["errors"] = errors;
        value["subtests"] = subtestCount;
        value["rps"] = rps;

        TString configString;
        google::protobuf::TextFormat::PrintToString(Request, &configString);

#define PARAM(NAME, VALUE) \
    TABLER() { \
        TABLED() { str << NAME; } \
        TABLED() { str << VALUE; } \
    }

        TStringStream str;
        HTML(str) {
            TABLE_CLASS("table table-condensed") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { str << "Parameter"; }
                        TABLEH() { str << "Value"; }
                    }
                }
                TABLEBODY() {
                    PARAM("Elapsed time total", duration.Seconds() << "s");
                    PARAM("Elapsed RPS time (all actors)", TDuration::MilliSeconds(actualDurationMs).Seconds() << "s");
                    PARAM("RPS", rps);
                    PARAM("OKs", oks);
                    PARAM("Errors", errors);
                    PARAM("Finished subactors", FinishedTests.size())
                }
            }
            DIV() {
                str << configString;
            }
        }

#undef PARAM

        auto finishEv = std::make_unique<TEvLoad::TEvLoadTestFinished>(Tag, report);
        finishEv->JsonResult = std::move(value);
        finishEv->LastHtmlPage = str.Str();

        ctx.Send(Parent, finishEv.release());

        Stop(ctx);
    }

    void Handle(NMon::TEvHttpInfo::TPtr& ev, const TActorContext& ctx) {
        HttpInfoWaiters[ev->Sender] = ev->Get()->SubRequestId;
        if (HttpInfoCollector) {
            return;
        }

        if (LoadActors.empty()) {
            auto response = std::make_unique<TEvDataShardLoad::TEvTestLoadInfoResponse>();
            ctx.Send(ctx.SelfID, response.release());
            return;
        }

        TVector<TActorId> actors;
        actors.reserve(LoadActors.size());
        for (const auto& actor : LoadActors) {
            actors.push_back(actor);
        }
        HttpInfoCollector = ctx.Register(CreateInfoCollector(SelfId(), std::move(actors)));
    }

    void Handle(TEvDataShardLoad::TEvTestLoadInfoResponse::TPtr& ev, const TActorContext& ctx) {
        // aggregate total info
        ui64 oks = 0;
        ui64 errors = 0;
        for (auto& info: ev->Get()->Record.GetReports()) {
            oks += info.GetOperationsOK();
            errors += info.GetOperationsError();
        }

        for (const auto& result: FinishedTests) {
            oks += result.Report.GetOperationsOK();
            errors += result.Report.GetOperationsError();
        }

#define PARAM(NAME, VALUE) \
    TABLER() { \
        TABLED() { str << NAME; } \
        TABLED() { str << VALUE; } \
    }
        TStringStream str;
        HTML(str) {
            TABLE_CLASS("table table-condensed") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { str << "Parameter"; }
                        TABLEH() { str << "Value"; }
                    }
                }
                TABLEBODY() {
                    PARAM("Elapsed time", (TAppData::TimeProvider->Now() - StartTs).Seconds() << "s");
                    PARAM("OKs", oks);
                    PARAM("Errors", errors);
                    PARAM("Running subactors", LoadActors.size())
                    PARAM("Finished subactors", FinishedTests.size())
                }
            }
        }
#undef PARAM

        for (const auto& it: HttpInfoWaiters) {
            ctx.Send(it.first, new NMon::TEvHttpInfoRes(str.Str(), it.second));
        }

        HttpInfoWaiters.clear();
        HttpInfoCollector = {};
    }

    void HandlePoison(const TActorContext& ctx) {
        LOG_INFO_S(ctx, NKikimrServices::DS_LOAD_TEST, "TLoad# " << Tag
            << " actor recieved PoisonPill, going to die with subactorsCount# " << LoadActors.size());
        Stop(ctx);
    }

    void StopWithError(const TActorContext& ctx, const TString& reason) {
        LOG_ERROR_S(ctx, NKikimrServices::DS_LOAD_TEST, "TLoad# " << Tag
            << " stopped with error: " << reason << ", killing subactorsCount# " << LoadActors.size());

        ctx.Send(Parent, new TEvDataShardLoad::TEvTestLoadFinished(Tag, reason));
        Stop(ctx);
    }

    void Stop(const TActorContext& ctx) {
        for (const auto& actorId: LoadActors) {
            ctx.Send(actorId, new TEvents::TEvPoison());
        }

        if (HttpInfoCollector) {
            ctx.Send(HttpInfoCollector, new TEvents::TEvPoison());
        }

        CloseSession(ctx);

        Die(ctx);
    }

    STRICT_STFUNC(StateFunc,
        HFunc(TEvDataShardLoad::TEvTestLoadFinished, Handle)
        HFunc(TEvDataShardLoad::TEvTestLoadInfoResponse, Handle)
        HFunc(NKqp::TEvKqp::TEvQueryResponse, Handle)
        HFunc(NKqp::TEvKqp::TEvCreateSessionResponse, Handle)
        HFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, Handle)
        HFunc(NMon::TEvHttpInfo, Handle)
        CFunc(TEvents::TSystem::PoisonPill, HandlePoison)
    )
};

// TLoadManager

} // anonymous

IActor *CreateTestLoadActor(
    const NKikimrDataShardLoad::TEvYCSBTestLoadRequest& request,
    TActorId parent,
    const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters,
    ui64 tag)
{
    return new TLoad(parent, tag, request, counters);
}

} // NKikimr::NDataShardLoad

template <>
inline void Out<NKikimr::NDataShardLoad::TLoad::EState>(
    IOutputStream& o,
    NKikimr::NDataShardLoad::TLoad::EState state)
{
    switch (state) {
    case NKikimr::NDataShardLoad::TLoad::EState::DropTable:
        o << "dropTable";
        break;
    case NKikimr::NDataShardLoad::TLoad::EState::CreateTable:
        o << "createTable";
        break;
    case NKikimr::NDataShardLoad::TLoad::EState::DescribePath:
        o << "describePath";
        break;
    case NKikimr::NDataShardLoad::TLoad::EState::Warmup:
        o << "warmup";
        break;
    case NKikimr::NDataShardLoad::TLoad::EState::RunLoad:
        o << "runLoad";
        break;
    default:
        o << (int)state;
        break;
    }
}

template <>
void Out<NKikimr::NDataShardLoad::TSubLoadId>(
    IOutputStream& o,
    const NKikimr::NDataShardLoad::TSubLoadId& loadId)
{
    o << "{Tag: " << loadId.Tag << ", parent: " << loadId.Parent << ", subTag: " << loadId.SubTag << "}";
}
