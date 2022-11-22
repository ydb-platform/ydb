#include "actors.h"
#include "info_collector.h"
#include "test_load_actor.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/public/lib/base/msgbus.h>

#include <google/protobuf/text_format.h>
#include <library/cpp/monlib/service/pages/templates.h>

// DataShard load is associated with full path to table: only one load per path can be started.
//
// TLoadManager is a "service" actor which is used to start/stop special TLoad. It controls
// that there is only one TLoad per path.
//
// TLoad is used to prepare database (drop, create, configure, etc) and then start load.
//
// Each TLoad actor has a tag TagStep 1000: i.e. 1000, 2000, 3000. Subactors created by
// TLoad must be within its Tag and next TagStep: i.e. 1001, 1002, etc

namespace NKikimr::NDataShardLoad {

namespace {

constexpr ui64 TagStep = 1000;

struct TFinishedTestInfo {
    ui64 Tag;
    TString ErrorReason;
    TInstant FinishTime;
    std::optional<TEvDataShardLoad::TLoadReport> Report;
};

// TLoad

class TLoad : public TActorBootstrapped<TLoad> {
public:
    enum class EState {
        Init,
        DropTable,
        CreateTable,
        DescribePath,
        RunLoad,
    };

private:
    const TActorId Parent;
    const ui64 Tag;
    NKikimrDataShardLoad::TEvTestLoadRequest Request;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;

    EState State = EState::Init;
    ui64 LastTag;
    TInstant StartTs;
    THashSet<TActorId> LoadActors;

    TString Session;

    TActorId HttpInfoCollector;

    // info about finished actors
    TVector<TFinishedTestInfo> FinishedTests;

    enum class State {
    };

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::DS_LOAD_ACTOR;
    }

    TLoad(TActorId parent,
          ui64 tag,
          NKikimrDataShardLoad::TEvTestLoadRequest&& request,
          const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters)
        : Parent(parent)
        , Tag(tag)
        , Request(std::move(request))
        , Counters(counters)
        , LastTag(tag)
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
        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "TLoad# " << Tag
            << " sends event for session creation to proxy# " << kqpProxy.ToString());

        auto ev = MakeHolder<NKqp::TEvKqp::TEvCreateSessionRequest>();
        ev->Record.MutableRequest()->SetDatabase(Request.GetTableSetup().GetWorkingDir());
        Send(kqpProxy, ev.Release());
    }

    void CloseSession(const TActorContext& ctx) {
        if (!Session)
            return;

        auto kqpProxy = NKqp::MakeKqpProxyID(ctx.SelfID.NodeId());
        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "TLoad# " << Tag
            << " sends session close query to proxy: " << kqpProxy);

        auto ev = MakeHolder<NKqp::TEvKqp::TEvCloseSessionRequest>();
        ev->Record.MutableRequest()->SetSessionId(Session);
        ctx.Send(kqpProxy, ev.Release());
    }

    void Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const TActorContext& ctx) {
        auto& response = ev->Get()->Record;

        if (response.GetYdbStatus() == Ydb::StatusIds_StatusCode_SUCCESS) {
            Session = response.GetResponse().GetSessionId();
            LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "TLoad# " << Tag << " session: " << Session);
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
                key Utf8,
                field0 Utf8,
                field1 Utf8,
                field2 Utf8,
                field3 Utf8,
                field4 Utf8,
                field5 Utf8,
                field6 Utf8,
                field7 Utf8,
                field8 Utf8,
                field9 Utf8,
                PRIMARY KEY(key)
            );
        )__";

        TString query = Sprintf(
            queryBase.c_str(),
            setup.GetWorkingDir().c_str(),
            setup.GetTableName().c_str());

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

        State = EState::RunLoad;
        PrepareTable(ctx);
    }

    void RunLoad(const TActorContext& ctx) {
        StartTs = TAppData::TimeProvider->Now();
        const ui64 tag = ++LastTag;
        std::unique_ptr<IActor> actor;
        auto counters = GetServiceCounters(Counters, "load_actor");

        switch (Request.Command_case()) {
        case NKikimrDataShardLoad::TEvTestLoadRequest::CommandCase::kUpsertBulkStart:
            actor.reset(CreateUpsertBulkActor(
                Request.GetUpsertBulkStart(),
                Request.GetTargetShard(),
                ctx.SelfID,
                counters,
                tag));
            break;
        case NKikimrDataShardLoad::TEvTestLoadRequest::CommandCase::kUpsertLocalMkqlStart:
            actor.reset(CreateLocalMkqlUpsertActor(
                Request.GetUpsertLocalMkqlStart(),
                Request.GetTargetShard(),
                ctx.SelfID,
                counters,
                tag));
            break;
        case NKikimrDataShardLoad::TEvTestLoadRequest::CommandCase::kUpsertKqpStart:
            actor.reset(CreateKqpUpsertActor(
                Request.GetUpsertKqpStart(),
                Request.GetTargetShard(),
                ctx.SelfID,
                counters,
                tag));
            break;
        case NKikimrDataShardLoad::TEvTestLoadRequest::CommandCase::kUpsertProposeStart:
            actor.reset(CreateProposeUpsertActor(
                Request.GetUpsertProposeStart(),
                Request.GetTargetShard(),
                ctx.SelfID,
                counters,
                tag));
            break;
        case NKikimrDataShardLoad::TEvTestLoadRequest::CommandCase::kReadIteratorStart:
            actor.reset(CreateReadIteratorActor(
                Request.GetReadIteratorStart(),
                Request.GetTargetShard(),
                ctx.SelfID,
                counters,
                tag));
            break;
        default: {
            TStringStream ss;
            ss << "TLoad: unexpected command case# " << ui32(Request.Command_case())
               << ", proto# " << Request.DebugString();
            StopWithError(ctx, ss.Str());
            return;
        }
        }

        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "TLoad# " << Tag << " created load actor of type# "
            << ui32(Request.Command_case())<<  " with tag# " << tag << ", proto# " << Request.DebugString());

        LoadActors.insert(ctx.Register(actor.release()));
    }

    void Handle(TEvDataShardLoad::TEvTestLoadFinished::TPtr& ev, const TActorContext& ctx) {
        const auto& msg = ev->Get();

        if (msg->ErrorReason || !msg->Report) {
            TStringStream ss;
            ss <<  "error from actor# " << ev->Sender << " with tag# " << msg->Tag;
            StopWithError(ctx, ss.Str());
            return;
        }

        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "TLoad# " << Tag
            << " received finished from actor# " << ev->Sender << " with tag# " << msg->Tag);

        FinishedTests.push_back({msg->Tag, msg->ErrorReason, TAppData::TimeProvider->Now(), msg->Report});

        LoadActors.erase(ev->Sender);
        if (LoadActors.empty()) {
            Finish(ctx);
            return;
        }
    }

    void Finish(const TActorContext& ctx) {
        auto endTs = TAppData::TimeProvider->Now();

        auto response = std::make_unique<TEvDataShardLoad::TEvTestLoadFinished>();
        response->Tag = Tag;
        response->Report = TEvDataShardLoad::TLoadReport();
        response->Report->Duration = endTs - StartTs;

        TStringStream ss;
        for (const auto& test: FinishedTests) {
            Y_VERIFY(test.Report);
            response->Report->OperationsOK += test.Report->OperationsOK;
            response->Report->OperationsError += test.Report->OperationsError;
            response->Report->SubtestCount += test.Report->SubtestCount;
            if (test.Report->Info)
                ss << test.Report->Info << Endl;
        }

        ctx.Send(Parent, response.release());
        Die(ctx);
    }

    void Handle(TEvDataShardLoad::TEvTestLoadInfoRequest::TPtr& ev, const TActorContext& ctx) {
        if (HttpInfoCollector)
            return;

        TVector<TActorId> actors;
        actors.reserve(LoadActors.size());

        // send messages to subactors
        for (const auto& actorId : LoadActors) {
            actors.push_back(actorId);
        }

        // note that only parent can send us this request
        Y_VERIFY(ev->Sender == Parent);
        HttpInfoCollector = ctx.Register(CreateInfoCollector(SelfId(), std::move(actors)));
    }

    void Handle(TEvDataShardLoad::TEvTestLoadInfoResponse::TPtr& ev, const TActorContext& ctx) {
        HttpInfoCollector = {};

        auto response = std::make_unique<TEvDataShardLoad::TEvTestLoadInfoResponse>();
        auto& record = response->Record;

        TStringStream ss;
        ss << "TLoad# " << Tag << " started on " << StartTs
           << " with subactors active# " << LoadActors.size()
           << ", finished# " << FinishedTests.size();

        auto* info = record.AddInfos();
        info->SetTag(Tag);
        info->SetData(ss.Str());

        // bad that we copy, but to have it sorted by tag we had to prepend with our info
        auto& collectedRecord = ev->Get()->Record;
        for (auto& info: collectedRecord.GetInfos()) {
            *record.AddInfos() = std::move(info);
        }

        ctx.Send(Parent, response.release());
    }

    void HandlePoison(const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "TLoad# " << Tag
            << " actor recieved PoisonPill, going to die with subactorsCount# " << LoadActors.size());
        Stop(ctx);
    }

    void StopWithError(const TActorContext& ctx, const TString& reason) {
        LOG_WARN_S(ctx, NKikimrServices::DS_LOAD_TEST, "TLoad# " << Tag
            << " stopped with error: " << reason << ", killing subactorsCount# " << LoadActors.size());

        ctx.Send(Parent, new TEvDataShardLoad::TEvTestLoadFinished(Tag, reason));
        Stop(ctx);
    }

    void Stop(const TActorContext& ctx) {
        for (const auto& actorId: LoadActors) {
            ctx.Send(actorId, new TEvents::TEvPoison());
        }

        CloseSession(ctx);

        Die(ctx);
    }

    STRICT_STFUNC(StateFunc,
        HFunc(TEvDataShardLoad::TEvTestLoadFinished, Handle)
        HFunc(TEvDataShardLoad::TEvTestLoadInfoRequest, Handle)
        HFunc(TEvDataShardLoad::TEvTestLoadInfoResponse, Handle)
        HFunc(NKqp::TEvKqp::TEvQueryResponse, Handle)
        HFunc(NKqp::TEvKqp::TEvCreateSessionResponse, Handle)
        HFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, Handle)
        CFunc(TEvents::TSystem::PoisonPill, HandlePoison)
    )
};

// TLoadManager

class TLoadManager : public TActorBootstrapped<TLoadManager> {
    struct TRunningActorInfo {
        TActorId ActorId;
        TActorId Parent; // if set we notify parent when actor finishes

        explicit TRunningActorInfo(const TActorId& actorId, const TActorId& parent = {})
            : ActorId(actorId)
            , Parent(parent)
        {
        }
    };

    // info about finished actors
    TVector<TFinishedTestInfo> FinishedTests;

    // currently running load actors
    TMap<ui64, TRunningActorInfo> LoadActors;

    ui64 LastTag = 0; // tags start from TagStep

    THashMap<TActorId, ui64> HttpInfoWaiters;
    TActorId HttpInfoCollector;

    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::DS_LOAD_ACTOR;
    }

    TLoadManager(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters)
        : Counters(counters)
    {}

    void Bootstrap(const TActorContext& /*ctx*/) {
        Become(&TLoadManager::StateFunc);
    }

    void Handle(TEvDataShardLoad::TEvTestLoadRequest::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record;
        ui32 status = NMsgBusProxy::MSTATUS_OK;
        TString error;
        ui64 tag = 0;
        try {
            tag = ProcessCmd(ev, ctx);
        } catch (const TLoadManagerException& ex) {
            LOG_ERROR_S(ctx, NKikimrServices::DS_LOAD_TEST, "Exception while creating load actor, what# "
                    << ex.what());
            status = NMsgBusProxy::MSTATUS_ERROR;
            error = ex.what();
        }
        auto response = std::make_unique<TEvDataShardLoad::TEvTestLoadResponse>();
        response->Record.SetStatus(status);
        if (error) {
            response->Record.SetErrorReason(error);
        }
        if (record.HasCookie()) {
            response->Record.SetCookie(record.GetCookie());
        }
        if (tag) {
            response->Record.SetTag(tag);
        }
        ctx.Send(ev->Sender, response.release());
    }

    ui64 GetTag() {
        LastTag += TagStep;

        // just sanity check
        if (LoadActors.contains(LastTag)) {
            ythrow TLoadManagerException() << Sprintf("duplicate load actor with Tag# %" PRIu64, LastTag);
        }

        return LastTag;
    }

    ui64 ProcessCmd(TEvDataShardLoad::TEvTestLoadRequest::TPtr& ev, const TActorContext& ctx) {
        auto& record = ev->Get()->Record;
        switch (record.Command_case()) {
        case NKikimrDataShardLoad::TEvTestLoadRequest::CommandCase::kLoadStop: {
            const auto& cmd = record.GetLoadStop();
            if (cmd.HasRemoveAllTags() && cmd.GetRemoveAllTags()) {
                LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "Delete all running load actors");
                for (auto& actorPair : LoadActors) {
                    ctx.Send(actorPair.second.ActorId, new TEvents::TEvPoisonPill);
                }
                LoadActors.clear();
            } else {
                if (!cmd.HasTag()) {
                    ythrow TLoadManagerException() << "Either RemoveAllTags or Tag must present";
                }
                const ui64 tag = cmd.GetTag();
                auto it = LoadActors.find(tag);
                if (it == LoadActors.end()) {
                    ythrow TLoadManagerException()
                        << Sprintf("load actor with Tag# %" PRIu64 " not found", tag);
                }
                LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "Delete running load actor# " << tag);
                ctx.Send(it->second.ActorId, new TEvents::TEvPoisonPill);
                LoadActors.erase(it);
            }

            return 0;
        }
        default: {
            const ui64 tag = GetTag();
            bool notifyParent = record.GetNotifyWhenFinished();
            TRunningActorInfo actorInfo(ctx.Register(new TLoad(SelfId(), tag, std::move(record), Counters)));
            if (notifyParent)
                actorInfo.Parent = ev->Sender;
            LoadActors.emplace(tag, std::move(actorInfo));
            return tag;
        }
        }
    }

    void Handle(TEvDataShardLoad::TEvTestLoadFinished::TPtr& ev, const TActorContext& ctx) {
        const auto& msg = ev->Get();
        auto it = LoadActors.find(msg->Tag);
        Y_VERIFY(it != LoadActors.end(), "%s", (TStringBuilder() << "failed to find actor with tag# " << msg->Tag
            << ", TEvTestLoadFinished from actor# " << ev->Sender).c_str());
        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "Load actor# " << ev->Sender
            << " with tag# " << msg->Tag << " finished");

        if (it->second.Parent) {
            auto response = std::make_unique<TEvDataShardLoad::TEvTestLoadFinished>();
            response->Tag = ev->Get()->Tag;
            response->ErrorReason = ev->Get()->ErrorReason;
            response->Report = ev->Get()->Report;
            ctx.Send(it->second.Parent, response.release());
        }

        LoadActors.erase(it);
        FinishedTests.push_back({msg->Tag, msg->ErrorReason, TAppData::TimeProvider->Now(), msg->Report});
    }

    void Handle(NMon::TEvHttpInfo::TPtr& ev, const TActorContext& ctx) {
        HttpInfoWaiters[ev->Sender] = ev->Get()->SubRequestId;
        if (HttpInfoCollector) {
            return;
        }

        TVector<TActorId> actors;
        actors.reserve(LoadActors.size());

        // send messages to subactors
        for (const auto& kv : LoadActors) {
            actors.push_back(kv.second.ActorId);
        }

        HttpInfoCollector = ctx.Register(CreateInfoCollector(SelfId(), std::move(actors)));
    }

    void Handle(TEvDataShardLoad::TEvTestLoadInfoResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record;

        TStringStream str;
        HTML(str) {
            for (const auto& info: record.GetInfos()) {
                DIV_CLASS("panel panel-info") {
                    DIV_CLASS("panel-heading") {
                        str << "Tag# " << info.GetTag();
                    }
                    DIV_CLASS("panel-body") {
                        str << info.GetData();
                    }
                }
            }

            COLLAPSED_BUTTON_CONTENT("finished_tests_info", "Finished tests") {
                for (const auto& req : FinishedTests) {
                    DIV_CLASS("panel panel-info") {
                        DIV_CLASS("panel-heading") {
                            str << "Tag# " << req.Tag;
                        }
                        DIV_CLASS("panel-body") {
                            str << "<p>";
                            if (req.Report)
                                str << "Report# " << req.Report->ToString() << "<br/>";
                            str << "Finish reason# " << req.ErrorReason << "<br/>";
                            str << "Finish time# " << req.FinishTime << "<br/>";
                            str << "</p>";
                        }
                    }
                }
            }
        }

        for (const auto& it: HttpInfoWaiters) {
            ctx.Send(it.first, new NMon::TEvHttpInfoRes(str.Str(), it.second));
        }

        HttpInfoWaiters.clear();
        HttpInfoCollector = {};
    }

    STRICT_STFUNC(StateFunc,
        HFunc(TEvDataShardLoad::TEvTestLoadRequest, Handle)
        HFunc(TEvDataShardLoad::TEvTestLoadFinished, Handle)
        HFunc(TEvDataShardLoad::TEvTestLoadInfoResponse, Handle)
        HFunc(NMon::TEvHttpInfo, Handle)
    )
};

} // anonymous

IActor *CreateTestLoadActor(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters) {
    return new TLoadManager(counters);
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
    case NKikimr::NDataShardLoad::TLoad::EState::RunLoad:
        o << "runLoad";
        break;
    default:
        o << (int)state;
        break;
    }
}
