#include "actors.h"

#include <ydb/core/base/tablet.h>
#include <ydb/core/kqp/kqp.h>
#include <ydb/core/ydb_convert/ydb_convert.h>

#include <ydb/public/lib/operation_id/operation_id.h>
#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <google/protobuf/text_format.h>

// * Scheme is hardcoded and it is like default YCSB setup:
// table name is "usertable", 1 utf8 "key" column, 10 utf8 "field0" - "field9" columns
// * row is ~ 1 KB, keys are like user1000385178204227360

namespace NKikimr::NDataShardLoad {

namespace {

void ConvertYdbParamsToMiniKQLParams(const NYdb::TParams& input, NKikimrMiniKQL::TParams& output) {
    output.MutableType()->SetKind(NKikimrMiniKQL::ETypeKind::Struct);
    auto type = output.MutableType()->MutableStruct();
    auto value = output.MutableValue();
    for (const auto& p : input.GetValues()) {
        auto typeMember = type->AddMember();
        auto valueItem = value->AddStruct();
        typeMember->SetName(p.first);

        ConvertYdbTypeToMiniKQLType(
            NYdb::TProtoAccessor::GetProto(p.second.GetType()),
            *typeMember->MutableType());

        ConvertYdbValueToMiniKQLValue(
            NYdb::TProtoAccessor::GetProto(p.second.GetType()),
            NYdb::TProtoAccessor::GetProto(p.second),
            *valueItem);
    }
}

struct TQueryInfo {
    TQueryInfo()
        : Query("")
        , Params(NYdb::TParamsBuilder().Build())
    {}

    TQueryInfo(const std::string& query, const NYdb::TParams&& params)
        : Query(query)
        , Params(std::move(params))
    {}

    TString Query;
    NYdb::TParams Params;
};

TQueryInfo GenerateUpsert(size_t n) {
    TStringStream str;

    NYdb::TParamsBuilder paramsBuilder;

    str << R"__(
        --!syntax_v1

        DECLARE $key AS Utf8;
        DECLARE $field0 AS Utf8;
        DECLARE $field1 AS Utf8;
        DECLARE $field2 AS Utf8;
        DECLARE $field3 AS Utf8;
        DECLARE $field4 AS Utf8;
        DECLARE $field5 AS Utf8;
        DECLARE $field6 AS Utf8;
        DECLARE $field7 AS Utf8;
        DECLARE $field8 AS Utf8;
        DECLARE $field9 AS Utf8;

        UPSERT INTO `usertable` ( key, field0, field1, field2, field3, field4, field5, field6, field7, field8, field9 )
            VALUES ( $key, $field0, $field1, $field2, $field3, $field4, $field5, $field6, $field7, $field8, $field9 );
    )__";

    paramsBuilder.AddParam("$key").Utf8(GetKey(n)).Build();

    for (size_t i = 0; i < 10; ++i) {
        TString name = "$field" + ToString(i);
        paramsBuilder.AddParam(name).Utf8(Value).Build();
    }

    auto params = paramsBuilder.Build();

    return TQueryInfo(str.Str(), std::move(params));
}


// it's a partial copy-paste from TUpsertActor: logic slightly differs, so that
// it seems better to have copy-paste rather if/else for different loads
class TKqpUpsertActor : public TActorBootstrapped<TKqpUpsertActor> {
    const NKikimrDataShardLoad::TEvTestLoadRequest::TUpdateStart Config;
    const TActorId Parent;
    const ui64 Tag;
    const TString Path;

    TString ConfingString;

    TString Session = "wrong sessionId";
    TRequestsVector Requests;
    size_t CurrentRequest = 0;
    size_t Inflight = 0;

    TInstant StartTs;
    TInstant EndTs;

    size_t Errors = 0;

public:
    TKqpUpsertActor(const NKikimrDataShardLoad::TEvTestLoadRequest::TUpdateStart& cmd, const TActorId& parent,
            TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, ui64 tag, TRequestsVector requests)
        : Config(cmd)
        , Parent(parent)
        , Tag(tag)
        , Path(cmd.GetPath())
        , Requests(std::move(requests))
    {
        Y_UNUSED(counters);
        google::protobuf::TextFormat::PrintToString(cmd, &ConfingString);
    }

    void Bootstrap(const TActorContext& ctx) {
        LOG_NOTICE_S(ctx, NKikimrServices::DS_LOAD_TEST, "TKqpUpsertActor# " << Tag
            << " Bootstrap called: " << ConfingString);

        Become(&TKqpUpsertActor::StateFunc);
        CreateSession(ctx);
    }

private:
    void CreateSession(const TActorContext& ctx) {
        auto kqpProxy = NKqp::MakeKqpProxyID(ctx.SelfID.NodeId());
        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "KqpUpsertActor# " << Tag
            << " sends event for session creation to proxy: " << kqpProxy.ToString());

        auto ev = MakeHolder<NKqp::TEvKqp::TEvCreateSessionRequest>();
        ev->Record.MutableRequest()->SetDatabase(Path);
        Send(kqpProxy, ev.Release());
    }

    void CloseSession(const TActorContext& ctx) {
        auto kqpProxy = NKqp::MakeKqpProxyID(ctx.SelfID.NodeId());
        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "KqpUpsertActor# " << Tag
            << " sends session close query to proxy: " << kqpProxy);

        auto ev = MakeHolder<NKqp::TEvKqp::TEvCloseSessionRequest>();
        ev->Record.MutableRequest()->SetSessionId(Session);
        ctx.Send(kqpProxy, ev.Release());
    }

    void SendRows(const TActorContext &ctx) {
        while (Inflight < Config.GetInflight() && CurrentRequest < Requests.size()) {
            auto* request = static_cast<NKqp::TEvKqp::TEvQueryRequest*>(Requests[CurrentRequest].get());
            request->Record.MutableRequest()->SetSessionId(Session);

            auto kqpProxy = NKqp::MakeKqpProxyID(ctx.SelfID.NodeId());
            LOG_TRACE_S(ctx, NKikimrServices::DS_LOAD_TEST, "KqpUpsertActor# " << Tag
                << " send request# " << CurrentRequest
                << " to proxy# " << kqpProxy << ": " << request->ToString());

            ctx.Send(kqpProxy, Requests[CurrentRequest].release());

            ++CurrentRequest;
            ++Inflight;
        }
    }

    void OnRequestDone(const TActorContext& ctx) {
        if (CurrentRequest < Requests.size()) {
            SendRows(ctx);
        } else if (Inflight == 0) {
            EndTs = TInstant::Now();
            auto delta = EndTs - StartTs;
            auto response = std::make_unique<TEvDataShardLoad::TEvTestLoadFinished>(Tag);
            response->Report = TEvDataShardLoad::TLoadReport();
            response->Report->Duration = delta;
            response->Report->OperationsOK = Requests.size() - Errors;
            response->Report->OperationsError = Errors;
            ctx.Send(Parent, response.release());

            LOG_NOTICE_S(ctx, NKikimrServices::DS_LOAD_TEST, "KqpUpsertActor# " << Tag
                << " finished in " << delta << ", errors=" << Errors);
            Die(ctx);
        }
    }

    void HandlePoison(const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "KqpUpsertActor# " << Tag << " tablet recieved PoisonPill, going to die");
        CloseSession(ctx);
        Die(ctx);
    }

    void Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const TActorContext& ctx) {
        auto& response = ev->Get()->Record;

        if (response.GetYdbStatus() == Ydb::StatusIds_StatusCode_SUCCESS) {
            Session = response.GetResponse().GetSessionId();
            LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "KqpUpsertActor# " << Tag << " session: " << Session);
            SendRows(ctx);
        } else {
            StopWithError(ctx, "failed to create session: " + ev->Get()->ToString());
        }
    }

    void Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
        LOG_TRACE_S(ctx, NKikimrServices::DS_LOAD_TEST, "KqpUpsertActor# " << Tag
            << " received from " << ev->Sender << ": " << ev->Get()->Record.DebugString());

        --Inflight;

        auto& response = ev->Get()->Record.GetRef();
        if (response.GetYdbStatus() != Ydb::StatusIds_StatusCode_SUCCESS) {
            ++Errors;
        }

        OnRequestDone(ctx);
    }

    void StopWithError(const TActorContext& ctx, const TString& reason) {
        LOG_WARN_S(ctx, NKikimrServices::DS_LOAD_TEST, "Load tablet stopped with error: " << reason);
        ctx.Send(Parent, new TEvDataShardLoad::TEvTestLoadFinished(Tag, reason));
        Die(ctx);
    }

    void Handle(NMon::TEvHttpInfo::TPtr& ev, const TActorContext& ctx) {
        TStringStream str;
        HTML(str) {
            str << "KqpUpsertActor# " << Tag << " started on " << StartTs
                << " sent " << CurrentRequest << " out of " << Requests.size();
            TInstant ts = EndTs ? EndTs : TInstant::Now();
            auto delta = ts - StartTs;
            auto throughput = Requests.size() / delta.Seconds();
            str << " in " << delta << " (" << throughput << " op/s)"
                << " errors=" << Errors;
        }
        ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str(), ev->Get()->SubRequestId));
    }

    STRICT_STFUNC(StateFunc,
        CFunc(TEvents::TSystem::PoisonPill, HandlePoison)
        HFunc(NMon::TEvHttpInfo, Handle)
        HFunc(NKqp::TEvKqp::TEvQueryResponse, Handle)
        HFunc(NKqp::TEvKqp::TEvCreateSessionResponse, Handle)
    )
};

// creates multiple TKqpUpsertActor for inflight > 1 and waits completion
class TKqpUpsertActorMultiSession : public TActorBootstrapped<TKqpUpsertActorMultiSession> {
    const NKikimrDataShardLoad::TEvTestLoadRequest::TUpdateStart Config;
    const TActorId Parent;
    const ui64 Tag;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    const TString Path;

    TString ConfingString;

    TVector<TActorId> Actors;

    size_t Inflight = 0;

    TInstant StartTs;
    TInstant EndTs;

    size_t Oks = 0;
    size_t Errors = 0;

public:
    TKqpUpsertActorMultiSession(const NKikimrDataShardLoad::TEvTestLoadRequest::TUpdateStart& cmd, const TActorId& parent,
            TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, ui64 tag)
        : Config(cmd)
        , Parent(parent)
        , Tag(tag)
        , Counters(counters)
        , Path(cmd.GetPath())
    {
        Y_UNUSED(counters);
        google::protobuf::TextFormat::PrintToString(cmd, &ConfingString);
    }

    void Bootstrap(const TActorContext& ctx) {
        LOG_NOTICE_S(ctx, NKikimrServices::DS_LOAD_TEST, "TKqpUpsertActorMultiSession# " << Tag
            << " Bootstrap called: " << ConfingString);

        Become(&TKqpUpsertActorMultiSession::StateFunc);
        StartActors(ctx);
    }

private:
    void StartActors(const TActorContext& ctx) {
        const auto actorsCount = Config.GetInflight();
        const auto requestsPerActor = Config.GetRowCount() / actorsCount;

        TVector<TRequestsVector> perActorRequests;
        perActorRequests.reserve(actorsCount);

        size_t rowCount = 0;
        for (size_t i = 0; i < actorsCount; ++i) {
            TRequestsVector requests;

            requests.reserve(requestsPerActor);
            for (size_t i = 0; i < requestsPerActor; ++i) {
                auto queryInfo = GenerateUpsert(rowCount++);

                auto request = std::make_unique<NKqp::TEvKqp::TEvQueryRequest>();
                request->Record.MutableRequest()->SetKeepSession(true);
                request->Record.MutableRequest()->SetDatabase(Path);

                request->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
                request->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
                request->Record.MutableRequest()->SetQuery(queryInfo.Query);

                request->Record.MutableRequest()->MutableQueryCachePolicy()->set_keep_in_cache(true);
                request->Record.MutableRequest()->MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
                request->Record.MutableRequest()->MutableTxControl()->set_commit_tx(true);

                NKikimrMiniKQL::TParams params;
                ConvertYdbParamsToMiniKQLParams(queryInfo.Params, params);
                request->Record.MutableRequest()->MutableParameters()->Swap(&params);

                requests.emplace_back(std::move(request));
            }
            perActorRequests.emplace_back(std::move(requests));
        }

        StartTs = TInstant::Now();

        Actors.reserve(actorsCount);
        Inflight = actorsCount;
        for (size_t i = 0; i < actorsCount; ++i) {
            ui32 pseudoTag = 1000000 + i;
            auto configCopy = Config;
            configCopy.SetInflight(1); // we have only 1 session
            configCopy.SetRowCount(requestsPerActor);

            auto* kqpActor = new TKqpUpsertActor(
                configCopy,
                SelfId(),
                Counters,
                pseudoTag,
                std::move(perActorRequests[i]));
            Actors.emplace_back(ctx.Register(kqpActor));
        }

        LOG_NOTICE_S(ctx, NKikimrServices::DS_LOAD_TEST, "TKqpUpsertActorMultiSession# " << Tag
            << " started# " << actorsCount << " actors each with inflight# " << requestsPerActor);
    }

    void Handle(const TEvDataShardLoad::TEvTestLoadFinished::TPtr& ev, const TActorContext& ctx) {
        const auto* msg = ev->Get();
        if (msg->ErrorReason || !msg->Report) {
            TStringStream ss;
            ss << "kqp actor# " << msg->Tag << " finished with error: " << msg->ErrorReason;
            if (msg->Report)
                ss << ", report: " << msg->Report->ToString();

            StopWithError(ctx, ss.Str());
            return;
        }

        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "kqp# " << Tag << " finished: " << msg->Report->ToString());

        Errors += msg->Report->OperationsError;
        Oks += msg->Report->OperationsOK;

        --Inflight;
        if (Inflight == 0) {
            EndTs = TInstant::Now();
            auto delta = EndTs - StartTs;
            auto response = std::make_unique<TEvDataShardLoad::TEvTestLoadFinished>(Tag);
            response->Report = TEvDataShardLoad::TLoadReport();
            response->Report->Duration = delta;
            response->Report->OperationsOK = Oks;
            response->Report->OperationsError = Errors;
            ctx.Send(Parent, response.release());

            LOG_NOTICE_S(ctx, NKikimrServices::DS_LOAD_TEST, "TKqpUpsertActorMultiSession# " << Tag
                << " finished in " << delta << ", oks# " << Oks << ", errors# " << Errors);

            Stop(ctx);
        }
    }

    void Handle(NMon::TEvHttpInfo::TPtr& ev, const TActorContext& ctx) {
        TStringStream str;
        HTML(str) {
            str << "TKqpUpsertActorMultiSession# " << Tag << " started on " << StartTs;
        }
        ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str(), ev->Get()->SubRequestId));
    }

    void HandlePoison(const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "TKqpUpsertActorMultiSession# " << Tag
            << " tablet recieved PoisonPill, going to die");
        Die(ctx);
    }

    void StopWithError(const TActorContext& ctx, const TString& reason) {
        LOG_WARN_S(ctx, NKikimrServices::DS_LOAD_TEST, "TKqpUpsertActorMultiSession# " << Tag
            << " stopped with error: " << reason);

        ctx.Send(Parent, new TEvDataShardLoad::TEvTestLoadFinished(Tag, reason));
        Stop(ctx);
    }

    void Stop(const TActorContext& ctx) {
        for (const auto& actorId: Actors) {
            ctx.Send(actorId, new TEvents::TEvPoison());
        }

        Die(ctx);
    }

    STRICT_STFUNC(StateFunc,
        CFunc(TEvents::TSystem::PoisonPill, HandlePoison)
        HFunc(NMon::TEvHttpInfo, Handle)
        HFunc(TEvDataShardLoad::TEvTestLoadFinished, Handle);
    )
};

} // anonymous

NActors::IActor *CreateKqpUpsertActor(const NKikimrDataShardLoad::TEvTestLoadRequest::TUpdateStart& cmd,
        const NActors::TActorId& parent, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, ui64 tag)
{
    return new TKqpUpsertActorMultiSession(cmd, parent, std::move(counters), tag);
}

} // NKikimr::NDataShardLoad
