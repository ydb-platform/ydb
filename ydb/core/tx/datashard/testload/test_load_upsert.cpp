#include "actors.h"

#include <ydb/core/base/tablet.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/kqp/kqp.h>
#include <ydb/core/ydb_convert/ydb_convert.h>

#include <ydb/public/lib/operation_id/operation_id.h>
#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/datetime/cputimer.h>
#include <util/random/random.h>

#include <google/protobuf/text_format.h>

// * Scheme is hardcoded and it is like default YCSB setup:
// table name is "usertable", 1 utf8 "key" column, 10 utf8 "field0" - "field9" columns
// * row is ~ 1 KB, keys are like user1000385178204227360

namespace NKikimr::NDataShardLoad {

using TUploadRowsRequestPtr = std::unique_ptr<TEvDataShard::TEvUploadRowsRequest>;

using TUploadRequest = std::unique_ptr<IEventBase>;
using TRequestsVector = std::vector<TUploadRequest>;

namespace {

enum class ERequestType {
    UpsertBulk,
    UpsertLocalMkql,
};

TString GetKey(size_t n) {
    // user1000385178204227360
    return Sprintf("user%.19lu", n);
}

const TString Value = TString(100, 'x');

void ConvertYdbParamsToMiniKQLParams(const NYdb::TParams& input, NKikimrMiniKQL::TParams& output) {
    output.MutableType()->SetKind(NKikimrMiniKQL::ETypeKind::Struct);
    auto type = output.MutableType()->MutableStruct();
    auto value = output.MutableValue();
    for (const auto& p : input.GetValues()) {
        auto typeMember = type->AddMember();
        auto valueItem = value->AddStruct();
        typeMember->SetName(p.first);
        ConvertYdbTypeToMiniKQLType(NYdb::TProtoAccessor::GetProto(p.second.GetType()), *typeMember->MutableType());
        ConvertYdbValueToMiniKQLValue(NYdb::TProtoAccessor::GetProto(p.second.GetType()), NYdb::TProtoAccessor::GetProto(p.second), *valueItem);
    }
}

TUploadRequest GenerateBulkRowRequest(ui64 tableId, ui64 keyNum) {
    TUploadRowsRequestPtr request(new TEvDataShard::TEvUploadRowsRequest());
    auto& record = request->Record;
    record.SetTableId(tableId);

    auto& rowScheme = *record.MutableRowScheme();
    for (size_t i = 2; i <= 11; ++i) {
        rowScheme.AddValueColumnIds(i);
    }
    rowScheme.AddKeyColumnIds(1);

    TVector<TCell> keys;
    keys.reserve(1);
    TString key = GetKey(keyNum);
    keys.emplace_back(key.data(), key.size());

    TVector<TCell> values;
    values.reserve(10);
    for (size_t i = 2; i <= 11; ++i) {
        values.emplace_back(Value.data(), Value.size());
    }

    auto& row = *record.AddRows();
    row.SetKeyColumns(TSerializedCellVec::Serialize(keys));
    row.SetValueColumns(TSerializedCellVec::Serialize(values));

    return TUploadRequest(request.release());
}

TUploadRequest GenerateMkqlRowRequest(ui64 /* tableId */, ui64 keyNum) {
    static TString programWithoutKey;

    if (!programWithoutKey) {
        TString fields;
        for (size_t i = 0; i < 10; ++i) {
            fields += Sprintf("'('field%lu (Utf8 '%s))", i, Value.data());
        }
        TString rowUpd = "(let upd_ '(" + fields + "))";

        programWithoutKey = rowUpd;

        programWithoutKey += R"(
            (let ret_ (AsList
                (UpdateRow '__user__usertable row1_ upd_
            )))
            (return ret_)
        ))";
    }

    TString key = GetKey(keyNum);

    auto programText = Sprintf(R"((
        (let row1_ '('('key (Utf8 '%s))))
    )", key.data()) + programWithoutKey;

    auto request = std::make_unique<TEvTablet::TEvLocalMKQL>();
    request->Record.MutableProgram()->MutableProgram()->SetText(programText);

    return TUploadRequest(request.release());
}

TRequestsVector GenerateRequests(ui64 tableId, ui64 n, ERequestType requestType) {
    TRequestsVector requests;
    requests.reserve(n);

    for (size_t i = 0; i < n; ++i) {
        auto keyNum = RandomNumber(Max<ui64>());
        switch (requestType) {
        case ERequestType::UpsertBulk:
            requests.emplace_back(GenerateBulkRowRequest(tableId, keyNum));
            break;
        case ERequestType::UpsertLocalMkql:
            requests.emplace_back(GenerateMkqlRowRequest(tableId, keyNum));
            break;
        default:
            // should not happen, just for compiler
            Y_FAIL("Unsupported request type");
        }
    }

    return requests;
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
        PRAGMA Kikimr.UseNewEngine = 'true';

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

} // anonymous

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



class TUpsertActor : public TActorBootstrapped<TUpsertActor> {
    const NKikimrDataShardLoad::TEvTestLoadRequest::TUpdateStart Config;
    const TActorId Parent;
    const ui64 Tag;
    const ERequestType RequestType;
    TString ConfingString;

    TActorId Pipe;

    TRequestsVector Requests;
    size_t CurrentRequest = 0;
    size_t Inflight = 0;

    TInstant StartTs;
    TInstant EndTs;

    size_t Errors = 0;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::DS_LOAD_ACTOR;
    }

    TUpsertActor(const NKikimrDataShardLoad::TEvTestLoadRequest::TUpdateStart& cmd, const TActorId& parent,
            TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, ui64 tag, ERequestType requestType)
        : Config(cmd)
        , Parent(parent)
        , Tag(tag)
        , RequestType(requestType)
    {
        Y_UNUSED(counters);
        google::protobuf::TextFormat::PrintToString(cmd, &ConfingString);
    }

    void Bootstrap(const TActorContext& ctx) {
        LOG_NOTICE_S(ctx, NKikimrServices::DS_LOAD_TEST, "Tag# " << Tag
            << " TUpsertActor Bootstrap called: " << ConfingString);

        // note that we generate all requests at once to send at max speed, i.e.
        // do not mess with protobufs, strings, etc when send data
        Requests = GenerateRequests(Config.GetTableId(), Config.GetRowCount(), RequestType);

        Become(&TUpsertActor::StateFunc);
        Connect(ctx);
    }

private:
    void Connect(const TActorContext &ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "Tag# " << Tag << " TUpsertActor Connect called");
        Pipe = Register(NTabletPipe::CreateClient(SelfId(), Config.GetTabletId()));
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr ev, const TActorContext& ctx) {
        TEvTabletPipe::TEvClientConnected *msg = ev->Get();

        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "Tag# " << Tag
            << " TUpsertActor Handle TEvClientConnected called, Status# " << msg->Status);

        if (msg->Status != NKikimrProto::OK) {
            TStringStream ss;
            ss << "Failed to connect to " << Config.GetTabletId() << ", status: " << msg->Status;
            StopWithError(ctx, ss.Str());
            return;
        }

        StartTs = TInstant::Now();
        SendRows(ctx);
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr, const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "Tag# " << Tag
            << " TUpsertActor Handle TEvClientDestroyed called");
        StopWithError(ctx, "broken pipe");
    }

    void SendRows(const TActorContext &ctx) {
        while (Inflight < Config.GetInflight() && CurrentRequest < Requests.size()) {
            const auto* request = Requests[CurrentRequest].get();
            LOG_TRACE_S(ctx, NKikimrServices::DS_LOAD_TEST, "Tag# " << Tag
                << "TUpsertActor# " << Tag << " send request# " << CurrentRequest << ": " << request->ToString());
            NTabletPipe::SendData(ctx, Pipe, Requests[CurrentRequest].release());
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

            LOG_NOTICE_S(ctx, NKikimrServices::DS_LOAD_TEST, "Tag# " << Tag
                << " TUpsertActor finished in " << delta << ", errors=" << Errors);
            Die(ctx);
        }
    }

    void Handle(TEvDataShard::TEvUploadRowsResponse::TPtr ev, const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "Tag# " << Tag
            << " TUpsertActor received from " << ev->Sender << ": " << ev->Get()->Record);
        --Inflight;

        TEvDataShard::TEvUploadRowsResponse *msg = ev->Get();
        if (msg->Record.GetStatus() != 0) {
            ++Errors;
            LOG_WARN_S(ctx, NKikimrServices::DS_LOAD_TEST, "Tag# " << Tag
                << " TUpsertActor TEvUploadRowsResponse: " << msg->ToString());
        }

        OnRequestDone(ctx);
    }

    void Handle(TEvTablet::TEvLocalMKQLResponse::TPtr ev, const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "Tag# " << Tag
            << " TUpsertActor received from " << ev->Sender << ": " << ev->Get()->Record);
        --Inflight;

        TEvTablet::TEvLocalMKQLResponse *msg = ev->Get();
        if (msg->Record.GetStatus() != 0) {
            ++Errors;
            LOG_WARN_S(ctx, NKikimrServices::DS_LOAD_TEST, "Tag# " << Tag
                << " TUpsertActor TEvLocalMKQLResponse: " << msg->ToString());
        }

        OnRequestDone(ctx);
    }

    void Handle(TEvents::TEvUndelivered::TPtr, const TActorContext& ctx) {
        StopWithError(ctx, "delivery failed");
    }

    void Handle(NMon::TEvHttpInfo::TPtr& ev, const TActorContext& ctx) {
        TStringStream str;
        HTML(str) {
            str << "DS bulk upsert load actor# " << Tag << " started on " << StartTs
                << " sent " << CurrentRequest << " out of " << Requests.size();
            TInstant ts = EndTs ? EndTs : TInstant::Now();
            auto delta = ts - StartTs;
            auto throughput = Requests.size() / delta.Seconds();
            str << " in " << delta << " (" << throughput << " op/s)"
                << " errors=" << Errors;
        }
        ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str(), ev->Get()->SubRequestId));
    }

    void HandlePoison(const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "Load tablet recieved PoisonPill, going to die");
        NTabletPipe::CloseClient(SelfId(), Pipe);
        Die(ctx);
    }

    void StopWithError(const TActorContext& ctx, const TString& reason) {
        LOG_NOTICE_S(ctx, NKikimrServices::DS_LOAD_TEST, "Load tablet stopped with error: " << reason);
        ctx.Send(Parent, new TEvDataShardLoad::TEvTestLoadFinished(Tag, reason));
        NTabletPipe::CloseClient(SelfId(), Pipe);
        Die(ctx);
    }

    STRICT_STFUNC(StateFunc,
        CFunc(TEvents::TSystem::PoisonPill, HandlePoison);
        HFunc(NMon::TEvHttpInfo, Handle)
        HFunc(TEvents::TEvUndelivered, Handle);
        HFunc(TEvDataShard::TEvUploadRowsResponse, Handle);
        HFunc(TEvTablet::TEvLocalMKQLResponse, Handle);
        HFunc(TEvTabletPipe::TEvClientConnected, Handle);
        HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
    )
};

NActors::IActor *CreateUpsertBulkActor(const NKikimrDataShardLoad::TEvTestLoadRequest::TUpdateStart& cmd,
        const NActors::TActorId& parent, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, ui64 tag)
{
    return new TUpsertActor(cmd, parent, std::move(counters), tag, ERequestType::UpsertBulk);
}

NActors::IActor *CreateLocalMkqlUpsertActor(const NKikimrDataShardLoad::TEvTestLoadRequest::TUpdateStart& cmd,
        const NActors::TActorId& parent, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, ui64 tag)
{
    return new TUpsertActor(cmd, parent, std::move(counters), tag, ERequestType::UpsertLocalMkql);
}

NActors::IActor *CreateKqpUpsertActor(const NKikimrDataShardLoad::TEvTestLoadRequest::TUpdateStart& cmd,
        const NActors::TActorId& parent, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, ui64 tag)
{
    return new TKqpUpsertActorMultiSession(cmd, parent, std::move(counters), tag);
}

NActors::IActor *CreateProposeUpsertActor(const NKikimrDataShardLoad::TEvTestLoadRequest::TUpdateStart& cmd,
        const NActors::TActorId& parent, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, ui64 tag)
{
    Y_UNUSED(cmd);
    Y_UNUSED(parent);
    Y_UNUSED(counters);
    Y_UNUSED(tag);
    return nullptr; // not yet implemented
}

} // NKikimr::NDataShardLoad
