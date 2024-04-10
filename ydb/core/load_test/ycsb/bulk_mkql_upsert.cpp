#include "actors.h"
#include "common.h"

#include <ydb/core/base/tablet.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/protos/tx_proxy.pb.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/datetime/cputimer.h>
#include <util/random/random.h>

#include <google/protobuf/text_format.h>

// * Scheme is hardcoded and it is like default YCSB setup:
// 1 Text "id" column, 10 Bytes "field0" - "field9" columns
// * row is ~ 1 KB, keys are like user1000385178204227360

namespace NKikimr::NDataShardLoad {

using TUploadRowsRequestPtr = std::unique_ptr<TEvDataShard::TEvUploadRowsRequest>;

namespace {

const ui64 RECONNECT_LIMIT = 10;

enum class ERequestType {
    UpsertBulk,
    UpsertLocalMkql,
};

TUploadRequest GenerateBulkRowRequest(ui64 tableId, ui64 keyStart, ui64 n) {
    TUploadRowsRequestPtr request(new TEvDataShard::TEvUploadRowsRequest());
    auto& record = request->Record;
    record.SetTableId(tableId);

    auto& rowScheme = *record.MutableRowScheme();
    for (size_t i = 2; i <= 11; ++i) {
        rowScheme.AddValueColumnIds(i);
    }
    rowScheme.AddKeyColumnIds(1);

    for (size_t i = keyStart; i < keyStart + n; ++i) {
        TVector<TCell> keys;
        keys.reserve(1);
        TString key = GetKey(i);
        keys.emplace_back(key.data(), key.size());

        TVector<TCell> values;
        values.reserve(10);
        for (size_t i = 2; i <= 11; ++i) {
            values.emplace_back(Value.data(), Value.size());
        }

        auto& row = *record.AddRows();
        row.SetKeyColumns(TSerializedCellVec::Serialize(keys));
        row.SetValueColumns(TSerializedCellVec::Serialize(values));
    }

    return TUploadRequest(request.release());
}

TUploadRequest GenerateMkqlRowRequest(ui64 /* tableId */, ui64 keyNum, const TString& table) {
    TString programWithoutKey;

    TString fields;
    for (size_t i = 0; i < 10; ++i) {
        fields += Sprintf("'('field%lu (String '%s))", i, Value.data());
    }
    TString rowUpd = "(let upd_ '(" + fields + "))";

    programWithoutKey = rowUpd;

    programWithoutKey += Sprintf(R"(
        (let ret_ (AsList
            (UpdateRow '__user__%s row1_ upd_
        )))
        (return ret_)
    ))", table.c_str());

    TString key = GetKey(keyNum);

    auto programText = Sprintf(R"((
        (let row1_ '('('id (Utf8 '%s))))
    )", key.data()) + programWithoutKey;

    auto request = std::make_unique<TEvTablet::TEvLocalMKQL>();
    request->Record.MutableProgram()->MutableProgram()->SetText(programText);

    return TUploadRequest(request.release());
}

TUploadRequest GenerateRequest(
    ui64 tableId,
    ui64 keyFrom,
    ui64 batchSize, // only bulk requests, otherwise 1
    ERequestType requestType,
    const TString& table)
{
    switch (requestType) {
    case ERequestType::UpsertBulk:
        return GenerateBulkRowRequest(tableId, keyFrom, batchSize);
        break;
    case ERequestType::UpsertLocalMkql:
        return GenerateMkqlRowRequest(tableId, keyFrom, table);
        break;
    default:
        // should not happen, just for compiler
        Y_ABORT("Unsupported request type");
    }
}

class TUpsertActor : public TActorBootstrapped<TUpsertActor> {
    const NKikimrDataShardLoad::TEvYCSBTestLoadRequest::TUpdateStart Config;
    const NKikimrDataShardLoad::TEvYCSBTestLoadRequest::TTargetShard Target;
    const TActorId Parent;
    const TSubLoadId Id;
    const ERequestType RequestType;
    TString ConfingString;

    TActorId Pipe;
    bool WasConnected = false;
    ui64 ReconnectLimit = RECONNECT_LIMIT;

    size_t CurrentRow = 0;
    size_t Inflight = 0;

    TInstant StartTs;
    TInstant EndTs;

    size_t Errors = 0;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::DS_LOAD_ACTOR;
    }

    TUpsertActor(const NKikimrDataShardLoad::TEvYCSBTestLoadRequest::TUpdateStart& cmd,
                 const NKikimrDataShardLoad::TEvYCSBTestLoadRequest::TTargetShard& target,
                 const TActorId& parent,
                 TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
                 const TSubLoadId& id,
                 ERequestType requestType)
        : Config(cmd)
        , Target(target)
        , Parent(parent)
        , Id(id)
        , RequestType(requestType)
    {
        Y_UNUSED(counters);
        google::protobuf::TextFormat::PrintToString(cmd, &ConfingString);
    }

    void Bootstrap(const TActorContext& ctx) {
        LOG_NOTICE_S(ctx, NKikimrServices::DS_LOAD_TEST, "Id# " << Id
            << " TUpsertActor Bootstrap called: " << ConfingString << " with type# " << int(RequestType)
            << ", target# " << Target.DebugString());

        Become(&TUpsertActor::StateFunc);
        Connect(ctx);
    }

private:
    void Connect(const TActorContext &ctx) {
        if (ReconnectLimit != RECONNECT_LIMIT) {
            LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "TUpsertActor# " << Id
                << " will reconnect to tablet# " << Target.GetTabletId()
                << " retries left# " << (ReconnectLimit - 1));
        } else {
            LOG_TRACE_S(ctx, NKikimrServices::DS_LOAD_TEST, "TUpsertActor# " << Id
                << " will connect to tablet# " << Target.GetTabletId());
        }

        --ReconnectLimit;
        if (ReconnectLimit == 0) {
            TStringStream ss;
            ss << "Failed to set pipe to " << Target.GetTabletId();
            return StopWithError(ctx, ss.Str());
        }
        Pipe = Register(NTabletPipe::CreateClient(SelfId(), Target.GetTabletId()));
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr ev, const TActorContext& ctx) {
        TEvTabletPipe::TEvClientConnected *msg = ev->Get();

        LOG_TRACE_S(ctx, NKikimrServices::DS_LOAD_TEST, "Id# " << Id
            << " TUpsertActor Handle TEvClientConnected called, Status# " << msg->Status);

        if (msg->Status != NKikimrProto::OK) {
            Pipe = {};
            return Connect(ctx);
        }

        StartTs = TInstant::Now();
        WasConnected = true;
        SendRows(ctx);
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr, const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "Id# " << Id
            << " TUpsertActor Handle TEvClientDestroyed called");

        // sanity check
        if (!WasConnected) {
            return Connect(ctx);
        }

        return StopWithError(ctx, "broken pipe");
    }

    void SendRows(const TActorContext &ctx) {
        while (Inflight < Config.GetInflight() && CurrentRow < Config.GetRowCount()) {
            auto rowsLest = Config.GetRowCount() - CurrentRow;
            auto batchSize = Min(size_t{Config.GetBatchSize()}, rowsLest);

            auto request = GenerateRequest(
                Target.GetTableId(),
                Config.GetKeyFrom() + CurrentRow,
                batchSize,
                RequestType,
                Target.GetTableName());

            LOG_TRACE_S(ctx, NKikimrServices::DS_LOAD_TEST, "TUpsertActor# " << Id
                << " sends request# " << CurrentRow << " with batchSize# " << batchSize
                <<  ": " << request->ToString());

            NTabletPipe::SendData(ctx, Pipe, request.release());

            CurrentRow += batchSize;
            ++Inflight;
        }
    }

    void OnRequestDone(const TActorContext& ctx) {
        if (Config.GetInfinite() && CurrentRow >= Config.GetRowCount()) {
            CurrentRow = 0;
        }

        if (CurrentRow < Config.GetRowCount()) {
            SendRows(ctx);
        } else if (Inflight == 0) {
            EndTs = TInstant::Now();
            auto delta = EndTs - StartTs;

            auto response = std::make_unique<TEvDataShardLoad::TEvTestLoadFinished>(Id.SubTag);
            auto& report = *response->Record.MutableReport();
            report.SetTag(Id.SubTag);
            report.SetDurationMs(delta.MilliSeconds());
            report.SetOperationsOK(Config.GetRowCount() - Errors);
            report.SetOperationsError(Errors);

            ctx.Send(Parent, response.release());

            LOG_NOTICE_S(ctx, NKikimrServices::DS_LOAD_TEST, "Id# " << Id
                << " TUpsertActor finished in " << delta << ", errors=" << Errors);
            Die(ctx);
        }
    }

    void Handle(TEvDataShard::TEvUploadRowsResponse::TPtr ev, const TActorContext& ctx) {
        LOG_TRACE_S(ctx, NKikimrServices::DS_LOAD_TEST, "Id# " << Id
            << " TUpsertActor received from " << ev->Sender << ": " << ev->Get()->Record);
        --Inflight;

        TEvDataShard::TEvUploadRowsResponse *msg = ev->Get();
        if (msg->Record.GetStatus() != 0) {
            ++Errors;
            LOG_WARN_S(ctx, NKikimrServices::DS_LOAD_TEST, "Id# " << Id
                << " TUpsertActor TEvUploadRowsResponse: " << msg->ToString());
        }

        OnRequestDone(ctx);
    }

    void Handle(TEvTablet::TEvLocalMKQLResponse::TPtr ev, const TActorContext& ctx) {
        LOG_TRACE_S(ctx, NKikimrServices::DS_LOAD_TEST, "Id# " << Id
            << " TUpsertActor received from " << ev->Sender << ": " << ev->Get()->Record);
        --Inflight;

        TEvTablet::TEvLocalMKQLResponse *msg = ev->Get();
        if (msg->Record.GetStatus() != 0) {
            ++Errors;
            LOG_WARN_S(ctx, NKikimrServices::DS_LOAD_TEST, "Id# " << Id
                << " TUpsertActor TEvLocalMKQLResponse: " << msg->ToString());
        }

        OnRequestDone(ctx);
    }

    void Handle(TEvents::TEvUndelivered::TPtr, const TActorContext& ctx) {
        StopWithError(ctx, "delivery failed");
    }

    void Handle(TEvDataShardLoad::TEvTestLoadInfoRequest::TPtr& ev, const TActorContext& ctx) {
        TStringStream str;
        HTML(str) {
            str << "DS bulk upsert load actor# " << Id << " started on " << StartTs
                << " sent " << CurrentRow << " out of " << Config.GetRowCount();
            TInstant ts = EndTs ? EndTs : TInstant::Now();
            auto delta = ts - StartTs;
            auto throughput = Config.GetRowCount() * 1000 / (delta.MilliSeconds() ? delta.MilliSeconds() : 1);
            str << " in " << delta << " (" << throughput << " op/s)"
                << " errors=" << Errors;
        }

        ctx.Send(ev->Sender, new TEvDataShardLoad::TEvTestLoadInfoResponse(Id.SubTag, str.Str()));
    }

    void HandlePoison(const TActorContext& ctx) {
        LOG_INFO_S(ctx, NKikimrServices::DS_LOAD_TEST, "Load tablet recieved PoisonPill, going to die");
        NTabletPipe::CloseClient(SelfId(), Pipe);
        Die(ctx);
    }

    void StopWithError(const TActorContext& ctx, const TString& reason) {
        LOG_ERROR_S(ctx, NKikimrServices::DS_LOAD_TEST, "Load tablet stopped with error: " << reason);
        ctx.Send(Parent, new TEvDataShardLoad::TEvTestLoadFinished(Id.SubTag, reason));
        NTabletPipe::CloseClient(SelfId(), Pipe);
        Die(ctx);
    }

    STRICT_STFUNC(StateFunc,
        CFunc(TEvents::TSystem::PoisonPill, HandlePoison);
        HFunc(TEvDataShardLoad::TEvTestLoadInfoRequest, Handle)
        HFunc(TEvents::TEvUndelivered, Handle);
        HFunc(TEvDataShard::TEvUploadRowsResponse, Handle);
        HFunc(TEvTablet::TEvLocalMKQLResponse, Handle);
        HFunc(TEvTabletPipe::TEvClientConnected, Handle);
        HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
    )
};

} // anonymous

IActor *CreateUpsertBulkActor(const NKikimrDataShardLoad::TEvYCSBTestLoadRequest::TUpdateStart& cmd,
        const NKikimrDataShardLoad::TEvYCSBTestLoadRequest::TTargetShard& target,
        const TActorId& parent, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
        const TSubLoadId& id)
{
    return new TUpsertActor(cmd, target, parent, std::move(counters), id, ERequestType::UpsertBulk);
}

IActor *CreateLocalMkqlUpsertActor(const NKikimrDataShardLoad::TEvYCSBTestLoadRequest::TUpdateStart& cmd,
        const NKikimrDataShardLoad::TEvYCSBTestLoadRequest::TTargetShard& target,
        const TActorId& parent, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
        const TSubLoadId& id)
{
    return new TUpsertActor(cmd, target, parent, std::move(counters), id, ERequestType::UpsertLocalMkql);
}

IActor *CreateProposeUpsertActor(const NKikimrDataShardLoad::TEvYCSBTestLoadRequest::TUpdateStart& cmd,
        const NKikimrDataShardLoad::TEvYCSBTestLoadRequest::TTargetShard& target,
        const TActorId& parent, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
        const TSubLoadId& id)
{
    Y_UNUSED(cmd);
    Y_UNUSED(target);
    Y_UNUSED(parent);
    Y_UNUSED(counters);
    Y_UNUSED(id);
    return nullptr; // not yet implemented
}

} // NKikimr::NDataShardLoad
