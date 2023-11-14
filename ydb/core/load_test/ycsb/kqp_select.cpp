#include "actors.h"
#include "common.h"

#include <ydb/core/base/tablet.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/ydb_convert/ydb_convert.h>

#include <ydb/public/lib/operation_id/operation_id.h>
#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <google/protobuf/text_format.h>

#include <random>

// * Scheme is hardcoded and it is like default YCSB setup:
// 1 Text "id" column, 10 Bytes "field0" - "field9" columns
// * row is ~ 1 KB, keys are like user1000385178204227360

namespace NKikimr::NDataShardLoad {

namespace {

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

TQueryInfo GenerateSelect(const TString& table, const TString& key) {
    TStringStream str;

    str << Sprintf(R"__(
        --!syntax_v1

        DECLARE $key AS Text;

        SELECT * FROM `%s` WHERE id == $key;
    )__", table.c_str());

    NYdb::TParamsBuilder paramsBuilder;
    paramsBuilder.AddParam("$key").Utf8(key).Build();
    auto params = paramsBuilder.Build();

    return TQueryInfo(str.Str(), std::move(params));
}

std::unique_ptr<NKqp::TEvKqp::TEvQueryRequest> GenerateSelectRequest(const TString& db, const TString& table, const TString& key) {
    auto queryInfo = GenerateSelect(table, key);

    auto request = std::make_unique<NKqp::TEvKqp::TEvQueryRequest>();
    request->Record.MutableRequest()->SetKeepSession(true);
    request->Record.MutableRequest()->SetDatabase(db);

    request->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
    request->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
    request->Record.MutableRequest()->SetQuery(queryInfo.Query);

    request->Record.MutableRequest()->MutableQueryCachePolicy()->set_keep_in_cache(true);
    request->Record.MutableRequest()->MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
    request->Record.MutableRequest()->MutableTxControl()->set_commit_tx(true);

    const auto& params = NYdb::TProtoAccessor::GetProtoMap(queryInfo.Params);
    request->Record.MutableRequest()->MutableYdbParameters()->insert(params.begin(), params.end());

    return request;
}

// it's a partial copy-paste from TUpsertActor: logic slightly differs, so that
// it seems better to have copy-paste rather if/else for different loads
class TKqpSelectActor : public TActorBootstrapped<TKqpSelectActor> {
    const NKikimrDataShardLoad::TEvYCSBTestLoadRequest::TTargetShard Target;
    const TActorId Parent;
    const TSubLoadId Id;
    const TString Database;
    const TString TableName;
    const TVector<TString>& Keys;
    const size_t ReadCount;
    const bool Infinite;

    TActorId KqpProxyId;
    TString Session;

    size_t KeysRead = 0;

    std::default_random_engine Rng;

    TInstant StartTs;
    TInstant EndTs;

    size_t Errors = 0;

public:
    TKqpSelectActor(const NKikimrDataShardLoad::TEvYCSBTestLoadRequest::TTargetShard& target,
                    const TActorId& parent,
                    TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
                    const TSubLoadId& id,
                    const TVector<TString>& keys,
                    size_t readCount,
                    bool infinite)
        : Target(target)
        , Parent(parent)
        , Id(id)
        , Database(Target.GetWorkingDir())
        , TableName(Target.GetTableName())
        , Keys(keys)
        , ReadCount(readCount)
        , Infinite(infinite)
    {
        Y_UNUSED(counters);
    }

    void Bootstrap(const TActorContext& ctx) {
        LOG_INFO_S(ctx, NKikimrServices::DS_LOAD_TEST, "TKqpSelectActor# " << Id
            << " Bootstrap called");

        Rng.seed(SelfId().Hash());
        KqpProxyId = NKqp::MakeKqpProxyID(ctx.SelfID.NodeId());

        Become(&TKqpSelectActor::StateFunc);
        CreateSession(ctx);
    }

private:
    void CreateSession(const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "TKqpSelectActor# " << Id
            << " sends event for session creation to proxy: " << KqpProxyId.ToString());

        auto ev = MakeHolder<NKqp::TEvKqp::TEvCreateSessionRequest>();
        ev->Record.MutableRequest()->SetDatabase(Database);
        Send(KqpProxyId, ev.Release());
    }

    void CloseSession(const TActorContext& ctx) {
        if (!Session)
            return;

        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "TKqpSelectActor# " << Id
            << " sends session close query to proxy: " << KqpProxyId);

        auto ev = MakeHolder<NKqp::TEvKqp::TEvCloseSessionRequest>();
        ev->Record.MutableRequest()->SetSessionId(Session);
        ctx.Send(KqpProxyId, ev.Release());
    }

    void ReadRow(const TActorContext &ctx) {
        auto index = Rng() % Keys.size();
        const auto& key = Keys[index];

        auto request = GenerateSelectRequest(Database, TableName, key);
        request->Record.MutableRequest()->SetSessionId(Session);

        LOG_TRACE_S(ctx, NKikimrServices::DS_LOAD_TEST, "TKqpSelectActor# " << Id
            << " send request# " << KeysRead
            << " to proxy# " << KqpProxyId << ": " << request->ToString());

        ctx.Send(KqpProxyId, request.release());
        ++KeysRead;
    }

    void OnRequestDone(const TActorContext& ctx) {
        if (Infinite && KeysRead == ReadCount) {
            KeysRead = 0;
        }

        if (KeysRead < ReadCount) {
            ReadRow(ctx);
        } else {
            EndTs = TInstant::Now();
            auto delta = EndTs - StartTs;

            auto response = std::make_unique<TEvDataShardLoad::TEvTestLoadFinished>(Id.SubTag);
            auto& report = *response->Record.MutableReport();
            report.SetTag(Id.SubTag);
            report.SetDurationMs(delta.MilliSeconds());
            report.SetOperationsOK(ReadCount - Errors);
            report.SetOperationsError(Errors);

            ctx.Send(Parent, response.release());

            LOG_NOTICE_S(ctx, NKikimrServices::DS_LOAD_TEST, "TKqpSelectActor# " << Id
                << " finished in " << delta << ", errors=" << Errors);
            Die(ctx);
        }
    }

    void HandlePoison(const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "TKqpSelectActor# " << Id
            << " tablet recieved PoisonPill, going to die");
        CloseSession(ctx);
        Die(ctx);
    }

    void Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const TActorContext& ctx) {
        auto& response = ev->Get()->Record;

        if (response.GetYdbStatus() == Ydb::StatusIds_StatusCode_SUCCESS) {
            Session = response.GetResponse().GetSessionId();
            LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "TKqpSelectActor# " << Id << " session: " << Session);
            StartTs = TInstant::Now();
            ReadRow(ctx);
        } else {
            StopWithError(ctx, "failed to create session: " + ev->Get()->ToString());
        }
    }

    void Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
        LOG_TRACE_S(ctx, NKikimrServices::DS_LOAD_TEST, "TKqpSelectActor# " << Id
            << " received from " << ev->Sender << ": " << ev->Get()->Record.DebugString());

        auto& response = ev->Get()->Record.GetRef();
        if (response.GetYdbStatus() != Ydb::StatusIds_StatusCode_SUCCESS) {
            ++Errors;
        }

        OnRequestDone(ctx);
    }

    void StopWithError(const TActorContext& ctx, const TString& reason) {
        LOG_WARN_S(ctx, NKikimrServices::DS_LOAD_TEST, "Load tablet stopped with error: " << reason);
        ctx.Send(Parent, new TEvDataShardLoad::TEvTestLoadFinished(Id.SubTag, reason));
        Die(ctx);
    }

    void Handle(TEvDataShardLoad::TEvTestLoadInfoRequest::TPtr& ev, const TActorContext& ctx) {
        TStringStream str;
        HTML(str) {
            str << "TKqpSelectActor# " << Id << " started on " << StartTs
                << " sent " << KeysRead << " out of " << ReadCount;
            TInstant ts = EndTs ? EndTs : TInstant::Now();
            auto delta = ts - StartTs;
            auto throughput = ReadCount * 1000 / (delta.MilliSeconds() ? delta.MilliSeconds() : 1);
            str << " in " << delta << " (" << throughput << " op/s)"
                << " errors=" << Errors;
        }

        ctx.Send(ev->Sender, new TEvDataShardLoad::TEvTestLoadInfoResponse(Id.SubTag, str.Str()));
    }

    STRICT_STFUNC(StateFunc,
        CFunc(TEvents::TSystem::PoisonPill, HandlePoison)
        HFunc(TEvDataShardLoad::TEvTestLoadInfoRequest, Handle)
        HFunc(NKqp::TEvKqp::TEvQueryResponse, Handle)
        HFunc(NKqp::TEvKqp::TEvCreateSessionResponse, Handle)
    )
};

// creates multiple TKqpSelectActor for inflight > 1 and waits completion
class TKqpSelectActorMultiSession : public TActorBootstrapped<TKqpSelectActorMultiSession> {
    const NKikimrDataShardLoad::TEvYCSBTestLoadRequest::TReadStart Config;
    const NKikimrDataShardLoad::TEvYCSBTestLoadRequest::TTargetShard Target;
    const TActorId Parent;
    const TSubLoadId Id;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    const TString Database;

    TString ConfingString;

    ui64 ReadCount = 0;

    ui64 TabletId = 0;
    ui64 TableId = 0;
    ui64 OwnerId = 0;

    TVector<ui32> KeyColumnIds;
    TVector<ui32> AllColumnIds;

    TVector<TString> Keys;

    ui64 LastReadId = 0;
    ui64 LastSubTag = 0;
    TVector<TActorId> Actors;

    size_t Inflight = 0;

    TInstant StartTs;
    TInstant EndTs;

    size_t Oks = 0;
    size_t Errors = 0;

public:
    TKqpSelectActorMultiSession(const NKikimrDataShardLoad::TEvYCSBTestLoadRequest::TReadStart& cmd,
                                const NKikimrDataShardLoad::TEvYCSBTestLoadRequest::TTargetShard& target,
                                const TActorId& parent,
                                TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
                                const TSubLoadId& id)
        : Config(cmd)
        , Target(target)
        , Parent(parent)
        , Id(id)
        , Counters(counters)
        , Database(target.GetWorkingDir())
    {
        Y_UNUSED(counters);
        google::protobuf::TextFormat::PrintToString(cmd, &ConfingString);

        if (Config.HasReadCount()) {
            ReadCount = Config.GetReadCount();
        } else {
            ReadCount = Config.GetRowCount();
        }
    }

    void Bootstrap(const TActorContext& ctx) {
        LOG_NOTICE_S(ctx, NKikimrServices::DS_LOAD_TEST, "TKqpSelectActorMultiSession# " << Id
            << " Bootstrap called: " << ConfingString);

        Become(&TKqpSelectActorMultiSession::StateFunc);
        DescribePath(ctx);
    }

private:
    void DescribePath(const TActorContext& ctx) {
        TString path = Target.GetWorkingDir() + "/" + Target.GetTableName();
        auto request = std::make_unique<TEvTxUserProxy::TEvNavigate>();
        request->Record.MutableDescribePath()->SetPath(path);
        ctx.Send(MakeTxProxyID(), request.release());
    }

    void Handle(const NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->GetRecord();
        OwnerId = record.GetPathOwnerId();
        TableId = record.GetPathId();

        const auto& description = record.GetPathDescription();
        if (description.TablePartitionsSize() != 1) {
            return StopWithError(
                ctx,
                TStringBuilder() << "Path must have exactly 1 part, has: " << description.TablePartitionsSize());
        }
        const auto& partition = description.GetTablePartitions(0);
        TabletId = partition.GetDatashardId();

        const auto& table = description.GetTable();

        KeyColumnIds.reserve(table.KeyColumnIdsSize());
        for (const auto& id: table.GetKeyColumnIds()) {
            KeyColumnIds.push_back(id);
        }

        AllColumnIds.reserve(table.ColumnsSize());
        for (const auto& column: table.GetColumns()) {
            AllColumnIds.push_back(column.GetId());
        }

        LOG_INFO_S(ctx, NKikimrServices::DS_LOAD_TEST, "TKqpSelectActorMultiSession# " << Id
            << " will work with tablet# " << TabletId << " with ownerId# " << OwnerId
            << " with tableId# " << TableId << " resolved for path# "
            << Target.GetWorkingDir() << "/" << Target.GetTableName()
            << " with columnsCount# " << AllColumnIds.size() << ", keyColumnCount# " << KeyColumnIds.size());

        RunFullScan(ctx, Config.GetRowCount());
    }

    void RunFullScan(const TActorContext& ctx, ui64 sampleKeys) {
        auto request = std::make_unique<TEvDataShard::TEvRead>();
        auto& record = request->Record;

        record.SetReadId(++LastReadId);
        record.MutableTableId()->SetOwnerId(OwnerId);
        record.MutableTableId()->SetTableId(TableId);

        if (sampleKeys) {
            for (const auto& id: KeyColumnIds) {
                record.AddColumns(id);
            }
        } else {
            for (const auto& id: AllColumnIds) {
                record.AddColumns(id);
            }
        }

        TVector<TString> from = {TString("user")};
        TVector<TString> to = {TString("zzz")};
        AddRangeQuery(*request, from, true, to, true);

        record.SetResultFormat(::NKikimrDataEvents::FORMAT_CELLVEC);

        TSubLoadId subId(Id.Tag, SelfId(), ++LastSubTag);
        auto* actor = CreateReadIteratorScan(request.release(), TabletId, SelfId(), subId, sampleKeys);
        Actors.emplace_back(ctx.Register(actor));

        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "TKqpSelectActorMultiSession# " << Id
            << " started fullscan actor# " << Actors.back());
    }

    void Handle(TEvPrivate::TEvKeys::TPtr& ev, const TActorContext& ctx) {
        const auto& keyCells = ev->Get()->Keys;
        if (keyCells.size() == 0) {
            return StopWithError(ctx, "Failed to read keys or no keys");
        }

        Keys.reserve(keyCells.size());
        for (const auto& keyCell: keyCells) {
            TStringBuf keyBuf = keyCell[0].AsBuf();
            Keys.emplace_back(keyBuf.data(), keyBuf.size());
        }

        LOG_INFO_S(ctx, NKikimrServices::DS_LOAD_TEST, "TKqpSelectActorMultiSession# " << Id
            << " received keyCount# " << Keys.size());

        StartActors(ctx);
    }

    void StartActors(const TActorContext& ctx) {
        Inflight = Config.GetInflights(0);

        TVector<TRequestsVector> perActorRequests;
        perActorRequests.reserve(Inflight);

        StartTs = TInstant::Now();

        Actors.reserve(Inflight);
        for (size_t i = 0; i < Inflight; ++i) {
            TSubLoadId subId(Id.Tag, SelfId(), ++LastSubTag);

            auto* kqpActor = new TKqpSelectActor(
                Target,
                SelfId(),
                Counters,
                subId,
                Keys,
                ReadCount,
                Config.GetInfinite());
            Actors.emplace_back(ctx.Register(kqpActor));
        }

        LOG_NOTICE_S(ctx, NKikimrServices::DS_LOAD_TEST, "TKqpSelectActorMultiSession# " << Id
            << " started# " << Inflight << " actors each with inflight# 1");
    }

    void Handle(const TEvDataShardLoad::TEvTestLoadFinished::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record;
        if (record.HasErrorReason() || !record.HasReport()) {
            TStringStream ss;
            ss << "kqp actor# " << record.GetTag() << " finished with error: " << record.GetErrorReason();
            if (record.HasReport())
                ss << ", report: " << ev->Get()->ToString();

            StopWithError(ctx, ss.Str());
            return;
        }

        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "TKqpSelectActorMultiSession# " << Id
            << " finished: " << ev->Get()->ToString());

        Errors += record.GetReport().GetOperationsError();
        Oks += record.GetReport().GetOperationsOK();

        --Inflight;
        if (Inflight == 0) {
            EndTs = TInstant::Now();
            auto delta = EndTs - StartTs;

            auto response = std::make_unique<TEvDataShardLoad::TEvTestLoadFinished>(Id.SubTag);
            auto& report = *response->Record.MutableReport();
            report.SetTag(Id.SubTag);
            report.SetDurationMs(delta.MilliSeconds());
            report.SetOperationsOK(Oks);
            report.SetOperationsError(Errors);
            ctx.Send(Parent, response.release());

            LOG_NOTICE_S(ctx, NKikimrServices::DS_LOAD_TEST, "TKqpSelectActorMultiSession# " << Id
                << " finished in " << delta << ", oks# " << Oks << ", errors# " << Errors);

            Stop(ctx);
        }
    }

    void Handle(TEvDataShardLoad::TEvTestLoadInfoRequest::TPtr& ev, const TActorContext& ctx) {
        TStringStream str;
        HTML(str) {
            str << "TKqpSelectActorMultiSession# " << Id << " started on " << StartTs;
        }
        ctx.Send(ev->Sender, new TEvDataShardLoad::TEvTestLoadInfoResponse(Id.SubTag, str.Str()));
    }

    void HandlePoison(const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "TKqpSelectActorMultiSession# " << Id
            << " tablet recieved PoisonPill, going to die");
        Stop(ctx);
    }

    void StopWithError(const TActorContext& ctx, const TString& reason) {
        LOG_WARN_S(ctx, NKikimrServices::DS_LOAD_TEST, "TKqpSelectActorMultiSession# " << Id
            << " stopped with error: " << reason);

        ctx.Send(Parent, new TEvDataShardLoad::TEvTestLoadFinished(Id.SubTag, reason));
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
        HFunc(TEvDataShardLoad::TEvTestLoadInfoRequest, Handle)
        HFunc(TEvDataShardLoad::TEvTestLoadFinished, Handle);
        HFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, Handle)
        HFunc(TEvPrivate::TEvKeys, Handle)
    )
};

} // anonymous

IActor *CreateKqpSelectActor(
    const NKikimrDataShardLoad::TEvYCSBTestLoadRequest::TReadStart& cmd,
    const NKikimrDataShardLoad::TEvYCSBTestLoadRequest::TTargetShard& target,
    const TActorId& parent,
    TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
    const TSubLoadId& id)
{
    return new TKqpSelectActorMultiSession(cmd, target, parent, std::move(counters), id);
}

} // NKikimr::NDataShardLoad
