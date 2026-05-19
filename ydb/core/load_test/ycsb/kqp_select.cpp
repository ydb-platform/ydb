#include "actors.h"
#include "common.h"

#include <ydb/core/base/tablet.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/ydb_convert/ydb_convert.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/operation_id/operation_id.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/params/params.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/params/params.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <google/protobuf/text_format.h>

#include <random>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::DS_LOAD_TEST

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
        YDB_LOG_CTX_INFO(ctx, "Bootstrap called",
            {"TKqpSelectActor", Id});

        Rng.seed(SelfId().Hash());
        KqpProxyId = NKqp::MakeKqpProxyID(ctx.SelfID.NodeId());

        Become(&TKqpSelectActor::StateFunc);
        CreateSession(ctx);
    }

private:
    void CreateSession(const TActorContext& ctx) {
        YDB_LOG_CTX_DEBUG(ctx, "sends event for session creation",
            {"TKqpSelectActor", Id},
            {"to_proxy", KqpProxyId.ToString()});

        auto ev = MakeHolder<NKqp::TEvKqp::TEvCreateSessionRequest>();
        ev->Record.MutableRequest()->SetDatabase(Database);
        Send(KqpProxyId, ev.Release());
    }

    void CloseSession(const TActorContext& ctx) {
        if (!Session)
            return;

        YDB_LOG_CTX_DEBUG(ctx, "sends session close query",
            {"TKqpSelectActor", Id},
            {"to_proxy", KqpProxyId});

        auto ev = MakeHolder<NKqp::TEvKqp::TEvCloseSessionRequest>();
        ev->Record.MutableRequest()->SetSessionId(Session);
        ctx.Send(KqpProxyId, ev.Release());
    }

    void ReadRow(const TActorContext &ctx) {
        auto index = Rng() % Keys.size();
        const auto& key = Keys[index];

        auto request = GenerateSelectRequest(Database, TableName, key);
        request->Record.MutableRequest()->SetSessionId(Session);

        YDB_LOG_CTX_TRACE(ctx, "send",
            {"TKqpSelectActor", Id},
            {"request", KeysRead},
            {"to_proxy", KqpProxyId},
            {"#_request", request->ToString()});

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

            YDB_LOG_CTX_NOTICE(ctx, "finished in",
                {"TKqpSelectActor", Id},
                {"delta", delta},
                {"errors", Errors});
            Die(ctx);
        }
    }

    void HandlePoison(const TActorContext& ctx) {
        YDB_LOG_CTX_DEBUG(ctx, "tablet received PoisonPill, going to die",
            {"TKqpSelectActor", Id});
        CloseSession(ctx);
        Die(ctx);
    }

    void Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const TActorContext& ctx) {
        auto& response = ev->Get()->Record;

        if (response.GetYdbStatus() == Ydb::StatusIds_StatusCode_SUCCESS) {
            Session = response.GetResponse().GetSessionId();
            YDB_LOG_CTX_DEBUG(ctx, "",
                {"TKqpSelectActor", Id},
                {"session", Session});
            StartTs = TInstant::Now();
            ReadRow(ctx);
        } else {
            StopWithError(ctx, "failed to create session: " + ev->Get()->ToString());
        }
    }

    void Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
        YDB_LOG_CTX_TRACE(ctx, "received from",
            {"TKqpSelectActor", Id},
            {"Sender", ev->Sender},
            {"DebugString", ev->Get()->Record.DebugString()});

        auto& response = ev->Get()->Record;
        if (response.GetYdbStatus() != Ydb::StatusIds_StatusCode_SUCCESS) {
            ++Errors;
        }

        OnRequestDone(ctx);
    }

    void StopWithError(const TActorContext& ctx, const TString& reason) {
        YDB_LOG_CTX_WARN(ctx, "Load tablet stopped with",
            {"error", reason});
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
        YDB_LOG_CTX_NOTICE(ctx, "Bootstrap",
            {"TKqpSelectActorMultiSession", Id},
            {"called", ConfingString});

        Become(&TKqpSelectActorMultiSession::StateFunc);
        DescribePath(ctx);
    }

private:
    void DescribePath(const TActorContext& ctx) {
        TString path = Target.GetWorkingDir() + "/" + Target.GetTableName();
        auto request = std::make_unique<TEvTxUserProxy::TEvNavigate>();
        request->Record.SetDatabaseName(Target.GetWorkingDir());
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

        YDB_LOG_CTX_INFO(ctx, "will work with with with resolved for / with",
            {"TKqpSelectActorMultiSession", Id},
            {"tablet", TabletId},
            {"ownerId", OwnerId},
            {"tableId", TableId},
            {"path", Target.GetWorkingDir()},
            {"GetTableName", Target.GetTableName()},
            {"columnsCount", AllColumnIds.size()},
            {"keyColumnCount", KeyColumnIds.size()});

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

        YDB_LOG_CTX_DEBUG(ctx, "started fullscan",
            {"TKqpSelectActorMultiSession", Id},
            {"actor", Actors.back()});
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

        YDB_LOG_CTX_INFO(ctx, "received",
            {"TKqpSelectActorMultiSession", Id},
            {"keyCount", Keys.size()});

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

        YDB_LOG_CTX_NOTICE(ctx, "actors each with inflight# 1",
            {"TKqpSelectActorMultiSession", Id},
            {"started", Inflight});
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

        YDB_LOG_CTX_DEBUG(ctx, "",
            {"TKqpSelectActorMultiSession", Id},
            {"finished", ev->Get()->ToString()});

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

            YDB_LOG_CTX_NOTICE(ctx, "finished in",
                {"TKqpSelectActorMultiSession", Id},
                {"delta", delta},
                {"oks", Oks},
                {"errors", Errors});

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
        YDB_LOG_CTX_DEBUG(ctx, "tablet received PoisonPill, going to die",
            {"TKqpSelectActorMultiSession", Id});
        Stop(ctx);
    }

    void StopWithError(const TActorContext& ctx, const TString& reason) {
        YDB_LOG_CTX_WARN(ctx, "stopped with",
            {"TKqpSelectActorMultiSession", Id},
            {"error", reason});

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
