#include "test_load_actor.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/core/kqp/kqp.h>
#include <ydb/core/protos/ydb_result_set_old.pb.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/ydb_convert/ydb_convert.h>

#include <ydb/library/workload/workload_factory.h>
#include <ydb/library/workload/stock_workload.h>
#include <ydb/library/workload/kv_workload.h>

#include <ydb/public/lib/operation_id/operation_id.h>
#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/histogram/hdr/histogram.h>

#include <util/generic/queue.h>
#include <util/random/fast.h>
#include <util/random/shuffle.h>


namespace NKikimr {

enum {
    EvKqpWorkerResponse
};

struct MonitoringData {
public:
    MonitoringData()
        : WindowHist(60000, 2)
        , WindowErrors(0) {}

    MonitoringData(const NHdr::THistogram& hist, ui64 window_errors)
        : WindowHist(60000, 2)
        , WindowErrors(window_errors)
    {
        WindowHist.Add(hist);
    }

    void Add(const MonitoringData& other) {
        WindowHist.Add(other.WindowHist);
        WindowErrors += other.WindowErrors;
    }

public:
    NHdr::THistogram WindowHist;
    ui64 WindowErrors;

};

struct TEvKqpWorkerResponse : TEventLocal<TEvKqpWorkerResponse, EvKqpWorkerResponse> {
public:
    TEvKqpWorkerResponse(const NHdr::THistogram& hist, ui64 window_errors, ui64 phase, ui64 worker_tag)
        : Data(hist, window_errors)
        , Phase(phase)
        , WorkerTag(worker_tag) {}

public:
    MonitoringData Data;
    ui64 Phase;
    ui64 WorkerTag;

};

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

class TKqpLoadWorker : public TActorBootstrapped<TKqpLoadWorker> {
public:
    TKqpLoadWorker(TActorId parent,
        TString working_dir,
        std::shared_ptr<NYdbWorkload::IWorkloadQueryGenerator> workload_query_gen,
        ui64 workload_type,
        ui64 parentTag,
        ui64 workerTag,
        ui64 durationSeconds,
        ui64 windowDuration,
        ui64 windowCount,
        NMonitoring::TDynamicCounters::TCounterPtr transactions,
        NMonitoring::TDynamicCounters::TCounterPtr transactionsBytesWritten)
        : Parent(std::move(parent))
        , WorkingDir(std::move(working_dir))
        , WorkloadQueryGen(workload_query_gen)
        , WorkloadType(workload_type)
        , ParentTag(parentTag)
        , WorkerTag(workerTag)
        , DurationSeconds(durationSeconds)
        , WindowHist(60000, 2)
        , WindowDuration(windowDuration)
        , WindowCount(windowCount)
        , Transactions(transactions)
        , TransactionsBytesWritten(transactionsBytesWritten) {}

    void Bootstrap(const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Worker Tag# " << ParentTag << "." << WorkerTag << " TKqpLoadWorker Bootstrap called");

        ctx.Schedule(TDuration::Seconds(DurationSeconds), new TEvents::TEvPoisonPill);
        ctx.Schedule(TDuration::Seconds(WindowDuration), new TEvUpdateMonitoring);

        Become(&TKqpLoadWorker::StateFunc);
        CreateWorkingSession(ctx);
    }

    STRICT_STFUNC(StateFunc,
        CFunc(TEvents::TSystem::PoisonPill, HandlePoisonPill)
        HFunc(NKqp::TEvKqp::TEvQueryResponse, HandleResponse)
        HFunc(NKqp::TEvKqp::TEvCreateSessionResponse, HandleCreateSessionResponse)
        HFunc(TEvUpdateMonitoring, HandleWindowTimer)
    )

private:

    // death

    void HandlePoisonPill(const TActorContext& ctx) {
        LOG_INFO_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Worker Tag# " << ParentTag << "." << WorkerTag << " HandlePoisonPill");

        if (Phase < WindowCount) {
            SendMonitoringEvent(ctx);
        }

        CloseSession(ctx);
        Die(ctx);
    }

    void CloseSession(const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Worker Tag# " << ParentTag << "." << WorkerTag << " creating event for session close");

        auto ev = MakeHolder<NKqp::TEvKqp::TEvCloseSessionRequest>();
        ev->Record.MutableRequest()->SetSessionId(WorkerSession);

        auto kqp_proxy = NKqp::MakeKqpProxyID(ctx.SelfID.NodeId());
        LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Worker Tag# " << ParentTag << "." << WorkerTag
            << " sending session close query to proxy: " + kqp_proxy.ToString());

        ctx.Send(kqp_proxy, ev.Release());
    }

private:

    // working

    void CreateWorkingSession(const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Worker Tag# " << ParentTag << "." << WorkerTag << " creating event for session creation");
        auto ev = MakeHolder<NKqp::TEvKqp::TEvCreateSessionRequest>();

        ev->Record.MutableRequest()->SetDatabase(WorkingDir);

        auto kqp_proxy = NKqp::MakeKqpProxyID(ctx.SelfID.NodeId());
        LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Worker Tag# " << ParentTag << "." << WorkerTag
            << " sending event for session creation to proxy: " << kqp_proxy.ToString());

        Send(kqp_proxy, ev.Release());
    }

    void HandleCreateSessionResponse(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const TActorContext& ctx) {
        auto& response = ev->Get()->Record;

        if (response.GetYdbStatus() == Ydb::StatusIds_StatusCode_SUCCESS) {
            WorkerSession = response.GetResponse().GetSessionId();
            LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Worker Tag# " << ParentTag << "." << WorkerTag << " Session is created: " + WorkerSession);
            CreateDataQuery(ctx);
        } else {
            LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Worker Tag# " << ParentTag << "." << WorkerTag
                << " Session creation failed: " + ev->Get()->ToString());
        }
    }

    void CreateDataQuery(const TActorContext& ctx) {
        if (queries.empty()) {
            queries = WorkloadQueryGen->GetWorkload(WorkloadType);
        }

        auto q = std::move(queries.front());
        queries.pop_front();

        LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Worker Tag# " << ParentTag << "." << WorkerTag
            << " query type: " << WorkloadType << ", params size: " << q.Params.GetValues().size());

        Transactions->Inc();

        TString query_text = TString(q.Query);
        NYdb::TParams query_params = q.Params;

        auto request = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();

        LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Worker Tag# " << ParentTag << "." << WorkerTag << " using session: " << WorkerSession);

        request->Record.MutableRequest()->SetSessionId(WorkerSession);
        request->Record.MutableRequest()->SetKeepSession(true);
        request->Record.MutableRequest()->SetDatabase(WorkingDir);

        request->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        request->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
        request->Record.MutableRequest()->SetQuery(query_text);

        request->Record.MutableRequest()->MutableQueryCachePolicy()->set_keep_in_cache(true);
        request->Record.MutableRequest()->MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
        request->Record.MutableRequest()->MutableTxControl()->set_commit_tx(true);

        request->Record.MutableRequest()->SetCollectStats(Ydb::Table::QueryStatsCollection_Mode::QueryStatsCollection_Mode_STATS_COLLECTION_BASIC);

        NKikimrMiniKQL::TParams params;
        ConvertYdbParamsToMiniKQLParams(query_params, params);
        request->Record.MutableRequest()->MutableParameters()->Swap(&params);

        auto kqp_proxy = NKqp::MakeKqpProxyID(ctx.SelfID.NodeId());
        LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Worker Tag# " << ParentTag << "." << WorkerTag
            << " sending data query to proxy: " + kqp_proxy.ToString());

        ctx.Send(kqp_proxy, request.Release());

    }

    void HandleResponse(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
        auto& response = ev->Get()->Record.GetRef();

        Transactions->Dec();

        if (response.GetYdbStatus() == Ydb::StatusIds_StatusCode_SUCCESS) {
            LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Worker Tag# " << ParentTag << "." << WorkerTag << " data request status: Success");
            TransactionsBytesWritten->Add(response.GetResponse().GetQueryStats().ByteSize());
            WindowHist.RecordValue(response.GetResponse().GetQueryStats().GetDurationUs());
        } else {
            LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Worker Tag# " << ParentTag << "." << WorkerTag
                << " data request status: Fail, Issue: " + ev->Get()->ToString());
            ++WindowErrors;
        }

        if (Phase < WindowCount) {
            CreateDataQuery(ctx);
        }
    }

private:

    // monitoring

    void HandleWindowTimer(TEvUpdateMonitoring::TPtr& /*ev*/, const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Worker Tag# " << ParentTag << "." << WorkerTag
            << " handle TEvUpdateMonitoring, Phase: " << Phase);

        SendMonitoringEvent(ctx);

        if (Phase < WindowCount) {
            LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Worker Tag# " << ParentTag << "." << WorkerTag
                << " reschedule TEvUpdateMonitoring, Phase: " << Phase);
            ctx.Schedule(TDuration::Seconds(WindowDuration), new TEvUpdateMonitoring);
        }
    }

private:

    // common

    void SendMonitoringEvent(const TActorContext& ctx) {
        auto ev = MakeHolder<TEvKqpWorkerResponse>(WindowHist, WindowErrors, Phase, WorkerTag);

        WindowHist.Reset();
        WindowErrors = 0;
        ++Phase;

        ctx.Send(Parent, ev.Release());
    }

private:
    TActorId Parent;
    TString WorkingDir;
    std::shared_ptr<NYdbWorkload::IWorkloadQueryGenerator> WorkloadQueryGen;
    ui64 WorkloadType;
    ui64 ParentTag;
    ui64 WorkerTag;

    NYdbWorkload::TQueryInfoList queries;

    TString WorkerSession = "wrong sessionId";
    ui64 DurationSeconds = 1;

private:
    // for monitoring
    NHdr::THistogram WindowHist;
    ui64 WindowErrors = 0;

    ui64 WindowDuration;
    ui64 WindowCount;

    ui64 Phase = 0;

    NMonitoring::TDynamicCounters::TCounterPtr Transactions;
    NMonitoring::TDynamicCounters::TCounterPtr TransactionsBytesWritten;

};

class TKqpWriterTestLoadActor : public TActorBootstrapped<TKqpWriterTestLoadActor> {
public:
    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::KQP_TEST_WORKLOAD;
    }

    TKqpWriterTestLoadActor(const NKikimrBlobStorage::TEvTestLoadRequest::TKqpLoadStart& cmd,
        const TActorId& parent,
        const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters,
        ui64 index,
        ui64 tag)
        : Parent(parent)
        , Tag(tag)
    {
        Y_UNUSED(index);
        VERIFY_PARAM(DurationSeconds);

        google::protobuf::TextFormat::PrintToString(cmd, &ConfingString);

        UniformPartitionsCount = cmd.GetUniformPartitionsCount();
        DeleteTableOnFinish = cmd.GetDeleteTableOnFinish();
        WorkingDir = cmd.GetWorkingDir();
        WorkloadType = cmd.GetWorkloadType();
        DurationSeconds = cmd.GetDurationSeconds();
        WindowDuration = cmd.GetWindowDuration();
        WindowCount = (DurationSeconds + WindowDuration - 1) / WindowDuration;
        NumOfSessions = cmd.GetNumOfSessions();
        ChunkLoad.resize(WindowCount);
        Chunk.reserve(WindowCount);
        Total = std::make_unique<MonitoringData>();
        for (size_t i = 0; i < WindowCount; ++i) {
            Chunk.push_back(std::make_unique<MonitoringData>());
        }

        NYdbWorkload::TWorkloadFactory factory;

        if (cmd.Workload_case() == NKikimrBlobStorage::TEvTestLoadRequest_TKqpLoadStart::WorkloadCase::kStock) {
            WorkloadClass = NYdbWorkload::EWorkload::STOCK;
            NYdbWorkload::TStockWorkloadParams params;
            params.PartitionsByLoad = cmd.GetStock().GetPartitionsByLoad();
            params.OrderCount = cmd.GetStock().GetOrderCount();
            params.ProductCount = cmd.GetStock().GetProductCount();
            params.Quantity = cmd.GetStock().GetQuantity();
            params.Limit = cmd.GetStock().GetLimit();
            params.DbPath = WorkingDir;
            params.MinPartitions = UniformPartitionsCount;
            WorkloadQueryGen = factory.GetWorkloadQueryGenerator(NYdbWorkload::EWorkload::STOCK, &params);
        } else if (cmd.Workload_case() == NKikimrBlobStorage::TEvTestLoadRequest_TKqpLoadStart::WorkloadCase::kKv) {
            WorkloadClass = NYdbWorkload::EWorkload::KV;
            NYdbWorkload::TKvWorkloadParams params;
            params.InitRowCount = cmd.GetKv().GetInitRowCount();
            params.PartitionsByLoad = cmd.GetKv().GetPartitionsByLoad();
            params.MaxFirstKey = cmd.GetKv().GetMaxFirstKey();
            params.StringLen = cmd.GetKv().GetStringLen();
            params.ColumnsCnt = cmd.GetKv().GetColumnsCnt();
            params.RowsCnt = cmd.GetKv().GetRowsCnt();
            params.MinPartitions = UniformPartitionsCount;
            params.DbPath = WorkingDir;
            WorkloadQueryGen = factory.GetWorkloadQueryGenerator(NYdbWorkload::EWorkload::KV, &params);
        } else {
            return;
        }

        Y_ASSERT(WorkloadQueryGen.get() != nullptr);
        Y_ASSERT(DurationSeconds > DelayBeforeMeasurements.Seconds());

        // Monitoring initialization

        LoadCounters = counters->GetSubgroup("tag", Sprintf("%" PRIu64, tag));
        Transactions = LoadCounters->GetCounter("Transactions", true);
        TransactionsBytesWritten = LoadCounters->GetCounter("TransactionsBytesWritten", true);
    }

    ~TKqpWriterTestLoadActor() {
        LoadCounters->ResetCounters();
    }

    void Bootstrap(const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag << " TKqpWriterTestLoadActor Bootstrap called");
        Become(&TKqpWriterTestLoadActor::StateStart);

        if (WorkloadClass == NYdbWorkload::EWorkload::STOCK) {
            NYdbWorkload::TStockWorkloadParams* params = static_cast<NYdbWorkload::TStockWorkloadParams*>(WorkloadQueryGen->GetParams());
            LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag << " Starting load actor with workload STOCK, Params: {"
                << "PartitionsByLoad: " << params->PartitionsByLoad << " "
                << "OrderCount: " << params->OrderCount << " "
                << "ProductCount: " << params->ProductCount << " "
                << "Quantity: " << params->Quantity << " "
                << "Limit: " << params->Limit << " "
                << "DbPath: " << params->DbPath << " "
                << "MinPartitions: " << params->MinPartitions);
        } else if (WorkloadClass == NYdbWorkload::EWorkload::KV) {
            NYdbWorkload::TKvWorkloadParams* params = static_cast<NYdbWorkload::TKvWorkloadParams*>(WorkloadQueryGen->GetParams());
            LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag << " Starting load actor with workload KV, Params: {"
                << "InitRowCount: " << params->InitRowCount << " "
                << "PartitionsByLoad: " << params->PartitionsByLoad << " "
                << "MaxFirstKey: " << params->MaxFirstKey << " "
                << "MinPartitions: " << params->MinPartitions << " "
                << "StringLen: " << params->StringLen << " "
                << "ColumnsCnt: " << params->ColumnsCnt << " "
                << "RowsCnt: " << params->RowsCnt << " "
                << "DbPath: " << params->DbPath);
        }

        LOG_INFO_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag << " Schedule PoisonPill");
        ctx.Schedule(TDuration::Seconds(DurationSeconds * 2), new TEvents::TEvPoisonPill);

        CreateSessionForTablesDDL(ctx);
    }

    STRICT_STFUNC(StateStart,
        CFunc(TEvents::TSystem::PoisonPill, HandlePoisonPill)
        HFunc(NKqp::TEvKqp::TEvCreateSessionResponse, HandleCreateSessionResponse)
        HFunc(NKqp::TEvKqp::TEvQueryResponse, HandleCreateTableResponse)
        HFunc(NMon::TEvHttpInfo, HandleHTML)
    )

    STRICT_STFUNC(StateMain,
        CFunc(TEvents::TSystem::PoisonPill, HandlePoisonPill)
        HFunc(NKqp::TEvKqp::TEvQueryResponse, HandleDataQueryResponse)
        HFunc(TEvKqpWorkerResponse, HandleMonitoring)
        HFunc(NMon::TEvHttpInfo, HandleHTML)
    )

    STRICT_STFUNC(StateEndOfWork,
        HFunc(NKqp::TEvKqp::TEvQueryResponse, HandleDropTablesResponse)
    )

private:

    // death

    void HandlePoisonPill(const TActorContext& ctx) {
        LOG_INFO_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag << " HandlePoisonPill, starting death process");
        StartDeathProcess(ctx);
    }

    void StartDeathProcess(const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag << " TKqpWriterTestLoadActor StartDeathProcess called");

        Become(&TKqpWriterTestLoadActor::StateEndOfWork);

        if (DeleteTableOnFinish) {
            DropTables(ctx);
        } else {
            DeathReport(ctx);
        }
    }

    void DropTables(const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag << " creating event for tables drop");

        auto ev = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
        ev->Record.MutableRequest()->SetDatabase(WorkingDir);
        ev->Record.MutableRequest()->SetSessionId(TableSession);
        ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_DDL);
        ev->Record.MutableRequest()->SetQuery(WorkloadQueryGen->GetCleanDDLQueries());

        auto kqp_proxy = NKqp::MakeKqpProxyID(ctx.SelfID.NodeId());
        LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag << " sending drop tables query to proxy: " + kqp_proxy.ToString());

        ctx.Send(kqp_proxy, ev.Release());
    }

    void HandleDropTablesResponse(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
        auto& response = ev->Get()->Record.GetRef();

        if (response.GetYdbStatus() == Ydb::StatusIds_StatusCode_SUCCESS) {
            LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag << " drop tables status: SUCCESS");
        } else {
            LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag << " drop tables status: FAIL, reason: " + ev->Get()->ToString());
        }

        DeathReport(ctx);
    }

    void DeathReport(const TActorContext& ctx) {
        CloseSession(ctx);

        TIntrusivePtr<TLoadReport> Report(new TLoadReport());
        Report->Duration = TDuration::Seconds(DurationSeconds);

        auto* finishEv = new TEvTestLoadFinished(Tag, Report, "OK called StartDeathProcess");
        finishEv->LastHtmlPage = RenderHTML();
        ctx.Send(Parent, finishEv);
        Die(ctx);
    }

private:

    // monitoring
    void HandleMonitoring(TEvKqpWorkerResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& response = ev->Get();

        LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag << " got monitoring response from worker Tag# " << response->WorkerTag
            << " Phase: " << response->Phase
            << " Min: " << response->Data.WindowHist.GetMin()
            << " Max: " << response->Data.WindowHist.GetMax()
            << " Count: " << response->Data.WindowHist.GetTotalCount());

        Chunk[response->Phase]->Add(response->Data);
        ChunkLoad[response->Phase] += 1;

        if (ChunkLoad[Phase] == NumOfSessions) {
            Total->Add(*Chunk[Phase]);
            SendNewRowToParent(ctx);
        }
    }

    void SendNewRowToParent(const TActorContext& ctx) {
        Phase += 1;

        LOG_INFO_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag
            << " total: Phase: " << Phase << " -> "
            << Total->WindowHist.GetTotalCount() << " | "
            << Total->WindowHist.GetTotalCount() / (WindowDuration * std::max(ui64(1), Phase) * 1.0) << " | "
            << Total->WindowErrors << " | "
            << Total->WindowHist.GetValueAtPercentile(50.0) / (WindowDuration * 1000.0) << " | "
            << Total->WindowHist.GetValueAtPercentile(95.0) / (WindowDuration * 1000.0) << " | "
            << Total->WindowHist.GetValueAtPercentile(99.0) / (WindowDuration * 1000.0) << " | "
            << Total->WindowHist.GetMax() / (WindowDuration * 1000.0)
        );

        if (Phase >= WindowCount) {
            StartDeathProcess(ctx);
        }
    }

private:

    // creating tables

    void CreateSessionForTablesDDL(const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag << " creating event for session creation");
        auto ev = MakeHolder<NKqp::TEvKqp::TEvCreateSessionRequest>();

        ev->Record.MutableRequest()->SetDatabase(WorkingDir);

        auto kqp_proxy = NKqp::MakeKqpProxyID(ctx.SelfID.NodeId());
        LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag << " sending event for session creation to proxy: " << kqp_proxy.ToString());

        Send(kqp_proxy, ev.Release());
    }

    void HandleCreateSessionResponse(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const TActorContext& ctx) {
        auto& response = ev->Get()->Record;

        if (response.GetYdbStatus() == Ydb::StatusIds_StatusCode_SUCCESS) {
            TableSession = response.GetResponse().GetSessionId();
            LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag << " Session is created: " + TableSession);
            CreateTables(ctx);
        } else {
            LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag << " Session creation failed: " + ev->Get()->ToString());
        }
    }

    void CreateTables(const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag << " creating event for tables creation");

        auto ev = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
        ev->Record.MutableRequest()->SetDatabase(WorkingDir);
        ev->Record.MutableRequest()->SetSessionId(TableSession);
        ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_DDL);
        ev->Record.MutableRequest()->SetQuery(WorkloadQueryGen->GetDDLQueries());

        auto kqp_proxy = NKqp::MakeKqpProxyID(ctx.SelfID.NodeId());
        LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag << " sending ddl query to proxy: " + kqp_proxy.ToString());

        ctx.Send(kqp_proxy, ev.Release());
    }

    void HandleCreateTableResponse(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
        auto& response = ev->Get()->Record.GetRef();

        if (response.GetYdbStatus() == Ydb::StatusIds_StatusCode_SUCCESS) {
            Become(&TKqpWriterTestLoadActor::StateMain);
            LOG_INFO_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag << " tables are created");
            InitData = WorkloadQueryGen->GetInitialData();
            InsertInitData(ctx);
        } else {
            LOG_INFO_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag << " tables creation failed: " + ev->Get()->ToString());
            CreateTables(ctx);
        }
    }

private:

    // table initialization

    void InsertInitData(const TActorContext& ctx) {
        if (InitData.empty()) {
            InitWorkers(ctx);
            return;
        }

        auto q = std::move(InitData.front());
        InitData.pop_front();

        LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag
            << " Creating request for init query, need to exec: " << InitData.size() + 1);

        TString query_text = TString(q.Query);
        NYdb::TParams query_params = q.Params;

        auto request = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();

        LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag << " using session: " << TableSession);

        request->Record.MutableRequest()->SetSessionId(TableSession);
        request->Record.MutableRequest()->SetKeepSession(true);
        request->Record.MutableRequest()->SetDatabase(WorkingDir);

        request->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        request->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
        request->Record.MutableRequest()->SetQuery(query_text);

        request->Record.MutableRequest()->MutableQueryCachePolicy()->set_keep_in_cache(true);
        request->Record.MutableRequest()->MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
        request->Record.MutableRequest()->MutableTxControl()->set_commit_tx(true);

        request->Record.MutableRequest()->SetCollectStats(Ydb::Table::QueryStatsCollection_Mode::QueryStatsCollection_Mode_STATS_COLLECTION_BASIC);

        NKikimrMiniKQL::TParams params;
        ConvertYdbParamsToMiniKQLParams(query_params, params);
        request->Record.MutableRequest()->MutableParameters()->Swap(&params);

        auto kqp_proxy = NKqp::MakeKqpProxyID(ctx.SelfID.NodeId());
        LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag
            << " sending init query to proxy: " + kqp_proxy.ToString());

        ctx.Send(kqp_proxy, request.Release());
    }

    void HandleDataQueryResponse(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
        auto& response = ev->Get()->Record.GetRef();

        if (response.GetYdbStatus() == Ydb::StatusIds_StatusCode_SUCCESS) {
            LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag << " init query status: SUCCESS");
        } else {
            LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag << " init query status: FAIL, reason: " + ev->Get()->ToString());
        }

        InsertInitData(ctx);
    }

private:

    TString RenderHTML() {
        TStringStream str;
        HTML(str) {
            TABLE_CLASS("table table-condensed") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() {
                            str << "Window";
                        }
                        TABLEH() {
                            str << "Txs";
                        }
                        TABLEH() {
                            str << "Txs/Sec";
                        }
                        TABLEH() {
                            str << "Errors";
                        }
                        TABLEH() {
                            str << "p50(ms)";
                        }
                        TABLEH() {
                            str << "p95(ms)";
                        }
                        TABLEH() {
                            str << "p99(ms)";
                        }
                        TABLEH() {
                            str << "pMax(ms)";
                        }
                    }
                }
                TABLEBODY() {
                    TABLER() {
                        TABLED() { str << "total"; };
                        TABLED() { str << Total->WindowHist.GetTotalCount(); };
                        TABLED() { str << Total->WindowHist.GetTotalCount() / (WindowDuration * std::max(ui64(1), Phase) * 1.0); };
                        TABLED() { str << Total->WindowErrors; };
                        TABLED() { str << Total->WindowHist.GetValueAtPercentile(50.0) / (WindowDuration * 1000.0); };
                        TABLED() { str << Total->WindowHist.GetValueAtPercentile(95.0) / (WindowDuration * 1000.0); };
                        TABLED() { str << Total->WindowHist.GetValueAtPercentile(99.0) / (WindowDuration * 1000.0); };
                        TABLED() { str << Total->WindowHist.GetMax() / (WindowDuration * 1000.0); };
                    }
                    for (size_t i = Phase; i >= 1; --i) {
                        TABLER() {
                            TABLED() { str << i; };
                            TABLED() { str << Chunk[i - 1]->WindowHist.GetTotalCount(); };
                            TABLED() { str << Chunk[i - 1]->WindowHist.GetTotalCount() / (WindowDuration * 1.0); };
                            TABLED() { str << Chunk[i - 1]->WindowErrors; };
                            TABLED() { str << Chunk[i - 1]->WindowHist.GetValueAtPercentile(50.0) / (WindowDuration * 1000.0); };
                            TABLED() { str << Chunk[i - 1]->WindowHist.GetValueAtPercentile(95.0) / (WindowDuration * 1000.0); };
                            TABLED() { str << Chunk[i - 1]->WindowHist.GetValueAtPercentile(99.0) / (WindowDuration * 1000.0); };
                            TABLED() { str << Chunk[i - 1]->WindowHist.GetMax() / (WindowDuration * 1000.0); };
                        }
                    }
                }
            }
            COLLAPSED_BUTTON_CONTENT(Sprintf("configProtobuf%" PRIu64, Tag), "Config") {
                str << "<pre>" << ConfingString << "</pre>";
            }
        }
        return str.Str();
    }

    void HandleHTML(NMon::TEvHttpInfo::TPtr& ev, const TActorContext& ctx) {
        ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(RenderHTML(), ev->Get()->SubRequestId));
    }


private:

    // common

    void InitWorkers(const TActorContext& ctx) {
        for (ui64 i = 0; i < NumOfSessions; ++i) {
            auto* worker = new TKqpLoadWorker(
                SelfId(),
                WorkingDir,
                WorkloadQueryGen,
                WorkloadType,
                Tag,
                i,
                DurationSeconds,
                WindowDuration,
                WindowCount,
                Transactions,
                TransactionsBytesWritten);
            Workers.push_back(ctx.Register(worker));
        }
    }

    void CloseSession(const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag << " creating event for session close");

        auto ev = MakeHolder<NKqp::TEvKqp::TEvCloseSessionRequest>();
        ev->Record.MutableRequest()->SetSessionId(TableSession);

        auto kqp_proxy = NKqp::MakeKqpProxyID(ctx.SelfID.NodeId());
        LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag << " sending session close query to proxy: " + kqp_proxy.ToString());

        ctx.Send(kqp_proxy, ev.Release());
    }

private:
    TString TableSession = "wrong sessionId";
    TString WorkingDir;
    ui64 WorkloadType;
    ui64 WindowCount;
    ui64 WindowDuration;
    std::vector<TActorId> Workers;
    TString ConfingString;
    ui64 UniformPartitionsCount;
    bool DeleteTableOnFinish;
    ui32 NumOfSessions;
    NYdbWorkload::EWorkload WorkloadClass;

    NYdbWorkload::TQueryInfoList InitData;

    const TActorId Parent;
    ui64 Tag;
    ui32 DurationSeconds;
    std::shared_ptr<NYdbWorkload::IWorkloadQueryGenerator> WorkloadQueryGen;

    // Monitoring
    std::vector<std::unique_ptr<MonitoringData>> Chunk;
    std::vector<ui64> ChunkLoad;
    std::unique_ptr<MonitoringData> Total;
    ui64 Phase = 0;

    // counters
    TIntrusivePtr<::NMonitoring::TDynamicCounters> LoadCounters;
    NMonitoring::TDynamicCounters::TCounterPtr Transactions;
    NMonitoring::TDynamicCounters::TCounterPtr TransactionsBytesWritten;

};

IActor * CreateKqpWriterTestLoad(const NKikimrBlobStorage::TEvTestLoadRequest::TKqpLoadStart& cmd,
        const TActorId& parent, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters, ui64 index, ui64 tag) {
    return new TKqpWriterTestLoadActor(cmd, parent, counters, index, tag);
}

} // NKikimr
