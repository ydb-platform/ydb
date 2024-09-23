#include "service_actor.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/protos/kqp_stats.pb.h>

#include <ydb/library/workload/abstract/workload_factory.h>
#include <ydb/library/workload/stock/stock.h>
#include <ydb/library/workload/kv/kv.h>

#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/histogram/hdr/histogram.h>
#include <library/cpp/time_provider/time_provider.h>

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
    {}

    MonitoringData(const NHdr::THistogram& hist, ui64 window_errors)
        : Errors(window_errors)
    {
        LatencyHist.Add(hist);
    }

    void Add(const MonitoringData& other) {
        LatencyHist.Add(other.LatencyHist);
        Errors += other.Errors;
    }

public:
    NHdr::THistogram LatencyHist{60000, 2};
    ui64 Errors = 0;
};

void SendQueryRequest(const TActorContext& ctx, NYdbWorkload::TQueryInfo& q, const NKikimrKqp::EQueryType queryType, const TString& session, const TString& workingDir) {
    TString query_text = TString(q.Query);
    auto request = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();

    request->Record.MutableRequest()->SetSessionId(session);
    request->Record.MutableRequest()->SetKeepSession(true);
    request->Record.MutableRequest()->SetDatabase(workingDir);

    request->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
    request->Record.MutableRequest()->SetType(queryType);
    request->Record.MutableRequest()->SetQuery(query_text);

    request->Record.MutableRequest()->MutableQueryCachePolicy()->set_keep_in_cache(true);
    request->Record.MutableRequest()->MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
    request->Record.MutableRequest()->MutableTxControl()->set_commit_tx(true);

    request->Record.MutableRequest()->SetCollectStats(Ydb::Table::QueryStatsCollection_Mode::QueryStatsCollection_Mode_STATS_COLLECTION_BASIC);

    const auto& paramsMap = NYdb::TProtoAccessor::GetProtoMap(q.Params);
    request->Record.MutableRequest()->MutableYdbParameters()->insert(paramsMap.begin(), paramsMap.end());

    auto kqp_proxy = NKqp::MakeKqpProxyID(ctx.SelfID.NodeId());

    ctx.Send(kqp_proxy, request.Release());
}

struct TEvKqpWorkerResponse : TEventLocal<TEvKqpWorkerResponse, EvKqpWorkerResponse> {
public:
    TEvKqpWorkerResponse(const NHdr::THistogram& hist, ui64 errors, ui64 workerTag)
        : Data(hist, errors)
        , WorkerTag(workerTag) {}

public:
    MonitoringData Data;
    ui64 WorkerTag;
};

class TKqpLoadWorker : public TActorBootstrapped<TKqpLoadWorker> {
public:
    TKqpLoadWorker(TActorId parent,
            TString working_dir,
            std::shared_ptr<NYdbWorkload::IWorkloadQueryGenerator> workload_query_gen,
            ui64 workload_type,
            NKikimrKqp::EQueryType queryType,
            ui64 parentTag,
            ui64 workerTag,
            TInstant endTimestamp,
            NMonitoring::TDynamicCounters::TCounterPtr transactions,
            NMonitoring::TDynamicCounters::TCounterPtr transactionsBytesWritten)
        : Parent(std::move(parent))
        , WorkingDir(std::move(working_dir))
        , WorkloadQueryGen(workload_query_gen)
        , WorkloadType(workload_type)
        , ParentTag(parentTag)
        , WorkerTag(workerTag)
        , EndTimestamp(endTimestamp)
        , QueryType(queryType)
        , LatencyHist(60000, 2)
        , Transactions(transactions)
        , TransactionsBytesWritten(transactionsBytesWritten)
    {}

    void Bootstrap(const TActorContext& ctx) {
        LOG_INFO_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Worker Tag# " << ParentTag << "." << WorkerTag << " TKqpLoadWorker Bootstrap called");

        ctx.Schedule(EndTimestamp, new TEvents::TEvPoisonPill);

        Become(&TKqpLoadWorker::StateFunc);
        CreateWorkingSession(ctx);
    }

private:
    // death

    void HandlePoisonPill(const TActorContext& ctx) {
        LOG_INFO_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Worker Tag# " << ParentTag << "." << WorkerTag << " HandlePoisonPill");

        Send(Parent, new TEvKqpWorkerResponse(LatencyHist, Errors, WorkerTag));

        CloseSession(ctx);
        PassAway();
    }

    void CloseSession(const TActorContext& ctx) {
        LOG_INFO_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Worker Tag# " << ParentTag << "." << WorkerTag << " creating event for session close");

        auto ev = MakeHolder<NKqp::TEvKqp::TEvCloseSessionRequest>();
        ev->Record.MutableRequest()->SetSessionId(WorkerSession);

        auto kqp_proxy = NKqp::MakeKqpProxyID(ctx.SelfID.NodeId());

        ctx.Send(kqp_proxy, ev.Release());
    }

    // working

    void CreateWorkingSession(const TActorContext& ctx) {
        LOG_INFO_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Worker Tag# " << ParentTag << "." << WorkerTag << " creating event for session creation");
        auto ev = MakeHolder<NKqp::TEvKqp::TEvCreateSessionRequest>();

        ev->Record.MutableRequest()->SetDatabase(WorkingDir);

        auto kqp_proxy = NKqp::MakeKqpProxyID(ctx.SelfID.NodeId());

        Send(kqp_proxy, ev.Release());
    }

    void Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const TActorContext& ctx) {
        auto& response = ev->Get()->Record;

        if (response.GetYdbStatus() == Ydb::StatusIds_StatusCode_SUCCESS) {
            WorkerSession = response.GetResponse().GetSessionId();
            LOG_INFO_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Worker Tag# " << ParentTag << "." << WorkerTag << " Session is created: " + WorkerSession);
            CreateDataQuery(ctx);
        } else {
            LOG_ERROR_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Worker Tag# " << ParentTag << "." << WorkerTag
                << " Session creation failed: " + ev->Get()->ToString());
        }
    }

    void CreateDataQuery(const TActorContext& ctx) {
        if (Queries.empty()) {
            Queries = WorkloadQueryGen->GetWorkload(WorkloadType);
        }

        Y_ABORT_UNLESS(!Queries.empty());
        auto q = std::move(Queries.front());
        Queries.pop_front();

        LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Worker Tag# " << ParentTag << "." << WorkerTag
            << " query type: " << WorkloadType << ", params size: " << q.Params.GetValues().size());

        Transactions->Inc();

        LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Worker Tag# " << ParentTag << "." << WorkerTag << " using session: " << WorkerSession);

        SendQueryRequest(ctx, q, QueryType, WorkerSession, WorkingDir);
    }

    void Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
        auto& response = ev->Get()->Record.GetRef();

        if (response.GetYdbStatus() == Ydb::StatusIds_StatusCode_SUCCESS) {
            LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Worker Tag# " << ParentTag << "." << WorkerTag << " data request status: Success");
            TransactionsBytesWritten->Add(response.GetResponse().GetQueryStats().ByteSize());
            LatencyHist.RecordValue(response.GetResponse().GetQueryStats().GetDurationUs());
        } else {
            LOG_INFO_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Worker Tag# " << ParentTag << "." << WorkerTag
                << " data request status: Fail, Issue: " + ev->Get()->ToString());
            ++Errors;
        }

        CreateDataQuery(ctx);
    }

    STRICT_STFUNC(StateFunc,
        CFunc(TEvents::TSystem::PoisonPill, HandlePoisonPill)
        HFunc(NKqp::TEvKqp::TEvQueryResponse, Handle)
        HFunc(NKqp::TEvKqp::TEvCreateSessionResponse, Handle)
    )

    TActorId Parent;
    TString WorkingDir;
    std::shared_ptr<NYdbWorkload::IWorkloadQueryGenerator> WorkloadQueryGen;
    ui64 WorkloadType;
    ui64 ParentTag;
    ui64 WorkerTag;
    TInstant EndTimestamp;
    NYdbWorkload::TQueryInfoList Queries;
    TString WorkerSession = "wrong sessionId";
    NKikimrKqp::EQueryType QueryType;

    // monitoring
    NHdr::THistogram LatencyHist;
    ui64 Errors = 0;
    NMonitoring::TDynamicCounters::TCounterPtr Transactions;
    NMonitoring::TDynamicCounters::TCounterPtr TransactionsBytesWritten;
};

class TKqpLoadActor : public TActorBootstrapped<TKqpLoadActor> {
public:
    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::KQP_TEST_WORKLOAD;
    }

    TKqpLoadActor(const NKikimr::TEvLoadTestRequest::TKqpLoad& cmd, const TActorId& parent,
            const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters, ui64 index, ui64 tag)
        : Parent(parent)
        , Tag(tag)
    {
        Y_UNUSED(index);
        VERIFY_PARAM(DurationSeconds);

        google::protobuf::TextFormat::PrintToString(cmd, &ConfigString);

        UniformPartitionsCount = cmd.GetUniformPartitionsCount();
        DeleteTableOnFinish = cmd.GetDeleteTableOnFinish();
        WorkingDir = cmd.GetWorkingDir();
        WorkloadType = cmd.GetWorkloadType();
        Y_ABORT_UNLESS(cmd.GetQueryType() == "generic" || cmd.GetQueryType() == "data");
        QueryType = cmd.GetQueryType() == "generic"
            ? NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY
            : NKikimrKqp::QUERY_TYPE_SQL_DML;
        DurationSeconds = cmd.GetDurationSeconds();
        NumOfSessions = cmd.GetNumOfSessions();
        IncreaseSessions = cmd.GetIncreaseSessions();
        Total = std::make_unique<MonitoringData>();

        if (cmd.Workload_case() == NKikimr::TEvLoadTestRequest_TKqpLoad::WorkloadCase::kStock) {
            WorkloadClass = "stock";
            auto params = std::make_shared<NYdbWorkload::TStockWorkloadParams>();
            params->PartitionsByLoad = cmd.GetStock().GetPartitionsByLoad();
            params->OrderCount = cmd.GetStock().GetOrderCount();
            params->ProductCount = cmd.GetStock().GetProductCount();
            params->Quantity = cmd.GetStock().GetQuantity();
            params->Limit = cmd.GetStock().GetLimit();
            params->DbPath = WorkingDir;
            params->MinPartitions = UniformPartitionsCount;
            WorkloadQueryGen = std::make_shared<NYdbWorkload::TStockWorkloadGenerator>(params.get());
            WorkloadQueryGenParams = params;
        } else if (cmd.Workload_case() == NKikimr::TEvLoadTestRequest_TKqpLoad::WorkloadCase::kKv) {
            WorkloadClass = "kv";
            auto params = std::make_shared<NYdbWorkload::TKvWorkloadParams>();
            params->InitRowCount = cmd.GetKv().GetInitRowCount();
            params->PartitionsByLoad = cmd.GetKv().GetPartitionsByLoad();
            params->MaxFirstKey = cmd.GetKv().GetMaxFirstKey();
            params->StringLen = cmd.GetKv().GetStringLen();
            params->ColumnsCnt = cmd.GetKv().GetColumnsCnt();
            params->RowsCnt = cmd.GetKv().GetRowsCnt();
            params->MinPartitions = UniformPartitionsCount;
            WorkloadQueryGen = std::make_shared<NYdbWorkload::TKvWorkloadGenerator>(params.get());
            WorkloadQueryGenParams = params;
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

    ~TKqpLoadActor() {
        LoadCounters->ResetCounters();
    }

    void Bootstrap(const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag << " TKqpLoadActor Bootstrap called");

        Become(&TKqpLoadActor::StateStart);

        if (WorkloadClass == "stock") {
            NYdbWorkload::TStockWorkloadParams* params = static_cast<NYdbWorkload::TStockWorkloadParams*>(WorkloadQueryGenParams.get());
            LOG_INFO_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag << " Starting load actor with workload STOCK, Params: {"
                << "PartitionsByLoad: " << params->PartitionsByLoad << " "
                << "OrderCount: " << params->OrderCount << " "
                << "ProductCount: " << params->ProductCount << " "
                << "Quantity: " << params->Quantity << " "
                << "Limit: " << params->Limit << " "
                << "DbPath: " << params->DbPath << " "
                << "MinPartitions: " << params->MinPartitions);
        } else if (WorkloadClass == "kv") {
            NYdbWorkload::TKvWorkloadParams* params = static_cast<NYdbWorkload::TKvWorkloadParams*>(WorkloadQueryGenParams.get());
            LOG_INFO_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag << " Starting load actor with workload KV, Params: {"
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
        EarlyStop = false;
        ctx.Schedule(TDuration::Seconds(DurationSeconds + 10), new TEvents::TEvPoisonPill);

        CreateSessionForTablesDDL(ctx);
    }

    void HandleWakeup(const TActorContext& ctx) {
        if (ResultsReceived) {
            // if death process is started, then break wakeup circuit
            return;
        }
        size_t targetSessions;
        if (IncreaseSessions) {
            targetSessions = 1 + NumOfSessions * (TAppData::TimeProvider->Now() - TestStartTime).Seconds() / DurationSeconds;
            targetSessions = std::min(targetSessions, NumOfSessions);
        } else {
            targetSessions = NumOfSessions;
        }
        while (Workers.size() < targetSessions) {
            AppendWorker(ctx);
        }
        ctx.Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup);
    }

    STRICT_STFUNC(StateStart,
        CFunc(TEvents::TSystem::PoisonPill, HandlePoisonPill)
        HFunc(NKqp::TEvKqp::TEvCreateSessionResponse, Handle)
        HFunc(NKqp::TEvKqp::TEvQueryResponse, HandleCreateTableResponse)
        HFunc(NMon::TEvHttpInfo, HandleHTML)
    )

    STRICT_STFUNC(StateMain,
        CFunc(TEvents::TSystem::PoisonPill, HandlePoisonPill)
        CFunc(TEvents::TSystem::Wakeup, HandleWakeup)
        HFunc(NKqp::TEvKqp::TEvQueryResponse, HandleDataQueryResponse)
        HFunc(TEvKqpWorkerResponse, HandleResult)
        HFunc(NMon::TEvHttpInfo, HandleHTML)
    )

    STRICT_STFUNC(StateEndOfWork,
        HFunc(NKqp::TEvKqp::TEvQueryResponse, HandleDropTablesResponse)
    )

private:

    // death

    void HandlePoisonPill(const TActorContext& ctx) {
        EarlyStop = (TAppData::TimeProvider->Now() - TestStartTime).Seconds() < DurationSeconds;
        LOG_CRIT_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag << " HandlePoisonPill, "
            << "but it is supposed to pass away by receiving TEvKqpWorkerResponse from all of the workers");
        StartDeathProcess(ctx);
    }

    void StartDeathProcess(const TActorContext& ctx) {
        LOG_NOTICE_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag << " TKqpLoadActor StartDeathProcess called,"
            << " DeleteTableOnFinish: " << DeleteTableOnFinish);

        Become(&TKqpLoadActor::StateEndOfWork);

        if (DeleteTableOnFinish) {
            DropTables(ctx);
        } else {
            DeathReport(ctx);
        }
    }

    void DropTables(const TActorContext& ctx) {
        auto ev = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
        ev->Record.MutableRequest()->SetDatabase(WorkingDir);
        ev->Record.MutableRequest()->SetSessionId(TableSession);
        ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_DDL);
        ev->Record.MutableRequest()->SetQuery(WorkloadQueryGen->GetCleanDDLQueries());

        auto kqp_proxy = NKqp::MakeKqpProxyID(ctx.SelfID.NodeId());

        ctx.Send(kqp_proxy, ev.Release());
    }

    void HandleDropTablesResponse(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
        auto& response = ev->Get()->Record.GetRef();

        if (response.GetYdbStatus() == Ydb::StatusIds_StatusCode_SUCCESS) {
            LOG_NOTICE_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag << " drop tables status: SUCCESS");
        } else {
            LOG_ERROR_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag << " drop tables status: FAIL, reason: " + ev->Get()->ToString());
        }

        DeathReport(ctx);
    }

    void DeathReport(const TActorContext& ctx) {
        CloseSession(ctx);

        TIntrusivePtr<TEvLoad::TLoadReport> report = nullptr;
        TString errorReason;
        if (ResultsReceived >= Workers.size()) {
            report.Reset(new TEvLoad::TLoadReport());
            report->Duration = TDuration::Seconds(DurationSeconds);
            errorReason = "OK, called StartDeathProcess";
        } else if (EarlyStop) {
            errorReason = "Abort, stop signal received";
        } else {
            errorReason = "Abort, timeout";
        }

        auto* finishEv = new TEvLoad::TEvLoadTestFinished(Tag, report, errorReason);
        finishEv->LastHtmlPage = RenderHTML();
        finishEv->JsonResult = GetJsonResult();
        ctx.Send(Parent, finishEv);
        LOG_NOTICE_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag << " DeathReport");
        PassAway();
    }

private:

    NJson::TJsonValue GetJsonResult() const {
        NJson::TJsonValue value;
        value["duration_s"] = DurationSeconds;
        value["txs"] = Total->LatencyHist.GetTotalCount();
        value["rps"] = Total->LatencyHist.GetTotalCount() / static_cast<double>(DurationSeconds);
        value["errors"] = Total->Errors;
        {
            auto& p = value["percentile"];
            p["50"] = Total->LatencyHist.GetValueAtPercentile(50.0) / 1000.0;
            p["95"] = Total->LatencyHist.GetValueAtPercentile(95.0) / 1000.0;
            p["99"] = Total->LatencyHist.GetValueAtPercentile(99.0) / 1000.0;
            p["100"] = Total->LatencyHist.GetMax() / 1000.0;
        }
        value["config"] = ConfigString;
        return value;
    }

    // monitoring
    void HandleResult(TEvKqpWorkerResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& response = ev->Get();

        LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag << " got monitoring response from worker Tag# "
            << response->WorkerTag);

        Total->Add(response->Data);
        ++ResultsReceived;
        if (ResultsReceived == Workers.size()) {
            StartDeathProcess(ctx);
        }
    }

    // tables creation

    void CreateSessionForTablesDDL(const TActorContext& ctx) {
        LOG_NOTICE_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag << " creating event for session creation");
        auto ev = MakeHolder<NKqp::TEvKqp::TEvCreateSessionRequest>();

        ev->Record.MutableRequest()->SetDatabase(WorkingDir);

        auto kqp_proxy = NKqp::MakeKqpProxyID(ctx.SelfID.NodeId());

        Send(kqp_proxy, ev.Release());
    }

    void Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const TActorContext& ctx) {
        auto& response = ev->Get()->Record;

        if (response.GetYdbStatus() == Ydb::StatusIds_StatusCode_SUCCESS) {
            TableSession = response.GetResponse().GetSessionId();
            LOG_INFO_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag << " Session is created: " + TableSession);
            CreateTables(ctx);
        } else {
            LOG_ERROR_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag << " Session creation failed: " + ev->Get()->ToString());
        }
    }

    void CreateTables(const TActorContext& ctx) {
        LOG_NOTICE_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag << " creating event for tables creation");

        auto ev = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
        ev->Record.MutableRequest()->SetDatabase(WorkingDir);
        ev->Record.MutableRequest()->SetSessionId(TableSession);
        ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_DDL);
        ev->Record.MutableRequest()->SetQuery(WorkloadQueryGen->GetDDLQueries());

        auto kqp_proxy = NKqp::MakeKqpProxyID(ctx.SelfID.NodeId());

        ctx.Send(kqp_proxy, ev.Release());
    }

    void HandleCreateTableResponse(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
        auto& response = ev->Get()->Record.GetRef();

        if (response.GetYdbStatus() == Ydb::StatusIds_StatusCode_SUCCESS) {
            Become(&TKqpLoadActor::StateMain);
            LOG_NOTICE_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag << " tables are created");
            InitData = WorkloadQueryGen->GetInitialData();
            InsertInitData(ctx);
        } else {
            LOG_ERROR_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag << " tables creation failed: " + ev->Get()->ToString());
            CreateTables(ctx);
        }
    }

    // table initialization

    void InsertInitData(const TActorContext& ctx) {
        Y_ABORT_UNLESS(!InitData.empty());
        auto q = std::move(InitData.front());
        InitData.pop_front();

        LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag
            << " Creating request for init query, need to exec: " << InitData.size() + 1 << " session: " << TableSession);

        SendQueryRequest(ctx, q, QueryType, TableSession, WorkingDir);
    }

    void HandleDataQueryResponse(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
        auto& response = ev->Get()->Record.GetRef();

        if (response.GetYdbStatus() == Ydb::StatusIds_StatusCode_SUCCESS) {
            LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag << " init query status: SUCCESS");
        } else {
            LOG_ERROR_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag << " init query status: FAIL, reason: " + ev->Get()->ToString());
        }

        if (InitData.empty()) {
            LOG_NOTICE_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag << " initial query is executed, going to create workers");
            TestStartTime = TAppData::TimeProvider->Now();
            if (IncreaseSessions) {
                ctx.Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup);
            } else {
                for (ui64 i = 0; i < NumOfSessions; ++i) {
                    AppendWorker(ctx);
                }
            }
        } else {
            InsertInitData(ctx);
        }
    }

    TString RenderHTML() {
        TStringStream str;
        HTML(str) {
            TABLE_CLASS("table table-condensed") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() {
                            str << "Passed/Total, sec";
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
                        TABLED() {
                            if (TestStartTime) {
                                str << (TAppData::TimeProvider->Now() - TestStartTime).Seconds() << " / " << DurationSeconds;
                            } else {
                                str << -1 << " / " << DurationSeconds;
                            }
                        };
                        TABLED() { str << Total->LatencyHist.GetTotalCount(); };
                        TABLED() { str << Total->LatencyHist.GetTotalCount() / static_cast<double>(DurationSeconds); };
                        TABLED() { str << Total->Errors; };
                        TABLED() { str << Total->LatencyHist.GetValueAtPercentile(50.0) / 1000.0; };
                        TABLED() { str << Total->LatencyHist.GetValueAtPercentile(95.0) / 1000.0; };
                        TABLED() { str << Total->LatencyHist.GetValueAtPercentile(99.0) / 1000.0; };
                        TABLED() { str << Total->LatencyHist.GetMax() / 1000.0; };
                    }
                }
            }
            COLLAPSED_BUTTON_CONTENT(Sprintf("configProtobuf%" PRIu64, Tag), "Config") {
                str << "<pre>" << ConfigString << "</pre>";
            }
        }
        return str.Str();
    }

    void HandleHTML(NMon::TEvHttpInfo::TPtr& ev, const TActorContext& ctx) {
        ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(RenderHTML(), ev->Get()->SubRequestId));
    }

    // common

    void AppendWorker(const TActorContext& ctx) {
        auto* worker = new TKqpLoadWorker(
            SelfId(),
            WorkingDir,
            WorkloadQueryGen,
            WorkloadType,
            QueryType,
            Tag,
            Workers.size(),
            TestStartTime + TDuration::Seconds(DurationSeconds),
            Transactions,
            TransactionsBytesWritten);
        Workers.push_back(ctx.Register(worker));
    }

    void CloseSession(const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag << " creating event for session close");

        auto ev = MakeHolder<NKqp::TEvKqp::TEvCloseSessionRequest>();
        ev->Record.MutableRequest()->SetSessionId(TableSession);

        auto kqp_proxy = NKqp::MakeKqpProxyID(ctx.SelfID.NodeId());

        ctx.Send(kqp_proxy, ev.Release());
    }

    TInstant TestStartTime;
    bool EarlyStop = false;
    TString TableSession = "wrong sessionId";
    TString WorkingDir;
    ui64 WorkloadType;
    std::vector<TActorId> Workers;
    TString ConfigString;
    ui64 UniformPartitionsCount;
    bool DeleteTableOnFinish;
    size_t NumOfSessions = 0;
    bool IncreaseSessions = false;
    size_t ResultsReceived = 0;
    TString WorkloadClass;
    NKikimrKqp::EQueryType QueryType;

    NYdbWorkload::TQueryInfoList InitData;

    const TActorId Parent;
    ui64 Tag;
    ui32 DurationSeconds;
    std::shared_ptr<NYdbWorkload::TWorkloadParams> WorkloadQueryGenParams;
    std::shared_ptr<NYdbWorkload::IWorkloadQueryGenerator> WorkloadQueryGen;

    // Monitoring
    std::unique_ptr<MonitoringData> Total;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> LoadCounters;
    NMonitoring::TDynamicCounters::TCounterPtr Transactions;
    NMonitoring::TDynamicCounters::TCounterPtr TransactionsBytesWritten;

};

IActor * CreateKqpLoadActor(const NKikimr::TEvLoadTestRequest::TKqpLoad& cmd,
        const TActorId& parent, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters, ui64 index, ui64 tag) {
    return new TKqpLoadActor(cmd, parent, counters, index, tag);
}

} // NKikimr
