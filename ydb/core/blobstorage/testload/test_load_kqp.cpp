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

#include <ydb/public/lib/operation_id/operation_id.h>
#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/queue.h>
#include <util/random/fast.h>
#include <util/random/shuffle.h>


namespace NKikimr {

class TEvKqpWriterTestLoadActor;

class TKqpWriterTestLoadActor : public TActorBootstrapped<TKqpWriterTestLoadActor> {
    struct TRequestInfo {
        ui32 Size;
        TInstant LogStartTime;
    };

    struct TRequestStat {
        ui64 BytesWrittenTotal;
        ui32 Size;
        TDuration Latency;
    };

    struct TLogWriteCookie {
        ui32 WorkerIdx;
        TInstant SentTime;
        ui64 Size;
    };

    ui64 Key;
    ui64 FirstKey;
    ui64 TotalRowsToUpsert;
    ui64 MaxInFlight;
    ui64 UniformPartitionsCount;
    ui32 NumOfSessions;
    TActorId Pipe;
    std::vector<TString> TxId;

    const TActorId Parent;
    ui64 Tag;
    ui32 DurationSeconds;
    ui32 StringValueSize;
    bool SequentialWrite;
    TString StringValue;
    TString WorkingDir;
    size_t ProductCount;
    size_t Quantity;
    bool DeleteTableOnFinish;
    std::vector<TString> preparedQuery;
    std::shared_ptr<NYdbWorkload::IWorkloadQueryGenerator> WorkloadQueryGen;

    TReallyFastRng32 Rng;

    std::unordered_map<TString, std::queue<TInstant>> SentTime;

    // Monitoring
    TIntrusivePtr<::NMonitoring::TDynamicCounters> LoadCounters;
    ::NMonitoring::TDynamicCounters::TCounterPtr Transactions;
    ::NMonitoring::TDynamicCounters::TCounterPtr TransactionsBytesWritten;
    TInstant TestStartTime;

    TMap<ui64, TLogWriteCookie> InFlightWrites;
    NMonitoring::TPercentileTrackerLg<6, 5, 15> ResponseTimes;
    std::vector<TString> Sessions;
    TString PreparedSelectQuery;

public:
    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::KQP_TEST_WORKLOAD;
    }

    TKqpWriterTestLoadActor(const NKikimrBlobStorage::TEvTestLoadRequest::TKqpLoadStart& cmd,
            const TActorId& parent, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters, ui64 index, ui64 tag)
        : Parent(parent)
        , Tag(tag)
        , Rng(Now().GetValue())
    {
        Y_UNUSED(index);
        VERIFY_PARAM(DurationSeconds);
        DurationSeconds = cmd.GetDurationSeconds();
        NumOfSessions = cmd.GetNumOfSessions();
        MaxInFlight = cmd.GetMaxInFlight();
        Key = cmd.GetFirstKey();
        FirstKey = Key;
        TotalRowsToUpsert = cmd.GetTotalRowsToUpsert();
        StringValueSize = cmd.GetStringValueSize();
        DeleteTableOnFinish = cmd.GetDeleteTableOnFinish();
        UniformPartitionsCount = cmd.GetUniformPartitionsCount();
        SequentialWrite = cmd.GetSequentialWrite();
        StringValue = TString(StringValueSize, 'a');
        WorkingDir = cmd.GetWorkingDir();

        if (cmd.Workload_case() == NKikimrBlobStorage::TEvTestLoadRequest_TKqpLoadStart::WorkloadCase::kStock) {
            ProductCount = cmd.GetStock().GetProductCount();
            Quantity = cmd.GetStock().GetQuantity();
        }

        if (cmd.GetWorkloadName() != "stock") {
            return;
        }

        NYdbWorkload::TStockWorkloadParams params;
        params.DbPath = WorkingDir;
        params.PartitionsByLoad = true;

        NYdbWorkload::TWorkloadFactory factory;
        WorkloadQueryGen = factory.GetWorkloadQueryGenerator(cmd.GetWorkloadName(), &params);
        Y_ASSERT(WorkloadQueryGen.get() != nullptr);
        Y_ASSERT(DurationSeconds > DelayBeforeMeasurements.Seconds());

        // Monitoring initialization
        TVector<float> percentiles {0.1f, 0.5f, 0.9f, 0.99f, 0.999f, 1.0f};
        LoadCounters = counters->GetSubgroup("tag", Sprintf("%" PRIu64, tag));
        Transactions = LoadCounters->GetCounter("Transactions", true);
        TransactionsBytesWritten = LoadCounters->GetCounter("TransactionsBytesWritten", true);

        ResponseTimes.Initialize(LoadCounters, "subsystem", "LoadActorLogWriteDuration", "Time in microseconds", percentiles);
    }

    ~TKqpWriterTestLoadActor() {
        LoadCounters->ResetCounters();
    }


    void Bootstrap(const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag
                << " TKqpWriterTestLoadActor Bootstrap called");
        Become(&TKqpWriterTestLoadActor::StateFunc);
        LOG_INFO_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag << " Schedule PoisonPill");
        ctx.Schedule(TDuration::Seconds(DurationSeconds), new TEvents::TEvPoisonPill);
        ctx.Schedule(TDuration::MilliSeconds(MonitoringUpdateCycleMs), new TEvUpdateMonitoring);

        LOG_INFO_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag << " Bootstrap");

        CreateSession(ctx);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Death management
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    void HandlePoisonPill(const TActorContext& ctx) {
        LOG_INFO_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag << " HandlePoisonPill, "
                << "all workers is initialized, so starting death process");
        StartDeathProcess(ctx);
    }

    void StartDeathProcess(const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tag# " << Tag
                << " TKqpWriterTestLoadActor StartDeathProcess called");
        for(const auto& session : Sessions) {
            auto request = std::make_unique<NKqp::TEvKqp::TEvCloseSessionRequest>();
            request->Record.MutableRequest()->SetSessionId(session);
            ctx.Send( new IEventHandle(NKqp::MakeKqpProxyID(1), SelfId(), request.release(),
                    0, /* via actor system */ true));
        }

        Become(&TKqpWriterTestLoadActor::StateEndOfWork);
        TIntrusivePtr<TLoadReport> Report(new TLoadReport());
        Report->Duration = TDuration::Seconds(DurationSeconds);
        ctx.Send(Parent, new TEvTestLoadFinished(Tag, Report, "OK called StartDeathProcess"));
        Die(ctx);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Monitoring
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    void Handle(TEvUpdateMonitoring::TPtr& /*ev*/, const TActorContext& ctx) {
        ResponseTimes.Update();
        ctx.Schedule(TDuration::MilliSeconds(MonitoringUpdateCycleMs), new TEvUpdateMonitoring);
    }

    STRICT_STFUNC(StateFunc,
        CFunc(TEvents::TSystem::PoisonPill, HandlePoisonPill)
        HFunc(TEvUpdateMonitoring, Handle)
    )

    STRICT_STFUNC(StateEndOfWork,
        HFunc(TEvUpdateMonitoring, Handle)
    )

private:

    void CreateSession(const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Creating session to run tables DDL.");
        auto request = Ydb::Table::CreateSessionRequest();
        auto cb = [this, &ctx](const Ydb::Table::CreateSessionResponse& resp) {
            auto op = resp.Getoperation();
            if (op.Getready()) {
                auto status = op.Getstatus();
                if (status == Ydb::StatusIds_StatusCode_SUCCESS) {
                    Ydb::Table::CreateSessionResult result;
                    op.result().UnpackTo(&result);
                    TString sessionId = result.session_id();
                    LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Session is created: " + sessionId);
                    CreateShardedTable(ctx, sessionId);
                } else {
                    LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Session creation failed " + std::to_string((int)status)
                        + " Issue: " + op.Getissues(0).Getmessage());
                }
            }
        };
        using namespace NGRpcService;
        using TEvCreateSessionRequest = TGrpcRequestOperationCall<Ydb::Table::CreateSessionRequest,
            Ydb::Table::CreateSessionResponse>;
        NKikimr::NRpcService::DoLocalRpcSameMailbox<TEvCreateSessionRequest>(
            std::move(request), std::move(cb), WorkingDir, TString(), ctx
        );
    }

    void CreateShardedTable(const TActorContext& ctx, const TString& sessionId) {
        LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Creating tables for workload.");
        auto request = Ydb::Table::ExecuteSchemeQueryRequest();
        request.set_session_id(sessionId);
        request.set_yql_text(WorkloadQueryGen->GetDDLQueries());
        auto cb = [&ctx](const Ydb::Table::ExecuteSchemeQueryResponse& resp) {
            auto op = resp.Getoperation();
            if (op.Getready()) {
                auto status = op.Getstatus();
                if (status == Ydb::StatusIds_StatusCode_SUCCESS) {
                    LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tables are created.");
                } else {
                    LOG_DEBUG_S(ctx, NKikimrServices::KQP_LOAD_TEST, "Tables creation failed " + std::to_string((int)status)
                        + " Issue: " + op.Getissues(0).Getmessage());
                }
            }
        };
        using namespace NGRpcService;
        using TEvExecuteSchemeQueryRequest = TGrpcRequestOperationCall<Ydb::Table::ExecuteSchemeQueryRequest,
            Ydb::Table::ExecuteSchemeQueryResponse>;
        NKikimr::NRpcService::DoLocalRpcSameMailbox<TEvExecuteSchemeQueryRequest>(
            std::move(request), std::move(cb), WorkingDir, TString(), ctx
        );
    }

};

IActor * CreateKqpWriterTestLoad(const NKikimrBlobStorage::TEvTestLoadRequest::TKqpLoadStart& cmd,
        const TActorId& parent, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters, ui64 index, ui64 tag) {
    return new TKqpWriterTestLoadActor(cmd, parent, counters, index, tag);
}

} // NKikimr
