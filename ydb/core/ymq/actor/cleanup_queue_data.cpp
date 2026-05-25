#include "cleanup_queue_data.h"

#include <ydb/core/ymq/actor/cfg/cfg.h>
#include <ydb/core/ymq/base/run_query.h>
#include <ydb/core/ymq/queues/common/key_hashes.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::SQS



namespace NKikimr::NSQS {
    constexpr TDuration LOCK_PERIOD = TDuration::Seconds(30);
    constexpr TDuration UPDATE_LOCK_PERIOD = TDuration::Seconds(20);

    constexpr TDuration IDLE_TIMEOUT = TDuration::Seconds(30);
    constexpr TDuration RETRY_PERIOD_MIN = TDuration::MilliSeconds(100);
    constexpr TDuration RETRY_PERIOD_MAX = TDuration::Seconds(30);

    // (table, has shard column)
    const TVector<std::pair<TString, bool>> StdTables = {
        {"Infly", true},
        {"MessageData", true},
        {"Messages", true},
        {"SentTimestampIdx", true},
        {"Attributes", false}
    };

    const TVector<std::pair<TString, bool>> FifoTables = {
        {"Attributes", false},
        {"Data", false},
        {"Deduplication", false},
        {"Groups", false},
        {"Messages", false},
        {"Reads", false},
        {"SentTimestampIdx", false}
    };

    TString GetCommonTablePath(const TString& name, bool isFifo) {
        return TStringBuilder() << Cfg().GetRoot() << "/." << (isFifo ? "FIFO" : "STD") << "/" << name;
    }

    TCleanupQueueDataActor::TCleanupQueueDataActor(TIntrusivePtr<TMonitoringCounters> monitoringCounters)
        : MonitoringCounters(monitoringCounters)
        , RetryPeriod(RETRY_PERIOD_MIN)
    {}

    void TCleanupQueueDataActor::Bootstrap(const TActorContext& ctx) {
        Become(&TCleanupQueueDataActor::StateFunc);

        TString removedQueuesTable = Cfg().GetRoot() + "/.RemovedQueues";

        LockQueueQuery = TStringBuilder() << R"__(
            --!syntax_v1
            DECLARE $StartProcessTimestamp as Uint64; DECLARE $NodeId as Uint32;
            DECLARE $LockFreeTimestamp as Uint64;
            $to_update = (
                SELECT
                    RemoveTimestamp, QueueIdNumber,
                    $StartProcessTimestamp AS StartProcessTimestamp, $NodeId AS NodeProcess
                FROM `)__" << removedQueuesTable <<  R"__(`
                WHERE StartProcessTimestamp < $LockFreeTimestamp OR StartProcessTimestamp IS NULL OR NodeProcess = $NodeId
                ORDER BY RemoveTimestamp LIMIT 1
            );
            UPDATE `)__" << removedQueuesTable <<  R"__(` ON SELECT * FROM $to_update;
        )__";

        UpdateLockQueueQuery = TStringBuilder() << R"__(
            --!syntax_v1
            DECLARE $StartProcessTimestamp as Uint64; DECLARE $NodeId as Uint32;
            DECLARE $RemoveTimestamp as Uint64; DECLARE $QueueIdNumber as Uint64;
            DECLARE $Now as Uint64;
            $to_update = (
                SELECT
                    RemoveTimestamp, QueueIdNumber, $Now AS StartProcessTimestamp
                FROM `)__" << removedQueuesTable <<  R"__(`
                WHERE RemoveTimestamp=$RemoveTimestamp AND QueueIdNumber=$QueueIdNumber
                    AND  StartProcessTimestamp = $StartProcessTimestamp AND NodeProcess = $NodeId
            );
            UPDATE `)__" << removedQueuesTable <<  R"__(` ON SELECT * FROM $to_update;
        )__";


        SelectQueuesQuery = TStringBuilder() << R"__(
            --!syntax_v1
            DECLARE $StartProcessTimestamp as Uint64; DECLARE $NodeId as Uint32;
            SELECT RemoveTimestamp, QueueIdNumber, FifoQueue, Shards, TablesFormat
                FROM `)__" << removedQueuesTable <<  R"__(`
                WHERE StartProcessTimestamp = $StartProcessTimestamp AND NodeProcess = $NodeId LIMIT 1;
        )__";

        RemoveQueueFromListQuery = TStringBuilder() << R"__(
            --!syntax_v1
            DECLARE $RemoveTimestamp as Uint64; DECLARE $QueueIdNumber as Uint64;
            DELETE FROM `)__" << removedQueuesTable <<  R"__(`
                WHERE RemoveTimestamp=$RemoveTimestamp AND QueueIdNumber=$QueueIdNumber
        )__";

        LockQueueToRemove(TDuration::Zero(), ctx);
    }

    void TCleanupQueueDataActor::RunGetQueuesQuery(EState state, TDuration sendAfter, const TActorContext& ctx) {
        YDB_LOG_CTX_DEBUG(ctx, "[cleanup removed queues] getting queues...");
        State = state;

        NYdb::TParams params = NYdb::TParamsBuilder()
            .AddParam("$StartProcessTimestamp")
                .Uint64(StartProcessTimestamp.Seconds())
                .Build()
            .AddParam("$NodeId")
                .Uint32(SelfId().NodeId())
                .Build()
            .Build();

        RunYqlQuery(SelectQueuesQuery, std::move(params), true, sendAfter, ctx);
    }

    void TCleanupQueueDataActor::HandleQueryResponse(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record;
        if (record.GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
            HandleError(record.DebugString(), ctx);
            return;
        }
        RetryPeriod = RETRY_PERIOD_MIN;
        auto& response = record.GetResponse();

        switch(State) {
            case EState::LockQueue: {
                RunGetQueuesQuery(EState::GetQueue, TDuration::Zero(), ctx);
                break;
            }
            case EState::UpdateLockQueue: {
                RunGetQueuesQuery(EState::GetQueueAfterLockUpdate, TDuration::Zero(), ctx);
                break;
            }
            case EState::GetQueueAfterLockUpdate:
            case EState::GetQueue: {
                Y_ABORT_UNLESS(response.YdbResultsSize() == 1);
                NYdb::TResultSetParser parser(response.GetYdbResults(0));
                if (parser.RowsCount() == 0) {
                    YDB_LOG_CTX_DEBUG(ctx, "[cleanup removed queues] there are no queues to delete");
                    LockQueueToRemove(IDLE_TIMEOUT, ctx);
                    return;
                }
                Y_ABORT_UNLESS(parser.RowsCount() == 1);
                parser.TryNextRow();
                if (State == EState::GetQueueAfterLockUpdate) {
                    ContinueRemoveData(parser, ctx);
                } else {
                    StartRemoveData(parser, ctx);
                }
                return;
            }
            case EState::RemoveData: {
                Y_ABORT_UNLESS(response.YdbResultsSize() == 1);
                NYdb::TResultSetParser parser(response.GetYdbResults(0));
                ui64 removedRows = 0;
                if (parser.TryNextRow()) {
                    removedRows = parser.ColumnParser(0).GetUint64();
                }
                OnRemovedData(removedRows, ctx);
                break;
            }
            case EState::Finish: {
                YDB_LOG_CTX_INFO(ctx, "[cleanup removed queues] queue data ) removed successfuly.",
                    {"(queue_id_number", QueueIdNumber});
                MonitoringCounters->CleanupRemovedQueuesDone->Inc();
                LockQueueToRemove(TDuration::Zero(), ctx);
                break;
            }
        }
    }

    void TCleanupQueueDataActor::HandleError(const TString& error, const TActorContext& ctx) {
        MonitoringCounters->CleanupRemovedQueuesErrors->Inc();
        auto runAfter = RetryPeriod;
        RetryPeriod = Min(RetryPeriod * 2, RETRY_PERIOD_MAX);
        YDB_LOG_CTX_ERROR(ctx, "[cleanup removed queues] got an error while deleting data",
            {"error", error});
        LockQueueToRemove(runAfter, ctx);
    }

    void TCleanupQueueDataActor::LockQueueToRemove(TDuration runAfter, const TActorContext& ctx) {
        State = EState::LockQueue;

        StartProcessTimestamp = ctx.Now();

        NYdb::TParams params = NYdb::TParamsBuilder()
            .AddParam("$StartProcessTimestamp")
                .Uint64(StartProcessTimestamp.Seconds())
                .Build()
            .AddParam("$LockFreeTimestamp")
                .Uint64((StartProcessTimestamp - LOCK_PERIOD).Seconds())
                .Build()
            .AddParam("$NodeId")
                .Uint32(SelfId().NodeId())
                .Build()
            .Build();

        RunYqlQuery(LockQueueQuery, std::move(params), false, runAfter, ctx);
    }

    void TCleanupQueueDataActor::UpdateLock(const TActorContext& ctx) {
        YDB_LOG_CTX_DEBUG(ctx, "[cleanup removed queues] update queue lock...");
        State = EState::UpdateLockQueue;

        NYdb::TParamsBuilder paramsBuilder;
        paramsBuilder
            .AddParam("$StartProcessTimestamp")
                .Uint64(StartProcessTimestamp.Seconds())
                .Build()
            .AddParam("$NodeId")
                .Uint32(SelfId().NodeId())
                .Build()
            .AddParam("$RemoveTimestamp")
                .Uint64(RemoveQueueTimetsamp)
                .Build()
            .AddParam("$QueueIdNumber")
                .Uint64(QueueIdNumber)
                .Build();

        StartProcessTimestamp = ctx.Now();
        paramsBuilder
            .AddParam("$Now")
                .Uint64(StartProcessTimestamp.Seconds())
                .Build();

        NYdb::TParams params = paramsBuilder.Build();

        RunYqlQuery(UpdateLockQueueQuery, std::move(params), false, TDuration::Zero(), ctx);
    }

    void TCleanupQueueDataActor::ContinueRemoveData(NYdb::TResultSetParser& parser, const TActorContext& ctx) {
        // Select RemoveTimestamp, QueueIdNumber, FifoQueue, Shards, TablesFormat
        ui64 queueIdNumber = *parser.ColumnParser(1).GetOptionalUint64();
        if (queueIdNumber != QueueIdNumber) {
            YDB_LOG_CTX_WARN(ctx, "[cleanup removed queues] got queue to continue remove data, but was locked",
                {"queue_id_number", queueIdNumber},
                {"#_queue_id_number", QueueIdNumber});
            StartRemoveData(parser, ctx);
            return;
        }

        State = EState::RemoveData;
        RunRemoveData(ctx);
    }

    void TCleanupQueueDataActor::StartRemoveData(NYdb::TResultSetParser& parser, const TActorContext& ctx) {
        State = EState::RemoveData;
        ClearedTablesCount = 0;

        // Select RemoveTimestamp, QueueIdNumber, FifoQueue, Shards, TablesFormat
        RemoveQueueTimetsamp = *parser.ColumnParser(0).GetOptionalUint64();
        QueueIdNumber = *parser.ColumnParser(1).GetOptionalUint64();
        IsFifoQueue = *parser.ColumnParser(2).GetOptionalBool();
        Shards = *parser.ColumnParser(3).GetOptionalUint32();
        TablesFormat = *parser.ColumnParser(4).GetOptionalUint32();

        YDB_LOG_CTX_INFO(ctx, "[cleanup removed queues] got queue to remove data: removed at",
            {"RemoveQueueTimetsamp", RemoveQueueTimetsamp},
            {"queue_id_number", QueueIdNumber},
            {"tables_format", TablesFormat});
        if (TablesFormat == 0) {
            Finish(ctx); // TODO move code for removing directories
        } else {
            ClearNextTable(ctx);
        }
    }

    void TCleanupQueueDataActor::OnRemovedData(ui64 removedRows, const TActorContext& ctx) {
        YDB_LOG_CTX_DEBUG(ctx, "[cleanup removed queues] removed rows, cleared tables, shards to remove",
            {"removedRows", removedRows},
            {"ClearedTablesCount", ClearedTablesCount},
            {"ShardsToRemove", ShardsToRemove});
        MonitoringCounters->CleanupRemovedQueuesRows->Add(removedRows);
        if (removedRows == 0) {
            if (ShardsToRemove) {
                --ShardsToRemove;
            }
            if (ShardsToRemove == 0) {
                ++ClearedTablesCount;
                ClearNextTable(ctx);
                return;
            }
        }
        RunRemoveData(ctx);
    }

    void TCleanupQueueDataActor::Finish(const TActorContext& ctx) {
        State = EState::Finish;

        NYdb::TParams params = NYdb::TParamsBuilder()
            .AddParam("$RemoveTimestamp")
                .Uint64(RemoveQueueTimetsamp)
                .Build()
            .AddParam("$QueueIdNumber")
                .Uint64(QueueIdNumber)
                .Build()
            .Build();

        RunYqlQuery(RemoveQueueFromListQuery, std::move(params), false, TDuration::Zero(), ctx);
    }

    std::optional<std::pair<TString, bool>> TCleanupQueueDataActor::GetNextTable() const {
        const auto& tables = IsFifoQueue ? FifoTables : StdTables;
        if (ClearedTablesCount < tables.size()) {
            return tables[ClearedTablesCount];
        }
        return std::nullopt;
    }

    void TCleanupQueueDataActor::RunRemoveData(const TActorContext& ctx) {
        if (ctx.Now() - StartProcessTimestamp > UPDATE_LOCK_PERIOD) {
            UpdateLock(ctx);
            return;
        }

        ui32 shard = ShardsToRemove ? (ShardsToRemove - 1) : 0;

        NYdb::TParams params = NYdb::TParamsBuilder()
            .AddParam("$QueueIdNumberAndShardHash")
                .Uint64(GetKeysHash(QueueIdNumber, shard))
                .Build()
            .AddParam("$Shard")
                .Uint32(shard)
                .Build()
            .AddParam("$QueueIdNumberHash")
                .Uint64(GetKeysHash(QueueIdNumber))
                .Build()
            .AddParam("$QueueIdNumber")
                .Uint64(QueueIdNumber)
                .Build()
            .Build();

        RunYqlQuery(RemoveDataQuery, std::move(params), false, TDuration::Zero(), ctx);
    }

    void TCleanupQueueDataActor::ClearNextTable(const TActorContext& ctx) {
        auto tableInfo = GetNextTable();
        if (!tableInfo) {
            Finish(ctx);
            return;
        }
        auto& [tableName, hasShardColumn] = tableInfo.value();
        auto table = GetCommonTablePath(tableName, IsFifoQueue);

        TString condition;
        if (hasShardColumn) {
            ShardsToRemove = Shards;
            condition = "QueueIdNumberAndShardHash = $QueueIdNumberAndShardHash AND QueueIdNumber=$QueueIdNumber AND Shard=$Shard";
        } else {
            ShardsToRemove = 0;
            condition = "QueueIdNumberHash = $QueueIdNumberHash AND QueueIdNumber=$QueueIdNumber";
        }

        RemoveDataQuery = TStringBuilder() << R"__(
            --!syntax_v1
            DECLARE $QueueIdNumberHash as Uint64; DECLARE $QueueIdNumber as Uint64;
            DECLARE $QueueIdNumberAndShardHash as Uint64; DECLARE $Shard as Uint32;
            $to_delete = SELECT * FROM `)__" << table << R"__(`
                WHERE )__" << condition << R"__( LIMIT 1000;

            SELECt COUNT(*) as count FROM $to_delete;

            DELETE FROM `)__" << table << R"__(` ON SELECT * FROM $to_delete;
        )__";

        RunRemoveData(ctx);
    }


} // namespace NKikimr::NSQS
