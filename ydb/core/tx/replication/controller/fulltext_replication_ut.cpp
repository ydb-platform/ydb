#include "index_replication.h"
#include "dst_creator.h"
#include "public_events.h"
#include "private_events.h"

#include <ydb/core/tx/replication/service/service.h>
#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/tx_helpers.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/sequenceproxy/public/events.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/library/testlib/helpers.h>
#include <ydb/library/mkql_proto/protos/minikql.pb.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>
#include <ydb/public/lib/yson_value/ydb_yson_value.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/datetime.h>
#include <util/string/printf.h>
#include <util/system/mutex.h>

#include <atomic>

namespace NKikimr::NReplication::NController {
namespace {

struct TReplicationSettings {
    TString Kind;
    bool Compact = false;
    bool Global = false;
    bool InitialIndex = false;
    bool JsonDocument = false;
    bool Finish = false;
    bool Stall = false;
    bool CustomKey = false;
    bool DropDuringScan = false;
    bool PrefixAndCovering = false;
    bool Attach = false;
    bool RaceDiscovery = false;
    bool SplitMerge = false;
    bool DropDuringReadiness = false;
    bool PauseBeforeFinish = false;
    bool FinishDuringInitialization = false;
    bool RestartDuringCancellation = false;
};

void RunReplication(const TReplicationSettings& settings) {
    const auto& [kind, compact, global, initialIndex, jsonDocument, finish, stall, customKey,
        dropDuringScan, prefixAndCovering, attach, raceDiscovery, splitMerge, dropDuringReadiness,
        pauseBeforeFinish, finishDuringInitialization, restartDuringCancellation] = settings;
    NTestHelpers::TFeatureFlags flags;
    flags.FeatureFlags.SetEnableFulltextIndexReplication(true);
    flags.FeatureFlags.SetEnableAsyncReplicationSchemaChanges(true);
    flags.FeatureFlags.SetEnableCompactFulltextIndex(compact);
    NTestHelpers::TEnv env(flags, true, stall || raceDiscovery || finish || splitMerge || dropDuringReadiness || finishDuringInitialization
        ? NTestHelpers::ERuntimeMode::Simulated : NTestHelpers::ERuntimeMode::RealThreads);
    auto& runtime = env.GetRuntime();
    // These integration cases run several CDC workers alongside the full
    // server. The default scheduling budget is sized for smaller actor tests.
    runtime.SetScheduledLimit(5'000'000);
    runtime.SetDispatchedEventsLimit(50'000'000);
    if (attach) {
        runtime.GetAppData().ReplicationConfig.SetInitialScan(false);
    }
    runtime.RegisterService(MakeReplicationServiceId(runtime.GetNodeId(0)), runtime.Register(CreateReplicationService()));
    NYdb::NQuery::TQueryClient db(env.GetDriver());

    auto query = [&](const TString& text) {
        return env.RunCall([&] { return db.ExecuteQuery(text, NYdb::NQuery::TTxControl::NoTx()).GetValueSync(); });
    };
    auto pause = [&] {
        if (runtime.IsRealThreads()) {
            Sleep(TDuration::MilliSeconds(100));
        } else {
            runtime.SimulateSleep(TDuration::MilliSeconds(100));
        }
    };
    auto execute = [&](const TString& text) {
        const auto deadline = TInstant::Now() + TDuration::Seconds(90);
        while (true) {
            auto result = query(text);
            if (result.IsSuccess()) {
                return result;
            }
            if (result.GetStatus() != NYdb::EStatus::OVERLOADED && result.GetStatus() != NYdb::EStatus::ABORTED) {
                UNIT_FAIL(text << ": " << result.GetIssues().ToString());
            }
            UNIT_ASSERT_C(TInstant::Now() < deadline, text << ": " << result.GetIssues().ToString());
            pause();
        }
    };
    auto waitQuery = [&](const TString& text, const TString& expected) {
        const auto deadline = TInstant::Now() + TDuration::Seconds(90);
        TString last;
        do {
            const auto result = query(text);
            last = result.IsSuccess() ? TString(NYdb::FormatResultSetYson(result.GetResultSet(0)))
                : TString(result.GetIssues().ToString());
            if (last == expected) {
                return;
            }
            pause();
        } while (TInstant::Now() < deadline);
        UNIT_FAIL(text << ": expected " << expected << ", got " << last);
    };
    auto compactIndexes = [&](const TString& path) {
        const auto table = env.GetDescription(path).GetPathDescription().GetTable();
        for (const auto& idx : table.GetTableIndexes()) {
            TVector<TString> keys(idx.GetKeyColumnNames().begin(), idx.GetKeyColumnNames().end());
            for (const auto impl : NTableIndex::GetImplTables(idx.GetType(), keys)) {
                const TString implPath = TStringBuilder() << path << "/" << idx.GetName() << "/" << impl;
                const auto desc = env.GetDescription(implPath).GetPathDescription();
                for (const auto& partition : desc.GetTablePartitions()) {
                    const auto deadline = TInstant::Now() + TDuration::Seconds(90);
                    while (true) {
                        const auto result = env.Send<TEvDataShard::TEvCompactTableResult>(partition.GetDatashardId(),
                            new TEvDataShard::TEvCompactTable(env.GetPathId(implPath)));
                        const auto status = result->Get()->Record.GetStatus();
                        if (status == NKikimrTxDataShard::TEvCompactTableResult::OK
                            || status == NKikimrTxDataShard::TEvCompactTableResult::NOT_NEEDED)
                        {
                            break;
                        }
                        Y_ENSURE(status != NKikimrTxDataShard::TEvCompactTableResult::FAILED && TInstant::Now() < deadline,
                            implPath << ": compaction status " << static_cast<ui32>(status));
                        if (status == NKikimrTxDataShard::TEvCompactTableResult::BORROWED) {
                            env.Send<TEvDataShard::TEvCompactBorrowedResult>(partition.GetDatashardId(),
                                new TEvDataShard::TEvCompactBorrowed(env.GetPathId(implPath)));
                        }
                        pause();
                    }
                }
            }
        }
    };
    auto repartition = [&](const TString& path, bool split) {
        const auto deadline = TInstant::Now() + TDuration::Seconds(90);
        while (true) {
            const auto description = env.GetDescription(path).GetPathDescription();
            UNIT_ASSERT_VALUES_EQUAL(description.TablePartitionsSize(), split ? 1 : 2);
            auto request = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
            request->Record.SetExecTimeoutPeriod(Max<ui64>());
            auto& tx = *request->Record.MutableTransaction()->MutableModifyScheme();
            tx.SetOperationType(NKikimrSchemeOp::ESchemeOpSplitMergeTablePartitions);
            tx.SetInternal(true);
            auto& params = *tx.MutableSplitMergeTablePartitions();
            params.SetTablePath(path);
            for (const auto& part : description.GetTablePartitions()) {
                params.AddSourceTabletId(part.GetDatashardId());
            }
            if (split) {
                const auto& table = description.GetTable();
                const ui64 number = 2;
                const TString token = "m";
                TCell boundary;
                bool found = false;
                for (const auto& column : table.GetColumns()) {
                    if (column.GetId() != table.GetKeyColumnIds(0)) {
                        continue;
                    }
                    found = true;
                    if (column.GetType() == "Uint64") {
                        boundary = TCell::Make(number);
                    } else {
                        UNIT_ASSERT_C(column.GetType() == "Utf8" || column.GetType() == "String", column.DebugString());
                        boundary = TCell(token.data(), token.size());
                    }
                }
                UNIT_ASSERT(found);
                params.AddSplitBoundary()->SetSerializedKeyPrefix(TSerializedCellVec::Serialize({boundary}));
            }
            const auto sender = runtime.AllocateEdgeActor();
            runtime.Send(new IEventHandle(MakeTxProxyID(), sender, request.Release()), 0, true);
            const auto result = runtime.GrabEdgeEvent<TEvTxUserProxy::TEvProposeTransactionStatus>(sender);
            const auto schemeStatus = result->Get()->Record.GetSchemeShardStatus();
            if (schemeStatus == NKikimrScheme::StatusNotAvailable || schemeStatus == NKikimrScheme::StatusMultipleModifications
                || schemeStatus == NKikimrScheme::StatusResourceExhausted)
            {
                UNIT_ASSERT_C(TInstant::Now() < deadline, result->Get()->Record.DebugString());
                pause();
                continue;
            }
            UNIT_ASSERT_VALUES_EQUAL_C(result->Get()->Record.GetStatus(),
                TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress, result->Get()->Record.DebugString());
            env.Send<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult>(env.GetSchemeshardId(path),
                new NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion(result->Get()->Record.GetTxId()));
            return;
        }
    };

    const bool json = kind == "json";
    auto keyValue = [customKey](ui64 value) -> TString {
        return customKey ? TString(TStringBuilder() << "'" << value << "'u") : ToString(value);
    };
    auto keyResult = [customKey](ui64 value) -> TString {
        return customKey ? TStringBuilder() << "[[\"" << value << "\"]]" : TStringBuilder() << "[[" << value << "u]]";
    };
    const TString index = TStringBuilder() << "INDEX idx GLOBAL USING " << kind << (prefixAndCovering ? " ON (tenant,value)" : " ON (value)")
        << (prefixAndCovering && !json ? " COVER (payload)" : "")
        << (json ? "" : " WITH (tokenizer=standard, use_filter_lowercase=true)");
    execute(TStringBuilder() << "CREATE TABLE `/Root/source` (key " << (customKey ? "Utf8" : "Uint64") << " NOT NULL, value "
        << (json ? (jsonDocument ? "JsonDocument" : "Json") : "Utf8") << ", tenant Uint32, payload Utf8, PRIMARY KEY (key)"
        << (initialIndex ? TString(TStringBuilder() << ", " << index) : TString()) << ");");
    const TString jsonType = jsonDocument ? "JsonDocument" : "Json";
    const TString original = json ? TString(TStringBuilder() << "CAST(@@{\"tag\":\"cats\"}@@ AS " << jsonType << ")") : TString("'cats chase mice'u");
    const TString updated = json ? TString(TStringBuilder() << "CAST(@@{\"tag\":\"dogs\"}@@ AS " << jsonType << ")") : TString("'dogs chase mice'u");
    if (!stall) {
        execute(TStringBuilder() << "UPSERT INTO `/Root/source` (key,value,tenant,payload) VALUES (" << keyValue(1) << "," << original << ",42,'cover'u);");
    }
    if (initialIndex) {
        compactIndexes("/Root/source");
    }
    if (attach) {
        if (initialIndex) {
            NYdb::NTable::TTableClient tables(env.GetDriver());
            auto session = env.RunCall([&] { return tables.GetSession().GetValueSync(); });
            UNIT_ASSERT_C(session.IsSuccess(), session.GetIssues().ToString());
            auto copied = env.RunCall([&] { return session.GetSession().CopyTable("/Root/source", "/Root/replica").GetValueSync(); });
            UNIT_ASSERT_C(copied.IsSuccess(), copied.GetIssues().ToString());
        } else {
            execute("CREATE TABLE `/Root/replica` (key Uint64 NOT NULL, value Utf8, tenant Uint32, payload Utf8, PRIMARY KEY(key));");
            execute(TStringBuilder() << "UPSERT INTO `/Root/replica` (key,value,tenant,payload) VALUES (1,"
                << original << ",42,'cover'u);");
        }
    }
    if (stall) {
        execute("CREATE TABLE `/Root/other` (key Uint64 NOT NULL, value Uint64, PRIMARY KEY(key));");
        if (attach) {
            execute("CREATE TABLE `/Root/other_replica` (key Uint64 NOT NULL, value Uint64, PRIMARY KEY(key));");
        }
    }
    if (attach) {
        // Consistent copies stand in for imported data. Bootstrap now requires
        // IMPORTED replica metadata on the base and every backing table before
        // the controller can attach and enter STANDBY.
        TVector<TString> paths{"/Root/replica"};
        if (stall) {
            paths.emplace_back("/Root/other_replica");
        }
        const auto table = env.GetDescription("/Root/replica").GetPathDescription().GetTable();
        for (const auto& idx : table.GetTableIndexes()) {
            TVector<TString> keys(idx.GetKeyColumnNames().begin(), idx.GetKeyColumnNames().end());
            for (const auto impl : NTableIndex::GetImplTables(idx.GetType(), keys)) {
                paths.emplace_back(TStringBuilder() << "/Root/replica/" << idx.GetName() << "/" << impl);
            }
        }
        ui64 importTxId = 100500;
        for (const auto& path : paths) {
            const auto schemeShardId = env.GetSchemeshardId(path);
            auto request = MakeHolder<NSchemeShard::TEvSchemeShard::TEvModifySchemeTransaction>(++importTxId, schemeShardId);
            auto& tx = *request->Record.AddTransaction();
            tx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterTable);
            tx.SetInternal(true);
            env.GetPathId(path).ToProto(tx.MutableAlterTable()->MutablePathId());
            auto& config = *tx.MutableAlterTable()->MutableReplicationConfig();
            FillReplicationConfig(config, EReplicationMode::ReadOnly,
                global ? EConsistencyLevel::Global : EConsistencyLevel::Row);
            config.SetImportState(NKikimrSchemeOp::TTableReplicationConfig::IMPORTED);
            const auto result = env.Send<NSchemeShard::TEvSchemeShard::TEvModifySchemeTransactionResult>(schemeShardId,
                std::move(request));
            UNIT_ASSERT_VALUES_EQUAL_C(result->Get()->Record.GetStatus(), NKikimrScheme::StatusAccepted,
                result->Get()->Record.GetReason());
            env.Send<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult>(schemeShardId,
                new NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion(importTxId));
        }
    }
    std::atomic<bool> holdProgress{stall || finishDuringInitialization};
    TMutex progressLock;
    ui64 stalledTargetId = 0;
    TVector<TAutoPtr<IEventHandle>> heldProgress;
    auto observer = runtime.AddObserver<TEvService::TEvIndexBuildProgress>(
        [&](TEvService::TEvIndexBuildProgress::TPtr& ev) {
            if (holdProgress.load()) {
                TGuard<TMutex> guard(progressLock);
                if (holdProgress.load()) {
                    if (finishDuringInitialization) {
                        const auto targetId = TWorkerId::Parse(ev->Get()->Record.GetWorker()).TargetId();
                        // The service adds the worker ID before forwarding to
                        // the controller. Earlier hops cannot identify a target.
                        if (!targetId) {
                            return;
                        }
                        if (!stalledTargetId) {
                            stalledTargetId = targetId;
                        }
                        if (targetId != stalledTargetId) {
                            return;
                        }
                    }
                    heldProgress.emplace_back(ev.Release());
                }
            }
        });
    TAutoPtr<IEventHandle> heldReadiness;
    TActorId readinessActor;
    bool readinessStopped = false;
    auto readinessObserver = runtime.AddObserver(
        [&](IEventHandle::TPtr& ev) {
            if ((!dropDuringReadiness && !restartDuringCancellation) || heldReadiness
                || ev->GetTypeRewrite() != TEvPrivate::EvSchemaChangeDstAlterTxId || !ev->HasEvent())
            {
                return;
            }
            const auto* request = dynamic_cast<TEvPrivate::TEvSchemaChangeDstAlterTxId*>(ev->GetBase());
            if (request && request->TargetId != 1) {
                readinessActor = ev->Sender;
                heldReadiness.Reset(ev.Release());
            }
        });
    auto stoppedObserver = runtime.AddObserver<TEvents::TEvPoison>([&](TEvents::TEvPoison::TPtr& ev) {
        readinessStopped |= readinessActor && ev->Recipient == readinessActor;
    });
    TMutex discoveryLock;
    TAutoPtr<IEventHandle> heldDiscovery;
    auto discoveryObserver = runtime.AddObserver<TEvYdbProxy::TEvDescribeTableResponse>(
        [&](TEvYdbProxy::TEvDescribeTableResponse::TPtr& ev) {
            if (raceDiscovery && ev->Get()->Result.IsSuccess()
                && ev->Get()->Result.GetTableDescription().GetIndexDescriptions().empty())
            {
                TGuard<TMutex> guard(discoveryLock);
                if (!heldDiscovery) {
                    heldDiscovery.Reset(ev.Release());
                }
            }
        });
    execute(TStringBuilder() << "CREATE ASYNC REPLICATION `/Root/replication` FOR `/Root/source` AS `/Root/replica`"
        << (stall ? ", `/Root/other` AS `/Root/other_replica`" : "") << " WITH ("
        << "CONNECTION_STRING='grpc://" << env.GetEndpoint() << "/?database=/Root', TOKEN='root@builtin'"
        << (global ? ", CONSISTENCY_LEVEL='GLOBAL', COMMIT_INTERVAL=Interval('PT1S')" : "") << ");");
    if (attach) {
        const auto deadline = TInstant::Now() + TDuration::Seconds(90);
        TVector<TString> paths{"/Root/source"};
        if (stall) {
            paths.emplace_back("/Root/other");
        }
        if (initialIndex) {
            const auto table = env.GetDescription("/Root/source").GetPathDescription().GetTable();
            for (const auto& idx : table.GetTableIndexes()) {
                TVector<TString> keys(idx.GetKeyColumnNames().begin(), idx.GetKeyColumnNames().end());
                for (const auto impl : NTableIndex::GetImplTables(idx.GetType(), keys)) {
                    paths.emplace_back(TStringBuilder() << "/Root/source/" << idx.GetName() << "/" << impl);
                }
            }
        }
        for (const auto& path : paths) {
            while (!env.GetDescription(path).GetPathDescription().GetTable().CdcStreamsSize()) {
                UNIT_ASSERT_C(TInstant::Now() < deadline, "Attachment stream was not created: " << path);
                pause();
            }
        }
        execute("ALTER ASYNC REPLICATION `/Root/replication` SET (STATE='STANDBY');");
        if (!initialIndex) {
            // The copied row predates attachment. Observe a new replicated
            // write before starting ADD INDEX so destination validation has
            // finished against the schema from the copy.
            execute(TStringBuilder() << "UPSERT INTO `/Root/source` (key,value,tenant,payload) VALUES ("
                << keyValue(8) << "," << original << ",42,'cover'u);");
            waitQuery(TStringBuilder() << "SELECT key FROM `/Root/replica` WHERE key=" << keyValue(8) << ";", keyResult(8));
            execute(TStringBuilder() << "DELETE FROM `/Root/source` WHERE key=" << keyValue(8) << ";");
            waitQuery(TStringBuilder() << "SELECT key FROM `/Root/replica` WHERE key=" << keyValue(8) << ";", "[]");
        }
    }
    if (raceDiscovery) {
        const bool observed = env.RunCall([&] {
            const auto deadline = TInstant::Now() + TDuration::Seconds(90);
            while (true) {
                {
                    TGuard<TMutex> guard(discoveryLock);
                    if (heldDiscovery) {
                        return true;
                    }
                }
                if (TInstant::Now() >= deadline) {
                    return false;
                }
                Sleep(TDuration::MilliSeconds(100));
            }
        });
        UNIT_ASSERT_C(observed, "Discovery did not describe the source");
        execute(TStringBuilder() << "ALTER TABLE `/Root/source` ADD " << index << ";");
        discoveryObserver.Remove();
        TGuard<TMutex> guard(discoveryLock);
        runtime.Send(heldDiscovery.Release());
    }
    if (stall) {
        waitQuery("SELECT key FROM `/Root/replica` ORDER BY key;", attach ? keyResult(1) : TString("[]"));
        waitQuery("SELECT key FROM `/Root/other_replica`;", "[]");
        execute(TStringBuilder() << "UPSERT INTO `/Root/source` (key,value,tenant,payload) VALUES (" << keyValue(1)
            << "," << original << ",42,'cover'u);");
    }
    waitQuery("SELECT key FROM `/Root/replica` ORDER BY key;", keyResult(1));
    if (attach) {
        const auto deadline = TInstant::Now() + TDuration::Seconds(90);
        while (!env.GetDescription("/Root/source").GetPathDescription().GetTable().CdcStreamsSize()) {
            UNIT_ASSERT_C(TInstant::Now() < deadline, "Source stream was not created for attachment");
            pause();
        }
    }
    if (!initialIndex && !raceDiscovery) {
        execute(TStringBuilder() << "ALTER TABLE `/Root/source` ADD " << index << ";");
    }

    if (finishDuringInitialization) {
        const auto controllerId = env.GetDescription("/Root/replication").GetPathDescription()
            .GetReplicationDescription().GetControllerId();
        const auto replicationPathId = env.GetPathId("/Root/replication");
        auto deadline = TInstant::Now() + TDuration::Seconds(90);
        // Hold one backing table in FILLING while its peers reach MAKING_READY.
        // The index must be cancelled as a group regardless of target order.
        while (true) {
            NKikimrMiniKQL::TResult result;
            UNIT_ASSERT_VALUES_EQUAL(LocalQuery(runtime, controllerId, R"((
                (let range '('('ReplicationId (Uint64 '1) (Uint64 '1))
                    '('Id (Null) (Void))))
                (return (AsList (SetResult 'Rows (SelectRange 'Targets range '('IndexBuild) '())))))
            )", result), NKikimrProto::OK);
            bool filling = false;
            bool makingReady = false;
            TString phases;
            for (const auto& row : result.GetValue().GetStruct(0).GetOptional().GetStruct(0).GetList()) {
                const auto& value = row.GetStruct(0);
                if (value.HasOptional()) {
                    NKikimrReplication::TIndexBuildState build;
                    UNIT_ASSERT(build.ParseFromString(value.GetOptional().GetBytes()));
                    filling |= build.GetPhase() == NKikimrReplication::TIndexBuildState::FILLING;
                    makingReady |= build.GetPhase() == NKikimrReplication::TIndexBuildState::MAKING_READY;
                    phases += build.ShortDebugString() + "; ";
                }
            }
            if (filling && makingReady) {
                break;
            }
            UNIT_ASSERT_C(TInstant::Now() < deadline, "Backing tables did not reach mixed initialization phases: " << phases);
            pause();
        }
        execute("ALTER ASYNC REPLICATION `/Root/replication` SET (STATE='DONE', FAILOVER_MODE='FORCE');");
        if (restartDuringCancellation) {
            runtime.WaitFor("pending index cancellation", [&] { return bool(heldReadiness); }, TDuration::Seconds(60));
            RebootTablet(runtime, controllerId, env.GetSender());
            readinessObserver.Remove();
            runtime.Send(heldReadiness.Release());
        }
        deadline = TInstant::Now() + TDuration::Seconds(90);
        while (true) {
            auto request = MakeHolder<TEvController::TEvDescribeReplication>();
            replicationPathId.ToProto(request->Record.MutablePathId());
            const auto result = env.Send<TEvController::TEvDescribeReplicationResult>(controllerId, std::move(request));
            UNIT_ASSERT_C(!result->Get()->Record.GetState().HasError(), result->Get()->Record.DebugString());
            if (result->Get()->Record.GetState().HasDone()) {
                break;
            }
            UNIT_ASSERT_C(TInstant::Now() < deadline, "Forced completion stalled with mixed backing-table phases");
            pause();
        }
        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/replica").GetPathDescription().GetTable().TableIndexesSize(), 0);
        execute("UPSERT INTO `/Root/replica` (key,value) VALUES (9,'cats'u);");
        waitQuery("SELECT key FROM `/Root/replica` WHERE key=9;", "[[9u]]");
        execute("DROP ASYNC REPLICATION `/Root/replication`;");
        return;
    }

    const TString predicate = json ? "JSON_VALUE(value,'$.tag' RETURNING Utf8) = 'cats'u"
        : "FulltextMatch(value,'cats')";
    const TString search = TStringBuilder() << "SELECT key FROM `/Root/replica` VIEW idx WHERE " << (prefixAndCovering ? "tenant=42 AND " : "") << predicate << " ORDER BY key;";
    if (stall) {
        const bool observed = env.RunCall([&] {
            const auto deadline = TInstant::Now() + TDuration::Seconds(90);
            while (true) {
                {
                    TGuard<TMutex> guard(progressLock);
                    if (!heldProgress.empty()) {
                        return true;
                    }
                }
                if (TInstant::Now() >= deadline) {
                    return false;
                }
                Sleep(TDuration::MilliSeconds(100));
            }
        });
        UNIT_ASSERT_C(observed, "Index scan did not report progress");
        UNIT_ASSERT_VALUES_EQUAL(env.GetDescription("/Root/replica").GetPathDescription().GetTable()
            .GetTableIndexes(0).GetState(), NKikimrSchemeOp::EIndexStateWriteOnly);
        UNIT_ASSERT(!query(search).IsSuccess());
        if (finish) {
            const auto controllerId = env.GetDescription("/Root/replication").GetPathDescription()
                .GetReplicationDescription().GetControllerId();
            RebootTablet(runtime, controllerId, env.GetSender());
        }
        execute("UPSERT INTO `/Root/other` (key,value) VALUES (7,7);");
        execute(TStringBuilder() << "UPSERT INTO `/Root/source` (key,value,tenant,payload) VALUES (" << keyValue(7) << "," << updated << ",42,'cover'u);");
        waitQuery("SELECT key FROM `/Root/other_replica` ORDER BY key;", "[[7u]]");
        waitQuery(TStringBuilder() << "SELECT key FROM `/Root/replica` WHERE key=" << keyValue(7) << ";", keyResult(7));
        if (dropDuringScan) {
            execute("ALTER TABLE `/Root/source` DROP INDEX idx;");
            const auto deadline = TInstant::Now() + TDuration::Seconds(90);
            while (env.GetDescription("/Root/replica").GetPathDescription().GetTable().TableIndexesSize()) {
                UNIT_ASSERT_C(TInstant::Now() < deadline, "Index was not dropped while its scan was stalled");
                pause();
            }
        }
        holdProgress.store(false);
        TVector<TAutoPtr<IEventHandle>> release;
        {
            TGuard<TMutex> guard(progressLock);
            release.swap(heldProgress);
        }
        for (auto& event : release) {
            runtime.Send(event.Release());
        }
    }
    if (dropDuringReadiness) {
        runtime.WaitFor("pending fulltext index readiness", [&] { return bool(heldReadiness); }, TDuration::Seconds(60));
        UNIT_ASSERT(!query(search).IsSuccess());
        execute("ALTER TABLE `/Root/source` DROP INDEX idx;");
        const auto deadline = TInstant::Now() + TDuration::Seconds(90);
        while (env.GetDescription("/Root/replica").GetPathDescription().GetTable().TableIndexesSize()) {
            UNIT_ASSERT_C(TInstant::Now() < deadline, "Index was not dropped while readiness was pending");
            pause();
        }
        runtime.WaitFor("readiness actor shutdown", [&] { return readinessStopped; }, TDuration::Seconds(60));
        readinessObserver.Remove();
        runtime.Send(heldReadiness.Release());
        execute(TStringBuilder() << "UPSERT INTO `/Root/source` (key,value,tenant,payload) VALUES (9," << original << ",42,'cover'u);");
        waitQuery("SELECT key FROM `/Root/replica` WHERE key=9;", "[[9u]]");
        execute("DROP ASYNC REPLICATION `/Root/replication`;");
        return;
    }
    if (dropDuringScan) {
        const auto deadline = TInstant::Now() + TDuration::Seconds(90);
        while (env.GetDescription("/Root/replica").GetPathDescription().GetTable().TableIndexesSize()) {
            UNIT_ASSERT_C(TInstant::Now() < deadline, "Index was not dropped during initialization");
            pause();
        }
        execute(TStringBuilder() << "UPSERT INTO `/Root/source` (key,value,tenant,payload) VALUES (9," << original << ",42,'cover'u);");
        waitQuery("SELECT key FROM `/Root/replica` WHERE key=9;", "[[9u]]");
        execute("DROP ASYNC REPLICATION `/Root/replication`;");
        return;
    }
    waitQuery(search, keyResult(1));
    const TVector<TString> partitionedPaths{
        "/Root/source", "/Root/source/idx/indexImplTable", "/Root/replica", "/Root/replica/idx/indexImplTable"};
    if (splitMerge) {
        for (const auto& path : partitionedPaths) {
            repartition(path, true);
        }
    }
    if (prefixAndCovering && !json) {
        const TString covered = TStringBuilder() << "SELECT key,payload FROM `/Root/replica` VIEW idx WHERE tenant=42 AND "
            << predicate << " ORDER BY key;";
        waitQuery(covered, "[[1u;[\"cover\"]]]");
    }
    if (finish) {
        const auto controllerId = env.GetDescription("/Root/replication").GetPathDescription()
            .GetReplicationDescription().GetControllerId();
        RebootTablet(runtime, controllerId, env.GetSender());
    }
    execute(TStringBuilder() << "UPSERT INTO `/Root/source` (key,value,tenant,payload) VALUES (" << keyValue(1) << "," << updated << ",42,'cover'u),(" << keyValue(2) << "," << original << ",42,'cover'u);");
    waitQuery(search, keyResult(2));
    if (kind == "fulltext_relevance") {
        const TString score = "SELECT key, FulltextScore(value,'cats') AS score FROM `%s` VIEW idx "
            "WHERE %sFulltextScore(value,'cats') > 0 ORDER BY key;";
        const auto source = execute(Sprintf(score.c_str(), "/Root/source", prefixAndCovering ? "tenant=42 AND " : ""));
        waitQuery(Sprintf(score.c_str(), "/Root/replica", prefixAndCovering ? "tenant=42 AND " : ""), TString(NYdb::FormatResultSetYson(source.GetResultSet(0))));
    }
    compactIndexes("/Root/replica");
    compactIndexes("/Root/source");
    waitQuery(search, keyResult(2));
    if (splitMerge) {
        for (const auto& path : partitionedPaths) {
            repartition(path, false);
        }
        waitQuery(search, keyResult(2));
    }
    execute(TStringBuilder() << "DELETE FROM `/Root/source` WHERE key=" << keyValue(2) << ";");
    waitQuery(search, "[]");

    const auto description = env.GetDescription("/Root/replica").GetPathDescription().GetTable();
    UNIT_ASSERT_VALUES_EQUAL(description.TableIndexesSize(), customKey ? 2 : 1);
    bool foundIndex = false;
    for (const auto& idx : description.GetTableIndexes()) {
        foundIndex |= NTableIndex::IsFulltextOrJsonIndex(idx.GetType());
    }
    UNIT_ASSERT(foundIndex);
    if (finish) {
        if (pauseBeforeFinish) {
            execute("ALTER ASYNC REPLICATION `/Root/replication` SET (STATE='PAUSED');");
            const auto deadline = TInstant::Now() + TDuration::Seconds(90);
            const auto controllerId = env.GetDescription("/Root/replication").GetPathDescription()
                .GetReplicationDescription().GetControllerId();
            const auto replicationPathId = env.GetPathId("/Root/replication");
            while (true) {
                auto request = MakeHolder<TEvController::TEvDescribeReplication>();
                replicationPathId.ToProto(request->Record.MutablePathId());
                const auto result = env.Send<TEvController::TEvDescribeReplicationResult>(controllerId, std::move(request));
                UNIT_ASSERT_C(!result->Get()->Record.GetState().HasError(), result->Get()->Record.DebugString());
                if (result->Get()->Record.GetState().HasPaused()) {
                    break;
                }
                UNIT_ASSERT_C(TInstant::Now() < deadline, "Replication did not pause before forced completion");
                pause();
            }
        }
        execute("ALTER ASYNC REPLICATION `/Root/replication` SET (STATE='DONE', FAILOVER_MODE='FORCE');");
        const auto deadline = TInstant::Now() + TDuration::Seconds(90);
        const auto controllerId = env.GetDescription("/Root/replication").GetPathDescription()
            .GetReplicationDescription().GetControllerId();
        const auto replicationPathId = env.GetPathId("/Root/replication");
        auto isDone = [&] {
            auto request = MakeHolder<TEvController::TEvDescribeReplication>();
            replicationPathId.ToProto(request->Record.MutablePathId());
            const auto result = env.Send<TEvController::TEvDescribeReplicationResult>(controllerId, std::move(request));
            UNIT_ASSERT_C(!result->Get()->Record.GetState().HasError(), result->Get()->Record.DebugString());
            return result->Get()->Record.GetState().HasDone();
        };
        while (!isDone()) {
            UNIT_ASSERT_C(TInstant::Now() < deadline, "Replication did not finish detaching its destination tables");
            pause();
        }
        while (true) {
            const auto result = query(TStringBuilder() << "UPSERT INTO `/Root/replica` (key,value,tenant,payload) VALUES (" << keyValue(3) << ","
                << original << ",42,'cover'u);");
            if (result.IsSuccess()) {
                break;
            }
            UNIT_ASSERT_C(TInstant::Now() < deadline, result.GetIssues().ToString());
            pause();
        }
        if (customKey) {
            const auto rows = execute("SELECT key,__ydb_row_id FROM `/Root/replica` ORDER BY key;");
            const auto unique = execute("SELECT key,__ydb_row_id FROM `/Root/replica` VIEW `__ydb_unique_row_id` ORDER BY key;");
            UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(rows.GetResultSet(0)),
                NYdb::FormatResultSetYson(unique.GetResultSet(0)));
        }
        waitQuery(search, keyResult(3));
        execute(TStringBuilder() << "UPSERT INTO `/Root/replica` (key,value,tenant,payload) VALUES (" << keyValue(3) << "," << updated << ",42,'cover'u);");
        waitQuery(search, "[]");
    } else {
        execute("ALTER TABLE `/Root/source` DROP INDEX idx;");
        const auto deadline = TInstant::Now() + TDuration::Seconds(90);
        while (env.GetDescription("/Root/replica").GetPathDescription().GetTable().TableIndexesSize() > (customKey ? 1 : 0)) {
            UNIT_ASSERT_C(TInstant::Now() < deadline, "Destination index was not dropped");
            pause();
        }
    }
    execute("DROP ASYNC REPLICATION `/Root/replication`;");
}

void RunLayoutMismatch(const TString& kind, bool compact, bool initialIndex) {
    NTestHelpers::TFeatureFlags sourceFlags;
    sourceFlags.FeatureFlags.SetEnableFulltextIndexReplication(true);
    sourceFlags.FeatureFlags.SetEnableAsyncReplicationSchemaChanges(true);
    sourceFlags.FeatureFlags.SetEnableCompactFulltextIndex(compact);
    auto destinationFlags = sourceFlags;
    destinationFlags.FeatureFlags.SetEnableCompactFulltextIndex(!compact);
    NTestHelpers::TEnv source(sourceFlags);
    NTestHelpers::TEnv destination(destinationFlags);
    auto& runtime = destination.GetRuntime();
    runtime.RegisterService(MakeReplicationServiceId(runtime.GetNodeId(0)), runtime.Register(CreateReplicationService()));
    NYdb::NQuery::TQueryClient src(source.GetDriver()), dst(destination.GetDriver());
    auto execute = [](NYdb::NQuery::TQueryClient& client, const TString& query) {
        auto result = client.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), query << ": " << result.GetIssues().ToString());
    };
    const bool json = kind == "json";
    const TString index = TStringBuilder() << "INDEX idx GLOBAL USING " << kind << " ON (value)"
        << (json ? "" : " WITH (tokenizer=standard, use_filter_lowercase=true)");
    execute(src, TStringBuilder() << "CREATE TABLE `/Root/source` (key Uint64 NOT NULL, value "
        << (json ? "Json" : "Utf8") << ", PRIMARY KEY(key)"
        << (initialIndex ? TString(TStringBuilder() << ", " << index) : TString()) << ");");
    execute(src, TStringBuilder() << "UPSERT INTO `/Root/source` (key,value) VALUES (1,"
        << (json ? "CAST(@@{\"tag\":\"cats\"}@@ AS Json)" : "'cats'u") << ");");
    execute(dst, TStringBuilder() << "CREATE ASYNC REPLICATION `/Root/replication` FOR `/Root/source` AS `/Root/replica` WITH ("
        << "CONNECTION_STRING='grpc://" << source.GetEndpoint() << "/?database=/Root', TOKEN='root@builtin');");
    if (!initialIndex) {
        const auto deadline = TInstant::Now() + TDuration::Seconds(90);
        while (true) {
            const auto result = dst.ExecuteQuery("SELECT key FROM `/Root/replica`;", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
            if (result.IsSuccess() && result.GetResultSet(0).RowsCount() == 1) {
                break;
            }
            UNIT_ASSERT_C(TInstant::Now() < deadline, result.GetIssues().ToString());
            Sleep(TDuration::MilliSeconds(100));
        }
        execute(src, TStringBuilder() << "ALTER TABLE `/Root/source` ADD " << index << ";");
    }
    const TString search = TStringBuilder() << "SELECT key FROM `/Root/replica` VIEW idx WHERE "
        << (json ? "JSON_VALUE(value,'$.tag' RETURNING Utf8)='cats'u" : "FulltextMatch(value,'cats')") << ";";
    const auto deadline = TInstant::Now() + TDuration::Seconds(90);
    while (true) {
        const auto result = dst.ExecuteQuery(search, NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        if (result.IsSuccess() && NYdb::FormatResultSetYson(result.GetResultSet(0)) == "[[1u]]") {
            break;
        }
        UNIT_ASSERT_C(TInstant::Now() < deadline, result.GetIssues().ToString());
        Sleep(TDuration::MilliSeconds(100));
    }
    const auto srcIndex = source.GetDescription("/Root/source").GetPathDescription().GetTable().GetTableIndexes(0);
    const auto dstIndex = destination.GetDescription("/Root/replica").GetPathDescription().GetTable().GetTableIndexes(0);
    UNIT_ASSERT_VALUES_EQUAL(srcIndex.GetType(), dstIndex.GetType());
    execute(dst, "DROP ASYNC REPLICATION `/Root/replication`;");
}

} // namespace

Y_UNIT_TEST_SUITE(FulltextReplication) {
    Y_UNIT_TEST(SourceIndexWaitIsVisibleDuringDiscovery) {
        NTestHelpers::TFeatureFlags flags;
        flags.FeatureFlags.SetEnableFulltextIndexReplication(true);
        NTestHelpers::TEnv env(flags, true, NTestHelpers::ERuntimeMode::Simulated);
        auto& runtime = env.GetRuntime();
        NYdb::NQuery::TQueryClient client(env.GetDriver());
        const auto execute = [&](const TString& text) {
            const auto result = env.RunCall([&] {
                return client.ExecuteQuery(text, NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
            });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        };
        execute(R"(
            CREATE TABLE `/Root/source` (key Uint64 NOT NULL, value Utf8, PRIMARY KEY(key),
                INDEX idx GLOBAL USING fulltext_plain ON (value) WITH (tokenizer=standard));
        )");
        bool building = true;
        auto observer = runtime.AddObserver<TEvYdbProxy::TEvDescribeTableResponse>(
            [&](TEvYdbProxy::TEvDescribeTableResponse::TPtr& ev) {
                const auto* actor = runtime.FindActor(ev->GetRecipientRewrite());
                if (!building || !actor || !ev->Get()->Result.IsSuccess()
                    || actor->GetActivityType() != NKikimrServices::TActivity::REPLICATION_CONTROLLER_TARGET_DISCOVERER)
                {
                    return;
                }
                auto table = NYdb::TProtoAccessor::GetProto(ev->Get()->Result.GetTableDescription());
                table.mutable_indexes(0)->set_status(Ydb::Table::TableIndexDescription::STATUS_BUILDING);
                TEvYdbProxy::TEvDescribeTableResponse replacement(NYdb::TStatus(NYdb::EStatus::SUCCESS, {}),
                    std::move(table), NYdb::NTable::TDescribeTableSettings());
                ev->Get()->Result = std::move(replacement.Result);
            });
        execute(TStringBuilder() << "CREATE ASYNC REPLICATION `/Root/replication` FOR `/Root/source` AS `/Root/replica` WITH ("
            << "CONNECTION_STRING='grpc://" << env.GetEndpoint() << "/?database=/Root', TOKEN='root@builtin');");
        const auto controller = env.GetDescription("/Root/replication").GetPathDescription()
            .GetReplicationDescription().GetControllerId();
        const auto monitoring = [&] {
            NActorsProto::TRemoteHttpInfo request;
            request.SetMethod(HTTP_METHOD_GET);
            request.SetPath("/app");
            return env.Send<NMon::TEvRemoteHttpInfoRes>(controller,
                new NMon::TEvRemoteHttpInfo(std::move(request)))->Get()->Html;
        };
        const TString reason = "Waiting for source fulltext/JSON index builds on /Root/source: idx";
        const auto deadline = TInstant::Now() + TDuration::Seconds(30);
        while (!monitoring().Contains(reason)) {
            UNIT_ASSERT_C(TInstant::Now() < deadline, "Source index wait was not visible in controller monitoring");
            runtime.SimulateSleep(TDuration::MilliSeconds(100));
        }
        // Completion must clear the diagnostic even if a transient progress
        // notification is lost or belongs to a retired discoverer.
        auto clearObserver = runtime.AddObserver<TEvPrivate::TEvSourceStatus>(
            [&](TEvPrivate::TEvSourceStatus::TPtr& ev) {
                if (!ev->Get()->TargetId && ev->Get()->Issue.empty()) {
                    ev.Reset();
                }
            });
        building = false;
        while (env.GetDescription("/Root/replica").GetSchemeStatus() != NKikimrScheme::StatusSuccess) {
            UNIT_ASSERT_C(TInstant::Now() < deadline, "Destination creation did not resume after the source index became ready");
            runtime.SimulateSleep(TDuration::MilliSeconds(100));
        }
        UNIT_ASSERT(!monitoring().Contains(reason));
    }

    Y_UNIT_TEST_TWIN(DescribeTableSurvivesSequenceLookupFailure, Transient) {
        NTestHelpers::TFeatureFlags flags;
        flags.FeatureFlags.SetEnableFulltextIndexReplication(true);
        NTestHelpers::TEnv env(flags, true, NTestHelpers::ERuntimeMode::Simulated);
        auto& runtime = env.GetRuntime();
        NYdb::NTable::TTableClient client(env.GetDriver());
        auto sessionResult = env.RunCall([&] { return client.GetSession().GetValueSync(); });
        UNIT_ASSERT_C(sessionResult.IsSuccess(), sessionResult.GetIssues().ToString());
        auto session = sessionResult.GetSession();
        const auto create = env.RunCall([&] {
            return session.ExecuteSchemeQuery(R"(
                CREATE TABLE `/Root/source` (key Utf8 NOT NULL, value Utf8, PRIMARY KEY(key),
                    INDEX idx GLOBAL USING fulltext_plain ON (value) WITH (tokenizer=standard));
            )").GetValueSync();
        });
        UNIT_ASSERT_C(create.IsSuccess(), create.GetIssues().ToString());
        runtime.GetAppData().FeatureFlags.SetEnableFulltextIndexReplication(false);
        ui32 lookups = 0;
        auto observer = runtime.AddObserver<NSequenceProxy::TEvSequenceProxy::TEvGetSequence>(
            [&](NSequenceProxy::TEvSequenceProxy::TEvGetSequence::TPtr& ev) {
                ++lookups;
                NYql::TIssues issues;
                issues.AddIssue(NYql::TIssue("injected sequence lookup failure"));
                runtime.Send(new IEventHandle(ev->Sender, ev->GetRecipientRewrite(),
                    new NSequenceProxy::TEvSequenceProxy::TEvGetSequenceResult(
                        Transient ? Ydb::StatusIds::UNAVAILABLE : Ydb::StatusIds::SCHEME_ERROR, issues),
                    0, ev->Cookie));
                ev.Reset();
            });
        const auto result = env.RunCall([&] {
            return session.DescribeTable("/Root/source", NYdb::NTable::TDescribeTableSettings().WithSetVal(true)).GetValueSync();
        });
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT(lookups);
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "injected sequence lookup failure");
        bool found = false;
        const auto& table = NYdb::TProtoAccessor::GetProto(result.GetTableDescription());
        UNIT_ASSERT_VALUES_EQUAL(table.indexes_size(), 2);
        for (const auto& column : table.columns()) {
            if (column.name() == NTableIndex::NFulltext::RowIdColumn) {
                UNIT_ASSERT(column.has_from_sequence());
                UNIT_ASSERT(!column.from_sequence().has_set_val());
                found = true;
            }
        }
        UNIT_ASSERT(found);
    }

    Y_UNIT_TEST_TWIN(PlainOnline, Compact) {
        RunReplication({.Kind = "fulltext_plain", .Compact = Compact});
        RunReplication({.Kind = "fulltext_plain", .Compact = Compact, .Global = true});
    }

    Y_UNIT_TEST_TWIN(RelevanceOnline, Compact) {
        RunReplication({.Kind = "fulltext_relevance", .Compact = Compact});
        RunReplication({.Kind = "fulltext_relevance", .Compact = Compact, .Global = true});
    }

    Y_UNIT_TEST_TWIN(JsonOnline, Compact) {
        RunReplication({.Kind = "json", .Compact = Compact});
        RunReplication({.Kind = "json", .Compact = Compact, .Global = true, .JsonDocument = true});
    }

    Y_UNIT_TEST_TWIN(ExistingIndexes, Compact) {
        RunReplication({.Kind = "fulltext_plain", .Compact = Compact, .InitialIndex = true});
        RunReplication({.Kind = "fulltext_relevance", .Compact = Compact, .Global = true, .InitialIndex = true});
        RunReplication({.Kind = "json", .Compact = Compact, .Global = true, .InitialIndex = true});
    }

    Y_UNIT_TEST_TWIN(RecoveryAndCompletion, Compact) {
        RunReplication({.Kind = "fulltext_relevance", .Compact = Compact, .Global = true, .Finish = true});
        RunReplication({.Kind = "json", .Compact = Compact, .JsonDocument = true, .Finish = true});
    }

    Y_UNIT_TEST_TWIN(CompletionFromPaused, Compact) {
        RunReplication({.Kind = "fulltext_relevance", .Compact = Compact, .Global = true,
            .Finish = true, .PauseBeforeFinish = true});
        RunReplication({.Kind = "json", .Compact = Compact, .JsonDocument = true,
            .Finish = true, .CustomKey = true, .PauseBeforeFinish = true});
    }

    Y_UNIT_TEST_TWIN(CompletionWithMixedBackingTablePhases, Compact) {
        RunReplication({.Kind = "fulltext_relevance", .Compact = Compact, .FinishDuringInitialization = true});
        RunReplication({.Kind = "fulltext_relevance", .Compact = Compact, .Global = true,
            .FinishDuringInitialization = true, .RestartDuringCancellation = true});
    }

    Y_UNIT_TEST_TWIN(CustomPrimaryKey, Compact) {
        RunReplication({.Kind = "fulltext_plain", .Compact = Compact, .InitialIndex = true, .Finish = true, .CustomKey = true});
    }

    Y_UNIT_TEST_TWIN(OnlineCustomPrimaryKey, Compact) {
        RunReplication({.Kind = "fulltext_relevance", .Compact = Compact, .Global = true, .Finish = true, .CustomKey = true});
    }

    Y_UNIT_TEST_TWIN(JsonCustomPrimaryKey, Compact) {
        RunReplication({.Kind = "json", .Compact = Compact, .JsonDocument = true, .Finish = true, .CustomKey = true});
    }

    Y_UNIT_TEST_TWIN(PrefixAndCoveringColumns, Compact) {
        RunReplication({.Kind = "fulltext_plain", .Compact = Compact, .PrefixAndCovering = true});
        if (Compact) {
            RunReplication({.Kind = "fulltext_relevance", .Compact = true, .Global = true, .InitialIndex = true, .PrefixAndCovering = true});
        }
        RunReplication({.Kind = "json", .Compact = Compact, .Global = true, .PrefixAndCovering = true});
    }

    Y_UNIT_TEST_TWIN(SourceIndexFinishesDuringDiscovery, Compact) {
        RunReplication({.Kind = "fulltext_relevance", .Compact = Compact, .Global = true, .RaceDiscovery = true});
        RunReplication({.Kind = "json", .Compact = Compact, .RaceDiscovery = true});
    }

    Y_UNIT_TEST_TWIN(OnlineAddAfterAttachment, Compact) {
        RunReplication({.Kind = "fulltext_relevance", .Compact = Compact, .Stall = true, .Attach = true});
        RunReplication({.Kind = "fulltext_relevance", .Compact = Compact, .Global = true, .Stall = true, .Attach = true});
    }

    Y_UNIT_TEST_TWIN(CopiedIndexAttachment, Compact) {
        RunReplication({.Kind = "fulltext_relevance", .Compact = Compact, .Global = true,
            .InitialIndex = true, .Finish = true, .Attach = true});
        RunReplication({.Kind = "json", .Compact = Compact, .InitialIndex = true,
            .JsonDocument = true, .Finish = true, .Attach = true});
    }

    Y_UNIT_TEST_TWIN(RecoveryDuringInitialization, Compact) {
        RunReplication({.Kind = "fulltext_relevance", .Compact = Compact, .Global = true, .Finish = true, .Stall = true});
        RunReplication({.Kind = "json", .Compact = Compact, .JsonDocument = true, .Finish = true, .Stall = true});
    }

    Y_UNIT_TEST_TWIN(SplitAndMerge, Compact) {
        RunReplication({.Kind = "fulltext_relevance", .Compact = Compact, .Global = true, .SplitMerge = true});
        RunReplication({.Kind = "json", .Compact = Compact, .JsonDocument = true, .SplitMerge = true});
    }

    Y_UNIT_TEST_TWIN(DropDuringScan, Compact) {
        RunReplication({.Kind = "fulltext_relevance", .Compact = Compact, .Global = true, .Stall = true, .DropDuringScan = true});
    }

    Y_UNIT_TEST_TWIN(DropDuringReadiness, Compact) {
        RunReplication({.Kind = "fulltext_relevance", .Compact = Compact, .Global = true, .DropDuringReadiness = true});
        RunReplication({.Kind = "json", .Compact = Compact, .DropDuringReadiness = true});
    }

    Y_UNIT_TEST_TWIN(DifferentDestinationLayout, Compact) {
        for (const TString kind : {"fulltext_plain", "fulltext_relevance", "json"}) {
            RunLayoutMismatch(kind, Compact, true);
            RunLayoutMismatch(kind, Compact, false);
        }
    }

    Y_UNIT_TEST_TWIN(StalledScanDoesNotBlockOtherTables, Compact) {
        RunReplication({.Kind = "fulltext_relevance", .Compact = Compact, .Global = true, .Stall = true});
    }
}

} // namespace NKikimr::NReplication::NController
