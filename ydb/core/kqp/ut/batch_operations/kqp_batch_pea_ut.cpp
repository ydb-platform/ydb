#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/tx/data_events/events.h>

#include <limits>

using namespace NYdb;
using namespace NYdb::NQuery;

namespace NKikimr::NKqp {

namespace {

TKikimrSettings GetDefaultSettings() {
    auto appConfig = NKikimrConfig::TAppConfig();
    appConfig.MutableTableServiceConfig()->SetEnableOltpSink(true);
    appConfig.MutableTableServiceConfig()->SetEnableBatchUpdates(true);
    return TKikimrSettings(std::move(appConfig))
        .SetWithSampleTables(false)
        .SetUseRealThreads(false);
}

void CreateTable(TKikimrRunner& kikimr, TSession& session, const std::string_view& tableName, size_t partitionCount) {
    const auto query = std::format(R"(
        CREATE TABLE `{}` (
            Key Uint32 NOT NULL,
            Value String NOT NULL,
            PRIMARY KEY (Key)
        ) WITH (
            UNIFORM_PARTITIONS = {}
        );
    )", tableName, partitionCount);

    const auto result = kikimr.RunCall([&]{
        return session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
    });
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

void FillTable(TKikimrRunner& kikimr, TSession& session, const std::string_view& tableName, size_t partitionCount, size_t rowsPerPartition) {
    for (size_t i = 0; i < partitionCount; ++i) {
        auto query = std::format(R"(
            UPSERT INTO `{}` (Key, Value) VALUES 
        )", tableName);

        const auto maxKey = std::numeric_limits<uint32_t>::max();
        for (size_t j = 0; j < rowsPerPartition; ++j) {
            query += std::format(R"(({}, "Value{}"))", i * (maxKey / partitionCount) + j, i);
            if (j < rowsPerPartition - 1) {
                query += ", ";
            }
        }
        query += ";";

        const auto result = kikimr.RunCall([&]{
            return session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
        });
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }
}

}  // namespace

/**
 * Tests for successful and failure scenarios for KqpPartitionedExecuterActor.
*/
Y_UNIT_TEST_SUITE(KqpBatchPEA) {
    Y_UNIT_TEST(PrepareState_PartitioningResolutionError) {
        auto kikimr = TKikimrRunner(GetDefaultSettings());
        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        auto db = kikimr.RunCall([&] { return kikimr.GetQueryClient(); });
        auto session = kikimr.RunCall([&] { return db.GetSession().GetValueSync().GetSession(); });

        const std::string_view tableName = "SampleTable";
        const size_t partitionCount = 4;
        const size_t rowsPerPartition = 5;

        CreateTable(kikimr, session, tableName, partitionCount);
        FillTable(kikimr, session, tableName, partitionCount, rowsPerPartition);

        size_t queryId = 0;

        using TTestEvent = TEvTxProxySchemeCache::TEvResolveKeySetResult;
        const auto observer = runtime.AddObserver<TTestEvent>([&](TTestEvent::TPtr& ev) {
            if (runtime.FindActorName(ev->GetRecipientRewrite()) != "KQP_PARTITIONED_EXECUTER") {
                return;
            }

            auto* request = ev->Get()->Request.Get();
            switch (queryId++) {
                case 0: {
                    // TEvResolveKeySetResult returns an error
                    request->ErrorCount = 1;
                    break;
                }

                case 1: {
                    // TEvResolveKeySetResult has no result set
                    request->ResultSet.clear();
                    break;
                }

                case 2: {
                    // TEvResolveKeySetResult has null partitioning (basic ptr checks)
                    request->ResultSet[0].KeyDescription->Partitioning = nullptr;
                    break;
                }

                case 3: {
                    // TEvResolveKeySetResult has empty partitioning (why not? o_O)
                    const_cast<TVector<TKeyDesc::TPartitionInfo>&>(*request->ResultSet[0].KeyDescription->Partitioning).clear();
                    break;
                }

                default: {
                    UNIT_FAIL("Unexpected queryId: " << queryId);
                    break;
                }
            }
        });

        const auto executeAndTestError = [&](const std::string& query, const std::vector<std::string>& expectedIssues) {
            const auto result = kikimr.RunCall([&]{
                return db.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            });

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::INTERNAL_ERROR, result.GetIssues().ToString());
            for (const auto& expectedIssue : expectedIssues) {
                UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), expectedIssue, result.GetIssues().ToString());
            }
        };

        const std::string batchQuery = std::format(R"(
            BATCH UPDATE `{}` SET Value = "Updated";
        )", tableName);

        // queryId = 0
        executeAndTestError(batchQuery, {"could not resolve a partitioning of the table", "state = PrepareState"});
        // queryId = 1
        executeAndTestError(batchQuery, {"could not resolve a partitioning of the table, resultSet is empty", "state = PrepareState"});
        // queryId = 2
        executeAndTestError(batchQuery, {"could not resolve a partitioning of the table, partitioning is null", "state = PrepareState"});
        // queryId = 3
        executeAndTestError(batchQuery, {"could not resolve a partitioning of the table, partitioning is empty", "state = PrepareState"});
    }

    Y_UNIT_TEST(PrepareState_AbortExecution) {
        auto kikimr = TKikimrRunner(GetDefaultSettings());
        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        auto db = kikimr.RunCall([&] { return kikimr.GetQueryClient(); });
        auto session = kikimr.RunCall([&] { return db.GetSession().GetValueSync().GetSession(); });

        const std::string_view tableName = "SampleTable";
        const size_t partitionCount = 4;
        const size_t rowsPerPartition = 5;

        CreateTable(kikimr, session, tableName, partitionCount);
        FillTable(kikimr, session, tableName, partitionCount, rowsPerPartition);

        using TTestEvent = TEvTxProxySchemeCache::TEvResolveKeySetResult;
        const auto observer = runtime.AddObserver<TTestEvent>([&](TTestEvent::TPtr& ev) {
            if (runtime.FindActorName(ev->GetRecipientRewrite()) == "KQP_PARTITIONED_EXECUTER") {
                // There is only one sender of abort execution in this state: SessionActor, but it does not matter
                auto abort = TEvKqp::TEvAbortExecution::Aborted("Test abort execution");
                runtime.Send(new IEventHandle(ev->Recipient, ev->Sender, abort.Release()));

                // Drop the event to stay in the PrepareState and get abort execution
                ev.Reset();
            }
        });

        const std::string batchQuery = std::format(R"(
            BATCH UPDATE `{}` SET Value = "Updated";
        )", tableName);

        const auto result = kikimr.RunCall([&]{
            return db.ExecuteQuery(batchQuery, TTxControl::NoTx()).GetValueSync();
        });

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::ABORTED, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Test abort execution", result.GetIssues().ToString());
        // Issues only contain forwarded messages from abort event
        // UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "state = PrepareState", result.GetIssues().ToString());
    }

    Y_UNIT_TEST(PrepareState_UnknownEvent) {
        auto kikimr = TKikimrRunner(GetDefaultSettings());
        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        auto db = kikimr.RunCall([&] { return kikimr.GetQueryClient(); });
        auto session = kikimr.RunCall([&] { return db.GetSession().GetValueSync().GetSession(); });

        const std::string_view tableName = "SampleTable";
        const size_t partitionCount = 4;
        const size_t rowsPerPartition = 5;

        CreateTable(kikimr, session, tableName, partitionCount);
        FillTable(kikimr, session, tableName, partitionCount, rowsPerPartition);

        using TTestEvent = TEvTxProxySchemeCache::TEvResolveKeySetResult;
        const auto observer = runtime.AddObserver<TTestEvent>([&](TTestEvent::TPtr& ev) {
            if (runtime.FindActorName(ev->GetRecipientRewrite()) == "KQP_PARTITIONED_EXECUTER") {
                runtime.Send(new IEventHandle(ev->Recipient, ev->Sender, new TEvents::TEvWakeup()));

                // Drop the event to stay in the PrepareState and get the unknown event
                ev.Reset();
            }
        });

        const std::string batchQuery = std::format(R"(
            BATCH UPDATE `{}` SET Value = "Updated";
        )", tableName);

        const auto result = kikimr.RunCall([&]{
            return db.ExecuteQuery(batchQuery, TTxControl::NoTx()).GetValueSync();
        });

        // An error is expected because the actor is in the PrepareState and does not know what to do with the unknown event
        // It is needed to check that new events are not ignored and the actor is aborted
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::INTERNAL_ERROR, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "got an unknown message", result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "state = PrepareState", result.GetIssues().ToString());
    }

    Y_UNIT_TEST(ExecuteState_AbortBeforeAnyResponse) {
        auto kikimr = TKikimrRunner(GetDefaultSettings());
        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        auto db = kikimr.RunCall([&] { return kikimr.GetQueryClient(); });
        auto session = kikimr.RunCall([&] { return db.GetSession().GetValueSync().GetSession(); });

        const std::string_view tableName = "SampleTable";
        const size_t partitionCount = 4;
        const size_t rowsPerPartition = 5;

        CreateTable(kikimr, session, tableName, partitionCount);
        FillTable(kikimr, session, tableName, partitionCount, rowsPerPartition);

        std::optional<TActorId> partitionedId;
        std::set<TActorId> executerIds;
        bool abortSent = false;

        // Get id of the PartitionedExecuterActor
        using TEvTestGetPartitioned = TEvTxProxySchemeCache::TEvResolveKeySetResult;
        const auto partitionedObserver = runtime.AddObserver<TEvTestGetPartitioned>([&](TEvTestGetPartitioned::TPtr& ev) {
            if (runtime.FindActorName(ev->GetRecipientRewrite()) == "KQP_PARTITIONED_EXECUTER" && !partitionedId.has_value()) {
                partitionedId = ev->Recipient;
            }
        });

        // Get id of the Executers
        using TEvTestGetExecuter = TEvTxUserProxy::TEvProposeKqpTransaction;
        const auto executerObserver = runtime.AddObserver<TEvTestGetExecuter>([&](TEvTestGetExecuter::TPtr& ev) {
            if (partitionedId.has_value() && ev->Sender == *partitionedId) {
                executerIds.insert(ev->Get()->ExecuterId);
            }
        });

        using TEvCaptureExecution = TEvKqpExecuter::TEvTxRequest;
        const auto captureObserver = runtime.AddObserver<TEvCaptureExecution>([&](TEvCaptureExecution::TPtr& ev) {
            if (partitionedId.has_value() && ActorIdFromProto(ev->Get()->Record.GetTarget()) == *partitionedId) {
                UNIT_ASSERT_C(executerIds.find(ev->Recipient) != executerIds.end(), "Executer actor is not found");

                // Send abort to the PartitionedExecuterActor while the executer actors are not finished yet
                if (!abortSent) {
                    auto abort = TEvKqp::TEvAbortExecution::Aborted("Test abort before any response");
                    runtime.Send(new IEventHandle(*partitionedId, ev->Recipient, abort.Release()));
                    abortSent = true;
                }

                // Events are dropped to freeze the execution
                ev.Reset();
            }
        });

        const std::string batchQuery = std::format(R"(
            BATCH UPDATE `{}` SET Value = "Updated";
        )", tableName);

        const auto result = kikimr.RunCall([&]{
            return db.ExecuteQuery(batchQuery, TTxControl::NoTx()).GetValueSync();
        });

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::ABORTED, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Test abort before any response", result.GetIssues().ToString());
    }

    Y_UNIT_TEST(ExecuteState_AbortAfterPartialCompletion) {
        auto kikimr = TKikimrRunner(GetDefaultSettings());
        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        auto db = kikimr.RunCall([&] { return kikimr.GetQueryClient(); });
        auto session = kikimr.RunCall([&] { return db.GetSession().GetValueSync().GetSession(); });

        const std::string_view tableName = "SampleTable";
        const size_t partitionCount = 4;
        const size_t rowsPerPartition = 5;

        CreateTable(kikimr, session, tableName, partitionCount);
        FillTable(kikimr, session, tableName, partitionCount, rowsPerPartition);

        std::optional<TActorId> partitionedId;
        std::set<TActorId> executerIds;
        std::set<TActorId> skippedExecuterIds;
        bool abortSent = false;

        // Get id of the PartitionedExecuterActor
        using TEvTestGetPartitioned = TEvTxProxySchemeCache::TEvResolveKeySetResult;
        const auto partitionedObserver = runtime.AddObserver<TEvTestGetPartitioned>([&](TEvTestGetPartitioned::TPtr& ev) {
            if (runtime.FindActorName(ev->GetRecipientRewrite()) == "KQP_PARTITIONED_EXECUTER" && !partitionedId.has_value()) {
                partitionedId = ev->Recipient;
            }
        });

        // Get id of the Executers
        using TEvTestGetExecuter = TEvTxUserProxy::TEvProposeKqpTransaction;
        const auto executerObserver = runtime.AddObserver<TEvTestGetExecuter>([&](TEvTestGetExecuter::TPtr& ev) {
            if (partitionedId.has_value() && ev->Sender == *partitionedId) {
                executerIds.insert(ev->Get()->ExecuterId);
            }
        });

        using TEvCaptureExecution = TEvKqpExecuter::TEvTxRequest;
        const auto captureObserver = runtime.AddObserver<TEvCaptureExecution>([&](TEvCaptureExecution::TPtr& ev) {
            if (partitionedId.has_value() && ActorIdFromProto(ev->Get()->Record.GetTarget()) == *partitionedId) {
                UNIT_ASSERT_C(executerIds.find(ev->Recipient) != executerIds.end(), "Executer actor is not found");

                if (skippedExecuterIds.find(ev->Recipient) != skippedExecuterIds.end()) {
                    return;
                }

                // Allow first half to complete
                if (skippedExecuterIds.size() < partitionCount / 2) {
                    skippedExecuterIds.insert(ev->Recipient);
                    return;
                }

                // Send abort to the PartitionedExecuterActor while half of the ExecuterActors are not finished
                if (!abortSent) {
                    auto abort = TEvKqp::TEvAbortExecution::Aborted("Test abort after partial completion");
                    runtime.Send(new IEventHandle(*partitionedId, ev->Recipient, abort.Release()));
                    abortSent = true;
                }

                // Events are dropped to freeze the execution
                ev.Reset();
            }
        });

        const std::string batchQuery = std::format(R"(
            BATCH UPDATE `{}` SET Value = "Updated";
        )", tableName);

        const auto result = kikimr.RunCall([&]{
            return db.ExecuteQuery(batchQuery, TTxControl::NoTx()).GetValueSync();
        });

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::ABORTED, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Test abort after partial completion", result.GetIssues().ToString());
    }

    Y_UNIT_TEST(ExecuteState_ChildExecuterAbort) {
        auto kikimr = TKikimrRunner(GetDefaultSettings());
        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        auto db = kikimr.RunCall([&] { return kikimr.GetQueryClient(); });
        auto session = kikimr.RunCall([&] { return db.GetSession().GetValueSync().GetSession(); });

        const std::string_view tableName = "SampleTable";
        const size_t partitionCount = 4;
        const size_t rowsPerPartition = 5;

        CreateTable(kikimr, session, tableName, partitionCount);
        FillTable(kikimr, session, tableName, partitionCount, rowsPerPartition);

        std::optional<TActorId> partitionedId;
        std::set<TActorId> executerIds;
        std::set<TActorId> capturedExecuterIds;

        // Get id of the PartitionedExecuterActor
        using TEvTestGetPartitioned = TEvTxProxySchemeCache::TEvResolveKeySetResult;
        const auto partitionedObserver = runtime.AddObserver<TEvTestGetPartitioned>([&](TEvTestGetPartitioned::TPtr& ev) {
            if (runtime.FindActorName(ev->GetRecipientRewrite()) == "KQP_PARTITIONED_EXECUTER" && !partitionedId.has_value()) {
                partitionedId = ev->Recipient;
            }
        });

        // Get id of the Executers
        using TEvTestGetExecuter = TEvTxUserProxy::TEvProposeKqpTransaction;
        const auto executerObserver = runtime.AddObserver<TEvTestGetExecuter>([&](TEvTestGetExecuter::TPtr& ev) {
            if (partitionedId.has_value() && ev->Sender == *partitionedId) {
                executerIds.insert(ev->Get()->ExecuterId);
            }
        });

        using TEvCaptureExecution = TEvKqpExecuter::TEvTxRequest;
        const auto captureObserver = runtime.AddObserver<TEvCaptureExecution>([&](TEvCaptureExecution::TPtr& ev) {
            if (partitionedId.has_value() && ActorIdFromProto(ev->Get()->Record.GetTarget()) == *partitionedId) {
                UNIT_ASSERT_C(executerIds.find(ev->Recipient) != executerIds.end(), "Executer actor is not found");

                if (capturedExecuterIds.find(ev->Recipient) != capturedExecuterIds.end()) {
                    return;
                }

                if (capturedExecuterIds.size() != partitionCount - 1) {
                    // Capture all executer actors except the last one
                    capturedExecuterIds.insert(ev->Recipient);
                } else {
                    // Send abort to the last executer from some actor (important: not parent),
                    // it must forward the event to the PartitionedExecuterActor
                    //
                    // There are two ways to get abort with timeout:
                    // 1. From PartitionedExecuterActor or SessionActor (parents, do not forward the event)
                    // 2. From some compute actors (the event forwards to the parent)
                    auto abort = MakeHolder<TEvKqp::TEvAbortExecution>(NYql::NDqProto::StatusIds::TIMEOUT, "Test child executer abort", NYql::TIssues{});
                    runtime.Send(new IEventHandle(ev->Recipient, MakeSchemeCacheID(), abort.Release()));
                }

                // Events are dropped to freeze the execution
                ev.Reset();
            }
        });

        const std::string batchQuery = std::format(R"(
            BATCH UPDATE `{}` SET Value = "Updated";
        )", tableName);

        const auto result = kikimr.RunCall([&]{
            return db.ExecuteQuery(batchQuery, TTxControl::NoTx()).GetValueSync();
        });

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::TIMEOUT, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Test child executer abort", result.GetIssues().ToString());
    }
}

}  // namespace NKikimr::NKqp
