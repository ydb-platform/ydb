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

        using TEvTestResolve = TEvTxProxySchemeCache::TEvResolveKeySetResult;
        const auto observer = runtime.AddObserver<TEvTestResolve>([&](TEvTestResolve::TPtr& ev) {
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

        using TEvTestResolve = TEvTxProxySchemeCache::TEvResolveKeySetResult;
        const auto observer = runtime.AddObserver<TEvTestResolve>([&](TEvTestResolve::TPtr& ev) {
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

        using TEvTestResolve = TEvTxProxySchemeCache::TEvResolveKeySetResult;
        const auto observer = runtime.AddObserver<TEvTestResolve>([&](TEvTestResolve::TPtr& ev) {
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

        using TEvTestCapture = TEvKqpExecuter::TEvTxRequest;
        const auto captureObserver = runtime.AddObserver<TEvTestCapture>([&](TEvTestCapture::TPtr& ev) {
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

        using TEvTestCapture = TEvKqpExecuter::TEvTxRequest;
        const auto captureObserver = runtime.AddObserver<TEvTestCapture>([&](TEvTestCapture::TPtr& ev) {
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

        using TEvTestCapture = TEvKqpExecuter::TEvTxRequest;
        const auto captureObserver = runtime.AddObserver<TEvTestCapture>([&](TEvTestCapture::TPtr& ev) {
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

    Y_UNIT_TEST(ExecuteState_AbortBeforeDelayedResponses) {
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
        TVector<TAutoPtr<IEventHandle>> delayedResponses;
        bool enableCapture = true;

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

        // Capture TEvTxResponse from executers to partitioned executer
        using TEvTestCapture = TEvKqpExecuter::TEvTxResponse;
        const auto captureObserver = runtime.AddObserver<TEvTestCapture>([&](TEvTestCapture::TPtr& ev) {
            if (enableCapture && partitionedId.has_value() && ev->Recipient == *partitionedId) {
                // Capture and drop the response to send them later
                UNIT_ASSERT_C(executerIds.find(ev->Sender) != executerIds.end(), "Executer actor is not found");
                delayedResponses.emplace_back(ev.Release());
            }
        });

        TDispatchOptions opts;
        opts.FinalEvents.emplace_back([&delayedResponses](IEventHandle&) {
            // Wait until all 4 responses are captured (one per partition)
            return delayedResponses.size() >= partitionCount;
        });

        const std::string batchQuery = std::format(R"(
            BATCH UPDATE `{}` SET Value = "Updated";
        )", tableName);

        auto queryFuture = kikimr.RunInThreadPool([&] {
            return db.ExecuteQuery(batchQuery, TTxControl::NoTx()).GetValueSync();
        });

        // Wait for all responses to be captured
        runtime.DispatchEvents(opts);
        UNIT_ASSERT_VALUES_EQUAL_C(delayedResponses.size(), partitionCount, "All responses should be captured");

        // Send abort to partitioned executer while responses are delayed
        auto abort = TEvKqp::TEvAbortExecution::Aborted("Test abort before delayed responses");
        runtime.Send(new IEventHandle(*partitionedId, MakeSchemeCacheID(), abort.Release()));

        // Disable capture and send delayed responses
        enableCapture = false;
        for (const auto& ev : delayedResponses) {
            runtime.Send(ev);
        }

        const auto result = runtime.WaitFuture(queryFuture);

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::ABORTED, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Test abort before delayed responses", result.GetIssues().ToString());
    }

    Y_UNIT_TEST(ExecuteState_ChildExecuterRetryLimitExceeded) {
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
        bool enableCapture = true;

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

        using TEvTestCapture = TEvKqpExecuter::TEvTxRequest;
        const auto captureObserver = runtime.AddObserver<TEvTestCapture>([&](TEvTestCapture::TPtr& ev) {
            if (partitionedId.has_value() && ActorIdFromProto(ev->Get()->Record.GetTarget()) == *partitionedId) {
                UNIT_ASSERT_C(executerIds.find(ev->Recipient) != executerIds.end(), "Executer actor is not found");

                if (capturedExecuterIds.find(ev->Recipient) != capturedExecuterIds.end()) {
                    return;
                }

                if (capturedExecuterIds.size() != partitionCount - 1) {
                    // Capture all executer actors except the last one
                    capturedExecuterIds.insert(ev->Recipient);
                    // Events are dropped to freeze the execution
                    ev.Reset();
                }
            }
        });

        // Change status of TEvTxResponse to set retriable error status
        using TEvTestResponse = TEvKqpExecuter::TEvTxResponse;
        const auto responseObserver = runtime.AddObserver<TEvTestResponse>([&](TEvTestResponse::TPtr& ev) {
            if (enableCapture && partitionedId.has_value() && ev->Recipient == *partitionedId) {
                UNIT_ASSERT_C(executerIds.find(ev->Sender) != executerIds.end(), "Executer actor is not found");

                if (ev->Get()->Record.GetResponse().GetStatus() != Ydb::StatusIds::ABORTED) {
                    // Set retriable error status to trigger retry
                    ev->Get()->Record.MutableResponse()->SetStatus(Ydb::StatusIds::OVERLOADED);
                } else {
                    enableCapture = false;
                }
            }
        });

        const std::string batchQuery = std::format(R"(
            BATCH UPDATE `{}` SET Value = "Updated";
        )", tableName);

        const auto result = kikimr.RunCall([&]{
            return db.ExecuteQuery(batchQuery, TTxControl::NoTx()).GetValueSync();
        });

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::UNAVAILABLE, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(),
            "Cannot retry query execution because the maximum retry delay has been reached", result.GetIssues().ToString());
    }

    Y_UNIT_TEST(ExecuteState_AbortDuringRetry) {
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
        TVector<TAutoPtr<IEventHandle>> delayedEvents;
        bool enableCapture = true;

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

        // Change status of TEvTxResponse to set retriable error status
        using TEvTestResponse = TEvKqpExecuter::TEvTxResponse;
        const auto responseObserver = runtime.AddObserver<TEvTestResponse>([&](TEvTestResponse::TPtr& ev) {
            if (enableCapture && partitionedId.has_value() && ev->Recipient == *partitionedId) {
                UNIT_ASSERT_C(executerIds.find(ev->Sender) != executerIds.end(), "Executer actor is not found");
                // Set retriable error status to trigger retry
                ev->Get()->Record.MutableResponse()->SetStatus(Ydb::StatusIds::OVERLOADED);
            }
        });

        // Capture TEvTxDelayedExecution to freeze the execution
        using TEvTestCapture = TEvKqpExecuter::TEvTxDelayedExecution;
        const auto captureObserver = runtime.AddObserver<TEvTestCapture>([&](TEvTestCapture::TPtr& ev) {
            if (enableCapture && partitionedId.has_value() && ev->Recipient == *partitionedId) {
                // Capture and drop the response to send them later
                delayedEvents.emplace_back(ev.Release());
            }
        });

        TDispatchOptions opts;
        opts.FinalEvents.emplace_back([&delayedEvents](IEventHandle&) {
            // Wait until all 4 delayed events are captured (one per partition)
            return delayedEvents.size() >= partitionCount;
        });

        const std::string batchQuery = std::format(R"(
            BATCH UPDATE `{}` SET Value = "Updated";
        )", tableName);

        auto queryFuture = kikimr.RunInThreadPool([&] {
            return db.ExecuteQuery(batchQuery, TTxControl::NoTx()).GetValueSync();
        });

        // Wait for all delayed events to be captured
        runtime.DispatchEvents(opts);
        UNIT_ASSERT_VALUES_EQUAL_C(delayedEvents.size(), partitionCount, "All delayed events should be captured");

        // Send abort to partitioned executer while delayed events are captured
        auto abort = TEvKqp::TEvAbortExecution::Aborted("Test abort during retry");
        runtime.Send(new IEventHandle(*partitionedId, MakeSchemeCacheID(), abort.Release()));

        // Disable capture and send delayed events
        enableCapture = false;
        for (const auto& ev : delayedEvents) {
            runtime.Send(ev);
        }

        const auto result = runtime.WaitFuture(queryFuture);

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::ABORTED, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Test abort during retry", result.GetIssues().ToString());
    }

    Y_UNIT_TEST(ExecuteState_ChildExecuterInternalError) {
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

        using TEvTestCapture = TEvKqpExecuter::TEvTxRequest;
        const auto captureObserver = runtime.AddObserver<TEvTestCapture>([&](TEvTestCapture::TPtr& ev) {
            if (partitionedId.has_value() && ActorIdFromProto(ev->Get()->Record.GetTarget()) == *partitionedId) {
                UNIT_ASSERT_C(executerIds.find(ev->Recipient) != executerIds.end(), "Executer actor is not found");

                if (capturedExecuterIds.find(ev->Recipient) != capturedExecuterIds.end()) {
                    return;
                }

                if (capturedExecuterIds.size() != partitionCount - 1) {
                    // Capture all executer actors except the last one
                    capturedExecuterIds.insert(ev->Recipient);
                    // Events are dropped to freeze the execution
                    ev.Reset();
                }
            }
        });

        // Change status of TEvTxResponse to set non-retriable error status
        using TEvTestResponse = TEvKqpExecuter::TEvTxResponse;
        const auto responseObserver = runtime.AddObserver<TEvTestResponse>([&](TEvTestResponse::TPtr& ev) {
            if (partitionedId.has_value() && ev->Recipient == *partitionedId) {
                UNIT_ASSERT_C(executerIds.find(ev->Sender) != executerIds.end(), "Executer actor is not found");

                if (ev->Get()->Record.GetResponse().GetStatus() == Ydb::StatusIds::SUCCESS) {
                    // Set non-retriable error status to trigger immediate abort
                    ev->Get()->Record.MutableResponse()->SetStatus(Ydb::StatusIds::INTERNAL_ERROR);
                    auto* issue = ev->Get()->Record.MutableResponse()->AddIssues();
                    issue->set_message("Test internal error from child executer");
                }
            }
        });

        const std::string batchQuery = std::format(R"(
            BATCH UPDATE `{}` SET Value = "Updated";
        )", tableName);

        const auto result = kikimr.RunCall([&]{
            return db.ExecuteQuery(batchQuery, TTxControl::NoTx()).GetValueSync();
        });

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::INTERNAL_ERROR, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Test internal error from child executer", result.GetIssues().ToString());
    }

    Y_UNIT_TEST(ExecuteState_MinKeyErrorIssues) {
        auto kikimr = TKikimrRunner(GetDefaultSettings());
        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        auto db = kikimr.RunCall([&] { return kikimr.GetQueryClient(); });
        auto session = kikimr.RunCall([&] { return db.GetSession().GetValueSync().GetSession(); });

        const std::string_view tableName = "SampleTable";
        const size_t partitionCount = 4;
        const size_t rowsPerPartition = 5;

        CreateTable(kikimr, session, tableName, partitionCount);
        FillTable(kikimr, session, tableName, partitionCount, rowsPerPartition);

        const size_t maxQueryId = 4;
        size_t queryId = 0;

        std::optional<TActorId> partitionedId;
        std::optional<TActorId> targetExecuterId;
        std::set<TActorId> executerIds;
        bool failureInjected = false;

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

        using TEvTestCapture = TEvKqpExecuter::TEvTxRequest;
        const auto captureObserver = runtime.AddObserver<TEvTestCapture>([&](TEvTestCapture::TPtr& ev) {
            if (partitionedId.has_value() && ActorIdFromProto(ev->Get()->Record.GetTarget()) == *partitionedId) {
                UNIT_ASSERT_C(executerIds.find(ev->Recipient) != executerIds.end(), "Executer actor is not found");

                if (targetExecuterId.has_value()) {
                    // Drop the event to freeze the execution for all executer actors except the first one
                    ev.Reset();
                } else {
                    // Capture the first executer actor to trigger error issues
                    targetExecuterId = ev->Recipient;
                }
            }
        });

        // Change BatchOperationKeyIds and BatchOperationMaxKeys to trigger error issues
        using TEvTestResponse = TEvKqpExecuter::TEvTxResponse;
        const auto responseObserver = runtime.AddObserver<TEvTestResponse>([&](TEvTestResponse::TPtr& ev) {
            if (!targetExecuterId.has_value() || ev->Sender != *targetExecuterId) {
                return;
            }

            if (!partitionedId.has_value() || ev->Recipient != *partitionedId) {
                return;
            }

            if (failureInjected) {
                return;
            }

            UNIT_ASSERT_C(executerIds.find(ev->Sender) != executerIds.end(), "Executer actor is not found");
            failureInjected = true;

            auto* event = ev->Get();
            switch (queryId++) {
                case 0: {
                    // Set invalid key ids
                    event->BatchOperationKeyIds.clear();
                    event->BatchOperationKeyIds.push_back(999);
                    break;
                }

                case 1: {
                    // Set empty key ids with non-empty key rows
                    UNIT_ASSERT_GE_C(event->BatchOperationMaxKeys.size(), 1, "KeyIds should be greater than 0");
                    event->BatchOperationKeyIds.clear();
                    break;
                }

                case 2: {
                    // Add a new invalid key (+inf)
                    auto cells = TSerializedCellVec(TVector<TCell>{});
                    event->BatchOperationMaxKeys.push_back(std::move(cells));
                    break;
                }

                case 3: {
                    // Set only one key that does not belong to the partitions
                    // New value is max because the executer is prepared for the first partition which values are less than the max value
                    auto newKey = TSerializedCellVec(TVector<TCell>{TCell::Make(std::numeric_limits<uint32_t>::max())});

                    event->BatchOperationMaxKeys.clear();
                    event->BatchOperationMaxKeys.push_back(std::move(newKey));
                    break;
                }

                default: {
                    UNIT_FAIL("Unexpected queryId: " << queryId);
                }
            }
        });

        const std::string batchQuery = std::format(R"(
            BATCH UPDATE `{}` SET Value = "Updated";
        )", tableName);

        for (size_t i = 0; i < maxQueryId; ++i) {
            const auto result = kikimr.RunCall([&]{
                return db.ExecuteQuery(batchQuery, TTxControl::NoTx()).GetValueSync();
            });

            if (i + 1 < maxQueryId) {
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::INTERNAL_ERROR, result.GetIssues().ToString());
                UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "got an unknown error", result.GetIssues().ToString());
                UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "state = ExecuteState", result.GetIssues().ToString());

                partitionedId.reset();
                targetExecuterId.reset();
                executerIds.clear();
                failureInjected = false;
            } else {
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
                UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(),
                    "The next key from KqpReadActor does not belong to the partition", result.GetIssues().ToString());
            }
        }

        UNIT_ASSERT_VALUES_EQUAL_C(queryId, maxQueryId, "Unexpected queryId: " << queryId);
    }

    Y_UNIT_TEST(ExecuteState_UnknownEvent) {
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
        bool eventSent = false;

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

        // Capture TEvTxRequest to send unknown event during ExecuteState
        using TEvTestCapture = TEvKqpExecuter::TEvTxRequest;
        const auto captureObserver = runtime.AddObserver<TEvTestCapture>([&](TEvTestCapture::TPtr& ev) {
            if (partitionedId.has_value() && ActorIdFromProto(ev->Get()->Record.GetTarget()) == *partitionedId) {
                UNIT_ASSERT_C(executerIds.find(ev->Recipient) != executerIds.end(), "Executer actor is not found");

                // Send unknown event to the PartitionedExecuterActor
                if (!eventSent) {
                    runtime.Send(new IEventHandle(*partitionedId, ev->Sender, new TEvents::TEvWakeup()));
                    eventSent = true;
                }

                // Drop the event to stay in the ExecuteState and get the unknown event
                ev.Reset();
            }
        });

        const std::string batchQuery = std::format(R"(
            BATCH UPDATE `{}` SET Value = "Updated";
        )", tableName);

        const auto result = kikimr.RunCall([&]{
            return db.ExecuteQuery(batchQuery, TTxControl::NoTx()).GetValueSync();
        });

        // An error is expected because the actor is in the ExecuteState and does not know what to do with the unknown event
        // It is needed to check that new events are not ignored and the actor is aborted
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::INTERNAL_ERROR, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "got an unknown message", result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "state = ExecuteState", result.GetIssues().ToString());
    }

    Y_UNIT_TEST(AbortState_DoubleAbort) {
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
        std::vector<TAutoPtr<IEventHandle>> abortEvents;
        bool abortSent = false;
        bool enableCapture = true;

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

        // Send abort to the PartitionedExecuterActor to get AbortState
        using TEvTestCapture = TEvKqpExecuter::TEvTxRequest;
        const auto captureObserver = runtime.AddObserver<TEvTestCapture>([&](TEvTestCapture::TPtr& ev) {
            if (partitionedId.has_value() && ActorIdFromProto(ev->Get()->Record.GetTarget()) == *partitionedId) {
                UNIT_ASSERT_C(executerIds.find(ev->Recipient) != executerIds.end(), "Executer actor is not found");

                // Send abort to the PartitionedExecuterActor while the executer actors are not finished yet
                if (!abortSent) {
                    auto abort = TEvKqp::TEvAbortExecution::Aborted("Test abort before any response");
                    runtime.Send(new IEventHandle(*partitionedId, MakeSchemeCacheID(), abort.Release()));
                    abortSent = true;
                }

                // Events are dropped to freeze the execution
                ev.Reset();
            }
        });

        // Capture TEvAbortExecution events from the PartitionedExecuterActor
        using TEvTestAbort = TEvKqp::TEvAbortExecution;
        const auto abortObserver = runtime.AddObserver<TEvTestAbort>([&](TEvTestAbort::TPtr& ev) {
            if (enableCapture && partitionedId.has_value() && ev->Sender == *partitionedId) {
                abortEvents.emplace_back(ev.Release());
            }
        });

        TDispatchOptions opts;
        opts.FinalEvents.emplace_back([&abortEvents](IEventHandle&) {
            // Wait until all abort events are captured (one per partition)
            return abortEvents.size() >= partitionCount;
        });

        const std::string batchQuery = std::format(R"(
            BATCH UPDATE `{}` SET Value = "Updated";
        )", tableName);

        auto queryFuture = kikimr.RunInThreadPool([&] {
            return db.ExecuteQuery(batchQuery, TTxControl::NoTx()).GetValueSync();
        });

        // Wait for all abort events to be captured
        runtime.DispatchEvents(opts);
        UNIT_ASSERT_VALUES_EQUAL_C(abortEvents.size(), partitionCount, "All abort events should be captured");

        // Send second abort to the PartitionedExecuterActor while it is in the AbortState
        auto abort = TEvKqp::TEvAbortExecution::Aborted("Test double abort");
        runtime.Send(new IEventHandle(*partitionedId, MakeSchemeCacheID(), abort.Release()));

        // Disable capture and send abort events
        enableCapture = false;
        for (const auto& ev : abortEvents) {
            runtime.Send(ev);
        }

        const auto result = runtime.WaitFuture(queryFuture);

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::ABORTED, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Test abort before any response", result.GetIssues().ToString());
        // The second issue is ignored to avoid duplication of issues (if there is a common problem for every executer)
        // UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Test double abort", result.GetIssues().ToString());
    }

    Y_UNIT_TEST(AbortState_AbortFromExecuterActor) {
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
        std::vector<TAutoPtr<IEventHandle>> abortEvents;
        bool abortSent = false;
        bool enableCapture = true;

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

        // Send abort to the PartitionedExecuterActor to get AbortState
        using TEvTestCapture = TEvKqpExecuter::TEvTxRequest;
        const auto captureObserver = runtime.AddObserver<TEvTestCapture>([&](TEvTestCapture::TPtr& ev) {
            if (partitionedId.has_value() && ActorIdFromProto(ev->Get()->Record.GetTarget()) == *partitionedId) {
                UNIT_ASSERT_C(executerIds.find(ev->Recipient) != executerIds.end(), "Executer actor is not found");

                // Send abort to the PartitionedExecuterActor while the executer actors are not finished yet
                if (!abortSent) {
                    auto abort = TEvKqp::TEvAbortExecution::Aborted("Test abort before any response");
                    runtime.Send(new IEventHandle(*partitionedId, MakeSchemeCacheID(), abort.Release()));
                    abortSent = true;
                }

                // Events are dropped to freeze the execution
                ev.Reset();
            }
        });

        // Capture TEvAbortExecution events from the PartitionedExecuterActor
        using TEvTestAbort = TEvKqp::TEvAbortExecution;
        const auto abortObserver = runtime.AddObserver<TEvTestAbort>([&](TEvTestAbort::TPtr& ev) {
            if (enableCapture && partitionedId.has_value() && ev->Sender == *partitionedId) {
                abortEvents.emplace_back(ev.Release());
            }
        });

        TDispatchOptions opts;
        opts.FinalEvents.emplace_back([&abortEvents](IEventHandle&) {
            // Wait until all abort events are captured (one per partition)
            return abortEvents.size() >= partitionCount;
        });

        const std::string batchQuery = std::format(R"(
            BATCH UPDATE `{}` SET Value = "Updated";
        )", tableName);

        auto queryFuture = kikimr.RunInThreadPool([&] {
            return db.ExecuteQuery(batchQuery, TTxControl::NoTx()).GetValueSync();
        });

        // Wait for all abort events to be captured
        runtime.DispatchEvents(opts);
        UNIT_ASSERT_VALUES_EQUAL_C(abortEvents.size(), partitionCount, "All abort events should be captured");

        // Send second abort to the PartitionedExecuterActor while it is in the AbortState
        // from the child ExecuterActor (!!!)
        auto abort = TEvKqp::TEvAbortExecution::Aborted("Test double abort");
        runtime.Send(new IEventHandle(*partitionedId, *executerIds.begin(), abort.Release()));

        // Disable capture and send abort events
        enableCapture = false;
        for (const auto& ev : abortEvents) {
            runtime.Send(ev);
        }

        const auto result = runtime.WaitFuture(queryFuture);

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::ABORTED, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Test abort before any response", result.GetIssues().ToString());
        // The second issue is ignored to avoid duplication of issues (if there is a common problem for every executer)
        // UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Test double abort", result.GetIssues().ToString());
    }

    Y_UNIT_TEST(AbortState_UnknownEvent) {
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
        std::vector<TAutoPtr<IEventHandle>> abortEvents;
        bool abortSent = false;
        bool enableCapture = true;

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

        // Send abort to the PartitionedExecuterActor to get AbortState
        using TEvTestCapture = TEvKqpExecuter::TEvTxRequest;
        const auto captureObserver = runtime.AddObserver<TEvTestCapture>([&](TEvTestCapture::TPtr& ev) {
            if (partitionedId.has_value() && ActorIdFromProto(ev->Get()->Record.GetTarget()) == *partitionedId) {
                UNIT_ASSERT_C(executerIds.find(ev->Recipient) != executerIds.end(), "Executer actor is not found");

                // Send abort to the PartitionedExecuterActor while the executer actors are not finished yet
                if (!abortSent) {
                    auto abort = TEvKqp::TEvAbortExecution::Aborted("Test abort before any response");
                    runtime.Send(new IEventHandle(*partitionedId, MakeSchemeCacheID(), abort.Release()));
                    abortSent = true;
                }

                // Events are dropped to freeze the execution
                ev.Reset();
            }
        });

        // Capture TEvAbortExecution events from the PartitionedExecuterActor
        using TEvTestAbort = TEvKqp::TEvAbortExecution;
        const auto abortObserver = runtime.AddObserver<TEvTestAbort>([&](TEvTestAbort::TPtr& ev) {
            if (enableCapture && partitionedId.has_value() && ev->Sender == *partitionedId) {
                abortEvents.emplace_back(ev.Release());
            }
        });

        TDispatchOptions opts;
        opts.FinalEvents.emplace_back([&abortEvents](IEventHandle&) {
            // Wait until all abort events are captured (one per partition)
            return abortEvents.size() >= partitionCount;
        });

        const std::string batchQuery = std::format(R"(
            BATCH UPDATE `{}` SET Value = "Updated";
        )", tableName);

        auto queryFuture = kikimr.RunInThreadPool([&] {
            return db.ExecuteQuery(batchQuery, TTxControl::NoTx()).GetValueSync();
        });

        // Wait for all abort events to be captured
        runtime.DispatchEvents(opts);
        UNIT_ASSERT_VALUES_EQUAL_C(abortEvents.size(), partitionCount, "All abort events should be captured");

        // Send some unknown event to the PartitionedExecuterActor while it is in the AbortState
        // It should be ignored and the actor must continue aborting the execution, but not die
        runtime.Send(new IEventHandle(*partitionedId, MakeSchemeCacheID(), new TEvents::TEvWakeup()));

        // Disable capture and send abort events
        enableCapture = false;
        for (const auto& ev : abortEvents) {
            runtime.Send(ev);
        }

        const auto result = runtime.WaitFuture(queryFuture);

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::ABORTED, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Test abort before any response", result.GetIssues().ToString());
    }
}

}  // namespace NKikimr::NKqp
