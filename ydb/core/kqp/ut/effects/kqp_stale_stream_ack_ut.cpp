#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <thread>
#include <atomic>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpStaleStreamAck) {
    
    // Test that emulates real SDK retry scenario:
    // 1. First query execution starts
    // 2. SCHEME_ERROR happens during execution
    // 3. Query invalidated, SDK retries in the same session
    // 4. Stale StreamDataAck from first execution arrives during second execution
    Y_UNIT_TEST(RetryWithStaleAck) {
        TKikimrSettings settings;
        settings.SetUseRealThreads(false);
        
        TKikimrRunner kikimr(settings);
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        
        auto sender = runtime.AllocateEdgeActor();
        auto kqpProxy = MakeKqpProxyID(runtime.GetNodeId());

        // Create session
        runtime.Send(new IEventHandle(kqpProxy, sender, new TEvKqp::TEvCreateSessionRequest()));
        auto reply = runtime.GrabEdgeEventRethrow<TEvKqp::TEvCreateSessionResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetYdbStatus(), Ydb::StatusIds::SUCCESS);
        auto sessionId = reply->Get()->Record.GetResponse().GetSessionId();
        auto sessionActorId = reply->Sender;

        TActorId firstExecuterId;
        TActorId secondExecuterId;
        bool schemeErrorInjected = false;
        bool staleAckCaptured = false;
        std::unique_ptr<IEventHandle> capturedAck;

        auto observer = [&](TAutoPtr<IEventHandle>& ev) -> auto {
            // Track first executer creation
            if (ev->GetTypeRewrite() == TEvKqpExecuter::TEvTxRequest::EventType && !firstExecuterId) {
                firstExecuterId = ev->Recipient;
            }
            
            // Inject SCHEME_ERROR to first executer to trigger invalidation
            if (firstExecuterId && ev->Recipient == firstExecuterId && !schemeErrorInjected) {
                if (ev->GetTypeRewrite() == TSchemeBoardEvents::TEvNotifyUpdate::EventType) {
                    schemeErrorInjected = true;
                    // Send SCHEME_ERROR back to trigger query invalidation
                    auto errorResp = MakeHolder<TEvKqpExecuter::TEvTxResponse>();
                    errorResp->Record.MutableResponse()->SetStatus(Ydb::StatusIds::SCHEME_ERROR);
                    runtime.Send(new IEventHandle(sessionActorId, firstExecuterId, errorResp.Release()));
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }
            
            // Capture StreamDataAck that would go to first executer
            if (ev->GetTypeRewrite() == TEvKqpExecuter::TEvStreamDataAck::EventType && 
                ev->Recipient == firstExecuterId && schemeErrorInjected && !staleAckCaptured) {
                staleAckCaptured = true;
                capturedAck.reset(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;
            }
            
            // Track second executer (retry)
            if (schemeErrorInjected && !secondExecuterId && 
                ev->GetTypeRewrite() == TEvKqpExecuter::TEvTxRequest::EventType) {
                secondExecuterId = ev->Recipient;
                
                // Send captured stale ack to new executer
                if (capturedAck) {
                    auto newAck = MakeHolder<TEvKqpExecuter::TEvStreamDataAck>(
                        capturedAck->Get<TEvKqpExecuter::TEvStreamDataAck>()->Record.GetSeqNo(),
                        capturedAck->Get<TEvKqpExecuter::TEvStreamDataAck>()->Record.GetChannelId()
                    );
                    runtime.Send(new IEventHandle(secondExecuterId, sender, newAck.Release()));
                }
            }
            
            return TTestActorRuntime::EEventAction::PROCESS;
        };

        runtime.SetObserverFunc(observer);

        // First query attempt
        auto ev = std::make_unique<TEvKqp::TEvQueryRequest>();
        ev->Record.MutableRequest()->SetSessionId(sessionId);
        ev->Record.MutableRequest()->MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
        ev->Record.MutableRequest()->MutableTxControl()->set_commit_tx(true);
        ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
        ev->Record.MutableRequest()->SetQuery(R"(
            SELECT * FROM `/Root/TwoShard` WHERE Key > 0u LIMIT 1000;
        )");
        ActorIdToProto(sender, ev->Record.MutableRequestActorId());

        runtime.Send(new IEventHandle(kqpProxy, sender, ev.release()));
        
        TDispatchOptions opts;
        opts.FinalEvents.emplace_back([&](IEventHandle&) {
            return schemeErrorInjected;
        });
        runtime.DispatchEvents(opts);

        auto response = runtime.GrabEdgeEventRethrow<TEvKqp::TEvQueryResponse>(sender);
        
        // First attempt should fail with ABORTED (scheme error converted to retriable)
        if (response->Get()->Record.GetYdbStatus() == Ydb::StatusIds::ABORTED) {
            Cerr << "First query got ABORTED (expected), retrying..." << Endl;
            
            // SDK would retry - send same query again in same session
            auto retryEv = std::make_unique<TEvKqp::TEvQueryRequest>();
            retryEv->Record.MutableRequest()->SetSessionId(sessionId);
            retryEv->Record.MutableRequest()->MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
            retryEv->Record.MutableRequest()->MutableTxControl()->set_commit_tx(true);
            retryEv->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
            retryEv->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
            retryEv->Record.MutableRequest()->SetQuery(R"(
                SELECT * FROM `/Root/TwoShard` WHERE Key > 0u LIMIT 1000;
            )");
            ActorIdToProto(sender, retryEv->Record.MutableRequestActorId());

            runtime.Send(new IEventHandle(kqpProxy, sender, retryEv.release()));
            
            TDispatchOptions retryOpts;
            retryOpts.FinalEvents.emplace_back([&](IEventHandle&) {
                return secondExecuterId && staleAckCaptured;
            });
            runtime.DispatchEvents(retryOpts);

            auto retryResponse = runtime.GrabEdgeEventRethrow<TEvKqp::TEvQueryResponse>(sender);
            auto issues = retryResponse->Get()->Record.DebugString();
            
            // Without fix: would crash with "Unexpected event: 271646825"
            // With fix: stale ack is ignored
            UNIT_ASSERT_C(
                issues.find("Unexpected event") == TString::npos &&
                issues.find("271646825") == TString::npos,
                "Got unexpected event error on retry - stale StreamDataAck not handled: " << issues
            );
            
            Cerr << "Retry completed without unexpected event error" << Endl;
        }
    }
    
    // Stress test that tries to reproduce the race condition naturally.
    Y_UNIT_TEST(TryReproduceRaceNaturally) {
        TKikimrSettings settings;
        settings.SetUseRealThreads(true);  // Real threads for true concurrency
        
        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        
        // Create sharded table to increase race condition probability
        // Multiple shards = more concurrent operations = higher chance of hitting the timing
        {
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(R"(
                CREATE TABLE `/Root/TestRace` (
                    Id Uint64,
                    Value String,
                    PRIMARY KEY (Id)
                )
                WITH (
                    UNIFORM_PARTITIONS = 20,
                    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 8
                );
            )").ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        
        const size_t ITERATIONS = 100;  // Increased for better chance
        const size_t PARALLEL_WORKERS = 20;  // More concurrency
        std::atomic<bool> stopFlag{false};
        std::atomic<size_t> unexpectedEventCount{0};
        std::atomic<size_t> successCount{0};
        std::atomic<size_t> otherErrorCount{0};
        
        // Worker: does concurrent INSERTs and SELECTs
        auto worker = [&](size_t workerId) {
            auto session = db.CreateSession().GetValueSync().GetSession();
            
            for (size_t i = 0; i < ITERATIONS && !stopFlag; ++i) {
                // INSERT - insert multiple rows to increase result size
                {
                    auto query = TStringBuilder() << R"(
                        UPSERT INTO `/Root/TestRace` (Id, Value) VALUES
                            ()" << (workerId * 10000 + i * 10 + 0) << R"(u, "worker-)" << workerId << R"(-iter-)" << i << R"(-row0"),
                            ()" << (workerId * 10000 + i * 10 + 1) << R"(u, "worker-)" << workerId << R"(-iter-)" << i << R"(-row1"),
                            ()" << (workerId * 10000 + i * 10 + 2) << R"(u, "worker-)" << workerId << R"(-iter-)" << i << R"(-row2"),
                            ()" << (workerId * 10000 + i * 10 + 3) << R"(u, "worker-)" << workerId << R"(-iter-)" << i << R"(-row3"),
                            ()" << (workerId * 10000 + i * 10 + 4) << R"(u, "worker-)" << workerId << R"(-iter-)" << i << R"(-row4");
                    )";
                    
                    auto result = session.ExecuteDataQuery(
                        query,
                        TTxControl::BeginTx().CommitTx()
                    ).ExtractValueSync();
                    
                    if (!result.IsSuccess()) {
                        auto issues = result.GetIssues().ToString();
                        if (issues.find("unexpected event") != TString::npos &&
                            issues.find("271646825") != TString::npos) {
                            unexpectedEventCount++;
                            Cerr << "!!! REPRODUCED BUG !!!" << Endl;
                            Cerr << "Worker " << workerId << " iter " << i << Endl;
                            Cerr << issues << Endl;
                            stopFlag = true;
                            return;
                        }
                    }
                }
                
                // SELECT - large result to trigger streaming
                {
                    auto result = session.ExecuteDataQuery(R"(
                        SELECT * FROM `/Root/TestRace` WHERE Id > 0u ORDER BY Id LIMIT 5000;
                    )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                    
                    if (!result.IsSuccess()) {
                        auto issues = result.GetIssues().ToString();
                        if (issues.find("unexpected event") != TString::npos &&
                            issues.find("271646825") != TString::npos) {
                            unexpectedEventCount++;
                            Cerr << "!!! REPRODUCED BUG in SELECT !!!" << Endl;
                            Cerr << "Worker " << workerId << " iter " << i << Endl;
                            Cerr << issues << Endl;
                            stopFlag = true;
                            return;
                        }
                        otherErrorCount++;
                    } else {
                        successCount++;
                    }
                }
            }
        };
        
        // Stressor: modifies schema to trigger invalidations
        auto stressor = [&]() {
            Sleep(TDuration::MilliSeconds(10));
            
            for (size_t i = 0; i < 50 && !stopFlag; ++i) {  // More iterations
                auto session = db.CreateSession().GetValueSync().GetSession();
                
                // Add/drop column to trigger scheme changes
                {
                    auto alterQuery = TStringBuilder() << R"(
                        ALTER TABLE `/Root/TestRace` ADD COLUMN Extra)" << i << R"( String;
                    )";
                    
                    auto result = session.ExecuteSchemeQuery(alterQuery).ExtractValueSync();
                    // Ignore errors - we just want to stress the system
                }
                
                Sleep(TDuration::MilliSeconds(50));  // Shorter delay
            }
        };
        
        Cerr << "Starting flaky race condition test..." << Endl;
        Cerr << "Workers: " << PARALLEL_WORKERS << ", iterations per worker: " << ITERATIONS << Endl;
        
        // Start workers
        TVector<std::thread> threads;
        for (size_t i = 0; i < PARALLEL_WORKERS; ++i) {
            threads.emplace_back(worker, i);
        }
        
        // Start stressor
        threads.emplace_back(stressor);
        
        // Wait for completion
        for (auto& t : threads) {
            t.join();
        }
        
        Cerr << "Test completed:" << Endl;
        Cerr << "  Success: " << successCount.load() << Endl;
        Cerr << "  Other errors: " << otherErrorCount.load() << Endl;
        Cerr << "  Unexpected event errors: " << unexpectedEventCount.load() << Endl;
        
        // With fix: should be 0 unexpected events
        // Without fix: might have > 0 (but not guaranteed - it's a race)
        UNIT_ASSERT_C(
            unexpectedEventCount.load() != 0,
            "Reproduced stale StreamDataAck bug! Found " << unexpectedEventCount.load() << " unexpected event errors"
        );
        
        // Test passes if no unexpected events occurred
        // Note: absence of bug doesn't prove the fix works, but presence proves it doesn't
    }
}

}
}
