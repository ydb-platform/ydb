#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/services/metadata/abstract/common.h>
#include <ydb/services/workload_manager/events.h>
#include <ydb/services/workload_manager/actors/workload_manager_state_actor.h>
#include <ydb/services/workload_manager/service/service.h>
#include <ydb/services/workload_manager/ut/common/query_classifier_ut_common.h>

#include <library/cpp/testing/unittest/registar.h>


namespace NKikimr::NKqp {

using namespace Tests;

namespace {

struct TWlmFixture {
    TPortManager PortManager;
    Tests::TServerSettings Settings;
    Tests::TServer::TPtr Server;
    TTestActorRuntime* Runtime = nullptr;
    TActorId KqpProxy;
    TActorId Sender;

    TWlmFixture()
        : Settings(BuildSettings(PortManager))
        , Server(new Tests::TServer(Settings))
    {
        Runtime = Server->GetRuntime();
        KqpProxy = MakeKqpProxyID(Runtime->GetNodeId(0));
        Sender = Runtime->AllocateEdgeActor();

        // NMetadata::NProvider::TServiceOperator is a process-wide singleton — if another test
        // in the same binary already flipped it to enabled, our state actor Bootstrap would send
        // TEvAskSnapshot into a metadata service that we disabled above, and classifier metadata
        // would stay Pending until the timeout. Force it Ready with an empty snapshot.
        Runtime->Send(new IEventHandle(
            StateActorId(),
            Sender,
            new NMetadata::NProvider::TEvRefreshSubscriberData(
                NWorkloadManager::MakeClassifierSnapshot({}))));
    }

    TActorId StateActorId() const {
        return Runtime->GetLocalServiceId(NWorkloadManager::MakeWorkloadManagerStateActorId(Runtime->GetNodeId(0)));
    }

    static Tests::TServerSettings BuildSettings(TPortManager& tp) {
        auto settings = Tests::TServerSettings(tp.GetPort(2134))
            .SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableMetadataProvider(false);  // avoids .metadata table lookups that hang init in this bare setup
        settings.AppConfig->MutableFeatureFlags()->SetEnableResourcePools(true);
        return settings;
    }
};

TAutoPtr<NKqp::TEvKqp::TEvQueryRequest> MakeSelect42Query(const TString& database) {
    auto ev = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
    ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
    ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_SCRIPT);
    ev->Record.MutableRequest()->SetQuery("SELECT 42; COMMIT;");
    ev->Record.MutableRequest()->SetDatabase(database);
    ev->Record.MutableRequest()->SetKeepSession(true);
    ev->Record.MutableRequest()->SetTimeoutMs(30000);
    return ev.Release();
}

}

Y_UNIT_TEST_SUITE(KqpProxyWorkloadManager) {
    ///
    /// Test query is deferred until wlm replies ready for DB
    ///
    Y_UNIT_TEST(QueryDeferredUntilWlmReady) {
        TWlmFixture fx;

        std::vector<TAutoPtr<IEventHandle>> held;
        fx.Runtime->SetEventFilter([&held](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) -> bool {
            if (ev->GetTypeRewrite() == NWorkloadManager::TEvWorkloadManagerReady::EventType) {
                held.push_back(ev.Release());
                return true;
            }
            return false;
        });

        fx.Runtime->Send(new IEventHandle(fx.KqpProxy, fx.Sender, MakeSelect42Query("/Root").Release()));

        TDispatchOptions opts;
        opts.FinalEvents.emplace_back([&held](IEventHandle&) { return !held.empty(); });
        fx.Runtime->DispatchEvents(opts);
        UNIT_ASSERT_C(!held.empty(), "Expected proxy to park query waiting for TEvWorkloadManagerReady");

        fx.Runtime->SetEventFilter([](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>&) { return false; });
        for (auto& e : held) {
            fx.Runtime->Send(e.Release());
        }

        auto reply = fx.Runtime->GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(fx.Sender);
        UNIT_ASSERT_VALUES_EQUAL_C(reply->Get()->Record.GetYdbStatus(), Ydb::StatusIds::SUCCESS,
                                    reply->Get()->Record.GetResponse().GetQueryIssues());
    }

    ///
    /// Test query fails when wlm replies with error for Db preparing
    ///
    Y_UNIT_TEST(QueryFailsWhenWlmFetchFails) {
        TWlmFixture fx;

        fx.Runtime->SetEventFilter([runtime = fx.Runtime](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) -> bool {
            if (ev->GetTypeRewrite() != NWorkloadManager::TEvWorkloadManagerReady::EventType) {
                return false;
            }
            const auto* original = ev->Get<NWorkloadManager::TEvWorkloadManagerReady>();
            // Only rewrite the real reply from the state actor (SUCCESS) — our injected replacement
            // reuses the same event type, so let it through to avoid an infinite filter loop.
            if (original->Status != Ydb::StatusIds::SUCCESS) {
                return false;
            }
            auto* replacement = new NWorkloadManager::TEvWorkloadManagerReady(
                original->Cookie, Ydb::StatusIds::NOT_FOUND, "simulated fetch failure");
            runtime->Send(new IEventHandle(ev->Recipient, ev->Sender, replacement, 0, ev->Cookie));
            return true;
        });

        fx.Runtime->Send(new IEventHandle(fx.KqpProxy, fx.Sender, MakeSelect42Query("/Root").Release()));

        auto reply = fx.Runtime->GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(fx.Sender);
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetYdbStatus(), Ydb::StatusIds::NOT_FOUND);
    }

    ///
    /// Test query runs after wlm replies with unsupported DB.
    /// In this case query has to run but wlm will be skipped
    ///
    Y_UNIT_TEST(QuerySucceedsOnWlmUnsupportedDb) {
        TWlmFixture fx;

        // Rewrite the successful fetch response from the DB fetcher to UNSUPPORTED — mimics
        // "path exists but isn't a subdomain" (e.g. a table). State actor marks the database
        // Unsupported and notifies subscribers with SUCCESS; proxy re-dispatches, EnsureReady
        // answers Disabled, query proceeds without a classifier and succeeds normally.
        fx.Runtime->SetEventFilter([runtime = fx.Runtime](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) -> bool {
            if (ev->GetTypeRewrite() != NWorkloadManager::TEvFetchDatabaseResponse::EventType) {
                return false;
            }
            const auto* original = ev->Get<NWorkloadManager::TEvFetchDatabaseResponse>();
            if (original->Status != Ydb::StatusIds::SUCCESS) {
                return false;
            }
            NYql::TIssues issues;
            issues.AddIssue(NYql::TIssue("simulated: path is not a subdomain"));
            auto* replacement = new NWorkloadManager::TEvFetchDatabaseResponse(
                Ydb::StatusIds::UNSUPPORTED,
                original->Database,
                original->DatabaseId,
                /*serverless=*/false,
                original->PathId,
                std::move(issues));
            runtime->Send(new IEventHandle(ev->Recipient, ev->Sender, replacement, 0, ev->Cookie));
            return true;
        });

        fx.Runtime->Send(new IEventHandle(fx.KqpProxy, fx.Sender, MakeSelect42Query("/Root").Release()));
        auto reply = fx.Runtime->GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(fx.Sender);
        UNIT_ASSERT_VALUES_EQUAL_C(reply->Get()->Record.GetYdbStatus(), Ydb::StatusIds::SUCCESS,
                                    reply->Get()->Record.GetResponse().GetQueryIssues());
    }

    ///
    /// Test query sends a warmup to the wlm for an unknown db
    ///
    Y_UNIT_TEST(QuerySendsWlmWarmupOnEntry) {
        TWlmFixture fx;

        int warmupCount = 0;
        fx.Runtime->SetObserverFunc([&warmupCount](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NWorkloadManager::TEvWarmupDatabaseInfo::EventType) {
                ++warmupCount;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        fx.Runtime->Send(new IEventHandle(fx.KqpProxy, fx.Sender, MakeSelect42Query("/Root").Release()));
        auto reply = fx.Runtime->GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(fx.Sender);
        UNIT_ASSERT_VALUES_EQUAL_C(reply->Get()->Record.GetYdbStatus(), Ydb::StatusIds::SUCCESS,
                                    reply->Get()->Record.GetResponse().GetQueryIssues());

        UNIT_ASSERT_C(warmupCount > 0, "Expected TEvWarmupDatabaseInfo to be sent to the state actor");
    }

    ///
    /// Test query fails fast for DB which has error in the wlm
    ///
    Y_UNIT_TEST(QueryFailsFastOnCachedWlmFailure) {
        TWlmFixture fx;

        // Every real SUCCESS fetch response is rewritten to NOT_FOUND — state actor marks the
        // database Failed with NOT_FOUND.
        fx.Runtime->SetEventFilter([runtime = fx.Runtime](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) -> bool {
            if (ev->GetTypeRewrite() != NWorkloadManager::TEvFetchDatabaseResponse::EventType) {
                return false;
            }
            const auto* original = ev->Get<NWorkloadManager::TEvFetchDatabaseResponse>();
            if (original->Status != Ydb::StatusIds::SUCCESS) {
                return false;
            }
            NYql::TIssues issues;
            issues.AddIssue(NYql::TIssue("simulated: DB not found"));
            auto* replacement = new NWorkloadManager::TEvFetchDatabaseResponse(
                Ydb::StatusIds::NOT_FOUND,
                original->Database,
                original->DatabaseId,
                /*serverless=*/false,
                original->PathId,
                std::move(issues));
            runtime->Send(new IEventHandle(ev->Recipient, ev->Sender, replacement, 0, ev->Cookie));
            return true;
        });

        // Count SubscribeOnReady to distinguish the park path from the sync-Failed path.
        int subscribes = 0;
        fx.Runtime->SetObserverFunc([&subscribes](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NWorkloadManager::TEvSubscribeOnWorkloadManagerReady::EventType) {
                ++subscribes;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        // First query: EnsureReady returns Pending, proxy parks, state actor's failed fetch
        // notifies subscribers, proxy routes NOT_FOUND to the client via TEvDelayedRequestError.
        fx.Runtime->Send(new IEventHandle(fx.KqpProxy, fx.Sender, MakeSelect42Query("/Root").Release()));
        auto reply1 = fx.Runtime->GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(fx.Sender);
        UNIT_ASSERT_VALUES_EQUAL(reply1->Get()->Record.GetYdbStatus(), Ydb::StatusIds::NOT_FOUND);
        const int subscribesAfterFirst = subscribes;
        UNIT_ASSERT_C(subscribesAfterFirst >= 1, "First query should have taken the park path");

        // Second query for the same DB: state actor's snapshot has the database Failed,
        // EnsureReady returns Failed synchronously — proxy replies via TEvDelayedRequestError
        // without calling SubscribeOnReady again.
        fx.Runtime->Send(new IEventHandle(fx.KqpProxy, fx.Sender, MakeSelect42Query("/Root").Release()));
        auto reply2 = fx.Runtime->GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(fx.Sender);
        UNIT_ASSERT_VALUES_EQUAL(reply2->Get()->Record.GetYdbStatus(), Ydb::StatusIds::NOT_FOUND);
        UNIT_ASSERT_VALUES_EQUAL_C(subscribes, subscribesAfterFirst,
                                    "Second query should hit the sync-Failed path (no extra SubscribeOnReady)");
    }

    ///
    /// Test two parallel queries resume after wlm replies ready
    ///
    Y_UNIT_TEST(ParallelQueriesResumeOnWlmReady) {
        TWlmFixture fx;
        const TActorId sender2 = fx.Runtime->AllocateEdgeActor();

        std::vector<TAutoPtr<IEventHandle>> held;
        fx.Runtime->SetEventFilter([&held](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) -> bool {
            if (ev->GetTypeRewrite() == NWorkloadManager::TEvWorkloadManagerReady::EventType) {
                held.push_back(ev.Release());
                return true;
            }
            return false;
        });

        fx.Runtime->Send(new IEventHandle(fx.KqpProxy, fx.Sender, MakeSelect42Query("/Root").Release()));
        fx.Runtime->Send(new IEventHandle(fx.KqpProxy, sender2, MakeSelect42Query("/Root").Release()));

        TDispatchOptions opts;
        opts.FinalEvents.emplace_back([&held](IEventHandle&) { return held.size() >= 2; });
        fx.Runtime->DispatchEvents(opts);
        UNIT_ASSERT_C(held.size() >= 2, "Expected both queries to be parked and both TEvWorkloadManagerReady captured");

        fx.Runtime->SetEventFilter([](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>&) { return false; });
        for (auto& e : held) {
            fx.Runtime->Send(e.Release());
        }

        auto reply1 = fx.Runtime->GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(fx.Sender);
        UNIT_ASSERT_VALUES_EQUAL_C(reply1->Get()->Record.GetYdbStatus(), Ydb::StatusIds::SUCCESS,
                                    reply1->Get()->Record.GetResponse().GetQueryIssues());
        auto reply2 = fx.Runtime->GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(sender2);
        UNIT_ASSERT_VALUES_EQUAL_C(reply2->Get()->Record.GetYdbStatus(), Ydb::StatusIds::SUCCESS,
                                    reply2->Get()->Record.GetResponse().GetQueryIssues());
    }

    ///
    /// Test query fails with retryable UNAVAILABLE when the db info fetch never completes:
    /// the state actor times the fetch out and releases the parked query, wlm is not bypassed
    ///
    Y_UNIT_TEST(QueryFailsRetryablyWhenWlmFetchTimesOut) {
        TWlmFixture fx;
        const TActorId stateActor = fx.StateActorId();

        // Drop only the fetch responses addressed to the state actor; the workload service
        // keeps its own database fetches.
        fx.Runtime->SetEventFilter([stateActor](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) -> bool {
            return ev->GetTypeRewrite() == NWorkloadManager::TEvFetchDatabaseResponse::EventType
                && ev->Recipient == stateActor;
        });

        fx.Runtime->Send(new IEventHandle(fx.KqpProxy, fx.Sender, MakeSelect42Query("/Root").Release()));
        auto reply = fx.Runtime->GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(fx.Sender);
        UNIT_ASSERT_VALUES_EQUAL_C(reply->Get()->Record.GetYdbStatus(), Ydb::StatusIds::UNAVAILABLE,
                                    reply->Get()->Record.GetResponse().GetQueryIssues());
    }

}

} // namespace NKikimr::NKqp
