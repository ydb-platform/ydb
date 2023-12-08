#include <ydb/core/persqueue/dread_cache_service/caching_service.h>
#include <ydb/core/persqueue/ut/common/pq_ut_common.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ {

Y_UNIT_TEST_SUITE(TPQCachingProxyTest) {
struct TTestSetup {
    TTestContext Context;
    TActorId ProxyId;
    TTestSetup() {
        Context.Prepare();
        Context.Runtime->SetLogPriority(NKikimrServices::PQ_READ_PROXY, NLog::PRI_DEBUG);
        ProxyId = Context.Runtime->Register(CreatePQDReadCacheService(new NMonitoring::TDynamicCounters()));
        Context.Runtime->AllocateEdgeActor();
        TDispatchOptions opts;
        opts.FinalEvents.emplace_back(TEvents::TEvBootstrap::EventType, 1);
        Context.Runtime->DispatchEvents(opts);
    }
    auto* GetRuntime() {
        return Context.Runtime.Get();
    }
    THolder<TEvPQ::TEvGetFullDirectReadData> SendRequest(TEvPQ::TEvGetFullDirectReadData* request, bool status = true) {
        GetRuntime()->Send(ProxyId, Context.Edge, request);
        auto resp = GetRuntime()->GrabEdgeEvent<TEvPQ::TEvGetFullDirectReadData>();
        UNIT_ASSERT(resp);
        UNIT_ASSERT(resp->Error != status);
        return resp;
    } 
};

Y_UNIT_TEST(TestPublishAndForget) {
    TTestSetup setup;
    auto runtime = setup.GetRuntime();
    auto resp = setup.SendRequest(new TEvPQ::TEvGetFullDirectReadData());
    UNIT_ASSERT(resp->Data.empty());

    {
        auto* reg = new TEvPQ::TEvRegisterDirectReadSession({"session1", 1}, 1);
        runtime->Send(setup.ProxyId, TActorId{}, reg);
    }
    resp = setup.SendRequest(new TEvPQ::TEvGetFullDirectReadData({"session1", 1}, 1));
    UNIT_ASSERT_VALUES_EQUAL(resp->Data.size(), 1);
    UNIT_ASSERT(resp->Data[0].second.Reads.empty());
    {
        auto* reg = new TEvPQ::TEvStageDirectReadData(
                {"session1", 1, 1}, 1, std::make_shared<NKikimrClient::TResponse>()
        );
        runtime->Send(setup.ProxyId, TActorId{}, reg);
    }
    {
        auto* reg = new TEvPQ::TEvPublishDirectRead(
                {"session1", 1, 1},
                1
        );
        runtime->Send(setup.ProxyId, TActorId{}, reg);
    }
    resp = setup.SendRequest(new TEvPQ::TEvGetFullDirectReadData({"session1", 1}, 1));
    UNIT_ASSERT(!resp->Error);
    UNIT_ASSERT_VALUES_EQUAL(resp->Data[0].second.Reads.size(), 1);
    {
        auto* reg = new TEvPQ::TEvForgetDirectRead(
                {"session1", 1, 1}, 1
        );
        runtime->Send(setup.ProxyId, TActorId{}, reg);
    }
    resp = setup.SendRequest(new TEvPQ::TEvGetFullDirectReadData({"session1", 1}, 1));
    UNIT_ASSERT_VALUES_EQUAL(resp->Data[0].second.Reads.size(), 0);
}

Y_UNIT_TEST(TestDeregister) {
    TTestSetup setup;
    auto runtime = setup.GetRuntime();
    {
        auto* reg = new TEvPQ::TEvRegisterDirectReadSession({"session1", 1}, 1);
        runtime->Send(setup.ProxyId, TActorId{}, reg);
    }
    {
        auto* reg = new TEvPQ::TEvRegisterDirectReadSession({"session2", 1}, 1);
        runtime->Send(setup.ProxyId, TActorId{}, reg);
    }
    auto resp = setup.SendRequest(new TEvPQ::TEvGetFullDirectReadData(
            {"session1", 1}, 1)
    );
    UNIT_ASSERT_VALUES_EQUAL(resp->Data.size(), 1);
    UNIT_ASSERT(resp->Data[0].second.Reads.empty());
    resp = setup.SendRequest(new TEvPQ::TEvGetFullDirectReadData());
    UNIT_ASSERT_VALUES_EQUAL(resp->Data.size(), 2);
    {
        auto* reg = new TEvPQ::TEvDeregisterDirectReadSession({"session1", 1}, 1);
        runtime->Send(setup.ProxyId, TActorId{}, reg);
    }
    resp = setup.SendRequest(new TEvPQ::TEvGetFullDirectReadData());
    UNIT_ASSERT_VALUES_EQUAL(resp->Data.size(), 1);
}

Y_UNIT_TEST(TestWrongSessionOrGeneration) {
    TTestSetup setup;
    auto runtime = setup.GetRuntime();
    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvRegisterDirectReadSession({"session1", 1}, 2)
    );
    {
        auto* reg = new TEvPQ::TEvStageDirectReadData(
                {"session1", 1, 1}, 2, std::make_shared<NKikimrClient::TResponse>()
        );
        runtime->Send(setup.ProxyId, TActorId{}, reg);
    }
    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvPublishDirectRead({"session1", 1, 1}, 2)
    );
    
    // Session with old id, shold not have any effect
    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvRegisterDirectReadSession({"session1", 1}, 1)
    );
    {
        auto* reg = new TEvPQ::TEvStageDirectReadData(
                {"session1", 1, 1}, 1, std::make_shared<NKikimrClient::TResponse>()
        );
        runtime->Send(setup.ProxyId, TActorId{}, reg);
    }
    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvPublishDirectRead({"session1", 1, 1}, 1)
    );
    
    auto resp = setup.SendRequest(new TEvPQ::TEvGetFullDirectReadData({"session1", 1}, 1));
    UNIT_ASSERT_VALUES_EQUAL(resp->Data.size(), 0);

    // Forget with old generation, should have no effect
    runtime->Send(
        setup.ProxyId, TActorId{},
        new TEvPQ::TEvForgetDirectRead({"session1", 1, 1}, 1)
    );
    
    resp = setup.SendRequest(new TEvPQ::TEvGetFullDirectReadData({"session1", 1}, 2));
    UNIT_ASSERT_VALUES_EQUAL(resp->Data.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(resp->Data[0].second.Reads.size(), 1);

    resp = setup.SendRequest(new TEvPQ::TEvGetFullDirectReadData({"session-2", 1}, 2), false);
    resp = setup.SendRequest(new TEvPQ::TEvGetFullDirectReadData({"session1", 99}, 2), false);
}

Y_UNIT_TEST(OutdatedSession) {
    TTestSetup setup;
    auto runtime = setup.GetRuntime();
    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvRegisterDirectReadSession({"session1", 1}, 1)
    );
    {
        auto* reg = new TEvPQ::TEvStageDirectReadData(
                {"session1", 1, 1}, 1, std::make_shared<NKikimrClient::TResponse>()
        );
        runtime->Send(setup.ProxyId, TActorId{}, reg);
    }
    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvPublishDirectRead({"session1", 1, 1}, 1)
    );
    
    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvRegisterDirectReadSession({"session1", 1}, 2)
    );

    auto resp = setup.SendRequest(new TEvPQ::TEvGetFullDirectReadData({"session1", 1}, 1));
    UNIT_ASSERT(resp->Data.empty());
}


Y_UNIT_TEST(MultipleSessions) {
    TTestSetup setup;
    auto runtime = setup.GetRuntime();
    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvRegisterDirectReadSession({"session1", 1}, 1)
    );
    {
        auto* reg = new TEvPQ::TEvStageDirectReadData(
                {"session1", 1, 1}, 1, std::make_shared<NKikimrClient::TResponse>()
        );
        runtime->Send(setup.ProxyId, TActorId{}, reg);
    }
    {
        auto* reg = new TEvPQ::TEvStageDirectReadData(
                {"session1", 1, 2}, 1, std::make_shared<NKikimrClient::TResponse>()
        );
        runtime->Send(setup.ProxyId, TActorId{}, reg);
    }
    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvPublishDirectRead({"session1", 1, 1}, 1)
    );
    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvPublishDirectRead({"session1", 1, 2}, 1)
    );
    
    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvRegisterDirectReadSession({"session2", 1}, 2)
    );
     {
        auto* reg = new TEvPQ::TEvStageDirectReadData(
                {"session2", 1, 3}, 2, std::make_shared<NKikimrClient::TResponse>()
        );
        runtime->Send(setup.ProxyId, TActorId{}, reg);
    }
    runtime->Send(
            setup.ProxyId, TActorId{},
            new TEvPQ::TEvPublishDirectRead({"session2", 1, 3}, 2)
    );

    auto resp = setup.SendRequest(new TEvPQ::TEvGetFullDirectReadData());
    UNIT_ASSERT_VALUES_EQUAL(resp->Data.size(), 2);
    for (const auto& [key, data] : resp->Data) {
        if (key.SessionId == "session1") {
            UNIT_ASSERT_VALUES_EQUAL(data.Generation, 1);
            UNIT_ASSERT_VALUES_EQUAL(data.Reads.size(), 2);
            auto iter = data.Reads.begin();
            UNIT_ASSERT_VALUES_EQUAL(iter->first, 1);
            UNIT_ASSERT_VALUES_EQUAL((++iter)->first, 2);
        }else if (key.SessionId == "session2") {
            UNIT_ASSERT_VALUES_EQUAL(data.Generation, 2);
            UNIT_ASSERT_VALUES_EQUAL(data.Reads.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(data.Reads.begin()->first, 3);
        } 
    }
}
} // Test suite

} //namespace NKikimr::NPQ
