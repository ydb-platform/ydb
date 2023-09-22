#include "quoter_resource_tree.h"

#include <library/cpp/testing/gmock_in_unittest/gmock.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NKesus {

using namespace testing;

class THDRRQuoterResourceTreeRuntimeTest: public TTestBase {
public:
    UNIT_TEST_SUITE(THDRRQuoterResourceTreeRuntimeTest)
    UNIT_TEST(TestCreateInactiveSession)
    UNIT_TEST(TestAllocateResource)
    UNIT_TEST(TestDeleteResourceSessions)
    UNIT_TEST(TestUpdateResourceSessions)
    UNIT_TEST(TestStopConsuming)
    UNIT_TEST(TestUpdateConsumptionState)
    UNIT_TEST(TestUpdateConsumptionStateAfterAllResourceAllocated)
    UNIT_TEST(TestAllocationGranularity)
    UNIT_TEST(TestDistributeResourcesBetweenConsumers)
    UNIT_TEST(TestHierarchicalQuotas)
    UNIT_TEST(TestHangDefence)
    UNIT_TEST(TestMoreStrongChildLimit)
    UNIT_TEST(TestAmountIsLessThanEpsilon)
    UNIT_TEST(TestEffectiveProps)
    UNIT_TEST(TestWeights)
    UNIT_TEST(TestWeightsChange)
    UNIT_TEST(TestVerySmallSpeed)
    UNIT_TEST(TestVeryBigWeights)
    UNIT_TEST(TestDeleteResourceWithActiveChildren)
    UNIT_TEST(TestActiveSessionDisconnectsAndThenConnectsAgain)
    UNIT_TEST(TestInactiveSessionDisconnectsAndThenConnectsAgain)
    UNIT_TEST(TestActiveMultiresourceSessionDisconnectsAndThenConnectsAgain)
    UNIT_TEST(TestInactiveMultiresourceSessionDisconnectsAndThenConnectsAgain)
    UNIT_TEST_SUITE_END();

    void SetUp() override {
        Resources = MakeHolder<TQuoterResources>();
        Queue = MakeHolder<TTickProcessorQueue>();
        NextResourceId = 1;
        NextActorId = 1;
        Time = TInstant::Now();
    }

    void TearDown() override {
        Resources = nullptr;
        Queue = nullptr;
    }

    // Helpers
    struct TTestResourceSink : public IResourceSink {
        TTestResourceSink(ui64 resourceId = 0)
            : ResourceId(resourceId)
        {
        }

        void Send(ui64 resourceId, double amount, const NKikimrKesus::TStreamingQuoterResource* props) override {
            UNIT_ASSERT(!SessionClosed);
            if (ResourceId) {
                UNIT_ASSERT_VALUES_EQUAL(ResourceId, resourceId);
            }
            UNIT_ASSERT_GE_C(amount, 0.0, "Actual amount: " << amount);
            if (!amount) {
                UNIT_ASSERT(props != nullptr);
            }
            SumAmount += amount;
            OnSend(resourceId, amount, props);
        };

        void CloseSession(ui64 resourceId, Ydb::StatusIds::StatusCode status, const TString& reason) override {
            UNIT_ASSERT(!IsIn(SessionClosed, resourceId));
            if (ResourceId) {
                UNIT_ASSERT_VALUES_EQUAL(ResourceId, resourceId);
            }
            UNIT_ASSERT_UNEQUAL(status, Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT(!reason.empty());
            SessionClosed.insert(resourceId);
            if (status == Ydb::StatusIds::NOT_FOUND) {
                OnNotFound(resourceId);
            } else if (status == Ydb::StatusIds::SESSION_EXPIRED) {
                OnSessionExpired(resourceId);
            } else {
                UNIT_ASSERT_C(false, "Unexpected status code: " << status);
            }
        }

        MOCK_METHOD(void, OnSend, (ui64 resourceId, double amount, const NKikimrKesus::TStreamingQuoterResource* props), ());
        MOCK_METHOD(void, OnNotFound, (ui64 resourceId), ());
        MOCK_METHOD(void, OnSessionExpired, (ui64 resourceId), ());

        const ui64 ResourceId;
        THashSet<ui64> SessionClosed;
        double SumAmount = 0.0;
    };

    struct TTestSession {
        TTestSession() = default;

        TTestSession(TQuoterSession* session, TIntrusivePtr<TTestResourceSink> sink)
            : Session(session)
            , Sink(std::move(sink))
        {
        }

        TQuoterSession* Session;
        TIntrusivePtr<TTestResourceSink> Sink;
    };

    TInstant ProcessOneTick() {
        UNIT_ASSERT(!Queue->Empty());
        const TInstant time = Queue->Top().Time;
        TInstant nextTime;
        TTickProcessorQueue queue;
        do {
            const TTickProcessorTask task = Queue->Top();
            Queue->Pop();
            Resources->ProcessTick(task, queue);
            nextTime = !Queue->Empty() ? Queue->Top().Time : TInstant::Max();
        } while (nextTime == time);
        Queue->Merge(std::move(queue));
        Time = time;
        return time;
    }

    TInstant ProcessTicks(size_t count) {
        TInstant time;
        while (count--) {
            time = ProcessOneTick();
        }
        return time;
    }

    size_t ProcessAllTicks() {
        size_t ticksPassed = 0;
        while (!Queue->Empty()) {
            ProcessOneTick();
            ++ticksPassed;
        }
        return ticksPassed;
    }

    void AssertQueueEmpty() {
        UNIT_ASSERT(Queue->Empty());
    }

    void AssertQueueNotEmpty() {
        UNIT_ASSERT(!Queue->Empty());
    }

    TQuoterResourceTree* AddResource(const TString& path, double maxUnitsPerSecond) {
        NKikimrKesus::THierarchicalDRRResourceConfig cfg;
        cfg.SetMaxUnitsPerSecond(maxUnitsPerSecond);
        return AddResource(path, cfg);
    }

    TQuoterResourceTree* AddResource(const TString& path, const NKikimrKesus::THierarchicalDRRResourceConfig& config = {}) {
        NKikimrKesus::TStreamingQuoterResource cfg;
        cfg.SetResourcePath(path);
        *cfg.MutableHierarchicalDRRResourceConfig() = config;
        TString errorMessage;
        TQuoterResourceTree* res = Resources->AddResource(NextResourceId++, cfg, errorMessage);
        UNIT_ASSERT_C(res, "Failed to add resource [" << path << "]: " << errorMessage);
        return res;
    }

    std::vector<TTestSession> CreateSession(std::vector<TQuoterResourceTree*> resources, bool consume, double amount, TIntrusivePtr<TTestResourceSink> sink = nullptr, NActors::TActorId clientId = {}, NActors::TActorId pipeServerId = {}) {
        UNIT_ASSERT(!resources.empty());

        if (!sink) {
            sink = new TTestResourceSink(resources.size() == 1 ? resources[0]->GetResourceId() : 0);
        }
        const bool newClientId = !clientId;
        if (!clientId) {
            clientId = NewActorID();
        }
        if (!pipeServerId) {
            pipeServerId = NewActorID();
        }
        TTickProcessorQueue queue;
        std::vector<TTestSession> result;
        result.reserve(resources.size());
        for (TQuoterResourceTree* resource : resources) {
            TQuoterSession* session = Resources->GetOrCreateSession(clientId, 1, resource);
            UNIT_ASSERT(session);
            session->SetResourceSink(sink);
            const NActors::TActorId prevPipeServerId = session->SetPipeServerId(pipeServerId);
            UNIT_ASSERT(!newClientId || !prevPipeServerId);
            Resources->SetPipeServerId(TQuoterSessionId(clientId, resource->GetResourceId()), prevPipeServerId, pipeServerId);
            session->UpdateConsumptionState(consume, amount, queue, Time);
            result.emplace_back(session, sink);
        }
        Queue->Merge(std::move(queue));
        UNIT_ASSERT_VALUES_EQUAL(resources.size(), result.size());
        return result;
    }

    TTestSession CreateSession(TQuoterResourceTree* resource, bool consume, double amount, TIntrusivePtr<TTestResourceSink> sink = nullptr, NActors::TActorId clientId = {}, NActors::TActorId pipeServerId = {}) {
        return CreateSession(std::vector<TQuoterResourceTree*>(1, resource), consume, amount, sink, clientId, pipeServerId)[0];
    }

    void DisconnectSession(TQuoterSession* session) {
        Resources->DisconnectSession(session->GetPipeServerId());
    }

    NActors::TActorId NewActorID() {
        const ui64 x1 = NextActorId++;
        const ui64 x2 = NextActorId++;
        return NActors::TActorId(x1, x2);
    }

    // Tests
    void TestCreateInactiveSession() {
        auto* res = AddResource("/Root", 100); // with small burst
        auto session = CreateSession(res, false, 0);
        AssertQueueEmpty();
    }

    void TestAllocateResource() {
        auto* res = AddResource("/Root", 10);
        auto session = CreateSession(res, true, 10);
        EXPECT_CALL(*session.Sink, OnSend(_, DoubleEq(1), nullptr))
            .Times(10);
        UNIT_ASSERT_VALUES_EQUAL(ProcessAllTicks(), 11);
        UNIT_ASSERT_DOUBLES_EQUAL(session.Sink->SumAmount, 10, 0.01);
    }

    void TestDeleteResourceSessions() {
        auto* res = AddResource("/Root", 10);
        auto inactiveSession = CreateSession(res, false, 0);
        auto activeSession = CreateSession(res, true, 10);
        EXPECT_CALL(*inactiveSession.Sink, OnNotFound(_));
        EXPECT_CALL(*activeSession.Sink, OnNotFound(_));
        EXPECT_CALL(*activeSession.Sink, OnSend(_, DoubleNear(1, 0.01), nullptr))
            .Times(5);
        ProcessTicks(5);
        AssertQueueNotEmpty();
        TString msg;
        UNIT_ASSERT(Resources->DeleteResource(res, msg));
        ProcessOneTick();
        AssertQueueEmpty();
    }

    void TestUpdateResourceSessions() {
        auto* root = AddResource("/Root", 100);
        auto* res = AddResource("/Root/Res");
        auto session = CreateSession(res, true, 60);
        auto& oldSettingsAllocation =
            EXPECT_CALL(*session.Sink, OnSend(_, DoubleNear(10, 0.01), nullptr))
                .Times(2);
        auto& updateSend =
            EXPECT_CALL(*session.Sink, OnSend(_, 0.0, _))
                .After(oldSettingsAllocation)
                .WillOnce(Invoke([](ui64, double, const NKikimrKesus::TStreamingQuoterResource* props) {
                    UNIT_ASSERT(props != nullptr);
                    UNIT_ASSERT_DOUBLES_EQUAL(props->GetHierarchicalDRRResourceConfig().GetMaxUnitsPerSecond(), 400, 0.001);
                }));
        EXPECT_CALL(*session.Sink, OnSend(_, DoubleNear(40, 0.01), nullptr))
            .After(updateSend);

        ProcessTicks(2);
        NKikimrKesus::TStreamingQuoterResource newProps;
        newProps.MutableHierarchicalDRRResourceConfig()->SetMaxUnitsPerSecond(400);
        TString msg;
        UNIT_ASSERT(root->Update(newProps, msg));
        Resources->OnUpdateResourceProps(root);
        ProcessAllTicks();
    }

    void TestStopConsuming() {
        auto* res = AddResource("/Root", 100);
        auto session = CreateSession(res, true, 100);
        EXPECT_CALL(*session.Sink, OnSend(_, DoubleNear(10, 0.01), nullptr))
            .Times(5);
        const TInstant lastProcessed = ProcessTicks(5);
        TTickProcessorQueue queue;
        session.Session->UpdateConsumptionState(false, 0, queue, lastProcessed);
        Queue->Merge(std::move(queue));
        UNIT_ASSERT_VALUES_EQUAL(ProcessAllTicks(), 1);
    }

    void TestUpdateConsumptionState() {
        auto* res = AddResource("/Root", 100);
        auto session = CreateSession(res, true, 100);
        EXPECT_CALL(*session.Sink, OnSend(_, DoubleNear(10, 0.01), nullptr))
            .Times(5);
        const TInstant lastProcessed = ProcessTicks(5);
        session.Sink = new TTestResourceSink(res->GetResourceId());
        session.Session->SetResourceSink(session.Sink);
        TTickProcessorQueue queue;
        session.Session->UpdateConsumptionState(true, 20, queue, lastProcessed);
        Queue->Merge(std::move(queue));
        EXPECT_CALL(*session.Sink, OnSend(_, DoubleNear(10, 0.01), nullptr))
            .Times(2);
        UNIT_ASSERT_VALUES_EQUAL(ProcessAllTicks(), 3);
    }

    void TestUpdateConsumptionStateAfterAllResourceAllocated() {
        auto* res = AddResource("/Root", 100);
        auto session = CreateSession(res, true, 500);
        EXPECT_CALL(*session.Sink, OnSend(_, DoubleNear(10, 0.01), nullptr))
            .Times(50);
        ProcessAllTicks();
        UNIT_ASSERT_DOUBLES_EQUAL(session.Sink->SumAmount, 500, 0.01);

        // Update state after all resource was allocated.
        session.Sink = new TTestResourceSink(res->GetResourceId());
        session.Session->SetResourceSink(session.Sink);
        EXPECT_CALL(*session.Sink, OnSend(_, DoubleNear(10, 0.01), nullptr))
            .Times(50);

        TTickProcessorQueue queue;
        session.Session->UpdateConsumptionState(true, 500, queue, Time); // Burst should be sent in UpdateConsumptionState() function.
        Queue->Merge(std::move(queue));

        ProcessAllTicks();
        UNIT_ASSERT_DOUBLES_EQUAL(session.Sink->SumAmount, 500, 0.01);
    }

    void TestAllocationGranularity() {
        auto* res = AddResource("/Root", 10);
        auto session1 = CreateSession(res, true, 0.4);
        auto session2 = CreateSession(res, true, 0.4);
        // 1 resource per second, but granularity is 0.1 per second, so both sessions will be satisfied.
        EXPECT_CALL(*session1.Sink, OnSend(_, DoubleEq(0.4), nullptr));
        EXPECT_CALL(*session2.Sink, OnSend(_, DoubleEq(0.4), nullptr));
        ProcessOneTick();
    }


    void TestDistributeResourcesBetweenConsumers() {
        auto* res = AddResource("/Root", 10);
        TTestSession sessions[] = {
            CreateSession(res, true, 2),
            CreateSession(res, true, 2),
            CreateSession(res, true, 2),
            CreateSession(res, true, 2),
        };
        for (auto& session : sessions) {
            EXPECT_CALL(*session.Sink, OnSend(_, DoubleNear(0.25, 0.01), nullptr))
                .Times(8);
        }

        auto AssertSessionsAreEquallyFilled = [&]() {
            for (size_t i = 1; i < Y_ARRAY_SIZE(sessions); ++i) {
                UNIT_ASSERT_DOUBLES_EQUAL(sessions[i - 1].Sink->SumAmount, sessions[i].Sink->SumAmount, 0.01);
            }
        };
        AssertSessionsAreEquallyFilled();
        ProcessOneTick();
        AssertSessionsAreEquallyFilled();
        ProcessOneTick();
        AssertSessionsAreEquallyFilled();
        ProcessOneTick();
        AssertSessionsAreEquallyFilled();
        ProcessOneTick();
        AssertSessionsAreEquallyFilled();

        ProcessOneTick();
        AssertSessionsAreEquallyFilled();
        ProcessOneTick();
        AssertSessionsAreEquallyFilled();
        ProcessOneTick();
        AssertSessionsAreEquallyFilled();
        ProcessOneTick();
        AssertSessionsAreEquallyFilled();
    }

    void TestHierarchicalQuotas() {
        auto* root = AddResource("/Root", 10);
        NKikimrKesus::THierarchicalDRRResourceConfig cfg;
        cfg.SetMaxUnitsPerSecond(7);
        auto* res1 = AddResource("/Root/Res1", cfg);
        cfg.SetMaxUnitsPerSecond(1);
        auto* res2 = AddResource("/Root/Res2", cfg);

        auto rootSession = CreateSession(root, true, 10);
        auto res1Session = CreateSession(res1, true, 10);
        auto res2Session = CreateSession(res2, true, 10);

        EXPECT_CALL(*rootSession.Sink, OnSend(_, Le(1.0), nullptr))
            .Times(AtLeast(1));
        EXPECT_CALL(*res1Session.Sink, OnSend(_, Le(0.7), nullptr))
            .Times(AtLeast(1));
        EXPECT_CALL(*res2Session.Sink, OnSend(_, Le(0.1), nullptr))
            .Times(AtLeast(1));

        ProcessOneTick();
        ProcessOneTick();
    }

    void TestHangDefence() {
        auto* root = AddResource("/Root", 100);
        // All these sessions will be filled in one tick. Next resource accumulation will spend zero amount of resource.
        // But despite that algorithm should detect it and finish.
        auto session1 = CreateSession(root, true, 0.1);
        auto session2 = CreateSession(root, true, 0.1);
        auto session3 = CreateSession(root, true, 0.1);
        ProcessAllTicks();
    }

    void TestMoreStrongChildLimit() {
        AddResource("/Root", 100);
        auto* res = AddResource("/Root/Res", 3); // 0.3 resource in one tick
        auto session = CreateSession(res, true, 3);

        // Parent resource tick amount is 1, but our resource tick is 0.3
        EXPECT_CALL(*session.Sink, OnSend(_, DoubleEq(0.3), nullptr))
            .Times(10);
        ProcessAllTicks();
    }

    void TestAmountIsLessThanEpsilon() {
        auto* root = AddResource("/Root", 100);
        auto session = CreateSession(root, true, 0.000001);

        // Session must not hang even if client requested too small resource.
        EXPECT_CALL(*session.Sink, OnSend(_, Ge(0.000001), nullptr)); // send epsilon
        ProcessAllTicks();
    }

    void TestEffectiveProps() {
        NKikimrKesus::THierarchicalDRRResourceConfig rootCfg;
        rootCfg.SetMaxUnitsPerSecond(100);
        AddResource("/Root", rootCfg);

        NKikimrKesus::THierarchicalDRRResourceConfig cfg;
        cfg.SetMaxUnitsPerSecond(200);
        auto* res = AddResource("/Root/Res", cfg);

        UNIT_ASSERT_DOUBLES_EQUAL(res->GetEffectiveProps().GetHierarchicalDRRResourceConfig().GetMaxUnitsPerSecond(), 100, 0.001); // min

        auto* res2 = AddResource("/Root/Res2");
        UNIT_ASSERT_DOUBLES_EQUAL(res2->GetEffectiveProps().GetHierarchicalDRRResourceConfig().GetMaxUnitsPerSecond(), 100, 0.001); // inherits
    }

    void TestWeights() {
        AddResource("/Root", 10);
        NKikimrKesus::THierarchicalDRRResourceConfig cfg;
        cfg.SetWeight(7);
        auto* lightRes = AddResource("/Root/LightRes", cfg);
        cfg.SetWeight(42);
        auto* heavyRes = AddResource("/Root/HeavyRes", cfg);

        auto lightResSession = CreateSession(lightRes, true, 10);
        auto heavyResSession = CreateSession(heavyRes, true, 10);

        EXPECT_CALL(*lightResSession.Sink, OnSend(_, DoubleNear(1.0 * 7.0 / 49.0, 0.001), nullptr))
            .Times(AtLeast(1));
        EXPECT_CALL(*heavyResSession.Sink, OnSend(_, DoubleNear(1.0 * 42.0 / 49.0, 0.001), nullptr))
            .Times(AtLeast(1));

        ProcessOneTick();
        ProcessOneTick();
    }

    void TestWeightsChange() {
        AddResource("/Root", 10);
        NKikimrKesus::THierarchicalDRRResourceConfig cfg;
        cfg.SetWeight(7);
        auto* lightRes = AddResource("/Root/LightRes", cfg);
        cfg.SetWeight(42);
        auto* heavyRes = AddResource("/Root/HeavyRes", cfg);

        auto lightResSession = CreateSession(lightRes, true, 10);
        auto heavyResSession = CreateSession(heavyRes, true, 10);

        // Before weights change
        auto& firstLightResAllocation = EXPECT_CALL(*lightResSession.Sink, OnSend(_, DoubleNear(1.0 * 7.0 / 49.0, 0.001), nullptr))
            .Times(2);
        auto& firstHeavyResAllocation = EXPECT_CALL(*heavyResSession.Sink, OnSend(_, DoubleNear(1.0 * 42.0 / 49.0, 0.001), nullptr))
            .Times(2);

        auto& heaveResPropsChange = EXPECT_CALL(*heavyResSession.Sink, OnSend(_, 0.0, _))
            .Times(1)
            .After(firstHeavyResAllocation);

        // After weights change
        EXPECT_CALL(*lightResSession.Sink, OnSend(_, DoubleNear(1.0 * 7.0 / 8.0, 0.001), nullptr))
            .Times(2)
            .After(firstLightResAllocation);
        EXPECT_CALL(*heavyResSession.Sink, OnSend(_, DoubleNear(1.0 / 8.0, 0.001), nullptr))
            .Times(2)
            .After(heaveResPropsChange);

        ProcessOneTick();
        ProcessOneTick();

        // Update one weight.
        // It is expected that a new weight will be applied in the next tick.
        TString msg;
        NKikimrKesus::TStreamingQuoterResource newPropsWithoutWeight;
        newPropsWithoutWeight.MutableHierarchicalDRRResourceConfig();
        UNIT_ASSERT_C(heavyRes->Update(newPropsWithoutWeight, msg), msg);
        Resources->OnUpdateResourceProps(heavyRes);

        ProcessOneTick();
        ProcessOneTick();
    }

    void TestVerySmallSpeed() {
        auto* res = AddResource("/Root", 0.000000000000000000000000000000000000000001);

        auto session = CreateSession(res, true, 10);

        EXPECT_CALL(*session.Sink, OnSend(_, DoubleNear(0.000000000000000000000000000000000000000001, 0.000000000000000000000000000000000000000001), nullptr))
            .Times(1);

        ProcessOneTick();
    }

    void TestVeryBigWeights() {
        AddResource("/Root", 10);

        NKikimrKesus::THierarchicalDRRResourceConfig cfg;
        cfg.SetWeight(std::numeric_limits<ui32>::max() - 3);
        auto* res1 = AddResource("/Root/Res1", cfg);
        auto* res2 = AddResource("/Root/Res1/Res2", cfg);

        auto session11 = CreateSession(res1, true, 10);
        auto session12 = CreateSession(res1, true, 10);
        auto session13 = CreateSession(res1, true, 10);
        auto session14 = CreateSession(res1, true, 10);
        auto session15 = CreateSession(res1, true, 10);

        auto session21 = CreateSession(res2, true, 10);
        auto session22 = CreateSession(res2, true, 10);

        EXPECT_CALL(*session11.Sink, OnSend(_, DoubleNear(0.000001, 0.000001), nullptr))
            .Times(1);
        EXPECT_CALL(*session12.Sink, OnSend(_, DoubleNear(0.000001, 0.000001), nullptr))
            .Times(1);
        EXPECT_CALL(*session13.Sink, OnSend(_, DoubleNear(0.000001, 0.000001), nullptr))
            .Times(1);
        EXPECT_CALL(*session14.Sink, OnSend(_, DoubleNear(0.000001, 0.000001), nullptr))
            .Times(1);
        EXPECT_CALL(*session15.Sink, OnSend(_, DoubleNear(0.000001, 0.000001), nullptr))
            .Times(1);
        EXPECT_CALL(*session21.Sink, OnSend(_, DoubleNear(0.5, 0.001), nullptr))
            .Times(1);
        EXPECT_CALL(*session22.Sink, OnSend(_, DoubleNear(0.5, 0.001), nullptr))
            .Times(1);

        ProcessOneTick();
    }

    void TestDeleteResourceWithActiveChildren() {
        AddResource("Root", 10);
        auto* res1 = AddResource("Root/Res1");
        auto* res2 = AddResource("Root/Res1/Res2");

        auto session1 = CreateSession(res1, true, std::numeric_limits<double>::infinity());
        auto session2 = CreateSession(res2, true, std::numeric_limits<double>::infinity());
        auto session3 = CreateSession(res2, true, std::numeric_limits<double>::infinity());

        EXPECT_CALL(*session1.Sink, OnNotFound(_));
        EXPECT_CALL(*session2.Sink, OnNotFound(_));
        EXPECT_CALL(*session3.Sink, OnSessionExpired(_));

        ProcessTicks(3);

        DisconnectSession(session3.Session);

        TString msg;
        UNIT_ASSERT_C(Resources->DeleteResource(res2, msg), msg);
        ProcessTicks(3);

        UNIT_ASSERT_C(Resources->DeleteResource(res1, msg), msg);
        ProcessAllTicks();
    }

    void TestSessionDisconnectsAndThenConnectsAgainImpl(bool consumes, size_t resourcesCount = 1) {
        UNIT_ASSERT(resourcesCount > 0);

        AddResource("Root", 100500);

        const NActors::TActorId clientId = NewActorID();
        const NActors::TActorId pipeServerId = NewActorID();
        std::vector<TQuoterResourceTree*> resources;
        resources.reserve(resourcesCount);
        for (size_t resourceIndex = 0; resourceIndex < resourcesCount; ++resourceIndex) {
            resources.push_back(AddResource(TStringBuilder() << "Root/Res_" << resourceIndex, 10));
        }

        auto sessionsForResources = CreateSession(resources, consumes, std::numeric_limits<double>::infinity(), nullptr, clientId, pipeServerId);

        for (size_t resourceIndex = 0; resourceIndex < resourcesCount; ++resourceIndex) {
            TTestSession& session = sessionsForResources[resourceIndex];
            const ui64 resourceId = resources[resourceIndex]->GetResourceId();
            if (consumes) {
                EXPECT_CALL(*session.Sink, OnSend(resourceId, DoubleNear(1, 0.000001), nullptr))
                    .Times(1);
            }
            EXPECT_CALL(*session.Sink, OnSessionExpired(resourceId));
        }

        if (consumes) {
            AssertQueueNotEmpty();
            ProcessOneTick();
        }
        Resources->DisconnectSession(pipeServerId);

        auto sessionsForResources2 = CreateSession(resources, true, std::numeric_limits<double>::infinity(), nullptr, clientId, pipeServerId);

        for (size_t resourceIndex = 0; resourceIndex < resourcesCount; ++resourceIndex) {
            TTestSession& session = sessionsForResources2[resourceIndex];
            const ui64 resourceId = resources[resourceIndex]->GetResourceId();
            EXPECT_CALL(*session.Sink, OnSend(resourceId, DoubleNear(1, 0.000001), nullptr))
                .Times(1);
        }

        ProcessOneTick();
    }

    void TestActiveSessionDisconnectsAndThenConnectsAgain() {
        TestSessionDisconnectsAndThenConnectsAgainImpl(true);
    }

    void TestInactiveSessionDisconnectsAndThenConnectsAgain() {
        TestSessionDisconnectsAndThenConnectsAgainImpl(false);
    }

    void TestActiveMultiresourceSessionDisconnectsAndThenConnectsAgain() {
        TestSessionDisconnectsAndThenConnectsAgainImpl(true, 5);
    }

    void TestInactiveMultiresourceSessionDisconnectsAndThenConnectsAgain() {
        TestSessionDisconnectsAndThenConnectsAgainImpl(false, 5);
    }

private:
    THolder<TQuoterResources> Resources;
    THolder<TTickProcessorQueue> Queue;
    ui64 NextResourceId = 1;
    ui64 NextActorId = 1;
    TInstant Time = TInstant::Now();
};

UNIT_TEST_SUITE_REGISTRATION(THDRRQuoterResourceTreeRuntimeTest);

}
}
