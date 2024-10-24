#include "quoter_service.h"
#include "kesus_quoter_proxy.h"
#include "ut_helpers.h"

namespace NKikimr {

Y_UNIT_TEST_SUITE(QuoterWithKesusTest) {
    Y_UNIT_TEST(ForbidsNotCanonizedQuoterPath) {
        TKesusQuoterTestSetup setup;
        // Without timeout
        setup.GetQuota(TKesusQuoterTestSetup::DEFAULT_KESUS_PATH + "/", TKesusQuoterTestSetup::DEFAULT_KESUS_RESOURCE, 1, TEvQuota::TEvClearance::EResult::GenericError);

        // With timeout
        setup.GetQuota("/" + TKesusQuoterTestSetup::DEFAULT_KESUS_PATH, TKesusQuoterTestSetup::DEFAULT_KESUS_RESOURCE, 1, TDuration::Seconds(1), TEvQuota::TEvClearance::EResult::GenericError);
    }

    Y_UNIT_TEST(ForbidsNotCanonizedResourcePath) {
        TKesusQuoterTestSetup setup;
        // Without timeout
        setup.GetQuota(TKesusQuoterTestSetup::DEFAULT_KESUS_PATH, TKesusQuoterTestSetup::DEFAULT_KESUS_RESOURCE + "/", 1, TEvQuota::TEvClearance::EResult::GenericError);

        // With timeout
        setup.GetQuota(TKesusQuoterTestSetup::DEFAULT_KESUS_PATH, "/" + TKesusQuoterTestSetup::DEFAULT_KESUS_RESOURCE + "/", 1, TDuration::Seconds(1), TEvQuota::TEvClearance::EResult::GenericError);
    }

    Y_UNIT_TEST(HandlesNonExistentResource) {
        TKesusQuoterTestSetup setup;
        // Without timeout
        setup.GetQuota(TKesusQuoterTestSetup::DEFAULT_KESUS_PATH, "UnknownResource", 1, TEvQuota::TEvClearance::EResult::UnknownResource);

        // With timeout
        setup.GetQuota(TKesusQuoterTestSetup::DEFAULT_KESUS_PATH, "UnknownResource", 1, TDuration::Seconds(1), TEvQuota::TEvClearance::EResult::UnknownResource);
    }

    Y_UNIT_TEST(HandlesAllRequestsForNonExistentResource) {
        TKesusQuoterTestSetup setup;
        constexpr size_t requestsCount = 5;
        for (size_t i = 0; i < requestsCount; ++i) {
            const TDuration deadline = (i & 1) ? TDuration::Max() : TDuration::Seconds(1);
            setup.SendGetQuotaRequest(TKesusQuoterTestSetup::DEFAULT_KESUS_PATH, "UnknownResource", 1, deadline);
        }

        for (size_t i = 0; i < requestsCount; ++i) {
            auto answer = setup.WaitGetQuotaAnswer();
            UNIT_ASSERT_VALUES_EQUAL(answer->Result, TEvQuota::TEvClearance::EResult::UnknownResource);
        }
    }

    Y_UNIT_TEST(GetsQuota) {
        TKesusQuoterTestSetup setup;
        setup.GetQuota(TKesusQuoterTestSetup::DEFAULT_KESUS_PATH, TKesusQuoterTestSetup::DEFAULT_KESUS_RESOURCE);
        setup.GetQuota(TKesusQuoterTestSetup::DEFAULT_KESUS_PATH, TKesusQuoterTestSetup::DEFAULT_KESUS_RESOURCE);
    }

    Y_UNIT_TEST(GetsBigQuota) {
        TKesusQuoterTestSetup setup;
        setup.GetQuota(TKesusQuoterTestSetup::DEFAULT_KESUS_PATH, TKesusQuoterTestSetup::DEFAULT_KESUS_RESOURCE); // stabilization
        setup.GetQuota(TKesusQuoterTestSetup::DEFAULT_KESUS_PATH, TKesusQuoterTestSetup::DEFAULT_KESUS_RESOURCE, 30); // default rate is 10
    }

    Y_UNIT_TEST(GetsBigQuotaWithDeadline) {
        TKesusQuoterTestSetup setup;
        setup.GetQuota(TKesusQuoterTestSetup::DEFAULT_KESUS_PATH, TKesusQuoterTestSetup::DEFAULT_KESUS_RESOURCE); // stabilization
        setup.GetQuota(TKesusQuoterTestSetup::DEFAULT_KESUS_PATH, TKesusQuoterTestSetup::DEFAULT_KESUS_RESOURCE, 20, TDuration::Seconds(3)); // default rate is 10
    }

    Y_UNIT_TEST(FailsToGetBigQuota) {
        TKesusQuoterTestSetup setup;
        setup.GetQuota(TKesusQuoterTestSetup::DEFAULT_KESUS_PATH, TKesusQuoterTestSetup::DEFAULT_KESUS_RESOURCE); // stabilization
        setup.GetQuota(TKesusQuoterTestSetup::DEFAULT_KESUS_PATH, TKesusQuoterTestSetup::DEFAULT_KESUS_RESOURCE, 40, TDuration::MilliSeconds(500), TEvQuota::TEvClearance::EResult::Deadline); // default rate is 10
    }

    Y_UNIT_TEST(PrefetchCoefficient) {
        TKesusQuoterTestSetup setup;
        NKikimrKesus::THierarchicalDRRResourceConfig cfg;
        double rate = 100.0;
        double prefetch = 1000.0;
        cfg.SetMaxUnitsPerSecond(rate);
        cfg.SetPrefetchCoefficient(prefetch);
        setup.CreateKesusResource(TKesusQuoterTestSetup::DEFAULT_KESUS_PATH, "root", cfg);

        cfg.ClearMaxUnitsPerSecond();
        cfg.ClearPrefetchCoefficient(); // should be inherited from root
        setup.CreateKesusResource(TKesusQuoterTestSetup::DEFAULT_KESUS_PATH, "root/leaf", cfg);

        setup.GetQuota(TKesusQuoterTestSetup::DEFAULT_KESUS_PATH, "root/leaf"); // stabilization
        setup.GetQuota(TKesusQuoterTestSetup::DEFAULT_KESUS_PATH, "root/leaf", rate * prefetch * 0.9, TDuration::MilliSeconds(500));
        setup.GetQuota(TKesusQuoterTestSetup::DEFAULT_KESUS_PATH, "root/leaf", rate * prefetch * 0.2, TDuration::MilliSeconds(500));
        setup.GetQuota(TKesusQuoterTestSetup::DEFAULT_KESUS_PATH, "root/leaf", 1, TDuration::MilliSeconds(500), TEvQuota::TEvClearance::EResult::Deadline);
    }

    Y_UNIT_TEST(GetsQuotaAfterPause) {
        TKesusQuoterTestSetup setup;
        setup.GetQuota(TKesusQuoterTestSetup::DEFAULT_KESUS_PATH, TKesusQuoterTestSetup::DEFAULT_KESUS_RESOURCE, 12);
        Sleep(TDuration::MilliSeconds(1000));
        setup.GetQuota(TKesusQuoterTestSetup::DEFAULT_KESUS_PATH, TKesusQuoterTestSetup::DEFAULT_KESUS_RESOURCE, 12);
    }

    Y_UNIT_TEST(GetsSeveralQuotas) {
        TKesusQuoterTestSetup setup;
        setup.CreateKesusResource(TKesusQuoterTestSetup::DEFAULT_KESUS_PATH, "Res1");
        setup.CreateKesusResource(TKesusQuoterTestSetup::DEFAULT_KESUS_PATH, "Res2");
        setup.GetQuota({{TKesusQuoterTestSetup::DEFAULT_KESUS_PATH, "Res1", 6}, {TKesusQuoterTestSetup::DEFAULT_KESUS_PATH, "Res2", 5}});
        setup.GetQuota({{TKesusQuoterTestSetup::DEFAULT_KESUS_PATH, "Res1", 10}, {TKesusQuoterTestSetup::DEFAULT_KESUS_PATH, "Res2", 1}});
    }

    Y_UNIT_TEST(KesusRecreation) {
        TKesusQuoterTestSetup setup;
        setup.GetQuota(TKesusQuoterTestSetup::DEFAULT_KESUS_PATH, TKesusQuoterTestSetup::DEFAULT_KESUS_RESOURCE);

        setup.GetClient().DeleteKesus(TKesusQuoterTestSetup::DEFAULT_KESUS_PARENT_PATH, TKesusQuoterTestSetup::DEFAULT_KESUS_NAME);
        Sleep(TDuration::MilliSeconds(500)); // Wait for pipe disconnection, reconnection and passing info that old kesus was destroyed
        setup.GetClient().RefreshPathCache(setup.GetServer().GetRuntime(), TKesusQuoterTestSetup::DEFAULT_KESUS_PATH);

        setup.CreateDefaultKesusAndResource();
        setup.CreateKesusResource(TKesusQuoterTestSetup::DEFAULT_KESUS_PATH, "Res");

        setup.GetQuota(TKesusQuoterTestSetup::DEFAULT_KESUS_PATH, "Res");
    }

    Y_UNIT_TEST(AllocationStatistics) {
        TKesusQuoterTestSetup setup;
        NKikimrKesus::THierarchicalDRRResourceConfig cfg;
        cfg.SetMaxUnitsPerSecond(100'000'000.0);
        setup.CreateKesusResource(TKesusQuoterTestSetup::DEFAULT_KESUS_PATH, "root", cfg);

        auto GetResourceName = [](size_t resIndex) -> TString {
            return TStringBuilder() << "root/" << resIndex;
        };

        constexpr size_t ResourceCount = 5;

        cfg.ClearMaxUnitsPerSecond();
        for (size_t resIndex = 0; resIndex < ResourceCount; ++resIndex) {
            setup.CreateKesusResource(TKesusQuoterTestSetup::DEFAULT_KESUS_PATH, GetResourceName(resIndex), cfg);
        }

        auto UseQuota = [&]() {
            for (size_t resIndex = 0; resIndex < ResourceCount; ++resIndex) {
                for (size_t i = 0; i < 3; ++i) {
                    setup.GetQuota(TKesusQuoterTestSetup::DEFAULT_KESUS_PATH, GetResourceName(resIndex), 10'000'000);
                }
            }
        };

        ui64 prevValue = 0;
        auto CheckCountersIncreased = [&]() {
            auto counters = setup.GetQuoterCounters(TKesusQuoterTestSetup::DEFAULT_KESUS_PATH);
            UNIT_ASSERT_C(counters.ResourceCountersSize() > 0, counters);
            UNIT_ASSERT_VALUES_EQUAL_C(counters.GetResourceCounters(1).GetResourcePath(), "root", counters);
            const ui64 allocated = counters.GetResourceCounters(1).GetAllocated();
            UNIT_ASSERT_C(allocated > prevValue, counters);
            prevValue = allocated;
        };

        for (size_t i = 0; i < 3; ++i) {
            UseQuota();
            CheckCountersIncreased();
        }
    }

    Y_UNIT_TEST(UpdatesCountersForParentResources) {
        TKesusQuoterTestSetup setup;
        setup.CreateKesusResource(TKesusQuoterTestSetup::DEFAULT_KESUS_PATH, TKesusQuoterTestSetup::DEFAULT_KESUS_RESOURCE + "/Child1");
        setup.CreateKesusResource(TKesusQuoterTestSetup::DEFAULT_KESUS_PATH, TKesusQuoterTestSetup::DEFAULT_KESUS_RESOURCE + "/Child1/Child2");
        setup.GetQuota(TKesusQuoterTestSetup::DEFAULT_KESUS_PATH, TKesusQuoterTestSetup::DEFAULT_KESUS_RESOURCE + "/Child1/Child2", 42);
        auto quoterCounters = setup.GetServer().GetRuntime()->GetAppData().Counters
            ->GetSubgroup("counters", "quoter_service")
            ->GetSubgroup("quoter", TKesusQuoterTestSetup::DEFAULT_KESUS_PATH);

        auto CheckConsumedCounter = [&](const TString& resourcePath) {
            size_t attempts = 30; // Counters are updated asynchronously, so make several attempts to get proper counter values.
            do {
                auto counter = quoterCounters->GetSubgroup("resource", resourcePath)->GetCounter("QuotaConsumed");
                if (counter->Val() != 42 && attempts > 1) {
                    Sleep(TDuration::MilliSeconds(50));
                } else {
                    UNIT_ASSERT_VALUES_EQUAL_C(counter->Val(), 42, "Resource path: " << resourcePath);
                }
            } while (--attempts);
        };

        CheckConsumedCounter(TKesusQuoterTestSetup::DEFAULT_KESUS_RESOURCE);
        CheckConsumedCounter(TKesusQuoterTestSetup::DEFAULT_KESUS_RESOURCE + "/Child1");
        CheckConsumedCounter(TKesusQuoterTestSetup::DEFAULT_KESUS_RESOURCE + "/Child1/Child2");
    }

    Y_UNIT_TEST(CanDeleteResourceWhenUsingIt) {
        TKesusQuoterTestSetup setup;
        setup.GetQuota(TKesusQuoterTestSetup::DEFAULT_KESUS_PATH, TKesusQuoterTestSetup::DEFAULT_KESUS_RESOURCE, 12); // success
        setup.SendGetQuotaRequest(TKesusQuoterTestSetup::DEFAULT_KESUS_PATH, TKesusQuoterTestSetup::DEFAULT_KESUS_RESOURCE, 100500); // will wait for big amount of resource
        setup.DeleteKesusResource();
        auto answer = setup.WaitGetQuotaAnswer();
        UNIT_ASSERT_VALUES_EQUAL(answer->Result, TEvQuota::TEvClearance::EResult::UnknownResource);
    }

    Y_UNIT_TEST(CanKillKesusWhenUsingIt) {
        TKesusQuoterTestSetup setup;
        setup.GetQuota(TKesusQuoterTestSetup::DEFAULT_KESUS_PATH, TKesusQuoterTestSetup::DEFAULT_KESUS_RESOURCE, 12);
        setup.SendGetQuotaRequest(TKesusQuoterTestSetup::DEFAULT_KESUS_PATH, TKesusQuoterTestSetup::DEFAULT_KESUS_RESOURCE, 5);
        setup.KillKesusTablet();
        auto answer = setup.WaitGetQuotaAnswer();
        UNIT_ASSERT_VALUES_EQUAL(answer->Result, TEvQuota::TEvClearance::EResult::Success);
    }
}

Y_UNIT_TEST_SUITE(KesusProxyTest) {
    void FillProps(NKikimrKesus::TStreamingQuoterResource* props, ui64 resId = 42, double speed = 100.0) {
        props->SetResourceId(resId);
        auto* cfg = props->MutableHierarchicalDRRResourceConfig();
        cfg->SetMaxUnitsPerSecond(speed);
    }

    void FillResult(NKikimrKesus::TEvSubscribeOnResourcesResult::TResourceSubscribeResult* result, ui64 resId = 42, double speed = 100.0) {
        result->SetResourceId(resId);
        result->MutableError()->SetStatus(Ydb::StatusIds::SUCCESS);
        FillProps(result->MutableEffectiveProps(), resId, speed);
    }

    void FillResultNotFound(NKikimrKesus::TEvSubscribeOnResourcesResult::TResourceSubscribeResult* result) {
        result->MutableError()->SetStatus(Ydb::StatusIds::NOT_FOUND);
    }

    Y_UNIT_TEST(ReconnectsWithKesusWhenNotConnected) {
        TKesusProxyTestSetup setup;
        auto* pipeMock = setup.GetPipeFactory().ExpectTabletPipeCreation(true);

        EXPECT_CALL(*pipeMock, OnPoisonPill());

        // expect a new pipe after receiving NotConnected
        setup.GetPipeFactory().ExpectTabletPipeCreation();

        setup.SendNotConnected(pipeMock);

        TDispatchOptions reconnected;
        reconnected.CustomFinalCondition = [&] {
            return setup.GetPipeFactory().GetPipesCreatedCount() >= 2;
        };
        setup.GetRuntime().DispatchEvents(reconnected);

        // Dispatch some events to let poison pill reach the mock
        setup.GetRuntime().SimulateSleep(TDuration::Zero());
    }

    Y_UNIT_TEST(ReconnectsWithKesusWhenPipeDestroyed) {
        TKesusProxyTestSetup setup;
        auto* pipeMock = setup.GetPipeFactory().ExpectTabletPipeConnection();
        EXPECT_CALL(*pipeMock, OnPoisonPill());

        setup.WaitConnected();

        setup.SendDestroyed(pipeMock);

        setup.WaitPipesCreated(2);

        // Dispatch some events to let poison pill reach the mock
        setup.GetRuntime().SimulateSleep(TDuration::Zero());
    }

    Y_UNIT_TEST(RejectsNotCanonizedResourceName) {
        TKesusProxyTestSetup setup;

        setup.ProxyRequest("/resource", TEvQuota::TEvProxySession::GenericError);
        setup.ProxyRequest("resource//resource", TEvQuota::TEvProxySession::GenericError);
    }

    Y_UNIT_TEST(SubscribesOnResource) {
        TKesusProxyTestSetup setup;
        auto* pipe = setup.GetPipeFactory().ExpectTabletPipeConnection();
        EXPECT_CALL(*pipe, OnSubscribeOnResources(_, _))
            .WillOnce(Invoke([&](const NKikimrKesus::TEvSubscribeOnResources& record, ui64 cookie) {
                UNIT_ASSERT_VALUES_EQUAL(record.ResourcesSize(), 1);
                UNIT_ASSERT_VALUES_EQUAL(record.GetResources(0).GetResourcePath(), "res");
                UNIT_ASSERT(!record.GetResources(0).GetStartConsuming());
                NKikimrKesus::TEvSubscribeOnResourcesResult ans;
                FillResult(ans.AddResults());
                pipe->SendSubscribeOnResourceResult(ans, cookie);
            }));

        auto session = setup.ProxyRequest("res");
        UNIT_ASSERT_VALUES_EQUAL(session->Get()->ResourceId, 42);
    }

    Y_UNIT_TEST(SubscribesOnResourcesWhenReconnected) {
        TKesusProxyTestSetup setup;
        auto* pipe = setup.GetPipeFactory().ExpectTabletPipeConnection();
        size_t resCounter = 0;
        EXPECT_CALL(*pipe, OnSubscribeOnResources(_, _))
            .Times(3)
            .WillRepeatedly(Invoke([&](const NKikimrKesus::TEvSubscribeOnResources& record, ui64 cookie) {
                UNIT_ASSERT_VALUES_EQUAL(record.ResourcesSize(), 1);
                UNIT_ASSERT_VALUES_EQUAL(record.GetResources(0).GetResourcePath(), TStringBuilder() << "res" << resCounter);
                UNIT_ASSERT(!record.GetResources(0).GetStartConsuming());
                NKikimrKesus::TEvSubscribeOnResourcesResult ans;
                FillResult(ans.AddResults(), 42 + resCounter, 5.0);
                pipe->SendSubscribeOnResourceResult(ans, cookie);
                ++resCounter;
            }));

        auto session = setup.ProxyRequest("res0");
        UNIT_ASSERT_VALUES_EQUAL(session->Get()->ResourceId, 42);
        session = setup.ProxyRequest("res1");
        UNIT_ASSERT_VALUES_EQUAL(session->Get()->ResourceId, 43);
        session = setup.ProxyRequest("res2");
        UNIT_ASSERT_VALUES_EQUAL(session->Get()->ResourceId, 44);

        EXPECT_CALL(*pipe, OnUpdateConsumptionState(_, _))
            .WillOnce(Invoke([&](const NKikimrKesus::TEvUpdateConsumptionState& record, ui64) {
                UNIT_ASSERT_VALUES_EQUAL(record.ResourcesInfoSize(), 1);
                UNIT_ASSERT_VALUES_EQUAL(record.GetResourcesInfo(0).GetResourceId(), 43);
                UNIT_ASSERT(record.GetResourcesInfo(0).GetConsumeResource());
            }));

        setup.SendProxyStats({TEvQuota::TProxyStat(43, 1, 0, {}, 3, 5.0, 0, 0)});
        setup.WaitEvent<NKesus::TEvKesus::TEvUpdateConsumptionState>();

        // Disconnected
        setup.SendDestroyed(pipe);

        // second pipe
        auto* pipe2 = setup.GetPipeFactory().ExpectTabletPipeConnection();
        EXPECT_CALL(*pipe2, OnSubscribeOnResources(_, _))
            .WillOnce(Invoke([&](const NKikimrKesus::TEvSubscribeOnResources& record, ui64 cookie) {
                UNIT_ASSERT_VALUES_EQUAL(record.ResourcesSize(), 3);
                UNIT_ASSERT_VALUES_EQUAL(record.GetResources(0).GetResourcePath(), "res0");
                UNIT_ASSERT_VALUES_EQUAL(record.GetResources(1).GetResourcePath(), "res1");
                UNIT_ASSERT_VALUES_EQUAL(record.GetResources(2).GetResourcePath(), "res2");
                UNIT_ASSERT(!record.GetResources(0).GetStartConsuming());
                UNIT_ASSERT(record.GetResources(1).GetStartConsuming());
                UNIT_ASSERT(!record.GetResources(2).GetStartConsuming());
                NKikimrKesus::TEvSubscribeOnResourcesResult ans;
                for (size_t res = 0; res < 3; ++res) {
                    FillResult(ans.AddResults(), 42 + res, 5.0);
                }
                pipe2->SendSubscribeOnResourceResult(ans, cookie);
            }));

        setup.WaitEvent<NKesus::TEvKesus::TEvSubscribeOnResources>();
    }

    Y_UNIT_TEST(ProxyRequestDuringDisconnection) {
        TKesusProxyTestSetup setup;
        auto* pipe = setup.GetPipeFactory().ExpectTabletPipeCreation();

        setup.SendProxyRequest("res");

        EXPECT_CALL(*pipe, OnSubscribeOnResources(_, _))
            .WillOnce(Invoke([&](const NKikimrKesus::TEvSubscribeOnResources& record, ui64 cookie) {
                UNIT_ASSERT_VALUES_EQUAL(record.ResourcesSize(), 1);
                UNIT_ASSERT_VALUES_EQUAL(record.GetResources(0).GetResourcePath(), "res");
                UNIT_ASSERT(!record.GetResources(0).GetStartConsuming());
                NKikimrKesus::TEvSubscribeOnResourcesResult ans;
                FillResult(ans.AddResults());
                pipe->SendSubscribeOnResourceResult(ans, cookie);
            }));

        // Connected
        setup.SendConnected(pipe);

        setup.WaitEvent<NKesus::TEvKesus::TEvSubscribeOnResources>();
    }

    Y_UNIT_TEST(DeactivateSessionWhenResourceClosed) {
        TKesusProxyTestSetup setup;
        auto* pipe = setup.GetPipeFactory().ExpectTabletPipeConnection();
        EXPECT_CALL(*pipe, OnSubscribeOnResources(_, _))
            .WillOnce(Invoke([&](const NKikimrKesus::TEvSubscribeOnResources& record, ui64 cookie) {
                UNIT_ASSERT_VALUES_EQUAL(record.ResourcesSize(), 1);
                UNIT_ASSERT_VALUES_EQUAL(record.GetResources(0).GetResourcePath(), "res");
                UNIT_ASSERT(!record.GetResources(0).GetStartConsuming());
                NKikimrKesus::TEvSubscribeOnResourcesResult ans;
                FillResult(ans.AddResults());
                pipe->SendSubscribeOnResourceResult(ans, cookie);
            }));

        auto session = setup.ProxyRequest("res");
        UNIT_ASSERT_VALUES_EQUAL(session->Get()->ResourceId, 42);

        setup.SendProxyStats({TEvQuota::TProxyStat(42, 1, 0, {}, 1, 25.0, 0, 0)});

        auto& startSession =
            EXPECT_CALL(*pipe, OnUpdateConsumptionState(_, _))
                .WillOnce(Invoke([&](const NKikimrKesus::TEvUpdateConsumptionState& record, ui64) {
                    UNIT_ASSERT_VALUES_EQUAL(record.ResourcesInfoSize(), 1);
                    UNIT_ASSERT_VALUES_EQUAL(record.GetResourcesInfo(0).GetResourceId(), 42);
                    UNIT_ASSERT(record.GetResourcesInfo(0).GetConsumeResource());
                }));

        EXPECT_CALL(*pipe, OnUpdateConsumptionState(_, _))
            .After(startSession)
            .WillOnce(Invoke([&](const NKikimrKesus::TEvUpdateConsumptionState& record, ui64) {
                UNIT_ASSERT_VALUES_EQUAL(record.ResourcesInfoSize(), 1);
                UNIT_ASSERT_VALUES_EQUAL(record.GetResourcesInfo(0).GetResourceId(), 42);
                UNIT_ASSERT(!record.GetResourcesInfo(0).GetConsumeResource());
            }));

        setup.WaitEvent<NKesus::TEvKesus::TEvUpdateConsumptionState>();
        setup.SendCloseSession("res", 42);
        setup.WaitEvent<TEvQuota::TEvProxyCloseSession>();

        // Dispatch some events to let pending events reach their destinations
        setup.GetRuntime().SimulateSleep(TDuration::Zero());
    }

    void SendsProxySessionOnce(bool onSuccess) {
        TKesusProxyTestSetup setup;
        auto* pipe = setup.GetPipeFactory().ExpectTabletPipeConnection();
        EXPECT_CALL(*pipe, OnSubscribeOnResources(_, _))
            .WillRepeatedly(Invoke([&](const NKikimrKesus::TEvSubscribeOnResources& record, ui64 cookie) {
                UNIT_ASSERT_VALUES_EQUAL(record.ResourcesSize(), 1);
                UNIT_ASSERT_VALUES_EQUAL(record.GetResources(0).GetResourcePath(), "res");
                UNIT_ASSERT(!record.GetResources(0).GetStartConsuming());
                NKikimrKesus::TEvSubscribeOnResourcesResult ans;
                FillResult(ans.AddResults(), 42, 5.0);
                pipe->SendSubscribeOnResourceResult(ans, cookie);
            }));

        auto session = setup.ProxyRequest("res");
        UNIT_ASSERT_VALUES_EQUAL(session->Get()->ResourceId, 42);

        EXPECT_CALL(*pipe, OnUpdateConsumptionState(_, _))
            .WillOnce(Invoke([&](const NKikimrKesus::TEvUpdateConsumptionState& record, ui64) {
                UNIT_ASSERT_VALUES_EQUAL(record.ResourcesInfoSize(), 1);
                UNIT_ASSERT_VALUES_EQUAL(record.GetResourcesInfo(0).GetResourceId(), 42);
                UNIT_ASSERT(record.GetResourcesInfo(0).GetConsumeResource());
            }));

        setup.SendProxyStats({TEvQuota::TProxyStat(42, 1, 0, {}, 3, 5.0, 0, 0)});
        setup.WaitEvent<NKesus::TEvKesus::TEvUpdateConsumptionState>();

        // Disconnected
        setup.SendDestroyed(pipe);

        // second pipe
        auto* pipe2 = setup.GetPipeFactory().ExpectTabletPipeConnection();
        EXPECT_CALL(*pipe2, OnSubscribeOnResources(_, _))
            .WillOnce(Invoke([&](const NKikimrKesus::TEvSubscribeOnResources& record, ui64 cookie) {
                UNIT_ASSERT_VALUES_EQUAL(record.ResourcesSize(), 1);
                UNIT_ASSERT_VALUES_EQUAL(record.GetResources(0).GetResourcePath(), "res");
                UNIT_ASSERT(record.GetResources(0).GetStartConsuming());
                NKikimrKesus::TEvSubscribeOnResourcesResult ans;
                if (onSuccess) {
                    FillResult(ans.AddResults(), 42, 5.0);
                } else {
                    FillResultNotFound(ans.AddResults());
                }
                pipe2->SendSubscribeOnResourceResult(ans, cookie);
            }));

        for (size_t i = 0; i < 5; ++i) {
            // Error request. If second ProxySession was sent, it will arrive first.
            setup.SendProxyRequest("//invalid res");

            const auto sessionEvent = setup.GetRuntime().GrabEdgeEvent<TEvQuota::TEvProxySession>(TDuration::MilliSeconds(300));
            UNIT_ASSERT_VALUES_EQUAL(sessionEvent->Resource, "//invalid res");
        }
    }

    Y_UNIT_TEST(SendsProxySessionOnceOnSuccess) {
        SendsProxySessionOnce(true);
    }

    Y_UNIT_TEST(SendsProxySessionOnceOnFailure) {
        SendsProxySessionOnce(false);
    }

    Y_UNIT_TEST(AnswersWithSessionWhenResourceIsAlreadyKnown) {
        TKesusProxyTestSetup setup;
        auto* pipe = setup.GetPipeFactory().ExpectTabletPipeConnection();
        EXPECT_CALL(*pipe, OnSubscribeOnResources(_, _))
            .WillOnce(Invoke([&](const NKikimrKesus::TEvSubscribeOnResources& record, ui64 cookie) {
                UNIT_ASSERT_VALUES_EQUAL(record.ResourcesSize(), 1);
                UNIT_ASSERT_VALUES_EQUAL(record.GetResources(0).GetResourcePath(), "res");
                UNIT_ASSERT(!record.GetResources(0).GetStartConsuming());
                NKikimrKesus::TEvSubscribeOnResourcesResult ans;
                FillResult(ans.AddResults());
                pipe->SendSubscribeOnResourceResult(ans, cookie);
            }));

        auto session = setup.ProxyRequest("res");
        UNIT_ASSERT_VALUES_EQUAL(session->Get()->ResourceId, 42);

        // Session with the same resource
        auto session2 = setup.ProxyRequest("res");
        UNIT_ASSERT_VALUES_EQUAL(session2->Get()->ResourceId, 42);
    }

    Y_UNIT_TEST(SendsBrokenUpdateWhenKesusPassesError) {
        TKesusProxyTestSetup setup;
        auto* pipe = setup.GetPipeFactory().ExpectTabletPipeConnection();
        EXPECT_CALL(*pipe, OnSubscribeOnResources(_, _))
            .WillOnce(Invoke([&](const NKikimrKesus::TEvSubscribeOnResources& record, ui64 cookie) {
                UNIT_ASSERT_VALUES_EQUAL(record.ResourcesSize(), 1);
                UNIT_ASSERT_VALUES_EQUAL(record.GetResources(0).GetResourcePath(), "res");
                UNIT_ASSERT(!record.GetResources(0).GetStartConsuming());
                NKikimrKesus::TEvSubscribeOnResourcesResult ans;
                FillResult(ans.AddResults());
                pipe->SendSubscribeOnResourceResult(ans, cookie);
            }));

        auto session = setup.ProxyRequest("res");
        UNIT_ASSERT_VALUES_EQUAL(session->Get()->ResourceId, 42);

        setup.SendResourcesAllocated(pipe, 42, 0, Ydb::StatusIds::NOT_FOUND);

        bool broken = false;
        for (size_t i = 0; i < 3; ++i) {
            auto update = setup.GetProxyUpdate();
            UNIT_ASSERT_VALUES_EQUAL(update->Get()->Resources.size(), 1);
            if (update->Get()->Resources[0].ResourceState == TEvQuota::EUpdateState::Broken) {
                broken = true;
                break;
            }
        }
        UNIT_ASSERT(broken);
    }

    Y_UNIT_TEST(AllocatesResourceWithKesus) {
        TKesusProxyTestSetup setup;
        auto* pipe = setup.GetPipeFactory().ExpectTabletPipeConnection();
        EXPECT_CALL(*pipe, OnSubscribeOnResources(_, _))
            .WillOnce(Invoke([&](const NKikimrKesus::TEvSubscribeOnResources& record, ui64 cookie) {
                UNIT_ASSERT_VALUES_EQUAL(record.ResourcesSize(), 1);
                UNIT_ASSERT_VALUES_EQUAL(record.GetResources(0).GetResourcePath(), "res");
                NKikimrKesus::TEvSubscribeOnResourcesResult ans;
                FillResult(ans.AddResults());
                pipe->SendSubscribeOnResourceResult(ans, cookie);
            }));

        auto session = setup.ProxyRequest("res");
        UNIT_ASSERT_VALUES_EQUAL(session->Get()->ResourceId, 42);

        UNIT_ASSERT(setup.ConsumeResourceAllocateByKesus(pipe, 42, 30.0, session->Get()->TickSize));
    }

    Y_UNIT_TEST(DisconnectsDuringActiveSession) {
        TKesusProxyTestSetup setup;
        auto* pipe = setup.GetPipeFactory().ExpectTabletPipeConnection();
        EXPECT_CALL(*pipe, OnSubscribeOnResources(_, _))
            .WillOnce(Invoke([&](const NKikimrKesus::TEvSubscribeOnResources& record, ui64 cookie) {
                UNIT_ASSERT_VALUES_EQUAL(record.ResourcesSize(), 1);
                UNIT_ASSERT_VALUES_EQUAL(record.GetResources(0).GetResourcePath(), "res");
                NKikimrKesus::TEvSubscribeOnResourcesResult ans;
                FillResult(ans.AddResults());
                pipe->SendSubscribeOnResourceResult(ans, cookie);
            }));

        setup.GetPipeFactory().ExpectTabletPipeCreation(); // Expect second pipe. Without connecting.

        auto session = setup.ProxyRequest("res");
        UNIT_ASSERT_VALUES_EQUAL(session->Get()->ResourceId, 42);

        UNIT_ASSERT(!setup.ConsumeResourceAllocateByKesus(pipe, 42, 30.0, session->Get()->TickSize, 1));

        // Disconnected
        setup.SendDestroyed(pipe);

        UNIT_ASSERT(setup.ConsumeResourceAdvanceTime(42, 30.0, session->Get()->TickSize));
    }

    Y_UNIT_TEST(AllocatesResourceOffline) {
        TKesusProxyTestSetup setup;
        auto* pipe = setup.GetPipeFactory().ExpectTabletPipeConnection();
        EXPECT_CALL(*pipe, OnSubscribeOnResources(_, _))
            .WillOnce(Invoke([&](const NKikimrKesus::TEvSubscribeOnResources& record, ui64 cookie) {
                UNIT_ASSERT_VALUES_EQUAL(record.ResourcesSize(), 1);
                UNIT_ASSERT_VALUES_EQUAL(record.GetResources(0).GetResourcePath(), "res");
                NKikimrKesus::TEvSubscribeOnResourcesResult ans;
                FillResult(ans.AddResults());
                pipe->SendSubscribeOnResourceResult(ans, cookie);
            }));

        setup.GetPipeFactory().ExpectTabletPipeCreation(); // Expect second pipe. Without connecting.

        // No statistics, so resource is allocated every 100 ms with default speed.

        auto session = setup.ProxyRequest("res");
        UNIT_ASSERT_VALUES_EQUAL(session->Get()->ResourceId, 42);

        // Disconnected
        setup.SendDestroyed(pipe);

        UNIT_ASSERT(setup.ConsumeResourceAdvanceTime(42, 30.0, session->Get()->TickSize));
    }

    Y_UNIT_TEST(ConnectsDuringOfflineAllocation) {
        TKesusProxyTestSetup setup;
        auto* pipe = setup.GetPipeFactory().ExpectTabletPipeConnection();
        EXPECT_CALL(*pipe, OnSubscribeOnResources(_, _))
            .WillOnce(Invoke([&](const NKikimrKesus::TEvSubscribeOnResources& record, ui64 cookie) {
                UNIT_ASSERT_VALUES_EQUAL(record.ResourcesSize(), 1);
                UNIT_ASSERT_VALUES_EQUAL(record.GetResources(0).GetResourcePath(), "res");
                NKikimrKesus::TEvSubscribeOnResourcesResult ans;
                FillResult(ans.AddResults());
                pipe->SendSubscribeOnResourceResult(ans, cookie);
            }));

        auto* pipe2 = setup.GetPipeFactory().ExpectTabletPipeCreation(); // Expect second pipe. Without connecting.

        // No statistics, so resource is allocated every 100 ms with default speed.

        auto session = setup.ProxyRequest("res");
        UNIT_ASSERT_VALUES_EQUAL(session->Get()->ResourceId, 42);

        // Disconnected
        setup.SendDestroyed(pipe);

        UNIT_ASSERT(!setup.ConsumeResourceAdvanceTime(42, 60.0, session->Get()->TickSize, 1));

        setup.SendConnected(pipe2);

        UNIT_ASSERT(setup.ConsumeResourceAllocateByKesus(pipe2, 42, 60.0, session->Get()->TickSize, 2));
    }
}

Y_UNIT_TEST_SUITE(KesusResourceAllocationStatisticsTest) {
    using NQuoter::TKesusResourceAllocationStatistics;

    void CheckParams(TKesusResourceAllocationStatistics& stat, TDuration delta, double amount) {
        auto params = stat.GetAverageAllocationParams();
        UNIT_ASSERT_VALUES_EQUAL(params.first, delta);
        UNIT_ASSERT_DOUBLES_EQUAL(params.second, amount, 0.001);
    }

    Y_UNIT_TEST(ReturnsDefaultValues) {
        TKesusResourceAllocationStatistics stat;
        NKikimrKesus::TStreamingQuoterResource props;
        props.MutableHierarchicalDRRResourceConfig()->SetMaxUnitsPerSecond(42);
        stat.SetProps(props);

        CheckParams(stat, TDuration::MilliSeconds(100), 4.2);

        // Add one allocation event:
        // still can't calculate.
        stat.OnResourceAllocated(TInstant::Seconds(100), 1000);
        CheckParams(stat, TDuration::MilliSeconds(100), 4.2);

        // Now we can calculate.
        stat.OnResourceAllocated(TInstant::Seconds(200), 2000);
        CheckParams(stat, TDuration::Seconds(100), 1500);
    }

    Y_UNIT_TEST(CalculatesAverage) {
        TKesusResourceAllocationStatistics stat(5);
        stat.OnResourceAllocated(TInstant::Seconds(100), 1000);
        stat.OnResourceAllocated(TInstant::Seconds(200), 1000);
        CheckParams(stat, TDuration::Seconds(100), 1000);

        stat.OnResourceAllocated(TInstant::Seconds(400), 4000);
        CheckParams(stat, TDuration::Seconds(150), 2000);

        stat.OnResourceAllocated(TInstant::Seconds(1000), 4000);
        CheckParams(stat, TDuration::Seconds(300), 2500);

        stat.OnResourceAllocated(TInstant::Seconds(1300), 5000);
        CheckParams(stat, TDuration::Seconds(300), 3000);

        // Forgets first value.
        stat.OnResourceAllocated(TInstant::Seconds(2000), 2000);
        CheckParams(stat, TDuration::Seconds(450), 3200);

        // Forgets second value.
        stat.OnResourceAllocated(TInstant::Seconds(4400), 5000);
        CheckParams(stat, TDuration::Seconds(1000), 4000);
    }

    Y_UNIT_TEST(TakesBestStat) {
        TKesusResourceAllocationStatistics stat(4);
        stat.OnResourceAllocated(TInstant::Seconds(100), 10);
        stat.OnResourceAllocated(TInstant::Seconds(200), 10);
        CheckParams(stat, TDuration::Seconds(100), 10);

        stat.OnConnected();
        CheckParams(stat, TDuration::Seconds(100), 10);
        stat.OnResourceAllocated(TInstant::Seconds(300), 20);
        CheckParams(stat, TDuration::Seconds(100), 10);
        stat.OnResourceAllocated(TInstant::Seconds(400), 20);
        CheckParams(stat, TDuration::Seconds(100), 20);
        stat.OnResourceAllocated(TInstant::Seconds(500), 20);
        CheckParams(stat, TDuration::Seconds(100), 20);

        stat.OnConnected();
        CheckParams(stat, TDuration::Seconds(100), 20);
        stat.OnResourceAllocated(TInstant::Seconds(700), 30);
        CheckParams(stat, TDuration::Seconds(100), 20);
        stat.OnResourceAllocated(TInstant::Seconds(800), 30);
        CheckParams(stat, TDuration::Seconds(100), 20);

        stat.OnConnected();
        CheckParams(stat, TDuration::Seconds(100), 20);
        stat.OnResourceAllocated(TInstant::Seconds(900), 40);
        CheckParams(stat, TDuration::Seconds(100), 20);
        stat.OnResourceAllocated(TInstant::Seconds(1100), 40);
        CheckParams(stat, TDuration::Seconds(100), 20);
        stat.OnResourceAllocated(TInstant::Seconds(1300), 40);
        CheckParams(stat, TDuration::Seconds(200), 40);
        stat.OnResourceAllocated(TInstant::Seconds(1800), 80);
        CheckParams(stat, TDuration::Seconds(300), 50);
    }
}

} // namespace NKikimr
