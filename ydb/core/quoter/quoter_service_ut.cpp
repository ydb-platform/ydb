#include "quoter_service.h"

#include <ydb/core/kesus/tablet/events.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/helpers.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/library/ydb_issue/proto/issue_id.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/compiler.h>
#include <util/system/valgrind.h>

namespace NKikimr {
using namespace Tests;

Y_UNIT_TEST_SUITE(TQuoterServiceTest) {
    Y_UNIT_TEST(StaticRateLimiter) {
        TServerSettings serverSettings(0);
        TServer server = TServer(serverSettings, true);
        TTestActorRuntime *runtime = server.GetRuntime();

        const TActorId serviceId = MakeQuoterServiceID();
        const TActorId serviceActorId = runtime->Register(CreateQuoterService());
        runtime->RegisterService(serviceId, serviceActorId);

        TActorId sender = runtime->AllocateEdgeActor();
        {
            runtime->Send(new IEventHandle(MakeQuoterServiceID(), sender,
                new TEvQuota::TEvRequest(TEvQuota::EResourceOperator::And, {
                    TEvQuota::TResourceLeaf(TEvQuota::TResourceLeaf::QuoterSystem, TEvQuota::TResourceLeaf::ResourceForbid, 1)
                    }, TDuration::Max())));

            THolder<TEvQuota::TEvClearance> reply = runtime->GrabEdgeEvent<TEvQuota::TEvClearance>();
            UNIT_ASSERT(reply->Result == TEvQuota::TEvClearance::EResult::Deadline);
        }
        {
            runtime->Send(new IEventHandle(MakeQuoterServiceID(), sender,
                new TEvQuota::TEvRequest(TEvQuota::EResourceOperator::And, {
                    TEvQuota::TResourceLeaf(TEvQuota::TResourceLeaf::QuoterSystem, TEvQuota::TResourceLeaf::ResourceNocheck, 1)
                    }, TDuration::Max())));

            THolder<TEvQuota::TEvClearance> reply = runtime->GrabEdgeEvent<TEvQuota::TEvClearance>();
            UNIT_ASSERT(reply->Result == TEvQuota::TEvClearance::EResult::Success);
        }

        {
            runtime->Send(new IEventHandle(MakeQuoterServiceID(), sender,
                new TEvQuota::TEvRequest(TEvQuota::EResourceOperator::And, {
                    TEvQuota::TResourceLeaf(TEvQuota::TResourceLeaf::QuoterSystem, TEvQuota::TResourceLeaf::MakeTaggedRateRes(1, 1000), 1)
                    }, TDuration::Max())));

            THolder<TEvQuota::TEvClearance> reply = runtime->GrabEdgeEvent<TEvQuota::TEvClearance>();
            UNIT_ASSERT(reply->Result == TEvQuota::TEvClearance::EResult::Success);
        }

        {
            // test static quter queues processing
            size_t cnt = 100;
            for (size_t i = 0; i < cnt; ++i) {
                runtime->Send(new IEventHandle(MakeQuoterServiceID(), sender,
                    new TEvQuota::TEvRequest(TEvQuota::EResourceOperator::And, {
                        TEvQuota::TResourceLeaf(TEvQuota::TResourceLeaf::QuoterSystem, TEvQuota::TResourceLeaf::MakeTaggedRateRes(2, 50), 1)
                        }, TDuration::Max()), 0, 300 + i));
            }

            TAutoPtr<IEventHandle> ev;
            for (size_t i = 0; i < cnt; ++i) {
                TEvQuota::TEvClearance* reply = runtime->GrabEdgeEvent<TEvQuota::TEvClearance>(ev);
                UNIT_ASSERT(reply->Result == TEvQuota::TEvClearance::EResult::Success);
                UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, 300 + i);
            }
        }

        {
            auto resId = TEvQuota::TResourceLeaf::MakeTaggedRateRes(1, 1);

            runtime->Send(new IEventHandle(MakeQuoterServiceID(), sender,
                new TEvQuota::TEvRequest(TEvQuota::EResourceOperator::And, {
                    TEvQuota::TResourceLeaf(TEvQuota::TResourceLeaf::QuoterSystem, resId, 1)
                    }, TDuration::Max())));

            THolder<TEvQuota::TEvClearance> reply = runtime->GrabEdgeEvent<TEvQuota::TEvClearance>();
            UNIT_ASSERT(reply->Result == TEvQuota::TEvClearance::EResult::Success);

            runtime->Send(new IEventHandle(MakeQuoterServiceID(), sender,
                new TEvQuota::TEvRequest(TEvQuota::EResourceOperator::And, {
                    TEvQuota::TResourceLeaf(TEvQuota::TResourceLeaf::QuoterSystem, resId, 1000, true)
                    }, TDuration::Seconds(1))));

            reply = runtime->GrabEdgeEvent<TEvQuota::TEvClearance>();
            UNIT_ASSERT(reply->Result == TEvQuota::TEvClearance::EResult::Success);

            runtime->Send(new IEventHandle(MakeQuoterServiceID(), sender,
                new TEvQuota::TEvRequest(TEvQuota::EResourceOperator::And, {
                    TEvQuota::TResourceLeaf(TEvQuota::TResourceLeaf::QuoterSystem, resId, 1000)
                    }, TDuration::Seconds(1))));

            reply = runtime->GrabEdgeEvent<TEvQuota::TEvClearance>();
            UNIT_ASSERT(reply->Result == TEvQuota::TEvClearance::EResult::Deadline);

            runtime->Send(new IEventHandle(MakeQuoterServiceID(), sender,
                new TEvQuota::TEvRequest(TEvQuota::EResourceOperator::And, {
                    TEvQuota::TResourceLeaf(TEvQuota::TResourceLeaf::QuoterSystem, resId, 1000, true)
                    }, TDuration::Seconds(1))));

            reply = runtime->GrabEdgeEvent<TEvQuota::TEvClearance>();
            UNIT_ASSERT(reply->Result == TEvQuota::TEvClearance::EResult::Success);
        }
    }

    // Send a scheme operation and wait for it to be accepted (ExecInProgress).
    // Use SimulateSleep after calling this to give schemeshard time to complete.
    void SendSchemeOpAndWaitAccepted(TTestActorRuntime* runtime, THolder<TEvTxUserProxy::TEvProposeTransaction> propose, const TString& opName) {
        TActorId sender = runtime->AllocateEdgeActor();
        runtime->Send(new IEventHandle(MakeTxProxyID(), sender, propose.Release()));

        TAutoPtr<IEventHandle> handle;
        auto event = runtime->GrabEdgeEventRethrow<TEvTxUserProxy::TEvProposeTransactionStatus>(handle);
        auto status = static_cast<TEvTxUserProxy::TResultStatus::EStatus>(event->Record.GetStatus());
        UNIT_ASSERT_C(status == TEvTxUserProxy::TResultStatus::ExecComplete
            || status == TEvTxUserProxy::TResultStatus::ExecInProgress,
            opName << " failed with status: " << event->Record.GetStatus());
    }

    // Async helper: initialize root scheme storage pools (equivalent of TClient::InitRootScheme)
    void InitRootSchemeAsync(TTestActorRuntime* runtime) {
        auto propose = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        auto* tx = propose->Record.MutableTransaction()->MutableModifyScheme();
        tx->SetWorkingDir("/");
        tx->SetOperationType(NKikimrSchemeOp::ESchemeOpAlterSubDomain);
        auto* op = tx->MutableSubDomain();
        op->SetName(Tests::TestDomainName);
        auto* pool = op->AddStoragePools();
        pool->SetKind("test");
        pool->SetName(TStringBuilder() << "/" << Tests::TestDomainName << ":test");

        SendSchemeOpAndWaitAccepted(runtime, std::move(propose), "InitRootScheme");
        runtime->SimulateSleep(TDuration::MilliSeconds(500));
    }

    // Async helper: create Kesus node via tx proxy
    void CreateKesusAsync(TTestActorRuntime* runtime) {
        auto propose = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        auto* tx = propose->Record.MutableTransaction()->MutableModifyScheme();
        tx->SetWorkingDir(TStringBuilder() << "/" << Tests::TestDomainName);
        tx->SetOperationType(NKikimrSchemeOp::ESchemeOpCreateKesus);
        tx->MutableKesus()->SetName("KesusQuoter");

        SendSchemeOpAndWaitAccepted(runtime, std::move(propose), "CreateKesus");
        runtime->SimulateSleep(TDuration::MilliSeconds(500));
    }

    // Async helper: get Kesus tablet ID via scheme cache
    ui64 GetKesusTabletIdAsync(TTestActorRuntime* runtime) {
        TActorId sender = runtime->AllocateEdgeActor();

        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        auto& entry = request->ResultSet.emplace_back();
        entry.Path = SplitPath(TStringBuilder() << "/" << Tests::TestDomainName << "/KesusQuoter");
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;

        runtime->Send(new IEventHandle(MakeSchemeCacheID(), sender,
            new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release())));

        TAutoPtr<IEventHandle> handle;
        runtime->GrabEdgeEventRethrow<TEvTxProxySchemeCache::TEvNavigateKeySetResult>(handle);
        auto* result = handle->Get<TEvTxProxySchemeCache::TEvNavigateKeySetResult>();
        auto& resultEntry = result->Request->ResultSet.at(0);
        UNIT_ASSERT_C(resultEntry.Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok,
            "Navigate failed: " << static_cast<int>(resultEntry.Status));
        UNIT_ASSERT(resultEntry.KesusInfo);
        return resultEntry.KesusInfo->Description.GetKesusTabletId();
    }

    // Async helper: create resource on Kesus tablet
    void CreateKesusResourceAsync(TTestActorRuntime* runtime, double rate) {
        const ui64 tabletId = GetKesusTabletIdAsync(runtime);

        TAutoPtr<NKesus::TEvKesus::TEvAddQuoterResource> request(new NKesus::TEvKesus::TEvAddQuoterResource());
        request->Record.MutableResource()->SetResourcePath("/Res");
        request->Record.MutableResource()->MutableHierarchicalDRRResourceConfig()->SetMaxUnitsPerSecond(rate);

        TActorId sender = runtime->AllocateEdgeActor();
        ForwardToTablet(*runtime, tabletId, sender, request.Release(), 0);

        TAutoPtr<IEventHandle> handle;
        runtime->GrabEdgeEvent<NKesus::TEvKesus::TEvAddQuoterResourceResult>(handle);
        const NKikimrKesus::TEvAddQuoterResourceResult& record = handle->Get<NKesus::TEvKesus::TEvAddQuoterResourceResult>()->Record;
        UNIT_ASSERT_VALUES_EQUAL(record.GetError().GetStatus(), Ydb::StatusIds::SUCCESS);
    }

    void CreateKesus(TServer& server) {
        Tests::TClient client(server.GetSettings());
        client.InitRootScheme();
        const NMsgBusProxy::EResponseStatus status = client.CreateKesus(Tests::TestDomainName, "KesusQuoter");
        UNIT_ASSERT_VALUES_EQUAL(status, NMsgBusProxy::MSTATUS_OK);
    }

    void CreateKesusResource(TServer& server, double rate) {
        Tests::TClient client(server.GetSettings());
        TTestActorRuntime* const runtime = server.GetRuntime();

        // request
        TAutoPtr<NKesus::TEvKesus::TEvAddQuoterResource> request(new NKesus::TEvKesus::TEvAddQuoterResource());
        request->Record.MutableResource()->SetResourcePath("/Res");
        request->Record.MutableResource()->MutableHierarchicalDRRResourceConfig()->SetMaxUnitsPerSecond(rate);

        // Get tablet id
        TAutoPtr<NMsgBusProxy::TBusResponse> resp = client.Ls(TStringBuilder() << Tests::TestDomainName << "/KesusQuoter");
        UNIT_ASSERT_EQUAL(resp->Record.GetStatusCode(), NKikimrIssues::TStatusIds::SUCCESS);
        const auto& pathDesc = resp->Record.GetPathDescription();
        UNIT_ASSERT(pathDesc.HasKesus());
        const ui64 tabletId = pathDesc.GetKesus().GetKesusTabletId();

        TActorId sender = runtime->AllocateEdgeActor();
        ForwardToTablet(*runtime, tabletId, sender, request.Release(), 0);

        TAutoPtr<IEventHandle> handle;
        runtime->GrabEdgeEvent<NKesus::TEvKesus::TEvAddQuoterResourceResult>(handle);
        const NKikimrKesus::TEvAddQuoterResourceResult& record = handle->Get<NKesus::TEvKesus::TEvAddQuoterResourceResult>()->Record;
        UNIT_ASSERT_VALUES_EQUAL(record.GetError().GetStatus(), Ydb::StatusIds::SUCCESS);
    }

#if defined(OPTIMIZED)
#error "Macro conflict."
#endif

#if defined(_MSC_VER)

#if defined(NDEBUG)
#define OPTIMIZED // release builds
#endif

#else // non msvc compiler: use __OPTIMIZE__ flag to include relwithdebinfo builds

#if defined(__OPTIMIZE__)
#define OPTIMIZED // release builds and relwithdebinfo builds
#endif

#endif

#if defined(OPTIMIZED) && !defined(_san_enabled_) && !defined(WITH_VALGRIND)
    enum class ESpeedTestResourceType {
        StaticTaggedRateResource,
        KesusResource,
    };

    // Tests that quoter service can serve resource allocation requests at high rates.
    void SpeedTest(ESpeedTestResourceType resType) {
        TPortManager portManager;
        TServerSettings serverSettings(portManager.GetPort());
        TServer server = TServer(serverSettings, true);

        TTestActorRuntime* runtime = server.GetRuntime();

        const TActorId serviceId = MakeQuoterServiceID();
        const TActorId serviceActorId = runtime->Register(CreateQuoterService());
        runtime->RegisterService(serviceId, serviceActorId);

        const TActorId sender = runtime->AllocateEdgeActor();

        constexpr TDuration testDuration = TDuration::Seconds(2);
        constexpr TDuration waitDuration = TDuration::MilliSeconds(150);
        constexpr ui32 rate = 2000;

        constexpr double secondsForTest = static_cast<double>(testDuration.MicroSeconds()) / 1000000.0;
        constexpr double secondsForWait = static_cast<double>(waitDuration.MicroSeconds()) / 1000000.0;
        constexpr double doubleRate = static_cast<double>(rate);

        TString quoter;
        TString resource;
        if (resType == ESpeedTestResourceType::KesusResource) {
            CreateKesus(server);
            CreateKesusResource(server, doubleRate);
            quoter = TStringBuilder() << "/" << Tests::TestDomainName << "/KesusQuoter";
            resource = "Res";
        }

        const TEvQuota::TResourceLeaf resLeaf = resType == ESpeedTestResourceType::StaticTaggedRateResource ?
            TEvQuota::TResourceLeaf(TEvQuota::TResourceLeaf::QuoterSystem, TEvQuota::TResourceLeaf::MakeTaggedRateRes(42, rate), 1) :
            TEvQuota::TResourceLeaf(quoter, resource, 1);

        for (size_t iteration = 0; iteration < 2; ++iteration) {
            const TInstant start = TInstant::Now();
            size_t sent = 0;
            while (TInstant::Now() - start < testDuration) {
                runtime->Send(new IEventHandle(MakeQuoterServiceID(), sender,
                                               new TEvQuota::TEvRequest(TEvQuota::EResourceOperator::And, { resLeaf }, waitDuration), 0, 0));
                ++sent;
                if ((sent & 3) != 0) {
                    Sleep(TDuration::MicroSeconds(1));
                }
            }
            Cerr << "Requests sent: " << sent << Endl;

            if (static_cast<double>(sent) > secondsForTest * doubleRate * 7.0) { // check if we have slow machine
                TAutoPtr<IEventHandle> ev;
                int ok = 0;
                int deadline = 0;
                for (size_t i = 0; i < sent; ++i) {
                    TEvQuota::TEvClearance* reply = runtime->GrabEdgeEvent<TEvQuota::TEvClearance>(ev);
                    if (reply->Result == TEvQuota::TEvClearance::EResult::Success) {
                        ++ok;
                    } else if (reply->Result == TEvQuota::TEvClearance::EResult::Deadline) {
                        ++deadline;
                    } else {
                        UNIT_ASSERT(false);
                    }
                }

                Cerr << "OK: " << ok << Endl;
                Cerr << "Deadline: " << deadline << Endl;
                const double expectedSuccesses = (secondsForTest + secondsForWait) * doubleRate;
                Cerr << "Expected OK's: " << expectedSuccesses << Endl;
                const double maxDeviation = expectedSuccesses * 0.2;
                UNIT_ASSERT_DOUBLES_EQUAL_C(static_cast<double>(ok), expectedSuccesses, maxDeviation,
                                            "ok: " << ok << ", deadline: " << deadline << ", sent: " << sent << ", expectedSuccesses: " << expectedSuccesses
                                            << ", secondsForTest: " << secondsForTest << ", secondsForWait: " << secondsForWait);
            } else {
                Cerr << "Too few requests sent" << Endl;
                break; // Else we would receive TEvClearance from previous test iteration.
            }

            if (iteration == 0) {
                Sleep(TDuration::MilliSeconds(300)); // Make a pause to check that algorithm will consider it.
            }
        }
    }

    Y_UNIT_TEST(StaticRateLimiterSpeed) {
        SpeedTest(ESpeedTestResourceType::StaticTaggedRateResource);
    }

    Y_UNIT_TEST(KesusResourceSpeed) {
        SpeedTest(ESpeedTestResourceType::KesusResource);
    }
#endif

    Y_UNIT_TEST(StaticMultipleAndResources) {
        TServerSettings serverSettings(0);
        TServer server = TServer(serverSettings, true);
        TTestActorRuntime *runtime = server.GetRuntime();

        const TActorId serviceId = MakeQuoterServiceID();
        const TActorId serviceActorId = runtime->Register(CreateQuoterService());
        runtime->RegisterService(serviceId, serviceActorId);

        TActorId sender = runtime->AllocateEdgeActor();
        {
            runtime->Send(new IEventHandle(MakeQuoterServiceID(), sender,
                new TEvQuota::TEvRequest(TEvQuota::EResourceOperator::And, {
                    TEvQuota::TResourceLeaf(TEvQuota::TResourceLeaf::QuoterSystem, TEvQuota::TResourceLeaf::ResourceForbid, 1),
                    TEvQuota::TResourceLeaf(TEvQuota::TResourceLeaf::QuoterSystem, TEvQuota::TResourceLeaf::ResourceNocheck, 1),
                    }, TDuration::Max())));

            THolder<TEvQuota::TEvClearance> reply = runtime->GrabEdgeEvent<TEvQuota::TEvClearance>();
            UNIT_ASSERT(reply->Result == TEvQuota::TEvClearance::EResult::Deadline);
        }
        {
            runtime->Send(new IEventHandle(MakeQuoterServiceID(), sender,
                new TEvQuota::TEvRequest(TEvQuota::EResourceOperator::And, {
                    TEvQuota::TResourceLeaf(TEvQuota::TResourceLeaf::QuoterSystem, TEvQuota::TResourceLeaf::ResourceNocheck, 1),
                    TEvQuota::TResourceLeaf(TEvQuota::TResourceLeaf::QuoterSystem, TEvQuota::TResourceLeaf::ResourceNocheck, 1),
                    }, TDuration::Max())));

            THolder<TEvQuota::TEvClearance> reply = runtime->GrabEdgeEvent<TEvQuota::TEvClearance>();
            UNIT_ASSERT(reply->Result == TEvQuota::TEvClearance::EResult::Success);
        }

        {
            runtime->Send(new IEventHandle(MakeQuoterServiceID(), sender,
                new TEvQuota::TEvRequest(TEvQuota::EResourceOperator::And, {
                    TEvQuota::TResourceLeaf(TEvQuota::TResourceLeaf::QuoterSystem, TEvQuota::TResourceLeaf::MakeTaggedRateRes(1, 1000), 1),
                    TEvQuota::TResourceLeaf(TEvQuota::TResourceLeaf::QuoterSystem, TEvQuota::TResourceLeaf::ResourceNocheck, 1),
                    }, TDuration::Max())));

            THolder<TEvQuota::TEvClearance> reply = runtime->GrabEdgeEvent<TEvQuota::TEvClearance>();
            UNIT_ASSERT(reply->Result == TEvQuota::TEvClearance::EResult::Success);
        }
    }

    Y_UNIT_TEST(StaticDeadlines) {
        TServerSettings serverSettings(0);
        TServer server = TServer(serverSettings, true);
        TTestActorRuntime *runtime = server.GetRuntime();

        const TActorId serviceId = MakeQuoterServiceID();
        const TActorId serviceActorId = runtime->Register(CreateQuoterService());
        runtime->RegisterService(serviceId, serviceActorId);

        TActorId sender = runtime->AllocateEdgeActor();
        {
            runtime->Send(new IEventHandle(MakeQuoterServiceID(), sender,
                new TEvQuota::TEvRequest(TEvQuota::EResourceOperator::And, {
                    TEvQuota::TResourceLeaf(TEvQuota::TResourceLeaf::QuoterSystem, TEvQuota::TResourceLeaf::MakeTaggedRateRes(1, 10), 20)
                    }, TDuration::Seconds(3))));

            runtime->Send(new IEventHandle(MakeQuoterServiceID(), sender,
                new TEvQuota::TEvRequest(TEvQuota::EResourceOperator::And, {
                    TEvQuota::TResourceLeaf(TEvQuota::TResourceLeaf::QuoterSystem, TEvQuota::TResourceLeaf::MakeTaggedRateRes(1, 10), 20)
                    }, TDuration::Seconds(3))));

            runtime->Send(new IEventHandle(MakeQuoterServiceID(), sender,
                new TEvQuota::TEvRequest(TEvQuota::EResourceOperator::And, {
                    TEvQuota::TResourceLeaf(TEvQuota::TResourceLeaf::QuoterSystem, TEvQuota::TResourceLeaf::MakeTaggedRateRes(1, 10), 20)
                    }, TDuration::Seconds(3))));

            THolder<TEvQuota::TEvClearance> reply1 = runtime->GrabEdgeEvent<TEvQuota::TEvClearance>();
            UNIT_ASSERT_C(reply1->Result == TEvQuota::TEvClearance::EResult::Success, "Result: " << static_cast<int>(reply1->Result));

            THolder<TEvQuota::TEvClearance> reply2 = runtime->GrabEdgeEvent<TEvQuota::TEvClearance>();
            UNIT_ASSERT_C(reply2->Result == TEvQuota::TEvClearance::EResult::Success, "Result: " << static_cast<int>(reply2->Result));

            THolder<TEvQuota::TEvClearance> reply3 = runtime->GrabEdgeEvent<TEvQuota::TEvClearance>();
            UNIT_ASSERT_C(reply3->Result == TEvQuota::TEvClearance::EResult::Deadline, "Result: " << static_cast<int>(reply3->Result));
        }
    }

    Y_UNIT_TEST(CleanupDoesNotCloseKesusResourceBeforeOneHour) {
        TPortManager portManager;
        TServerSettings serverSettings(portManager.GetPort());
        serverSettings.SetUseRealThreads(false);
        TServer server = TServer(serverSettings, true);
        TTestActorRuntime* runtime = server.GetRuntime();

        const TActorId serviceId = MakeQuoterServiceID();
        const TActorId serviceActorId = runtime->Register(CreateQuoterService());
        runtime->RegisterService(serviceId, serviceActorId);

        auto dispatchEvents = [&]() {
            TDispatchOptions options;
            options.FinalEvents.emplace_back([](IEventHandle&) { return false; });
            runtime->DispatchEvents(options, TDuration::MilliSeconds(1));
        };

        InitRootSchemeAsync(runtime);
        CreateKesusAsync(runtime);
        CreateKesusResourceAsync(runtime, 10.0);
        runtime->AdvanceCurrentTime(TDuration::Seconds(2));
        dispatchEvents();

        const TActorId sender = runtime->AllocateEdgeActor();
        const TEvQuota::TResourceLeaf resLeaf(
            TStringBuilder() << "/" << Tests::TestDomainName << "/KesusQuoter",
            "Res",
            1);

        ui32 proxySessions = 0;
        ui32 proxyCloseSessions = 0;
        ui64 requestCookie = 1;
        auto proxySessionObserver = runtime->AddObserver<TEvQuota::TEvProxySession>(
            [&](TEvQuota::TEvProxySession::TPtr& ev) {
                if (ev->Get()->Result == TEvQuota::TEvProxySession::Success) {
                    ++proxySessions;
                }
            });
        auto proxyCloseObserver = runtime->AddObserver<TEvQuota::TEvProxyCloseSession>(
            [&](TEvQuota::TEvProxyCloseSession::TPtr&) {
                ++proxyCloseSessions;
            });

        auto requestQuota = [&] {
            const ui64 cookie = requestCookie++;
            runtime->Send(new IEventHandle(
                MakeQuoterServiceID(),
                sender,
                new TEvQuota::TEvRequest(TEvQuota::EResourceOperator::And, {resLeaf}, TDuration::Max()),
                0,
                cookie));
            runtime->AdvanceCurrentTime(TDuration::Seconds(10));

            auto event = runtime->GrabEdgeEventIf<TEvQuota::TEvClearance>(
                {sender},
                [cookie](const auto& ev) {
                    return ev->Cookie == cookie;
                });
            UNIT_ASSERT_VALUES_EQUAL(event->Get()->Result, TEvQuota::TEvClearance::EResult::Success);
        };

        requestQuota();
        UNIT_ASSERT_VALUES_EQUAL(proxySessions, 1);
        UNIT_ASSERT_VALUES_EQUAL(proxyCloseSessions, 0);

        runtime->AdvanceCurrentTime(TDuration::Minutes(59));
        dispatchEvents();

        UNIT_ASSERT_VALUES_EQUAL(proxyCloseSessions, 0);

        requestQuota();
        UNIT_ASSERT_VALUES_EQUAL(proxySessions, 1);
        UNIT_ASSERT_VALUES_EQUAL(proxyCloseSessions, 0);
    }

    Y_UNIT_TEST(CleanupClosesAndReopensIdleKesusResource) {
        TPortManager portManager;
        TServerSettings serverSettings(portManager.GetPort());
        serverSettings.SetUseRealThreads(false);
        TServer server = TServer(serverSettings, true);
        TTestActorRuntime* runtime = server.GetRuntime();

        const TActorId serviceId = MakeQuoterServiceID();
        const TActorId serviceActorId = runtime->Register(CreateQuoterService());
        runtime->RegisterService(serviceId, serviceActorId);

        auto dispatchEvents = [&]() {
            TDispatchOptions options;
            options.FinalEvents.emplace_back([](IEventHandle&) { return false; });
            runtime->DispatchEvents(options, TDuration::MilliSeconds(1));
        };

        InitRootSchemeAsync(runtime);
        CreateKesusAsync(runtime);
        CreateKesusResourceAsync(runtime, 10.0);
        runtime->AdvanceCurrentTime(TDuration::Seconds(2));
        dispatchEvents();

        const TActorId sender = runtime->AllocateEdgeActor();
        const TEvQuota::TResourceLeaf resLeaf(
            TStringBuilder() << "/" << Tests::TestDomainName << "/KesusQuoter",
            "Res",
            1);

        ui32 proxySessions = 0;
        ui32 proxyCloseSessions = 0;
        ui64 requestCookie = 1;
        auto proxySessionObserver = runtime->AddObserver<TEvQuota::TEvProxySession>(
            [&](TEvQuota::TEvProxySession::TPtr& ev) {
                if (ev->Get()->Result == TEvQuota::TEvProxySession::Success) {
                    ++proxySessions;
                }
            });
        auto proxyCloseObserver = runtime->AddObserver<TEvQuota::TEvProxyCloseSession>(
            [&](TEvQuota::TEvProxyCloseSession::TPtr&) {
                ++proxyCloseSessions;
            });

        auto requestQuota = [&] {
            const ui64 cookie = requestCookie++;
            runtime->Send(new IEventHandle(
                MakeQuoterServiceID(),
                sender,
                new TEvQuota::TEvRequest(TEvQuota::EResourceOperator::And, {resLeaf}, TDuration::Max()),
                0,
                cookie));
            runtime->AdvanceCurrentTime(TDuration::Seconds(10));

            auto event = runtime->GrabEdgeEventIf<TEvQuota::TEvClearance>(
                {sender},
                [cookie](const auto& ev) {
                    return ev->Cookie == cookie;
                });
            UNIT_ASSERT_VALUES_EQUAL(event->Get()->Result, TEvQuota::TEvClearance::EResult::Success);
        };

        requestQuota();
        UNIT_ASSERT_VALUES_EQUAL(proxySessions, 1);
        UNIT_ASSERT_VALUES_EQUAL(proxyCloseSessions, 0);

        runtime->AdvanceCurrentTime(TDuration::Hours(1) + TDuration::Minutes(2));
        dispatchEvents();
        UNIT_ASSERT_VALUES_EQUAL(proxySessions, 1);
        UNIT_ASSERT_VALUES_EQUAL(proxyCloseSessions, 1);

        requestQuota();
        UNIT_ASSERT_VALUES_EQUAL(proxySessions, 2);
        UNIT_ASSERT_VALUES_EQUAL(proxyCloseSessions, 1);
    }
}

}
