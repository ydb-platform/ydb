#include <ydb/core/util/actorsys_test/testactorsys.h>
#include <ydb/core/base/blobstorage_common.h>
#include "skeleton_front_mock.h"
#include "loader.h"

using namespace NKikimr;

Y_UNIT_TEST_SUITE(Backpressure) {

    Y_UNIT_TEST(FailedRealmConnectionLifecycle) {
        using R = NKikimrBlobStorage::TGroupInfo::TFailRealm;
        for (const auto [clientType, donor] : {
                std::pair{NBackpressure::EQueueClientType::DSProxy, false},
                std::pair{NBackpressure::EQueueClientType::ReplJob, false},
                std::pair{NBackpressure::EQueueClientType::ReplJob, true}}) {
            TTestActorSystem runtime(1);
            runtime.Start();
            const auto remote = runtime.Register(new TSkeletonFrontMockActor, 1);
            const auto parent = runtime.AllocateEdgeActor(1);
            const TVDiskIdShort target(1, 0, 0);
            auto makeInfo = [&](ui32 generation, R::EState state) {
                TBlobStorageGroupInfo::TDynamicInfo dynamic(TGroupId::Zero(), generation);
                dynamic.FailRealmStates = {R::N0, state, R::N0};
                for (ui32 i = 0; i < 9; ++i) {
                    dynamic.PushBackActorId(remote);
                }
                return MakeIntrusive<TBlobStorageGroupInfo>(TBlobStorageGroupInfo::TTopology(
                    TBlobStorageGroupType::ErasureMirror3dc, 3, 3, 1), std::move(dynamic), "");
            };
            ui32 readinessRequests = 0;
            ui64 lastReadinessCookie = 0;
            runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
                if (ev->Recipient == parent && ev->GetTypeRewrite() == TEvProxyQueueState::EventType) {
                    return false; // Connection state is queried explicitly through separate edge actors below.
                }
                if (ev->Recipient == remote && ev->GetTypeRewrite() == TEvBlobStorage::TEvVCheckReadiness::EventType) {
                    ++readinessRequests;
                    lastReadinessCookie = ev->Cookie;
                }
                return true;
            };
            auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
            auto context = MakeIntrusive<TBSProxyContext>(counters);
            auto flow = MakeIntrusive<NBackpressure::TFlowRecord>();
            const auto queue = runtime.Register(CreateVDiskBackpressureClient(makeInfo(1, R::F), target,
                NKikimrBlobStorage::PutTabletLog, counters, context, NBackpressure::TQueueClientId(clientType, 1),
                "failed-realm", 0, true, TDuration::Minutes(1), flow,
                NMonitoring::TCountableBase::EVisibility::Public), parent, 0, std::nullopt, 1);
            auto advance = [&] {
                const auto barrier = runtime.GetClock() + TDuration::Seconds(20);
                runtime.Schedule(barrier, nullptr, new TFakeSchedulerCookie, 1);
                runtime.Sim([&] { return runtime.GetClock() < barrier; });
            };
            auto connected = [&] {
                const auto probe = runtime.AllocateEdgeActor(1);
                runtime.Send(new IEventHandle(queue, probe, new TEvRequestProxyQueueState), 1);
                return runtime.WaitForEdgeActorEvent<TEvProxyQueueState>(probe)->Get()->IsConnected;
            };
            auto configure = [&](ui32 generation, R::EState state) {
                auto info = makeInfo(donor ? 1 : generation, state);
                runtime.Send(new IEventHandle(queue, parent,
                    new TEvVGenerationChange(info->CreateVDiskID(target), info, generation)), 1);
            };

            advance();
            UNIT_ASSERT(!connected());
            UNIT_ASSERT_VALUES_EQUAL(readinessRequests, 0);
            const auto probe = runtime.AllocateEdgeActor(1);
            runtime.Send(new IEventHandle(queue, probe, new TEvBlobStorage::TEvVStatus(makeInfo(1, R::F)->CreateVDiskID(target))), 1);
            UNIT_ASSERT_VALUES_EQUAL(runtime.WaitForEdgeActorEvent<TEvBlobStorage::TEvVStatusResult>(probe)
                ->Get()->Record.GetStatus(), NKikimrProto::NOTREADY);

            configure(2, R::R);
            advance();
            UNIT_ASSERT(connected());
            UNIT_ASSERT(readinessRequests);
            const auto beforeFailure = readinessRequests;

            configure(3, R::F);
            UNIT_ASSERT(!connected());
            // Neither a stale handshake response nor a scheduled reconnect may reopen F.
            runtime.Send(new IEventHandle(queue, remote,
                new TEvBlobStorage::TEvVCheckReadinessResult(NKikimrProto::OK, false), 0, lastReadinessCookie), 1);
            runtime.Send(new IEventHandle(queue, remote, new TEvBlobStorage::TEvVReadyNotify), 1);
            configure(2, R::N0); // stale group generation
            advance();
            UNIT_ASSERT(!connected());
            UNIT_ASSERT_VALUES_EQUAL(readinessRequests, beforeFailure);

            configure(4, R::N0);
            advance();
            UNIT_ASSERT(connected());
            UNIT_ASSERT(readinessRequests > beforeFailure);
            runtime.Send(new IEventHandle(TEvents::TSystem::Poison, 0, queue, {}, {}, 0), 1);
            runtime.Stop();
        }
    }

    Y_UNIT_TEST(MonteCarlo) {
        TTestActorSystem runtime(1);
        runtime.Start();

        const TVDiskID vdiskId(TGroupId::Zero(), 1, 0, 0, 0);
        TActorId vdiskActorId = runtime.Register(new TSkeletonFrontMockActor, TActorId(), 0, std::nullopt, 1);

        std::vector<TActorId> clients;
        ui64 clientId = 1;

        const TInstant simDuration = TInstant::Hours(1);
        const TDuration minQ = TDuration::Seconds(10);
        const TDuration maxQ = TDuration::Seconds(20);

        TInstant start = TInstant::Now();

        while (runtime.GetClock() < simDuration) {
            Cerr << "Clock# " << runtime.GetClock()
                << " elapsed# " << (TInstant::Now() - start)
                << " EventsProcessed# " << runtime.GetEventsProcessed()
                << " clients.size# " << clients.size()
                << Endl;

            // TODO(alexvru): pause, resume, network disconnect, vdisk restart
            enum EActivity : ui32 {
                NOOP,
                ADD_CLIENT,
                TERM_CLIENT,
                COUNT // the last entry
            };
            std::array<ui32, EActivity::COUNT> weights;
            weights.fill(0);
            weights[NOOP] = 1000;
            weights[ADD_CLIENT] = clients.size() < 10 ? 100 : 0;
            weights[TERM_CLIENT] = clients.empty() ? 0 : 90;
            ui32 accum = 0;
            for (ui32& w : weights) {
                w = std::exchange(accum, accum + w);
            }
            const auto selector = RandomNumber(accum);
            const auto activity = std::upper_bound(weights.begin(), weights.end(), selector) - weights.begin() - 1;
            switch (activity) {
                case NOOP:
                    break;

                case ADD_CLIENT: {
                    const NBackpressure::TQueueClientId id(NBackpressure::EQueueClientType::VDiskLoad, clientId++);
                    const TActorId actorId = runtime.Register(new TLoaderActor(id, vdiskId, vdiskActorId), TActorId(),
                        0, std::nullopt, 1);
                    clients.push_back(actorId);
                    break;
                }

                case TERM_CLIENT: {
                    const size_t index = RandomNumber(clients.size());
                    runtime.Send(new IEventHandle(TEvents::TSystem::Poison, 0, clients[index], {}, {}, 0), 1);
                    clients.erase(clients.begin() + index);
                    break;
                }

                default:
                    Y_ABORT();
            }

            const TDuration quantum = TDuration::FromValue(minQ.GetValue() + RandomNumber(maxQ.GetValue() - minQ.GetValue() + 1));
            const TInstant barrier = Min(simDuration, runtime.GetClock() + quantum);
            runtime.Schedule(barrier, nullptr, new TFakeSchedulerCookie, 1);
            runtime.Sim([&] { return runtime.GetClock() < barrier; });
        }

        // terminate loader actors and the disk
        for (const TActorId& actorId : std::exchange(clients, {})) {
            runtime.Send(new IEventHandle(TEvents::TSystem::Poison, 0, actorId, {}, {}, 0), 1);
        }
        runtime.Send(new IEventHandle(TEvents::TSystem::Poison, 0, vdiskActorId, {}, {}, 0), 1);

        runtime.Stop();
    }

    Y_UNIT_TEST(VDiskTryLaterOverload) {
        TTestActorSystem runtime(1);
        runtime.Start();

        const TVDiskID vdiskId(TGroupId::Zero(), 1, 0, 0, 0);
        TActorId vdiskActorId = runtime.Register(new TSkeletonFrontMockActor, TActorId(), 0, std::nullopt, 1);

        const TInstant simDuration = TInstant::Minutes(10);

        TInstant start = TInstant::Now();

        std::vector<TActorId> clients;
        for (ui32 i = 0; i < 10; ++i) {
            const NBackpressure::TQueueClientId id(NBackpressure::EQueueClientType::VDiskLoad, i);
            TActorId actorId = runtime.Register(new TLoaderActor(id, vdiskId, vdiskActorId, 8 << 20), TActorId(),
                0, std::nullopt, 1);
            clients.push_back(actorId);
        }

        while (runtime.GetClock() < simDuration) {
            Cerr << "Clock# " << runtime.GetClock()
                << " elapsed# " << (TInstant::Now() - start)
                << " EventsProcessed# " << runtime.GetEventsProcessed()
                << Endl;

            const TDuration quantum = TDuration::Seconds(1);
            const TInstant barrier = Min(simDuration, runtime.GetClock() + quantum);
            runtime.Schedule(barrier, nullptr, new TFakeSchedulerCookie, 1);
            runtime.Sim([&] { return runtime.GetClock() < barrier; });
        }

        for (const TActorId& actorId : std::exchange(clients, {})) {
            runtime.Send(new IEventHandle(TEvents::TSystem::Poison, 0, actorId, {}, {}, 0), 1);
        }
        runtime.Send(new IEventHandle(TEvents::TSystem::Poison, 0, vdiskActorId, {}, {}, 0), 1);

        runtime.Stop();
    }
}
