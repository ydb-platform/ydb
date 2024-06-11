#include <ydb/core/util/testactorsys.h>
#include <ydb/core/base/blobstorage_common.h>
#include "skeleton_front_mock.h"
#include "loader.h"

using namespace NKikimr;

Y_UNIT_TEST_SUITE(Backpressure) {

    Y_UNIT_TEST(MonteCarlo) {
        TTestActorSystem runtime(1);
        runtime.Start();

        const TVDiskID vdiskId(TGroupId::Zero(), 1, 0, 0, 0);
        TActorId vdiskActorId = runtime.Register(new TSkeletonFrontMockActor, TActorId(), 0, std::nullopt, 1);

        std::vector<TActorId> clients;
        ui64 clientId = 1;

        const TInstant simDuration = TInstant::Hours(6);
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

}
