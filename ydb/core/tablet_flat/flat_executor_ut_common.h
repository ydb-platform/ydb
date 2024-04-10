#pragma once

#include <ydb/core/tablet_flat/test/libs/exec/runner.h>
#include <ydb/core/tablet_flat/test/libs/exec/world.h>
#include <ydb/core/tablet_flat/test/libs/exec/dummy.h>
#include <ydb/core/tablet_flat/ut/flat_database_ut_common.h>
#include <ydb/core/tablet_flat/flat_executor_compaction_logic.h>
#include <ydb/core/tablet/tablet_impl.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <library/cpp/testing/unittest/registar.h>
#include "tablet_flat_executed.h"
#include "flat_executor.h"

namespace NKikimr {
namespace NTabletFlatExecutor {

    struct TMyEnvBase : public NFake::TRunner {

        TMyEnvBase()
            : Edge(Env.AllocateEdgeActor())
        {
            Env.SetLogPriority(NKikimrServices::TABLET_SAUSAGECACHE, NActors::NLog::PRI_INFO);

            if (false) {
                Env.SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_INFO);
                Env.SetLogPriority(NKikimrServices::TABLET_OPS_HOST, NActors::NLog::PRI_INFO);
                Env.SetLogPriority(NKikimrServices::OPS_COMPACT, NActors::NLog::PRI_INFO);
            }
        }

        ~TMyEnvBase()
        {
            using namespace NFake;

            /* Temporary hack until getting rid of edge events in this UTs */

            SendEv(TWorld::Where(EPath::Root), new TEvents::TEvPoison);
            Finalize();
        }

        void FireDummyTablet(ui32 flags = 0)
        {
            FireTablet(Edge, Tablet, [this, &flags](const TActorId &tablet, TTabletStorageInfo *info) {
                return new NFake::TDummy(tablet, info, Edge, flags);
            });

            WaitFor<NFake::TEvReady>();
        }

        void FireDummyFollower(ui32 followerId, bool wait = true)
        {
            FireFollower(Edge, Tablet, [this](const TActorId &tablet, TTabletStorageInfo *info) {
                return new NFake::TDummy(tablet, info, Edge, 0);
            }, followerId);

            if (wait) {
                WaitFor<NFake::TEvReady>();
            }
        }

        void SendSync(IEventBase *event, bool retry = false, bool gone = false)
        {
            const auto wretry = PipeCfgRetries();
            const auto basic = NTabletPipe::TClientConfig();

            Env.SendToPipe(Tablet, Edge, event, 0, retry ? wretry : basic);

            gone ? WaitForGone() : WaitForWakeUp();
        }

        void SendAsync(IEventBase *event)
        {
            Env.SendToPipe(Tablet, Edge, event);
        }

        void SendFollowerSync(IEventBase *event, bool retry = false, bool gone = false)
        {
            SendFollowerAsync(event, retry);

            if (gone) {
                WaitForGone();
            } else {
                WaitForWakeUp();
            }
        }

        void SendFollowerAsync(IEventBase *event, bool retry = false)
        {
            auto config = retry ? PipeCfgRetries() : NTabletPipe::TClientConfig();
            config.AllowFollower = true;
            config.ForceFollower = true;

            Env.SendToPipe(Tablet, Edge, event, 0, config);
        }

        void WaitForWakeUp(size_t num = 1) { WaitFor<TEvents::TEvWakeup>(num); }
        void WaitForGone(size_t num = 1) { WaitFor<TEvents::TEvGone>(num); }

        template<typename TEv>
        typename TEv::TPtr GrabEdgeEvent()
        {
            return Env.GrabEdgeEventRethrow<TEv>(Edge);
        }

        template<typename TEv>
        void WaitFor(size_t num = 1)
        {
            for (; num > 0; num--) {
                TAutoPtr<IEventHandle> handle;
                Env.GrabEdgeEventRethrow<TEv>(handle);
            }
        }

        void SendEv(const TActorId &to, IEventBase *ev)
        {
            Env.Send(new IEventHandle(to, Edge, ev));
        }

        static NTabletPipe::TClientConfig PipeCfgRetries()
        {
            NTabletPipe::TClientConfig pipeConfig;
            pipeConfig.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
            return pipeConfig;
        }

        ui64 Tablet = MakeTabletID(false, 1) & 0xFFFF'FFFF;
        const TActorId Edge;
    };

} // namespace NTabletFlatExecutor
} // namespace NKikimr
