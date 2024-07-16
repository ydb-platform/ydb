#pragma once

#include "config.h"
#include "leader.h"
#include "owner.h"
#include "events.h"
#include "logger.h"
#include "warden.h"
#include "helper.h"

#include <ydb/core/base/statestorage_impl.h>
#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/tablet/resource_broker.h>
#include <ydb/core/tablet_flat/shared_sausagecache.h>
#include <ydb/library/services/services.pb.h>
#include <library/cpp/time_provider/time_provider.h>

#include <ydb/core/tablet_flat/test/libs/rows/tool.h>
#include <ydb/core/testlib/actors/test_runtime.h>

#include <util/generic/xrange.h>

namespace NKikimr {
namespace NFake {

    struct TRunner {
        using ELnLev = NUtil::ELnLev;

        TRunner()
            : Time(TAppData::TimeProvider.Get())
            , NodeId(Env.GetNodeId())
            , Names(MakeComponentsNames())
            , Sink(new TSink(false ? Time->Now() : TInstant::Now(), Names))
            , Logger(new TLogEnv(Time, ELnLev::Info, Sink))
        {
            if (auto logl = Logger->Log(ELnLev::Info)) {
                logl << "Born at "<< TInstant::Now();
            }

            auto *logger = new NFake::TLogFwd(Sink);

            AddService(TActorId(NodeId, "logger"), logger, EMail::Simple);

            SetupStaticServices();

            auto *types = NTable::NTest::DbgRegistry();
            auto *app = new TAppData(0, 0, 0, 0, { }, types, nullptr, nullptr, nullptr);

            Env.Initialize({ app, nullptr, nullptr, {} });
            Env.SetDispatchTimeout(DEFAULT_DISPATCH_TIMEOUT);
            Env.SetLogPriority(NKikimrServices::FAKE_ENV, NActors::NLog::PRI_INFO);

            Leader = Env.Register(new NFake::TLeader(8, Stopped), 0);

            NFake::TConf conf;

            conf.Shared = 8 * (1 << 20);
            conf.ScanQueue = 256 * 1024;
            conf.AsyncQueue = 256 * 1024;

            SetupModelServices(conf);
        }

        TTestActorRuntime* operator->() noexcept
        {
            return &Env;
        }

        void FireTablet(TActorId user, ui32 tablet, TStarter::TMake make, ui32 followerId = 0, TStarter *starter = nullptr)
        {
            const auto mbx =  EMail::Simple;
            TStarter defaultStarter;
            if (starter == nullptr) {
                starter = &defaultStarter;
            }

            RunOn(7, { }, starter->Do(user, 1, tablet, std::move(make), followerId), mbx);
        }

        void FireFollower(TActorId user, ui32 tablet, TStarter::TMake make, ui32 followerId)
        {
            FireTablet(user, tablet, make, followerId);
        }

        void AddService(TActorId service, IActor *actor, EMail box)
        {
            Env.AddLocalService(service, TActorSetupCmd(actor, box, 0), 0);
        }

        void RunTest(TAutoPtr<IActor> actor) noexcept
        {
            return RunOn(8, { }, actor.Release(), EMail::Simple);
        }

        void RunOn(ui32 lvl, TActorId alias, IActor* actor, EMail box)
        {
            auto *event = new NFake::TEvFire{ lvl, alias, { actor, box, 0 } };

            Env.SingleSys()->Send(Leader, event);
        }

        void Finalize()
        {
            /* Test scene should either get off any events in all actors
                queue or send special NFake::TEvTerm in order to terminate.
                On regular flow the EPath::Root actor emits this event but
                on urgent error termination is performed by TLogFwd logger.
             */

            TDispatchOptions options;
            options.FinalEvents.push_back({ NFake::EvTerm });
            Env.SetDispatchTimeout(TDuration::Seconds(15));

            try {
                Env.DispatchEvents(options);
            } catch (TEmptyEventQueueException&) {

            }

            const auto stat = Sink->Stats();
            const bool fail = (stat[0] + stat[1] > 0) || Stopped == 0;

            if (auto logl = Logger->Log(fail ? ELnLev::Emerg : ELnLev::Info)) {
                ui64 other = std::accumulate(&stat[5], stat.end(), ui64(0));

                logl
                    << "Logged {Emerg " << (stat[0] + stat[1])
                    << " Alert " << stat[2] << " Crit " << stat[3]
                    << " Error " << stat[4] << " Left " << other << "}"
                    << ", " << (Stopped ? "stopped" : "dangled");
            }

            UNIT_ASSERT_C(!fail, "Critical events has been logged (see ^^)");
        }

        void SetupStaticServices()
        {
            {
                const auto replica = MakeStateStorageReplicaID(NodeId, 0);

                TIntrusivePtr<TStateStorageInfo> info(new TStateStorageInfo());

                info->NToSelect = 1;
                info->Rings.resize(1);
                info->Rings[0].Replicas.push_back(replica);

                {
                    auto *actor = CreateStateStorageReplica(info, 0);

                    AddService(replica, actor, TMailboxType::Revolving);
                }

                {
                    auto *actor = CreateStateStorageProxy(info, nullptr, nullptr);

                    AddService(MakeStateStorageProxyID(), actor, TMailboxType::Revolving);
                }
            }

            { /*_ Tablet resolver, need for pipes ? */
                auto *actor = CreateTabletResolver(new TTabletResolverConfig);

                AddService(MakeTabletResolverID(), actor, EMail::Revolving);
            }

            { /*_ Resource broker service, used for generic scans */
                using namespace NResourceBroker;

                auto *actor = CreateResourceBrokerActor(MakeDefaultConfig(), Env.GetDynamicCounters());

                AddService(MakeResourceBrokerID(), actor, EMail::Revolving);
            }
        }

        void SetupModelServices(NFake::TConf conf)
        {
            { /*_ Blob storage proxies mock factory */
                auto *actor = new NFake::TWarden(4);

                RunOn(2, MakeBlobStorageNodeWardenID(NodeId), actor, EMail::Simple);
            }

            { /*_ Shared page collection cache service, used by executor */
                auto config = MakeHolder<TSharedPageCacheConfig>();

                config->CacheConfig = new TCacheCacheConfig(conf.Shared, nullptr, nullptr, nullptr);
                config->TotalAsyncQueueInFlyLimit = conf.AsyncQueue;
                config->TotalScanQueueInFlyLimit = conf.ScanQueue;
                config->Counters = MakeIntrusive<TSharedPageCacheCounters>(Env.GetDynamicCounters());

                auto *actor = CreateSharedPageCache(std::move(config));

                RunOn(3, MakeSharedPageCacheId(0), actor, EMail::ReadAsFilled);
            }
        }

        static TVector<TString> MakeComponentsNames() noexcept
        {
            const auto begin = ui32(NKikimrServices::EServiceKikimr_MIN);
            const auto end = ui32(NKikimrServices::EServiceKikimr_MAX) + 1;

            Y_ABORT_UNLESS(end < 8192, "Looks like there is too many services");

            TVector<TString> names(end);

            for (auto num : xrange(begin, end)) {
                auto token = NKikimrServices::EServiceKikimr(num);

                names[num] = NKikimrServices::EServiceKikimr_Name(token);
            }

            return names;
        }

    public:
        TTestActorRuntime Env{ NActors::THeSingleSystemEnv{ } };

        ITimeProvider * const Time = nullptr;
        const ui32 NodeId = Max<ui32>();
        const TVector<TString> Names;   /* { Component -> Name } */
        const TIntrusivePtr<TSink> Sink;
        const TAutoPtr<TLogEnv> Logger;

    private:
        TActorId Leader;
        TAtomic Stopped = 0;
    };

}
}
