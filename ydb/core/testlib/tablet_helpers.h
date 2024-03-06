#pragma once
#include "defs.h"

#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/basics/helpers.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/hive.h>
#include <ydb/core/base/storage_pools.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_factory.h>
#include <ydb/core/mind/hive/domain_info.h>
#include <ydb/core/protos/tablet_database.pb.h>
#include <ydb/core/tx/tx.h>

#include <functional>

namespace NKikimr {
    struct TAppPrepare;

    const TBlobStorageGroupType::EErasureSpecies DataGroupErasure = TBlobStorageGroupType::ErasureNone;

    TActorId FollowerTablet(TTestActorRuntime &runtime, const TActorId &launcher, TTabletStorageInfo *info,
        std::function<IActor* (const TActorId &, TTabletStorageInfo*)> op);
    TActorId ResolveTablet(TTestActorRuntime& runtime, ui64 tabletId, ui32 nodeIndex = 0, bool sysTablet = false);
    void ForwardToTablet(TTestActorRuntime& runtime, ui64 tabletId, const TActorId& sender, IEventBase *ev, ui32 nodeIndex = 0, bool sysTablet = false);
    void InvalidateTabletResolverCache(TTestActorRuntime& runtime, ui64 tabletId, ui32 nodeIndex = 0);
    void RebootTablet(TTestActorRuntime& runtime, ui64 tabletId, const TActorId& sender, ui32 nodeIndex = 0, bool sysTablet = false);
    void GracefulRestartTablet(TTestActorRuntime& runtime, ui64 tabletId, const TActorId& sender, ui32 nodeIndex = 0);
    void SetupTabletServices(TTestActorRuntime& runtime, TAppPrepare* app = nullptr, bool mockDisk = false,
                             NFake::TStorage storage = {}, NFake::TCaches caches = {}, bool forceFollowers = false);

    const TString DEFAULT_STORAGE_POOL = "Storage Pool with id: 1";

    static TChannelBind GetDefaultChannelBind(const TString& storagePool = DEFAULT_STORAGE_POOL) {
        TChannelBind bind;
        bind.SetStoragePoolName(storagePool);
        return bind;
    }

    const TChannelsBindings DEFAULT_BINDED_CHANNELS = {GetDefaultChannelBind(), GetDefaultChannelBind(), GetDefaultChannelBind()};
    void SetupBoxAndStoragePool(TTestActorRuntime &runtime, const TActorId& sender, ui32 nGroups = 1);
    inline void SetupBoxAndStoragePool(TTestActorRuntime &runtime, const TActorId& sender, ui32, ui32 nGroups) {
        SetupBoxAndStoragePool(runtime, sender, nGroups);
    }
    void SetupChannelProfiles(TAppPrepare &app, ui32 nchannels = 3);
    inline void SetupChannelProfiles(TAppPrepare &app, ui32, ui32 nchannels) {
        SetupChannelProfiles(app, nchannels);
    }
    TDomainsInfo::TDomain::TStoragePoolKinds DefaultPoolKinds(ui32 count = 1);

    i64 SetSplitMergePartCountLimit(TTestActorRuntime* runtime, i64 val);
    bool SetAllowServerlessStorageBilling(TTestActorRuntime* runtime, bool isAllow);

    const TString INITIAL_TEST_DISPATCH_NAME = "Trace";

    void RunTestWithReboots(const TVector<ui64>& tabletIds, std::function<TTestActorRuntime::TEventFilter()> filterFactory,
        std::function<void(const TString& dispatchPass, std::function<void(TTestActorRuntime&)> setup, bool& activeZone)> testFunc,
        ui32 selectedReboot = Max<ui32>(), ui64 selectedTablet = Max<ui64>(), ui32 bucket = 0, ui32 totalBuckets = 0, bool killOnCommit = false);

    // Resets pipe when receiving client events
    void RunTestWithPipeResets(const TVector<ui64>& tabletIds, std::function<TTestActorRuntime::TEventFilter()> filterFactory,
        std::function<void(const TString& dispatchPass, std::function<void(TTestActorRuntime&)> setup, bool& activeZone)> testFunc,
        ui32 selectedReboot = Max<ui32>(), ui32 bucket = 0, ui32 totalBuckets = 0);

    struct TRunWithDelaysConfig {
        double DelayInjectionProbability;
        TDuration ReschedulingDelay;
        ui32 VariantsLimit;

        TRunWithDelaysConfig()
            : DelayInjectionProbability(0.2)
            , ReschedulingDelay(TDuration::MilliSeconds(300))
            , VariantsLimit(50)
        {}
    };

    void RunTestWithDelays(const TRunWithDelaysConfig& config, const TVector<ui64>& tabletIds,
        std::function<void(const TString& dispatchPass, std::function<void(TTestActorRuntime&)> setup, bool& activeZone)> testFunc);

    class ITabletScheduledEventsGuard {
    public:
        virtual ~ITabletScheduledEventsGuard() {}
    };

    TAutoPtr<ITabletScheduledEventsGuard> CreateTabletScheduledEventsGuard(const TVector<ui64>& tabletIds, TTestActorRuntime& runtime, const TActorId& sender);
    ui64 GetFreePDiskSize(TTestActorRuntime& runtime, const TActorId& sender);
    void PrintTabletDb(TTestActorRuntime& runtime, ui64 tabletId, const TActorId& sender);

    NTabletPipe::TClientConfig GetPipeConfigWithRetriesAndFollowers();

    IActor* CreateFlatDummyTablet(const TActorId &tablet, TTabletStorageInfo *info);

    void WaitScheduledEvents(TTestActorRuntime &runtime, TDuration delay, const TActorId &sender, ui32 nodeIndex = 0);


    struct TEvFakeHive {
        enum EEv {
            EvSubscribeToTabletDeletion = TEvHive::EvEnd + 1,
            EvNotifyTabletDeleted,
            EvRequestDomainInfo,
            EvRequestDomainInfoReply
        };

        struct TEvSubscribeToTabletDeletion : public TEventLocal<TEvSubscribeToTabletDeletion, EvSubscribeToTabletDeletion> {
            ui64 TabletId;

            explicit TEvSubscribeToTabletDeletion(ui64 tabletId)
                : TabletId(tabletId)
            {}
        };

        struct TEvNotifyTabletDeleted : public TEventLocal<TEvNotifyTabletDeleted, EvNotifyTabletDeleted> {
            ui64 TabletId;

            explicit TEvNotifyTabletDeleted(ui64 tabletId)
                : TabletId(tabletId)
            {}
        };

        struct TEvRequestDomainInfo : public TEventLocal<TEvRequestDomainInfo, EvRequestDomainInfo> {
            TSubDomainKey DomainKey;

            explicit TEvRequestDomainInfo(TSubDomainKey domainKey)
                : DomainKey(domainKey)
            {}
        };
        
        struct TEvRequestDomainInfoReply: public TEventLocal<TEvRequestDomainInfoReply, EvRequestDomainInfoReply> {
            NHive::TDomainInfo DomainInfo;

            explicit TEvRequestDomainInfoReply(const NHive::TDomainInfo& domainInfo)
                : DomainInfo(domainInfo)
            {}
        };

    };

    struct TFakeHiveTabletInfo {
        const TTabletTypes::EType Type;
        const ui64 TabletId;
        TActorId BootstrapperActorId;

        TChannelsBindings BoundChannels;
        ui32 ChannelsProfile;

        THashSet<TActorId> DeletionWaiters;

        TFakeHiveTabletInfo(TTabletTypes::EType type, ui64 tabletId, TActorId bootstrapperActorId)
            : Type(type)
            , TabletId(tabletId)
            , BootstrapperActorId(bootstrapperActorId)
        {}

        TFakeHiveTabletInfo(const TFakeHiveTabletInfo& info) = default;
    };

    struct TFakeHiveState : TThrRefBase {
        TMap<std::pair<ui64, ui64>, TFakeHiveTabletInfo> Tablets;
        TMap<ui64, std::pair<ui64, ui64>> TabletIdToOwner;
        TMap<ui64, ui64> TabletIdToHive;
        ui64 NextTabletId;
        ui64 NextHiveNextTabletId;
        TMap<TSubDomainKey, NHive::TDomainInfo> Domains;
        static constexpr ui64 TABLETS_PER_CHILD_HIVE = 1000000; // amount of tablet ids we reserve for child hive

        typedef TIntrusivePtr<TFakeHiveState> TPtr;

        TFakeHiveState()
            : NextTabletId(TTestTxConfig::FakeHiveTablets)
            , NextHiveNextTabletId(NextTabletId + TABLETS_PER_CHILD_HIVE)
        {}

        TPtr AllocateSubHive() {
            if (NextHiveNextTabletId == 0) {
                return nullptr;
            }
            TPtr state = new TFakeHiveState();
            state->NextTabletId = NextHiveNextTabletId;
            state->NextHiveNextTabletId = 0;
            ui64 hiveId = NextTabletId;
            TabletIdToHive[NextHiveNextTabletId] = hiveId;
            NextHiveNextTabletId += TABLETS_PER_CHILD_HIVE;
            return state;
        }

        ui64 AllocateTabletId() {
            return NextTabletId++;
        }
    };

    typedef std::function<std::function<IActor* (const TActorId &, TTabletStorageInfo*)>(ui32 type)> TGetTabletCreationFunc;

    void BootFakeHive(TTestActorRuntime& runtime, ui64 tabletId, TFakeHiveState::TPtr state,
                      TGetTabletCreationFunc getTabletCreationFunc = nullptr);

}
