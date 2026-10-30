#include "blobstorage_skeletonfront.h"
#include "skeleton_events.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_params.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_config.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_histograms.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_pdisk_error.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
    namespace {

        constexpr ui32 NodeId = 1;
        constexpr ui32 PDiskId = 1;
        constexpr ui32 VDiskSlotId = 0;
        constexpr ui64 PDiskGuid = 1;
        constexpr ui64 OwnerRound = 1;
        constexpr ui32 MinHugeBlobInBytes = 64 << 10;
        const TString StoragePoolName = "test_storage_pool";

        TIntrusivePtr<TPDiskParams> MakePDiskParams() {
            return MakeIntrusive<TPDiskParams>(
                NPDisk::TOwner(0),
                OwnerRound,
                ui32(0),
                ui32(128 << 20),
                ui32(4 << 10),
                ui64(1'000),
                ui64(100 << 20),
                ui64(100 << 20),
                ui64(4 << 10),
                ui64(4 << 10),
                ui64(4 << 10),
                NPDisk::DEVICE_TYPE_ROT,
                false,
                ui32(4 << 10));
        }

        TIntrusivePtr<TVDiskConfig> MakeConfig(ui64 maxHugePutsInFlight = 0) {
            TVDiskIdShort vdiskId(0, 0, 0);
            TVDiskConfig::TBaseInfo baseInfo(
                vdiskId,
                MakeBlobStoragePDiskID(NodeId, PDiskId),
                PDiskGuid,
                PDiskId,
                NPDisk::DEVICE_TYPE_ROT,
                VDiskSlotId,
                NKikimrBlobStorage::TVDiskKind::Default,
                OwnerRound,
                StoragePoolName);

            auto config = MakeIntrusive<TVDiskConfig>(baseInfo);
            config->SkeletonFrontHugePuts_MaxInFlightCount = maxHugePutsInFlight;
            config->SkeletonFrontExtPutUserData_TotalCost = 1'000'000'000'000ull;
            config->SkeletonFrontQueueBackpressureCheckMsgId = false;
            config->StatsUpdateInterval = TDuration::Days(1);
            config->RunRepl = false;
            return config;
        }

        TIntrusivePtr<NMonitoring::TDynamicCounters> FindSubgroup(
            const TIntrusivePtr<NMonitoring::TDynamicCounters>& counters,
            const TString& name,
            const TString& value) {
            auto subgroup = counters->FindSubgroup(name, value);
            UNIT_ASSERT_C(subgroup, "missing subgroup " << name << "=" << value);
            return subgroup;
        }

        void DispatchUntil(TTestBasicRuntime& runtime, const TActorId& actorId, ui32 eventType) {
            TDispatchOptions options;
            options.OnlyMailboxes.emplace_back(actorId.NodeId(), actorId.Hint());
            options.FinalEvents.emplace_back(eventType);
            runtime.DispatchEvents(options, TDuration::Seconds(1));
        }

        void DispatchBootstrap(TTestBasicRuntime& runtime, const TActorId& skeletonFrontId) {
            DispatchUntil(runtime, skeletonFrontId, TEvents::TSystem::Bootstrap);
        }

        void SendToSkeletonFront(
            TTestBasicRuntime& runtime,
            const TActorId& skeletonFrontId,
            const TActorId& edgeActor,
            IEventBase* event,
            ui32 eventType) {
            const ui64 before = runtime.GetCounter(eventType);
            runtime.Send(new IEventHandle(skeletonFrontId, edgeActor, event));
            if (runtime.GetCounter(eventType) == before) {
                DispatchUntil(runtime, skeletonFrontId, eventType);
            }
        }

        TIntrusivePtr<NMonitoring::TDynamicCounters> GetLatencyGroup(
            const TIntrusivePtr<NMonitoring::TDynamicCounters>& counters,
            const TIntrusivePtr<TVDiskConfig>& config,
            const TIntrusivePtr<TBlobStorageGroupInfo>& info,
            const TString& handleClass) {
            const bool async = NVDiskMon::THistograms::IsAsyncHandleClass(handleClass);
            auto group = GetServiceCounters(counters, async ? "vdisks_async" : "vdisks");
            group = FindSubgroup(group, "storagePool", config->BaseInfo.StoragePoolName);
            group = FindSubgroup(group, "group", Sprintf("%09" PRIu32, info->GroupID.GetRawId()));
            group = FindSubgroup(group, "orderNumber", Sprintf("%02" PRIu32, info->GetOrderNumber(config->BaseInfo.VDiskIdShort)));
            group = FindSubgroup(group, "pdisk", Sprintf("%09" PRIu32, config->BaseInfo.PDiskId));
            group = FindSubgroup(group, "media", "rot");
            group = FindSubgroup(group, "handleclass", handleClass);
            return FindSubgroup(group, "subsystem", "latency_histo");
        }

        void SendRecoveryStatus(
            TTestBasicRuntime& runtime,
            const TActorId& skeletonFrontId,
            const TActorId& edgeActor,
            TEvFrontRecoveryStatus::EPhase phase) {
            SendToSkeletonFront(
                runtime,
                skeletonFrontId,
                edgeActor,
                new TEvFrontRecoveryStatus(
                    phase,
                    NKikimrProto::OK,
                    MakePDiskParams(),
                    MinHugeBlobInBytes,
                    TVDiskIncarnationGuid(1)),
                TEvBlobStorage::EvFrontRecoveryStatus);
        }

        struct TTestEnv {
            TIntrusivePtr<NMonitoring::TDynamicCounters> Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
            TIntrusivePtr<TBlobStorageGroupInfo> GroupInfo = MakeIntrusive<TBlobStorageGroupInfo>(
                TBlobStorageGroupType(TErasureType::ErasureNone));
            TIntrusivePtr<TVDiskConfig> Config;
            TTestBasicRuntime Runtime;
            TActorId EdgeActor;
            TActorId SkeletonFrontId;
            TActorId SkeletonId;

            explicit TTestEnv(ui64 maxHugePutsInFlight)
                : Config(MakeConfig(maxHugePutsInFlight))
                , Runtime(NodeId, false)
            {
                Runtime.SetScheduledEventsSelectorFunc(&TTestActorRuntimeBase::DroppingScheduledEventsSelector);

                TAppPrepare app;
                app.ClearDomainsAndHive();
                Runtime.Initialize(app.Unwrap());

                EdgeActor = Runtime.AllocateEdgeActor(NodeId - 1);
                SkeletonFrontId = Runtime.Register(
                    CreateVDiskSkeletonFront(Config, GroupInfo, Counters),
                    NodeId - 1);
                Runtime.SetRegistrationObserverFunc([this](
                                                        TTestActorRuntimeBase&,
                                                        const TActorId& parentId,
                                                        const TActorId& actorId) {
                    if (parentId == SkeletonFrontId) {
                        SkeletonId = actorId;
                    }
                });
                DispatchBootstrap(Runtime, SkeletonFrontId);
                UNIT_ASSERT_C(Runtime.FindActor(SkeletonFrontId, NodeId - 1), "SkeletonFront died during bootstrap");
                UNIT_ASSERT_C(SkeletonId, "Skeleton actor was not registered during bootstrap");

                SendRecoveryStatus(Runtime, SkeletonFrontId, EdgeActor, TEvFrontRecoveryStatus::LocalRecoveryDone);
                SendRecoveryStatus(Runtime, SkeletonFrontId, EdgeActor, TEvFrontRecoveryStatus::SyncGuidRecoveryDone);

                SendToSkeletonFront(
                    Runtime,
                    SkeletonFrontId,
                    EdgeActor,
                    new TEvBlobStorage::TEvVCheckReadiness(false),
                    TEvBlobStorage::EvVCheckReadiness);
                auto readiness = Runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvVCheckReadinessResult>(
                    EdgeActor,
                    TDuration::Seconds(1));
                UNIT_ASSERT_VALUES_EQUAL(readiness->Get()->Record.GetStatus(), NKikimrProto::OK);
            }

        };

    } // namespace

    Y_UNIT_TEST_SUITE(TSkeletonFrontLatency) {

        Y_UNIT_TEST(AsyncCountersAreRemovedWhenSkeletonFrontStops) {
            TTestEnv env(0);
            for (const TString& handleClass : {"GetAsync", "GetDiscover", "GetLow", "PutAsyncBlob",
                    "GetFast", "PutTabletLog", "PutUserData"}) {
                UNIT_ASSERT(GetLatencyGroup(env.Counters, env.Config, env.GroupInfo, handleClass));
            }
            auto counters = env.Counters->FindSubgroup("counters", "vdisks");
            auto asyncCounters = env.Counters->FindSubgroup("counters", "vdisks_async");
            UNIT_ASSERT(counters);
            UNIT_ASSERT(asyncCounters);
            SendToSkeletonFront(env.Runtime, env.SkeletonFrontId, env.EdgeActor,
                new TEvents::TEvPoisonPill(), TEvents::TSystem::PoisonPill);
            UNIT_ASSERT(!env.Runtime.FindActor(env.SkeletonFrontId, NodeId - 1));
            UNIT_ASSERT(!counters->FindSubgroup("storagePool", StoragePoolName));
            UNIT_ASSERT(!asyncCounters->FindSubgroup("storagePool", StoragePoolName));
        }

    } // Y_UNIT_TEST_SUITE(TSkeletonFrontLatency)

} // NKikimr
