#include "blobstorage_skeletonfront.h"
#include "skeleton_events.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_params.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_config.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
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
            ui32(128 << 20),
            ui32(4 << 10),
            ui64(1'000),
            ui64(100 << 20),
            ui64(100 << 20),
            ui64(4 << 10),
            ui64(4 << 10),
            ui64(4 << 10),
            NPDisk::DEVICE_TYPE_ROT);
    }

    TIntrusivePtr<TVDiskConfig> MakeConfig() {
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
        config->SkeletonFrontHugePuts_MaxInFlightCount = 0;
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

    NMonitoring::TDynamicCounters::TCounterPtr FindCounter(
            const TIntrusivePtr<NMonitoring::TDynamicCounters>& counters,
            const TString& name) {
        auto counter = counters->FindCounter(name);
        UNIT_ASSERT_C(counter, "missing counter " << name);
        return counter;
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

    TIntrusivePtr<NMonitoring::TDynamicCounters> GetPutUserDataLatencyGroup(
            const TIntrusivePtr<NMonitoring::TDynamicCounters>& counters,
            const TIntrusivePtr<TVDiskConfig>& config,
            const TIntrusivePtr<TBlobStorageGroupInfo>& info) {
        auto group = GetServiceCounters(counters, "vdisks");
        group = FindSubgroup(group, "storagePool", config->BaseInfo.StoragePoolName);
        group = FindSubgroup(group, "group", Sprintf("%09" PRIu32, info->GroupID.GetRawId()));
        group = FindSubgroup(group, "orderNumber", Sprintf("%02" PRIu32, info->GetOrderNumber(config->BaseInfo.VDiskIdShort)));
        group = FindSubgroup(group, "pdisk", Sprintf("%09" PRIu32, config->BaseInfo.PDiskId));
        group = FindSubgroup(group, "media", "rot");
        group = FindSubgroup(group, "handleclass", "PutUserData");
        return FindSubgroup(group, "subsystem", "latency_histo");
    }

    TIntrusivePtr<NMonitoring::TDynamicCounters> GetSkeletonFrontGroup(
            const TIntrusivePtr<NMonitoring::TDynamicCounters>& counters,
            const TIntrusivePtr<TVDiskConfig>& config,
            const TIntrusivePtr<TBlobStorageGroupInfo>& info) {
        auto group = GetServiceCounters(counters, "vdisks");
        group = FindSubgroup(group, "storagePool", config->BaseInfo.StoragePoolName);
        group = FindSubgroup(group, "group", Sprintf("%09" PRIu32, info->GroupID.GetRawId()));
        group = FindSubgroup(group, "orderNumber", Sprintf("%02" PRIu32, info->GetOrderNumber(config->BaseInfo.VDiskIdShort)));
        group = FindSubgroup(group, "pdisk", Sprintf("%09" PRIu32, config->BaseInfo.PDiskId));
        group = FindSubgroup(group, "media", "rot");
        return FindSubgroup(group, "subsystem", "skeletonfront");
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

    void UpdateStats(TTestBasicRuntime& runtime, const TActorId& skeletonFrontId, const TActorId& edgeActor) {
        SendToSkeletonFront(
            runtime,
            skeletonFrontId,
            edgeActor,
            new TEvTimeToUpdateStats,
            TEvBlobStorage::EvTimeToUpdateStats);
    }

} // namespace

Y_UNIT_TEST_SUITE(TSkeletonFrontLatency) {

    Y_UNIT_TEST(DroppedDelayedPutUserDataRemovesInFlightLatency) {
        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        auto groupInfo = MakeIntrusive<TBlobStorageGroupInfo>(
            TBlobStorageGroupType(TErasureType::ErasureMirror3),
            ui32(2),
            ui32(4));
        auto config = MakeConfig();

        TTestBasicRuntime runtime(NodeId, false);
        runtime.SetScheduledEventsSelectorFunc(&TTestActorRuntimeBase::DroppingScheduledEventsSelector);

        TAppPrepare app;
        app.ClearDomainsAndHive();
        runtime.Initialize(app.Unwrap());

        const TActorId edgeActor = runtime.AllocateEdgeActor(NodeId - 1);
        const TActorId skeletonFrontId = runtime.Register(
            CreateVDiskSkeletonFront(config, groupInfo, counters),
            NodeId - 1);
        DispatchBootstrap(runtime, skeletonFrontId);
        UNIT_ASSERT_C(runtime.FindActor(skeletonFrontId, NodeId - 1), "SkeletonFront died during bootstrap");

        SendRecoveryStatus(runtime, skeletonFrontId, edgeActor, TEvFrontRecoveryStatus::LocalRecoveryDone);
        SendRecoveryStatus(runtime, skeletonFrontId, edgeActor, TEvFrontRecoveryStatus::SyncGuidRecoveryDone);

        SendToSkeletonFront(
            runtime,
            skeletonFrontId,
            edgeActor,
            new TEvBlobStorage::TEvVCheckReadiness(false),
            TEvBlobStorage::EvVCheckReadiness);
        auto readiness = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvVCheckReadinessResult>(
            edgeActor,
            TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(readiness->Get()->Record.GetStatus(), NKikimrProto::OK);

        const TString data(MinHugeBlobInBytes, 'x');
        const TLogoBlobID blobId(1, 1, 1, 0, data.size(), 0);
        auto put = std::make_unique<TEvBlobStorage::TEvVPut>(
            blobId,
            TRope(data),
            groupInfo->GetVDiskId(config->BaseInfo.VDiskIdShort),
            false,
            nullptr,
            TInstant::Max(),
            NKikimrBlobStorage::UserData);
        UNIT_ASSERT_VALUES_EQUAL(put->Record.GetHandleClass(), NKikimrBlobStorage::UserData);
        SendToSkeletonFront(
            runtime,
            skeletonFrontId,
            edgeActor,
            put.release(),
            TEvBlobStorage::EvVPut);

        auto latencyGroup = GetPutUserDataLatencyGroup(counters, config, groupInfo);
        auto inFlightCount = FindCounter(latencyGroup, "InFlightCount");
        auto inFlightLatencyUsSum = FindCounter(latencyGroup, "InFlightLatencyUsSum");
        auto skeletonFrontGroup = GetSkeletonFrontGroup(counters, config, groupInfo);
        auto hugePutsForegroundDelayedCount = FindCounter(skeletonFrontGroup, "SkeletonFront/HugePutsForeground/DelayedCount");
        auto hugePutsForegroundInFlightCount = FindCounter(skeletonFrontGroup, "SkeletonFront/HugePutsForeground/InFlightCount");

        runtime.AdvanceCurrentTime(TDuration::MilliSeconds(10));
        UpdateStats(runtime, skeletonFrontId, edgeActor);
        UNIT_ASSERT_VALUES_EQUAL(hugePutsForegroundDelayedCount->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(hugePutsForegroundInFlightCount->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL_C(inFlightCount->Val(), 1,
            "delayed# " << hugePutsForegroundDelayedCount->Val()
            << " queueInFlight# " << hugePutsForegroundInFlightCount->Val());
        UNIT_ASSERT_GT(inFlightLatencyUsSum->Val(), 0);

        SendToSkeletonFront(
            runtime,
            skeletonFrontId,
            edgeActor,
            new TEvPDiskErrorStateChange(NKikimrProto::CORRUPTED, 0, "test error"),
            TEvBlobStorage::EvPDiskErrorStateChange);

        auto result = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvVPutResult>(edgeActor, TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetStatus(), NKikimrProto::ERROR);

        // TQueueInplace::Pop() keeps the popped slot alive, so the forceError path has
        // to reset the guard explicitly before the next counter snapshot is published.
        UpdateStats(runtime, skeletonFrontId, edgeActor);
        UNIT_ASSERT_VALUES_EQUAL(inFlightCount->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(inFlightLatencyUsSum->Val(), 0);
    }

} // Y_UNIT_TEST_SUITE(TSkeletonFrontLatency)

} // namespace NKikimr
