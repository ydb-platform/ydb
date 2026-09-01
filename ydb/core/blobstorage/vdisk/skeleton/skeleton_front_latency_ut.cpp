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
        constexpr size_t MaxTrackerSlots = 15;
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

        std::unique_ptr<TEvBlobStorage::TEvVPut> MakeUserDataPut(
            const TLogoBlobID& blobId,
            const TString& data,
            const TVDiskID& vdiskId) {
            auto put = std::make_unique<TEvBlobStorage::TEvVPut>(
                blobId,
                TRope(data),
                vdiskId,
                false,
                nullptr,
                TInstant::Max(),
                NKikimrBlobStorage::UserData);
            UNIT_ASSERT_VALUES_EQUAL(put->Record.GetHandleClass(), NKikimrBlobStorage::UserData);
            return put;
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

            TVDiskID GetVDiskId() const {
                return GroupInfo->GetVDiskId(Config->BaseInfo.VDiskIdShort);
            }
        };

        TAutoPtr<IEventHandle> GrabForwardedVPut(TTestEnv& env) {
            NActors::TEventsList events = env.Runtime.CaptureMailboxEvents(env.SkeletonId.Hint(), env.SkeletonId.NodeId());
            TAutoPtr<IEventHandle> handle;
            auto* put = NActors::GrabEvent<TEvBlobStorage::TEvVPut>(events, handle);
            UNIT_ASSERT_C(put, "missing forwarded TEvVPut");
            env.Runtime.PushMailboxEventsFront(env.SkeletonId.Hint(), env.SkeletonId.NodeId(), events);
            return handle;
        }

        TEvBlobStorage::TEvVPutResult::TPtr CompleteForwardedVPut(
            TTestEnv& env,
            TAutoPtr<IEventHandle> putHandle,
            NKikimrProto::EReplyStatus status = NKikimrProto::OK) {
            auto* put = putHandle->Get<TEvBlobStorage::TEvVPut>();
            auto& record = put->Record;
            const TLogoBlobID blobId = LogoBlobIDFromLogoBlobID(record.GetBlobID());
            const TVDiskID vdiskId = VDiskIDFromVDiskID(record.GetVDiskID());
            const ui32 recByteSize = put->GetCachedByteSize();
            const TVMsgContext msgCtx(recByteSize, record.GetMsgQoS());
            auto result = std::make_unique<TEvBlobStorage::TEvVPutResult>(
                status,
                blobId,
                vdiskId,
                nullptr,
                TOutOfSpaceStatus(0u, 0.0),
                TAppData::TimeProvider->Now(),
                recByteSize,
                &record,
                nullptr,
                nullptr,
                nullptr,
                put->GetBufferBytes(),
                1,
                status == NKikimrProto::OK ? TString() : TString("test error"));
            std::unique_ptr<IEventHandle> resultHandle = std::make_unique<IEventHandle>(
                putHandle->Sender,
                env.SkeletonId,
                result.release());

            SendToSkeletonFront(
                env.Runtime,
                env.SkeletonFrontId,
                env.SkeletonId,
                new TEvVDiskRequestCompleted(msgCtx, std::move(resultHandle)),
                TEvBlobStorage::EvVDiskRequestCompleted);

            auto completed = env.Runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvVPutResult>(
                env.EdgeActor,
                TDuration::Seconds(1));
            UNIT_ASSERT_VALUES_EQUAL(completed->Get()->Record.GetStatus(), status);
            return completed;
        }

    } // namespace

    Y_UNIT_TEST_SUITE(TSkeletonFrontLatency) {

        Y_UNIT_TEST(DroppedDelayedPutUserDataRemovesInFlightLatency) {
            TTestEnv env(0);

            const TString data(MinHugeBlobInBytes, 'x');
            const TLogoBlobID blobId(1, 1, 1, 0, data.size(), 0);
            auto put = MakeUserDataPut(blobId, data, env.GetVDiskId());
            SendToSkeletonFront(
                env.Runtime,
                env.SkeletonFrontId,
                env.EdgeActor,
                put.release(),
                TEvBlobStorage::EvVPut);

            auto latencyGroup = GetPutUserDataLatencyGroup(env.Counters, env.Config, env.GroupInfo);
            auto inFlightCount = FindCounter(latencyGroup, "InFlightCount");
            auto inFlightLatencyUsSum = FindCounter(latencyGroup, "InFlightLatencyUsSum");
            auto maxLatencyUs = FindCounter(latencyGroup, "LatencyUsMax");
            auto skeletonFrontGroup = GetSkeletonFrontGroup(env.Counters, env.Config, env.GroupInfo);
            auto hugePutsForegroundDelayedCount = FindCounter(skeletonFrontGroup, "SkeletonFront/HugePutsForeground/DelayedCount");
            auto hugePutsForegroundInFlightCount = FindCounter(skeletonFrontGroup, "SkeletonFront/HugePutsForeground/InFlightCount");

            env.Runtime.AdvanceCurrentTime(TDuration::MilliSeconds(10));
            UpdateStats(env.Runtime, env.SkeletonFrontId, env.EdgeActor);
            UNIT_ASSERT_VALUES_EQUAL(hugePutsForegroundDelayedCount->Val(), 1);
            UNIT_ASSERT_VALUES_EQUAL(hugePutsForegroundInFlightCount->Val(), 0);
            UNIT_ASSERT_VALUES_EQUAL_C(inFlightCount->Val(), 1,
                                       "delayed# " << hugePutsForegroundDelayedCount->Val()
                                                   << " queueInFlight# " << hugePutsForegroundInFlightCount->Val());
            UNIT_ASSERT_GT(inFlightLatencyUsSum->Val(), 0);
            UNIT_ASSERT_VALUES_EQUAL(maxLatencyUs->Val(), inFlightLatencyUsSum->Val());
            const ui64 maxLatencyUsBeforeError = maxLatencyUs->Val();

            SendToSkeletonFront(
                env.Runtime,
                env.SkeletonFrontId,
                env.EdgeActor,
                new TEvPDiskErrorStateChange(NKikimrProto::CORRUPTED, 0, "test error"),
                TEvBlobStorage::EvPDiskErrorStateChange);

            auto result = env.Runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvVPutResult>(env.EdgeActor, TDuration::Seconds(1));
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetStatus(), NKikimrProto::ERROR);

            // TQueueInplace::Pop() keeps the popped slot alive, so the forceError path has
            // to reset the guard explicitly before the next counter snapshot is published.
            UpdateStats(env.Runtime, env.SkeletonFrontId, env.EdgeActor);
            UNIT_ASSERT_VALUES_EQUAL(inFlightCount->Val(), 0);
            UNIT_ASSERT_VALUES_EQUAL(inFlightLatencyUsSum->Val(), 0);
            UNIT_ASSERT_VALUES_EQUAL(maxLatencyUs->Val(), maxLatencyUsBeforeError);
        }

        Y_UNIT_TEST(CompletedDelayedPutUserDataRemovesInFlightLatency) {
            TTestEnv env(1);

            const TString data(MinHugeBlobInBytes, 'x');
            const TLogoBlobID firstBlobId(1, 1, 1, 0, data.size(), 0);
            auto firstPut = MakeUserDataPut(firstBlobId, data, env.GetVDiskId());
            SendToSkeletonFront(
                env.Runtime,
                env.SkeletonFrontId,
                env.EdgeActor,
                firstPut.release(),
                TEvBlobStorage::EvVPut);
            auto firstForwarded = GrabForwardedVPut(env);

            const TLogoBlobID secondBlobId(1, 1, 2, 0, data.size(), 0);
            auto secondPut = MakeUserDataPut(secondBlobId, data, env.GetVDiskId());
            SendToSkeletonFront(
                env.Runtime,
                env.SkeletonFrontId,
                env.EdgeActor,
                secondPut.release(),
                TEvBlobStorage::EvVPut);

            auto latencyGroup = GetPutUserDataLatencyGroup(env.Counters, env.Config, env.GroupInfo);
            auto inFlightCount = FindCounter(latencyGroup, "InFlightCount");
            auto inFlightLatencyUsSum = FindCounter(latencyGroup, "InFlightLatencyUsSum");
            auto maxLatencyUs = FindCounter(latencyGroup, "LatencyUsMax");
            auto skeletonFrontGroup = GetSkeletonFrontGroup(env.Counters, env.Config, env.GroupInfo);
            auto hugePutsForegroundDelayedCount = FindCounter(skeletonFrontGroup, "SkeletonFront/HugePutsForeground/DelayedCount");
            auto hugePutsForegroundInFlightCount = FindCounter(skeletonFrontGroup, "SkeletonFront/HugePutsForeground/InFlightCount");

            env.Runtime.AdvanceCurrentTime(TDuration::MilliSeconds(10));
            UpdateStats(env.Runtime, env.SkeletonFrontId, env.EdgeActor);
            UNIT_ASSERT_VALUES_EQUAL(hugePutsForegroundDelayedCount->Val(), 1);
            UNIT_ASSERT_VALUES_EQUAL(hugePutsForegroundInFlightCount->Val(), 1);
            UNIT_ASSERT_VALUES_EQUAL_C(inFlightCount->Val(), 2,
                                       "delayed# " << hugePutsForegroundDelayedCount->Val()
                                                   << " queueInFlight# " << hugePutsForegroundInFlightCount->Val());
            UNIT_ASSERT_GT(inFlightLatencyUsSum->Val(), 0);
            UNIT_ASSERT_GT(maxLatencyUs->Val(), 0);
            UNIT_ASSERT_LE(maxLatencyUs->Val(), inFlightLatencyUsSum->Val());

            CompleteForwardedVPut(env, std::move(firstForwarded));
            auto secondForwarded = GrabForwardedVPut(env);
            UNIT_ASSERT_VALUES_EQUAL(
                LogoBlobIDFromLogoBlobID(secondForwarded->Get<TEvBlobStorage::TEvVPut>()->Record.GetBlobID()),
                secondBlobId);

            env.Runtime.AdvanceCurrentTime(TDuration::MilliSeconds(10));
            UpdateStats(env.Runtime, env.SkeletonFrontId, env.EdgeActor);
            UNIT_ASSERT_VALUES_EQUAL(hugePutsForegroundDelayedCount->Val(), 0);
            UNIT_ASSERT_VALUES_EQUAL(hugePutsForegroundInFlightCount->Val(), 1);
            UNIT_ASSERT_VALUES_EQUAL(inFlightCount->Val(), 1);
            UNIT_ASSERT_GT(inFlightLatencyUsSum->Val(), 0);
            UNIT_ASSERT_VALUES_EQUAL(maxLatencyUs->Val(), inFlightLatencyUsSum->Val());
            const ui64 maxLatencyUsBeforeComplete = maxLatencyUs->Val();

            CompleteForwardedVPut(env, std::move(secondForwarded));

            UpdateStats(env.Runtime, env.SkeletonFrontId, env.EdgeActor);
            UNIT_ASSERT_VALUES_EQUAL(hugePutsForegroundDelayedCount->Val(), 0);
            UNIT_ASSERT_VALUES_EQUAL(hugePutsForegroundInFlightCount->Val(), 0);
            UNIT_ASSERT_VALUES_EQUAL(inFlightCount->Val(), 0);
            UNIT_ASSERT_VALUES_EQUAL(inFlightLatencyUsSum->Val(), 0);
            UNIT_ASSERT_VALUES_EQUAL(maxLatencyUs->Val(), maxLatencyUsBeforeComplete);
        }

        Y_UNIT_TEST(MaxLatencyUsesMaxInFlightRequestLatency) {
            TTestEnv env(2);

            const TDuration firstAgeBeforeSecond = TDuration::MilliSeconds(10);
            const TDuration ageAfterSecond = TDuration::MilliSeconds(20);

            const TString data(MinHugeBlobInBytes, 'x');
            const TLogoBlobID firstBlobId(1, 1, 1, 0, data.size(), 0);
            auto firstPut = MakeUserDataPut(firstBlobId, data, env.GetVDiskId());
            SendToSkeletonFront(
                env.Runtime,
                env.SkeletonFrontId,
                env.EdgeActor,
                firstPut.release(),
                TEvBlobStorage::EvVPut);
            auto firstForwarded = GrabForwardedVPut(env);

            auto latencyGroup = GetPutUserDataLatencyGroup(env.Counters, env.Config, env.GroupInfo);
            auto inFlightCount = FindCounter(latencyGroup, "InFlightCount");
            auto inFlightLatencyUsSum = FindCounter(latencyGroup, "InFlightLatencyUsSum");
            auto maxLatencyUs = FindCounter(latencyGroup, "LatencyUsMax");

            env.Runtime.AdvanceCurrentTime(firstAgeBeforeSecond);
            UpdateStats(env.Runtime, env.SkeletonFrontId, env.EdgeActor);

            UNIT_ASSERT_VALUES_EQUAL(inFlightCount->Val(), 1);
            UNIT_ASSERT_VALUES_EQUAL(inFlightLatencyUsSum->Val(), firstAgeBeforeSecond.MicroSeconds());
            UNIT_ASSERT_VALUES_EQUAL(maxLatencyUs->Val(), firstAgeBeforeSecond.MicroSeconds());

            const TLogoBlobID secondBlobId(1, 1, 2, 0, data.size(), 0);
            auto secondPut = MakeUserDataPut(secondBlobId, data, env.GetVDiskId());
            SendToSkeletonFront(
                env.Runtime,
                env.SkeletonFrontId,
                env.EdgeActor,
                secondPut.release(),
                TEvBlobStorage::EvVPut);
            auto secondForwarded = GrabForwardedVPut(env);
            UpdateStats(env.Runtime, env.SkeletonFrontId, env.EdgeActor);

            UNIT_ASSERT_VALUES_EQUAL(inFlightCount->Val(), 2);
            UNIT_ASSERT_VALUES_EQUAL(inFlightLatencyUsSum->Val(), firstAgeBeforeSecond.MicroSeconds());
            UNIT_ASSERT_VALUES_EQUAL(maxLatencyUs->Val(), firstAgeBeforeSecond.MicroSeconds());

            env.Runtime.AdvanceCurrentTime(ageAfterSecond);
            UpdateStats(env.Runtime, env.SkeletonFrontId, env.EdgeActor);

            const auto expectedMaxLatencyUs = (firstAgeBeforeSecond + ageAfterSecond).MicroSeconds();
            const auto expectedInFlightLatencyUsSum = expectedMaxLatencyUs + ageAfterSecond.MicroSeconds();
            UNIT_ASSERT_VALUES_EQUAL(inFlightCount->Val(), 2);
            UNIT_ASSERT_VALUES_EQUAL(inFlightLatencyUsSum->Val(), expectedInFlightLatencyUsSum);
            UNIT_ASSERT_VALUES_EQUAL(maxLatencyUs->Val(), expectedMaxLatencyUs);

            CompleteForwardedVPut(env, std::move(firstForwarded));
            UpdateStats(env.Runtime, env.SkeletonFrontId, env.EdgeActor);

            const auto remainingRequestLatencyUs = ageAfterSecond.MicroSeconds();
            const auto maxLatencyUsBeforeRoll = maxLatencyUs->Val();
            UNIT_ASSERT_VALUES_EQUAL(inFlightCount->Val(), 1);
            UNIT_ASSERT_VALUES_EQUAL(inFlightLatencyUsSum->Val(), remainingRequestLatencyUs);
            UNIT_ASSERT_VALUES_EQUAL(maxLatencyUsBeforeRoll, expectedMaxLatencyUs);

            for (size_t i = 0; i < MaxTrackerSlots; ++i) {
                UpdateStats(env.Runtime, env.SkeletonFrontId, env.EdgeActor);
            }

            UNIT_ASSERT_VALUES_EQUAL(inFlightCount->Val(), 1);
            UNIT_ASSERT_VALUES_EQUAL(inFlightLatencyUsSum->Val(), remainingRequestLatencyUs);
            UNIT_ASSERT_LT(maxLatencyUs->Val(), maxLatencyUsBeforeRoll);
            UNIT_ASSERT_VALUES_EQUAL(maxLatencyUs->Val(), remainingRequestLatencyUs);

            CompleteForwardedVPut(env, std::move(secondForwarded));
        }

    } // Y_UNIT_TEST_SUITE(TSkeletonFrontLatency)

} // namespace NKikimr
