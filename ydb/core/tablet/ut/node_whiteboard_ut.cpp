#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NNodeWhiteboard {
namespace {

using TDDiskState = NKikimrWhiteboard::TDDiskStateInfo;
using TPDiskResponse = NKikimrWhiteboard::TEvPDiskStateResponse;

constexpr ui32 PDiskId = 1;
constexpr ui32 SlotId = 2;

struct TTestEnv {
    TTestBasicRuntime Runtime;
    TActorId Board;
    TActorId Client;
    TActorId DDisk;
    TActorId Buffer;
    TActorId Replacement;

    TTestEnv() {
        SetupTabletServices(Runtime);
        Board = MakeNodeWhiteboardServiceId(Runtime.GetNodeId(0));
        Client = Runtime.AllocateEdgeActor();
        DDisk = Runtime.AllocateEdgeActor();
        Buffer = Runtime.AllocateEdgeActor();
        Replacement = Runtime.AllocateEdgeActor();
    }

    void ReportHealth(const TActorId& sender, ui64 round, bool isBuffer,
            TDDiskState::EComponentStatus status, TDuration lifetime, TString reason = {}, TString storagePoolName = {}) {
        Runtime.Send(new IEventHandle(Board, sender, new TEvWhiteboard::TEvDDiskHealthUpdate(
            PDiskId, SlotId, round, isBuffer, status, std::move(reason), lifetime, std::move(storagePoolName))));
    }

    void ReportCapacity(const TActorId& sender, ui64 round, TDuration lifetime) {
        auto* update = new TEvWhiteboard::TEvDDiskStateUpdate;
        update->OwnerRound = round;
        update->Lifetime = lifetime;
        update->Record.SetPDiskId(PDiskId);
        update->Record.SetDDiskSlotId(SlotId);
        update->Record.SetDDiskOccupancy(0.25);
        update->Record.SetPersistentBufferOccupancy(0.0);
        update->Record.SetAllocatedSize(256);
        update->Record.SetAvailableSize(768);
        update->Record.SetTotalSize(1024);
        update->Record.SetStoragePoolName("ddisk-pool");
        Runtime.Send(new IEventHandle(Board, sender, update));
    }

    void Delete(const TActorId& sender, ui64 round, bool isBuffer) {
        Runtime.Send(new IEventHandle(Board, sender,
            new TEvWhiteboard::TEvDDiskStateDelete(PDiskId, SlotId, round, isBuffer)));
    }

    TPDiskResponse Query(ui64 changedSince = 0, bool includeDDiskState = true) {
        auto* request = new TEvWhiteboard::TEvPDiskStateRequest;
        request->Record.SetIncludeDDiskState(includeDDiskState);
        request->Record.SetChangedSince(changedSince);
        Runtime.Send(new IEventHandle(Board, Client, request));
        return Runtime.GrabEdgeEventRethrow<TEvWhiteboard::TEvPDiskStateResponse>(Client)->Get()->Record;
    }

    TDDiskState QuerySlot(ui64 changedSince = 0) {
        const auto response = Query(changedSince);
        UNIT_ASSERT_VALUES_EQUAL(response.DDiskStateInfoSize(), 1);
        const auto& slot = response.GetDDiskStateInfo(0);
        UNIT_ASSERT_VALUES_EQUAL(slot.GetPDiskId(), PDiskId);
        UNIT_ASSERT_VALUES_EQUAL(slot.GetDDiskSlotId(), SlotId);
        return slot;
    }
};

void AssertNoCapacity(const TDDiskState& slot) {
    UNIT_ASSERT(slot.HasHasWhiteboardData());
    UNIT_ASSERT(!slot.GetHasWhiteboardData());
    UNIT_ASSERT(!slot.HasChangeTime());
    UNIT_ASSERT(!slot.HasDDiskOccupancy());
    UNIT_ASSERT(!slot.HasPersistentBufferOccupancy());
    UNIT_ASSERT(!slot.HasAllocatedSize());
    UNIT_ASSERT(!slot.HasAvailableSize());
    UNIT_ASSERT(!slot.HasTotalSize());
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TNodeWhiteboardDDiskTest) {
    Y_UNIT_TEST(HealthOnlyMatchesChangedSince) {
        TTestEnv env;
        const ui64 since = env.Query().GetResponseTime() + 1;
        env.Runtime.AdvanceCurrentTime(TDuration::MilliSeconds(2));
        env.ReportHealth(env.DDisk, 1, false, TDDiskState::Initializing, TDuration::Seconds(30), {}, "ddisk-pool");

        auto slot = env.QuerySlot(since);
        AssertNoCapacity(slot);
        UNIT_ASSERT(slot.HasDDiskStatus());
        UNIT_ASSERT(slot.GetDDiskStatus() == TDDiskState::Initializing);
        UNIT_ASSERT(!slot.HasPersistentBufferStatus());
        UNIT_ASSERT_VALUES_EQUAL(slot.GetStoragePoolName(), "ddisk-pool");
        UNIT_ASSERT_VALUES_EQUAL(slot.GetPersistentBufferId(),
            MakeBlobStoragePersistentBufferId(env.Runtime.GetNodeId(0), PDiskId, SlotId).ToString());
        UNIT_ASSERT(!slot.HasGroupId());
        UNIT_ASSERT_VALUES_EQUAL(env.Query(since, false).DDiskStateInfoSize(), 0);
        const ui64 afterHealth = env.Query().GetResponseTime() + 1;
        UNIT_ASSERT_VALUES_EQUAL(env.Query(afterHealth).DDiskStateInfoSize(), 0);

        env.ReportCapacity(env.Buffer, 1, TDuration::Seconds(30));
        slot = env.QuerySlot();
        UNIT_ASSERT(slot.HasChangeTime());
        const ui64 capacityTime = slot.GetChangeTime();
        env.Runtime.AdvanceCurrentTime(TDuration::MilliSeconds(2));
        env.ReportHealth(env.DDisk, 1, false, TDDiskState::Ready, TDuration::Seconds(30));
        slot = env.QuerySlot(capacityTime + 1);
        UNIT_ASSERT(slot.HasDDiskStatus());
        UNIT_ASSERT(slot.GetDDiskStatus() == TDDiskState::Ready);
        UNIT_ASSERT(slot.GetHasWhiteboardData());
        UNIT_ASSERT_VALUES_EQUAL(slot.GetChangeTime(), capacityTime);
        UNIT_ASSERT_VALUES_EQUAL(slot.GetAllocatedSize(), 256);
    }

    Y_UNIT_TEST(CapacityAndHealthExpireIndependently) {
        TTestEnv env;
        env.ReportHealth(env.DDisk, 1, false, TDDiskState::Ready, TDuration::Seconds(90));
        env.ReportHealth(env.Buffer, 1, true, TDDiskState::Broken, TDuration::Seconds(5), "buffer broken");
        env.ReportCapacity(env.Buffer, 1, TDuration::Seconds(10));
        auto slot = env.QuerySlot();
        UNIT_ASSERT(slot.GetPersistentBufferStatus() == TDDiskState::Broken);
        UNIT_ASSERT_VALUES_EQUAL(slot.GetPersistentBufferStatusReason(), "buffer broken");
        UNIT_ASSERT(slot.GetHasWhiteboardData());
        UNIT_ASSERT(slot.HasPersistentBufferOccupancy());

        env.Runtime.AdvanceCurrentTime(TDuration::Seconds(6));
        slot = env.QuerySlot();
        UNIT_ASSERT(slot.HasDDiskStatus());
        UNIT_ASSERT(slot.GetDDiskStatus() == TDDiskState::Ready);
        UNIT_ASSERT(!slot.HasPersistentBufferStatus());
        UNIT_ASSERT(!slot.HasPersistentBufferStatusReason());
        UNIT_ASSERT(slot.GetHasWhiteboardData());
        UNIT_ASSERT_VALUES_EQUAL(slot.GetTotalSize(), 1024);

        env.Runtime.AdvanceCurrentTime(TDuration::Seconds(55));
        slot = env.QuerySlot();
        UNIT_ASSERT(slot.HasDDiskStatus());
        UNIT_ASSERT(slot.GetDDiskStatus() == TDDiskState::Ready);
        AssertNoCapacity(slot);
        UNIT_ASSERT_VALUES_EQUAL(slot.GetStoragePoolName(), "ddisk-pool");

        env.Runtime.AdvanceCurrentTime(TDuration::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL(env.Query().DDiskStateInfoSize(), 0);
        env.ReportHealth(env.Replacement, 1, false, TDDiskState::Broken, TDuration::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL(env.Query().DDiskStateInfoSize(), 0);
        env.ReportHealth(env.DDisk, 1, false, TDDiskState::Ready, TDuration::Seconds(30));
        slot = env.QuerySlot();
        AssertNoCapacity(slot);
        UNIT_ASSERT(slot.HasDDiskStatus());
        UNIT_ASSERT(slot.GetDDiskStatus() == TDDiskState::Ready);
    }

    Y_UNIT_TEST(FencesStaleOwnerAndWrongRoleSender) {
        TTestEnv env;
        const auto lifetime = TDuration::Seconds(30);
        env.ReportHealth(env.DDisk, 1, false, TDDiskState::Ready, lifetime);
        env.ReportHealth(env.Buffer, 1, true, TDDiskState::Ready, lifetime);
        env.ReportCapacity(env.Buffer, 1, lifetime);
        UNIT_ASSERT(env.QuerySlot().GetHasWhiteboardData());

        env.ReportHealth(env.Replacement, 2, false, TDDiskState::Initializing, lifetime);
        env.ReportHealth(env.DDisk, 1, false, TDDiskState::Broken, lifetime);
        env.ReportHealth(env.Buffer, 1, true, TDDiskState::Broken, lifetime);
        env.ReportCapacity(env.Buffer, 1, lifetime);
        env.Delete(env.DDisk, 1, false);
        env.Delete(env.Buffer, 1, true);
        auto slot = env.QuerySlot();
        UNIT_ASSERT(slot.GetDDiskStatus() == TDDiskState::Initializing);
        UNIT_ASSERT(!slot.HasPersistentBufferStatus());
        UNIT_ASSERT(!slot.HasStoragePoolName());
        AssertNoCapacity(slot);

        const auto newBuffer = env.Runtime.AllocateEdgeActor();
        env.ReportHealth(newBuffer, 2, true, TDDiskState::Ready, lifetime);
        env.ReportHealth(env.Replacement, 2, true, TDDiskState::Broken, lifetime);
        env.ReportHealth(newBuffer, 2, false, TDDiskState::Broken, lifetime);
        env.ReportCapacity(env.Replacement, 2, lifetime);
        env.Delete(newBuffer, 2, false);
        env.Delete(env.Replacement, 2, true);
        slot = env.QuerySlot();
        UNIT_ASSERT(slot.GetDDiskStatus() == TDDiskState::Initializing);
        UNIT_ASSERT(slot.HasPersistentBufferStatus());
        UNIT_ASSERT(slot.GetPersistentBufferStatus() == TDDiskState::Ready);
        AssertNoCapacity(slot);
    }

    Y_UNIT_TEST(DeletesOnlyThePublishingRole) {
        const auto lifetime = TDuration::Seconds(30);
        for (bool deleteBuffer : {true, false}) {
            TTestEnv env;
            env.ReportHealth(env.DDisk, 1, false, TDDiskState::Ready, lifetime);
            env.ReportHealth(env.Buffer, 1, true, TDDiskState::Ready, lifetime);
            env.ReportCapacity(env.Buffer, 1, lifetime);
            UNIT_ASSERT(env.QuerySlot().GetHasWhiteboardData());
            const auto& sender = deleteBuffer ? env.Buffer : env.DDisk;
            env.Delete(sender, 1, deleteBuffer);
            env.ReportHealth(sender, 1, deleteBuffer, TDDiskState::Broken, lifetime);
            env.ReportCapacity(env.Buffer, 1, lifetime);
            const auto slot = env.QuerySlot();
            if (deleteBuffer) {
                AssertNoCapacity(slot);
                UNIT_ASSERT(!slot.HasPersistentBufferStatus());
                UNIT_ASSERT(slot.HasDDiskStatus());
                UNIT_ASSERT(slot.GetDDiskStatus() == TDDiskState::Ready);
                UNIT_ASSERT_VALUES_EQUAL(slot.GetStoragePoolName(), "ddisk-pool");
            } else {
                UNIT_ASSERT(slot.GetHasWhiteboardData());
                UNIT_ASSERT(!slot.HasDDiskStatus());
                UNIT_ASSERT(slot.HasPersistentBufferStatus());
                UNIT_ASSERT(slot.GetPersistentBufferStatus() == TDDiskState::Ready);
            }
        }
    }
}

} // namespace NKikimr::NNodeWhiteboard
