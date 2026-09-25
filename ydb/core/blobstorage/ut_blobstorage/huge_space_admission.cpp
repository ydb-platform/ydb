#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/vdisk/huge/blobstorage_hullhuge.h>
#include <ydb/core/blobstorage/vdisk/hullop/blobstorage_hullactor.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_private_events.h>

namespace {

using TColor = NKikimrBlobStorage::TPDiskSpaceColor;
using TDataKind = NKikimrBlobStorage::TDataKind;

struct THugeAdmissionEnv {
    static TFeatureFlags Flags() {
        TFeatureFlags flags;
        flags.SetEnableVDiskFreshSpaceProjection(true);
        flags.SetEnableVDiskHeapAllocator(true);
        return flags;
    }

    TEnvironmentSetup Env{{
        .NodeCount = 1,
        .Erasure = TBlobStorageGroupType::ErasureNone,
        .VDiskConfigPreprocessor = [](TVDiskConfig& config) {
            config.FreshBufSizeLogoBlobs = 256_MB;
            config.LevelCompaction = false;
        },
        .FeatureFlags = Flags(),
        .MinHugeBlobInBytes = 512_KB,
        .PDiskChunkSize = 32_MB,
    }};
    TIntrusivePtr<TBlobStorageGroupInfo> Info;
    TActorId Queue;
    TActorId Skeleton;
    TActorId HugeKeeper;
    ui32 NextStep = 1;
    ui32 HugeWrites = 0;
    ui32 FreshReserves = 0;
    std::vector<TColor::E> HugeReserveBounds;
    bool RejectFresh = false;
    bool HoldHugeReserve = false;
    bool RejectFurtherFresh = false;
    std::unique_ptr<IEventHandle> HeldReserve;

    THugeAdmissionEnv() {
        Env.CreateBoxAndPool(1, 1);
        Env.Sim(TDuration::Seconds(30));
        Info = Env.GetGroupInfo(Env.GetGroups().front());
        Queue = Env.CreateQueueActor(Info->GetVDiskId(0), NKikimrBlobStorage::EVDiskQueueId::PutTabletLog, 1000);
        Env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvBlobStorage::EvCutLog:
                    return false;
                case TEvBlobStorage::EvVPut:
                    // the skeleton front forwards the put to the skeleton, which reserves Fresh chunks
                    if (ev->Recipient != Info->GetActorId(0)) {
                        Skeleton = ev->Recipient;
                    }
                    break;
                case TEvBlobStorage::EvHullWriteHugeBlob: {
                    UNIT_ASSERT_VALUES_EQUAL(ev->Sender, Skeleton);
                    HugeKeeper = ev->Recipient;
                    ++HugeWrites;
                    const auto* msg = ev->Get<TEvHullWriteHugeBlob>();
                    UNIT_ASSERT_C(!msg->FreshAdmission.Empty(), "huge data sent without an index reservation");
                    UNIT_ASSERT(msg->FreshRefuseAtColor);
                    break;
                }
                case TEvBlobStorage::EvChunkReserve: {
                    const auto* msg = ev->Get<NPDisk::TEvChunkReserve>();
                    if (msg->ForHousekeeping) {
                        break;
                    }
                    // Fresh reservations come from the skeleton, data chunk ones from the allocators HugeKeeper
                    // registers in its own mailbox. Anything else (sync log, chunk keeper) is not ours to count.
                    if (ev->Sender == Skeleton) {
                        ++FreshReserves;
                        if (RejectFresh || RejectFurtherFresh) {
                            Reject(std::move(ev), nodeId);
                            return false;
                        }
                    } else if (HugeKeeper && ev->Sender.Hint() == HugeKeeper.Hint()) {
                        HugeReserveBounds.push_back(msg->RefuseAtColor);
                        UNIT_ASSERT_VALUES_EQUAL(msg->SizeChunks, 1);
                        if (HoldHugeReserve) {
                            UNIT_ASSERT(!HeldReserve);
                            HeldReserve = std::move(ev);
                            return false;
                        }
                    }
                    break;
                }
            }
            return true;
        };
    }

    ~THugeAdmissionEnv() {
        Env.Runtime->FilterFunction = {};
    }

    void Reject(std::unique_ptr<IEventHandle> ev, ui32 nodeId) {
        auto result = std::make_unique<NPDisk::TEvChunkReserveResult>(NKikimrProto::OUT_OF_SPACE,
            ui32(NKikimrBlobStorage::StatusNotEnoughDiskSpaceForOperation));
        result->ErrorReason = "injected projected color refusal";
        Env.Runtime->Send(new IEventHandle(ev->Sender, ev->Recipient, result.release(), 0, ev->Cookie), nodeId);
    }

    TActorId SendPut(TDataKind::E kind = TDataKind::USER, bool unavoidable = false, ui32 size = 1_MB) {
        const TActorId edge = Env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        const TString data(size, 'x');
        const TLogoBlobID id(1000, 1, NextStep++, 0, data.size(), 0, 1);
        Env.Runtime->Send(new IEventHandle(Queue, edge, new TEvBlobStorage::TEvVPut(id, TRope(data),
            Info->GetVDiskId(0), unavoidable, nullptr, TInstant::Max(), NKikimrBlobStorage::TabletLog,
            false, TWriteSource::Unknown, kind)), 1);
        return edge;
    }

    void ExpectPut(TActorId edge, NKikimrProto::EReplyStatus status) {
        auto result = Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVPutResult>(edge, true,
            Env.Runtime->GetClock() + TDuration::Minutes(1));
        UNIT_ASSERT_C(result, "huge put did not finish");
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetStatus(), status);
    }

    void Compact() {
        const TActorId edge = Env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        Env.Runtime->Send(new IEventHandle(Info->GetActorId(0), edge,
            TEvCompactVDisk::Create(EHullDbType::LogoBlobs, TEvCompactVDisk::EMode::FRESH_ONLY)), 1);
        auto result = Env.WaitForEdgeActorEvent<TEvCompactVDiskResult>(edge, true,
            Env.Runtime->GetClock() + TDuration::Minutes(1));
        UNIT_ASSERT_C(result, "Fresh admission leaked: compaction could not rotate the segment");
    }
};

} // namespace

Y_UNIT_TEST_SUITE(VDiskHugeSpaceAdmission) {
    Y_UNIT_TEST(IndexRefusalPrecedesDataAllocation) {
        THugeAdmissionEnv env;
        env.RejectFresh = true;
        env.ExpectPut(env.SendPut(), NKikimrProto::OUT_OF_SPACE);
        UNIT_ASSERT_VALUES_EQUAL(env.FreshReserves, 1);
        UNIT_ASSERT_VALUES_EQUAL(env.HugeWrites, 0);
        UNIT_ASSERT(env.HugeReserveBounds.empty());
        env.RejectFresh = false;
        env.ExpectPut(env.SendPut(), NKikimrProto::OK);
        env.Compact();
    }

    Y_UNIT_TEST(QueuedSystemPutRetriesAfterUserAllocationRefusal) {
        THugeAdmissionEnv env;
        env.HoldHugeReserve = true;
        const TActorId user = env.SendPut();
        env.Env.Sim(TDuration::Seconds(1));
        UNIT_ASSERT(env.HeldReserve);
        UNIT_ASSERT_VALUES_EQUAL(env.HugeReserveBounds.size(), 1);
        UNIT_ASSERT_EQUAL(env.HugeReserveBounds.front(), TColor::PRE_ORANGE);

        const TActorId system = env.SendPut(TDataKind::SYSTEM);
        env.Env.Sim(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(env.HugeWrites, 2);
        UNIT_ASSERT_VALUES_EQUAL(env.HugeReserveBounds.size(), 1); // same slot-size queue

        env.HoldHugeReserve = false;
        env.Reject(std::move(env.HeldReserve), 1);
        env.ExpectPut(user, NKikimrProto::OUT_OF_SPACE);
        env.ExpectPut(system, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(env.HugeReserveBounds.size(), 2);
        UNIT_ASSERT_EQUAL(env.HugeReserveBounds.back(), TColor::RED);
        env.Compact(); // also checks that the rejected USER index charge landed
    }

    // The index of a huge put waiting for its data to be written has no LSN yet: Fresh compaction rotates the
    // segment without waiting for it, and the index lands in the new one.
    Y_UNIT_TEST(FreshCompactionDoesNotWaitForHugeData) {
        THugeAdmissionEnv env;
        env.ExpectPut(env.SendPut(TDataKind::USER, false, 100), NKikimrProto::OK); // something for Fresh to compact
        env.HoldHugeReserve = true;
        const TActorId huge = env.SendPut();
        env.Env.Sim(TDuration::Seconds(1));
        UNIT_ASSERT(env.HeldReserve);

        env.Compact(); // with the huge put's index still admitted and its data not written
        env.HoldHugeReserve = false;
        env.Env.Runtime->Send(env.HeldReserve.release(), 1);
        env.ExpectPut(huge, NKikimrProto::OK);
        env.Compact();
    }

    Y_UNIT_TEST(CompactionSlotsSurviveUserAllocationRefusal) {
        THugeAdmissionEnv env;
        env.HoldHugeReserve = true;
        const TActorId user = env.SendPut();
        env.Env.Sim(TDuration::Seconds(1));
        UNIT_ASSERT(env.HeldReserve);
        UNIT_ASSERT(env.HugeKeeper);

        // What a compaction asks for: it queues behind the USER put's allocator, the heap having no room yet.
        const TActorId compaction = env.Env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        env.Env.Runtime->Send(new IEventHandle(env.HugeKeeper, compaction,
            new TEvHugeAllocateSlots(std::vector<ui32>{ui32(1_MB)})), 1);
        env.Env.Sim(TDuration::Seconds(1));

        // Refusing the USER allocator refuses the USER put, not the maintenance request behind it.
        env.HoldHugeReserve = false;
        env.Reject(std::move(env.HeldReserve), 1);
        env.ExpectPut(user, NKikimrProto::OUT_OF_SPACE);
        auto slots = env.Env.WaitForEdgeActorEvent<TEvHugeAllocateSlotsResult>(compaction, true,
            env.Env.Runtime->GetClock() + TDuration::Minutes(1));
        UNIT_ASSERT_C(slots, "compaction slot allocation did not finish");
        UNIT_ASSERT_VALUES_EQUAL(slots->Get()->Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(slots->Get()->Locations.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.HugeReserveBounds.size(), 1); // no second USER allocator

        env.Env.Runtime->Send(new IEventHandle(env.HugeKeeper, compaction,
            new TEvHugeDropAllocatedSlots(std::move(slots->Get()->Locations))), 1);
        env.Compact();
    }

    Y_UNIT_TEST(IndexCreditSurvivesDelayedDataAndSlotsShareChunks) {
        THugeAdmissionEnv env;
        env.HoldHugeReserve = true;
        const TActorId first = env.SendPut();
        env.Env.Sim(TDuration::Seconds(1));
        UNIT_ASSERT(env.HeldReserve);
        UNIT_ASSERT_VALUES_EQUAL(env.FreshReserves, 1);

        // A new index reservation would now fail, but the admitted put already
        // has one. Let the data write finish after this transition.
        env.RejectFurtherFresh = true;
        env.HoldHugeReserve = false;
        env.Env.Runtime->Send(env.HeldReserve.release(), 1);
        env.ExpectPut(first, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(env.FreshReserves, 1);

        // With the first index in Cur, the next unsequenced one takes a chunk of its own, so that a rotation can
        // carry it over to the new Cur; after that Cur has what every further one needs. The data of all of them
        // shares the chunk the first put allocated.
        env.RejectFurtherFresh = false;
        const size_t requests = env.HugeReserveBounds.size();
        for (ui32 i = 0; i < 8; ++i) {
            env.ExpectPut(env.SendPut(), NKikimrProto::OK);
        }
        UNIT_ASSERT_VALUES_EQUAL(env.FreshReserves, 2);
        UNIT_ASSERT_VALUES_EQUAL(env.HugeReserveBounds.size(), requests);
        env.Compact();
    }

    Y_UNIT_TEST(AllocatorPreservesDataKindAndUnavoidableBounds) {
        for (const auto kind : {TDataKind::USER, TDataKind::SYSTEM}) {
            for (const bool unavoidable : {false, true}) {
                THugeAdmissionEnv env;
                env.ExpectPut(env.SendPut(kind, unavoidable), NKikimrProto::OK);
                UNIT_ASSERT_VALUES_EQUAL(env.HugeReserveBounds.size(), 1);
                const auto expected = kind == TDataKind::SYSTEM
                    ? (unavoidable ? TColor::BLACK : TColor::RED)
                    : (unavoidable ? TColor::RED : TColor::PRE_ORANGE);
                UNIT_ASSERT_EQUAL(env.HugeReserveBounds.front(), expected);
                env.Compact();
            }
        }
    }
}
