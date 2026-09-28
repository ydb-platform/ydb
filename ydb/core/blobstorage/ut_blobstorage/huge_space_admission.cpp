#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/vdisk/huge/blobstorage_hullhuge.h>
#include <ydb/core/blobstorage/vdisk/hullop/blobstorage_hullactor.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_private_events.h>

namespace {

using TColor = NKikimrBlobStorage::TPDiskSpaceColor;
using TDataKind = NKikimrBlobStorage::TDataKind;
using TPurpose = NPDisk::EAllocationPurpose;

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
    std::vector<TPurpose> FreshReservePurposes;
    std::vector<TPurpose> HugeWritePurposes;
    std::vector<TPurpose> HugeReservePurposes;
    bool RejectFresh = false;
    bool HoldHugeReserve = false;
    bool RejectFurtherFresh = false;
    std::unique_ptr<IEventHandle> HeldReserve;
    ui32 HoldHugeLogStep = 0;
    std::unique_ptr<IEventHandle> HeldHugeLog;

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
                    HugeWritePurposes.push_back(msg->AllocationPurpose);
                    UNIT_ASSERT_C(!msg->FreshAdmission.Empty(), "huge data sent without an index reservation");
                    UNIT_ASSERT(msg->FreshRefuseAtColor);
                    break;
                }
                case TEvBlobStorage::EvHullLogHugeBlob:
                    if (HoldHugeLogStep && ev->Get<TEvHullLogHugeBlob>()->LogoBlobID.Step() == HoldHugeLogStep) {
                        UNIT_ASSERT(!HeldHugeLog);
                        HeldHugeLog = std::move(ev);
                        return false;
                    }
                    break;
                case TEvBlobStorage::EvChunkReserve: {
                    const auto* msg = ev->Get<NPDisk::TEvChunkReserve>();
                    if (HugeKeeper && ev->Sender.Hint() == HugeKeeper.Hint()) {
                        HugeReservePurposes.push_back(msg->Purpose);
                    }
                    if (msg->ForHousekeeping) {
                        break;
                    }
                    // Fresh reservations come from the skeleton, data chunk ones from the allocators HugeKeeper
                    // registers in its own mailbox. Anything else (sync log, chunk keeper) is not ours to count.
                    if (ev->Sender == Skeleton) {
                        ++FreshReserves;
                        FreshReservePurposes.push_back(msg->Purpose);
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

    TDiskPart ExpectPut(TActorId edge, NKikimrProto::EReplyStatus status) {
        auto result = Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVPutResult>(edge, true,
            Env.Runtime->GetClock() + TDuration::Minutes(1));
        UNIT_ASSERT_C(result, "huge put did not finish");
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetStatus(), status);
        return result->Get()->WrittenLocation;
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
    Y_UNIT_TEST(RewriteUsesMaintenanceReserveForFreshAndHugeData) {
        for (const bool lockChunk : {false, true}) {
            THugeAdmissionEnv env;
            const TDiskPart original = env.ExpectPut(env.SendPut(), NKikimrProto::OK);
            UNIT_ASSERT(original.ChunkIdx);
            env.Compact(); // the rewrite must obtain new Fresh credit

            if (lockChunk) {
                // Defrag locks the source chunk so its free slots cannot serve the rewrite.
                const TActorId edge = env.Env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
                env.Env.Runtime->Send(new IEventHandle(env.HugeKeeper, edge,
                    new TEvHugeLockChunks(TDefragChunks{{original.ChunkIdx, 0}})), 1);
                auto result = env.Env.WaitForEdgeActorEvent<TEvHugeLockChunksResult>(edge, true,
                    env.Env.Runtime->GetClock() + TDuration::Minutes(1));
                UNIT_ASSERT(result);
                UNIT_ASSERT_VALUES_EQUAL(result->Get()->LockedChunks.size(), 1);
            }

            // Keep physical space available, but put all remaining User headroom into reserves.
            for (auto& [key, pdisk] : env.Env.PDiskMockStates) {
                pdisk->SetAllocationReserves(1, env.Env.Settings.PDiskSize / env.Env.Settings.PDiskChunkSize);
            }
            const ui32 freshBefore = env.FreshReserves;
            env.ExpectPut(env.SendPut(TDataKind::USER, true), NKikimrProto::OUT_OF_SPACE);
            UNIT_ASSERT_VALUES_EQUAL(env.FreshReserves, freshBefore + 1);
            UNIT_ASSERT(env.FreshReservePurposes.back() == TPurpose::User);

            const size_t hugeReservesBefore = env.HugeReservePurposes.size();
            const TActorId edge = env.Env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
            const TLogoBlobID id(1000, 1, 1, 0, 1_MB, 0, 1); // rewrite the seeded blob
            // Match TDefragRewriter: default USER data, IgnoreBlock and RewriteBlob set, sent to Skeleton.
            auto rewrite = std::make_unique<TEvBlobStorage::TEvVPut>(id, TRope(TString(1_MB, 'x')),
                env.Info->GetVDiskId(0), true, nullptr, TInstant::Max(), NKikimrBlobStorage::AsyncBlob,
                false, TWriteSource::DefragRewrite);
            rewrite->RewriteBlob = true;
            env.Env.Runtime->Send(new IEventHandle(env.Skeleton, edge, rewrite.release()), 1);
            const TDiskPart rewritten = env.ExpectPut(edge, NKikimrProto::OK);
            UNIT_ASSERT(rewritten.ChunkIdx);
            UNIT_ASSERT(rewritten != original);
            UNIT_ASSERT_VALUES_EQUAL(env.FreshReserves, freshBefore + 2);
            UNIT_ASSERT(env.FreshReservePurposes.back() == TPurpose::Maintenance);
            UNIT_ASSERT(env.HugeWritePurposes.back() == TPurpose::Maintenance);
            UNIT_ASSERT_VALUES_EQUAL(env.HugeReservePurposes.size(), hugeReservesBefore + lockChunk);
            if (lockChunk) {
                UNIT_ASSERT(rewritten.ChunkIdx != original.ChunkIdx);
                UNIT_ASSERT(env.HugeReservePurposes.back() == TPurpose::Maintenance);
            }
            env.Compact();
        }
    }

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

    Y_UNIT_TEST(ConcurrentHugeWritesReserveForSeparateFreshSegments) {
        THugeAdmissionEnv env;
        env.HoldHugeReserve = true;
        env.HoldHugeLogStep = 2;
        const TActorId first = env.SendPut();
        const TActorId second = env.SendPut();
        env.Env.Sim(TDuration::Seconds(1));
        UNIT_ASSERT(env.HeldReserve);
        UNIT_ASSERT_VALUES_EQUAL(env.HugeWrites, 2); // both admitted into an empty Cur

        env.HoldHugeReserve = false;
        env.Env.Runtime->Send(env.HeldReserve.release(), 1);
        env.ExpectPut(first, NKikimrProto::OK);
        env.Env.Sim(TDuration::Seconds(1));
        UNIT_ASSERT(env.HeldHugeLog); // second write still has no LSN
        const ui32 reserves = env.FreshReserves;
        env.Compact(); // the first index must compact without waiting for the second
        UNIT_ASSERT_VALUES_EQUAL(env.FreshReserves, reserves);

        // A pending rotation must not block new writes while the second huge write is delayed.
        env.ExpectPut(env.SendPut(TDataKind::USER, false, 100), NKikimrProto::OK);
        env.HoldHugeLogStep = 0;
        env.Env.Runtime->Send(env.HeldHugeLog.release(), 1);
        env.ExpectPut(second, NKikimrProto::OK);
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
                const auto purpose = kind == TDataKind::SYSTEM ? TPurpose::System : TPurpose::User;
                UNIT_ASSERT(env.FreshReservePurposes.front() == purpose);
                UNIT_ASSERT(env.HugeWritePurposes.front() == purpose);
                UNIT_ASSERT(env.HugeReservePurposes.front() == purpose);
                env.Compact();
            }
        }
    }
}
