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
                case TEvBlobStorage::EvHullWriteHugeBlob: {
                    Skeleton = ev->Sender;
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
                    // Before the first huge write, the only client reservation
                    // is its Fresh index. Afterwards Skeleton is known explicitly.
                    const bool fresh = !Skeleton || ev->Sender == Skeleton;
                    if (fresh) {
                        ++FreshReserves;
                        if (RejectFresh || RejectFurtherFresh) {
                            Reject(std::move(ev), nodeId);
                            return false;
                        }
                    } else {
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

    TActorId SendPut(TDataKind::E kind = TDataKind::USER, bool unavoidable = false) {
        const TActorId edge = Env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        const TString data(1_MB, 'x');
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
        const size_t requests = env.HugeReserveBounds.size();
        for (ui32 i = 0; i < 8; ++i) {
            env.ExpectPut(env.SendPut(), NKikimrProto::OK);
        }
        UNIT_ASSERT_VALUES_EQUAL(env.FreshReserves, 1);
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
