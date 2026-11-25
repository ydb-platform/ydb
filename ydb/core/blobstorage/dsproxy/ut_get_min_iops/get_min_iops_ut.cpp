#include "../ut/dsproxy_vdisk_mock_ut.h"

#include <ydb/core/blobstorage/dsproxy/dsproxy_get_impl.h>
#include <ydb/core/blobstorage/dsproxy/dsproxy_strategy_get_min_iops_block.h>

namespace NKikimr {
namespace {

struct TReadScenario {
    TActorSystemStub ActorSystemStub;
    TGroupMock Group{0, TErasureType::Erasure4Plus2Block, 8, 1, 1};
    TIntrusivePtr<TBlobStorageGroupInfo> Info = Group.GetInfo();
    TIntrusivePtr<TGroupQueues> Queues = Group.MakeGroupQueues();
    TLogoBlobID Blob{1, 1, 1, 0, 4096, 0};
    TString Data = TString::Uninitialized(Blob.BlobSize());
    TArrayHolder<TEvBlobStorage::TEvGet::TQuery> Queries{new TEvBlobStorage::TEvGet::TQuery[1]};
    std::unique_ptr<TEvBlobStorage::TEvGet> Request;
    std::unique_ptr<TGetImpl> Impl;
    TLogContext LogCtx{NKikimrServices::BS_PROXY_GET, false};
    TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> Pending;
    TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> Puts;
    TAutoPtr<TEvBlobStorage::TEvGetResult> Result;

    TReadScenario() {
        for (size_t i = 0; i < Data.size(); ++i) {
            Data[i] = static_cast<char>(i * 37 + i / 257);
        }
        for (ui32 part = 1; part <= 6; ++part) {
            Group.PutPartAtSlot(Blob, part - 1, part, Data);
        }
        for (ui32 disk = 0; disk < 8; ++disk) {
            // Initially avoid the primary for part 3 and one handoff.
            Group.GetVDiskInSubgroup(Blob, disk).SetPredictedDelayNs(
                disk == 2 || disk == 6 ? 10'000'000 : 100'000);
        }
        Queries[0].Id = Blob;
        Queries[0].Shift = 0;
        Queries[0].Size = Blob.BlobSize();
        Request = std::make_unique<TEvBlobStorage::TEvGet>(
            Queries, 1, TInstant::Max(), NKikimrBlobStorage::AsyncRead, false, false);
        Impl = std::make_unique<TGetImpl>(Info, Queues, Request.get(), nullptr, TAccelerationParams{});
        Impl->GenerateInitialRequests(LogCtx, Pending);
    }

    ui32 Disk(const TEvBlobStorage::TEvVGet& get) const {
        return Info->GetIdxInSubgroup(VDiskIDFromVDiskID(get.Record.GetVDiskID()), Blob.Hash());
    }

    void Append(TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>>& gets) {
        while (!gets.empty()) {
            Pending.push_back(std::move(gets.front()));
            gets.pop_front();
        }
    }

    void Reply(ui32 disk) {
        auto it = std::find_if(Pending.begin(), Pending.end(), [&](const auto& get) {
            return Disk(*get) == disk;
        });
        UNIT_ASSERT_C(it != Pending.end(), "No pending request for subgroup disk " << disk);
        auto get = std::move(*it);
        Pending.erase(it);
        TEvBlobStorage::TEvVGetResult reply;
        Group.OnVGet(*get, reply);
        TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> next;
        Impl->OnVGetResult(LogCtx, reply, next, Puts, Result);
        Append(next);
    }
};

} // anonymous namespace

Y_UNIT_TEST_SUITE(TMinIopsBlockGet) {
    Y_UNIT_TEST(ReadParityWithOneErrorAndTwoOutstandingDisks) {
        TReadScenario s;
        UNIT_ASSERT_VALUES_EQUAL(s.Pending.size(), 4);
        // Parts 1, 2 and 4 arrive; the other handoff fails.
        for (ui32 disk : {0u, 1u, 3u}) {
            s.Reply(disk);
        }
        // Predictions may change while the initial requests are in flight.
        // The ordinary step keeps the direct path; acceleration below uses
        // the outstanding mask rather than these prediction values.
        for (ui32 disk : {2u, 6u}) {
            s.Group.GetVDiskInSubgroup(s.Blob, disk).SetPredictedDelayNs(100'000);
        }
        s.Group.GetVDiskInSubgroup(s.Blob, 7).SetError(NKikimrProto::ERROR);
        s.Reply(7);
        UNIT_ASSERT(!s.Result);
        UNIT_ASSERT_VALUES_EQUAL(s.Pending.size(), 2);
        ui32 outstanding = 0;
        for (const auto& get : s.Pending) {
            const ui32 disk = s.Disk(*get);
            UNIT_ASSERT(disk == 2 || disk == 6);
            // Earlier no-progress steps may also hedge parts 2 and 4 on the
            // handoff. Part 3 must still be outstanding on both sources.
            bool readsPart3 = false;
            for (const auto& query : get->Record.GetExtremeQueries()) {
                readsPart3 |= LogoBlobIDFromLogoBlobID(query.GetId()).PartId() == 3;
            }
            UNIT_ASSERT(readsPart3);
            outstanding |= 1u << s.Info->GetOrderNumber(s.Group.GetVDiskInSubgroup(s.Blob, disk).GetVDiskId());
        }

        // Excluding both outstanding disks exceeds the failure model together
        // with the real ERROR. It still leaves enough readable parts to recover.
        TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> accelerated;
        s.Impl->AccelerateGet(s.LogCtx, outstanding, accelerated, s.Puts);
        UNIT_ASSERT_VALUES_EQUAL(accelerated.size(), 2);
        ui32 paritySources = 0;
        for (const auto& get : accelerated) {
            paritySources |= 1u << s.Disk(*get);
            UNIT_ASSERT_VALUES_EQUAL(get->Record.ExtremeQueriesSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(LogoBlobIDFromLogoBlobID(get->Record.GetExtremeQueries(0).GetId()).PartId(), 5);
        }
        // Part 5 is still Unknown on the failed handoff: only data parts were
        // queried there before. Its extra parity probe can fail independently.
        UNIT_ASSERT_VALUES_EQUAL(paritySources, (1u << 4) | (1u << 7));
        s.Append(accelerated);
        s.Reply(7);
        UNIT_ASSERT(!s.Result);
        s.Reply(4);

        UNIT_ASSERT(s.Result);
        UNIT_ASSERT_VALUES_EQUAL(s.Result->Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(s.Result->Responses[0].Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(s.Result->Responses[0].Buffer.ConvertToString(), s.Data);
        // Neither original read of part 3 has been answered.
        UNIT_ASSERT_VALUES_EQUAL(s.Pending.size(), 2);
        UNIT_ASSERT(s.Puts.empty());
    }

    Y_UNIT_TEST(FallBackWhenFastPlanOnlyWaitsForAlreadyRequestedData) {
        TReadScenario s;
        TBlobState state;
        state.Init(s.Blob, *s.Info);
        state.AddNeeded(0, s.Blob.BlobSize());
        for (auto& disk : state.Disks) {
            for (auto& part : disk.DiskParts) {
                part.Situation = TBlobState::ESituation::Absent;
            }
        }
        for (ui32 part : {0u, 1u, 3u}) {
            state.Parts[part].Data.Write(0, TRope(TString(1024, 'x')));
            state.Disks[part].DiskParts[part].Situation = TBlobState::ESituation::Present;
        }
        // The fast main is already being read. A slow handoff can also be asked
        // for part 3, but that interval has not been requested there yet.
        state.Disks[2].DiskParts[2].Situation = TBlobState::ESituation::Unknown;
        state.Disks[2].DiskParts[2].Requested.Add(0, 1024);
        state.Disks[6].DiskParts[2].Situation = TBlobState::ESituation::Unknown;
        state.Disks[6].IsSlow = true;
        TBlackboard blackboard(s.Info, s.Queues, NKikimrBlobStorage::UserData, NKikimrBlobStorage::FastRead);
        blackboard.AccelerationMode = TBlackboard::AccelerationModeSkipMarked;
        TMinIopsBlockStrategy strategy;
        UNIT_ASSERT(strategy.Process(s.LogCtx, state, *s.Info, blackboard,
            blackboard.GroupDiskRequests, TAccelerationParams{}).Value == EStrategyOutcome::IN_PROGRESS);
        const auto& gets = blackboard.GroupDiskRequests.GetsPending;
        UNIT_ASSERT_VALUES_EQUAL(gets.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(gets.front().OrderNumber, state.Disks[6].OrderNumber);
        UNIT_ASSERT_VALUES_EQUAL(gets.front().Id.PartId(), 3);
        UNIT_ASSERT_VALUES_EQUAL(gets.front().Shift, 0);
        UNIT_ASSERT_VALUES_EQUAL(gets.front().Size, 1024);
    }

    Y_UNIT_TEST(RestorationNeedsEnoughDistinctPartsAtEveryOffset) {
        TReadScenario s;
        TMinIopsBlockStrategy strategy;
        for (ui32 split : {16u, 512u}) {
            TBlobState state;
            state.Init(s.Blob, *s.Info);
            state.AddNeeded(0, s.Blob.BlobSize());
            for (auto& disk : state.Disks) {
                for (auto& part : disk.DiskParts) {
                    part.Situation = TBlobState::ESituation::Absent;
                }
            }
            for (ui32 part : {0u, 1u, 2u}) {
                state.Parts[part].Data.Write(0, TRope(TString(1024, 'x')));
            }
            // Four distinct parts cover every byte, but the fourth part differs.
            // A split inside an erasure block cannot be used for restoration.
            state.Parts[3].Data.Write(0, TRope(TString(split, 'x')));
            state.Parts[4].Data.Write(split, TRope(TString(1024 - split, 'x')));
            state.Disks[0].IsSlow = true; // Already received bytes are still usable.
            UNIT_ASSERT_VALUES_EQUAL(strategy.IsRestorationPossibleWithoutSlowDisks(state, *s.Info), split == 512);
            state.Parts[4].Data = {};
            UNIT_ASSERT(!strategy.IsRestorationPossibleWithoutSlowDisks(state, *s.Info));
        }
    }

    Y_UNIT_TEST(RealDisintegratedLayoutStillReturnsError) {
        TReadScenario s;
        for (ui32 disk : {0u, 1u, 3u}) {
            s.Group.GetVDiskInSubgroup(s.Blob, disk).SetError(NKikimrProto::ERROR);
            s.Reply(disk);
        }
        UNIT_ASSERT(s.Result);
        UNIT_ASSERT_VALUES_EQUAL(s.Result->Status, NKikimrProto::ERROR);
        UNIT_ASSERT_VALUES_EQUAL(s.Result->Responses[0].Status, NKikimrProto::ERROR);
    }
}

} // NKikimr
