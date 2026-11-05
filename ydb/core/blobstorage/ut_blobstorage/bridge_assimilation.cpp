#include "bridge_common.h"

#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/vdisk/huge/blobstorage_hullhuge.h>
#include <ydb/library/testlib/helpers.h>

namespace {

void AssimilationWaitsForLog(bool huge) {
    TEnvironmentSetup env{{.NodeCount = 1, .Erasure = TBlobStorageGroupType::ErasureNone}};
    env.CreateBoxAndPool(1, 1);
    env.Sim(TDuration::Seconds(30));
    auto& runtime = env.Runtime;
    const auto info = env.GetGroupInfo(env.GetGroups().front());
    const auto vdiskId = info->GetVDiskId(0);
    const auto queue = env.CreateQueueActor(vdiskId, NKikimrBlobStorage::PutTabletLog, 0, 1);
    const auto writer = runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
    const auto reader = runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
    const TString data(huge ? 1024 * 1024 : 100, 'x');
    const TLogoBlobID id(100500, 1, 1, 0, data.size(), 0, 1);
    std::vector<std::unique_ptr<IEventHandle>> delayed;
    bool hold = true;
    bool hugeDataWritten = false;
    std::unique_ptr<IEventHandle> putResult;
    std::unique_ptr<IEventHandle> assimilationResult;
    runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
        if (ev->GetRecipientRewrite() == writer) {
            putResult = std::move(ev);
            return false;
        }
        if (ev->GetRecipientRewrite() == reader) {
            assimilationResult = std::move(ev);
            return false;
        }
        if (ev->GetTypeRewrite() == TEvHullLogHugeBlob::EventType &&
                ev->Get<TEvHullLogHugeBlob>()->LogoBlobID == id) {
            hugeDataWritten = true;
        }
        if (hold && (!huge || hugeDataWritten) && ev->GetTypeRewrite() == NPDisk::TEvLogResult::EventType) {
            delayed.push_back(std::move(ev));
            return false;
        }
        return true;
    };
    runtime->Send(new IEventHandle(queue, writer, new TEvBlobStorage::TEvVPut(id, TRope(data), vdiskId,
        false, nullptr, TInstant::Max(), NKikimrBlobStorage::TabletLog, false)), writer.NodeId());
    env.Sim(TDuration::Seconds(1));
    UNIT_ASSERT(!huge || hugeDataWritten);
    UNIT_ASSERT(!delayed.empty());
    runtime->Send(new IEventHandle(info->GetActorId(0), reader, new TEvBlobStorage::TEvVAssimilate(
        vdiskId, std::nullopt, std::nullopt, std::nullopt, true, true)), reader.NodeId());
    env.Sim(TDuration::Seconds(1));
    hold = false;
    for (auto& ev : delayed) {
        runtime->Send(ev.release(), 1);
    }
    env.Sim(TDuration::Seconds(1));
    UNIT_ASSERT(putResult);
    UNIT_ASSERT(assimilationResult);
    UNIT_ASSERT_VALUES_EQUAL(putResult->Get<TEvBlobStorage::TEvVPutResult>()->Record.GetStatus(), NKikimrProto::OK);
    const auto& record = assimilationResult->Get<TEvBlobStorage::TEvVAssimilateResult>()->Record;
    UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrProto::OK);
    UNIT_ASSERT_VALUES_EQUAL(record.BlobsSize(), 1);
    const auto& blob = record.GetBlobs(0);
    const ui64 raw[] = {blob.GetRawX1(), blob.GetRawX2(), blob.GetRawX3()};
    UNIT_ASSERT_VALUES_EQUAL(TLogoBlobID(raw), id.FullID());
    runtime->FilterFunction = {};
}

void RunHugeDataWriteAcrossGeneration(bool changeGeneration) {
    TEnvironmentSetup env{{.NodeCount = 1, .Erasure = TBlobStorageGroupType::ErasureNone}};
    env.CreateBoxAndPool(1, 1);
    env.Sim(TDuration::Seconds(30));
    auto& runtime = env.Runtime;
    const auto info = env.GetGroupInfo(env.GetGroups().front());
    auto vdiskId = info->GetVDiskId(0);
    const auto queue = env.CreateQueueActor(vdiskId, NKikimrBlobStorage::PutTabletLog, 0, 1);
    const auto writer = runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
    const auto reader = runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
    const TString data(1024 * 1024, 'x');
    const TLogoBlobID id(100500, 1, 1, 0, data.size(), 0, 1);
    std::unique_ptr<IEventHandle> delayed;
    TActorId skeleton;
    std::optional<NKikimrProto::EReplyStatus> status;
    bool hold = true;
    runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
        if (hold && ev->GetTypeRewrite() == TEvHullLogHugeBlob::EventType &&
                ev->Get<TEvHullLogHugeBlob>()->LogoBlobID == id) {
            UNIT_ASSERT(!delayed);
            skeleton = ev->GetRecipientRewrite();
            delayed = std::move(ev);
            return false;
        }
        if (ev->GetTypeRewrite() == TEvBlobStorage::TEvVPutResult::EventType && ev->Sender == skeleton) {
            status = ev->Get<TEvBlobStorage::TEvVPutResult>()->Record.GetStatus();
        }
        return ev->GetRecipientRewrite() != writer;
    };
    runtime->Send(new IEventHandle(queue, writer, new TEvBlobStorage::TEvVPut(id, TRope(data), vdiskId,
        false, nullptr, TInstant::Max(), NKikimrBlobStorage::TabletLog, false)), writer.NodeId());
    env.Sim(TDuration::Seconds(1));
    UNIT_ASSERT(delayed);
    if (changeGeneration) {
        TBlobStorageGroupInfo::TTopology topology(info->Type);
        topology.FailRealms = info->GetTopology().FailRealms;
        TBlobStorageGroupInfo::TDynamicInfo dynamic(info->GroupID, info->GroupGeneration + 1);
        dynamic.ServiceIdForOrderNumber = info->GetDynamicInfo().ServiceIdForOrderNumber;
        const auto updated = MakeIntrusive<TBlobStorageGroupInfo>(std::move(topology), std::move(dynamic),
            TString(), Nothing(), info->GetDeviceType());
        vdiskId = updated->GetVDiskId(0);
        runtime->Send(new IEventHandle(info->GetActorId(0), {}, new TEvVGenerationChange(vdiskId, updated)), 1);
        env.Sim(TDuration::MilliSeconds(100));
    }
    // This is the snapshot used by the new-generation bridge syncer, while the old write has no LSN yet.
    runtime->Send(new IEventHandle(info->GetActorId(0), reader, new TEvBlobStorage::TEvVAssimilate(
        vdiskId, std::nullopt, std::nullopt, std::nullopt, true, true)), reader.NodeId());
    const auto snapshot = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVAssimilateResult>(reader);
    UNIT_ASSERT_VALUES_EQUAL(snapshot->Get()->Record.GetStatus(), NKikimrProto::OK);
    UNIT_ASSERT_VALUES_EQUAL(snapshot->Get()->Record.BlobsSize(), 0);
    hold = false;
    status.reset();
    runtime->Send(delayed.release(), 1);
    env.Sim(TDuration::Seconds(1));
    UNIT_ASSERT(status);
    UNIT_ASSERT_VALUES_EQUAL(*status, changeGeneration ? NKikimrProto::RACE : NKikimrProto::OK);
    runtime->FilterFunction = {};
}

} // namespace

Y_UNIT_TEST_SUITE(BridgeAssimilationPages) {
    Y_UNIT_TEST_TWIN(CommonFrontierPreservesContinuationAndMetadata, reverse) {
        using TResult = TEvBlobStorage::TEvAssimilateResult;
        TEnvironmentSetup env{{
            .NodeCount = 16,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = NBridgeTest::GetBridgedLocation,
            .SelfManagementConfig = true,
            .NumPiles = 2,
            .AutomaticBootstrap = true,
        }};
        const ui32 groupId = NBridgeTest::CreatePoolAndGetGroup(env);
        const auto pileGroups = NBridgeTest::GetPileGroups(env, groupId);
        std::array<TResult, 2> inventory{TResult(NKikimrProto::OK), TResult(NKikimrProto::OK)};
        inventory[0].Blocks = {{10, 1}, {30, 2}};
        inventory[1].Blocks = {{20, 3}, {30, 4}};
        inventory[0].Barriers = {{40, 0, {1, 1, 1, 5}, {2, 1, 1, 8}}, {60, 1, {1, 1, 1, 6}, {}}};
        inventory[1].Barriers = {{40, 0, {1, 2, 2, 5}, {1, 4, 1, 9}}, {50, 0, {1, 1, 1, 7}, {}}};
        const auto id = [](ui32 step) { return TLogoBlobID(100500, 1, step, 0, 10, 0); };
        inventory[0].Blobs = {{id(1), true, false}, {id(3), true, false}};
        inventory[1].Blobs = {{id(2), false, true}, {id(3), false, true}};
        ui32 requests = 0;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() != TEvBlobStorage::EvAssimilate) {
                return true;
            }
            for (ui32 pile : {0, 1}) {
                if (ev->Recipient != MakeBlobStorageProxyID(pileGroups[pile])) {
                    continue;
                }
                const auto *request = ev->Get<TEvBlobStorage::TEvAssimilate>();
                auto response = std::make_unique<TResult>(NKikimrProto::OK);
                // Uneven pages cross category boundaries, so concatenating their contents skips unseen items.
                size_t remaining = pile ? 3 : 1;
                const auto append = [&](const auto& items, auto& out, const auto& skip, auto keyOf) {
                    for (size_t i = 0; i < items.size() && remaining; ++i) {
                        const auto& item = items[reverse ? items.size() - i - 1 : i];
                        const auto key = keyOf(item);
                        if (!skip || (reverse ? key < *skip : *skip < key)) {
                            out.push_back(item);
                            --remaining;
                        }
                    }
                };
                append(inventory[pile].Blocks, response->Blocks, request->SkipBlocksUpTo,
                    [](const auto& item) { return item.TabletId; });
                append(inventory[pile].Barriers, response->Barriers, request->SkipBarriersUpTo,
                    [](const auto& item) { return std::tuple(item.TabletId, item.Channel); });
                append(inventory[pile].Blobs, response->Blobs, request->SkipBlobsUpTo,
                    [](const auto& item) { return item.Id; });
                env.Runtime->Send(new IEventHandle(ev->Sender, ev->Recipient, response.release(), 0, ev->Cookie), nodeId);
                ++requests;
                return false;
            }
            return true;
        };
        std::optional<ui64> skipBlocks;
        std::optional<std::tuple<ui64, ui8>> skipBarriers;
        std::optional<TLogoBlobID> skipBlobs;
        std::vector<TResult::TBlock> blocks;
        std::vector<TResult::TBarrier> barriers;
        std::vector<TResult::TBlob> blobs;
        const auto edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        bool finished = false;
        for (ui32 page = 0; page < 20 && !finished; ++page) {
            env.Runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvAssimilate(skipBlocks, skipBarriers, skipBlobs,
                    true, reverse));
            });
            const auto result = env.WaitForEdgeActorEvent<TResult>(edge, false);
            const auto *msg = result->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Status, NKikimrProto::OK);
            finished = msg->Blocks.empty() && msg->Barriers.empty() && msg->Blobs.empty();
            blocks.insert(blocks.end(), msg->Blocks.begin(), msg->Blocks.end());
            barriers.insert(barriers.end(), msg->Barriers.begin(), msg->Barriers.end());
            blobs.insert(blobs.end(), msg->Blobs.begin(), msg->Blobs.end());
            if (!msg->Blocks.empty()) {
                skipBlocks = msg->Blocks.back().TabletId;
            }
            if (!msg->Barriers.empty()) {
                skipBlocks = reverse ? 0 : Max<ui64>();
                skipBarriers = std::tuple(msg->Barriers.back().TabletId, msg->Barriers.back().Channel);
            }
            if (!msg->Blobs.empty()) {
                skipBlocks = reverse ? 0 : Max<ui64>();
                skipBarriers = reverse ? std::tuple<ui64, ui8>(0, 0) : std::tuple<ui64, ui8>(Max<ui64>(), Max<ui8>());
                skipBlobs = msg->Blobs.back().Id;
            }
        }
        env.Runtime->FilterFunction = {};
        UNIT_ASSERT(finished);
        UNIT_ASSERT(requests > 2);
        if (reverse) {
            std::reverse(blocks.begin(), blocks.end());
            std::reverse(barriers.begin(), barriers.end());
            std::reverse(blobs.begin(), blobs.end());
        }
        UNIT_ASSERT_VALUES_EQUAL(blocks.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(barriers.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(blobs.size(), 3);
        for (size_t i = 0; i < 3; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(blocks[i].TabletId, 10 * (i + 1));
            UNIT_ASSERT_VALUES_EQUAL(blocks[i].BlockedGeneration, i ? i + 2 : 1);
            UNIT_ASSERT_VALUES_EQUAL(barriers[i].TabletId, 40 + 10 * i);
            UNIT_ASSERT_VALUES_EQUAL(blobs[i].Id, id(i + 1));
        }
        UNIT_ASSERT_VALUES_EQUAL(barriers[0].Soft.RecordGeneration, 1);
        UNIT_ASSERT_VALUES_EQUAL(barriers[0].Soft.PerGenerationCounter, 2);
        UNIT_ASSERT_VALUES_EQUAL(barriers[0].Soft.CollectGeneration, 2);
        UNIT_ASSERT_VALUES_EQUAL(barriers[0].Hard.RecordGeneration, 2);
        UNIT_ASSERT_VALUES_EQUAL(barriers[0].Hard.CollectStep, 8);
        UNIT_ASSERT(blobs[0].Keep && !blobs[0].DoNotKeep);
        UNIT_ASSERT(!blobs[1].Keep && blobs[1].DoNotKeep);
        UNIT_ASSERT(blobs[2].Keep && blobs[2].DoNotKeep);
    }
}

Y_UNIT_TEST_SUITE(BridgeAssimilationFence) {
    Y_UNIT_TEST(HugeDataWriteAcrossGeneration) {
        RunHugeDataWriteAcrossGeneration(true);
    }
    Y_UNIT_TEST(HugeDataWriteSameGeneration) {
        RunHugeDataWriteAcrossGeneration(false);
    }
    Y_UNIT_TEST(SmallWriteInFlight) {
        AssimilationWaitsForLog(false);
    }
    Y_UNIT_TEST(HugeWriteLogInFlight) {
        AssimilationWaitsForLog(true);
    }
}
