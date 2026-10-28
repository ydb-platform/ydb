#include "bridge_common.h"

#include <ydb/core/blobstorage/bridge/syncer/syncer.h>
#include <util/system/type_name.h>

namespace {

constexpr ui64 TabletId = 100500;
constexpr ui32 Generation = 10;

class TBlockSyncTest {
public:
    TEnvironmentSetup Env{{
        .NodeCount = 16,
        .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        .LocationGenerator = NBridgeTest::GetBridgedLocation,
        .SelfManagementConfig = true,
        .NumPiles = 2,
        .AutomaticBootstrap = true,
    }};
    TIntrusivePtr<TBlobStorageGroupInfo> Info;
    TIntrusivePtr<TBlobStorageGroupInfo> Source;
    TIntrusivePtr<TBlobStorageGroupInfo> Target;
    TIntrusivePtr<TBlobStorageGroupInfo> FaultyGroup;
    TActorId Syncer;
    TActorId Edge;
    TActorId BlockQueue;
    ui32 NumErrors = 0;
    bool HoldAssimilation = false;
    bool HoldBlocks = false;
    std::vector<std::pair<ui32, std::unique_ptr<IEventHandle>>> HeldAssimilation;
    std::vector<std::pair<ui32, std::unique_ptr<IEventHandle>>> HeldBlocks;
    std::unique_ptr<IEventHandle> Completion;

    TBlockSyncTest() {
        Env.CreatePool();
        auto group = NBridgeTest::GetGroupInfo(Env, Env.GetGroups().front());
        auto *state = group.MutableBridgeGroupState();
        UNIT_ASSERT_VALUES_EQUAL(state->PileSize(), 2);
        Source = Env.GetGroupInfo(state->GetPile(0).GetGroupId());
        Target = Env.GetGroupInfo(state->GetPile(1).GetGroupId());
        state->MutablePile(1)->SetStage(NKikimrBridge::TGroupState::BLOCKS);
        Info = TBlobStorageGroupInfo::Parse(group, nullptr, nullptr);
        UNIT_ASSERT(Info);
        Edge = Env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        BlockQueue = Env.CreateQueueActor(Target->GetVDiskId(0), NKikimrBlobStorage::PutTabletLog, 1000);
        Env.Sim(TDuration::Seconds(30));
        FaultyGroup = Target;

        Env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvVSync ||
                    ev->GetTypeRewrite() == TEvBlobStorage::EvVSyncFull) {
                return false;
            }
            if (ev->GetTypeRewrite() == NStorage::TEvNodeWardenNotifySyncerFinished::EventType &&
                    ev->Sender == Syncer) {
                UNIT_ASSERT(!Completion);
                Completion = std::move(ev);
                return false;
            }
            if (HoldAssimilation && ev->GetTypeRewrite() == TEvBlobStorage::EvAssimilateResult &&
                    ev->GetRecipientRewrite() == Syncer) {
                HeldAssimilation.emplace_back(nodeId, std::move(ev));
                return false;
            }
            if (HoldBlocks && ev->GetTypeRewrite() == TEvBlobStorage::EvVBlock) {
                auto& record = ev->Get<TEvBlobStorage::TEvVBlock>()->Record;
                const auto vdisk = VDiskIDFromVDiskID(record.GetVDiskID());
                const auto *recipient = Env.Runtime->GetActor(ev->GetRecipientRewrite());
                if (record.GetTabletId() == TabletId && vdisk.GroupID == FaultyGroup->GroupID &&
                        recipient && TypeName(*recipient).Contains("TSkeletonFront")) {
                    const ui32 order = FaultyGroup->GetOrderNumber(vdisk);
                    if (order && order <= NumErrors) {
                        auto result = std::make_unique<TEvBlobStorage::TEvVBlockResult>(NKikimrProto::ERROR,
                            nullptr, vdisk, Env.Runtime->GetClock(), ev->Get<TEvBlobStorage::TEvVBlock>()->GetCachedByteSize(),
                            &record, nullptr, nullptr, nullptr, 0);
                        const ui32 flags = IEventHandle::MakeFlags(result->GetChannelToSend(), 0);
                        Env.Runtime->Send(new IEventHandle(ev->Sender, ev->Recipient, result.release(), flags,
                            ev->Cookie), nodeId);
                        return false;
                    } else if (order > NumErrors) {
                        HeldBlocks.emplace_back(nodeId, std::move(ev));
                        return false;
                    }
                }
            }
            return true;
        };
    }

    ~TBlockSyncTest() {
        Env.Runtime->FilterFunction = {};
    }

    void Wait(const std::function<bool()>& condition) {
        const auto deadline = Env.Runtime->GetClock() + TDuration::Seconds(5);
        while (!condition() && Env.Runtime->GetClock() < deadline) {
            Env.Sim(TDuration::MilliSeconds(10));
        }
        UNIT_ASSERT_C(condition(), "Timed out waiting for syncer events");
    }

    void BlockGroup(const TIntrusivePtr<TBlobStorageGroupInfo>& info, bool isIdempotent = false,
            NKikimrProto::EReplyStatus expected = NKikimrProto::OK) {
        Env.Runtime->WrapInActorContext(Edge, [&] {
            auto request = std::make_unique<TEvBlobStorage::TEvBlock>(TabletId, Generation, TInstant::Max());
            request->IsIdempotent = isIdempotent;
            request->ForceGroupGeneration = info->GroupGeneration;
            SendToBSProxy(Edge, info->GroupID, request.release());
        });
        auto result = Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvBlockResult>(Edge, false,
            Env.Runtime->GetClock() + TDuration::Seconds(5));
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, expected);
        Env.Sim(TDuration::MilliSeconds(100));
    }

    void BlockReplica(const TIntrusivePtr<TBlobStorageGroupInfo>& info, ui32 generation = Generation) {
        const auto queue = info == Target ? BlockQueue
            : Env.CreateQueueActor(info->GetVDiskId(0), NKikimrBlobStorage::PutTabletLog, 1001);
        Env.Runtime->Send(new IEventHandle(queue, Edge, new TEvBlobStorage::TEvVBlock(TabletId, generation,
            info->GetVDiskId(0), TInstant::Max())), Edge.NodeId());
        auto result = Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVBlockResult>(Edge, false,
            Env.Runtime->GetClock() + TDuration::Seconds(5));
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetStatus(), NKikimrProto::OK);
    }

    void Start() {
        Completion.reset();
        Syncer = Env.Runtime->Register(NBridge::CreateSyncerActor(Info, Source->GroupID, Target->GroupID,
            std::make_shared<NBridge::TSyncerDataStats>(), nullptr, Source->Type), 1);
    }

    void CheckCompletion(bool success) {
        Wait([&] { return bool(Completion); });
        const auto& result = *Completion->Get<NStorage::TEvNodeWardenNotifySyncerFinished>();
        UNIT_ASSERT_VALUES_EQUAL_C(!result.ErrorReason, success, result.ErrorReason.value_or(""));
    }

    void Release(std::vector<std::pair<ui32, std::unique_ptr<IEventHandle>>>& held) {
        for (auto& [nodeId, ev] : std::exchange(held, {})) {
            Env.Runtime->Send(ev.release(), nodeId);
        }
    }
};

void CheckDelayedBlockReplyWithoutIncarnationGuid(NKikimrProto::EReplyStatus status) {
    TBlockSyncTest test;
    test.BlockGroup(test.Target);
    auto& runtime = test.Env.Runtime;
    std::vector<std::pair<ui32, std::unique_ptr<IEventHandle>>> replies(test.Target->GetTotalVDisksNum());
    ui32 numReplies = 0;
    bool holdReplies = true;
    bool captureResult = true;
    std::unique_ptr<IEventHandle> result;
    auto filter = std::move(runtime->FilterFunction);
    runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == TEvBlobStorage::EvVBlockResult) {
            auto& record = ev->Get<TEvBlobStorage::TEvVBlockResult>()->Record;
            const auto vdisk = VDiskIDFromVDiskID(record.GetVDiskID());
            const auto *recipient = runtime->GetActor(ev->GetRecipientRewrite());
            if (vdisk.GroupID == test.Target->GroupID && recipient &&
                    TypeName(*recipient).Contains("TBlobStorageGroupBlockRequest")) {
                const ui32 order = test.Target->GetOrderNumber(vdisk);
                if (order == 5) {
                    record.SetStatus(status);
                    record.ClearIncarnationGuid();
                }
                if (holdReplies) {
                    UNIT_ASSERT(!replies[order].second);
                    replies[order] = {nodeId, std::move(ev)};
                    ++numReplies;
                    return false;
                }
            }
        }
        if (captureResult && ev->GetRecipientRewrite() == test.Edge &&
                ev->GetTypeRewrite() == TEvBlobStorage::EvBlockResult) {
            UNIT_ASSERT(!result);
            result = std::move(ev);
            return false;
        }
        return filter(nodeId, ev);
    };
    runtime->WrapInActorContext(test.Edge, [&] {
        auto request = std::make_unique<TEvBlobStorage::TEvBlock>(TabletId, Generation, TInstant::Max());
        request->IsIdempotent = true;
        request->ForceGroupGeneration = test.Target->GroupGeneration;
        SendToBSProxy(test.Edge, test.Target->GroupID, request.release());
    });
    test.Wait([&] { return numReplies == replies.size(); });
    holdReplies = false;
    auto release = [&](ui32 order) {
        auto& [nodeId, ev] = replies[order];
        runtime->Send(ev.release(), nodeId);
    };
    for (ui32 order = 0; order < 5; ++order) {
        UNIT_ASSERT(replies[order].second->Get<TEvBlobStorage::TEvVBlockResult>()->Record.HasIncarnationGuid());
        release(order);
    }
    test.Env.Sim(VDiskCooldownTimeoutOnProxy + TDuration::Seconds(1));
    UNIT_ASSERT(!result);
    release(5);
    test.Env.Sim(TDuration::MilliSeconds(100));
    UNIT_ASSERT_C(!result, "A reply without GUID completed a quorum using expired confirmations");
    for (ui32 order = 6; order < replies.size(); ++order) {
        auto& record = replies[order].second->Get<TEvBlobStorage::TEvVBlockResult>()->Record;
        record.SetStatus(NKikimrProto::ERROR);
        record.ClearIncarnationGuid();
        release(order);
    }
    test.Wait([&] { return bool(result); });
    UNIT_ASSERT_VALUES_EQUAL(result->Get<TEvBlobStorage::TEvBlockResult>()->Status, NKikimrProto::ERROR);
    captureResult = false;
    test.BlockGroup(test.Target, true);
    runtime->FilterFunction = std::move(filter);
}

}

Y_UNIT_TEST_SUITE(BridgeBlockQuorum) {
    Y_UNIT_TEST(DelayedAlreadyWithoutIncarnationGuidCannotCompleteQuorum) {
        CheckDelayedBlockReplyWithoutIncarnationGuid(NKikimrProto::ALREADY);
    }

    Y_UNIT_TEST(DelayedOkWithoutIncarnationGuidCannotCompleteQuorum) {
        CheckDelayedBlockReplyWithoutIncarnationGuid(NKikimrProto::OK);
    }

    Y_UNIT_TEST(AlreadyAndTwoErrorsWaitForQuorum) {
        TBlockSyncTest test;
        test.BlockGroup(test.Source);
        test.HoldAssimilation = true;
        test.Start();
        test.Wait([&] { return test.HeldAssimilation.size() == 2; });
        test.BlockReplica(test.Target, Generation + 1);
        test.NumErrors = 2;
        test.HoldBlocks = true;
        test.HoldAssimilation = false;
        test.Release(test.HeldAssimilation);
        test.Wait([&] { return test.HeldBlocks.size() == 5; });
        test.Env.Sim(TDuration::Seconds(1));
        UNIT_ASSERT_C(!test.Completion, "BLOCKS completed before the block was installed on a quorum");
        test.HoldBlocks = false;
        test.Release(test.HeldBlocks);
        test.CheckCompletion(true);
    }

    Y_UNIT_TEST(RestartReconfirmsEqualAssimilatedGenerations) {
        TBlockSyncTest test;
        test.BlockGroup(test.Source);
        test.HoldAssimilation = true;
        test.Start();
        test.Wait([&] { return test.HeldAssimilation.size() == 2; });
        test.BlockReplica(test.Target);
        test.NumErrors = 3;
        test.HoldBlocks = true;
        test.HoldAssimilation = false;
        test.Release(test.HeldAssimilation);
        test.CheckCompletion(false);
        UNIT_ASSERT(!test.HeldBlocks.empty());
        test.Start();
        test.CheckCompletion(false);
        test.HoldBlocks = false;
        test.Release(test.HeldBlocks);
        test.Start();
        test.CheckCompletion(true);
    }

    Y_UNIT_TEST(SourceMaximumAlsoRequiresQuorum) {
        TBlockSyncTest test;
        test.BlockReplica(test.Source);
        test.FaultyGroup = test.Source;
        test.NumErrors = 3;
        test.HoldBlocks = true;
        test.Start();
        test.CheckCompletion(false);
        test.HoldBlocks = false;
        test.Release(test.HeldBlocks);
        test.Start();
        test.CheckCompletion(true);
    }

    Y_UNIT_TEST(AlreadyBlockedQuorumsAreIdempotent) {
        TBlockSyncTest test;
        test.BlockGroup(test.Source);
        test.BlockGroup(test.Target);
        test.BlockGroup(test.Source, false, NKikimrProto::ALREADY);
        test.BlockGroup(test.Info, true);
        test.Start();
        test.CheckCompletion(true);
        test.Start();
        test.CheckCompletion(true);
    }

    Y_UNIT_TEST(FreshBlockQuorum) {
        TBlockSyncTest test;
        test.BlockGroup(test.Source);
        test.Start();
        test.CheckCompletion(true);
    }
}
