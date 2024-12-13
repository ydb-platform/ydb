#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/vdisk_delay_emulator.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>

#include <util/stream/null.h>

#include "ut_helpers.h"

#define Ctest Cnull

Y_UNIT_TEST_SUITE(Deadlines) {

    struct TestCtx {
        TestCtx(const TBlobStorageGroupType& erasure, TDuration vdiskDelay)
            : NodeCount(erasure.BlobSubgroupSize() + 1)
            , Erasure(erasure)
            , VDiskDelay(vdiskDelay)
        {
            TFeatureFlags ff;
            ff.SetEnableVPatch(true);
            Env.reset(new TEnvironmentSetup(TEnvironmentSetup::TSettings{
                .NodeCount = NodeCount,
                .Erasure = erasure,
                .LocationGenerator = [this](ui32 nodeId) { return LocationGenerator(nodeId); },
                .FeatureFlags = std::move(ff),
            }));
            VDiskDelayEmulator.reset(new TVDiskDelayEmulator(Env));
        }
    
        TNodeLocation LocationGenerator(ui32 nodeId) {
            if (Erasure.BlobSubgroupSize() == 9) {
                if (nodeId == NodeCount) {
                    return TNodeLocation{"4", "1", "1", "1"};
                }
                return TNodeLocation{
                    std::to_string((nodeId - 1) / 3),
                    "1",
                    std::to_string((nodeId - 1) % 3),
                    "0"
                };
            } else {
                if (nodeId == NodeCount) {
                    return TNodeLocation{"2", "1", "1", "1"};
                }
                return TNodeLocation{"1", "1", std::to_string(nodeId), "0"};
            }
        }

        void Initialize() {
            Env->CreateBoxAndPool(1, 1);
            Env->Sim(TDuration::Minutes(1));

            NKikimrBlobStorage::TBaseConfig base = Env->FetchBaseConfig();
            UNIT_ASSERT_VALUES_EQUAL(base.GroupSize(), 1);
            const auto& group = base.GetGroup(0);
            GroupId = group.GetGroupId();

            Edge = Env->Runtime->AllocateEdgeActor(NodeCount);
            VDiskDelayEmulator->Edge = Edge;

            std::unordered_map<ui32, ui32> OrderNumberToNodeId;

            for (ui32 orderNum = 0; orderNum < group.VSlotIdSize(); ++orderNum) {
                OrderNumberToNodeId[orderNum] = group.GetVSlotId(orderNum).GetNodeId();
            }

            Env->Runtime->WrapInActorContext(Edge, [&] {
                SendToBSProxy(Edge, GroupId, new TEvBlobStorage::TEvStatus(TInstant::Max()));
            });
            auto res = Env->WaitForEdgeActorEvent<TEvBlobStorage::TEvStatusResult>(Edge, false, TInstant::Max());
            VDiskDelayEmulator->DefaultDelay = VDiskDelay;

            Env->Runtime->FilterFunction = TDelayFilterFunctor{ .VDiskDelayEmulator = VDiskDelayEmulator };

        }

        ~TestCtx() {
            Env->Runtime->FilterFunction = {};
        }

        ui32 NodeCount;
        TBlobStorageGroupType Erasure;
        std::shared_ptr<TEnvironmentSetup> Env;

        ui32 GroupId;
        TActorId Edge;
        TDuration VDiskDelay;
        std::shared_ptr<TVDiskDelayEmulator> VDiskDelayEmulator;
    };

    using TSendRequest = std::function<void(TestCtx& ctx)>;

    template <class TDelayedRequest, class TResultRequest>
    void TestImpl(const TBlobStorageGroupType& erasure, TDuration delay, TDuration timeout, TSendRequest sendCallback,
            ui32 requestsSent = 1) {
        Y_ABORT_UNLESS(timeout < delay);

        TestCtx ctx(erasure, delay);
        ctx.Initialize();

        TInstant now = TAppData::TimeProvider->Now();

        ctx.VDiskDelayEmulator->LogUnwrap = true;
        ctx.VDiskDelayEmulator->AddHandler(TDelayedRequest::EventType, [&](std::unique_ptr<IEventHandle>& ev) {
            ui32 nodeId = ev->Sender.NodeId();
            if (nodeId < ctx.NodeCount) {
                ctx.VDiskDelayEmulator->DelayMsg(ev);
                return false;
            }
            return true;
        });

        sendCallback(ctx);

        TInstant t1 = now + timeout / 2;            // Still waiting for vdisks
        TInstant t2 = now + (timeout + delay) / 2;  // Should hit deadline already

        {
            auto res = ctx.Env->WaitForEdgeActorEvent<TResultRequest>(ctx.Edge, false, t1);
            UNIT_ASSERT_C(!res, "Premature response, now# " << TAppData::TimeProvider->Now() << " res->ToString()# "
                    << res->Get()->ToString());
        }

        for (ui32 i = 0; i < requestsSent; ++i) {
            auto res = ctx.Env->WaitForEdgeActorEvent<TResultRequest>(ctx.Edge, false, t2);
            UNIT_ASSERT_C(res, "Recieved responses# " << i);
            UNIT_ASSERT(res->Get()->Status == NKikimrProto::DEADLINE);
        }
    }

    template <class TVResult>
    void TestPutImpl(const TBlobStorageGroupType& erasure, ui32 requests) {
        TDuration delay = TDuration::Seconds(50);
        TDuration timeout = TDuration::Seconds(40);

        auto send = [&](TestCtx& ctx) {
            TInstant now = TAppData::TimeProvider->Now();

            for (ui32 i = 0; i < requests; ++i) {
                TString data = MakeData(10);
                TLogoBlobID blobId(1, 1, 1, 1, data.size(), 123 + i);
                ctx.Env->Runtime->WrapInActorContext(ctx.Edge, [&] {
                        SendToBSProxy(ctx.Edge, ctx.GroupId, new TEvBlobStorage::TEvPut(blobId, data, now + timeout,
                                NKikimrBlobStorage::TabletLog));
                });
            }
        };

        TestImpl<TVResult, TEvBlobStorage::TEvPutResult>(erasure, delay, timeout, send, requests);
    }

    void TestGetImpl(const TBlobStorageGroupType& erasure, bool restoreGet, bool indexOnly) {
        TDuration delay = TDuration::Seconds(50);
        TDuration timeout = TDuration::Seconds(40);

        auto send = [&](TestCtx& ctx) {
            TInstant now = TAppData::TimeProvider->Now();
            TString data = MakeData(1000);
            TLogoBlobID blobId(1, 1, 1, 1, data.size(), 123);
            ctx.Env->Runtime->WrapInActorContext(ctx.Edge, [&] {
                SendToBSProxy(ctx.Edge, ctx.GroupId, new TEvBlobStorage::TEvPut(blobId, data, now + timeout,
                        NKikimrBlobStorage::TabletLog));
            });
            auto putRes = ctx.Env->WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(ctx.Edge, false, TInstant::Max());
            UNIT_ASSERT(putRes);
            UNIT_ASSERT_VALUES_EQUAL(putRes->Get()->Status, NKikimrProto::OK);

            ctx.Env->Runtime->WrapInActorContext(ctx.Edge, [&] {
                SendToBSProxy(ctx.Edge, ctx.GroupId, new TEvBlobStorage::TEvGet(blobId, 0, data.size(), now + timeout,
                    NKikimrBlobStorage::FastRead, restoreGet, indexOnly));
            });
        };

        TestImpl<TEvBlobStorage::TEvVGetResult, TEvBlobStorage::TEvGetResult>(erasure, delay, timeout, send);
    }

    void TestBlockImpl(const TBlobStorageGroupType& erasure) {
        TDuration delay = TDuration::Seconds(50);
        TDuration timeout = TDuration::Seconds(40);

        auto send = [&](TestCtx& ctx) {
            TInstant now = TAppData::TimeProvider->Now();
            ctx.Env->Runtime->WrapInActorContext(ctx.Edge, [&] {
                SendToBSProxy(ctx.Edge, ctx.GroupId, new TEvBlobStorage::TEvBlock(1, 2, now + timeout));
            });
        };

        TestImpl<TEvBlobStorage::TEvVBlockResult, TEvBlobStorage::TEvBlockResult>(erasure, delay, timeout, send);
    }

    void TestPatchImpl(const TBlobStorageGroupType& erasure) {
        TDuration delay = TDuration::Seconds(50);
        TDuration timeout = TDuration::Seconds(40);

        auto send = [&](TestCtx& ctx) {
            TString data = MakeData(1000);
            TLogoBlobID oldId(1, 1, 1, 1, data.size(), 123);
            ctx.Env->Runtime->WrapInActorContext(ctx.Edge, [&] {
                SendToBSProxy(ctx.Edge, ctx.GroupId, new TEvBlobStorage::TEvPut(oldId, data, TInstant::Max(),
                        NKikimrBlobStorage::TabletLog));
            });
            auto putRes = ctx.Env->WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(ctx.Edge, false, TInstant::Max());
            UNIT_ASSERT(putRes);
            UNIT_ASSERT_VALUES_EQUAL(putRes->Get()->Status, NKikimrProto::OK);

            TLogoBlobID newId(1, 1, 2, 1, data.size(), 123);
            ui32 blobIdMask = TLogoBlobID::MaxCookie & 0xfffff000;
            Y_ABORT_UNLESS(TEvBlobStorage::TEvPatch::GetBlobIdWithSamePlacement(oldId, &newId, blobIdMask,
                    ctx.GroupId, ctx.GroupId));
            TArrayHolder<TEvBlobStorage::TEvPatch::TDiff> diffs(new TEvBlobStorage::TEvPatch::TDiff[1]);
            diffs[0].Set(TString(data.size(), 'x'), 0);
            TInstant now = TAppData::TimeProvider->Now();
            ctx.Env->Runtime->WrapInActorContext(ctx.Edge, [&] {
                SendToBSProxy(ctx.Edge, ctx.GroupId, new TEvBlobStorage::TEvPatch(ctx.GroupId, oldId, newId, blobIdMask,
                        std::move(diffs), 1, now + timeout));
            });
        };

        TestImpl<TEvBlobStorage::TEvVMovedPatchResult, TEvBlobStorage::TEvPatchResult>(erasure, delay, timeout, send);
    }

    void TestDiscoverImpl(const TBlobStorageGroupType& erasure, bool readBody) {
        TDuration delay = TDuration::Seconds(50);
        TDuration timeout = TDuration::Seconds(40);

        auto send = [&](TestCtx& ctx) {
            TString data = MakeData(1000);
            ui64 tabletId = 100;
            TLogoBlobID blobId(tabletId, 2, 1, 1, data.size(), 123);
            ctx.Env->Runtime->WrapInActorContext(ctx.Edge, [&] {
                SendToBSProxy(ctx.Edge, ctx.GroupId, new TEvBlobStorage::TEvPut(blobId, data, TInstant::Max(),
                        NKikimrBlobStorage::TabletLog));
            });
            auto putRes = ctx.Env->WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(ctx.Edge, false, TInstant::Max());
            UNIT_ASSERT(putRes);
            UNIT_ASSERT_VALUES_EQUAL(putRes->Get()->Status, NKikimrProto::OK);

            TInstant now = TAppData::TimeProvider->Now();
            ctx.Env->Runtime->WrapInActorContext(ctx.Edge, [&] {
                SendToBSProxy(ctx.Edge, ctx.GroupId, new TEvBlobStorage::TEvDiscover(tabletId, 1, readBody, true,
                        now + timeout, 1, true));
            });
        };

        TestImpl<TEvBlobStorage::TEvVGetResult, TEvBlobStorage::TEvDiscoverResult>(erasure, delay, timeout, send);
    }

    void TestRangeImpl(const TBlobStorageGroupType& erasure, bool restore, bool indexOnly) {
        TDuration delay = TDuration::Seconds(50);
        TDuration timeout = TDuration::Seconds(40);

        auto send = [&](TestCtx& ctx) {
            ui32 tabletId = 100;
            for (ui32 cookie = 123; cookie < 150; ++cookie) {
                TString data = MakeData(1000);
                TLogoBlobID blobId(tabletId, 1, 1, 1, data.size(), cookie);
                ctx.Env->Runtime->WrapInActorContext(ctx.Edge, [&] {
                    SendToBSProxy(ctx.Edge, ctx.GroupId, new TEvBlobStorage::TEvPut(blobId, data, TInstant::Max(),
                            NKikimrBlobStorage::TabletLog));
                });
                auto putRes = ctx.Env->WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(ctx.Edge, false, TInstant::Max());
                UNIT_ASSERT(putRes);
                UNIT_ASSERT_VALUES_EQUAL(putRes->Get()->Status, NKikimrProto::OK);
            }

            TInstant now = TAppData::TimeProvider->Now();
            TLogoBlobID from(tabletId, 1, 1, 1, 1, 1);
            TLogoBlobID to(tabletId, 1, 1, 1, 9999, 9999);
            ctx.Env->Runtime->WrapInActorContext(ctx.Edge, [&] {
                SendToBSProxy(ctx.Edge, ctx.GroupId, new TEvBlobStorage::TEvRange(tabletId, from, to, restore, now + timeout,
                        indexOnly));
            });
        };

        TestImpl<TEvBlobStorage::TEvVGetResult, TEvBlobStorage::TEvRangeResult>(erasure, delay, timeout, send);
    }

    void TestCollectGarbageImpl(const TBlobStorageGroupType& erasure, bool multicollect, bool hard) {
        TDuration delay = TDuration::Seconds(50);
        TDuration timeout = TDuration::Seconds(40);

        std::unique_ptr<TVector<TLogoBlobID>> keep(new TVector<TLogoBlobID>);

        auto send = [&](TestCtx& ctx) {
            ui32 tabletId = 100;
            ui32 generation = 10;
            ui32 channel = 1;
            ui32 steps = 10'000;
            for (ui32 step = 1; step < steps; ++step) {
                TString data = MakeData(1000);
                TLogoBlobID blobId(tabletId, generation, step, channel, data.size(), 123);
                if (step % 2 == 0) {
                    keep->push_back(blobId);
                }
                ctx.Env->Runtime->WrapInActorContext(ctx.Edge, [&] {
                    SendToBSProxy(ctx.Edge, ctx.GroupId, new TEvBlobStorage::TEvPut(blobId, data, TInstant::Max(),
                            NKikimrBlobStorage::TabletLog));
                });
                auto putRes = ctx.Env->WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(ctx.Edge, false, TInstant::Max());
                UNIT_ASSERT(putRes);
                UNIT_ASSERT_VALUES_EQUAL(putRes->Get()->Status, NKikimrProto::OK);
            }

            TInstant now = TAppData::TimeProvider->Now();
            ctx.Env->Runtime->WrapInActorContext(ctx.Edge, [&] {
                SendToBSProxy(ctx.Edge, ctx.GroupId, new TEvBlobStorage::TEvCollectGarbage(tabletId, generation, 1, channel, 
                        true, generation, steps / 2, keep.release(), nullptr, now + timeout, multicollect, hard));
            });
        };

        TestImpl<TEvBlobStorage::TEvVCollectGarbageResult, TEvBlobStorage::TEvCollectGarbageResult>(erasure, delay, timeout, send);
    }

    void TestStatusImpl(const TBlobStorageGroupType& erasure) {
        TDuration delay = TDuration::Seconds(50);
        TDuration timeout = TDuration::Seconds(40);

        auto send = [&](TestCtx& ctx) {
            TInstant now = TAppData::TimeProvider->Now();
            ctx.Env->Runtime->WrapInActorContext(ctx.Edge, [&] {
                SendToBSProxy(ctx.Edge, ctx.GroupId, new TEvBlobStorage::TEvStatus(now + timeout));
            });
        };

        TestImpl<TEvBlobStorage::TEvVStatusResult, TEvBlobStorage::TEvStatusResult>(erasure, delay, timeout, send);
    }

    void TestPut(const TBlobStorageGroupType& erasure) {
        TestPutImpl<TEvBlobStorage::TEvVPutResult>(erasure, 1);
    }

    void TestMultiPut(const TBlobStorageGroupType& erasure) {
        TestPutImpl<TEvBlobStorage::TEvVMultiPutResult>(erasure, 10);
    }

    void TestGet(const TBlobStorageGroupType& erasure) {
        TestGetImpl(erasure, false, false);
    }

    void TestRestoreGet(const TBlobStorageGroupType& erasure) {
        TestGetImpl(erasure, true, false);
    }

    void TestIndexGet(const TBlobStorageGroupType& erasure) {
        TestGetImpl(erasure, false, true);
    }

    void TestIndexRestoreGet(const TBlobStorageGroupType& erasure) {
        TestGetImpl(erasure, true, true);
    }

    void TestBlock(const TBlobStorageGroupType& erasure) {
        TestBlockImpl(erasure);
    }

    void TestPatch(const TBlobStorageGroupType& erasure) {
        TestPatchImpl(erasure);
    }

    void TestDiscover(const TBlobStorageGroupType& erasure) {
        TestDiscoverImpl(erasure, false);
    }

    void TestDiscoverReadBody(const TBlobStorageGroupType& erasure) {
        TestDiscoverImpl(erasure, true);
    }

    void TestRange(const TBlobStorageGroupType& erasure) {
        TestRangeImpl(erasure, false, false);
    }

    void TestRestoreRange(const TBlobStorageGroupType& erasure) {
        TestRangeImpl(erasure, true, false);
    }

    void TestIndexRange(const TBlobStorageGroupType& erasure) {
        TestRangeImpl(erasure, false, true);
    }

    void TestIndexRestoreRange(const TBlobStorageGroupType& erasure) {
        TestRangeImpl(erasure, true, true);
    }

    void TestHardCollectGarbage(const TBlobStorageGroupType& erasure) {
        TestCollectGarbageImpl(erasure, false, true);
    }

    void TestSoftCollectGarbage(const TBlobStorageGroupType& erasure) {
        TestCollectGarbageImpl(erasure, false, false);
    }

    void TestHardMultiCollectGarbage(const TBlobStorageGroupType& erasure) {
        TestCollectGarbageImpl(erasure, false, true);
    }

    void TestSoftMultiCollectGarbage(const TBlobStorageGroupType& erasure) {
        TestCollectGarbageImpl(erasure, false, false);
    }

    void TestStatus(const TBlobStorageGroupType& erasure) {
        TestStatusImpl(erasure);
    }

    #define TEST_DEADLINE(method, erasure)                      \
    Y_UNIT_TEST(Test##method##erasure) {                        \
        Test##method(TBlobStorageGroupType::Erasure##erasure);  \
    }

    TEST_DEADLINE(Put, Mirror3dc);
    TEST_DEADLINE(Put, 4Plus2Block);
    TEST_DEADLINE(Put, Mirror3of4);

    TEST_DEADLINE(MultiPut, Mirror3dc);
    TEST_DEADLINE(MultiPut, 4Plus2Block);
    TEST_DEADLINE(MultiPut, Mirror3of4);

    TEST_DEADLINE(Get, Mirror3dc);
    TEST_DEADLINE(Get, 4Plus2Block);
    TEST_DEADLINE(Get, Mirror3of4);

    TEST_DEADLINE(RestoreGet, Mirror3dc);
    TEST_DEADLINE(RestoreGet, 4Plus2Block);
    TEST_DEADLINE(RestoreGet, Mirror3of4);

    TEST_DEADLINE(IndexGet, Mirror3dc);
    TEST_DEADLINE(IndexGet, 4Plus2Block);
    TEST_DEADLINE(IndexGet, Mirror3of4);

    TEST_DEADLINE(IndexRestoreGet, Mirror3dc);
    TEST_DEADLINE(IndexRestoreGet, 4Plus2Block);
    TEST_DEADLINE(IndexRestoreGet, Mirror3of4);

    TEST_DEADLINE(Block, Mirror3dc);
    TEST_DEADLINE(Block, 4Plus2Block);
    TEST_DEADLINE(Block, Mirror3of4);

    TEST_DEADLINE(Patch, Mirror3dc);
    TEST_DEADLINE(Patch, 4Plus2Block);
    TEST_DEADLINE(Patch, Mirror3of4);

    TEST_DEADLINE(Discover, Mirror3dc);
    TEST_DEADLINE(Discover, 4Plus2Block);
    TEST_DEADLINE(Discover, Mirror3of4);

    TEST_DEADLINE(DiscoverReadBody, Mirror3dc);
    TEST_DEADLINE(DiscoverReadBody, 4Plus2Block);
    TEST_DEADLINE(DiscoverReadBody, Mirror3of4);

    TEST_DEADLINE(Range, Mirror3dc);
    TEST_DEADLINE(Range, 4Plus2Block);
    TEST_DEADLINE(Range, Mirror3of4);

    TEST_DEADLINE(RestoreRange, Mirror3dc);
    TEST_DEADLINE(RestoreRange, 4Plus2Block);
    TEST_DEADLINE(RestoreRange, Mirror3of4);

    TEST_DEADLINE(IndexRange, Mirror3dc);
    TEST_DEADLINE(IndexRange, 4Plus2Block);
    TEST_DEADLINE(IndexRange, Mirror3of4);

    TEST_DEADLINE(IndexRestoreRange, Mirror3dc);
    TEST_DEADLINE(IndexRestoreRange, 4Plus2Block);
    TEST_DEADLINE(IndexRestoreRange, Mirror3of4);

    TEST_DEADLINE(HardCollectGarbage, Mirror3dc);
    TEST_DEADLINE(HardCollectGarbage, 4Plus2Block);
    TEST_DEADLINE(HardCollectGarbage, Mirror3of4);

    TEST_DEADLINE(SoftCollectGarbage, Mirror3dc);
    TEST_DEADLINE(SoftCollectGarbage, 4Plus2Block);
    TEST_DEADLINE(SoftCollectGarbage, Mirror3of4);

    TEST_DEADLINE(HardMultiCollectGarbage, Mirror3dc);
    TEST_DEADLINE(HardMultiCollectGarbage, 4Plus2Block);
    TEST_DEADLINE(HardMultiCollectGarbage, Mirror3of4);

    TEST_DEADLINE(SoftMultiCollectGarbage, Mirror3dc);
    TEST_DEADLINE(SoftMultiCollectGarbage, 4Plus2Block);
    TEST_DEADLINE(SoftMultiCollectGarbage, Mirror3of4);

    TEST_DEADLINE(Status, Mirror3dc);
    TEST_DEADLINE(Status, 4Plus2Block);
    TEST_DEADLINE(Status, Mirror3of4);

    #undef TEST_DEADLINE
}
