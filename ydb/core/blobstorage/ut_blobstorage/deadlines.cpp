#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/vdisk_delay_emulator.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>

#include <util/stream/null.h>

#include <ydb/core/blobstorage/ut_blobstorage/lib/ut_helpers.h>

#define Ctest Cnull

Y_UNIT_TEST_SUITE(Deadlines) {

    struct TestCtx {
        TestCtx(const TBlobStorageGroupType& erasure, TDuration vdiskDelay)
            : NodeCount(erasure.BlobSubgroupSize() + 1)
            , Erasure(erasure)
            , Env(new TEnvironmentSetup({
                .NodeCount = NodeCount,
                .Erasure = erasure,
                .LocationGenerator = [this](ui32 nodeId) { return LocationGenerator(nodeId); },
            }))
            , VDiskDelay(vdiskDelay)
            , VDiskDelayEmulator(new TVDiskDelayEmulator(Env))
        {}
    
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

    void TestPut(const TBlobStorageGroupType& erasure, TDuration delay, TDuration timeout) {
        Y_ABORT_UNLESS(timeout < delay);

        TestCtx ctx(erasure, delay);
        ctx.Initialize();

        TInstant now = TAppData::TimeProvider->Now();

        ctx.VDiskDelayEmulator->LogUnwrap = true;
        ctx.VDiskDelayEmulator->AddHandler(TEvBlobStorage::TEvVPutResult::EventType, [&](std::unique_ptr<IEventHandle>& ev) {
            ui32 nodeId = ev->Sender.NodeId();
            if (nodeId < ctx.NodeCount) {
                ctx.VDiskDelayEmulator->DelayMsg(ev);
                return false;
            }
            return true;
        });

        ctx.Env->Runtime->WrapInActorContext(ctx.Edge, [&] {
            TString data = MakeData(1000);
            TLogoBlobID blobId(1, 1, 1, 1, data.size(), 123);
            TEvBlobStorage::TEvPut* ev = new TEvBlobStorage::TEvPut(blobId, data, now + timeout);
            SendToBSProxy(ctx.Edge, ctx.GroupId, ev, NKikimrBlobStorage::TabletLog);
        });

        TInstant t1 = now + timeout / 2;            // Still waiting for vdisks
        TInstant t2 = now + (timeout + delay) / 2;  // Should hit deadline already

        {
            auto res = ctx.Env->WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(
                    ctx.Edge, false, t1);
            UNIT_ASSERT_C(!res, "Premature response, now# " << TAppData::TimeProvider->Now());
        }

        {
            auto res = ctx.Env->WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(
                    ctx.Edge, false, t2);
            UNIT_ASSERT(res);
            UNIT_ASSERT(res->Get()->Status == NKikimrProto::DEADLINE);
        }
    }

    void TestGet(const TBlobStorageGroupType& erasure, TDuration delay, TDuration timeout) {
        Y_ABORT_UNLESS(timeout < delay);

        TestCtx ctx(erasure, delay);
        ctx.Initialize();

        TInstant now = TAppData::TimeProvider->Now();

        ctx.VDiskDelayEmulator->LogUnwrap = true;
        ctx.VDiskDelayEmulator->AddHandler(TEvBlobStorage::TEvVGetResult::EventType, [&](std::unique_ptr<IEventHandle>& ev) {
            ui32 nodeId = ev->Sender.NodeId();
            if (nodeId < ctx.NodeCount) {
                ctx.VDiskDelayEmulator->DelayMsg(ev);
                return false;
            }
            return true;
        });

        {
            TString data = MakeData(1000);
            TLogoBlobID blobId(1, 1, 1, 1, data.size(), 123);
            ctx.Env->Runtime->WrapInActorContext(ctx.Edge, [&] {
                TEvBlobStorage::TEvPut* ev = new TEvBlobStorage::TEvPut(blobId, data, now + timeout);
                SendToBSProxy(ctx.Edge, ctx.GroupId, ev, NKikimrBlobStorage::TabletLog);
            });
            auto putRes = ctx.Env->WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(ctx.Edge, false, TInstant::Max());
            UNIT_ASSERT_VALUES_EQUAL(putRes->Get()->Status, NKikimrProto::OK);

            ctx.Env->Runtime->WrapInActorContext(ctx.Edge, [&] {
                SendToBSProxy(ctx.Edge, ctx.GroupId, new TEvBlobStorage::TEvGet(blobId, 0, data.size(), now + timeout,
                    NKikimrBlobStorage::FastRead));
            });
        }

        TInstant t1 = now + timeout / 2;            // Still waiting for vdisks
        TInstant t2 = now + (timeout + delay) / 2;  // Should hit deadline already
        // TInstant t2 = now + delay + TDuration::Seconds(1);

        {
            auto res = ctx.Env->WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(
                    ctx.Edge, false, t1);
            UNIT_ASSERT_C(!res, "Premature response, now# " << TAppData::TimeProvider->Now());
        }

        {
            auto res = ctx.Env->WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(
                    ctx.Edge, false, t2);
            UNIT_ASSERT(res);
            UNIT_ASSERT(res->Get()->Status == NKikimrProto::DEADLINE);
        }
    }

    #define TEST_DEADLINE(method, erasure)                      \
    Y_UNIT_TEST(Test##method##erasure) {                        \
        Test##method(TBlobStorageGroupType::Erasure##erasure,   \
            TDuration::Seconds(50), TDuration::Seconds(40));    \
    }

    TEST_DEADLINE(Put, Mirror3dc);
    TEST_DEADLINE(Put, 4Plus2Block);
    TEST_DEADLINE(Put, Mirror3of4);

    TEST_DEADLINE(Get, Mirror3dc);
    TEST_DEADLINE(Get, 4Plus2Block);
    TEST_DEADLINE(Get, Mirror3of4);

    #undef TEST_DEADLINE
}
