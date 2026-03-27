#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/ut_helpers.h>

Y_UNIT_TEST_SUITE(RequestValidation) {
    struct TestCtx {
        void Initialize() {
            TFeatureFlags ff;
            ff.SetEnableVPatch(true);
            Env.reset(new TEnvironmentSetup(TEnvironmentSetup::TSettings{
                .FeatureFlags = std::move(ff),
            }));

            Env->CreateBoxAndPool(1, 1);
            Env->Sim(TDuration::Minutes(1));

            auto groups = Env->GetGroups();
            auto groupInfo = Env->GetGroupInfo(groups.front());
            GroupId = groupInfo->GroupID;

            Edge = Env->Runtime->AllocateEdgeActor(1);
            VDiskId = groupInfo->GetVDiskId(0);
            VDiskActorId = groupInfo->GetActorId(0);
        }

        std::shared_ptr<TEnvironmentSetup> Env;

        TGroupId GroupId;
        TActorId Edge;
        TVDiskID VDiskId;
        TActorId VDiskActorId;
    };

    void TestBlobSize(ui32 putsCount, ui32 blobSize = 0) {
        TestCtx ctx;
        ctx.Initialize();

        for (ui32 i = 0; i < putsCount; ++i) {
            TLogoBlobID blobId(100, 1, 1, 0, blobSize, i);
            TString data;

            std::unique_ptr<IEventBase> ev = std::make_unique<TEvBlobStorage::TEvPut>(blobId, data, TInstant::Max());
            ctx.Env->Runtime->WrapInActorContext(ctx.Edge, [&] {
                SendToBSProxy(ctx.Edge, ctx.GroupId.GetRawId(), ev.release());
            });
        }

        for (ui32 i = 0; i < putsCount; ++i) {
            auto res = ctx.Env->WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(ctx.Edge, false, TInstant::Max());
            UNIT_ASSERT(res);
            UNIT_ASSERT(res->Get()->Status == NKikimrProto::ERROR);
        }
    }

    Y_UNIT_TEST(TestSinglePutBlobSize0) {
        TestBlobSize(1);
    }

    Y_UNIT_TEST(TestMultiPutBlobSize0) {
        TestBlobSize(10);
    }

    Y_UNIT_TEST(TestVPutBlobSize0) {
        TestCtx ctx;
        ctx.Initialize();

        TLogoBlobID partId(100, 1, 1, 0, /*blobSize=*/0, 0, /*partId=*/1);
        TString data;
        auto ev = std::make_unique<TEvBlobStorage::TEvVPut>(partId, TRope(data), ctx.VDiskId, false, nullptr, TInstant::Max(),
            NKikimrBlobStorage::EPutHandleClass::TabletLog, false);
        ctx.Env->Runtime->Send(new IEventHandle(ctx.VDiskActorId, ctx.Edge, ev.release()), ctx.VDiskActorId.NodeId());

        auto res = ctx.Env->WaitForEdgeActorEvent<TEvBlobStorage::TEvVPutResult>(ctx.Edge, false, TInstant::Max());
        UNIT_ASSERT(res);
        UNIT_ASSERT(res->Get()->Record.GetStatus() == NKikimrProto::ERROR);
    }

    Y_UNIT_TEST(TestVMultiPutBlobSize0) {
        TestCtx ctx;
        ctx.Initialize();
        auto ev = std::make_unique<TEvBlobStorage::TEvVMultiPut>(ctx.VDiskId, TInstant::Max(),
                NKikimrBlobStorage::EPutHandleClass::TabletLog, false);
        ui32 vputsCount = 10;

        for (ui32 i = 0; i < vputsCount; ++i) {
            TLogoBlobID partId(100, 1, 1, 0, /*blobSize=*/0, i, /*partId=*/1);
            TString data;
            ev->AddVPut(partId, TRcBuf(data), nullptr, false, false, nullptr, {}, false);
        }
        ctx.Env->Runtime->Send(new IEventHandle(ctx.VDiskActorId, ctx.Edge, ev.release()), ctx.VDiskActorId.NodeId());

        auto res = ctx.Env->WaitForEdgeActorEvent<TEvBlobStorage::TEvVMultiPutResult>(ctx.Edge, false, TInstant::Max());
        UNIT_ASSERT(res);
        UNIT_ASSERT(res->Get()->Record.GetStatus() == NKikimrProto::OK);
        for (ui32 i = 0; i < vputsCount; ++i) {
            UNIT_ASSERT(res->Get()->Record.GetItems(i).GetStatus() == NKikimrProto::ERROR);
        }
    }

    Y_UNIT_TEST(TestVMovedPatchSize0) {
        TestCtx ctx;
        ctx.Initialize();
        TLogoBlobID oldId(100, 1, 1, 0, /*blobSize=*/100, 0, /*partId=*/1);
        TLogoBlobID newId(100, 1, 1, 0, /*blobSize=*/0, 1, /*partId=*/1);

        auto ev = std::make_unique<TEvBlobStorage::TEvVMovedPatch>(ctx.GroupId.GetRawId(), ctx.GroupId.GetRawId(),
                oldId, newId, ctx.VDiskId, false, 0, TInstant::Max());

        ctx.Env->Runtime->Send(new IEventHandle(ctx.VDiskActorId, ctx.Edge, ev.release()), ctx.VDiskActorId.NodeId());

        auto res = ctx.Env->WaitForEdgeActorEvent<TEvBlobStorage::TEvVMovedPatchResult>(ctx.Edge, false, TInstant::Max());
        UNIT_ASSERT(res);
        UNIT_ASSERT(res->Get()->Record.GetStatus() == NKikimrProto::ERROR);
    }

    void TestSameId(bool changeData) {
        TestCtx ctx;
        ctx.Initialize();

        const ui32 blobSize = 10;
        const ui32 putsCount = 10;
        TLogoBlobID blobId(100, 1, 1, 0, blobSize, 0);
        for (ui32 i = 0; i < putsCount; ++i) {
            TString data = MakeData(blobSize, i * changeData);

            std::unique_ptr<IEventBase> ev = std::make_unique<TEvBlobStorage::TEvPut>(blobId, data, TInstant::Max());
            ctx.Env->Runtime->WrapInActorContext(ctx.Edge, [&] {
                SendToBSProxy(ctx.Edge, ctx.GroupId.GetRawId(), ev.release());
            });
        }

        for (ui32 i = 0; i < putsCount; ++i) {
            auto res = ctx.Env->WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(ctx.Edge, false, TInstant::Max());
            UNIT_ASSERT(res);
        }
    }

    Y_UNIT_TEST(TestMultiputSameId) {
        TestSameId(true);
    }

    Y_UNIT_TEST(TestMultiputSameIdSameData) {
        TestSameId(false);
    }
}
