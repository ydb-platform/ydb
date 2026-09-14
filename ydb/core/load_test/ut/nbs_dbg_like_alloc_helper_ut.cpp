#include <ydb/core/load_test/nbs_dbg_like_alloc_helper.h>

#include <ydb/core/protos/blobstorage.pb.h>
#include <ydb/core/protos/load_test.pb.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NKikimr::NNbsDbgLike;

namespace {

NKikimrBlobStorage::TEvControllerAllocateDDiskBlockGroupResult MakeOkReply(
    ui64 baseDbgId, ui32 nDbgs)
{
    NKikimrBlobStorage::TEvControllerAllocateDDiskBlockGroupResult rec;
    rec.SetStatus(NKikimrProto::OK);
    for (ui32 i = 0; i < nDbgs; ++i) {
        auto* resp = rec.AddResponses();
        resp->SetDirectBlockGroupId(baseDbgId + i);
        for (ui32 k = 0; k < 5; ++k) {
            auto* node = resp->AddNodes();
            auto* dd = node->MutableDDiskId();
            dd->SetNodeId(100 + i);
            dd->SetPDiskId(10 + k);
            dd->SetDDiskSlotId(1000 + i * 5 + k);
            auto* pb = node->MutablePersistentBufferDDiskId();
            pb->SetNodeId(200 + i);
            pb->SetPDiskId(20 + k);
            pb->SetDDiskSlotId(2000 + i * 5 + k);
        }
    }
    return rec;
}

TEvLoadTestRequest::TNbsDbgLikeLoad::TAllocConfig MakeCfg(ui32 nDbgs)
{
    TEvLoadTestRequest::TNbsDbgLikeLoad::TAllocConfig cfg;
    cfg.SetTabletId(0x4242);
    cfg.SetNumDirectBlockGroups(nDbgs);
    cfg.SetTargetNumVChunks(7);
    cfg.SetVChunkSizeBytes(4 * 1024 * 1024);
    cfg.SetDDiskPoolName("dd-pool");
    cfg.SetPersistentBufferDDiskPoolName("pb-pool");
    return cfg;
}

}  // namespace

Y_UNIT_TEST_SUITE(NbsDbgLikeAllocHelper) {
    Y_UNIT_TEST(BuildRequest_FillsQueriesAndPools) {
        const ui32 n = 3;
        auto cfg = MakeCfg(n);

        NKikimrBlobStorage::TEvControllerAllocateDDiskBlockGroup rec;
        BuildAllocateRequest(rec, cfg, /*dealloc=*/false);

        UNIT_ASSERT_VALUES_EQUAL(rec.GetTabletId(), 0x4242u);
        UNIT_ASSERT_VALUES_EQUAL(rec.GetDDiskPoolName(), "dd-pool");
        UNIT_ASSERT_VALUES_EQUAL(rec.GetPersistentBufferDDiskPoolName(), "pb-pool");
        UNIT_ASSERT_VALUES_EQUAL(rec.QueriesSize(), n);
        for (ui32 i = 0; i < n; ++i) {
            const auto& q = rec.GetQueries(i);
            UNIT_ASSERT_VALUES_EQUAL(q.GetDirectBlockGroupId(), static_cast<ui64>(i));
            UNIT_ASSERT_VALUES_EQUAL(q.GetTargetNumVChunks(), 7u);
        }
    }

    Y_UNIT_TEST(BuildRequest_DeallocSetsZeroVChunks) {
        auto cfg = MakeCfg(2);

        NKikimrBlobStorage::TEvControllerAllocateDDiskBlockGroup rec;
        BuildAllocateRequest(rec, cfg, /*dealloc=*/true);

        UNIT_ASSERT_VALUES_EQUAL(rec.QueriesSize(), 2u);
        for (size_t i = 0; i < rec.QueriesSize(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(rec.GetQueries(i).GetTargetNumVChunks(), 0u);
        }
    }

    Y_UNIT_TEST(Parse_OkExtractsDbgIdsAndPbIds) {
        const ui32 n = 4;
        const ui64 base = 1000;
        auto cfg = MakeCfg(n);
        auto rec = MakeOkReply(base, n);

        auto out = ParseAllocateResult(rec, cfg);
        UNIT_ASSERT_C(out.has_value(), out.error());
        UNIT_ASSERT_VALUES_EQUAL(out->size(), n);
        for (ui32 i = 0; i < n; ++i) {
            const auto& d = (*out)[i];
            UNIT_ASSERT_VALUES_EQUAL(d.DbgIndex, i);
            UNIT_ASSERT_VALUES_EQUAL(d.DirectBlockGroupId, base + i);
            for (ui32 k = 0; k < 5; ++k) {
                UNIT_ASSERT_VALUES_EQUAL(d.DDiskIds[k].GetNodeId(), 100u + i);
                UNIT_ASSERT_VALUES_EQUAL(d.DDiskIds[k].GetPDiskId(), 10u + k);
                UNIT_ASSERT_VALUES_EQUAL(d.DDiskIds[k].GetDDiskSlotId(), 1000u + i * 5 + k);
                UNIT_ASSERT_VALUES_EQUAL(d.PBIds[k].GetNodeId(), 200u + i);
                UNIT_ASSERT_VALUES_EQUAL(d.PBIds[k].GetPDiskId(), 20u + k);
                UNIT_ASSERT_VALUES_EQUAL(d.PBIds[k].GetDDiskSlotId(), 2000u + i * 5 + k);
            }
        }
    }

    Y_UNIT_TEST(Parse_AlreadyTreatedAsOk) {
        const ui32 n = 2;
        auto cfg = MakeCfg(n);
        auto rec = MakeOkReply(0, n);
        rec.SetStatus(NKikimrProto::ALREADY);

        auto out = ParseAllocateResult(rec, cfg);
        UNIT_ASSERT_C(out.has_value(), out.error());
        UNIT_ASSERT_VALUES_EQUAL(out->size(), n);
    }

    Y_UNIT_TEST(Parse_StatusErrorReturnsReason) {
        auto cfg = MakeCfg(1);
        NKikimrBlobStorage::TEvControllerAllocateDDiskBlockGroupResult rec;
        rec.SetStatus(NKikimrProto::ERROR);
        rec.SetErrorReason("BSC unhappy");

        auto out = ParseAllocateResult(rec, cfg);
        UNIT_ASSERT(!out.has_value());
        UNIT_ASSERT_STRING_CONTAINS(out.error(), "ERROR");
        UNIT_ASSERT_STRING_CONTAINS(out.error(), "BSC unhappy");
    }

    Y_UNIT_TEST(Parse_ResponseCountMismatchFails) {
        auto cfg = MakeCfg(3);
        auto rec = MakeOkReply(0, /*nDbgs=*/2);  // BSC returned fewer

        auto out = ParseAllocateResult(rec, cfg);
        UNIT_ASSERT(!out.has_value());
        UNIT_ASSERT_STRING_CONTAINS(out.error(), "expected 3");
    }

    Y_UNIT_TEST(Parse_NodeCountMismatchFails) {
        auto cfg = MakeCfg(1);
        auto rec = MakeOkReply(42, 1);
        // remove the last node so the response only has 4 nodes (need 5)
        rec.MutableResponses(0)->MutableNodes()->RemoveLast();

        auto out = ParseAllocateResult(rec, cfg);
        UNIT_ASSERT(!out.has_value());
        UNIT_ASSERT_STRING_CONTAINS(out.error(), "4 nodes");
    }

    Y_UNIT_TEST(Routing_ZeroMeansAllDbgs) {
        auto alloc = MakeCfg(4);
        TEvLoadTestRequest::TNbsDbgLikeLoad::TConfigureTablet cfg;
        cfg.SetNumDirectBlockGroupsToUse(0);
        cfg.SetIoSizeBytes(4096);

        auto p = ComputeRoutingParams(cfg, alloc, /*numDbgs=*/4);
        UNIT_ASSERT_VALUES_EQUAL(p.ActiveDbgs, 4u);
        UNIT_ASSERT(p.IoValid);
        UNIT_ASSERT_VALUES_EQUAL(p.IoSizeBytes, 4096u);
        UNIT_ASSERT_VALUES_EQUAL(p.BytesPerDbg, 7ull * 4 * 1024 * 1024);
    }

    Y_UNIT_TEST(Routing_TooManyClampsToAll) {
        auto alloc = MakeCfg(2);
        TEvLoadTestRequest::TNbsDbgLikeLoad::TConfigureTablet cfg;
        cfg.SetNumDirectBlockGroupsToUse(9);
        cfg.SetIoSizeBytes(4096);

        auto p = ComputeRoutingParams(cfg, alloc, /*numDbgs=*/2);
        UNIT_ASSERT_VALUES_EQUAL(p.ActiveDbgs, 2u);
    }

    Y_UNIT_TEST(Routing_SubsetHonored) {
        auto alloc = MakeCfg(4);
        TEvLoadTestRequest::TNbsDbgLikeLoad::TConfigureTablet cfg;
        cfg.SetNumDirectBlockGroupsToUse(3);
        cfg.SetIoSizeBytes(4096);

        auto p = ComputeRoutingParams(cfg, alloc, /*numDbgs=*/4);
        UNIT_ASSERT_VALUES_EQUAL(p.ActiveDbgs, 3u);
        UNIT_ASSERT(p.IoValid);
    }

    Y_UNIT_TEST(Routing_NonDividingIoIsInvalid) {
        auto alloc = MakeCfg(2);  // VChunkSizeBytes = 4MB
        TEvLoadTestRequest::TNbsDbgLikeLoad::TConfigureTablet cfg;
        cfg.SetNumDirectBlockGroupsToUse(2);
        cfg.SetIoSizeBytes(4097);  // does not divide 4MB

        auto p = ComputeRoutingParams(cfg, alloc, /*numDbgs=*/2);
        UNIT_ASSERT_VALUES_EQUAL(p.ActiveDbgs, 2u);
        UNIT_ASSERT(!p.IoValid);
        UNIT_ASSERT_VALUES_EQUAL(p.IoSizeBytes, 0u);
        UNIT_ASSERT_VALUES_EQUAL(p.BytesPerDbg, 0ull);
    }

    Y_UNIT_TEST(Routing_IoLargerThanVChunkIsInvalid) {
        auto alloc = MakeCfg(2);  // VChunkSizeBytes = 4MB
        TEvLoadTestRequest::TNbsDbgLikeLoad::TConfigureTablet cfg;
        cfg.SetNumDirectBlockGroupsToUse(2);
        cfg.SetIoSizeBytes(8 * 1024 * 1024);  // > VChunkSizeBytes

        auto p = ComputeRoutingParams(cfg, alloc, /*numDbgs=*/2);
        UNIT_ASSERT(!p.IoValid);
    }

    Y_UNIT_TEST(Routing_ZeroVChunkSizeIsInvalid) {
        auto alloc = MakeCfg(2);
        alloc.SetVChunkSizeBytes(0);
        TEvLoadTestRequest::TNbsDbgLikeLoad::TConfigureTablet cfg;
        cfg.SetNumDirectBlockGroupsToUse(2);
        cfg.SetIoSizeBytes(4096);

        auto p = ComputeRoutingParams(cfg, alloc, /*numDbgs=*/2);
        UNIT_ASSERT(!p.IoValid);
    }
}
