#include "env.h"

#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/vdisk/ingress/blobstorage_ingress.h>
#include <ydb/core/util/lz4_data_generator.h>

using namespace NKikimr;

Y_UNIT_TEST_SUITE(VDiskCompactionTests) {

    Y_UNIT_TEST(FreshCompactionKeepsDoNotKeepFlag) {
        // Regression test for fresh compaction: DoNotKeep metadata must survive compaction.
        std::optional<TTestEnv> env(std::in_place);

        const ui64 tabletId = 1;
        const ui32 generation = 1;
        const ui32 step = 1;
        const ui8 channel = 0;
        const TLogoBlobID id(tabletId, generation, step, channel, 100, 0, 1);
        const TString data = FastGenDataForLZ4(id.BlobSize(), id.Hash());

        auto putRes = env->Put(id, data);
        UNIT_ASSERT_VALUES_EQUAL(putRes.GetStatus(), NKikimrProto::OK);

        auto checkGet = [&](const TLogoBlobID& blobId, NKikimrProto::EReplyStatus expectedStatus,
                bool expectedKeep, bool expectedDoNotKeep, const TString& stage) {
            for (bool indexOnly : {false, true}) {
                auto res = indexOnly ? env->GetIndex(blobId) : env->Get(blobId);
                const TString context = stage + (indexOnly ? " (index)" : " (data)");
                UNIT_ASSERT_VALUES_EQUAL_C(res.GetStatus(), NKikimrProto::OK, context);
                UNIT_ASSERT_VALUES_EQUAL_C(res.ResultSize(), 1, context);

                const auto& value = res.GetResult(0);
                UNIT_ASSERT_VALUES_EQUAL_C(value.GetStatus(), expectedStatus, context);
                UNIT_ASSERT_VALUES_EQUAL_C(value.GetKeep(), expectedKeep, context);
                UNIT_ASSERT_VALUES_EQUAL_C(value.GetDoNotKeep(), expectedDoNotKeep, context);
                if (indexOnly) {
                    const bool readable = expectedStatus == NKikimrProto::OK;
                    UNIT_ASSERT_VALUES_EQUAL_C(value.HasIngress(), readable, context);
                    UNIT_ASSERT_VALUES_EQUAL_C(value.PartsSize(), readable ? 1 : 0, context);
                    if (readable) {
                        UNIT_ASSERT_VALUES_EQUAL_C(value.GetParts(0), blobId.PartId(), context);
                        const auto local = TIngress(value.GetIngress()).LocalParts(TBlobStorageGroupType::ErasureNone);
                        UNIT_ASSERT_C(local.Get(blobId.PartId() - 1), context);
                    }
                } else if (expectedStatus == NKikimrProto::OK) {
                    UNIT_ASSERT_EQUAL_C(value.GetBufferData(), data, context);
                }
            }
        };

        const TLogoBlobID missingId(tabletId, generation, step, channel, 100, 1, 1);
        checkGet(missingId, NKikimrProto::NODATA, false, false, "absent index record");
        checkGet(id, NKikimrProto::OK, false, false, "after put");

        // Compact Keep first so the subsequent DoNotKeep lives in the fresh index.
        auto keepRes = env->Collect(tabletId, generation, 1, channel, std::nullopt, false,
            {id.FullID()}, {});
        UNIT_ASSERT_VALUES_EQUAL(keepRes.GetStatus(), NKikimrProto::OK);
        checkGet(id, NKikimrProto::OK, true, false, "after Keep");
        env->Compact(true);
        auto doNotKeepRes = env->Collect(tabletId, generation, 2, channel, std::nullopt, false, {}, {id.FullID()});
        UNIT_ASSERT_VALUES_EQUAL(doNotKeepRes.GetStatus(), NKikimrProto::OK);
        checkGet(id, NKikimrProto::OK, false, true, "DoNotKeep before the soft barrier");

        auto barrierRes = env->Collect(tabletId, generation, 3, channel, std::make_pair(generation, step), false, {}, {});
        UNIT_ASSERT_VALUES_EQUAL(barrierRes.GetStatus(), NKikimrProto::OK);
        checkGet(id, NKikimrProto::NODATA, false, true, "before fresh compaction");
        env->Compact(true);
        checkGet(id, NKikimrProto::NODATA, false, true, "after fresh compaction");
    }

}
