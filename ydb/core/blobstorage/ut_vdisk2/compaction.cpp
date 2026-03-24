#include "env.h"

#include <ydb/core/blobstorage/base/blobstorage_events.h>
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

        // Validates status and payload.
        auto checkGet = [&](NKikimrProto::EReplyStatus expectedStatus,
                const TString& stage) {
            auto res = env->Get(id);
            UNIT_ASSERT_VALUES_EQUAL_C(res.GetStatus(), NKikimrProto::OK, stage);
            UNIT_ASSERT_VALUES_EQUAL_C(res.ResultSize(), 1, stage);

            const auto& value = res.GetResult(0);
            UNIT_ASSERT_VALUES_EQUAL_C(value.GetStatus(), expectedStatus, stage);
            if (expectedStatus == NKikimrProto::OK) {
                UNIT_ASSERT_EQUAL_C(value.GetBufferData(), data, stage);
            }
        };

        // Validates Keep/DoNotKeep flags stored in index records.
        auto checkGetIndex = [&](NKikimrProto::EReplyStatus expectedStatus, bool expectedKeep, bool expectedDoNotKeep,
                const TString& stage) {
            auto res = env->GetIndex(id);
            UNIT_ASSERT_VALUES_EQUAL_C(res.GetStatus(), NKikimrProto::OK, stage);
            UNIT_ASSERT_VALUES_EQUAL_C(res.ResultSize(), 1, stage);

            const auto& value = res.GetResult(0);
            UNIT_ASSERT_VALUES_EQUAL_C(value.GetStatus(), expectedStatus, stage);
            UNIT_ASSERT_VALUES_EQUAL_C(value.GetKeep(), expectedKeep, stage);
            UNIT_ASSERT_VALUES_EQUAL_C(value.GetDoNotKeep(), expectedDoNotKeep, stage);
        };

        // Issue only DoNotKeep for this blob and move it behind soft barrier.
        auto doNotKeepRes = env->Collect(tabletId, generation, 1, channel, std::make_pair(generation, step), false,
            {}, {id.FullID()});
        UNIT_ASSERT_VALUES_EQUAL(doNotKeepRes.GetStatus(), NKikimrProto::OK);

        // Sanity check before explicit fresh compaction.
        checkGet(NKikimrProto::NODATA, "before fresh compaction data");
        checkGetIndex(NKikimrProto::NODATA, false, true, "before fresh compaction index");

        // Fresh-only compaction must not drop the DoNotKeep-only metadata record.
        env->Compact(true);
        checkGet(NKikimrProto::NODATA, "after fresh compaction data");
        checkGetIndex(NKikimrProto::NODATA, false, true, "after fresh compaction index");
    }

}