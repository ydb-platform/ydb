#include "blobstorage_hullcompaction_state.h"

#include <library/cpp/testing/unittest/registar.h>

#include <memory>

namespace NKikimr {

    Y_UNIT_TEST_SUITE(TFullCompactionStateTest) {

        struct TMockLevelIndex {
            bool IsWrittenToSstBeforeLsn(ui64) const {
                return true;
            }
        };

        struct TMockRTCtx {
            std::shared_ptr<TMockLevelIndex> LevelIndex = std::make_shared<TMockLevelIndex>();
        };

        Y_UNIT_TEST(DefersFullCompactionLsnDuringActiveCompaction) {
            auto config = MakeIntrusive<TVDiskConfig>(TVDiskConfig::TBaseInfo::SampleForTests());
            TFullCompactionState state(config);
            TMockRTCtx rtCtx;

            const TActorId recipient1(1, 0, 1, 0);
            const TActorId recipient2(1, 0, 2, 0);

            state.FullCompactionTask(100, TInstant::Seconds(1), EHullDbType::LogoBlobs, 1, recipient1, {}, true);
            auto attrs1 = state.GetFullCompactionAttrsForLevelCompactionSelector(&rtCtx);
            UNIT_ASSERT(attrs1);
            UNIT_ASSERT_VALUES_EQUAL(attrs1->FullCompactionLsn, 100);

            state.FullCompactionTask(200, TInstant::Seconds(2), EHullDbType::LogoBlobs, 2, recipient2, {}, true);
            auto attrsStill = state.GetFullCompactionAttrsForLevelCompactionSelector(&rtCtx);
            UNIT_ASSERT(attrsStill);
            UNIT_ASSERT_VALUES_EQUAL(attrsStill->FullCompactionLsn, 100);

            const auto completed1 = state.Compacted({attrs1, true}, TInstant::Seconds(3));
            UNIT_ASSERT_VALUES_EQUAL(completed1.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(completed1[0].RequestId, 1);

            auto attrs2 = state.GetFullCompactionAttrsForLevelCompactionSelector(&rtCtx);
            UNIT_ASSERT(attrs2);
            UNIT_ASSERT_VALUES_EQUAL(attrs2->FullCompactionLsn, 200);

            const auto completed2 = state.Compacted({attrs2, true}, TInstant::Seconds(4));
            UNIT_ASSERT_VALUES_EQUAL(completed2.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(completed2[0].RequestId, 2);
        }

    }

} // NKikimr
