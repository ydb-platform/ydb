#include "blobstorage_syncer_localwriter.h"
#include <library/cpp/testing/unittest/registar.h>
#include <util/stream/null.h>

#define STR Cnull

namespace NKikimr {

    Y_UNIT_TEST_SUITE(TEvLocalSyncDataTests) {

        struct TTest : public TEvLocalSyncData {
            using TEvLocalSyncData::Squeeze;
        };

        Y_UNIT_TEST(SqueezeBlocks1) {
            TFreshAppendixBlocks::TVec vec = {{1, 1}, {3, 4}, {1, 15}};
            TTest::Squeeze(vec);
            TFreshAppendixBlocks::TVec res = {{1, 15}, {3, 4}};
            UNIT_ASSERT(vec == res);
        }

        Y_UNIT_TEST(SqueezeBlocks2) {
            TFreshAppendixBlocks::TVec vec = {};
            TTest::Squeeze(vec);
            TFreshAppendixBlocks::TVec res = {};
            UNIT_ASSERT(vec == res);
        }

        Y_UNIT_TEST(SqueezeBlocks3) {
            TFreshAppendixBlocks::TVec vec = {{4, 7}, {4, 88}, {4, 15}, {4, 2}, {4, 77}};
            TTest::Squeeze(vec);
            TFreshAppendixBlocks::TVec res = {{4, 88}};
            UNIT_ASSERT(vec == res);
        }
    }

} // NKikimr
