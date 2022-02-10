#include "hyperloglog.h"

#include <util/generic/buffer.h>
#include <util/random/mersenne.h>
#include <util/stream/buffer.h>

#include <library/cpp/testing/unittest/registar.h>

#include <cmath>

Y_UNIT_TEST_SUITE(THyperLogLog) {
    Y_UNIT_TEST(TestPrecision18) {
        TMersenne<ui64> rand;

        auto counter = THyperLogLog::Create(18);

        static const std::pair<ui64, ui64> POINTS[] = {
            {10, 10},
            {100, 100},
            {1000, 998},
            {10000, 9978},
            {100000, 99995},
            {1000000, 997017},
            {10000000, 9983891},
            {100000000, 100315572},
            {1000000000, 998791445},
            //1:37: {10000000000, 10015943904}
        };
        ui64 unique = 0;
        for (const auto& pnt : POINTS) {
            while (unique < pnt.first) {
                const auto val = rand();
                counter.Update(val);
                ++unique;
            }
            const auto estimation = counter.Estimate();
            const auto delta = i64(estimation) - i64(unique);
            const auto error = double(delta) / unique;
            UNIT_ASSERT(std::abs(error) < 0.0032);
            UNIT_ASSERT_EQUAL(estimation, pnt.second);
        }
        {
            auto counter2 = THyperLogLog::Create(18);
            while (unique < 2000000000) {
                const auto val = rand();
                counter2.Update(val);
                ++unique;
            }
            const auto estimation = counter2.Estimate();
            UNIT_ASSERT_EQUAL(estimation, 1000013484);

            counter.Merge(counter2);
            UNIT_ASSERT_EQUAL(counter.Estimate(), 1998488794);
        }

        {
            TBufferStream stream;
            counter.Save(stream);
            UNIT_ASSERT_EQUAL(stream.Buffer().Size(), 1 + (1 << 18));

            stream.Rewind();
            const auto copy = THyperLogLog::Load(stream);

            UNIT_ASSERT_EQUAL(counter.Estimate(), copy.Estimate());
        }
    }
}
