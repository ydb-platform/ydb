// DO_NOT_STYLE

#include "position.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/buffer.h>
#include <util/generic/buffer.h>

Y_UNIT_TEST_SUITE(PositionStreamTestSuite) {
    Y_UNIT_TEST(PositionDump) {
        TBuffer data;
        data.Resize(10000);
        for (ui32 i = 0; i < 10000; ++i)
            data.Data()[i] = i % 256;
        TPositionStream in(new TBufferInput(data));
        for (ui32 i = 0; i < 10000; i += 3) {
            char d;
            UNIT_ASSERT_EQUAL(in.SkipTo(i), i);
            UNIT_ASSERT_EQUAL(in.Read(&d, 1), 1);
            UNIT_ASSERT_EQUAL(data.Data()[i], d);
            UNIT_ASSERT_EQUAL(in.GetPosition(), i + 1);
        }
    }
}
