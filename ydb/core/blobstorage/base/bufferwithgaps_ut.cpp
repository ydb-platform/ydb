#include "bufferwithgaps.h"
#include <library/cpp/testing/gtest/gtest.h>
#include <ydb/library/testlib/unittest_gtest_macro_subst.h>

using NKikimr::TBufferWithGaps;

namespace {

    TEST(BufferWithGaps, Basic) {
        TBufferWithGaps buffer(0);
        TString data = "Hello!";
        buffer.SetData(TRcBuf::Copy(data));
        UNIT_ASSERT_STRINGS_EQUAL(data, buffer.Substr(0, buffer.Size()));
    }

    TEST(BufferWithGaps, IsReadable) {
        TBufferWithGaps buffer(0);
        TString data = "Hello! How are you? I'm fine, and you? Me too, thanks!";
        TString gaps = "G           GGGG           GG              GGG G     G";
        buffer.SetData(TRcBuf::Copy(data));
        for (size_t k = 0; k < gaps.size(); ++k) {
            if (gaps[k] != ' ') {
                buffer.AddGap(k, k + 1);
            }
        }
        UNIT_ASSERT_EQUAL(buffer.Size(), data.size());
        for (size_t k = 0; k < buffer.Size(); ++k) {
            for (size_t len = 1; len <= buffer.Size() - k; ++len) {
                bool haveGaps = false;
                for (size_t i = k; i < k + len; ++i) {
                    if (gaps[i] != ' ') {
                        haveGaps = true;
                        break;
                    }
                }
                UNIT_ASSERT_EQUAL(!haveGaps, buffer.IsReadable(k, len));
                if (!haveGaps) {
                    UNIT_ASSERT_STRINGS_EQUAL(buffer.Substr(k, len), data.substr(k, len));
                }
            }
        }
    }

}
