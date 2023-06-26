#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/erasure/erasure.h>
#include <ydb/core/util/lz4_data_generator.h>
#include <util/system/hp_timer.h>

using namespace NKikimr;

Y_UNIT_TEST_SUITE(ErasureBrandNew) {

    Y_UNIT_TEST(Block42_encode) {
        TErasureType erasure(TErasureType::Erasure4Plus2Block);

        std::vector<TString> buffers;
        ui64 totalSize = 0;
        for (ui32 iter = 0; iter < 10000; ++iter) {
            const ui32 length = 1 + RandomNumber(100000u);
            buffers.push_back(FastGenDataForLZ4(length, iter));
            totalSize += length;
        }

        std::vector<TDataPartSet> parts1;
        parts1.reserve(buffers.size());
        std::vector<std::array<TRope, 6>> parts2;
        parts2.reserve(buffers.size());

        THPTimer timer;
        for (const auto& buffer : buffers) {
            erasure.SplitData(TErasureType::CrcModeNone, buffer, parts1.emplace_back());
        }
        TDuration period1 = TDuration::Seconds(timer.PassedReset());

        for (const auto& buffer : buffers) {
            ErasureSplit(TErasureType::CrcModeNone, TErasureType::Erasure4Plus2Block, TRope(buffer), parts2.emplace_back());
        }
        TDuration period2 = TDuration::Seconds(timer.PassedReset());

        Cerr << "totalSize# " << totalSize
            << " period1# " << period1
            << " period2# " << period2
            << " MB/s1# " << (1.0 * totalSize / period1.MicroSeconds())
            << " MB/s2# " << (1.0 * totalSize / period2.MicroSeconds())
            << " factor# " << (1.0 * period1.MicroSeconds() / period2.MicroSeconds())
            << Endl;

        for (ui32 i = 0; i < buffers.size(); ++i) {
            auto& p1 = parts1[i];
            auto& p2 = parts2[i];

            for (ui32 i = 0; i < 6; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(p1.Parts[i].OwnedString.size(), p2[i].size());
                UNIT_ASSERT_EQUAL(p1.Parts[i].OwnedString, p2[i]);
            }
        }
    }

    Y_UNIT_TEST(Block42_chunked) {
        TErasureType erasure(TErasureType::Erasure4Plus2Block);

        for (ui32 iter = 0; iter < 10000; ++iter) {
            const ui32 length = 1 + RandomNumber(100000u);
            TString buffer = FastGenDataForLZ4(length, iter);
            UNIT_ASSERT_VALUES_EQUAL(buffer.size(), length);

            TRope rope;
            size_t offset = 0;
            while (offset < length) {
                size_t chunkLen = 1 + RandomNumber<size_t>(length - offset);
                auto chunk = TRcBuf::Uninitialized(chunkLen);
                memcpy(chunk.GetDataMut(), buffer.data() + offset, chunkLen);
                rope.Insert(rope.End(), std::move(chunk));
                offset += chunkLen;
            }
            UNIT_ASSERT_VALUES_EQUAL(rope.size(), length);
            UNIT_ASSERT_VALUES_EQUAL(rope.ConvertToString().size(), buffer.size());
            UNIT_ASSERT_EQUAL(rope.ConvertToString(), buffer);

            TDataPartSet p1;
            erasure.SplitData(TErasureType::CrcModeNone, buffer, p1);

            std::array<TRope, 6> p2;
            ErasureSplit(TErasureType::CrcModeNone, TErasureType::Erasure4Plus2Block, TRope(rope), p2);

            for (ui32 i = 0; i < 6; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(p1.Parts[i].OwnedString.size(), p2[i].size());
                UNIT_ASSERT_EQUAL(p1.Parts[i].OwnedString, p2[i]);
            }
        }
    }

}
