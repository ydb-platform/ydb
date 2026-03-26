#include "sglist.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NYdb::NBS {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 DefaultBlockSize = 4096;

}   // namespace

Y_UNIT_TEST_SUITE(TSgListTest)
{
    Y_UNIT_TEST(ShouldNormalizeSgList)
    {
        TVector<size_t> blocksCountList = {3, 1, 5, 7};

        TVector<TString> buffers;
        TSgList sglist;
        size_t totalBlocksCount = 0;

        for (auto blocksCount: blocksCountList) {
            TString buffer(blocksCount * DefaultBlockSize, 'a');
            sglist.emplace_back(buffer.data(), buffer.size());
            buffers.push_back(std::move(buffer));
            totalBlocksCount += blocksCount;
        }

        auto sgListOrError =
            SgListNormalize(std::move(sglist), DefaultBlockSize);
        UNIT_ASSERT(!HasError(sgListOrError));
        auto normalizedSglist = sgListOrError.ExtractResult();

        UNIT_ASSERT(totalBlocksCount == normalizedSglist.size());

        size_t num = 0;
        const char* ptr = nullptr;
        size_t size = 0;

        for (const auto& buf: normalizedSglist) {
            if (size == 0) {
                ptr = buffers[num].data();
                size = buffers[num].size();
                ++num;
            }

            UNIT_ASSERT(ptr == buf.Data());
            UNIT_ASSERT(DefaultBlockSize == buf.Size());
            ptr += DefaultBlockSize;
            size -= DefaultBlockSize;
        }
    }

    Y_UNIT_TEST(ShouldNormalizeBuffer)
    {
        size_t blocksCount = 12;

        TString buffer(blocksCount * DefaultBlockSize, 'a');
        TBlockDataRef blockDataRef{buffer.data(), buffer.size()};

        auto sgListOrError = SgListNormalize(blockDataRef, DefaultBlockSize);
        UNIT_ASSERT(!HasError(sgListOrError));
        auto sglist = sgListOrError.ExtractResult();

        UNIT_ASSERT(blocksCount == sglist.size());
        auto ptr = buffer.data();

        for (const auto& buf: sglist) {
            UNIT_ASSERT(ptr == buf.Data());
            UNIT_ASSERT(DefaultBlockSize == buf.Size());
            ptr += DefaultBlockSize;
        }
    }

    Y_UNIT_TEST(ShouldCreateSubSgListFromSingleBuffer)
    {
        TString buffer = "abcdefghijkl";
        const TSgList src{{buffer.data(), buffer.size()}};

        {
            const TSgList fromStart = CreateSgListSubRange(src, 0, 4);
            UNIT_ASSERT(fromStart.size() == 1);
            UNIT_ASSERT_VALUES_EQUAL("abcd", fromStart[0].AsStringBuf());
        }
        {
            const TSgList fromMiddle = CreateSgListSubRange(src, 4, 4);
            UNIT_ASSERT(fromMiddle.size() == 1);
            UNIT_ASSERT_VALUES_EQUAL("efgh", fromMiddle[0].AsStringBuf());
        }
        {
            const TSgList fromEnd = CreateSgListSubRange(src, 8, 4);
            UNIT_ASSERT(fromEnd.size() == 1);
            UNIT_ASSERT_VALUES_EQUAL("ijkl", fromEnd[0].AsStringBuf());
        }
        {
            const TSgList full = CreateSgListSubRange(src, 0, 12);
            UNIT_ASSERT(full.size() == 1);
            UNIT_ASSERT_VALUES_EQUAL("abcdefghijkl", full[0].AsStringBuf());
        }
    }

    Y_UNIT_TEST(ShouldCreateSubSgListFromMultipleBuffer)
    {
        TString buffer1 = "abcdef";
        TString buffer2 = "ghijkl";
        TString buffer3 = "mnopqr";
        const TSgList src{
            {buffer1.data(), buffer1.size()},
            {buffer2.data(), buffer2.size()},
            {buffer3.data(), buffer3.size()}};

        {
            const TSgList fromStart = CreateSgListSubRange(src, 0, 4);
            UNIT_ASSERT(fromStart.size() == 1);
            UNIT_ASSERT_VALUES_EQUAL("abcd", fromStart[0].AsStringBuf());
        }
        {
            const TSgList fromMiddle = CreateSgListSubRange(src, 4, 4);
            UNIT_ASSERT(fromMiddle.size() == 2);
            UNIT_ASSERT_VALUES_EQUAL("ef", fromMiddle[0].AsStringBuf());
            UNIT_ASSERT_VALUES_EQUAL("gh", fromMiddle[1].AsStringBuf());
        }
        {
            const TSgList fromMiddle = CreateSgListSubRange(src, 4, 10);
            UNIT_ASSERT(fromMiddle.size() == 3);
            UNIT_ASSERT_VALUES_EQUAL("ef", fromMiddle[0].AsStringBuf());
            UNIT_ASSERT_VALUES_EQUAL("ghijkl", fromMiddle[1].AsStringBuf());
            UNIT_ASSERT_VALUES_EQUAL("mn", fromMiddle[2].AsStringBuf());
        }
        {
            const TSgList fromEnd = CreateSgListSubRange(src, 8, 4);
            UNIT_ASSERT(fromEnd.size() == 1);
            UNIT_ASSERT_VALUES_EQUAL("ijkl", fromEnd[0].AsStringBuf());
        }
        {
            const TSgList full = CreateSgListSubRange(src, 0, 18);
            UNIT_ASSERT(full.size() == 3);
            UNIT_ASSERT_VALUES_EQUAL("abcdef", full[0].AsStringBuf());
            UNIT_ASSERT_VALUES_EQUAL("ghijkl", full[1].AsStringBuf());
            UNIT_ASSERT_VALUES_EQUAL("mnopqr", full[2].AsStringBuf());
        }
    }

    Y_UNIT_TEST(ShouldNotCreateSubSgListWhenOutOfRange)
    {
        TString buffer1 = "abcdef";
        TString buffer2 = "ghijkl";
        const TSgList src{
            {buffer1.data(), buffer1.size()},
            {buffer2.data(), buffer2.size()},
        };

        const TSgList outOfRange = CreateSgListSubRange(src, 12, 4);
        UNIT_ASSERT(outOfRange.size() == 0);
    }

    Y_UNIT_TEST(ShouldNotCreateSubSgListFromEmptySgList)
    {
        const TSgList src;

        const TSgList outOfRange = CreateSgListSubRange(src, 12, 4);
        UNIT_ASSERT(outOfRange.size() == 0);
    }

    Y_UNIT_TEST(ShouldNotCreateSubSgListWithZeroSize)
    {
        TString buffer1 = "abcdef";
        TString buffer2 = "ghijkl";
        const TSgList src{
            {buffer1.data(), buffer1.size()},
            {buffer2.data(), buffer2.size()},
        };
        const TSgList outOfRange = CreateSgListSubRange(src, 4, 0);
        UNIT_ASSERT(outOfRange.size() == 0);
    }

    Y_UNIT_TEST(ShouldTruncateSubSgList)
    {
        TString buffer1 = "abcdef";
        TString buffer2 = "ghijkl";
        const TSgList src{
            {buffer1.data(), buffer1.size()},
            {buffer2.data(), buffer2.size()},
        };
        const TSgList truncated = CreateSgListSubRange(src, 4, 10);
        UNIT_ASSERT(truncated.size() == 2);
        UNIT_ASSERT_VALUES_EQUAL("ef", truncated[0].AsStringBuf());
        UNIT_ASSERT_VALUES_EQUAL("ghijkl", truncated[1].AsStringBuf());
    }
}

}   // namespace NYdb::NBS
