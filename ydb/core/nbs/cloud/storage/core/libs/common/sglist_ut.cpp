#include "sglist.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

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
}

}   // namespace NYdb::NBS
