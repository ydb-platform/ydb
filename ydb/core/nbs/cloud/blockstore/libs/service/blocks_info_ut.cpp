#include "blocks_info.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(BlocksInfoTest)
{
    Y_UNIT_TEST(TestSplit)
    {
        const ui32 BlockSize = 512;
        {
            // aligned message is not split
            TBlocksInfo blocksInfo(0, 8 * BlockSize, BlockSize);
            auto [first, second] = blocksInfo.Split();
            UNIT_ASSERT_VALUES_EQUAL(blocksInfo, first);
            UNIT_ASSERT(!second);
        }

        {
            // unaligned message with 1 block size is not split
            TBlocksInfo blocksInfo(5, BlockSize - 10, BlockSize);
            auto [first, second] = blocksInfo.Split();
            UNIT_ASSERT_VALUES_EQUAL(blocksInfo, first);
            UNIT_ASSERT(!second);
        }

        {
            // unaligned message with size less or equal than 2 blocks is not
            // split
            TBlocksInfo blocksInfo(5, 2 * BlockSize - 10, BlockSize);
            auto [first, second] = blocksInfo.Split();
            UNIT_ASSERT_VALUES_EQUAL(blocksInfo, first);
            UNIT_ASSERT(!second);
        }

        {
            // unaligned message with size more than 2 blocks is split
            const ui32 offset = 5;
            TBlocksInfo blocksInfo(offset, 2 * BlockSize, BlockSize);
            auto [first, second] = blocksInfo.Split();
            UNIT_ASSERT_VALUES_EQUAL(
                TBlocksInfo(offset, BlockSize - offset, BlockSize),
                first);
            UNIT_ASSERT(second);
            UNIT_ASSERT_VALUES_EQUAL(
                TBlocksInfo(BlockSize, BlockSize + offset, BlockSize),
                *second);
        }

        {
            // message with unaligned end offset is split
            const ui32 endOffset = 5;
            TBlocksInfo blocksInfo(0, 10 * BlockSize + endOffset, BlockSize);
            auto [first, second] = blocksInfo.Split();
            UNIT_ASSERT_VALUES_EQUAL(
                TBlocksInfo(0, 10 * BlockSize, BlockSize),
                first);
            UNIT_ASSERT(second);
            UNIT_ASSERT_VALUES_EQUAL(
                TBlocksInfo(10 * BlockSize, endOffset, BlockSize),
                *second);
        }
    }
}

}   // namespace NYdb::NBS::NBlockStore
