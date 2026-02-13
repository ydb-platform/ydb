#include "block_buffer.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/maybe.h>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TBlockBufferTest)
{
    using TFill = TMaybe<char>;

    static TFill Zero = TFill();
    static TFill One = TFill(char(1));
    static TFill Two = TFill(char(2));

    struct TBlock
    {
        TFill fill;
        size_t size = 0;
    };

    using TContent = TVector<TBlock>;

    void Fill(TBlockBuffer & b, const TContent& content)
    {
        for (auto c: content) {
            if (c.fill) {
                TString tmp(c.size, *c.fill);
                b.AddBlock({tmp.data(), tmp.size()});
            } else {
                b.AddZeroBlock(c.size);
            }
        }
    }

    bool CheckBlocks(const TContent& expected,
                     const TVector<TBlockDataRef>& actual)
    {
        UNIT_ASSERT_VALUES_EQUAL(expected.size(), actual.size());

        for (size_t i = 0; i < expected.size(); i++) {
            const auto expectedBlock = expected[i];
            const auto actualBlock = actual[i];

            if (expectedBlock.fill) {
                const TString expectedData(expectedBlock.size,
                                           *expectedBlock.fill);
                UNIT_ASSERT_VALUES_EQUAL(expectedData,
                                         actualBlock.AsStringBuf());
            } else {
                UNIT_ASSERT_VALUES_EQUAL(nullptr, actualBlock.Data());
                UNIT_ASSERT_VALUES_EQUAL(expectedBlock.size,
                                         actualBlock.Size());
            }
        }

        return true;
    }

    void Check(const TContent& expected, const TBlockBuffer& b)
    {
        UNIT_ASSERT(static_cast<bool>(expected) == static_cast<bool>(b));

        size_t expectedBytesCount = 0;
        for (auto block: expected) {
            expectedBytesCount += block.size;
        }
        UNIT_ASSERT_VALUES_EQUAL(expectedBytesCount, b.GetBytesCount());

        UNIT_ASSERT_VALUES_EQUAL(expected.size(), b.GetBlocksCount());

        TVector<TBlockDataRef> actualBlocks;
        for (size_t i = 0; i < expected.size(); i++) {
            actualBlocks.push_back(b.GetBlock(i));
        }
        CheckBlocks(expected, actualBlocks);

        CheckBlocks(expected, b.GetBlocks());

        TString expectedData;
        for (auto block: expected) {
            if (block.fill) {
                expectedData += TString(block.size, *block.fill);
            } else {
                expectedData += TString(block.size, char(0));
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(expectedData, b.AsString());
    }

    Y_UNIT_TEST(Empty)
    {
        TBlockBuffer empty;
        Check({}, empty);
    }

    Y_UNIT_TEST(MoveConstructor)
    {
        TBlockBuffer moved;
        const TContent expected = {{One, 1}, {Zero, 2}, {Two, 3}};
        Fill(moved, expected);

        TBlockBuffer b(std::move(moved));

        Check({}, moved);
        Check(expected, b);
    }

    Y_UNIT_TEST(MoveAssignment)
    {
        TBlockBuffer moved;
        const TContent expected = {{Zero, 1}, {One, 2}, {Two, 3}};
        Fill(moved, expected);

        TBlockBuffer b;
        b = std::move(moved);

        Check({}, moved);
        Check(expected, b);
    }

    Y_UNIT_TEST(Clone)
    {
        TBlockBuffer cloned;
        const TContent expected = {{One, 1}, {Zero, 2}, {Two, 3}};
        Fill(cloned, expected);

        TBlockBuffer b = cloned.Clone();

        Check(expected, b);
        Check(expected, cloned);
    }

    Y_UNIT_TEST(DetachBlocks)
    {
        TBlockBuffer detached;
        const TContent expected = {{Zero, 1}, {One, 2}, {Two, 3}};
        Fill(detached, expected);

        const auto blocks = detached.DetachBlocks();

        Check({}, detached);
        CheckBlocks(expected, blocks);

        auto* allocator = TDefaultAllocator::Instance();
        for (auto block: blocks) {
            if (block.Data()) {
                allocator->Release({(void*)block.Data(), block.Size()});
            }
        }
    }

    Y_UNIT_TEST(Clear)
    {
        TBlockBuffer b;
        Fill(b, {{One, 1}, {Zero, 2}, {Two, 3}});

        b.Clear();
        Check({}, b);
    }
}

}   // namespace NYdb::NBS
