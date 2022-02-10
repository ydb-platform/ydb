#include "bititerator.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/vector.h>

Y_UNIT_TEST_SUITE(TBitIteratorTest) {
    TVector<ui16> GenWords() {
        TVector<ui16> words(1, 0);
        for (ui16 word = 1; word; ++word)
            words.push_back(word);
        return words;
    }

    template <typename TWord>
    void AssertPeekRead(TBitIterator<TWord> & iter, ui8 count, TWord expected) {
        auto peek = iter.Peek(count);
        auto read = iter.Read(count);
        UNIT_ASSERT_EQUAL(peek, read);
        UNIT_ASSERT_EQUAL(peek, expected);
    }

    Y_UNIT_TEST(TestNextAndPeek) {
        const auto& words = GenWords();

        TBitIterator<ui16> iter(words.data());
        ui16 word = 0;
        for (int i = 0; i != (1 << 16); ++i, ++word) {
            for (int bit = 0; bit != 16; ++bit) {
                auto peek = iter.Peek();
                auto next = iter.Next();
                UNIT_ASSERT_EQUAL(peek, next);
                UNIT_ASSERT_EQUAL(peek, (word >> bit) & 1);
            }
            UNIT_ASSERT_EQUAL(iter.NextWord(), words.data() + i + 1);
        }

        UNIT_ASSERT_EQUAL(iter.NextWord(), words.data() + words.size());
    }

    Y_UNIT_TEST(TestAlignedReadAndPeek) {
        const auto& words = GenWords();

        TBitIterator<ui16> iter(words.data());
        ui16 word = 0;
        for (int i = 0; i != (1 << 16); ++i, ++word) {
            AssertPeekRead(iter, 16, word);
            UNIT_ASSERT_EQUAL(iter.NextWord(), words.data() + i + 1);
        }

        UNIT_ASSERT_EQUAL(iter.NextWord(), words.data() + words.size());
    }

    Y_UNIT_TEST(TestForward) {
        TVector<ui32> words;
        words.push_back((1 << 10) | (1 << 20) | (1 << 25));
        words.push_back(1 | (1 << 5) | (1 << 6) | (1 << 30));
        for (int i = 0; i < 3; ++i)
            words.push_back(0);
        words.push_back(1 << 10);

        TBitIterator<ui32> iter(words.data());
        UNIT_ASSERT(!iter.Next());
        UNIT_ASSERT(!iter.Next());
        UNIT_ASSERT(!iter.Next());
        iter.Forward(6);
        UNIT_ASSERT(!iter.Next());
        UNIT_ASSERT(iter.Next());
        UNIT_ASSERT(!iter.Next());
        iter.Forward(8);
        UNIT_ASSERT(iter.Next());
        iter.Forward(4);
        UNIT_ASSERT(iter.Next());
        iter.Forward(5);
        UNIT_ASSERT(!iter.Next());
        UNIT_ASSERT(iter.Next());
        iter.Forward(4);
        UNIT_ASSERT(iter.Next());

        iter.Reset(words.data());
        iter.Forward(38);
        UNIT_ASSERT(iter.Next());
        UNIT_ASSERT(!iter.Next());
        UNIT_ASSERT_EQUAL(iter.NextWord(), words.data() + 2);

        iter.Forward(24 + 32 * 3 + 9);
        UNIT_ASSERT(!iter.Next());
        UNIT_ASSERT(iter.Next());
        UNIT_ASSERT(!iter.Next());
        UNIT_ASSERT_EQUAL(iter.NextWord(), words.data() + 6);
    }

    Y_UNIT_TEST(TestUnalignedReadAndPeek) {
        TVector<ui32> words;
        words.push_back((1 << 10) | (1 << 20) | (1 << 25));
        words.push_back(1 | (1 << 5) | (1 << 6) | (1 << 30));
        for (int i = 0; i < 5; ++i)
            words.push_back(1 | (1 << 10));

        TBitIterator<ui32> iter(words.data());
        AssertPeekRead(iter, 5, ui32(0));
        AssertPeekRead(iter, 7, ui32(1 << 5));
        AssertPeekRead(iter, 21, ui32((1 << 8) | (1 << 13) | (1 << 20)));
        AssertPeekRead(iter, 32, (words[1] >> 1) | (1 << 31));
        iter.Forward(8);
        UNIT_ASSERT(!iter.Next());
        UNIT_ASSERT(iter.Next());
        UNIT_ASSERT(!iter.Next());
    }
}
