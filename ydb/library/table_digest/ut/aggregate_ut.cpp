#include <ydb/library/table_digest/aggregate.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/string/hex.h>
#include <util/system/byteorder.h>
#include <util/system/unaligned_mem.h>

#include <algorithm>
#include <iterator>
#include <limits>

using namespace NYql::NDigest;

namespace {

constexpr EAlgorithm Algorithms[] = {EAlgorithm::CityHash64, EAlgorithm::CityHash128, EAlgorithm::Sha256};

void AssertEqual(const TState& left, const TState& right) {
    UNIT_ASSERT(left.GetAlgorithm() == right.GetAlgorithm());
    UNIT_ASSERT_VALUES_EQUAL(left.GetCount(), right.GetCount());
    UNIT_ASSERT(left.GetHash() == right.GetHash());
    UNIT_ASSERT_VALUES_EQUAL(left.GetHashBytes(), right.GetHashBytes());
    UNIT_ASSERT_VALUES_EQUAL(left.Serialize(), right.Serialize());
}

TWords TestHash(EAlgorithm algorithm, ui64 seed) {
    TWords hash{};
    for (size_t i = 0; i < GetWordCount(algorithm); ++i) {
        hash[i] = seed * 0x123456789abcdefULL + i;
    }
    return hash;
}

void SetCount(TString& bytes, ui64 count) {
    // Magic (7 bytes), version (4 bytes), algorithm (1 byte), then count.
    WriteUnaligned<ui64>(bytes.Detach() + 12, HostToLittle(count));
}

} // namespace

Y_UNIT_TEST_SUITE(DigestAggregate) {

Y_UNIT_TEST(AlgorithmsAndGoldenFormats) {
    const TWords words{0x0123456789abcdefULL, 0x0011223344556677ULL, 0x8899aabbccddeeffULL, 0xfedcba9876543210ULL};
    const TStringBuf hashes[] = {
        "0123456789abcdef",
        "00112233445566770123456789abcdef",
        "fedcba98765432108899aabbccddeeff00112233445566770123456789abcdef",
    };
    const TStringBuf states[] = {
        "59514c4447315401000000010100000000000000efcdab8967452301",
        "59514c4447315401000000020100000000000000efcdab89674523017766554433221100",
        "59514c4447315401000000030100000000000000efcdab89674523017766554433221100ffeeddccbbaa99881032547698badcfe",
    };
    const TStringBuf names[] = {"CityHash64", "CityHash128", "Sha256"};
    for (size_t i = 0; i < std::size(Algorithms); ++i) {
        const auto algorithm = Algorithms[i];
        UNIT_ASSERT(ParseAlgorithm(names[i]) == algorithm);
        UNIT_ASSERT_VALUES_EQUAL(GetAlgorithmName(algorithm), names[i]);
        TState state(algorithm);
        TWords hash{};
        std::copy_n(words.begin(), GetWordCount(algorithm), hash.begin());
        state.AddHash(hash);
        UNIT_ASSERT_VALUES_EQUAL(state.GetCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(state.GetHashBytes(), HexDecode(hashes[i]));
        UNIT_ASSERT_VALUES_EQUAL(state.Serialize(), HexDecode(states[i]));
        AssertEqual(state, TState::Deserialize(HexDecode(states[i])));

        TState fromBytes(algorithm);
        fromBytes.AddHash(HexDecode(hashes[i]));
        AssertEqual(state, fromBytes);
    }
    UNIT_ASSERT_EXCEPTION(ParseAlgorithm("sha256"), yexception);
    UNIT_ASSERT_EXCEPTION(GetAlgorithmName(static_cast<EAlgorithm>(0)), yexception);
    UNIT_ASSERT_EXCEPTION(GetWordCount(static_cast<EAlgorithm>(4)), yexception);
    UNIT_ASSERT_EXCEPTION(TState(static_cast<EAlgorithm>(0)), yexception);
}

Y_UNIT_TEST(FullWidthCarry) {
    const ui64 max = std::numeric_limits<ui64>::max();
    for (const auto algorithm : Algorithms) {
        TState state(algorithm);
        TWords hash{};
        std::fill_n(hash.begin(), GetWordCount(algorithm), max);
        state.AddHash(hash);
        state.AddHash(TWords{1, 0, 0, 0});
        UNIT_ASSERT(state.GetHash() == TWords{});
        UNIT_ASSERT_VALUES_EQUAL(state.GetCount(), 2);
    }

    TState state(EAlgorithm::Sha256);
    state.AddHash(TWords{max, max, 7, 9});
    state.AddHash(TWords{1, 0, 3, 4});
    UNIT_ASSERT(state.GetHash() == (TWords{0, 0, 11, 13}));

    TState bothOverflow(EAlgorithm::Sha256);
    bothOverflow.AddHash(TWords{max, max, 0, 0});
    bothOverflow.AddHash(TWords{1, max, 0, 0});
    UNIT_ASSERT(bothOverflow.GetHash() == (TWords{0, max, 1, 0}));
}

Y_UNIT_TEST(PartitionAndPermutationInvariance) {
    for (const auto algorithm : Algorithms) {
        TState whole(algorithm);
        TVector<TState> partitions(11, TState(algorithm));
        for (ui64 i = 0; i < 200; ++i) {
            const auto hash = TestHash(algorithm, i % 6);
            whole.AddHash(hash);
            partitions[(i * 13) % 9].AddHash(hash);
        }
        TState backwards(algorithm);
        for (ui64 i = 200; i > 0; --i) {
            backwards.AddHash(TestHash(algorithm, (i - 1) % 6));
        }
        AssertEqual(whole, backwards);

        TState merged(algorithm);
        for (size_t i = partitions.size(); i > 0; --i) {
            merged.Merge(TState::Deserialize(partitions[i - 1].Serialize()));
        }
        AssertEqual(whole, merged);

        TState twice(algorithm);
        twice.Merge(whole);
        twice.Merge(whole);
        auto selfMerged = whole;
        selfMerged.Merge(selfMerged);
        AssertEqual(twice, selfMerged);
        UNIT_ASSERT_VALUES_EQUAL(twice.GetCount(), 400);
    }
}

Y_UNIT_TEST(EmptyStateAndHashValidation) {
    for (const auto algorithm : Algorithms) {
        TState empty(algorithm);
        const auto bytes = empty.Serialize();
        UNIT_ASSERT_VALUES_EQUAL(empty.GetCount(), 0);
        UNIT_ASSERT(empty.GetHash() == TWords{});
        UNIT_ASSERT_VALUES_EQUAL(empty.GetHashBytes(), TString(GetWordCount(algorithm) * 8, '\0'));
        AssertEqual(empty, TState::Deserialize(bytes));
        UNIT_ASSERT_EXCEPTION(empty.AddHash(TString(GetWordCount(algorithm) * 8 - 1, '\0')), yexception);
        UNIT_ASSERT_EXCEPTION(empty.AddHash(TString(GetWordCount(algorithm) * 8 + 1, '\0')), yexception);
        if (GetWordCount(algorithm) < 4) {
            TWords tooWide{};
            tooWide[GetWordCount(algorithm)] = 1;
            UNIT_ASSERT_EXCEPTION(empty.AddHash(tooWide), yexception);
        }
        UNIT_ASSERT_VALUES_EQUAL(empty.Serialize(), bytes);

        empty.AddHash(TWords{});
        UNIT_ASSERT_VALUES_EQUAL(empty.GetCount(), 1);
        UNIT_ASSERT(empty.GetHash() == TWords{});
        AssertEqual(empty, TState::Deserialize(empty.Serialize()));
    }
}

Y_UNIT_TEST(InvalidSerializedStates) {
    for (const auto algorithm : Algorithms) {
        TState state(algorithm);
        state.AddHash(TestHash(algorithm, 1));
        const auto bytes = state.Serialize();
        for (size_t i = 0; i < bytes.size(); ++i) {
            UNIT_ASSERT_EXCEPTION(TState::Deserialize(TStringBuf(bytes).SubStr(0, i)), yexception);
        }
        UNIT_ASSERT_EXCEPTION(TState::Deserialize(bytes + "trailing"), yexception);
        auto bad = bytes;
        bad[0] = 'X';
        UNIT_ASSERT_EXCEPTION(TState::Deserialize(bad), yexception);
        bad = bytes;
        bad[7] = 2;
        UNIT_ASSERT_EXCEPTION(TState::Deserialize(bad), yexception);
        bad = bytes;
        bad[11] = 99;
        UNIT_ASSERT_EXCEPTION(TState::Deserialize(bad), yexception);
        bad = bytes;
        SetCount(bad, 0);
        UNIT_ASSERT_EXCEPTION(TState::Deserialize(bad), yexception);
    }
}

Y_UNIT_TEST(CountOverflowAndMismatchedAlgorithms) {
    for (const auto algorithm : Algorithms) {
        TState state(algorithm);
        state.AddHash(TestHash(algorithm, 1));
        auto saturatedBytes = state.Serialize();
        SetCount(saturatedBytes, std::numeric_limits<ui64>::max());
        auto saturated = TState::Deserialize(saturatedBytes);
        UNIT_ASSERT_EXCEPTION(saturated.AddHash(TWords{}), yexception);
        UNIT_ASSERT_EXCEPTION(saturated.Merge(state), yexception);
        UNIT_ASSERT_EXCEPTION(saturated.Merge(saturated), yexception);
        saturated.Merge(TState(algorithm));
        UNIT_ASSERT_VALUES_EQUAL(saturated.Serialize(), saturatedBytes);
    }
    TState a(EAlgorithm::CityHash64);
    const TState b(EAlgorithm::CityHash128);
    UNIT_ASSERT_EXCEPTION(a.Merge(b), yexception);
    UNIT_ASSERT_VALUES_EQUAL(a.GetCount(), 0);
}

} // Y_UNIT_TEST_SUITE(DigestAggregate)
