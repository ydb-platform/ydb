#include <ydb/core/formats/arrow/hash/calcer.h>
#include <ydb/core/kqp/ut/common/arrow_builders.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/helper/case_helper.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NOlap::NIndexes {

namespace {

using NKikimr::NKqp::NTestArrow::MakeArrayNullable;
using NKikimr::NKqp::NTestArrow::MakeInt32ArrayNullable;

std::vector<ui64> CollectHashes(const std::shared_ptr<arrow::Array>& array, const ui64 seed, const bool caseSensitive) {
    TCaseAwareHashCalcer calcer(caseSensitive);
    std::vector<ui64> hashes(array->length());
    calcer.CalcForAll(array, seed, [&](const ui64 hash, const ui32 idx) {
        hashes[idx] = hash;
    });

    return hashes;
}

}   // namespace

Y_UNIT_TEST_SUITE(TCaseHelperTests) {
    Y_UNIT_TEST(CaseSensitiveReturnsOriginal) {
        TCaseStringNormalizer normalizer(true);
        const TString value = "AbC";
        UNIT_ASSERT_VALUES_EQUAL(normalizer.Normalize(value), value);
    }

    Y_UNIT_TEST(CaseInsensitiveLowercasesAscii) {
        TCaseStringNormalizer normalizer(false);
        UNIT_ASSERT_VALUES_EQUAL(normalizer.Normalize("AbC"), TStringBuf("abc"));
        UNIT_ASSERT_VALUES_EQUAL(normalizer.Normalize("XYZ"), TStringBuf("xyz"));
    }

    Y_UNIT_TEST(CaseInsensitiveReusesBuffer) {
        TCaseStringNormalizer normalizer(false);
        const TStringBuf first = normalizer.Normalize("AbC");
        UNIT_ASSERT_VALUES_EQUAL(first, TStringBuf("abc"));
        const TStringBuf second = normalizer.Normalize("XyZ");
        UNIT_ASSERT_VALUES_EQUAL(second, TStringBuf("xyz"));
        UNIT_ASSERT_VALUES_EQUAL(first, TStringBuf("xyz"));
    }

    Y_UNIT_TEST(CalcStringCaseSensitiveDifferentHashes) {
        constexpr ui64 seed = 42;
        TCaseAwareHashCalcer calcer(true);
        UNIT_ASSERT_VALUES_UNEQUAL(calcer.CalcString("ABC", seed), calcer.CalcString("abc", seed));
    }

    Y_UNIT_TEST(CalcStringCaseInsensitiveSameHash) {
        constexpr ui64 seed = 42;
        TCaseAwareHashCalcer calcer(false);
        UNIT_ASSERT_VALUES_EQUAL(calcer.CalcString("ABC", seed), calcer.CalcString("abc", seed));
        UNIT_ASSERT_VALUES_EQUAL(calcer.CalcString("AbC", seed), calcer.CalcString("aBc", seed));
    }

    Y_UNIT_TEST(CalcStringMatchesNormalizedHash) {
        constexpr ui64 seed = 7;
        TCaseAwareHashCalcer calcer(false);
        const ui64 expected = NArrow::NHash::TXX64::CalcSimple(TStringBuf("hello"), seed);
        UNIT_ASSERT_VALUES_EQUAL(calcer.CalcString("HeLLo", seed), expected);
    }

    Y_UNIT_TEST(CalcJsonScalarSameAsCalcString) {
        constexpr ui64 seed = 11;
        TCaseAwareHashCalcer calcer(false);
        UNIT_ASSERT_VALUES_EQUAL(calcer.CalcJsonScalar("AbC", seed), calcer.CalcString("AbC", seed));
    }

    Y_UNIT_TEST(CalcScalarStringCaseInsensitive) {
        constexpr ui64 seed = 3;
        TCaseAwareHashCalcer calcer(false);
        const auto lowerScalar = std::make_shared<arrow::StringScalar>("abc");
        const auto mixedScalar = std::make_shared<arrow::StringScalar>("AbC");
        UNIT_ASSERT_VALUES_EQUAL(calcer.CalcScalar(lowerScalar, seed), calcer.CalcScalar(mixedScalar, seed));
    }

    Y_UNIT_TEST(CalcScalarIntSameInBothModes) {
        constexpr ui64 seed = 5;
        const auto scalar = std::make_shared<arrow::Int32Scalar>(12345);
        TCaseAwareHashCalcer caseSensitiveCalcer(true);
        TCaseAwareHashCalcer caseInsensitiveCalcer(false);
        UNIT_ASSERT_VALUES_EQUAL(caseSensitiveCalcer.CalcScalar(scalar, seed), caseInsensitiveCalcer.CalcScalar(scalar, seed));
        UNIT_ASSERT_VALUES_EQUAL(caseSensitiveCalcer.CalcScalar(scalar, seed), NArrow::NHash::TXX64::CalcForScalar(scalar, seed));
    }

    Y_UNIT_TEST(CalcForAllStringArrayCaseInsensitive) {
        constexpr ui64 seed = 13;
        const auto array = MakeArrayNullable<arrow::StringType, std::string>({ "AbC", "abc", "XYZ", std::nullopt });
        const auto hashes = CollectHashes(array, seed, false);
        UNIT_ASSERT_VALUES_EQUAL(hashes[0], hashes[1]);
        UNIT_ASSERT_VALUES_UNEQUAL(hashes[0], hashes[2]);
        UNIT_ASSERT_VALUES_EQUAL(hashes[3], NArrow::NHash::TXX64::CalcSimple(TStringBuf(), seed));
    }

    Y_UNIT_TEST(CalcForAllIntArray) {
        constexpr ui64 seed = 17;
        const auto array = MakeInt32ArrayNullable({ 10, 20, std::nullopt });
        const auto caseSensitiveHashes = CollectHashes(array, seed, true);
        const auto caseInsensitiveHashes = CollectHashes(array, seed, false);
        UNIT_ASSERT_VALUES_EQUAL(caseSensitiveHashes, caseInsensitiveHashes);

        i32 firstValue = 10;
        i32 secondValue = 20;
        UNIT_ASSERT_VALUES_EQUAL(caseSensitiveHashes[0], NArrow::NHash::TXX64::CalcSimple(&firstValue, sizeof(firstValue), seed));
        UNIT_ASSERT_VALUES_EQUAL(caseSensitiveHashes[1], NArrow::NHash::TXX64::CalcSimple(&secondValue, sizeof(secondValue), seed));
        UNIT_ASSERT_VALUES_EQUAL(caseSensitiveHashes[2], NArrow::NHash::TXX64::CalcSimple(TStringBuf(), seed));
    }

    Y_UNIT_TEST(CalcForAllMatchesReferenceForStrings) {
        constexpr ui64 seed = 19;
        const auto array = MakeArrayNullable<arrow::StringType, std::string>({ "Hello", "WORLD", std::nullopt });
        const auto hashes = CollectHashes(array, seed, false);
        TCaseAwareHashCalcer calcer(false);
        for (ui32 idx = 0; idx < array->length(); ++idx) {
            if (array->IsNull(idx)) {
                UNIT_ASSERT_VALUES_EQUAL(hashes[idx], NArrow::NHash::TXX64::CalcSimple(TStringBuf(), seed));
            } else {
                const auto& stringArray = static_cast<const arrow::StringArray&>(*array);
                const auto view = stringArray.GetView(idx);
                UNIT_ASSERT_VALUES_EQUAL(hashes[idx], calcer.CalcString(TStringBuf(view.data(), view.size()), seed));
            }
        }
    }
}

}   // namespace NKikimr::NOlap::NIndexes
