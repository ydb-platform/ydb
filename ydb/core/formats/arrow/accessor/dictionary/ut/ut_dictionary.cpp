#include <ydb/core/formats/arrow/accessor/common/chunk_data.h>
#include <ydb/core/formats/arrow/accessor/dictionary/accessor.h>
#include <ydb/core/formats/arrow/accessor/dictionary/additional_data.h>
#include <ydb/core/formats/arrow/accessor/dictionary/constructor.h>
#include <ydb/core/formats/arrow/accessor/sparsed/accessor.h>
#include <ydb/core/formats/arrow/arrow_filter.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>

#include <ydb/library/actors/core/log.h>

#include <library/cpp/testing/unittest/registar.h>

#include <regex>

Y_UNIT_TEST_SUITE(DictionaryArrayAccessor) {
    using namespace NKikimr::NArrow::NAccessor;
    using namespace NKikimr::NArrow;

    std::string PrepareToCompare(const std::string& str) {
        return std::regex_replace(str, std::regex(" |\\n"), "");
    }

    Y_UNIT_TEST(NullsBuild) {
        TTrivialArray::TPlainBuilder builder;
        auto arr = builder.Finish(5);
        TChunkConstructionData info(
            arr->GetRecordsCount(), nullptr, arr->GetDataType(), NSerialization::TSerializerContainer::GetDefaultSerializer());
        auto dict = std::static_pointer_cast<TDictionaryArray>(NDictionary::TConstructor().Construct(arr, info).DetachResult());
        AFL_VERIFY(PrepareToCompare(dict->GetChunkedArray()->ToString()) == R"([[null,null,null,null,null]])");
    }

    Y_UNIT_TEST(SimpleBuild) {
        TTrivialArray::TPlainBuilder builder;
        builder.AddRecord(0, "abc");
        builder.AddRecord(1, "abcd");
        builder.AddRecord(2, "abcd");
        builder.AddRecord(4, "abc");
        builder.AddRecord(6, "ab");
        auto arr = builder.Finish(10);
        TChunkConstructionData info(
            arr->GetRecordsCount(), nullptr, arr->GetDataType(), NSerialization::TSerializerContainer::GetDefaultSerializer());
        auto dict = std::static_pointer_cast<TDictionaryArray>(NDictionary::TConstructor().Construct(arr, info).DetachResult());
        AFL_VERIFY(PrepareToCompare(dict->GetChunkedArray()->ToString()) == R"([["abc","abcd","abcd",null,"abc",null,"ab",null,null,null]])");
    }

    Y_UNIT_TEST(Slices) {
        TTrivialArray::TPlainBuilder builder;
        builder.AddRecord(0, "abc");
        builder.AddRecord(1, "abcd");
        builder.AddRecord(2, "abcd");
        builder.AddRecord(4, "abc");
        builder.AddRecord(6, "ab");
        auto arr = builder.Finish(10);
        TChunkConstructionData info(
            arr->GetRecordsCount(), nullptr, arr->GetDataType(), NSerialization::TSerializerContainer::GetDefaultSerializer());
        auto dict = std::static_pointer_cast<TDictionaryArray>(NDictionary::TConstructor().Construct(arr, info).DetachResult());
        {
            auto slice = std::static_pointer_cast<TDictionaryArray>(dict->ISlice(5, 3));
            AFL_VERIFY(slice->GetType() == IChunkedArray::EType::Dictionary);
            AFL_VERIFY(PrepareToCompare(slice->GetChunkedArray()->ToString()) == R"([[null,"ab",null]])");
            AFL_VERIFY(PrepareToCompare(slice->GetDictionary()->ToString()) == R"(["ab"])");
            AFL_VERIFY(PrepareToCompare(slice->GetPositions()->ToString()) == R"([null,0,null])");
        }
        {
            auto slice = std::static_pointer_cast<TDictionaryArray>(dict->ISlice(7, 2));
            AFL_VERIFY(slice->GetType() == IChunkedArray::EType::Dictionary);
            AFL_VERIFY(PrepareToCompare(slice->GetChunkedArray()->ToString()) == R"([[null,null]])");
            AFL_VERIFY(PrepareToCompare(slice->GetDictionary()->ToString()) == R"([])");
            AFL_VERIFY(PrepareToCompare(slice->GetPositions()->ToString()) == R"([null,null])");
        }
        {
            auto slice = std::static_pointer_cast<TDictionaryArray>(dict->ISlice(2, 0));
            AFL_VERIFY(slice->GetType() == IChunkedArray::EType::Dictionary);
            AFL_VERIFY(PrepareToCompare(slice->GetChunkedArray()->ToString()) == R"([])");
            AFL_VERIFY(PrepareToCompare(slice->GetDictionary()->ToString()) == R"([])");
            AFL_VERIFY(PrepareToCompare(slice->GetPositions()->ToString()) == R"([])");
        }
    }

    // Slice that uses a non-consecutive subset of the dictionary (e.g. indices 1 and 2 but not 0).
    // Without position remapping, positions would point into the original dictionary while the
    // slice holds only the filtered dictionary -> wrong values or OOB. Verifies DoISlice remaps
    // positions to indices into the filtered dictionary.
    Y_UNIT_TEST(SliceSubsetOfDictionaryRemapsPositions) {
        TTrivialArray::TPlainBuilder builder;
        builder.AddRecord(0, "A");
        builder.AddRecord(1, "B");
        builder.AddRecord(2, "C");
        builder.AddRecord(3, "A");
        auto arr = builder.Finish(4);
        TChunkConstructionData info(
            arr->GetRecordsCount(), nullptr, arr->GetDataType(), NSerialization::TSerializerContainer::GetDefaultSerializer());
        auto dict = std::static_pointer_cast<TDictionaryArray>(NDictionary::TConstructor().Construct(arr, info).DetachResult());
        // Full dict order (map iteration): A, B, C -> indices 0, 1, 2. Positions: [0, 1, 2, 0].
        // Slice(1, 2) -> positions [1, 2] -> we use dict indices 1 and 2 (B and C), not 0.
        // Filtered dict = [B, C]. Old 1->0, 2->1. Positions must be remapped to [0, 1].
        auto slice = std::static_pointer_cast<TDictionaryArray>(dict->ISlice(1, 2));
        AFL_VERIFY(slice->GetType() == IChunkedArray::EType::Dictionary);
        Cerr << PrepareToCompare(slice->GetChunkedArray()->ToString()) << Endl;
        Cerr << PrepareToCompare(slice->GetDictionary()->ToString()) << Endl;
        Cerr << PrepareToCompare(slice->GetPositions()->ToString()) << Endl;
        AFL_VERIFY(PrepareToCompare(slice->GetChunkedArray()->ToString()) == R"([["B","C"]])");
        AFL_VERIFY(PrepareToCompare(slice->GetDictionary()->ToString()) == R"(["B","C"])");
        AFL_VERIFY(PrepareToCompare(slice->GetPositions()->ToString()) == R"([0,1])");
    }

    Y_UNIT_TEST(Serialization) {
        TTrivialArray::TPlainBuilder builder;
        builder.AddRecord(0, "abc");
        builder.AddRecord(1, "abcd");
        builder.AddRecord(2, "abcd");
        builder.AddRecord(4, "abc");
        builder.AddRecord(6, "ab");
        builder.AddRecord(8, "");
        auto arr = builder.Finish(10);
        TChunkConstructionData info(
            arr->GetRecordsCount(), nullptr, arr->GetDataType(), NSerialization::TSerializerContainer::GetDefaultSerializer());
        auto dict = std::static_pointer_cast<TDictionaryArray>(NDictionary::TConstructor().Construct(arr, info).DetachResult());
        auto blobAndMeta = NDictionary::TConstructor::SerializeToBlobAndMeta(dict, info);
        TChunkConstructionData infoWithMeta(
            arr->GetRecordsCount(), nullptr, arr->GetDataType(), NSerialization::TSerializerContainer::GetDefaultSerializer(),
            std::nullopt, blobAndMeta.Meta);
        auto dictParsed = std::static_pointer_cast<TDictionaryArray>(
            NDictionary::TConstructor().DeserializeFromString(blobAndMeta.Blob, infoWithMeta).DetachResult());
        Cerr << PrepareToCompare(dictParsed->GetChunkedArray()->ToString()) << Endl;
        Cerr << PrepareToCompare(dictParsed->GetPositions()->ToString()) << Endl;
        Cerr << PrepareToCompare(dictParsed->GetDictionary()->ToString()) << Endl;
        AFL_VERIFY(PrepareToCompare(dictParsed->GetChunkedArray()->ToString()) == R"([["abc","abcd","abcd",null,"abc",null,"ab",null,"",null]])");
        AFL_VERIFY(PrepareToCompare(dictParsed->GetPositions()->ToString()) == R"([2,3,3,null,2,null,1,null,0,null])");
        AFL_VERIFY(PrepareToCompare(dictParsed->GetDictionary()->ToString()) == R"(["","ab","abc","abcd",null])");
    }

    Y_UNIT_TEST(BuildDictionaryOnlyReader) {
        TTrivialArray::TPlainBuilder builder;
        builder.AddRecord(0, "abc");
        builder.AddRecord(1, "abcd");
        builder.AddRecord(2, "abcd");
        builder.AddRecord(4, "abc");
        builder.AddRecord(6, "ab");
        builder.AddRecord(8, "");
        auto arr = builder.Finish(10);
        TChunkConstructionData info(
            arr->GetRecordsCount(), nullptr, arr->GetDataType(), NSerialization::TSerializerContainer::GetDefaultSerializer());
        auto dict = std::static_pointer_cast<TDictionaryArray>(NDictionary::TConstructor().Construct(arr, info).DetachResult());
        auto blobAndMeta = NDictionary::TConstructor::SerializeToBlobAndMeta(dict, info);
        const auto* dictData = dynamic_cast<const TDictionaryAccessorData*>(blobAndMeta.Meta.get());
        AFL_VERIFY(dictData);
        TString dictionaryBlobOnly(blobAndMeta.Blob.data(), dictData->DictionaryBlobSize);
        TChunkConstructionData infoWithMeta(
            arr->GetRecordsCount(), nullptr, arr->GetDataType(), NSerialization::TSerializerContainer::GetDefaultSerializer(),
            std::nullopt, blobAndMeta.Meta);
        auto conclusion = NDictionary::TConstructor::BuildDictionaryOnlyReader(dictionaryBlobOnly, infoWithMeta);
        AFL_VERIFY(conclusion.IsSuccess())("error", conclusion.GetErrorMessage());
        auto dictionaryArray = conclusion.DetachResult();
        AFL_VERIFY(PrepareToCompare(dictionaryArray->ToString()) == PrepareToCompare(dict->GetDictionary()->ToString()));
        AFL_VERIFY(dictionaryArray->length() == dict->GetDictionary()->length());
    }

    Y_UNIT_TEST(ManyDistinctValuesUint16Positions) {
        // Use > 255 distinct values so positions are serialized as uint16 (not uint8).
        constexpr ui32 NumDistinct = 300;
        constexpr ui32 NumRecords = 320;
        TTrivialArray::TPlainBuilder builder(NumRecords, NumRecords * 16);
        for (ui32 i = 0; i < NumRecords; ++i) {
            if (i == 50 || i == 150 || i == 250) {
                continue; // nulls
            }
            TString v = "v" + ToString(i % NumDistinct);
            builder.AddRecord(i, v);
        }
        auto arr = builder.Finish(NumRecords);
        TChunkConstructionData info(
            arr->GetRecordsCount(), nullptr, arr->GetDataType(), NSerialization::TSerializerContainer::GetDefaultSerializer());
        auto dict = std::static_pointer_cast<TDictionaryArray>(NDictionary::TConstructor().Construct(arr, info).DetachResult());
        // Dictionary has 300 distinct values + null slot = 301 variants -> uint16 positions
        AFL_VERIFY(dict->GetPositions()->type()->id() == arrow::Type::UINT16);

        auto blobAndMeta = NDictionary::TConstructor::SerializeToBlobAndMeta(dict, info);
        const auto* dictData = dynamic_cast<const TDictionaryAccessorData*>(blobAndMeta.Meta.get());
        AFL_VERIFY(dictData);
        AFL_VERIFY(dictData->PositionsBlobSize > 0);

        TChunkConstructionData infoWithMeta(
            arr->GetRecordsCount(), nullptr, arr->GetDataType(), NSerialization::TSerializerContainer::GetDefaultSerializer(),
            std::nullopt, blobAndMeta.Meta);
        auto dictParsed = std::static_pointer_cast<TDictionaryArray>(
            NDictionary::TConstructor().DeserializeFromString(blobAndMeta.Blob, infoWithMeta).DetachResult());

        AFL_VERIFY(dictParsed->GetPositions()->type()->id() == arrow::Type::UINT16);
        AFL_VERIFY(dictParsed->GetDictionary()->length() == dict->GetDictionary()->length());
        AFL_VERIFY(dictParsed->GetChunkedArray()->length() == NumRecords);
        AFL_VERIFY(PrepareToCompare(dictParsed->GetChunkedArray()->ToString()) == PrepareToCompare(dict->GetChunkedArray()->ToString()));
    }

    // Corner cases around 254/255/256 variants: with and without nulls. Ensures positions type (uint8 vs uint16) and null slot are correct.
    void RunCornerCaseVariants(ui32 numDistinct, bool withNulls, arrow::Type::type expectedPositionsType) {
        const ui32 numRecords = numDistinct + (withNulls ? 1 : 0);
        TTrivialArray::TPlainBuilder builder(numRecords, numRecords * 16);
        for (ui32 i = 0; i < numRecords; ++i) {
            if (withNulls && i == numDistinct) {
                continue;
            }
            TString v = "v" + ToString(i % numDistinct);
            builder.AddRecord(i, v);
        }
        auto arr = builder.Finish(numRecords);
        TChunkConstructionData info(
            arr->GetRecordsCount(), nullptr, arr->GetDataType(), NSerialization::TSerializerContainer::GetDefaultSerializer());
        auto dict = std::static_pointer_cast<TDictionaryArray>(NDictionary::TConstructor().Construct(arr, info).DetachResult());
        AFL_VERIFY(dict->GetPositions()->type()->id() == expectedPositionsType)
            ("numDistinct", numDistinct)("withNulls", withNulls)("expected", static_cast<int>(expectedPositionsType))("actual", static_cast<int>(dict->GetPositions()->type()->id()));
        AFL_VERIFY(dict->GetDictionary()->length() == numDistinct + (withNulls ? 1 : 0));

        auto blobAndMeta = NDictionary::TConstructor::SerializeToBlobAndMeta(dict, info);
        TChunkConstructionData infoWithMeta(
            arr->GetRecordsCount(), nullptr, arr->GetDataType(), NSerialization::TSerializerContainer::GetDefaultSerializer(),
            std::nullopt, blobAndMeta.Meta);
        auto dictParsed = std::static_pointer_cast<TDictionaryArray>(
            NDictionary::TConstructor().DeserializeFromString(blobAndMeta.Blob, infoWithMeta).DetachResult());
        AFL_VERIFY(dictParsed->GetPositions()->type()->id() == expectedPositionsType);
        AFL_VERIFY(dictParsed->GetDictionary()->length() == dict->GetDictionary()->length());
        AFL_VERIFY(PrepareToCompare(dictParsed->GetChunkedArray()->ToString()) == PrepareToCompare(dict->GetChunkedArray()->ToString()));
    }

    Y_UNIT_TEST(CornerCase254NoNulls) {
        RunCornerCaseVariants(254, false, arrow::Type::UINT8); // 254 variants <= 255 -> uint8
    }
    Y_UNIT_TEST(CornerCase254WithNulls) {
        RunCornerCaseVariants(254, true, arrow::Type::UINT8); // 255 variants (254 + null), 255 <= 255 -> uint8
    }
    Y_UNIT_TEST(CornerCase255NoNulls) {
        RunCornerCaseVariants(255, false, arrow::Type::UINT8); // 255 variants <= 255 -> uint8
    }
    Y_UNIT_TEST(CornerCase255WithNulls) {
        RunCornerCaseVariants(255, true, arrow::Type::UINT16); // 256 variants > 255 -> uint16
    }
    Y_UNIT_TEST(CornerCase256NoNulls) {
        RunCornerCaseVariants(256, false, arrow::Type::UINT16); // 256 variants > 255 -> uint16
    }
    Y_UNIT_TEST(CornerCase256WithNulls) {
        RunCornerCaseVariants(256, true, arrow::Type::UINT16); // 257 variants -> uint16
    }
};
