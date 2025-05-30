#include <ydb/core/formats/arrow/accessor/common/chunk_data.h>
#include <ydb/core/formats/arrow/accessor/dictionary/accessor.h>
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
            AFL_VERIFY(PrepareToCompare(slice->GetVariants()->ToString()) == R"(["ab"])");
            AFL_VERIFY(PrepareToCompare(slice->GetRecords()->ToString()) == R"([null,0,null])");
        }
        {
            auto slice = std::static_pointer_cast<TDictionaryArray>(dict->ISlice(7, 2));
            AFL_VERIFY(slice->GetType() == IChunkedArray::EType::Dictionary);
            AFL_VERIFY(PrepareToCompare(slice->GetChunkedArray()->ToString()) == R"([[null,null]])");
            AFL_VERIFY(PrepareToCompare(slice->GetVariants()->ToString()) == R"([])");
            AFL_VERIFY(PrepareToCompare(slice->GetRecords()->ToString()) == R"([null,null])");
        }
        {
            auto slice = std::static_pointer_cast<TDictionaryArray>(dict->ISlice(2, 0));
            AFL_VERIFY(slice->GetType() == IChunkedArray::EType::Dictionary);
            AFL_VERIFY(PrepareToCompare(slice->GetChunkedArray()->ToString()) == R"([])");
            AFL_VERIFY(PrepareToCompare(slice->GetVariants()->ToString()) == R"([])");
            AFL_VERIFY(PrepareToCompare(slice->GetRecords()->ToString()) == R"([])");
        }
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
        auto dictParsed = std::static_pointer_cast<TDictionaryArray>(
            NDictionary::TConstructor().DeserializeFromString(NDictionary::TConstructor().SerializeToString(dict, info), info).DetachResult());
        Cerr << PrepareToCompare(dictParsed->GetChunkedArray()->ToString()) << Endl;
        Cerr << PrepareToCompare(dictParsed->GetRecords()->ToString()) << Endl;
        Cerr << PrepareToCompare(dictParsed->GetVariants()->ToString()) << Endl;
        AFL_VERIFY(PrepareToCompare(dictParsed->GetChunkedArray()->ToString()) == R"([["abc","abcd","abcd",null,"abc",null,"ab",null,"",null]])");
        AFL_VERIFY(PrepareToCompare(dictParsed->GetRecords()->ToString()) == R"([2,3,3,null,2,null,1,null,0,null])");
        AFL_VERIFY(PrepareToCompare(dictParsed->GetVariants()->ToString()) == R"(["","ab","abc","abcd"])");
    }
};
