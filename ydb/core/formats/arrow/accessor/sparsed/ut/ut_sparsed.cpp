#include <ydb/core/formats/arrow/accessor/sparsed/accessor.h>

#include <library/cpp/testing/unittest/registar.h>

#include <regex>

Y_UNIT_TEST_SUITE(SparsedArrayAccessor) {
    using namespace NKikimr::NArrow::NAccessor;
    using namespace NKikimr::NArrow;

    std::string PrepareToCompare(const std::string& str) {
        return std::regex_replace(str, std::regex(" |\\n"), "");
    }

    Y_UNIT_TEST(SlicesDef) {
        TSparsedArray::TSparsedBuilder<arrow::StringType> builder(std::make_shared<arrow::StringScalar>("aaa"), 10, 0);
        builder.AddRecord(5, "abc5");
        builder.AddRecord(6, "abcd6");
        builder.AddRecord(8, "abcde8");
        auto arr = builder.Finish(10);
        {
            const TString arrString = PrepareToCompare(arr->GetChunkedArray()->ToString());
            AFL_VERIFY(arrString == "[[\"aaa\",\"aaa\",\"aaa\",\"aaa\",\"aaa\"],[\"abc5\",\"abcd6\"],[\"aaa\"],[\"abcde8\"],[\"aaa\"]]")(
                "string", arrString);
            AFL_VERIFY(arr->GetScalar(3)->ToString() == "aaa");
        }
        {
            auto arrSlice = arr->ISlice(0, 4);
            AFL_VERIFY(arrSlice->GetRecordsCount() == 4);
            AFL_VERIFY(arrSlice->GetScalar(3)->ToString() == "aaa");
            const TString arrString = PrepareToCompare(arrSlice->GetChunkedArray()->ToString());
            AFL_VERIFY(arrString == "[[\"aaa\",\"aaa\",\"aaa\",\"aaa\"]]")("string", arrString);
        }
        {
            auto arrSlice = arr->ISlice(2, 4);
            AFL_VERIFY(arrSlice->GetRecordsCount() == 4);
            AFL_VERIFY(arrSlice->GetScalar(3)->ToString() == "abc5");
            const TString arrString = PrepareToCompare(arrSlice->GetChunkedArray()->ToString());
            AFL_VERIFY(arrString == "[[\"aaa\",\"aaa\",\"aaa\"],[\"abc5\"]]")("string", arrString);
        }
        {
            auto arrSlice = arr->ISlice(6, 3);
            AFL_VERIFY(arrSlice->GetRecordsCount() == 3);
            AFL_VERIFY(arrSlice->GetScalar(0)->ToString() == "abcd6");
            const TString arrString = PrepareToCompare(arrSlice->GetChunkedArray()->ToString());
            AFL_VERIFY(arrString == "[[\"abcd6\"],[\"aaa\"],[\"abcde8\"]]")("string", arrString);
        }
    }

    Y_UNIT_TEST(SlicesNull) {
        TSparsedArray::TSparsedBuilder<arrow::StringType> builder(nullptr, 10, 0);
        builder.AddRecord(5, "abc5");
        builder.AddRecord(6, "abcd6");
        builder.AddRecord(8, "abcde8");
        auto arr = builder.Finish(10);
        {
            const TString arrString = PrepareToCompare(arr->GetChunkedArray()->ToString());
            AFL_VERIFY(arrString == "[[null,null,null,null,null],[\"abc5\",\"abcd6\"],[null],[\"abcde8\"],[null]]")("string", arrString);
            AFL_VERIFY(!arr->GetScalar(3));
        }
        {
            auto arrSlice = arr->ISlice(0, 4);
            AFL_VERIFY(arrSlice->GetRecordsCount() == 4);
            AFL_VERIFY(!arrSlice->GetScalar(3));
            const TString arrString = PrepareToCompare(arrSlice->GetChunkedArray()->ToString());
            AFL_VERIFY(arrString == "[[null,null,null,null]]")("string", arrString);
        }
        {
            auto arrSlice = arr->ISlice(2, 4);
            AFL_VERIFY(arrSlice->GetRecordsCount() == 4);
            AFL_VERIFY(arrSlice->GetScalar(3)->ToString() == "abc5");
            AFL_VERIFY(!arrSlice->GetScalar(0));
            const TString arrString = PrepareToCompare(arrSlice->GetChunkedArray()->ToString());
            AFL_VERIFY(arrString == "[[null,null,null],[\"abc5\"]]")("string", arrString);
        }
        {
            auto arrSlice = arr->ISlice(6, 3);
            AFL_VERIFY(arrSlice->GetRecordsCount() == 3);
            AFL_VERIFY(arrSlice->GetScalar(0)->ToString() == "abcd6");
            const TString arrString = PrepareToCompare(arrSlice->GetChunkedArray()->ToString());
            AFL_VERIFY(arrString == "[[\"abcd6\"],[null],[\"abcde8\"]]")("string", arrString);
        }
    }
};
