#include <ydb/core/formats/arrow/accessor/composite/accessor.h>
#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/arrow_filter.h>

#include <library/cpp/testing/unittest/registar.h>

#include <regex>

Y_UNIT_TEST_SUITE(CompositeArrayAccessor) {
    using namespace NKikimr::NArrow::NAccessor;
    using namespace NKikimr::NArrow;

    std::string PrepareToCompare(const std::string& str) {
        return std::regex_replace(str, std::regex(" |\\n"), "");
    }

    static std::shared_ptr<IChunkedArray> BuildCompositeArray() {
        TCompositeChunkedArray::TBuilder builder(arrow::utf8());
        {
            TTrivialArray::TPlainBuilder arrBuilder;
            arrBuilder.AddRecord(1, "a1");
            arrBuilder.AddRecord(3, "a3");
            arrBuilder.AddRecord(7, "a7");
            builder.AddChunk(arrBuilder.Finish(10));
        }
        {
            TTrivialArray::TPlainBuilder arrBuilder;
            arrBuilder.AddRecord(1, "b1");
            arrBuilder.AddRecord(2, "b2");
            arrBuilder.AddRecord(3, "b3");
            builder.AddChunk(arrBuilder.Finish(5));
        }
        {
            TTrivialArray::TPlainBuilder arrBuilder;
            arrBuilder.AddRecord(0, "c0");
            arrBuilder.AddRecord(2, "c2");
            arrBuilder.AddRecord(3, "c3");
            builder.AddChunk(arrBuilder.Finish(4));
        }
        return builder.Finish();
    }

    Y_UNIT_TEST(SlicesSimple) {
        auto arr = BuildCompositeArray();
        Cerr << PrepareToCompare(arr->GetChunkedArray()->ToString()) << Endl;
        {
            auto slice = arr->ISlice(0, 10);
            const TString arrString = PrepareToCompare(slice->GetChunkedArray()->ToString());
            AFL_VERIFY(arrString == R"([[null,"a1",null,"a3",null,null,null,"a7",null,null]])")("string", arrString);
        }
        {
            auto slice = arr->ISlice(3, 8);
            const TString arrString = PrepareToCompare(slice->GetChunkedArray()->ToString());
            AFL_VERIFY(arrString == R"([["a3",null,null,null,"a7",null,null],[null]])")("string", arrString);
        }
        {
            auto slice = arr->ISlice(3, 16);
            const TString arrString = PrepareToCompare(slice->GetChunkedArray()->ToString());
            AFL_VERIFY(arrString == R"([["a3",null,null,null,"a7",null,null],[null,"b1","b2","b3",null],["c0",null,"c2","c3"]])")("string", arrString);
        }
        {
            auto slice = arr->ISlice(8, 11);
            const TString arrString = PrepareToCompare(slice->GetChunkedArray()->ToString());
            AFL_VERIFY(arrString == R"([[null,null],[null,"b1","b2","b3",null],["c0",null,"c2","c3"]])")("string", arrString);
        }
        {
            auto slice = arr->ISlice(8, 2);
            const TString arrString = PrepareToCompare(slice->GetChunkedArray()->ToString());
            AFL_VERIFY(arrString == R"([[null,null]])")("string", arrString);
        }
        {
            auto slice = arr->ISlice(9, 3);
            const TString arrString = PrepareToCompare(slice->GetChunkedArray()->ToString());
            AFL_VERIFY(arrString == R"([[null],[null,"b1"]])")("string", arrString);
        }
    }

    Y_UNIT_TEST(FilterSimple) {
        auto arr = BuildCompositeArray();
        {
            const TColumnFilter filter = TColumnFilter::BuildConstFilter(true, { 1, 2, 3, 4, 5, 4 });

            auto filtered = arr->ApplyFilter(filter, arr);
            const TString arrString = PrepareToCompare(filtered->GetChunkedArray()->ToString());
            AFL_VERIFY(arrString == R"([[null,"a3",null,null],[null,"b1","b2","b3",null]])")("string", arrString);
        }
        {
            const TColumnFilter filter = TColumnFilter::BuildConstFilter(false, { 1, 2, 3, 4, 5, 4 });

            auto filtered = arr->ApplyFilter(filter, arr);
            const TString arrString = PrepareToCompare(filtered->GetChunkedArray()->ToString());
            AFL_VERIFY(arrString == R"([["a1",null,null,"a7",null,null],["c0",null,"c2","c3"]])")("string", arrString);
        }
        {
            const TColumnFilter filter = TColumnFilter::BuildConstFilter(false, { 3, 1, 3, 1, 3, 1, 3, 1, 3 });

            auto filtered = arr->ApplyFilter(filter, arr);
            const TString arrString = PrepareToCompare(filtered->GetChunkedArray()->ToString());
            AFL_VERIFY(arrString == R"([["a3","a7"],["b1"],["c0"]])")("string", arrString);
        }
    }
};
