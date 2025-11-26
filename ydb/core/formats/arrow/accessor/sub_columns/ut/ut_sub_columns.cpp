#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/data_extractor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/json_value_path.h>

#include <library/cpp/testing/unittest/registar.h>
#include <yql/essentials/types/binary_json/read.h>
#include <yql/essentials/types/binary_json/write.h>

#include <regex>

Y_UNIT_TEST_SUITE(SubColumnsArrayAccessor) {
    using namespace NKikimr::NArrow::NAccessor;
    using namespace NKikimr::NArrow;
    using namespace NKikimr;

    std::string PrepareToCompare(const std::string& str) {
        return std::regex_replace(str, std::regex(" |\\n"), "");
    }

    TString PrintBinaryJsons(const std::shared_ptr<arrow::ChunkedArray>& array) {
        TStringBuilder sb;
        sb << "[";
        for (auto&& i : array->chunks()) {
            sb << "[";
            AFL_VERIFY(i->type()->id() == arrow::binary()->id());
            auto views = std::static_pointer_cast<arrow::BinaryArray>(i);
            for (ui32 r = 0; r < views->length(); ++r) {
                if (views->IsNull(r)) {
                    sb << "null";
                } else {
                    sb << NBinaryJson::SerializeToJson(TStringBuf(views->GetView(r).data(), views->GetView(r).size()));
                }
                if (r + 1 != views->length()) {
                    sb << ",";
                }
            }
            sb << "]";
        }
        sb << "]";
        return sb;
    }

    Y_UNIT_TEST(EmptyOthers){
        auto arrEmpty = NSubColumns::TOthersData::BuildEmpty();
        auto arrSliceEmpty = arrEmpty.Slice(0, 1000, NSubColumns::TSettings());
        AFL_VERIFY(arrSliceEmpty.GetRecords()->num_rows() == 0);
        AFL_VERIFY(arrSliceEmpty.GetRecords()->GetColumnsCount() == (ui32)NSubColumns::TOthersData::GetSchema()->num_fields());
    }

    Y_UNIT_TEST(SlicesDef) {
        for (ui32 colsCount = 0; colsCount < 5; ++colsCount) {
            NSubColumns::TSettings settings(4, colsCount, 0, 0, NKikimr::NArrow::NAccessor::NSubColumns::TDataAdapterContainer::GetDefault());

            const std::vector<TString> jsons = {
                R"({"a" : 1, "b" : 1, "c" : "111"})",
                "null",
                R"({"a1" : 2, "b" : 2, "c" : "222"})",
                R"({"a" : 3, "b" : 3, "c" : "333"})",
                "null",
                R"({"a" : 5, "b1" : 5})",
            };

            TTrivialArray::TPlainBuilder<arrow::BinaryType> arrBuilder;
            ui32 idx = 0;
            for (auto&& i : jsons) {
                if (i != "null") {
                    auto v = NBinaryJson::SerializeToBinaryJson(i);
                    NBinaryJson::TBinaryJson* bJson = std::get_if<NBinaryJson::TBinaryJson>(&v);
                    arrBuilder.AddRecord(idx, std::string_view(bJson->data(), bJson->size()));
                }
                ++idx;
            }
            auto bJsonArr = arrBuilder.Finish(jsons.size());
            auto arrData = TSubColumnsArray::Make(bJsonArr, settings, bJsonArr->GetDataType()).DetachResult();
            Cerr << arrData->DebugJson() << Endl;
            AFL_VERIFY(PrintBinaryJsons(arrData->GetChunkedArray()) == R"([[{"a":1,"b":1,"c":"111"},null,{"a1":2,"b":2,"c":"222"},{"a":3,"b":3,"c":"333"},null,{"a":5,"b1":5}]])")(
                    "string", PrintBinaryJsons(arrData->GetChunkedArray()));
            {
                auto arrSlice = arrData->ISlice(1, 1);
                AFL_VERIFY(PrintBinaryJsons(arrSlice->GetChunkedArray()) == R"([[null]])")(
                    "string", PrintBinaryJsons(arrSlice->GetChunkedArray()));
            }
            {
                auto arrSlice = arrData->ISlice(5, 1);
                AFL_VERIFY(PrintBinaryJsons(arrSlice->GetChunkedArray()) == R"([[{"a":5,"b1":5}]])")(
                    "string", PrintBinaryJsons(arrSlice->GetChunkedArray()));
            }
            {
                auto arrSlice = arrData->ISlice(0, 6);
                AFL_VERIFY(PrintBinaryJsons(arrSlice->GetChunkedArray()) == R"([[{"a":1,"b":1,"c":"111"},null,{"a1":2,"b":2,"c":"222"},{"a":3,"b":3,"c":"333"},null,{"a":5,"b1":5}]])")(
                        "string", PrintBinaryJsons(arrSlice->GetChunkedArray()));
            }
            {
                auto arrSlice = arrData->ISlice(0, 5);
                AFL_VERIFY(PrintBinaryJsons(arrSlice->GetChunkedArray()) == R"([[{"a":1,"b":1,"c":"111"},null,{"a1":2,"b":2,"c":"222"},{"a":3,"b":3,"c":"333"},null]])")(
                        "string", PrintBinaryJsons(arrSlice->GetChunkedArray()));
            }
            {
                auto arrSlice = arrData->ISlice(0, 0);
                AFL_VERIFY(PrintBinaryJsons(arrSlice->GetChunkedArray()) == R"([])")("string", PrintBinaryJsons(arrSlice->GetChunkedArray()));
                AFL_VERIFY(arrSlice->DebugJson()["internal"]["columns_data"]["stats"].GetStringRobust() == R"({"accessor":[],"size":[],"key_names":[],"records":[]})")
                ("string", arrSlice->DebugJson().GetStringRobust());
                AFL_VERIFY(arrSlice->DebugJson()["internal"]["others_data"]["stats"].GetStringRobust() == R"({"accessor":[],"size":[],"key_names":[],"records":[]})")
                ("string", arrSlice->DebugJson().GetStringRobust());
            }
            {
                auto arrSlice = arrData->ISlice(0, 2);
                AFL_VERIFY(PrintBinaryJsons(arrSlice->GetChunkedArray()) == R"([[{"a":1,"b":1,"c":"111"},null]])")(
                    "string", PrintBinaryJsons(arrSlice->GetChunkedArray()));
                if (colsCount == 1) {
                    AFL_VERIFY(arrSlice->DebugJson()["internal"]["columns_data"]["stats"].GetStringRobust() == R"({"accessor":[1],"size":[33],"key_names":["a"],"records":[1]})")
                    ("string", arrSlice->DebugJson().GetStringRobust());
                    AFL_VERIFY(arrSlice->DebugJson()["internal"]["others_data"]["stats"].GetStringRobust() == R"({"accessor":[1,1],"size":[24,24],"key_names":["b","c"],"records":[1,1]})")
                    ("string", arrSlice->DebugJson().GetStringRobust());
                }
            }
            {
                auto arrSlice = arrData->ISlice(0, 3);
                AFL_VERIFY(PrintBinaryJsons(arrSlice->GetChunkedArray()) == R"([[{"a":1,"b":1,"c":"111"},null,{"a1":2,"b":2,"c":"222"}]])")(
                        "string", PrintBinaryJsons(arrSlice->GetChunkedArray()));
                if (colsCount == 1) {
                    AFL_VERIFY(arrSlice->DebugJson()["internal"]["columns_data"]["stats"].GetStringRobust() == R"({"accessor":[1],"size":[37],"key_names":["a"],"records":[1]})")
                    ("string", arrSlice->DebugJson().GetStringRobust());
                    AFL_VERIFY(arrSlice->DebugJson()["internal"]["others_data"]["stats"].GetStringRobust() == R"({"accessor":[1,1,1],"size":[24,48,48],"key_names":["a1","b","c"],"records":[1,2,2]})")
                    ("string", arrSlice->DebugJson().GetStringRobust());
                }
            }
            {
                auto arrSlice = arrData->ISlice(3, 3);
                AFL_VERIFY(PrintBinaryJsons(arrSlice->GetChunkedArray()) == R"([[{"a":3,"b":3,"c":"333"},null,{"a":5,"b1":5}]])")(
                    "string", PrintBinaryJsons(arrSlice->GetChunkedArray()));
                if (colsCount == 1) {
                    AFL_VERIFY(arrSlice->DebugJson()["internal"]["columns_data"]["stats"].GetStringRobust() == R"({"accessor":[1],"size":[61],"key_names":["a"],"records":[2]})")
                    ("string", arrSlice->DebugJson().GetStringRobust());
                    AFL_VERIFY(arrSlice->DebugJson()["internal"]["others_data"]["stats"].GetStringRobust() == R"({"accessor":[1,1,1],"size":[24,24,24],"key_names":["b","b1","c"],"records":[1,1,1]})")
                    ("string", arrSlice->DebugJson().GetStringRobust());
                }
            }
        }
    }

    Y_UNIT_TEST(FiltersDef) {
        for (ui32 colsCount = 0; colsCount < 5; ++colsCount) {
            NSubColumns::TSettings settings(4, colsCount, 0, 0, NKikimr::NArrow::NAccessor::NSubColumns::TDataAdapterContainer::GetDefault());

            const std::vector<TString> jsons = {
                R"({"a" : 1, "b" : 1, "c" : "111"})",
                "null",
                R"({"a1" : 2, "b" : 2, "c" : "222"})",
                R"({"a" : 3, "b" : 3, "c" : "333"})",
                "null",
                R"({"a" : 5, "b1" : 5})",
            };

            TTrivialArray::TPlainBuilder<arrow::BinaryType> arrBuilder;
            ui32 idx = 0;
            for (auto&& i : jsons) {
                if (i != "null") {
                    auto v = NBinaryJson::SerializeToBinaryJson(i);
                    NBinaryJson::TBinaryJson* bJson = std::get_if<NBinaryJson::TBinaryJson>(&v);
                    arrBuilder.AddRecord(idx, std::string_view(bJson->data(), bJson->size()));
                }
                ++idx;
            }
            auto bJsonArr = arrBuilder.Finish(jsons.size());
            auto arrData = TSubColumnsArray::Make(bJsonArr, settings, bJsonArr->GetDataType()).DetachResult();
            Cerr << arrData->DebugJson() << Endl;
            AFL_VERIFY(PrintBinaryJsons(arrData->GetChunkedArray()) == R"([[{"a":1,"b":1,"c":"111"},null,{"a1":2,"b":2,"c":"222"},{"a":3,"b":3,"c":"333"},null,{"a":5,"b1":5}]])")(
                    "string", PrintBinaryJsons(arrData->GetChunkedArray()));
            {
                TColumnFilter filter = TColumnFilter::BuildAllowFilter();
                filter.Add(true, 1);
                filter.Add(false, 1);
                filter.Add(true, 1);
                filter.Add(false, 1);
                filter.Add(true, 1);
                filter.Add(false, 1);
                auto arrSlice = filter.Apply(arrData);
                AFL_VERIFY(PrintBinaryJsons(arrSlice->GetChunkedArray()) == R"([[{"a":1,"b":1,"c":"111"},{"a1":2,"b":2,"c":"222"},null]])")(
                        "string", PrintBinaryJsons(arrSlice->GetChunkedArray()));
            }
            {
                TColumnFilter filter = TColumnFilter::BuildAllowFilter();
                filter.Add(false, 1);
                filter.Add(true, 1);
                filter.Add(false, 1);
                filter.Add(true, 1);
                filter.Add(false, 1);
                filter.Add(true, 1);
                auto arrSlice = filter.Apply(arrData);
                AFL_VERIFY(PrintBinaryJsons(arrSlice->GetChunkedArray()) == R"([[null,{"a":3,"b":3,"c":"333"},{"a":5,"b1":5}]])")(
                    "string", PrintBinaryJsons(arrSlice->GetChunkedArray()));
            }
            {
                TColumnFilter filter = TColumnFilter::BuildAllowFilter();
                filter.Add(false, 1);
                filter.Add(true, 3);
                filter.Add(false, 2);
                auto arrSlice = filter.Apply(arrData);
                AFL_VERIFY(PrintBinaryJsons(arrSlice->GetChunkedArray()) == R"([[null,{"a1":2,"b":2,"c":"222"},{"a":3,"b":3,"c":"333"}]])")(
                        "string", PrintBinaryJsons(arrSlice->GetChunkedArray()));
            }
            {
                TColumnFilter filter = TColumnFilter::BuildAllowFilter();
                filter.Add(false, 1);
                filter.Add(true, 1);
                filter.Add(false, 4);
                auto arrSlice = filter.Apply(arrData);
                AFL_VERIFY(PrintBinaryJsons(arrSlice->GetChunkedArray()) == R"([[null]])")(
                    "string", PrintBinaryJsons(arrSlice->GetChunkedArray()));
            }
            {
                TColumnFilter filter = TColumnFilter::BuildAllowFilter();
                filter.Add(true, 1);
                filter.Add(false, 5);
                auto arrSlice = filter.Apply(arrData);
                AFL_VERIFY(PrintBinaryJsons(arrSlice->GetChunkedArray()) == R"([[{"a":1,"b":1,"c":"111"}]])")(
                    "string", PrintBinaryJsons(arrSlice->GetChunkedArray()));
            }
        }
    }

    Y_UNIT_TEST(JsonRestorer) {
        NKikimr::NArrow::NAccessor::TJsonRestorer restorer;
        restorer.SetValueByPath("a", "b");
        restorer.SetValueByPath("b.c", "d");
        restorer.SetValueByPath(R"("d'".e)", "f");
        restorer.SetValueByPath(R"("g.h.".i)", "j");
        restorer.SetValueByPath(R"(".".k)", "l");
        restorer.SetValueByPath(R"("\"")", "o");
        restorer.SetValueByPath(R"("\'")", "p");

        NJson::TJsonValue expected;
        expected["a"] = "b";
        expected["b"]["c"] = "d";
        expected["d'"]["e"] = "f";
        expected["g.h."]["i"] = "j";
        expected["."]["k"] = "l";
        expected["\""] = "o";
        expected["'"] = "p";

        UNIT_ASSERT_VALUES_EQUAL(expected, restorer.GetResult());
    }

    Y_UNIT_TEST(SplitJsonPath) {
        TString path = R"($.a."b".'c'.'d"'."'"."\"".""."."[0,2].b[0].c[3][4].d[2 to 5].e[last])";
        TVector<TString> expectedItems = {"a", "b", "c", "d\"", "'", "\"", "", ".", "[0,2]", "b", "[0]", "c", "[3]", "[4]", "d", "[2 to 5]", "e", "[last]"};
        using enum NYql::NJsonPath::EJsonPathItemType;
        TVector<NYql::NJsonPath::EJsonPathItemType> expectedTypes = {MemberAccess, MemberAccess, MemberAccess, MemberAccess, MemberAccess, MemberAccess, MemberAccess, MemberAccess, ArrayAccess,
            MemberAccess, ArrayAccess, MemberAccess, ArrayAccess, ArrayAccess, MemberAccess, ArrayAccess, MemberAccess, ArrayAccess};
        TVector<NKikimr::NArrow::NAccessor::NSubColumns::TJsonPathBuf::size_type> expectedStartPositions = {1, 3, 7, 11, 16, 20, 25, 28, 32, 37, 39, 42, 44, 47, 50, 52, 60, 62};

        auto result = NKikimr::NArrow::NAccessor::NSubColumns::SplitJsonPath(path, NSubColumns::TJsonPathSplitSettings{.FillTypes = true, .FillStartPositions = true});
        UNIT_ASSERT_C(result.IsSuccess(), result.GetErrorMessage());
        const auto [pathItems, pathTypes, startPositions] = result.DetachResult();

        UNIT_ASSERT_VALUES_EQUAL(expectedItems, pathItems);
        UNIT_ASSERT_EQUAL(expectedTypes, pathTypes);
        UNIT_ASSERT_VALUES_EQUAL(expectedStartPositions, startPositions);
    }

    Y_UNIT_TEST(JsonPathTrie) {
        TVector<TString> testPaths = {"$.a.b", "$.b", "$.c.d"};
        TVector<std::shared_ptr<IChunkedArray>> testAccessors;
        NKikimr::NArrow::NAccessor::NSubColumns::TJsonPathAccessorTrie jsonPathAccessorTrie;

        for (const auto& path : testPaths) {
            testAccessors.emplace_back(TTrivialArray::BuildEmpty(std::make_shared<arrow::BinaryType>()));
            jsonPathAccessorTrie.Insert(path, testAccessors.back());
        }

        {
            for (decltype(testPaths)::size_type i = 0; i < testPaths.size(); ++i) {
                auto jsonPathAccessorResult = jsonPathAccessorTrie.GetAccessor(testPaths[i]);
                UNIT_ASSERT_C(jsonPathAccessorResult.IsSuccess(), testPaths[i] + " error: " + jsonPathAccessorResult.GetErrorMessage());
                UNIT_ASSERT(jsonPathAccessorResult->IsValid());
                UNIT_ASSERT_VALUES_EQUAL(testAccessors[i].get(), jsonPathAccessorResult->GetChunkedArrayAccessor().get());
                UNIT_ASSERT_VALUES_EQUAL(TString{}, jsonPathAccessorResult->GetRemainingPath());
            }
        }

        {
            TVector<TString> testShortPaths = {"$.a", "$.\"\"", "$.c"};
            for (const auto& path : testShortPaths) {
                auto jsonPathAccessorResult = jsonPathAccessorTrie.GetAccessor(path);
                UNIT_ASSERT_C(jsonPathAccessorResult.IsSuccess(), path + " error: " + jsonPathAccessorResult.GetErrorMessage());
                UNIT_ASSERT(!jsonPathAccessorResult->IsValid());
                UNIT_ASSERT_VALUES_EQUAL(nullptr, jsonPathAccessorResult->GetChunkedArrayAccessor().get());
                UNIT_ASSERT_VALUES_EQUAL(TString{}, jsonPathAccessorResult->GetRemainingPath());
            }
        }

        {
            TVector<TString> testLongPaths = {"$.a.b.e", "$.b.\"\".g[3].h", "$.c.d[54]"};
            TVector<TString> testLongRemainingPaths = {"$.e", "$.\"\".g[3].h", "$[54]"};
            for (decltype(testPaths)::size_type i = 0; i < testPaths.size(); ++i) {
                auto jsonPathAccessorResult = jsonPathAccessorTrie.GetAccessor(testLongPaths[i]);
                UNIT_ASSERT_C(jsonPathAccessorResult.IsSuccess(), testPaths[i] + " error: " + jsonPathAccessorResult.GetErrorMessage());
                UNIT_ASSERT(jsonPathAccessorResult->IsValid());
                UNIT_ASSERT_VALUES_EQUAL(testAccessors[i].get(), jsonPathAccessorResult->GetChunkedArrayAccessor().get());
                UNIT_ASSERT_VALUES_EQUAL(testLongRemainingPaths[i], jsonPathAccessorResult->GetRemainingPath());
            }
        }

        {
            TVector<TString> testInvalidPaths = {"$.a.b.[2]", "$.b.", "$.c[]"};
            for (decltype(testPaths)::size_type i = 0; i < testPaths.size(); ++i) {
                auto jsonPathAccessorResult = jsonPathAccessorTrie.GetAccessor(testInvalidPaths[i]);
                UNIT_ASSERT(jsonPathAccessorResult.IsFail());
            }
        }
    }
};
