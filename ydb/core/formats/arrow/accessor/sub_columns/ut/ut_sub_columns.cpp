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
                R"({"a" : 1, "b" : 1, "c" : "1111"})",
                "null",
                R"({"a1" : 2, "b" : 2, "c" : "2222"})",
                R"({"a" : 3, "b" : 3, "c" : "3333"})",
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
            Cerr << "Original data: " << arrData->DebugJson() << Endl;
            UNIT_ASSERT_VALUES_EQUAL(PrintBinaryJsons(arrData->GetChunkedArray()), R"([[{"a":1,"b":1,"c":"1111"},null,{"a1":2,"b":2,"c":"2222"},{"a":3,"b":3,"c":"3333"},null,{"a":5,"b1":5}]])");
            {
                auto arrSlice = arrData->ISlice(1, 1);
                UNIT_ASSERT_VALUES_EQUAL(PrintBinaryJsons(arrSlice->GetChunkedArray()), R"([[null]])");
            }
            {
                auto arrSlice = arrData->ISlice(5, 1);
                UNIT_ASSERT_VALUES_EQUAL(PrintBinaryJsons(arrSlice->GetChunkedArray()), R"([[{"a":5,"b1":5}]])");
            }
            {
                auto arrSlice = arrData->ISlice(0, 6);
                UNIT_ASSERT_VALUES_EQUAL(PrintBinaryJsons(arrSlice->GetChunkedArray()), R"([[{"a":1,"b":1,"c":"1111"},null,{"a1":2,"b":2,"c":"2222"},{"a":3,"b":3,"c":"3333"},null,{"a":5,"b1":5}]])");
            }
            {
                auto arrSlice = arrData->ISlice(0, 5);
                UNIT_ASSERT_VALUES_EQUAL(PrintBinaryJsons(arrSlice->GetChunkedArray()), R"([[{"a":1,"b":1,"c":"1111"},null,{"a1":2,"b":2,"c":"2222"},{"a":3,"b":3,"c":"3333"},null]])");
            }
            {
                auto arrSlice = arrData->ISlice(0, 0);
                UNIT_ASSERT_VALUES_EQUAL(PrintBinaryJsons(arrSlice->GetChunkedArray()), R"([])");
                UNIT_ASSERT_VALUES_EQUAL(arrSlice->DebugJson()["internal"]["columns_data"]["stats"].GetStringRobust(), R"({"accessor":[],"size":[],"key_names":[],"records":[]})");
                UNIT_ASSERT_VALUES_EQUAL(arrSlice->DebugJson()["internal"]["others_data"]["stats"].GetStringRobust(), R"({"accessor":[],"size":[],"key_names":[],"records":[]})");
            }
            {
                auto arrSlice = arrData->ISlice(0, 2);
                UNIT_ASSERT_VALUES_EQUAL(PrintBinaryJsons(arrSlice->GetChunkedArray()), R"([[{"a":1,"b":1,"c":"1111"},null]])");
                if (colsCount == 1) {
                    UNIT_ASSERT_VALUES_EQUAL(arrSlice->DebugJson()["internal"]["columns_data"]["stats"].GetStringRobust(), R"({"accessor":[1],"size":[34],"key_names":["\"c\""],"records":[1]})");
                    UNIT_ASSERT_VALUES_EQUAL(arrSlice->DebugJson()["internal"]["others_data"]["stats"].GetStringRobust(), R"({"accessor":[1,1],"size":[24,24],"key_names":["\"a\"","\"b\""],"records":[1,1]})");
                }
            }
            {
                auto arrSlice = arrData->ISlice(0, 3);
                UNIT_ASSERT_VALUES_EQUAL(PrintBinaryJsons(arrSlice->GetChunkedArray()), R"([[{"a":1,"b":1,"c":"1111"},null,{"a1":2,"b":2,"c":"2222"}]])");
                if (colsCount == 1) {
                    UNIT_ASSERT_VALUES_EQUAL(arrSlice->DebugJson()["internal"]["columns_data"]["stats"].GetStringRobust(), R"({"accessor":[1],"size":[63],"key_names":["\"c\""],"records":[2]})");
                    UNIT_ASSERT_VALUES_EQUAL(arrSlice->DebugJson()["internal"]["others_data"]["stats"].GetStringRobust(), R"({"accessor":[1,1,1],"size":[24,24,48],"key_names":["\"a\"","\"a1\"","\"b\""],"records":[1,1,2]})");
                }
            }
            {
                auto arrSlice = arrData->ISlice(3, 3);
                UNIT_ASSERT_VALUES_EQUAL(PrintBinaryJsons(arrSlice->GetChunkedArray()), R"([[{"a":3,"b":3,"c":"3333"},null,{"a":5,"b1":5}]])");
                if (colsCount == 1) {
                    UNIT_ASSERT_VALUES_EQUAL(arrSlice->DebugJson()["internal"]["columns_data"]["stats"].GetStringRobust(), R"({"accessor":[1],"size":[38],"key_names":["\"c\""],"records":[1]})");
                    UNIT_ASSERT_VALUES_EQUAL(arrSlice->DebugJson()["internal"]["others_data"]["stats"].GetStringRobust(), R"({"accessor":[1,1,1],"size":[48,24,24],"key_names":["\"a\"","\"b\"","\"b1\""],"records":[2,1,1]})");
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
        TVector<ui64> testCookies;
        NKikimr::NArrow::NAccessor::NSubColumns::TJsonPathAccessorTrie jsonPathAccessorTrie;

        {
            ui64 testCookie = 0;
            for (const auto& path : testPaths) {
                testAccessors.emplace_back(TTrivialArray::BuildEmpty(std::make_shared<arrow::BinaryType>()));
                testCookies.emplace_back(testCookie++);
                UNIT_ASSERT(jsonPathAccessorTrie.Insert(path, testAccessors.back(), testCookies.back()).IsSuccess());
            }
        }

        {
            for (decltype(testPaths)::size_type i = 0; i < testPaths.size(); ++i) {
                auto jsonPathAccessorResult = jsonPathAccessorTrie.GetAccessor(testPaths[i]);
                UNIT_ASSERT_C(jsonPathAccessorResult.IsSuccess(), testPaths[i] + " error: " + jsonPathAccessorResult.GetErrorMessage());
                auto jsonPathAccessor = jsonPathAccessorResult.DetachResult();
                UNIT_ASSERT(jsonPathAccessor->IsValid());
                UNIT_ASSERT_VALUES_EQUAL(testAccessors[i].get(), jsonPathAccessor->GetChunkedArrayAccessor().get());
                UNIT_ASSERT_VALUES_EQUAL(testCookies[i], jsonPathAccessor->GetCookie());
                UNIT_ASSERT_VALUES_EQUAL(TString{}, jsonPathAccessor->GetRemainingPath());
            }
        }

        {
            TVector<TString> testShortPaths = {"$.a", "$.\"\"", "$.c"};
            for (const auto& path : testShortPaths) {
                auto jsonPathAccessorResult = jsonPathAccessorTrie.GetAccessor(path);
                UNIT_ASSERT_C(jsonPathAccessorResult.IsSuccess(), path + " error: " + jsonPathAccessorResult.GetErrorMessage());
                auto jsonPathAccessor = jsonPathAccessorResult.DetachResult();
                UNIT_ASSERT(!jsonPathAccessor->IsValid());
                UNIT_ASSERT_VALUES_EQUAL(nullptr, jsonPathAccessor->GetChunkedArrayAccessor().get());
                UNIT_ASSERT_VALUES_EQUAL(std::optional<ui64>{}, jsonPathAccessor->GetCookie());
                UNIT_ASSERT_VALUES_EQUAL(TString{}, jsonPathAccessor->GetRemainingPath());
            }
        }

        {
            TVector<TString> testLongPaths = {"$.a.b.e", "$.b.\"\".g[3].h", "$.c.d[54]"};
            TVector<TString> testLongRemainingPaths = {"strict $.e", "strict $.\"\".g[3].h", "strict $[54]"};
            for (decltype(testPaths)::size_type i = 0; i < testPaths.size(); ++i) {
                auto jsonPathAccessorResult = jsonPathAccessorTrie.GetAccessor(testLongPaths[i]);
                UNIT_ASSERT_C(jsonPathAccessorResult.IsSuccess(), testPaths[i] + " error: " + jsonPathAccessorResult.GetErrorMessage());
                auto jsonPathAccessor = jsonPathAccessorResult.DetachResult();
                UNIT_ASSERT(jsonPathAccessor->IsValid());
                UNIT_ASSERT_VALUES_EQUAL(testAccessors[i].get(), jsonPathAccessor->GetChunkedArrayAccessor().get());
                UNIT_ASSERT_VALUES_EQUAL(testCookies[i], jsonPathAccessor->GetCookie());
                UNIT_ASSERT_VALUES_EQUAL(testLongRemainingPaths[i], jsonPathAccessor->GetRemainingPath());
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

    std::shared_ptr<TTrivialArray> CreateTrivialArrayAccessor(TStringBuf data) {
        auto binaryJsonResult = NBinaryJson::SerializeToBinaryJson(data);
        UNIT_ASSERT(std::holds_alternative<NBinaryJson::TBinaryJson>(binaryJsonResult));

        auto binaryJson = std::get<NBinaryJson::TBinaryJson>(binaryJsonResult);
        return std::make_shared<TTrivialArray>(NKikimr::NArrow::NAccessor::TTrivialArray::BuildArrayFromScalar(
            std::make_shared<arrow::BinaryScalar>(std::make_shared<arrow::Buffer>((const ui8*)binaryJson.data(), binaryJson.size()), arrow::binary())));
    }

    void CheckValueByPath(const NKikimr::NArrow::NAccessor::NSubColumns::TJsonPathAccessorTrie& jsonPathAccessorTrie, TStringBuf path, std::optional<TStringBuf> expected) {
        auto jsonPathAccessorResult = jsonPathAccessorTrie.GetAccessor(path);
        UNIT_ASSERT_C(jsonPathAccessorResult.IsSuccess(), TString(path) + " error: " + jsonPathAccessorResult.GetErrorMessage());
        auto jsonPathAccessor = jsonPathAccessorResult.DetachResult();
        UNIT_ASSERT(jsonPathAccessor->IsValid());

        int callsCount = 0;
        jsonPathAccessor->VisitValues([&](const std::optional<TStringBuf>& value) {
            UNIT_ASSERT_VALUES_EQUAL_C(expected, value, TString(path));
            callsCount++;
            UNIT_ASSERT_VALUES_EQUAL(1, callsCount);
        });
    }

    Y_UNIT_TEST(JsonPathAccessorObject) {
        auto accessor = CreateTrivialArrayAccessor(
            R"({"root_integer": 1, "root_string": "a", "root_true": true, "root_false": false, "root_null": null, "root_object": {"a": "b"}, "root_array": ["a", 1, true, false, null, {}, [], [1, 2]]})");

        NKikimr::NArrow::NAccessor::NSubColumns::TJsonPathAccessorTrie jsonPathAccessorTrie;
        UNIT_ASSERT(jsonPathAccessorTrie.Insert("$.a", accessor).IsSuccess());

        // Non-existing paths and root path must return std::nullopt and called only once for our binary JSON
        // Object, array, null must return std::nullopt
        {
            for (const auto& path : {"$.a", "$.a[0]", "$.a.h", "$.a.e.p", "$.a.f.z", "$.a.root_object", "$.a.root_array", "$.a.root_null", "$.a.root_array[4]", "$.a.root_array[5]",
                     "$.a.root_array[6]", "$.a.root_array[7]", "$.a.root_array[7][4]", "$.a.root_array[10]"}) {
                CheckValueByPath(jsonPathAccessorTrie, path, std::nullopt);
            }
        }

        // Root scalars
        {
            CheckValueByPath(jsonPathAccessorTrie, "$.a.root_string", "a");
            CheckValueByPath(jsonPathAccessorTrie, "$.a.root_integer", "1");
            CheckValueByPath(jsonPathAccessorTrie, "$.a.root_true", "true");
            CheckValueByPath(jsonPathAccessorTrie, "$.a.root_false", "false");
        }

        // Non-root scalars
        {
            CheckValueByPath(jsonPathAccessorTrie, "$.a.root_object.a", "b");
            CheckValueByPath(jsonPathAccessorTrie, "$.a.root_array[0]", "a");
            CheckValueByPath(jsonPathAccessorTrie, "$.a.root_array[1]", "1");
            CheckValueByPath(jsonPathAccessorTrie, "$.a.root_array[2]", "true");
            CheckValueByPath(jsonPathAccessorTrie, "$.a.root_array[3]", "false");
            CheckValueByPath(jsonPathAccessorTrie, "$.a.root_array[7][0]", "1");
            CheckValueByPath(jsonPathAccessorTrie, "$.a.root_array[7][1]", "2");
        }

        // Different quotes
        {
            for (const auto& path : {"$.a.root_integer", "$.a.\"root_integer\"", "$.a.'root_integer'", "$.\"a\".root_integer", "$.\"a\".\"root_integer\"", "$.\"a\".'root_integer'",
                     "$.'a'.root_integer", "$.'a'.\"root_integer\"", "$.'a'.'root_integer'"}) {
                CheckValueByPath(jsonPathAccessorTrie, path, "1");
            }
        }
    }

    Y_UNIT_TEST(JsonPathAccessorArray) {
        auto accessor = CreateTrivialArrayAccessor(R"(["a", 1, true, false, null, {"a": "b"}, {}, [1,2], []])");

        NKikimr::NArrow::NAccessor::NSubColumns::TJsonPathAccessorTrie jsonPathAccessorTrie;
        UNIT_ASSERT(jsonPathAccessorTrie.Insert("$.a", accessor).IsSuccess());

        // Non-existing paths and root path must return std::nullopt and called only once for our binary JSON
        // Object, array, null must return std::nullopt
        {
            for (const auto& path : {"$.a", "$.a[100]", "$.a.h", "$.a.e.p", "$.a[4]", "$.a[5]", "$.a[6]", "$.a[7]", "$.a[7][10]", "$.a[8]", "$.a[8][0]"}) {
                CheckValueByPath(jsonPathAccessorTrie, path, std::nullopt);
            }
        }

        // Root scalars
        {
            CheckValueByPath(jsonPathAccessorTrie, "$.a[0]", "a");
            CheckValueByPath(jsonPathAccessorTrie, "$.a[1]", "1");
            CheckValueByPath(jsonPathAccessorTrie, "$.a[2]", "true");
            CheckValueByPath(jsonPathAccessorTrie, "$.a[3]", "false");
        }

        // Non-root scalars
        {
            CheckValueByPath(jsonPathAccessorTrie, "$.a[5].a", "b");
            CheckValueByPath(jsonPathAccessorTrie, "$.a[7][0]", "1");
            CheckValueByPath(jsonPathAccessorTrie, "$.a[7][1]", "2");
        }

        // Different quotes
        {
            for (const auto& path : {"$.a[1]", "$.\"a\"[1]", "$.'a'[1]"}) {
                CheckValueByPath(jsonPathAccessorTrie, path, "1");
            }
        }
    }

    Y_UNIT_TEST(JsonPathAccessorScalar) {
        TVector<TString> scalarJsons = {"\"a\"", "1", "true", "false", "null"};
        TVector<std::optional<TString>> expectedValues = {"a", "1", "true", "false", std::nullopt};

        for (TVector<TString>::size_type i = 0; i < scalarJsons.size(); ++i) {
            const auto& scalarJson = scalarJsons[i];
            const auto& expectedValue = expectedValues[i];
            auto accessor = CreateTrivialArrayAccessor(scalarJson);

            NKikimr::NArrow::NAccessor::NSubColumns::TJsonPathAccessorTrie jsonPathAccessorTrie;
            UNIT_ASSERT(jsonPathAccessorTrie.Insert("$.a", accessor).IsSuccess());

            // Non-existing paths must return std::nullopt and called only once for our binary JSON
            {
                for (const auto& path : {"$.a.h", "$.a.e[3]", "$.a[4]"}) {
                    CheckValueByPath(jsonPathAccessorTrie, path, std::nullopt);
                }
            }

            // Existing path should return expected value
            {
                CheckValueByPath(jsonPathAccessorTrie, "$.a", expectedValue);
            }
        }
    }

    Y_UNIT_TEST(JsonPathAccessorDifferentWithIntersects) {
        auto accessorTopObject = CreateTrivialArrayAccessor(R"({"data": 1})");
        auto accessorTopScalar1 = CreateTrivialArrayAccessor("1");
        auto accessorTopScalar2 = CreateTrivialArrayAccessor("2");
        auto accessorTopScalar3 = CreateTrivialArrayAccessor("3");
        auto accessorTopArray = CreateTrivialArrayAccessor("[3,4]");

        NKikimr::NArrow::NAccessor::NSubColumns::TJsonPathAccessorTrie jsonPathAccessorTrie;
        UNIT_ASSERT(jsonPathAccessorTrie.Insert("$.a", accessorTopObject).IsSuccess());
        UNIT_ASSERT(jsonPathAccessorTrie.Insert("$.a.b", accessorTopScalar2).IsSuccess());
        UNIT_ASSERT(jsonPathAccessorTrie.Insert("$.b", accessorTopArray).IsSuccess());
        UNIT_ASSERT(jsonPathAccessorTrie.Insert("$.d.e.f", accessorTopScalar1).IsSuccess());
        UNIT_ASSERT(jsonPathAccessorTrie.Insert("$.d.e.g", accessorTopScalar2).IsSuccess());
        UNIT_ASSERT(jsonPathAccessorTrie.Insert("$.d.e.h", accessorTopScalar3).IsSuccess());
        UNIT_ASSERT(jsonPathAccessorTrie.Insert("$.d.i.j", accessorTopScalar1).IsSuccess());
        UNIT_ASSERT(jsonPathAccessorTrie.Insert("$.d.i", accessorTopScalar2).IsSuccess());
        UNIT_ASSERT(jsonPathAccessorTrie.Insert("$.d", accessorTopArray).IsSuccess());
        UNIT_ASSERT(jsonPathAccessorTrie.Insert("$.k", accessorTopScalar1).IsSuccess());
        UNIT_ASSERT(jsonPathAccessorTrie.Insert("$.k.l", accessorTopObject).IsSuccess());
        UNIT_ASSERT(jsonPathAccessorTrie.Insert("$.k.m", accessorTopArray).IsSuccess());

        CheckValueByPath(jsonPathAccessorTrie, "$.a", std::nullopt);
        CheckValueByPath(jsonPathAccessorTrie, "$.a.data", "1");
        CheckValueByPath(jsonPathAccessorTrie, "$.a.b", "2");
        CheckValueByPath(jsonPathAccessorTrie, "$.b", std::nullopt);
        CheckValueByPath(jsonPathAccessorTrie, "$.b[0]", "3");
        CheckValueByPath(jsonPathAccessorTrie, "$.b[1]", "4");
        CheckValueByPath(jsonPathAccessorTrie, "$.d.e.f", "1");
        CheckValueByPath(jsonPathAccessorTrie, "$.d.e.g", "2");
        CheckValueByPath(jsonPathAccessorTrie, "$.d.e.h", "3");
        CheckValueByPath(jsonPathAccessorTrie, "$.d.i.j", "1");
        CheckValueByPath(jsonPathAccessorTrie, "$.d.i", "2");
        CheckValueByPath(jsonPathAccessorTrie, "$.d", std::nullopt);
        CheckValueByPath(jsonPathAccessorTrie, "$.d[0]", "3");
        CheckValueByPath(jsonPathAccessorTrie, "$.d[1]", "4");
        CheckValueByPath(jsonPathAccessorTrie, "$.k", "1");
        CheckValueByPath(jsonPathAccessorTrie, "$.k.l", std::nullopt);
        CheckValueByPath(jsonPathAccessorTrie, "$.k.l.data", "1");
        CheckValueByPath(jsonPathAccessorTrie, "$.k.m", std::nullopt);
        CheckValueByPath(jsonPathAccessorTrie, "$.k.m[0]", "3");
        CheckValueByPath(jsonPathAccessorTrie, "$.k.m[1]", "4");
    }

    Y_UNIT_TEST(SubColumnNameFromDifferentPaths) {
        UNIT_ASSERT_VALUES_EQUAL(R"("a")", NSubColumns::ToSubcolumnName("a"));
        UNIT_ASSERT_VALUES_EQUAL(R"("a")", NSubColumns::ToSubcolumnName("$.a"));
        UNIT_ASSERT_VALUES_EQUAL(R"("a")", NSubColumns::ToSubcolumnName("strict $.a"));
        UNIT_ASSERT_VALUES_EQUAL(R"("a")", NSubColumns::ToSubcolumnName("lax $.a"));
        UNIT_ASSERT_VALUES_EQUAL(R"("a"."b")", NSubColumns::ToSubcolumnName("$.a.b"));
        UNIT_ASSERT_VALUES_EQUAL(R"("a"."b ! ?")", NSubColumns::ToSubcolumnName("$.a.\"b ! \?\""));
        UNIT_ASSERT_VALUES_EQUAL(R"("a"."b ! ?")", NSubColumns::ToSubcolumnName("$.a.\"b ! \\?\""));
        UNIT_ASSERT_VALUES_EQUAL(R"("a"."b ! ?")", NSubColumns::ToSubcolumnName("$.a.\"b ! \\\?\""));

        for (const auto& str : {"a", "'", "\"", "\?", "\\", "\a", "\b", "\f", "\n", "\r", "\t", "\v", R"(??()", "\\\"", "\\\\\"", "\\\\\\\""}) {
            NKikimr::NArrow::NAccessor::TJsonRestorer restorer;
            NJson::TJsonValue expected;

            restorer.SetValueByPath(NSubColumns::ToSubcolumnName(NSubColumns::QuoteJsonItem(str)), 1);
            expected[str] = 1;
            UNIT_ASSERT_VALUES_EQUAL(expected, restorer.GetResult());
        }
    }
};
