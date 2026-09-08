#include <ydb/core/formats/arrow/accessor/common/chunk_data.h>
#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/constructor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/data_extractor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/json_value_path.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/portions/extractor/sub_column.h>

#include <ydb/core/formats/arrow/accessor/sub_columns/ut_common/ut_helpers.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_binary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>

#include <library/cpp/testing/unittest/registar.h>
#include <yql/essentials/types/binary_json/read.h>
#include <yql/essentials/types/binary_json/write.h>

#include <regex>
#include <utility>

using NKikimr::NArrow::NAccessor::NSubColumns::NTesting::PrintBinaryJsons;

Y_UNIT_TEST_SUITE(SubColumnsArrayAccessor) {
    using namespace NKikimr::NArrow::NAccessor;
    using namespace NKikimr::NArrow;
    using namespace NKikimr;
    using TResolvedPath = NSubColumns::TDictStats::TResolvedPath;

    std::string PrepareToCompare(const std::string& str) {
        return std::regex_replace(str, std::regex(" |\\n"), "");
    }

    TResolvedPath ResolvePathVerified(const NSubColumns::TDictStats& stats, TStringBuf path) {
        auto pathInfoResult = stats.ResolvePath(path);
        UNIT_ASSERT_C(pathInfoResult.IsSuccess(), pathInfoResult.GetErrorMessage());
        const auto pathInfo = pathInfoResult.DetachResult();
        UNIT_ASSERT_C(pathInfo, path);
        const auto keyIndex = stats.GetKeyOrPrefixIndexOptional(NSubColumns::ToSubcolumnName(path));
        UNIT_ASSERT_C(keyIndex, path);
        UNIT_ASSERT_VALUES_EQUAL(*keyIndex, pathInfo->ColumnIndex);
        return *pathInfo;
    }

    void CheckPathHasNoMatch(const NSubColumns::TDictStats& stats, TStringBuf path) {
        auto pathInfoResult = stats.ResolvePath(path);
        UNIT_ASSERT_C(pathInfoResult.IsSuccess(), pathInfoResult.GetErrorMessage());
        UNIT_ASSERT_C(!pathInfoResult.DetachResult(), path);
        UNIT_ASSERT_C(!stats.GetKeyOrPrefixIndexOptional(NSubColumns::ToSubcolumnName(path)), path);
    }

    NSubColumns::TDictStats BuildStats(const std::initializer_list<std::pair<TStringBuf, NSubColumns::EValueType>>& columns) {
        auto builder = NSubColumns::TDictStats::MakeBuilder();
        for (const auto& [name, valueType] : columns) {
            builder.Add(TString(name), 1, 1, IChunkedArray::EType::Array, valueType);
        }
        return builder.Finish();
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
                UNIT_ASSERT_VALUES_EQUAL(arrSlice->DebugJson()["internal"]["columns_data"]["stats"].GetStringRobust(), R"({"accessor":[],"value_type":[],"size":[],"key_names":[],"records":[]})");
                UNIT_ASSERT_VALUES_EQUAL(arrSlice->DebugJson()["internal"]["others_data"]["stats"].GetStringRobust(), R"({"accessor":[],"value_type":[],"size":[],"key_names":[],"records":[]})");
            }
            {
                auto arrSlice = arrData->ISlice(0, 2);
                UNIT_ASSERT_VALUES_EQUAL(PrintBinaryJsons(arrSlice->GetChunkedArray()), R"([[{"a":1,"b":1,"c":"1111"},null]])");
                if (colsCount == 1) {
                    UNIT_ASSERT_VALUES_EQUAL(arrSlice->DebugJson()["internal"]["columns_data"]["stats"].GetStringRobust(), R"({"accessor":[1],"value_type":[0],"size":[34],"key_names":["\"c\""],"records":[1]})");
                    UNIT_ASSERT_VALUES_EQUAL(arrSlice->DebugJson()["internal"]["others_data"]["stats"].GetStringRobust(), R"({"accessor":[1,1],"value_type":[0,0],"size":[24,24],"key_names":["\"a\"","\"b\""],"records":[1,1]})");
                }
            }
            {
                auto arrSlice = arrData->ISlice(0, 3);
                UNIT_ASSERT_VALUES_EQUAL(PrintBinaryJsons(arrSlice->GetChunkedArray()), R"([[{"a":1,"b":1,"c":"1111"},null,{"a1":2,"b":2,"c":"2222"}]])");
                if (colsCount == 1) {
                    UNIT_ASSERT_VALUES_EQUAL(arrSlice->DebugJson()["internal"]["columns_data"]["stats"].GetStringRobust(), R"({"accessor":[1],"value_type":[0],"size":[63],"key_names":["\"c\""],"records":[2]})");
                    UNIT_ASSERT_VALUES_EQUAL(arrSlice->DebugJson()["internal"]["others_data"]["stats"].GetStringRobust(), R"({"accessor":[1,1,1],"value_type":[0,0,0],"size":[24,24,48],"key_names":["\"a\"","\"a1\"","\"b\""],"records":[1,1,2]})");
                }
            }
            {
                auto arrSlice = arrData->ISlice(3, 3);
                UNIT_ASSERT_VALUES_EQUAL(PrintBinaryJsons(arrSlice->GetChunkedArray()), R"([[{"a":3,"b":3,"c":"3333"},null,{"a":5,"b1":5}]])");
                if (colsCount == 1) {
                    UNIT_ASSERT_VALUES_EQUAL(arrSlice->DebugJson()["internal"]["columns_data"]["stats"].GetStringRobust(), R"({"accessor":[1],"value_type":[0],"size":[38],"key_names":["\"c\""],"records":[1]})");
                    UNIT_ASSERT_VALUES_EQUAL(arrSlice->DebugJson()["internal"]["others_data"]["stats"].GetStringRobust(), R"({"accessor":[1,1,1],"value_type":[0,0,0],"size":[48,24,24],"key_names":["\"a\"","\"b\"","\"b1\""],"records":[2,1,1]})");
                }
            }
        }
    }

    Y_UNIT_TEST(DictionaryColumns) {
        using namespace NKikimr::NArrow::NAccessor::NSubColumns;
        // dictionaryKff = 2: a separated column is dictionary-encoded when
        // distinct * 2 <= usageCount. othersFraction = 0 => everything separated.
        NSubColumns::TSettings settings(4, 1024, 0, 0, TDataAdapterContainer::GetDefault(), /*dictionaryKff*/ 2);

        std::vector<TString> jsons;
        for (ui32 i = 0; i < 40; ++i) {
            // "c" repeats over 2 distinct values -> dictionary; "a" is all distinct -> plain.
            jsons.push_back(TStringBuilder() << R"({"a":")" << i << R"(","c":")" << (i % 2 ? "xxxx" : "yyyy") << R"("})");
        }

        TTrivialArray::TPlainBuilder<arrow::BinaryType> arrBuilder;
        ui32 idx = 0;
        for (auto&& i : jsons) {
            auto v = NBinaryJson::SerializeToBinaryJson(i);
            NBinaryJson::TBinaryJson* bJson = std::get_if<NBinaryJson::TBinaryJson>(&v);
            UNIT_ASSERT(bJson);
            arrBuilder.AddRecord(idx++, std::string_view(bJson->data(), bJson->size()));
        }
        auto bJsonArr = arrBuilder.Finish(jsons.size());
        auto arrData = TSubColumnsArray::Make(bJsonArr, settings, bJsonArr->GetDataType()).DetachResult();

        // At least one separated column ("c") must be dictionary-encoded.
        const auto& cstats = arrData->GetColumnsData().GetStats();
        bool anyDict = false;
        for (ui32 i = 0; i < cstats.GetColumnsCount(); ++i) {
            anyDict |= (cstats.GetAccessorType(i) == IChunkedArray::EType::Dictionary);
        }
        UNIT_ASSERT_C(anyDict, "expected at least one dictionary column: " + arrData->DebugJson().GetStringRobust());

        const TString original = PrintBinaryJsons(arrData->GetChunkedArray());

        // Full serialize -> deserialize round-trip must reconstruct identical values.
        auto serializer = NSerialization::TSerializerContainer::GetDefaultSerializer();
        TChunkConstructionData cData(arrData->GetRecordsCount(), nullptr, arrow::binary(), serializer);
        const TString blob = arrData->SerializeToString(cData);
        NSubColumns::TConstructor constructor(settings);
        auto restored = constructor.DeserializeFromString(blob, cData).DetachResult();
        UNIT_ASSERT_VALUES_EQUAL(PrintBinaryJsons(restored->GetChunkedArray()), original);
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
        restorer.SetValueByPath(R"("b"."c")", "d");
        restorer.SetValueByPath("p.q", "r");
        restorer.SetValueByPath(R"("d'".e)", "f");
        restorer.SetValueByPath(R"("g.h.".i)", "j");
        restorer.SetValueByPath(R"(".".k)", "l");
        restorer.SetValueByPath(R"("\"")", "o");
        restorer.SetValueByPath(R"("\'")", "p");

        NJson::TJsonValue expected;
        expected["a"] = "b";
        expected["b"]["c"] = "d";
        expected["p.q"] = "r";
        expected["d'"]["e"] = "f";
        expected["g.h."]["i"] = "j";
        expected["."]["k"] = "l";
        expected["\""] = "o";
        expected["'"] = "p";

        UNIT_ASSERT_VALUES_EQUAL(expected, restorer.GetResult());
    }

    Y_UNIT_TEST(ValidateJsonPath) {
        UNIT_ASSERT(NSubColumns::IsValidJsonPath(R"($."type")"));
        UNIT_ASSERT(NSubColumns::IsValidJsonPath(R"($."deployment.environment")"));
        UNIT_ASSERT(!NSubColumns::IsValidJsonPath(R"("deployment.environment")"));

        const auto invalidResult = NSubColumns::ValidateJsonPath(R"("deployment.environment")");
        UNIT_ASSERT(invalidResult.IsFail());
        UNIT_ASSERT(invalidResult.GetErrorMessage().Contains("Unsupported path"));

        const auto stats = BuildStats({ { R"("a")", NSubColumns::EValueType::BinaryJson } });
        UNIT_ASSERT(!stats.GetKeyOrPrefixIndexOptional("\""));
    }

    Y_UNIT_TEST(ParseJsonPath) {
        TString path = R"($.a."b".'c'.'d"'."'"."\"".""."."[0,2].b[0].c[3][4].d[2 to 5].e[last])";
        TVector<TString> expectedItems = {"a", "b", "c", "d\"", "'", "\"", "", ".", "[0,2]", "b", "[0]", "c", "[3]", "[4]", "d", "[2 to 5]", "e", "[last]"};
        using enum NYql::NJsonPath::EJsonPathItemType;
        TVector<NYql::NJsonPath::EJsonPathItemType> expectedTypes = {MemberAccess, MemberAccess, MemberAccess, MemberAccess, MemberAccess, MemberAccess, MemberAccess, MemberAccess, ArrayAccess,
            MemberAccess, ArrayAccess, MemberAccess, ArrayAccess, ArrayAccess, MemberAccess, ArrayAccess, MemberAccess, ArrayAccess};
        TVector<NKikimr::NArrow::NAccessor::NSubColumns::TJsonPathBuf::size_type> expectedStartPositions = {1, 3, 7, 11, 16, 20, 25, 28, 32, 37, 39, 42, 44, 47, 50, 52, 60, 62};

        auto result = NKikimr::NArrow::NAccessor::NSubColumns::ParseJsonPath(path);
        UNIT_ASSERT_C(result.IsSuccess(), result.GetErrorMessage());
        const auto [pathItems, pathTypes, startPositions] = result.DetachResult().Items;

        UNIT_ASSERT_VALUES_EQUAL(expectedItems, pathItems);
        UNIT_ASSERT_EQUAL(expectedTypes, pathTypes);
        UNIT_ASSERT_VALUES_EQUAL(expectedStartPositions, startPositions);
    }

    Y_UNIT_TEST(JsonPathResolutionHandlesMultipleRequestedPaths) {
        auto stats = BuildStats({
            { R"("a"."b")", NSubColumns::EValueType::BinaryJson },
            { R"("b")", NSubColumns::EValueType::BinaryJson },
            { R"("c"."d")", NSubColumns::EValueType::BinaryJson },
        });

        UNIT_ASSERT_VALUES_EQUAL(ResolvePathVerified(stats, "$.a.b"), (TResolvedPath{0, NSubColumns::EValueType::BinaryJson, ""}));
        UNIT_ASSERT_VALUES_EQUAL(ResolvePathVerified(stats, "$.b"), (TResolvedPath{1, NSubColumns::EValueType::BinaryJson, ""}));
        UNIT_ASSERT_VALUES_EQUAL(ResolvePathVerified(stats, "$.c.d"), (TResolvedPath{2, NSubColumns::EValueType::BinaryJson, ""}));
        UNIT_ASSERT_VALUES_EQUAL(ResolvePathVerified(stats, "$.a.b.e"), (TResolvedPath{0, NSubColumns::EValueType::BinaryJson, "strict $.e"}));
        UNIT_ASSERT_VALUES_EQUAL(ResolvePathVerified(stats, "$.b.\"\".g[3].h"), (TResolvedPath{1, NSubColumns::EValueType::BinaryJson, "strict $.\"\".g[3].h"}));
        UNIT_ASSERT_VALUES_EQUAL(ResolvePathVerified(stats, "$.c.d[54]"), (TResolvedPath{2, NSubColumns::EValueType::BinaryJson, "strict $[54]"}));

        for (const auto& path : { "$.a", "$.\"\"", "$.c" }) {
            CheckPathHasNoMatch(stats, path);
        }
    }

    Y_UNIT_TEST(JsonPathResolutionUsesLongestPrefix) {
        auto stats = BuildStats({ { R"("a")", NSubColumns::EValueType::BinaryJson }, { R"("a"."b")", NSubColumns::EValueType::String } });

        UNIT_ASSERT_VALUES_EQUAL(ResolvePathVerified(stats, "$.a.b.c"), (TResolvedPath{1, NSubColumns::EValueType::String, "strict $.c"}));
    }



    Y_UNIT_TEST(JsonPathResolutionMatchesExactPath) {
        auto stats = BuildStats({ { R"("a"."b")", NSubColumns::EValueType::String } });

        UNIT_ASSERT_VALUES_EQUAL(ResolvePathVerified(stats, "$.a.b"), (TResolvedPath{0, NSubColumns::EValueType::String, ""}));
    }

    Y_UNIT_TEST(JsonPathResolutionReturnsNoMatch) {
        auto stats = BuildStats({ { R"("a")", NSubColumns::EValueType::BinaryJson } });

        CheckPathHasNoMatch(stats, "$.b");
    }

    Y_UNIT_TEST(JsonPathResolutionMatchesQuotedMember) {
        auto stats = BuildStats({ { R"("a.b")", NSubColumns::EValueType::String } });

        UNIT_ASSERT_VALUES_EQUAL(ResolvePathVerified(stats, "$.\"a.b\".c"), (TResolvedPath{0, NSubColumns::EValueType::String, "strict $.c"}));
    }

    Y_UNIT_TEST(JsonPathResolutionRetainsArraySuffix) {
        auto stats = BuildStats({ { R"("a")", NSubColumns::EValueType::BinaryJson } });

        UNIT_ASSERT_VALUES_EQUAL(ResolvePathVerified(stats, "$.a[0].b"), (TResolvedPath{0, NSubColumns::EValueType::BinaryJson, "strict $[0].b"}));
    }

    Y_UNIT_TEST(JsonPathResolutionMatchesAncestorBeforeUnstoredDescendant) {
        auto stats = BuildStats({ { R"("d")", NSubColumns::EValueType::BinaryJson }, { R"("d"."e"."f")", NSubColumns::EValueType::BinaryJson } });

        // A descendant stored path does not prevent selecting the longest stored ancestor.
        UNIT_ASSERT_VALUES_EQUAL(ResolvePathVerified(stats, "$.d.e"), (TResolvedPath{0, NSubColumns::EValueType::BinaryJson, "strict $.e"}));
    }

    Y_UNIT_TEST(JsonPathResolutionRejectsInvalidPaths) {
        auto stats = NSubColumns::TDictStats::BuildEmpty();

        for (const auto& path : { "$.a.b.[2]", "$.b.", "$.c[]" }) {
            UNIT_ASSERT(stats.ResolvePath(path).IsFail());
        }
    }

    std::shared_ptr<TTrivialArray> CreateTrivialArrayAccessor(TStringBuf data) {
        auto binaryJsonResult = NBinaryJson::SerializeToBinaryJson(data);
        UNIT_ASSERT(std::holds_alternative<NBinaryJson::TBinaryJson>(binaryJsonResult));

        auto binaryJson = std::get<NBinaryJson::TBinaryJson>(binaryJsonResult);
        return std::make_shared<TTrivialArray>(NKikimr::NArrow::NAccessor::TTrivialArray::BuildArrayFromScalar(
            std::make_shared<arrow::BinaryScalar>(std::make_shared<arrow::Buffer>((const ui8*)binaryJson.data(), binaryJson.size()), arrow::binary())));
    }

    std::shared_ptr<TSubColumnsArray> BuildArrayWithStoredPaths(const std::initializer_list<std::pair<TStringBuf, TStringBuf>>& columns,
        const TStringBuf otherName, const TStringBuf otherValue) {
        auto columnsBuilder = NSubColumns::TDictStats::MakeBuilder();
        for (const auto& [name, _] : columns) {
            columnsBuilder.Add(TString(name), 1, 1, IChunkedArray::EType::Array, NSubColumns::EValueType::BinaryJson);
        }
        auto columnsStats = columnsBuilder.Finish();
        auto columnsRecords = std::make_shared<TGeneralContainer>(1);
        ui32 index = 0;
        for (const auto& [_, value] : columns) {
            columnsRecords->AddField(columnsStats.GetField(index++), CreateTrivialArrayAccessor(value)).Validate();
        }

        auto othersStats = BuildStats({ { otherName, NSubColumns::EValueType::BinaryJson } });
        const auto binaryJsonResult = NBinaryJson::SerializeToBinaryJson(otherValue);
        UNIT_ASSERT(std::holds_alternative<NBinaryJson::TBinaryJson>(binaryJsonResult));
        const auto& binaryJson = std::get<NBinaryJson::TBinaryJson>(binaryJsonResult);
        auto othersBuilder = NSubColumns::TOthersData::MakeMergedBuilder();
        othersBuilder->Add(0, 0, std::string_view(binaryJson.data(), binaryJson.size()));
        auto others = othersBuilder->Finish(NSubColumns::TOthersData::TFinishContext(othersStats));

        return std::make_shared<TSubColumnsArray>(
            NSubColumns::TColumnsData(columnsStats, columnsRecords), std::move(others), arrow::binary(), 1, NSubColumns::TSettings());
    }

    void CheckMostSpecificStoredPath(const std::initializer_list<std::pair<TStringBuf, TStringBuf>>& columns, const TStringBuf otherName,
        const TStringBuf otherValue, const TStringBuf path, const TStringBuf expected) {
        auto array = BuildArrayWithStoredPaths(columns, otherName, otherValue);
        auto accessorResult = array->GetPathAccessor(path, 1);
        UNIT_ASSERT_C(accessorResult.IsSuccess(), accessorResult.GetErrorMessage());
        accessorResult.DetachResult()->VisitValues([expected](const std::optional<TStringBuf>& value) {
            UNIT_ASSERT_VALUES_EQUAL(value, expected);
        });
    }

    void CheckValueByPath(const std::shared_ptr<IChunkedArray>& accessor, TStringBuf path, std::optional<TStringBuf> expected) {
        static const auto stats = BuildStats({ { R"("a")", NSubColumns::EValueType::BinaryJson } });
        auto pathInfo = ResolvePathVerified(stats, path);
        NSubColumns::TJsonPathAccessor jsonPathAccessor(accessor, std::move(pathInfo.RemainingPath), pathInfo.ValueType);

        int callsCount = 0;
        jsonPathAccessor.VisitValues([&](const std::optional<TStringBuf>& value) {
            UNIT_ASSERT_VALUES_EQUAL_C(expected, value, TString(path));
            ++callsCount;
        });
        UNIT_ASSERT_VALUES_EQUAL(callsCount, 1);
    }

    Y_UNIT_TEST(JsonPathAccessorObject) {
        auto accessor = CreateTrivialArrayAccessor(
            R"({"root_integer": 1, "root_string": "a", "root_true": true, "root_false": false, "root_null": null, "root_object": {"a": "b"}, "root_array": ["a", 1, true, false, null, {}, [], [1, 2]]})");
        // Non-existing paths and root path must return std::nullopt and called only once for our binary JSON
        // Object, array, null must return std::nullopt
        {
            for (const auto& path : {"$.a", "$.a[0]", "$.a.h", "$.a.e.p", "$.a.f.z", "$.a.root_object", "$.a.root_array", "$.a.root_null", "$.a.root_array[4]", "$.a.root_array[5]",
                     "$.a.root_array[6]", "$.a.root_array[7]", "$.a.root_array[7][4]", "$.a.root_array[10]"}) {
                CheckValueByPath(accessor, path, std::nullopt);
            }
        }

        // Root scalars
        {
            CheckValueByPath(accessor, "$.a.root_string", "a");
            CheckValueByPath(accessor, "$.a.root_integer", "1");
            CheckValueByPath(accessor, "$.a.root_true", "true");
            CheckValueByPath(accessor, "$.a.root_false", "false");
        }

        // Non-root scalars
        {
            CheckValueByPath(accessor, "$.a.root_object.a", "b");
            CheckValueByPath(accessor, "$.a.root_array[0]", "a");
            CheckValueByPath(accessor, "$.a.root_array[1]", "1");
            CheckValueByPath(accessor, "$.a.root_array[2]", "true");
            CheckValueByPath(accessor, "$.a.root_array[3]", "false");
            CheckValueByPath(accessor, "$.a.root_array[7][0]", "1");
            CheckValueByPath(accessor, "$.a.root_array[7][1]", "2");
        }

        // Different quotes
        {
            for (const auto& path : {"$.a.root_integer", "$.a.\"root_integer\"", "$.a.'root_integer'", "$.\"a\".root_integer", "$.\"a\".\"root_integer\"", "$.\"a\".'root_integer'",
                     "$.'a'.root_integer", "$.'a'.\"root_integer\"", "$.'a'.'root_integer'"}) {
                CheckValueByPath(accessor, path, "1");
            }
        }
    }

    Y_UNIT_TEST(JsonPathAccessorTopLevelNull) {
        auto accessor = CreateTrivialArrayAccessor("null");
        CheckValueByPath(accessor, "$.a", std::nullopt);
        CheckValueByPath(accessor, "$.a.b", std::nullopt);
        CheckValueByPath(accessor, "$.a.b.c", std::nullopt);
    }

    Y_UNIT_TEST(JsonPathAccessorArray) {
        auto accessor = CreateTrivialArrayAccessor(R"(["a", 1, true, false, null, {"a": "b"}, {}, [1,2], []])");
        // Non-existing paths and root path must return std::nullopt and called only once for our binary JSON
        // Object, array, null must return std::nullopt
        {
            for (const auto& path : {"$.a", "$.a[100]", "$.a.h", "$.a.e.p", "$.a[4]", "$.a[5]", "$.a[6]", "$.a[7]", "$.a[7][10]", "$.a[8]", "$.a[8][0]"}) {
                CheckValueByPath(accessor, path, std::nullopt);
            }
        }

        // Root scalars
        {
            CheckValueByPath(accessor, "$.a[0]", "a");
            CheckValueByPath(accessor, "$.a[1]", "1");
            CheckValueByPath(accessor, "$.a[2]", "true");
            CheckValueByPath(accessor, "$.a[3]", "false");
        }

        // Non-root scalars
        {
            CheckValueByPath(accessor, "$.a[5].a", "b");
            CheckValueByPath(accessor, "$.a[7][0]", "1");
            CheckValueByPath(accessor, "$.a[7][1]", "2");
        }

        // Different quotes
        {
            for (const auto& path : {"$.a[1]", "$.\"a\"[1]", "$.'a'[1]"}) {
                CheckValueByPath(accessor, path, "1");
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
            // Non-existing paths must return std::nullopt and called only once for our binary JSON
            {
                for (const auto& path : {"$.a.h", "$.a.e[3]", "$.a[4]"}) {
                    CheckValueByPath(accessor, path, std::nullopt);
                }
            }

            // Existing path should return expected value
            {
                CheckValueByPath(accessor, "$.a", expectedValue);
            }
        }
    }

    Y_UNIT_TEST(JsonPathAccessorDifferentWithIntersects) {
        auto accessorTopObject = CreateTrivialArrayAccessor(R"({"data": 1})");
        auto accessorTopScalar1 = CreateTrivialArrayAccessor("1");
        auto accessorTopScalar2 = CreateTrivialArrayAccessor("2");
        auto accessorTopScalar3 = CreateTrivialArrayAccessor("3");
        auto accessorTopArray = CreateTrivialArrayAccessor("[3,4]");
        auto stats = BuildStats({
            { R"("a")", NSubColumns::EValueType::BinaryJson },
            { R"("a"."b")", NSubColumns::EValueType::BinaryJson },
            { R"("b")", NSubColumns::EValueType::BinaryJson },
            { R"("d"."e"."f")", NSubColumns::EValueType::BinaryJson },
            { R"("d"."e"."g")", NSubColumns::EValueType::BinaryJson },
            { R"("d"."e"."h")", NSubColumns::EValueType::BinaryJson },
            { R"("d"."i"."j")", NSubColumns::EValueType::BinaryJson },
            { R"("d"."i")", NSubColumns::EValueType::BinaryJson },
            { R"("d")", NSubColumns::EValueType::BinaryJson },
            { R"("k")", NSubColumns::EValueType::BinaryJson },
            { R"("k"."l")", NSubColumns::EValueType::BinaryJson },
            { R"("k"."m")", NSubColumns::EValueType::BinaryJson },
        });
        auto records = std::make_shared<TGeneralContainer>(1);
        const std::vector<std::shared_ptr<IChunkedArray>> accessors = {
            accessorTopObject, accessorTopScalar2, accessorTopArray, accessorTopScalar1, accessorTopScalar2, accessorTopScalar3,
            accessorTopScalar1, accessorTopScalar2, accessorTopArray, accessorTopScalar1, accessorTopObject, accessorTopArray,
        };
        for (ui32 i = 0; i < accessors.size(); ++i) {
            records->AddField(stats.GetField(i), accessors[i]).Validate();
        }
        NSubColumns::TColumnsData columns(stats, records);

        const auto check = [&](const TStringBuf path, const std::optional<TStringBuf> expected) {
            const auto pathInfo = ResolvePathVerified(stats, path);
            const auto accessor = columns.GetPathAccessor(pathInfo);
            accessor->VisitValues([&](const std::optional<TStringBuf>& value) {
                UNIT_ASSERT_VALUES_EQUAL_C(value, expected, path);
            });
        };

        check("$.a", std::nullopt);
        check("$.a.data", "1");
        check("$.a.b", "2");
        check("$.b", std::nullopt);
        check("$.b[0]", "3");
        check("$.b[1]", "4");
        check("$.d.e.f", "1");
        check("$.d.e.g", "2");
        check("$.d.e.h", "3");
        check("$.d.i.j", "1");
        check("$.d.i", "2");
        check("$.d", std::nullopt);
        check("$.d[0]", "3");
        check("$.d[1]", "4");
        check("$.k", "1");
        check("$.k.l", std::nullopt);
        check("$.k.l.data", "1");
        check("$.k.m", std::nullopt);
        check("$.k.m[0]", "3");
        check("$.k.m[1]", "4");
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

    Y_UNIT_TEST(PartialJsonPathAccessorResolvesToAddedColumn) {
        auto header = NSubColumns::TSubColumnsHeader(
            BuildStats({ { R"("a")", NSubColumns::EValueType::BinaryJson }, { R"("a"."b")", NSubColumns::EValueType::BinaryJson } }),
            NSubColumns::TDictStats::BuildEmpty(), NKikimrArrowAccessorProto::TSubColumnsAccessor(), 0);
        TSubColumnsPartialArray partial(std::move(header), 1, arrow::binary(), NSubColumns::TSettings());
        partial.AddColumn(partial.GetHeader().GetColumnStats().GetExactKeyIndexVerified(R"("a")"), CreateTrivialArrayAccessor(R"({"b":{"c":"value"}})"));

        auto accessorResult = partial.GetPathAccessor("$.a.b.c", 1);
        UNIT_ASSERT_C(accessorResult.IsSuccess(), accessorResult.GetErrorMessage());
        accessorResult.DetachResult()->VisitValues([](const std::optional<TStringBuf>& value) {
            UNIT_ASSERT_VALUES_EQUAL(value, "value");
        });
    }

    Y_UNIT_TEST(PartialArrayNeedsFetchForMoreSpecificOthersPath) {
        auto header = NSubColumns::TSubColumnsHeader(
            BuildStats({ { R"("a")", NSubColumns::EValueType::BinaryJson }, { R"("a"."b"."c")", NSubColumns::EValueType::BinaryJson } }),
            BuildStats({ { R"("a"."b")", NSubColumns::EValueType::BinaryJson } }), NKikimrArrowAccessorProto::TSubColumnsAccessor(), 0);
        TSubColumnsPartialArray partial(std::move(header), 1, arrow::binary(), NSubColumns::TSettings());
        partial.AddColumn(partial.GetHeader().GetColumnStats().GetExactKeyIndexVerified(R"("a")"), CreateTrivialArrayAccessor(R"({"b":"columns"})"));

        UNIT_ASSERT(!partial.HasSubColumnData(R"("a"."b")"));
    }

    Y_UNIT_TEST(JsonPathAccessorPreferBestMatchOthers) {
        // Others have an exact match while separated only a prefix
        CheckMostSpecificStoredPath({ { R"("a")", R"({"b":"columns"})" }, { R"("a"."b"."c")", R"("descendant")" } }, R"("a"."b")",
            R"("others")", "$.a.b", "others");
    }

    // Others have an exact match while separated only a prefix
    Y_UNIT_TEST(JsonPathAccessorPreferBestMatchSeparated) {
        CheckMostSpecificStoredPath({ { R"("a"."b")", R"("columns")" } }, R"("a")", R"({"c":"others"})", "$.a.b", "columns");
    }

    Y_UNIT_TEST(SubColumnDataExtractorUsesExactStoredPath) {
        auto array = BuildArrayWithStoredPaths(
            { { R"("a")", R"("columns")" }, { R"("a"."b"."c")", R"("descendant")" } }, R"("a"."b")", R"("others")");
        NOlap::NIndexes::TSubColumnDataExtractor extractor;
        NJson::TJsonValue config(NJson::JSON_MAP);
        config.InsertValue("sub_column_name", R"("a"."b")");
        UNIT_ASSERT(extractor.DeserializeFromJson(config).IsSuccess());

        TString result;
        extractor.VisitAll(array, {}, [&result](const NArrow::NAccessor::TJsonValueView& value, ui64) {
            result = value.ToJsonValue().GetString();
        });
        UNIT_ASSERT_VALUES_EQUAL(result, "others");
    }
};

Y_UNIT_TEST_SUITE(SubColumnsDictStats) {
    using namespace NKikimr;
    using namespace NKikimr::NArrow;
    using namespace NKikimr::NArrow::NAccessor;
    using namespace NKikimr::NArrow::NAccessor::NSubColumns;

    struct TStatsRow {
        TString Name;
        ui32 Records;
        ui32 Size;
        IChunkedArray::EType Accessor;
        EValueType ValueType;
    };

    void AssertStatsMatch(const TDictStats& stats, const std::vector<TStatsRow>& expected) {
        UNIT_ASSERT_VALUES_EQUAL(stats.GetColumnsCount(), expected.size());
        for (ui32 i = 0; i < expected.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(stats.GetColumnNameString(i), expected[i].Name);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetColumnRecordsCount(i), expected[i].Records);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetColumnSize(i), expected[i].Size);
            UNIT_ASSERT_VALUES_EQUAL((ui32)stats.GetAccessorType(i), (ui32)expected[i].Accessor);
            UNIT_ASSERT_VALUES_EQUAL((ui32)stats.GetValueType(i), (ui32)expected[i].ValueType);
        }
    }

    Y_UNIT_TEST(ValueTypeCodesArePersistent) {
        UNIT_ASSERT_VALUES_EQUAL((ui32)EValueType::BinaryJson, 0u);
        UNIT_ASSERT_VALUES_EQUAL((ui32)EValueType::Double, 1u);
        UNIT_ASSERT_VALUES_EQUAL((ui32)EValueType::Bool, 2u);
        UNIT_ASSERT_VALUES_EQUAL((ui32)EValueType::String, 3u);
    }

    // The current format (5-column, with value_type) round-trips through serialization, preserving every field.
    Y_UNIT_TEST(NewFormatRoundTrip) {
        const std::vector<TStatsRow> rows = {
            { "a", 3, 30, IChunkedArray::EType::Array, EValueType::String },
            { "b", 5, 40, IChunkedArray::EType::Dictionary, EValueType::BinaryJson },
            { "c", 2, 16, IChunkedArray::EType::SparsedArray, EValueType::Double },
            { "d", 1, 8, IChunkedArray::EType::Array, EValueType::Bool },
        };
        auto builder = TDictStats::MakeBuilder();
        for (const auto& r : rows) {
            builder.Add(r.Name, r.Records, r.Size, r.Accessor, r.ValueType);
        }
        auto stats = builder.Finish();
        auto restored = TDictStats::DeserializeFromBlob(stats.SerializeAsString(nullptr));
        AssertStatsMatch(restored, rows);
    }

    Y_UNIT_TEST(LegacyFourColumnFormatDeserializes) {
        const std::vector<TStatsRow> rows = {
            { "a", 3, 30, IChunkedArray::EType::Array, EValueType::BinaryJson },
            { "b", 5, 40, IChunkedArray::EType::Dictionary, EValueType::BinaryJson },
            { "c", 2, 16, IChunkedArray::EType::SparsedArray, EValueType::BinaryJson },
        };
        auto legacySchema = std::make_shared<arrow::Schema>(arrow::FieldVector{
            std::make_shared<arrow::Field>("name", arrow::binary()), std::make_shared<arrow::Field>("count", arrow::uint32()),
            std::make_shared<arrow::Field>("size", arrow::uint32()), std::make_shared<arrow::Field>("accessor_type", arrow::uint8()) });

        arrow::BinaryBuilder names;
        arrow::UInt32Builder count;
        arrow::UInt32Builder size;
        arrow::UInt8Builder acc;
        for (const auto& r : rows) {
            UNIT_ASSERT(names.Append(r.Name.data(), r.Name.size()).ok());
            UNIT_ASSERT(count.Append(r.Records).ok());
            UNIT_ASSERT(size.Append(r.Size).ok());
            UNIT_ASSERT(acc.Append((ui8)r.Accessor).ok());
        }
        std::shared_ptr<arrow::Array> namesArr, countArr, sizeArr, accArr;
        UNIT_ASSERT(names.Finish(&namesArr).ok());
        UNIT_ASSERT(count.Finish(&countArr).ok());
        UNIT_ASSERT(size.Finish(&sizeArr).ok());
        UNIT_ASSERT(acc.Finish(&accArr).ok());
        auto legacyBatch = arrow::RecordBatch::Make(legacySchema, rows.size(), { namesArr, countArr, sizeArr, accArr });

        auto restored = TDictStats::DeserializeFromBlob(NArrow::SerializeBatchNoCompression(legacyBatch));
        AssertStatsMatch(restored, rows);
    }
}
