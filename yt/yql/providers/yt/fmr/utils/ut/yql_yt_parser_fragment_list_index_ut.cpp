#include <yt/yql/providers/yt/fmr/utils/yql_yt_parser_fragment_list_index.h>
#include <yt/yql/providers/yt/fmr/test_tools/yson/yql_yt_yson_helpers.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/yson/writer.h>
#include <util/stream/str.h>

namespace NYql::NFmr {

THashMap<TString, TColumnOffsetRange> ParseYsonAndCollectColumnOffsets(const TString& ysonData, const TSet<TString>& columnsToTrack) {
    TVector<TString> columnsVector(columnsToTrack.begin(), columnsToTrack.end());

    TParserFragmentListIndex parser(ysonData, columnsVector);
    parser.Parse();
    const auto& rows = parser.GetRows();

    if (rows.empty()) {
        return {};
    }

    // Convert TVector<TColumnOffsetRange> back to THashMap for backward compatibility
    THashMap<TString, TColumnOffsetRange> offsets;
    const auto& lastRow = rows.back();
    for (size_t i = 0; i < columnsVector.size() && i < lastRow.size(); ++i) {
        if (lastRow[i].IsValid()) {
            offsets[columnsVector[i]] = lastRow[i];
        }
    }
    return offsets;
}

TVector<THashMap<TString, TColumnOffsetRange>> ParseYsonAndCollectColumnOffsetsStack(const TString& ysonData, const TSet<TString>& columnsToTrack) {
    TVector<TString> columnsVector(columnsToTrack.begin(), columnsToTrack.end());

    TParserFragmentListIndex parser(ysonData, columnsVector);
    parser.Parse();
    const auto& rows = parser.GetRows();

    // Convert TVector<TRowIndexMarkup> back to TVector<THashMap<...>> for backward compatibility
    TVector<THashMap<TString, TColumnOffsetRange>> result;
    for (const auto& row : rows) {
        THashMap<TString, TColumnOffsetRange> offsets;
        for (size_t i = 0; i < columnsVector.size() && i < row.size(); ++i) {
            if (row[i].IsValid()) {
                offsets[columnsVector[i]] = row[i];
            }
        }
        result.push_back(std::move(offsets));
    }
    return result;
}

Y_UNIT_TEST_SUITE(TParserFragmentListIndexTests) {

    Y_UNIT_TEST(BasicStructureTracking) {
        TString textYson = "{name=\"Alice\";age=30}";
        TString binaryYson = GetBinaryYson(textYson);
        auto offsets = ParseYsonAndCollectColumnOffsets(binaryYson, {"name", "age"});
        UNIT_ASSERT(offsets.contains("name"));
        UNIT_ASSERT(offsets.contains("age"));
    }


    Y_UNIT_TEST(NestedMapHandling) {
        TString textYson = "{data={nested=123}}";
        TString binaryYson = GetBinaryYson(textYson);
        auto offsets = ParseYsonAndCollectColumnOffsets(binaryYson, {"data"});
        UNIT_ASSERT(offsets.contains("data"));
        UNIT_ASSERT(!offsets.contains("nested"));
    }

    Y_UNIT_TEST(MultipleRecords) {
        TString textYson = "{name=\"Alice\"};{name=\"Bob\"}";
        TString binaryYson = GetBinaryYson(textYson);
        auto offsetsStack = ParseYsonAndCollectColumnOffsetsStack(binaryYson, {"name"});
        UNIT_ASSERT_EQUAL(offsetsStack.size(), 2);
        UNIT_ASSERT(offsetsStack[0].contains("name"));
        UNIT_ASSERT(offsetsStack[1].contains("name"));
    }

    Y_UNIT_TEST(NestedListsHandling) {
        TString textYson = "{data=[[1;2];[3;4]]}";
        TString binaryYson = GetBinaryYson(textYson);
        auto offsets = ParseYsonAndCollectColumnOffsets(binaryYson, {"data"});
        UNIT_ASSERT(offsets.contains("data"));
    }

    Y_UNIT_TEST(ComplexNestingMapListMap) {
        TString textYson = "{users=[{name=\"Alice\";age=30};{name=\"Bob\";age=25}];count=2}";
        TString binaryYson = GetBinaryYson(textYson);
        auto offsets = ParseYsonAndCollectColumnOffsets(binaryYson, {"users", "count"});
        UNIT_ASSERT(offsets.contains("users"));
        UNIT_ASSERT(offsets.contains("count"));

        // For binary YSON, we need to convert extracted value back to text for comparison
        const auto& usersRange = offsets.at("users");
        TString usersValueBinary = binaryYson.substr(usersRange.StartOffset, usersRange.EndOffset - usersRange.StartOffset);
        TString usersValueText = GetBinaryYson("[{name=\"Alice\";age=30};{name=\"Bob\";age=25}]", NYson::EYsonType::Node);
        UNIT_ASSERT_EQUAL(usersValueBinary, usersValueText);

        const auto& countRange = offsets.at("count");
        TString countValueBinary = binaryYson.substr(countRange.StartOffset, countRange.EndOffset - countRange.StartOffset);
        // Binary representation of integer 2
        UNIT_ASSERT(!countValueBinary.empty());
    }

    Y_UNIT_TEST(ComplexNestingMapListMap2) {
        TString textYson = "{users=[{name=\"Alice\";age=30};{name=\"Bob\";age=25}];count=2;users2=[{name=\"Charlie\";age=30}]}";
        TString binaryYson = GetBinaryYson(textYson);
        auto offsets = ParseYsonAndCollectColumnOffsets(binaryYson, {"users", "count", "users2"});
        UNIT_ASSERT(offsets.contains("users"));
        UNIT_ASSERT(offsets.contains("count"));
        UNIT_ASSERT(offsets.contains("users2"));

        const auto& usersRange = offsets.at("users2");
        TString usersValueBinary = binaryYson.substr(usersRange.StartOffset, usersRange.EndOffset - usersRange.StartOffset);
        TString expectedBinary = GetBinaryYson("[{name=\"Charlie\";age=30}]", NYson::EYsonType::Node);
        UNIT_ASSERT_EQUAL(usersValueBinary, expectedBinary);

        const auto& countRange = offsets.at("count");
        TString countValueBinary = binaryYson.substr(countRange.StartOffset, countRange.EndOffset - countRange.StartOffset);
        UNIT_ASSERT(!countValueBinary.empty());
    }

    Y_UNIT_TEST(ComplexNestingMapMapListMap) {
        TString textYson = "{config={items=[{id=1;tags=[\"a\";\"b\"]};{id=2;tags=[\"c\"]}]}}";
        TString binaryYson = GetBinaryYson(textYson);
        auto offsets = ParseYsonAndCollectColumnOffsets(binaryYson, {"config"});

        UNIT_ASSERT_EQUAL(offsets.size(), 1);
        UNIT_ASSERT(offsets.contains("config"));

        const auto& configRange = offsets.at("config");
        TString configValueBinary = binaryYson.substr(configRange.StartOffset, configRange.EndOffset - configRange.StartOffset);
        TString expectedBinary = GetBinaryYson("{items=[{id=1;tags=[\"a\";\"b\"]};{id=2;tags=[\"c\"]}]}", NYson::EYsonType::Node);
        UNIT_ASSERT_EQUAL(configValueBinary, expectedBinary);
    }

    Y_UNIT_TEST(ComplexNestingMapMapListMap2) {
        TString textYson = "{config={items=[{id=1;tags=[\"a\";\"b\"]};{id=2;tags=[\"c\"]}]};config2={items2=[{id=3;tags=[\"d\";\"e\"]};{id=4;tags=[\"f\"]}]}}";
        TString binaryYson = GetBinaryYson(textYson);
        auto offsets = ParseYsonAndCollectColumnOffsets(binaryYson, {"config", "config2"});

        UNIT_ASSERT_EQUAL(offsets.size(), 2);
        UNIT_ASSERT(offsets.contains("config"));
        UNIT_ASSERT(offsets.contains("config2"));

        const auto& configRange = offsets.at("config2");
        TString configValueBinary = binaryYson.substr(configRange.StartOffset, configRange.EndOffset - configRange.StartOffset);
        TString expectedBinary = GetBinaryYson("{items2=[{id=3;tags=[\"d\";\"e\"]};{id=4;tags=[\"f\"]}]}", NYson::EYsonType::Node);
        UNIT_ASSERT_EQUAL(configValueBinary, expectedBinary);
    }

    Y_UNIT_TEST(OffsetTracking) {
        TString textYson = "{col1=\"value1\"}";
        TString binaryYson = GetBinaryYson(textYson);
        auto offsets = ParseYsonAndCollectColumnOffsets(binaryYson, {"col1"});

        UNIT_ASSERT(offsets.contains("col1"));
        const auto& range = offsets.at("col1");
        UNIT_ASSERT(range.IsValid());
        TString configValueBinary = binaryYson.substr(range.StartOffset, range.EndOffset - range.StartOffset);
        TString expectedBinary = GetBinaryYson("\"value1\"", NYson::EYsonType::Node);
        UNIT_ASSERT_EQUAL(configValueBinary, expectedBinary);
    }

    Y_UNIT_TEST(AllScalarTypes) {
        TString textYson = "{str=\"test\";i64=-123;ui64=456u;dbl=3.14;bool=%true;entity=#}";
        TString binaryYson = GetBinaryYson(textYson);
        auto offsets = ParseYsonAndCollectColumnOffsets(binaryYson, {"str", "i64", "ui64", "dbl", "bool", "entity"});

        UNIT_ASSERT_EQUAL(offsets.size(), 6);
    }

    Y_UNIT_TEST(ParseYsonAndCollectColumnOffsetsHelper) {
        TString textYson = "{name=\"Alice\";age=30}";
        TString binaryYson = GetBinaryYson(textYson);
        auto offsets = ParseYsonAndCollectColumnOffsets(binaryYson, {"name", "age"});

        UNIT_ASSERT(offsets.contains("name"));
        UNIT_ASSERT(offsets.contains("age"));
        UNIT_ASSERT_EQUAL(offsets.size(), 2);

        for (const auto& [columnName, range] : offsets) {
            Y_UNUSED(columnName);
            UNIT_ASSERT(range.IsValid());
            UNIT_ASSERT(range.StartOffset < range.EndOffset);
        }
    }

    Y_UNIT_TEST(ParseYsonSelectiveTracking) {
        TString textYson = "{name=\"Alice\";age=30;city=\"NYC\"}";
        TString binaryYson = GetBinaryYson(textYson);
        TSet<TString> columnsToTrack = {"name", "age"};
        auto offsets = ParseYsonAndCollectColumnOffsets(binaryYson, columnsToTrack);

        UNIT_ASSERT(offsets.contains("name"));
        UNIT_ASSERT(offsets.contains("age"));
        UNIT_ASSERT(!offsets.contains("city"));
        UNIT_ASSERT_EQUAL(offsets.size(), 2);
    }

    Y_UNIT_TEST(ExampleUsage) {
        TString textYson = "{name=\"Alice\";age=30;city=\"NYC\"}";
        TString binaryYson = GetBinaryYson(textYson);
        TSet<TString> columnsToTrack = {"name", "age"};

        auto offsets = ParseYsonAndCollectColumnOffsets(binaryYson, columnsToTrack);

        if (offsets.contains("name")) {
            const auto& nameRange = offsets.at("name");
            TStringBuf columnData = TStringBuf(
                binaryYson.data() + nameRange.StartOffset,
                nameRange.EndOffset - nameRange.StartOffset
            );
            UNIT_ASSERT(!columnData.empty());
        }
    }

    Y_UNIT_TEST(ExtractMultipleColumns) {
        TString textYson = "{name=\"Alice\";age=30;city=\"NYC\"}";
        TString binaryYson = GetBinaryYson(textYson);
        TSet<TString> columnsToExtract = {"name", "city"};

        auto offsets = ParseYsonAndCollectColumnOffsets(binaryYson, columnsToExtract);

        THashMap<TString, TString> extractedData;

        for (const auto& [columnName, offsetRange] : offsets) {
            if (offsetRange.IsValid()) {
                TString columnData = binaryYson.substr(
                    offsetRange.StartOffset,
                    offsetRange.EndOffset - offsetRange.StartOffset
                );
                extractedData[columnName] = columnData;
            }
        }

        UNIT_ASSERT_EQUAL(extractedData.size(), 2);
        UNIT_ASSERT(extractedData.contains("name"));
        UNIT_ASSERT(extractedData.contains("city"));
    }

    Y_UNIT_TEST(ParseYsonWithAttributes) {
        TString textYson = "{name=\"Alice\";age=30;city=<attr1=\"value1\";attr2=\"value2\">\"NYC\"}";
        TString binaryYson = GetBinaryYson(textYson);
        TSet<TString> columnsToExtract = {"name", "city"};

        auto offsets = ParseYsonAndCollectColumnOffsets(binaryYson, columnsToExtract);

        THashMap<TString, TString> extractedData;

        for (const auto& [columnName, offsetRange] : offsets) {
            if (offsetRange.IsValid()) {
                TString columnData = binaryYson.substr(
                    offsetRange.StartOffset,
                    offsetRange.EndOffset - offsetRange.StartOffset
                );
                extractedData[columnName] = columnData;
            }
        }

        UNIT_ASSERT_EQUAL(extractedData.size(), 2);
        UNIT_ASSERT(extractedData.contains("name"));
        UNIT_ASSERT(extractedData.contains("city"));
    }
}

} // namespace NYql::NFmr
