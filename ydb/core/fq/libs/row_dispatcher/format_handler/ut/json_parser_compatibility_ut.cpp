#include <ydb/core/fq/libs/row_dispatcher/format_handler/parsers/json_parser.h>
#include <ydb/core/fq/libs/row_dispatcher/format_handler/ut/common/ut_common.h>

#include <util/stream/output.h>
#include <util/string/builder.h>

#include <optional>

namespace NFq::NRowDispatcher::NTests {
namespace {

// Same payloads and names as ClickHouseClient's test_parse_format.py.
// These tests record the current RD contract, including differences from ClickHouse.
class TJsonCompatibilityFixture: public TBaseFixture {
    class TConsumer: public IParsedDataConsumer {
    public:
        explicit TConsumer(TJsonCompatibilityFixture& owner, bool nullable)
            : Owner(owner)
            , Columns({ { "a", nullable ? "[OptionalType; [DataType; Int32]]" : "[DataType; Int32]" } })
            , Nullable(nullable)
        {
        }

        const TVector<TSchemaColumn>& GetColumns() const override {
            return Columns;
        }

        void OnParsingError(TStatus status) override {
            Owner.Error = status.GetErrorMessage();
        }

        void OnParsedData(ui64 rows) override {
            const auto column = Owner.Parser->GetParsedColumn(0);
            if (column.IsFail()) {
                Owner.Error = column.GetErrorMessage();
                return;
            }
            const auto values = column.GetResult();
            const auto offsets = Owner.Parser->GetOffsets();
            for (size_t i = 0; i < rows; ++i) {
                if (Nullable && !values[i]) {
                    Owner.Values.push_back(std::nullopt);
                } else {
                    Owner.Values.push_back(Nullable ? values[i].GetOptionalValue().Get<i32>() : values[i].Get<i32>());
                }
                Owner.Offsets.push_back(offsets[i]);
            }
        }

    private:
        TJsonCompatibilityFixture& Owner;
        const TVector<TSchemaColumn> Columns;
        const bool Nullable;
    };

public:
    void TearDown(NUnitTest::TTestContext& ctx) override {
        Parser.Reset();
        TBaseFixture::TearDown(ctx);
    }

    void Check(const TString& name, const std::vector<TMessageStreamRecord>& records, bool nullable, bool expectedError,
        const TVector<std::optional<i32>>& expectedValues = {}, const TVector<ui64>& expectedOffsets = {}, bool skipErrors = false)
    {
        auto consumer = MakeIntrusive<TConsumer>(*this, nullable);
        TJsonParserConfig config{ .FunctionRegistry = FunctionRegistry, .LatencyLimit = TDuration::Zero(), .SkipErrors = skipErrors };
        Parser = CheckSuccess(CreateJsonParser(consumer, config, Counters));
        Parser->ParseRecords(records);
        Parser->Refresh(true);

        TStringBuilder report;
        report << "RD_JSON_CASE " << name << " " << (Error ? "ERROR" : "OK") << " values=[";
        for (size_t i = 0; i < Values.size(); ++i) {
            if (i) {
                report << ",";
            }
            if (Values[i]) {
                report << *Values[i];
            } else {
                report << "null";
            }
        }
        report << "] offsets=[";
        for (size_t i = 0; i < Offsets.size(); ++i) {
            if (i) {
                report << ",";
            }
            report << Offsets[i];
        }
        report << "]";
        Cerr << report << Endl;
        UNIT_ASSERT_VALUES_EQUAL_C(bool(Error), expectedError, report << " " << Error.value_or(""));
        if (!expectedError) {
            UNIT_ASSERT_VALUES_EQUAL(Values.size(), expectedValues.size());
            UNIT_ASSERT_VALUES_EQUAL(Offsets.size(), expectedOffsets.size());
            for (size_t i = 0; i < Values.size(); ++i) {
                UNIT_ASSERT_C(Values[i] == expectedValues[i], report);
                UNIT_ASSERT_VALUES_EQUAL_C(Offsets[i], expectedOffsets[i], report);
            }
        }
    }

    void CheckNoParsingErrors() const {
        UNIT_ASSERT_VALUES_EQUAL(Counters.CountersSubgroup->GetCounter("ParsingErrors", true)->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(Counters.ReadGroupSubgroup->GetCounter("JsonParsingErrors", true)->Val(), 0);
    }

    void CheckSkipErrors(const TString& name, std::vector<TMessageStreamRecord> records, const TVector<std::optional<i32>>& retainedValues = {},
        const TVector<ui64>& retainedOffsets = {})
    {
        UNIT_ASSERT(!records.empty());
        const ui64 prefixOffset = records.front().Id.Offset - 1;
        const ui64 suffixOffset = records.back().Id.Offset + 1;
        records.insert(records.begin(), GetRecord(prefixOffset, R"({"a":101})"));
        records.push_back(GetRecord(suffixOffset, R"({"a":201}{"a":202})"));
        records.push_back(GetRecord(suffixOffset + 2, R"([{"a":203},{"a":204}])"));

        TVector<std::optional<i32>> values = { 101 };
        values.insert(values.end(), retainedValues.begin(), retainedValues.end());
        values.insert(values.end(), { 201, 202, 203, 204 });
        TVector<ui64> offsets = { prefixOffset };
        offsets.insert(offsets.end(), retainedOffsets.begin(), retainedOffsets.end());
        offsets.insert(offsets.end(), { suffixOffset, suffixOffset, suffixOffset + 2, suffixOffset + 2 });
        Check(name, records, false, false, values, offsets, true);
    }

private:
    TCountersDesc Counters;
    ITopicParser::TPtr Parser;
    std::optional<TString> Error;
    TVector<std::optional<i32>> Values;
    TVector<ui64> Offsets;
};

}   // namespace

Y_UNIT_TEST_SUITE(RowDispatcherJsonCompatibility) {
    Y_UNIT_TEST_F(SingleObject, TJsonCompatibilityFixture) {
        Check("SingleObject", { GetRecord(42, "{\"a\":1}") }, false, false, { 1 }, { 42 });
    }

    Y_UNIT_TEST_F(AdjacentObjects, TJsonCompatibilityFixture) {
        Check("AdjacentObjects", { GetRecord(42, "{\"a\":1}{\"a\":2}") }, false, false, { 1, 2 }, { 42, 42 });
    }

    Y_UNIT_TEST_F(SpaceSeparatedObjects, TJsonCompatibilityFixture) {
        Check("SpaceSeparatedObjects", { GetRecord(42, "{\"a\":1} {\"a\":2}") }, false, false, { 1, 2 }, { 42, 42 });
    }

    Y_UNIT_TEST_F(NewlineSeparatedObjects, TJsonCompatibilityFixture) {
        Check("NewlineSeparatedObjects", { GetRecord(42, "{\"a\":1}\n{\"a\":2}") }, false, false, { 1, 2 }, { 42, 42 });
    }

    Y_UNIT_TEST_F(CrLfSeparatedObjects, TJsonCompatibilityFixture) {
        Check("CrLfSeparatedObjects", { GetRecord(42, "{\"a\":1}\r\n{\"a\":2}") }, false, false, { 1, 2 }, { 42, 42 });
    }

    Y_UNIT_TEST_F(TabSeparatedObjects, TJsonCompatibilityFixture) {
        Check("TabSeparatedObjects", { GetRecord(42, "{\"a\":1}\t{\"a\":2}") }, false, false, { 1, 2 }, { 42, 42 });
    }

    Y_UNIT_TEST_F(BlankLinesBetweenObjects, TJsonCompatibilityFixture) {
        Check("BlankLinesBetweenObjects", { GetRecord(42, "{\"a\":1}\n\n \t\n{\"a\":2}") }, false, false, { 1, 2 }, { 42, 42 });
    }

    Y_UNIT_TEST_F(CommaSeparatedObjects, TJsonCompatibilityFixture) {
        Check("CommaSeparatedObjects", { GetRecord(42, "{\"a\":1}, {\"a\":2}") }, false, true);
    }

    Y_UNIT_TEST_F(ArrayOfObjects, TJsonCompatibilityFixture) {
        Check("ArrayOfObjects", { GetRecord(42, "[{\"a\":1},{\"a\":2}]") }, false, false, { 1, 2 }, { 42, 42 });
    }

    Y_UNIT_TEST_F(MultilineArray, TJsonCompatibilityFixture) {
        Check("MultilineArray", { GetRecord(42, "[\n {\"a\":1},\n {\"a\":2}\n ]") }, false, false, { 1, 2 }, { 42, 42 });
    }

    Y_UNIT_TEST_F(ArraysAndObjectsAcrossMessages, TJsonCompatibilityFixture) {
        Check("ArraysAndObjectsAcrossMessages", { GetRecord(42, R"([][{"a":1},{"a":2}]{"a":3})"), GetRecord(43, R"([{"a":4}][])") }, false,
            false, { 1, 2, 3, 4 }, { 42, 42, 42, 43 });
    }

    Y_UNIT_TEST_F(ScalarAfterObjectInArray, TJsonCompatibilityFixture) {
        Check("ScalarAfterObjectInArray", { GetRecord(42, R"([{"a":1},null])") }, false, true);
    }

    Y_UNIT_TEST_F(TrailingCommaInArray, TJsonCompatibilityFixture) {
        Check("TrailingCommaInArray", { GetRecord(42, R"([{"a":1},])") }, false, true);
    }

    Y_UNIT_TEST_F(NullableArrayElements, TJsonCompatibilityFixture) {
        Check("NullableArrayElements", { GetRecord(42, R"([{"a":1},{"a":null},{}])") }, true, false, { 1, std::nullopt, std::nullopt },
            { 42, 42, 42 });
    }

    Y_UNIT_TEST_F(LargeArray, TJsonCompatibilityFixture) {
        TStringBuilder input;
        input << "[";
        TVector<std::optional<i32>> values;
        TVector<ui64> offsets;
        for (size_t i = 0; i < 2001; ++i) {
            if (i) {
                input << ",";
            }
            input << "{\"a\":" << i << "}";
            values.push_back(i);
            offsets.push_back(42);
        }
        input << "]";
        Check("LargeArray", { GetRecord(42, input) }, false, false, values, offsets);
    }

    Y_UNIT_TEST_F(AdjacentObjectsInArray, TJsonCompatibilityFixture) {
        Check("AdjacentObjectsInArray", { GetRecord(42, "[{\"a\":1}{\"a\":2}]") }, false, true);
    }

    Y_UNIT_TEST_F(MultilineObject, TJsonCompatibilityFixture) {
        Check("MultilineObject", { GetRecord(42, "{\n \"a\" : 1\n}\n{\n \"a\" : 2\n}") }, false, false, { 1, 2 }, { 42, 42 });
    }

    Y_UNIT_TEST_F(SurroundingWhitespace, TJsonCompatibilityFixture) {
        Check("SurroundingWhitespace", { GetRecord(42, " \r\n\t{\"a\":1}{\"a\":2}\t\r\n ") }, false, false, { 1, 2 }, { 42, 42 });
    }

    Y_UNIT_TEST_F(EmptyStream, TJsonCompatibilityFixture) {
        Check("EmptyStream", {}, false, false, {}, {});
    }

    Y_UNIT_TEST_F(EmptyPayload, TJsonCompatibilityFixture) {
        Check("EmptyPayload", { GetRecord(42, "") }, false, false);
        CheckNoParsingErrors();
    }

    Y_UNIT_TEST_F(SpaceOnlyPayload, TJsonCompatibilityFixture) {
        Check("SpaceOnlyPayload", { GetRecord(42, " ") }, false, false);
        CheckNoParsingErrors();
    }

    Y_UNIT_TEST_F(WhitespaceOnlyPayload, TJsonCompatibilityFixture) {
        Check("WhitespaceOnlyPayload", { GetRecord(42, "\n\r\t  \r\n") }, false, false);
        CheckNoParsingErrors();
    }

    Y_UNIT_TEST_F(EmptyArray, TJsonCompatibilityFixture) {
        Check("EmptyArray", { GetRecord(42, "[]") }, false, false);
    }

    Y_UNIT_TEST_F(WhitespaceOnlyArray, TJsonCompatibilityFixture) {
        Check("WhitespaceOnlyArray", { GetRecord(42, "[ \n\t ]") }, false, false);
    }

    Y_UNIT_TEST_F(EmptyObject, TJsonCompatibilityFixture) {
        // Optional a isolates object framing from required-column validation.
        Check("EmptyObject", { GetRecord(42, "{}") }, true, false, { std::nullopt }, { 42 });
    }

    Y_UNIT_TEST_F(EmptyObjects, TJsonCompatibilityFixture) {
        Check("EmptyObjects", { GetRecord(42, "{}{}") }, true, false, { std::nullopt, std::nullopt }, { 42, 42 });
    }

    Y_UNIT_TEST_F(MissingRequiredField, TJsonCompatibilityFixture) {
        // A schema error despite the valid JSON object.
        Check("MissingRequiredField", { GetRecord(42, "{}") }, false, true);
    }

    Y_UNIT_TEST_F(EmptyMessagesBetweenObjects, TJsonCompatibilityFixture) {
        Check("EmptyMessagesBetweenObjects",
            { GetRecord(42, ""), GetRecord(43, "{\"a\":1}{\"a\":2}"), GetRecord(44, " \n"), GetRecord(45, "[{\"a\":3}]"), GetRecord(46, "") },
            false, false, { 1, 2, 3 }, { 43, 43, 45 });
        CheckNoParsingErrors();
    }

    Y_UNIT_TEST_F(SeveralPayloads, TJsonCompatibilityFixture) {
        Check("SeveralPayloads", { GetRecord(42, "{\"a\":1}{\"a\":2}"), GetRecord(43, "{\"a\":3}") }, false, false, { 1, 2, 3 }, { 42, 42, 43 });
    }

    Y_UNIT_TEST_F(QuotedNumber, TJsonCompatibilityFixture) {
        Check("QuotedNumber", { GetRecord(42, "{\"a\":\"1\"}{\"a\":2}") }, false, false, { 1, 2 }, { 42, 42 });
    }

    Y_UNIT_TEST_F(NestedUnknownFields, TJsonCompatibilityFixture) {
        Check("NestedUnknownFields", { GetRecord(42, "{\"a\":1,\"unused\":{\"array\":[1,{\"x\":2}],\"text\":\"} {\\n\"}}{\"a\":2}") }, false,
            false, { 1, 2 }, { 42, 42 });
    }

    Y_UNIT_TEST_F(TrailingSemicolon, TJsonCompatibilityFixture) {
        Check("TrailingSemicolon", { GetRecord(42, "{\"a\":1};") }, false, true);
    }

    Y_UNIT_TEST_F(ArrayWithSemicolon, TJsonCompatibilityFixture) {
        Check("ArrayWithSemicolon", { GetRecord(42, "[{\"a\":1},{\"a\":2}];") }, false, true);
    }

    Y_UNIT_TEST_F(RootNull, TJsonCompatibilityFixture) {
        Check("RootNull", { GetRecord(42, "null") }, false, true);
    }

    Y_UNIT_TEST_F(RootNumber, TJsonCompatibilityFixture) {
        Check("RootNumber", { GetRecord(42, "123") }, false, true);
    }

    Y_UNIT_TEST_F(RootBoolean, TJsonCompatibilityFixture) {
        Check("RootBoolean", { GetRecord(42, "true") }, false, true);
    }

    Y_UNIT_TEST_F(RootString, TJsonCompatibilityFixture) {
        Check("RootString", { GetRecord(42, "\"text\"") }, false, true);
    }

    Y_UNIT_TEST_F(ArrayOfScalars, TJsonCompatibilityFixture) {
        Check("ArrayOfScalars", { GetRecord(42, "[1,2]") }, false, true);
    }

    Y_UNIT_TEST_F(NestedRootArray, TJsonCompatibilityFixture) {
        Check("NestedRootArray", { GetRecord(42, "[[{\"a\":1}]]") }, false, true);
    }

    Y_UNIT_TEST_F(LeadingComma, TJsonCompatibilityFixture) {
        Check("LeadingComma", { GetRecord(42, ",{\"a\":1}") }, false, true);
    }

    Y_UNIT_TEST_F(UnclosedObject, TJsonCompatibilityFixture) {
        Check("UnclosedObject", { GetRecord(42, "{\"a\":1") }, false, true);
    }

    Y_UNIT_TEST_F(UnclosedArray, TJsonCompatibilityFixture) {
        Check("UnclosedArray", { GetRecord(42, "[{\"a\":1}") }, false, true);
    }

    Y_UNIT_TEST_F(MissingColon, TJsonCompatibilityFixture) {
        Check("MissingColon", { GetRecord(42, "{\"a\" 1}") }, false, true);
    }

    Y_UNIT_TEST_F(MissingFieldComma, TJsonCompatibilityFixture) {
        Check("MissingFieldComma", { GetRecord(42, "{\"a\":1 \"b\":2}") }, false, true);
    }

    Y_UNIT_TEST_F(WrongFieldType, TJsonCompatibilityFixture) {
        Check("WrongFieldType", { GetRecord(42, "{\"a\":\"not a number\"}") }, false, true);
    }

    Y_UNIT_TEST_F(DuplicateField, TJsonCompatibilityFixture) {
        Check("DuplicateField", { GetRecord(42, "{\"a\":1,\"a\":2}") }, false, false, { 1 }, { 42 });
    }

    Y_UNIT_TEST_F(GarbageAfterObject, TJsonCompatibilityFixture) {
        Check("GarbageAfterObject", { GetRecord(42, "{\"a\":1} garbage") }, false, true);
    }

    Y_UNIT_TEST_F(TruncatedTail, TJsonCompatibilityFixture) {
        Check("TruncatedTail", { GetRecord(42, "{\"a\":1}{") }, false, true);
    }

    Y_UNIT_TEST_F(JsonSplitAcrossMessages, TJsonCompatibilityFixture) {
        Check("JsonSplitAcrossMessages", { GetRecord(42, "{\"a\":1"), GetRecord(43, "}") }, false, true);
    }

    Y_UNIT_TEST_F(RepeatedMessageMetadata, TJsonCompatibilityFixture) {
        Check("RepeatedMessageMetadata", { GetRecord(42, "{\"a\":1}{\"a\":2}"), GetRecord(43, "{\"a\":3}") }, false, false, { 1, 2, 3 },
            { 42, 42, 43 });
    }

    Y_UNIT_TEST_F(EmptyMessagesDoNotShiftMetadata, TJsonCompatibilityFixture) {
        Check("EmptyMessagesDoNotShiftMetadata",
            { GetRecord(41, ""), GetRecord(42, "[{\"a\":1},{\"a\":2}]"), GetRecord(43, " \n"), GetRecord(44, "{\"a\":3}") }, false, false,
            { 1, 2, 3 }, { 42, 42, 44 });
        CheckNoParsingErrors();
    }

    Y_UNIT_TEST_F(NullableFieldAndMissingField, TJsonCompatibilityFixture) {
        Check("NullableFieldAndMissingField", { GetRecord(42, "{\"a\":1}{\"a\":null}{}") }, true, false, { 1, std::nullopt, std::nullopt },
            { 42, 42, 42 });
    }

    // SkipJsonErrors=true (TJsonParserConfig::SkipErrors). Error cases and empty
    // messages are repeated with valid messages before and after the original payloads.
    // Check the surviving rows as well as recovery, ordering and topic offsets.
    Y_UNIT_TEST_F(SkipBadArrayRow, TJsonCompatibilityFixture) {
        Check("SkipBadArrayRow", { GetRecord(42, R"([{"a":1},{"a":"bad"},{},{"a":2}])") }, false, false, { 1, 2 }, { 42, 42 }, true);
    }

    Y_UNIT_TEST_F(SkipMalformedArrayKeepsNextOffset, TJsonCompatibilityFixture) {
        Check("SkipMalformedArrayKeepsNextOffset", { GetRecord(42, R"([{"a":1},null,{"a":2}])"), GetRecord(43, R"([{"a":3}])") }, false, false,
            { 1, 3 }, { 42, 43 }, true);
    }

    Y_UNIT_TEST_F(CommaSeparatedObjectsSkipErrors, TJsonCompatibilityFixture) {
        CheckSkipErrors("CommaSeparatedObjectsSkipErrors", { GetRecord(42, "{\"a\":1}, {\"a\":2}") }, { 1 }, { 42 });
    }

    Y_UNIT_TEST_F(ScalarAfterObjectInArraySkipErrors, TJsonCompatibilityFixture) {
        CheckSkipErrors("ScalarAfterObjectInArraySkipErrors", { GetRecord(42, R"([{"a":1},null])") }, { 1 }, { 42 });
    }

    Y_UNIT_TEST_F(TrailingCommaInArraySkipErrors, TJsonCompatibilityFixture) {
        CheckSkipErrors("TrailingCommaInArraySkipErrors", { GetRecord(42, R"([{"a":1},])") }, { 1 }, { 42 });
    }

    Y_UNIT_TEST_F(AdjacentObjectsInArraySkipErrors, TJsonCompatibilityFixture) {
        CheckSkipErrors("AdjacentObjectsInArraySkipErrors", { GetRecord(42, "[{\"a\":1}{\"a\":2}]") }, { 1 }, { 42 });
    }

    Y_UNIT_TEST_F(EmptyPayloadSkipErrors, TJsonCompatibilityFixture) {
        CheckSkipErrors("EmptyPayloadSkipErrors", { GetRecord(42, "") });
        CheckNoParsingErrors();
    }

    Y_UNIT_TEST_F(SpaceOnlyPayloadSkipErrors, TJsonCompatibilityFixture) {
        CheckSkipErrors("SpaceOnlyPayloadSkipErrors", { GetRecord(42, " ") });
        CheckNoParsingErrors();
    }

    Y_UNIT_TEST_F(WhitespaceOnlyPayloadSkipErrors, TJsonCompatibilityFixture) {
        CheckSkipErrors("WhitespaceOnlyPayloadSkipErrors", { GetRecord(42, "\n\r\t  \r\n") });
        CheckNoParsingErrors();
    }

    Y_UNIT_TEST_F(MissingRequiredFieldSkipErrors, TJsonCompatibilityFixture) {
        // A schema error despite the valid JSON object.
        CheckSkipErrors("MissingRequiredFieldSkipErrors", { GetRecord(42, "{}") });
    }

    Y_UNIT_TEST_F(EmptyMessagesBetweenObjectsSkipErrors, TJsonCompatibilityFixture) {
        CheckSkipErrors("EmptyMessagesBetweenObjectsSkipErrors",
            { GetRecord(42, ""), GetRecord(43, "{\"a\":1}{\"a\":2}"), GetRecord(44, " \n"), GetRecord(45, "[{\"a\":3}]"), GetRecord(46, "") },
            { 1, 2, 3 }, { 43, 43, 45 });
        CheckNoParsingErrors();
    }

    Y_UNIT_TEST_F(TrailingSemicolonSkipErrors, TJsonCompatibilityFixture) {
        CheckSkipErrors("TrailingSemicolonSkipErrors", { GetRecord(42, "{\"a\":1};") }, { 1 }, { 42 });
    }

    Y_UNIT_TEST_F(ArrayWithSemicolonSkipErrors, TJsonCompatibilityFixture) {
        CheckSkipErrors("ArrayWithSemicolonSkipErrors", { GetRecord(42, "[{\"a\":1},{\"a\":2}];") }, { 1, 2 }, { 42, 42 });
    }

    Y_UNIT_TEST_F(RootNullSkipErrors, TJsonCompatibilityFixture) {
        CheckSkipErrors("RootNullSkipErrors", { GetRecord(42, "null") });
    }

    Y_UNIT_TEST_F(RootNumberSkipErrors, TJsonCompatibilityFixture) {
        CheckSkipErrors("RootNumberSkipErrors", { GetRecord(42, "123") });
    }

    Y_UNIT_TEST_F(RootBooleanSkipErrors, TJsonCompatibilityFixture) {
        CheckSkipErrors("RootBooleanSkipErrors", { GetRecord(42, "true") });
    }

    Y_UNIT_TEST_F(RootStringSkipErrors, TJsonCompatibilityFixture) {
        CheckSkipErrors("RootStringSkipErrors", { GetRecord(42, "\"text\"") });
    }

    Y_UNIT_TEST_F(ArrayOfScalarsSkipErrors, TJsonCompatibilityFixture) {
        CheckSkipErrors("ArrayOfScalarsSkipErrors", { GetRecord(42, "[1,2]") });
    }

    Y_UNIT_TEST_F(NestedRootArraySkipErrors, TJsonCompatibilityFixture) {
        CheckSkipErrors("NestedRootArraySkipErrors", { GetRecord(42, "[[{\"a\":1}]]") });
    }

    Y_UNIT_TEST_F(LeadingCommaSkipErrors, TJsonCompatibilityFixture) {
        CheckSkipErrors("LeadingCommaSkipErrors", { GetRecord(42, ",{\"a\":1}") });
    }

    Y_UNIT_TEST_F(UnclosedObjectSkipErrors, TJsonCompatibilityFixture) {
        CheckSkipErrors("UnclosedObjectSkipErrors", { GetRecord(42, "{\"a\":1") });
    }

    Y_UNIT_TEST_F(UnclosedArraySkipErrors, TJsonCompatibilityFixture) {
        CheckSkipErrors("UnclosedArraySkipErrors", { GetRecord(42, "[{\"a\":1}") });
    }

    Y_UNIT_TEST_F(MissingColonSkipErrors, TJsonCompatibilityFixture) {
        CheckSkipErrors("MissingColonSkipErrors", { GetRecord(42, "{\"a\" 1}") });
    }

    Y_UNIT_TEST_F(MissingFieldCommaSkipErrors, TJsonCompatibilityFixture) {
        CheckSkipErrors("MissingFieldCommaSkipErrors", { GetRecord(42, "{\"a\":1 \"b\":2}") });
    }

    Y_UNIT_TEST_F(WrongFieldTypeSkipErrors, TJsonCompatibilityFixture) {
        CheckSkipErrors("WrongFieldTypeSkipErrors", { GetRecord(42, "{\"a\":\"not a number\"}") });
    }

    Y_UNIT_TEST_F(GarbageAfterObjectSkipErrors, TJsonCompatibilityFixture) {
        CheckSkipErrors("GarbageAfterObjectSkipErrors", { GetRecord(42, "{\"a\":1} garbage") }, { 1 }, { 42 });
    }

    Y_UNIT_TEST_F(TruncatedTailSkipErrors, TJsonCompatibilityFixture) {
        CheckSkipErrors("TruncatedTailSkipErrors", { GetRecord(42, "{\"a\":1}{") }, { 1 }, { 42 });
    }

    Y_UNIT_TEST_F(JsonSplitAcrossMessagesSkipErrors, TJsonCompatibilityFixture) {
        CheckSkipErrors("JsonSplitAcrossMessagesSkipErrors", { GetRecord(42, "{\"a\":1"), GetRecord(43, "}") });
    }

    Y_UNIT_TEST_F(EmptyMessagesDoNotShiftMetadataSkipErrors, TJsonCompatibilityFixture) {
        CheckSkipErrors("EmptyMessagesDoNotShiftMetadataSkipErrors",
            { GetRecord(41, ""), GetRecord(42, "[{\"a\":1},{\"a\":2}]"), GetRecord(43, " \n"), GetRecord(44, "{\"a\":3}") }, { 1, 2, 3 },
            { 42, 42, 44 });
        CheckNoParsingErrors();
    }
}

}   // namespace NFq::NRowDispatcher::NTests
