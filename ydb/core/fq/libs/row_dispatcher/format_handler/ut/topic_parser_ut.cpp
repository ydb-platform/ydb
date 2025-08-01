#include <ydb/core/fq/libs/row_dispatcher/format_handler/parsers/json_parser.h>
#include <ydb/core/fq/libs/row_dispatcher/format_handler/parsers/raw_parser.h>
#include <ydb/core/fq/libs/row_dispatcher/format_handler/ut/common/ut_common.h>

namespace NFq::NRowDispatcher::NTests {

namespace {

class TBaseParserFixture : public TBaseFixture {
public:
    static constexpr ui64 FIRST_OFFSET = 42;

    using TBase = TBaseFixture;
    using TCallback = std::function<void(ui64 numberRows, TVector<const TVector<NYql::NUdf::TUnboxedValue>*> result)>;

    class TParsedDataConsumer : public IParsedDataConsumer {
    public:
        using TPtr = TIntrusivePtr<TParsedDataConsumer>;

    public:
        TParsedDataConsumer(const TBaseParserFixture& self, const TVector<TSchemaColumn>& columns, TCallback callback)
            : Self(self)
            , Columns(columns)
            , Callback(callback)
        {}

        void ExpectColumnError(ui64 columnId, TStatusCode statusCode, const TString& message) {
            UNIT_ASSERT_C(ExpectedErrors.insert({columnId, {statusCode, message}}).second, "Can not add existing column error");
        }

        void ExpectCommonError(TStatusCode statusCode, const TString& message) {
            UNIT_ASSERT_C(!ExpectedCommonError, "Can not add existing common error");
            ExpectedCommonError = {statusCode, message};
        }

    public:
        const TVector<TSchemaColumn>& GetColumns() const override {
            return Columns;
        }

        void OnParsingError(TStatus status) override {
            NumberBatches++;
            CurrentOffset++;
            if (ExpectedCommonError) {
                CheckError(status, ExpectedCommonError->first, ExpectedCommonError->second);
                ExpectedCommonError = std::nullopt;
            } else {
                CheckSuccess(status);
            }
        }

        void OnParsedData(ui64 numberRows) override {
            NumberBatches++;
            UNIT_ASSERT_C(!ExpectedCommonError, "Expected common error: " << ExpectedCommonError->second);

            const auto& offsets = Self.Parser->GetOffsets();
            UNIT_ASSERT_VALUES_EQUAL_C(offsets.size(), numberRows, "Unexpected offsets size");
            for (const ui64 offset : offsets) {
                UNIT_ASSERT_VALUES_EQUAL_C(offset, CurrentOffset, "Unexpected offset");
                CurrentOffset++;
            }

            TVector<const TVector<NYql::NUdf::TUnboxedValue>*> result(Columns.size(), nullptr);
            for (ui64 i = 0; i < Columns.size(); ++i) {
                if (const auto it = ExpectedErrors.find(i); it != ExpectedErrors.end()) {
                    CheckError(Self.Parser->GetParsedColumn(i), it->second.first, it->second.second);
                    ExpectedErrors.erase(i);
                } else {
                    result[i] = CheckSuccess(Self.Parser->GetParsedColumn(i));
                }
            }
            Callback(numberRows, std::move(result));
        }

    public:
        ui64 CurrentOffset = FIRST_OFFSET;
        ui64 NumberBatches = 0;

    private:
        const TBaseParserFixture& Self;
        const TVector<TSchemaColumn> Columns;
        const TCallback Callback;

        std::optional<std::pair<TStatusCode, TString>> ExpectedCommonError;
        std::unordered_map<ui64, std::pair<TStatusCode, TString>> ExpectedErrors;
    };

public:
    void TearDown(NUnitTest::TTestContext& ctx) override {
        if (ParserHandler) {
            UNIT_ASSERT_VALUES_EQUAL_C(ExpectedBatches, ParserHandler->NumberBatches, "Unexpected number of batches");
        }
        Parser.Reset();
        ParserHandler.Reset();

        TBase::TearDown(ctx);
    }

public:
    TStatus MakeParser(TVector<TSchemaColumn> columns, TCallback callback) {
        ParserHandler = MakeIntrusive<TParsedDataConsumer>(*this, columns, callback);

        auto parserStatus = CreateParser();
        if (parserStatus.IsFail()) {
            return parserStatus;
        }

        Parser = parserStatus.DetachResult();
        return TStatus::Success();
    }

    TStatus MakeParser(TVector<TString> columnNames, TString columnType, TCallback callback) {
        TVector<TSchemaColumn> columns;
        for (const auto& columnName : columnNames) {
            columns.push_back({.Name = columnName, .TypeYson = columnType});
        }
        return MakeParser(columns, callback);
    }

    TStatus MakeParser(TVector<TString> columnNames, TString columnType) {
        return MakeParser(columnNames, columnType, [](ui64, TVector<const TVector<NYql::NUdf::TUnboxedValue>*>) {});
    }

    TStatus MakeParser(TVector<TSchemaColumn> columns) {
        return MakeParser(columns, [](ui64, TVector<const TVector<NYql::NUdf::TUnboxedValue>*>) {});
    }

    void PushToParser(ui64 offset, const TString& data) {
        ExpectedBatches++;
        Parser->ParseMessages({GetMessage(offset, data)});
    }

    void CheckColumnError(const TString& data, ui64 columnId, TStatusCode statusCode, const TString& message) {
        ExpectedBatches++;
        ParserHandler->ExpectColumnError(columnId, statusCode, message);
        Parser->ParseMessages({GetMessage(ParserHandler->CurrentOffset, data)});
    }

    void CheckBatchError(const TString& data, TStatusCode statusCode, const TString& message) {
        ExpectedBatches++;
        ParserHandler->ExpectCommonError(statusCode, message);
        Parser->ParseMessages({GetMessage(ParserHandler->CurrentOffset, data)});
    }

protected:
    virtual TValueStatus<ITopicParser::TPtr> CreateParser() = 0;

public:
    TParsedDataConsumer::TPtr ParserHandler;
    ITopicParser::TPtr Parser;
    ui64 ExpectedBatches = 0;
};

class TJsonParserFixture : public TBaseParserFixture {
    using TBase = TBaseParserFixture;

public:
    TJsonParserFixture()
        : TBase()
        , Config({.BatchSize = 1_MB, .LatencyLimit = TDuration::Zero(), .BufferCellCount = 1000})
    {}

protected:
    TValueStatus<ITopicParser::TPtr> CreateParser() override {
        return CreateJsonParser(ParserHandler, Config, {});
    }

public:
    TJsonParserConfig Config;
};

class TRawParserFixture : public TBaseParserFixture {
protected:
    TValueStatus<ITopicParser::TPtr> CreateParser() override {
        return CreateRawParser(ParserHandler, {});
    }
};

}  // anonymous namespace

Y_UNIT_TEST_SUITE(TestJsonParser) {
    Y_UNIT_TEST_F(Simple1, TJsonParserFixture) {
        CheckSuccess(MakeParser({{"a1", "[DataType; String]"}, {"a2", "[OptionalType; [DataType; Uint64]]"}}, [](ui64 numberRows, TVector<const TVector<NYql::NUdf::TUnboxedValue>*> result) {
            UNIT_ASSERT_VALUES_EQUAL(1, numberRows);
            UNIT_ASSERT_VALUES_EQUAL(2, result.size());
            UNIT_ASSERT_VALUES_EQUAL("hello1", TString(result[0]->at(0).AsStringRef()));
            UNIT_ASSERT_VALUES_EQUAL(101, result[1]->at(0).GetOptionalValue().Get<ui64>());
        }));
        PushToParser(FIRST_OFFSET, R"({"a1": "hello1", "a2": 101, "event": "event1"})");
    }

    Y_UNIT_TEST_F(Simple2, TJsonParserFixture) {
        CheckSuccess(MakeParser({"a2", "a1"}, "[DataType; String]", [](ui64 numberRows, TVector<const TVector<NYql::NUdf::TUnboxedValue>*> result) {
            UNIT_ASSERT_VALUES_EQUAL(1, numberRows);
            UNIT_ASSERT_VALUES_EQUAL(2, result.size());
            UNIT_ASSERT_VALUES_EQUAL("101", TString(result[0]->at(0).AsStringRef()));
            UNIT_ASSERT_VALUES_EQUAL("hello1", TString(result[1]->at(0).AsStringRef()));
        }));
        PushToParser(FIRST_OFFSET, R"({"a1": "hello1", "a2": "101", "event": "event1"})");
    }

    Y_UNIT_TEST_F(Simple3, TJsonParserFixture) {
        CheckSuccess(MakeParser({"a1", "a2"}, "[DataType; String]", [](ui64 numberRows, TVector<const TVector<NYql::NUdf::TUnboxedValue>*> result) {
            UNIT_ASSERT_VALUES_EQUAL(1, numberRows);
            UNIT_ASSERT_VALUES_EQUAL(2, result.size());
            UNIT_ASSERT_VALUES_EQUAL("101", TString(result[0]->at(0).AsStringRef()));
            UNIT_ASSERT_VALUES_EQUAL("hello1", TString(result[1]->at(0).AsStringRef()));
        }));
        PushToParser(FIRST_OFFSET,R"({"a2": "hello1", "a1": "101", "event": "event1"})");
    }

    Y_UNIT_TEST_F(Simple4, TJsonParserFixture) {
        CheckSuccess(MakeParser({"a2", "a1"}, "[DataType; String]", [](ui64 numberRows, TVector<const TVector<NYql::NUdf::TUnboxedValue>*> result) {
            UNIT_ASSERT_VALUES_EQUAL(1, numberRows);
            UNIT_ASSERT_VALUES_EQUAL(2, result.size());
            UNIT_ASSERT_VALUES_EQUAL("hello1", TString(result[0]->at(0).AsStringRef()));
            UNIT_ASSERT_VALUES_EQUAL("101", TString(result[1]->at(0).AsStringRef()));
        }));
        PushToParser(FIRST_OFFSET, R"({"a2": "hello1", "a1": "101", "event": "event1"})");
    }

    Y_UNIT_TEST_F(LargeStrings, TJsonParserFixture) {
        ExpectedBatches = 1;

        const TString largeString = "abcdefghjkl1234567890+abcdefghjkl1234567890";

        CheckSuccess(MakeParser({"col"}, "[DataType; String]", [&](ui64 numberRows, TVector<const TVector<NYql::NUdf::TUnboxedValue>*> result) {
            UNIT_ASSERT_VALUES_EQUAL(2, numberRows);
            UNIT_ASSERT_VALUES_EQUAL(1, result.size());
            UNIT_ASSERT_VALUES_EQUAL(largeString, TString(result[0]->at(0).AsStringRef()));
            UNIT_ASSERT_VALUES_EQUAL(largeString, TString(result[0]->at(1).AsStringRef()));
        }));

        const TString jsonString = TStringBuilder() << "{\"col\": \"" << largeString << "\"}";
        Parser->ParseMessages({
            GetMessage(FIRST_OFFSET, jsonString),
            GetMessage(FIRST_OFFSET + 1, jsonString)
        });
    }

    Y_UNIT_TEST_F(ManyValues, TJsonParserFixture) {
        ExpectedBatches = 1;

        CheckSuccess(MakeParser({"a1", "a2"}, "[DataType; String]", [&](ui64 numberRows, TVector<const TVector<NYql::NUdf::TUnboxedValue>*> result) {
            UNIT_ASSERT_VALUES_EQUAL(3, numberRows);
            UNIT_ASSERT_VALUES_EQUAL(2, result.size());
            for (size_t i = 0; i < numberRows; ++i) {
                UNIT_ASSERT_VALUES_EQUAL_C("hello1", TString(result[0]->at(i).AsStringRef()), i);
                UNIT_ASSERT_VALUES_EQUAL_C("101", TString(result[1]->at(i).AsStringRef()), i);
            }
        }));

        Parser->ParseMessages({
            GetMessage(FIRST_OFFSET, R"({"a1": "hello1", "a2": "101", "event": "event1"})"),
            GetMessage(FIRST_OFFSET + 1, R"({"a1": "hello1", "a2": "101", "event": "event2"})"),
            GetMessage(FIRST_OFFSET + 2, R"({"a2": "101", "a1": "hello1", "event": "event3"})")
        });
    }

    Y_UNIT_TEST_F(MissingFields, TJsonParserFixture) {
        ExpectedBatches = 1;

        CheckSuccess(MakeParser({{"a1", "[OptionalType; [DataType; String]]"}, {"a2", "[OptionalType; [DataType; Uint64]]"}}, [&](ui64 numberRows, TVector<const TVector<NYql::NUdf::TUnboxedValue>*> result) {
            UNIT_ASSERT_VALUES_EQUAL(3, numberRows);
            UNIT_ASSERT_VALUES_EQUAL(2, result.size());
            for (size_t i = 0; i < numberRows; ++i) {
                if (i == 2) {
                    UNIT_ASSERT_C(!result[0]->at(i), i);
                } else {
                    NYql::NUdf::TUnboxedValue value = result[0]->at(i).GetOptionalValue();
                    UNIT_ASSERT_VALUES_EQUAL_C("hello1", TString(value.AsStringRef()), i);
                }
                if (i == 1) {
                    UNIT_ASSERT_C(!result[1]->at(i), i);
                } else {
                    UNIT_ASSERT_VALUES_EQUAL_C(101, result[1]->at(i).GetOptionalValue().Get<ui64>(), i);
                }
            }
        }));

        Parser->ParseMessages({
            GetMessage(FIRST_OFFSET, R"({"a1": "hello1", "a2": 101  , "event": "event1"})"),
            GetMessage(FIRST_OFFSET + 1, R"({"a1": "hello1", "event": "event2"})"),
            GetMessage(FIRST_OFFSET + 2, R"({"a2": "101", "a1": null, "event": "event3"})")
        });
    }

    Y_UNIT_TEST_F(NestedTypes, TJsonParserFixture) {
        ExpectedBatches = 1;

        CheckSuccess(MakeParser({{"nested", "[OptionalType; [DataType; Json]]"}, {"a1", "[DataType; String]"}}, [&](ui64 numberRows, TVector<const TVector<NYql::NUdf::TUnboxedValue>*> result) {
            UNIT_ASSERT_VALUES_EQUAL(4, numberRows);

            UNIT_ASSERT_VALUES_EQUAL(2, result.size());
            UNIT_ASSERT_VALUES_EQUAL("{\"key\": \"value\"}", TString(result[0]->at(0).AsStringRef()));
            UNIT_ASSERT_VALUES_EQUAL("hello1", TString(result[1]->at(0).AsStringRef()));

            UNIT_ASSERT_VALUES_EQUAL("[\"key1\", \"key2\"]", TString(result[0]->at(1).AsStringRef()));
            UNIT_ASSERT_VALUES_EQUAL("hello2", TString(result[1]->at(1).AsStringRef()));

            UNIT_ASSERT_VALUES_EQUAL("\"some string\"", TString(result[0]->at(2).AsStringRef()));
            UNIT_ASSERT_VALUES_EQUAL("hello3", TString(result[1]->at(2).AsStringRef()));

            UNIT_ASSERT_VALUES_EQUAL("123456", TString(result[0]->at(3).AsStringRef()));
            UNIT_ASSERT_VALUES_EQUAL("hello4", TString(result[1]->at(3).AsStringRef()));
        }));

        Parser->ParseMessages({
            GetMessage(FIRST_OFFSET, R"({"a1": "hello1", "nested": {"key": "value"}})"),
            GetMessage(FIRST_OFFSET + 1, R"({"a1": "hello2", "nested": ["key1", "key2"]})"),
            GetMessage(FIRST_OFFSET + 2, R"({"a1": "hello3", "nested": "some string"})"),
            GetMessage(FIRST_OFFSET + 3, R"({"a1": "hello4", "nested": 123456})")
        });
    }

    Y_UNIT_TEST_F(SimpleBooleans, TJsonParserFixture) {
        ExpectedBatches = 1;

        CheckSuccess(MakeParser({{"a", "[DataType; Bool]"}}, [&](ui64 numberRows, TVector<const TVector<NYql::NUdf::TUnboxedValue>*> result) {
            UNIT_ASSERT_VALUES_EQUAL(2, numberRows);

            UNIT_ASSERT_VALUES_EQUAL(1, result.size());
            UNIT_ASSERT_VALUES_EQUAL(true, result[0]->at(0).Get<bool>());
            UNIT_ASSERT_VALUES_EQUAL(false, result[0]->at(1).Get<bool>());
        }));

        Parser->ParseMessages({
            GetMessage(FIRST_OFFSET, R"({"a": true})"),
            GetMessage(FIRST_OFFSET + 1, R"({"a": false})")
        });
    }

    Y_UNIT_TEST_F(ManyBatches, TJsonParserFixture) {
        ExpectedBatches = 2;
        Config.BufferCellCount = 1;

        const TString largeString = "abcdefghjkl1234567890+abcdefghjkl1234567890";
        CheckSuccess(MakeParser({"col"}, "[DataType; String]", [&](ui64 numberRows, TVector<const TVector<NYql::NUdf::TUnboxedValue>*> result) {
            UNIT_ASSERT_VALUES_EQUAL(1, numberRows);
            UNIT_ASSERT_VALUES_EQUAL(1, result.size());
            UNIT_ASSERT_VALUES_EQUAL(largeString, TString(result[0]->at(0).AsStringRef()));
        }));

        const TString jsonString = TStringBuilder() << "{\"col\": \"" << largeString << "\"}";
        Parser->ParseMessages({
            GetMessage(FIRST_OFFSET, jsonString),
            GetMessage(FIRST_OFFSET + 1, jsonString)
        });
    }

    Y_UNIT_TEST_F(LittleBatches, TJsonParserFixture) {
        ExpectedBatches = 2;
        Config.BatchSize = 10;

        const TString largeString = "abcdefghjkl1234567890+abcdefghjkl1234567890";
        CheckSuccess(MakeParser({"col"}, "[DataType; String]", [&](ui64 numberRows, TVector<const TVector<NYql::NUdf::TUnboxedValue>*> result) {
            UNIT_ASSERT_VALUES_EQUAL(1, numberRows);
            UNIT_ASSERT_VALUES_EQUAL(1, result.size());
            UNIT_ASSERT_VALUES_EQUAL(largeString, TString(result[0]->at(0).AsStringRef()));
        }));

        const TString jsonString = TStringBuilder() << "{\"col\": \"" << largeString << "\"}";
        Parser->ParseMessages({
            GetMessage(FIRST_OFFSET, jsonString),
            GetMessage(FIRST_OFFSET + 1, jsonString)
        });
    }

    Y_UNIT_TEST_F(MissingFieldsValidation, TJsonParserFixture) {
        CheckSuccess(MakeParser({{"a1", "[DataType; String]"}, {"a2", "[DataType; Uint64]"}}));
        CheckColumnError(R"({"a2": 105, "event": "event1"})", 0, EStatusId::PRECONDITION_FAILED, TStringBuilder() << "Failed to parse json messages, found 1 missing values in non optional column 'a1' with type [DataType; String], buffered offsets: " << FIRST_OFFSET);
        CheckColumnError(R"({"a1": "hello1", "a2": null, "event": "event1"})", 1, EStatusId::PRECONDITION_FAILED, TStringBuilder() << "Failed to parse json string at offset " << FIRST_OFFSET + 1 << ", got parsing error for column 'a2' with type [DataType; Uint64] subissue: { <main>: Error: Found unexpected null value, expected non optional data type Uint64 }");
    }

    Y_UNIT_TEST_F(TypeKindsValidation, TJsonParserFixture) {
        CheckError(
            MakeParser({{"a1", "[[BAD TYPE]]"}}),
            EStatusId::INTERNAL_ERROR,
            "Failed to parse column 'a1' type [[BAD TYPE]] subissue: { <main>: Error: Failed to parse type from yson: Failed to parse scheme from YSON:"
        );
        CheckError(
            MakeParser({{"a2", "[OptionalType; [DataType; String]]"}, {"a1", "[ListType; [DataType; String]]"}}),
            EStatusId::UNSUPPORTED,
            "Failed to create parser for column 'a1' with type [ListType; [DataType; String]] subissue: { <main>: Error: Unsupported type kind: List }"
        );
    }

    Y_UNIT_TEST_F(NumbersValidation, TJsonParserFixture) {
        CheckSuccess(MakeParser({{"a1", "[OptionalType; [DataType; String]]"}, {"a2", "[DataType; Uint8]"}, {"a3", "[OptionalType; [DataType; Float]]"}}));
        CheckColumnError(R"({"a1": 456, "a2": 42})", 0, EStatusId::PRECONDITION_FAILED, TStringBuilder() << "Failed to parse json string at offset " << FIRST_OFFSET << ", got parsing error for column 'a1' with type [OptionalType; [DataType; String]] subissue: { <main>: Error: Failed to parse data type String from json number (raw: '456') subissue: { <main>: Error: Number value is not expected for data type String } }");
        CheckColumnError(R"({"a1": "456", "a2": -42})", 1, EStatusId::BAD_REQUEST, TStringBuilder() << "Failed to parse json string at offset " << FIRST_OFFSET + 1 << ", got parsing error for column 'a2' with type [DataType; Uint8] subissue: { <main>: Error: Failed to parse data type Uint8 from json number (raw: '-42') subissue: { <main>: Error: Failed to extract json integer number, error: INCORRECT_TYPE: The JSON element does not have the requested type. } }");
        CheckColumnError(R"({"a1": "str", "a2": 99999})", 1, EStatusId::BAD_REQUEST, TStringBuilder() << "Failed to parse json string at offset " << FIRST_OFFSET + 2 << ", got parsing error for column 'a2' with type [DataType; Uint8] subissue: { <main>: Error: Failed to parse data type Uint8 from json number (raw: '99999') subissue: { <main>: Error: Number is out of range [0, 255] } }");
        CheckColumnError(R"({"a1": "456", "a2": 42, "a3": 1.11.1})", 2, EStatusId::BAD_REQUEST, TStringBuilder() << "Failed to parse json string at offset " << FIRST_OFFSET + 3 << ", got parsing error for column 'a3' with type [OptionalType; [DataType; Float]] subissue: { <main>: Error: Failed to parse data type Float from json number (raw: '1.11.1') subissue: { <main>: Error: Failed to extract json float number, error: NUMBER_ERROR: Problem while parsing a number } }");
    }

    Y_UNIT_TEST_F(StringsValidation, TJsonParserFixture) {
        CheckSuccess(MakeParser({{"a1", "[OptionalType; [DataType; Uint8]]"}}));
        CheckColumnError(R"({"a1": "-456"})", 0, EStatusId::BAD_REQUEST, TStringBuilder() << "Failed to parse json string at offset " << FIRST_OFFSET << ", got parsing error for column 'a1' with type [OptionalType; [DataType; Uint8]] subissue: { <main>: Error: Failed to parse data type Uint8 from json string: '-456' }");
    }

    Y_UNIT_TEST_F(NestedJsonValidation, TJsonParserFixture) {
        CheckSuccess(MakeParser({{"a1", "[OptionalType; [DataType; Json]]"}, {"a2", "[OptionalType; [DataType; String]]"}}));
        CheckColumnError(R"({"a1": {"key": "value"}, "a2": {"key2": "value2"}})", 1, EStatusId::PRECONDITION_FAILED, TStringBuilder() << "Failed to parse json string at offset " << FIRST_OFFSET << ", got parsing error for column 'a2' with type [OptionalType; [DataType; String]] subissue: { <main>: Error: Found unexpected nested value (raw: '{\"key2\": \"value2\"}'), expected data type String, please use Json type for nested values }");
        CheckColumnError(R"({"a1": {"key": "value", "nested": {"a": "b", "c":}}, "a2": "str"})", 0, EStatusId::BAD_REQUEST, TStringBuilder() << "Failed to parse json string at offset " << FIRST_OFFSET + 1 << ", got parsing error for column 'a1' with type [OptionalType; [DataType; Json]] subissue: { <main>: Error: Found bad json value: '{\"key\": \"value\", \"nested\": {\"a\": \"b\", \"c\":}}' }");
        CheckColumnError(R"({"a1": {"key" "value"}, "a2": "str"})", 0, EStatusId::BAD_REQUEST, TStringBuilder() << "Failed to parse json string at offset " << FIRST_OFFSET + 2 << ", got parsing error for column 'a1' with type [OptionalType; [DataType; Json]] subissue: { <main>: Error: Failed to extract json value, current token: '{', error: TAPE_ERROR: The JSON document has an improper structure");
    }

    Y_UNIT_TEST_F(BoolsValidation, TJsonParserFixture) {
        CheckSuccess(MakeParser({{"a1", "[OptionalType; [DataType; String]]"}, {"a2", "[DataType; Bool]"}}));
        CheckColumnError(R"({"a1": true, "a2": false})", 0, EStatusId::PRECONDITION_FAILED, TStringBuilder() << "Failed to parse json string at offset " << FIRST_OFFSET << ", got parsing error for column 'a1' with type [OptionalType; [DataType; String]] subissue: { <main>: Error: Found unexpected bool value, expected data type String }");
        CheckColumnError(R"({"a1": "true", "a2": falce})", 1, EStatusId::BAD_REQUEST, TStringBuilder() << "Failed to parse json string at offset " << FIRST_OFFSET + 1 << ", got parsing error for column 'a2' with type [DataType; Bool] subissue: { <main>: Error: Failed to extract json bool, current token: 'falce', error: INCORRECT_TYPE: The JSON element does not have the requested type. }");
    }

    Y_UNIT_TEST_F(JsonStructureValidation, TJsonParserFixture) {
        CheckSuccess(MakeParser({{"a1", "[OptionalType; [DataType; String]]"}}));
        CheckColumnError(R"({"a1": Yelse})", 0, EStatusId::BAD_REQUEST, TStringBuilder() << "Failed to parse json string at offset " << FIRST_OFFSET << ", got parsing error for column 'a1' with type [OptionalType; [DataType; String]] subissue: { <main>: Error: Failed to determine json value type, current token: 'Yelse', error: TAPE_ERROR: The JSON document has an improper structure");
        CheckBatchError(R"({"a1": "st""r"})", EStatusId::BAD_REQUEST, TStringBuilder() << "Failed to parse json message for offset " << FIRST_OFFSET + 1 << ", json item was corrupted: TAPE_ERROR: The JSON document has an improper structure");
        CheckBatchError(R"({"a1": "x"} {"a1": "y"})", EStatusId::INTERNAL_ERROR, TStringBuilder() << "Failed to parse json messages, expected 1 json rows from offset " << FIRST_OFFSET + 2 << " but got 2 (expected one json row for each offset from topic API in json each row format, maybe initial data was corrupted or messages is not in json format), current data batch: {\"a1\": \"x\"} {\"a1\": \"y\"}");
        CheckBatchError(R"({)", EStatusId::INTERNAL_ERROR, TStringBuilder() << "Failed to parse json messages, expected 1 json rows from offset " << FIRST_OFFSET + 3 << " but got 0 (expected one json row for each offset from topic API in json each row format, maybe initial data was corrupted or messages is not in json format), current data batch: {");
    }
}

Y_UNIT_TEST_SUITE(TestRawParser) {
    Y_UNIT_TEST_F(Simple, TRawParserFixture) {
        CheckSuccess(MakeParser({{"data", "[OptionalType; [DataType; String]]"}}, [](ui64 numberRows, TVector<const TVector<NYql::NUdf::TUnboxedValue>*> result) {
            UNIT_ASSERT_VALUES_EQUAL(1, numberRows);
            UNIT_ASSERT_VALUES_EQUAL(1, result.size());

            NYql::NUdf::TUnboxedValue value = result[0]->at(0).GetOptionalValue();
            UNIT_ASSERT_VALUES_EQUAL(R"({"a1": "hello1__large_str", "a2": 101, "event": "event1"})", TString(value.AsStringRef()));
        }));
        PushToParser(FIRST_OFFSET, R"({"a1": "hello1__large_str", "a2": 101, "event": "event1"})");
    }

    Y_UNIT_TEST_F(ManyValues, TRawParserFixture) {
        TVector<TString> data = {
            R"({"a1": "hello1", "a2": "101", "event": "event1"})",
            R"({"a1": "hello1", "a2": "101", "event": "event2"})",
            R"({"a2": "101", "a1": "hello1", "event": "event3"})"
        };
        ExpectedBatches = data.size();

        int i = 0;
        CheckSuccess(MakeParser({"a1"}, "[DataType; String]", [&](ui64 numberRows, TVector<const TVector<NYql::NUdf::TUnboxedValue>*> result) {
            UNIT_ASSERT_VALUES_EQUAL(1, numberRows);
            UNIT_ASSERT_VALUES_EQUAL(1, result.size());
            UNIT_ASSERT_VALUES_EQUAL(data[i], TString(result[0]->at(0).AsStringRef()));
            i++;
        }));

        Parser->ParseMessages({
            GetMessage(FIRST_OFFSET, data[0]),
            GetMessage(FIRST_OFFSET + 1, data[1]),
            GetMessage(FIRST_OFFSET + 2, data[2])
        });
    }

    Y_UNIT_TEST_F(TypeKindsValidation, TRawParserFixture) {
        CheckError(
            MakeParser({{"a1", "[DataType; String]"}, {"a2", "[DataType; String]"}}),
            EStatusId::INTERNAL_ERROR,
            "Expected only one column for raw format, but got 2"
        );
        CheckError(
            MakeParser({{"a1", "[[BAD TYPE]]"}}),
            EStatusId::INTERNAL_ERROR,
            "Failed to create raw parser for column 'a1' : [[BAD TYPE]] subissue: { <main>: Error: Failed to parse type from yson: Failed to parse scheme from YSON:"
        );
        CheckError(
            MakeParser({{"a1", "[ListType; [DataType; String]]"}}),
            EStatusId::UNSUPPORTED,
            "Failed to create raw parser for column 'a1' : [ListType; [DataType; String]] subissue: { <main>: Error: Unsupported type kind for raw format: List }"
        );
    }
}

}  // namespace NFq::NRowDispatcher::NTests
