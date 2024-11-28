#include <ydb/core/base/backtrace.h>

#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/core/fq/libs/events/events.h>

#include <ydb/core/fq/libs/row_dispatcher/json_parser.h>

#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/helpers.h>
#include <ydb/core/testlib/actor_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <contrib/libs/simdjson/include/simdjson.h>

namespace {

using namespace NKikimr;
using namespace NFq;

class TFixture : public NUnitTest::TBaseFixture {

public:
    TFixture()
    : Runtime(true) {}

    static void SegmentationFaultHandler(int) {
        Cerr << "segmentation fault call stack:" << Endl;
        FormatBackTrace(&Cerr);
        abort();
    }

    void SetUp(NUnitTest::TTestContext&) override {
        NKikimr::EnableYDBBacktraceFormat();
        signal(SIGSEGV, &SegmentationFaultHandler);

        TAutoPtr<TAppPrepare> app = new TAppPrepare();
        Runtime.SetLogBackend(CreateStderrBackend());
        Runtime.SetLogPriority(NKikimrServices::FQ_ROW_DISPATCHER, NLog::PRI_TRACE);
        Runtime.Initialize(app->Unwrap());
    }

    void TearDown(NUnitTest::TTestContext& /* context */) override {
        if (Parser) {
            Parser.reset();
        }
    }

    void MakeParser(TVector<TString> columns, TVector<TString> types, TJsonParser::TCallback callback, ui64 batchSize = 1_MB, ui64 bufferCellCount = 1000) {
        Parser = NFq::NewJsonParser(columns, types, callback, batchSize, TDuration::Hours(1), bufferCellCount);
    }

    void MakeParser(TVector<TString> columns, TJsonParser::TCallback callback) {
        MakeParser(columns, TVector<TString>(columns.size(), "[DataType; String]"), callback);
    }

    void MakeParser(TVector<TString> columns, TVector<TString> types) {
        MakeParser(columns, types, [](ui64, ui64, const TVector<TVector<NYql::NUdf::TUnboxedValue>>&) {});
    }

    void MakeParser(TVector<TString> columns) {
        MakeParser(columns, TVector<TString>(columns.size(), "[DataType; String]"));
    }

    void PushToParser(ui64 offset, const TString& data) {
        Parser->AddMessages({GetMessage(offset, data)});
        Parser->Parse();
    }

    static NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage GetMessage(ui64 offset, const TString& data) {
        NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessageInformation info(offset, "", 0, TInstant::Zero(), TInstant::Zero(), nullptr, nullptr, 0, "");
        return NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage(data, nullptr, info, nullptr);
    }

    TActorSystemStub actorSystemStub;
    NActors::TTestActorRuntime Runtime;
    std::unique_ptr<NFq::TJsonParser> Parser;
};

Y_UNIT_TEST_SUITE(TJsonParserTests) {
    Y_UNIT_TEST_F(Simple1, TFixture) {
        MakeParser({"a1", "a2"}, {"[DataType; String]", "[OptionalType; [DataType; Uint64]]"}, [](ui64 rowsOffset, ui64 numberRows, const TVector<TVector<NYql::NUdf::TUnboxedValue>>& result) {
            UNIT_ASSERT_VALUES_EQUAL(0, rowsOffset);
            UNIT_ASSERT_VALUES_EQUAL(1, numberRows);
            UNIT_ASSERT_VALUES_EQUAL(2, result.size());
            UNIT_ASSERT_VALUES_EQUAL("hello1", TString(result[0][0].AsStringRef()));
            UNIT_ASSERT_VALUES_EQUAL(101, result[1][0].GetOptionalValue().Get<ui64>());
        });
        PushToParser(42,R"({"a1": "hello1", "a2": 101, "event": "event1"})");
    }

    Y_UNIT_TEST_F(Simple2, TFixture) {
        MakeParser({"a2", "a1"}, [](ui64 rowsOffset, ui64 numberRows, const TVector<TVector<NYql::NUdf::TUnboxedValue>>& result) {
            UNIT_ASSERT_VALUES_EQUAL(0, rowsOffset);
            UNIT_ASSERT_VALUES_EQUAL(1, numberRows);
            UNIT_ASSERT_VALUES_EQUAL(2, result.size());
            UNIT_ASSERT_VALUES_EQUAL("101", TString(result[0][0].AsStringRef()));
            UNIT_ASSERT_VALUES_EQUAL("hello1", TString(result[1][0].AsStringRef()));
        });
        PushToParser(42,R"({"a1": "hello1", "a2": "101", "event": "event1"})");
    }

    Y_UNIT_TEST_F(Simple3, TFixture) {
        MakeParser({"a1", "a2"}, [](ui64 rowsOffset, ui64 numberRows, const TVector<TVector<NYql::NUdf::TUnboxedValue>>& result) {
            UNIT_ASSERT_VALUES_EQUAL(0, rowsOffset);
            UNIT_ASSERT_VALUES_EQUAL(1, numberRows);
            UNIT_ASSERT_VALUES_EQUAL(2, result.size());
            UNIT_ASSERT_VALUES_EQUAL("101", TString(result[0][0].AsStringRef()));
            UNIT_ASSERT_VALUES_EQUAL("hello1", TString(result[1][0].AsStringRef()));
        });
        PushToParser(42,R"({"a2": "hello1", "a1": "101", "event": "event1"})");
    }

    Y_UNIT_TEST_F(Simple4, TFixture) {
        MakeParser({"a2", "a1"}, [](ui64 rowsOffset, ui64 numberRows, const TVector<TVector<NYql::NUdf::TUnboxedValue>>& result) {
            UNIT_ASSERT_VALUES_EQUAL(0, rowsOffset);
            UNIT_ASSERT_VALUES_EQUAL(1, numberRows);
            UNIT_ASSERT_VALUES_EQUAL(2, result.size());
            UNIT_ASSERT_VALUES_EQUAL("hello1", TString(result[0][0].AsStringRef()));
            UNIT_ASSERT_VALUES_EQUAL("101", TString(result[1][0].AsStringRef()));
        });
        PushToParser(42, R"({"a2": "hello1", "a1": "101", "event": "event1"})");
    }

    Y_UNIT_TEST_F(LargeStrings, TFixture) {
        const TString largeString = "abcdefghjkl1234567890+abcdefghjkl1234567890";

        MakeParser({"col"}, [&](ui64 rowsOffset, ui64 numberRows, const TVector<TVector<NYql::NUdf::TUnboxedValue>>& result) {
            UNIT_ASSERT_VALUES_EQUAL(0, rowsOffset);
            UNIT_ASSERT_VALUES_EQUAL(2, numberRows);
            UNIT_ASSERT_VALUES_EQUAL(1, result.size());
            UNIT_ASSERT_VALUES_EQUAL(largeString, TString(result[0][0].AsStringRef()));
            UNIT_ASSERT_VALUES_EQUAL(largeString, TString(result[0][1].AsStringRef()));
        });

        const TString jsonString = TStringBuilder() << "{\"col\": \"" << largeString << "\"}";
        Parser->AddMessages({
            GetMessage(42, jsonString),
            GetMessage(43, jsonString)
        });
        Parser->Parse();
    }

    Y_UNIT_TEST_F(ManyValues, TFixture) {
        MakeParser({"a1", "a2"}, [&](ui64 rowsOffset, ui64 numberRows, const TVector<TVector<NYql::NUdf::TUnboxedValue>>& result) {
            UNIT_ASSERT_VALUES_EQUAL(0, rowsOffset);
            UNIT_ASSERT_VALUES_EQUAL(3, numberRows);
            UNIT_ASSERT_VALUES_EQUAL(2, result.size());
            for (size_t i = 0; i < numberRows; ++i) {
                UNIT_ASSERT_VALUES_EQUAL_C("hello1", TString(result[0][i].AsStringRef()), i);
                UNIT_ASSERT_VALUES_EQUAL_C("101", TString(result[1][i].AsStringRef()), i);
            }
        });

        Parser->AddMessages({
            GetMessage(42, R"({"a1": "hello1", "a2": "101", "event": "event1"})"),
            GetMessage(43, R"({"a1": "hello1", "a2": "101", "event": "event2"})"),
            GetMessage(44, R"({"a2": "101", "a1": "hello1", "event": "event3"})")
        });
        Parser->Parse();
    }

    Y_UNIT_TEST_F(MissingFields, TFixture) {
        MakeParser({"a1", "a2"}, {"[OptionalType; [DataType; String]]", "[OptionalType; [DataType; Uint64]]"}, [&](ui64 rowsOffset, ui64 numberRows, const TVector<TVector<NYql::NUdf::TUnboxedValue>>& result) {
            UNIT_ASSERT_VALUES_EQUAL(0, rowsOffset);
            UNIT_ASSERT_VALUES_EQUAL(3, numberRows);
            UNIT_ASSERT_VALUES_EQUAL(2, result.size());
            for (size_t i = 0; i < numberRows; ++i) {
                if (i == 2) {
                    UNIT_ASSERT_C(!result[0][i], i);
                } else {
                    NYql::NUdf::TUnboxedValue value = result[0][i].GetOptionalValue();
                    UNIT_ASSERT_VALUES_EQUAL_C("hello1", TString(value.AsStringRef()), i);
                }
                if (i == 1) {
                    UNIT_ASSERT_C(!result[1][i], i);
                } else {
                    UNIT_ASSERT_VALUES_EQUAL_C(101, result[1][i].GetOptionalValue().Get<ui64>(), i);
                }
            }
        });

        Parser->AddMessages({
            GetMessage(42, R"({"a1": "hello1", "a2": 101  , "event": "event1"})"),
            GetMessage(43, R"({"a1": "hello1", "event": "event2"})"),
            GetMessage(44, R"({"a2": "101", "a1": null, "event": "event3"})")
        });
        Parser->Parse();
    }

    Y_UNIT_TEST_F(NestedTypes, TFixture) {
        MakeParser({"nested", "a1"}, {"[OptionalType; [DataType; Json]]", "[DataType; String]"}, [&](ui64 rowsOffset, ui64 numberRows, const TVector<TVector<NYql::NUdf::TUnboxedValue>>& result) {
            UNIT_ASSERT_VALUES_EQUAL(0, rowsOffset);
            UNIT_ASSERT_VALUES_EQUAL(4, numberRows);

            UNIT_ASSERT_VALUES_EQUAL(2, result.size());
            UNIT_ASSERT_VALUES_EQUAL("{\"key\": \"value\"}", TString(result[0][0].AsStringRef()));
            UNIT_ASSERT_VALUES_EQUAL("hello1", TString(result[1][0].AsStringRef()));

            UNIT_ASSERT_VALUES_EQUAL("[\"key1\", \"key2\"]", TString(result[0][1].AsStringRef()));
            UNIT_ASSERT_VALUES_EQUAL("hello2", TString(result[1][1].AsStringRef()));

            UNIT_ASSERT_VALUES_EQUAL("\"some string\"", TString(result[0][2].AsStringRef()));
            UNIT_ASSERT_VALUES_EQUAL("hello3", TString(result[1][2].AsStringRef()));

            UNIT_ASSERT_VALUES_EQUAL("123456", TString(result[0][3].AsStringRef()));
            UNIT_ASSERT_VALUES_EQUAL("hello4", TString(result[1][3].AsStringRef()));
        });

        Parser->AddMessages({
            GetMessage(42, R"({"a1": "hello1", "nested": {"key": "value"}})"),
            GetMessage(43, R"({"a1": "hello2", "nested": ["key1", "key2"]})"),
            GetMessage(43, R"({"a1": "hello3", "nested": "some string"})"),
            GetMessage(43, R"({"a1": "hello4", "nested": 123456})")
        });
        Parser->Parse();
    }

    Y_UNIT_TEST_F(SimpleBooleans, TFixture) {
        MakeParser({"a"}, {"[DataType; Bool]"}, [&](ui64 rowsOffset, ui64 numberRows, const TVector<TVector<NYql::NUdf::TUnboxedValue>>& result) {
            UNIT_ASSERT_VALUES_EQUAL(0, rowsOffset);
            UNIT_ASSERT_VALUES_EQUAL(2, numberRows);

            UNIT_ASSERT_VALUES_EQUAL(1, result.size());
            UNIT_ASSERT_VALUES_EQUAL(true, result[0][0].Get<bool>());
            UNIT_ASSERT_VALUES_EQUAL(false, result[0][1].Get<bool>());
        });

        Parser->AddMessages({
            GetMessage(42, R"({"a": true})"),
            GetMessage(43, R"({"a": false})")
        });
        Parser->Parse();
    }

    Y_UNIT_TEST_F(ManyBatches, TFixture) {
        const TString largeString = "abcdefghjkl1234567890+abcdefghjkl1234567890";

        ui64 currentOffset = 0;
        MakeParser({"col"}, {"[DataType; String]"}, [&](ui64 rowsOffset, ui64 numberRows, const TVector<TVector<NYql::NUdf::TUnboxedValue>>& result) {
            UNIT_ASSERT_VALUES_EQUAL(currentOffset, rowsOffset);
            currentOffset++;

            UNIT_ASSERT_VALUES_EQUAL(1, numberRows);
            UNIT_ASSERT_VALUES_EQUAL(1, result.size());
            UNIT_ASSERT_VALUES_EQUAL(largeString, TString(result[0][0].AsStringRef()));
        }, 1_MB, 1);

        const TString jsonString = TStringBuilder() << "{\"col\": \"" << largeString << "\"}";
        Parser->AddMessages({
            GetMessage(42, jsonString),
            GetMessage(43, jsonString)
        });
        Parser->Parse();
    }

    Y_UNIT_TEST_F(LittleBatches, TFixture) {
        const TString largeString = "abcdefghjkl1234567890+abcdefghjkl1234567890";

        ui64 currentOffset = 42;
        MakeParser({"col"}, {"[DataType; String]"}, [&](ui64 rowsOffset, ui64 numberRows, const TVector<TVector<NYql::NUdf::TUnboxedValue>>& result) {
            UNIT_ASSERT_VALUES_EQUAL(Parser->GetOffsets().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(Parser->GetOffsets().front(), currentOffset);
            currentOffset++;

            UNIT_ASSERT_VALUES_EQUAL(0, rowsOffset);
            UNIT_ASSERT_VALUES_EQUAL(1, numberRows);
            UNIT_ASSERT_VALUES_EQUAL(1, result.size());
            UNIT_ASSERT_VALUES_EQUAL(largeString, TString(result[0][0].AsStringRef()));
        }, 10);

        const TString jsonString = TStringBuilder() << "{\"col\": \"" << largeString << "\"}";
        Parser->AddMessages({
            GetMessage(42, jsonString),
            GetMessage(43, jsonString)
        });
        UNIT_ASSERT_VALUES_EQUAL(Parser->GetNumberValues(), 0);
    }

    Y_UNIT_TEST_F(MissingFieldsValidation, TFixture) {
        MakeParser({"a1", "a2"}, {"[DataType; String]", "[DataType; Uint64]"});
        UNIT_ASSERT_EXCEPTION_CONTAINS(PushToParser(42, R"({"a1": "hello1", "a2": null, "event": "event1"})"), TJsonParserError, "Failed to parse json string at offset 42, got parsing error for column 'a2' with type [DataType; Uint64], description: (NFq::TJsonParserError) found unexpected null value, expected non optional data type Uint64");
        UNIT_ASSERT_EXCEPTION_CONTAINS(PushToParser(42, R"({"a2": 105, "event": "event1"})"), TJsonParserError, "Failed to parse json messages, found 1 missing values from offset 42 in non optional column 'a1' with type [DataType; String]");
    }

    Y_UNIT_TEST_F(TypeKindsValidation, TFixture) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            MakeParser({"a2", "a1"}, {"[OptionalType; [DataType; String]]", "[ListType; [DataType; String]]"}),
            NFq::TJsonParserError,
            "Failed to create parser for column 'a1' with type [ListType; [DataType; String]], description: (NFq::TJsonParserError) unsupported type kind List"
        );
    }

    Y_UNIT_TEST_F(NumbersValidation, TFixture) {
        MakeParser({"a1", "a2"}, {"[OptionalType; [DataType; String]]", "[DataType; Uint8]"});
        UNIT_ASSERT_EXCEPTION_CONTAINS(PushToParser(42, R"({"a1": 456, "a2": 42})"), NFq::TJsonParserError, "Failed to parse json string at offset 42, got parsing error for column 'a1' with type [OptionalType; [DataType; String]], description: (NFq::TJsonParserError) failed to parse data type String from json number (raw: '456'), error: (NFq::TJsonParserError) number value is not expected for data type String");
        UNIT_ASSERT_EXCEPTION_CONTAINS(PushToParser(42, R"({"a1": "456", "a2": -42})"), NFq::TJsonParserError, "Failed to parse json string at offset 42, got parsing error for column 'a2' with type [DataType; Uint8], description: (NFq::TJsonParserError) failed to parse data type Uint8 from json number (raw: '-42'), error: (simdjson::simdjson_error) INCORRECT_TYPE: The JSON element does not have the requested type.");
        UNIT_ASSERT_EXCEPTION_CONTAINS(PushToParser(42, R"({"a1": "str", "a2": 99999})"), NFq::TJsonParserError, "Failed to parse json string at offset 42, got parsing error for column 'a2' with type [DataType; Uint8], description: (NFq::TJsonParserError) failed to parse data type Uint8 from json number (raw: '99999'), error: (NFq::TJsonParserError) number is out of range");
    }

    Y_UNIT_TEST_F(NestedJsonValidation, TFixture) {
        MakeParser({"a1", "a2"}, {"[OptionalType; [DataType; Json]]", "[OptionalType; [DataType; String]]"});
        UNIT_ASSERT_EXCEPTION_CONTAINS(PushToParser(42, R"({"a1": {"key": "value"}, "a2": {"key2": "value2"}})"), NFq::TJsonParserError, "Failed to parse json string at offset 42, got parsing error for column 'a2' with type [OptionalType; [DataType; String]], description: (NFq::TJsonParserError) found unexpected nested value (raw: '{\"key2\": \"value2\"}'), expected data type String, please use Json type for nested values");
        UNIT_ASSERT_EXCEPTION_CONTAINS(PushToParser(42, R"({"a1": {"key" "value"}, "a2": "str"})"), NFq::TJsonParserError, "Failed to parse json string at offset 42, got parsing error for column 'a1' with type [OptionalType; [DataType; Json]], description: (simdjson::simdjson_error) TAPE_ERROR: The JSON document has an improper structure: missing or superfluous commas, braces, missing keys, etc.");
    }

    Y_UNIT_TEST_F(BoolsValidation, TFixture) {
        MakeParser({"a1", "a2"}, {"[OptionalType; [DataType; String]]", "[DataType; Bool]"});
        UNIT_ASSERT_EXCEPTION_CONTAINS(PushToParser(42, R"({"a1": true, "a2": false})"), NFq::TJsonParserError, "Failed to parse json string at offset 42, got parsing error for column 'a1' with type [OptionalType; [DataType; String]], description: (NFq::TJsonParserError) found unexpected bool value, expected data type String");
    }

    Y_UNIT_TEST_F(ThrowExceptionByError, TFixture) {
        MakeParser({"a"});
        UNIT_ASSERT_EXCEPTION_CONTAINS(PushToParser(42, R"(ydb)"), simdjson::simdjson_error, "INCORRECT_TYPE: The JSON element does not have the requested type.");
        UNIT_ASSERT_EXCEPTION_CONTAINS(PushToParser(42, R"({"a": "value1"} {"a": "value2"})"), NFq::TJsonParserError, "Failed to parse json messages, expected 1 json rows from offset 42 but got 2");
    }
}

}
