#include <ydb/core/kqp/event_log/column_shard_log_writer.h>
#include <ydb/core/kqp/event_log/log_column.h>

#include <ydb/core/kqp/ut/olap/combinatory/variator.h>
#include <ydb/core/kqp/ut/olap/helpers/get_value.h>
#include <ydb/core/kqp/ut/olap/helpers/local.h>
#include <ydb/core/kqp/ut/olap/helpers/query_executor.h>
#include <ydb/core/kqp/ut/olap/helpers/typed_local.h>
#include <ydb/core/kqp/ut/olap/helpers/writer.h>

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>
#include <ydb/core/protos/long_tx_service_config.pb.h>
#include <ydb/core/wrappers/fake_storage.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/public/lib/yson_value/ydb_yson_value.h>

namespace NKikimr::NKqp {

using namespace NKikimr::NKqp::NSchematizedLog;

class TBaseTestExampleLogWriter : public TColumnShardLogWriter {
public:
    unsigned WrittenCount{0};

    TBaseTestExampleLogWriter(TKikimrRunner& runner, NLog::EComponent component, TVector<std::shared_ptr<TSchematizedLogColumn>> columns,
            std::optional<ui32> maxBatchSize = {})
        : TColumnShardLogWriter(runner, [component](NActors::NStructuredLog::TLogMessage message) {
            return message.Component == component;
        }, TColumnShardLogWriter::TDatabaseSettings {
            .TableName = "olapTable",
            .StoreName = "olapStore",
            .MaxBatchSize = maxBatchSize
        }, columns)
    {
        Y_UNUSED(component);
    }

    bool Write(const NActors::NStructuredLog::TLogMessage& message) override {
        if (!TColumnShardLogWriter::Write(message)) {
            return false;
        }
        WrittenCount++;
        return true;
    }

    TString FormatLogColumnValueYson(const NYdb::TValue& value) {
        NYdb::TValueParser parser(value);
        const bool optional = parser.GetKind() == NYdb::TTypeParser::ETypeKind::Optional;
        if (optional) {
            parser.OpenOptional();
            if (parser.IsNull()) {
                return "#";
            }
        }

        if (parser.GetKind() == NYdb::TTypeParser::ETypeKind::Primitive
            && parser.GetPrimitiveType() == NYdb::EPrimitiveType::Timestamp)
        {
            const TString timestamp = parser.GetTimestamp().ToString();
            if (optional) {
                return TStringBuilder() << "[\"" << timestamp << "\"]";
            }
            return TStringBuilder() << "\"" << timestamp << "\"";
        }

        return NYdb::FormatValueYson(value);
    }

    using TQueryResult = std::vector<std::vector<std::string>>;
    TQueryResult FetchStreamData(NYdb::NTable::TScanQueryPartIterator& it) {
        TQueryResult rows;

        for (;;) {
            auto streamPart = it.ReadNext().GetValueSync();
            if (!streamPart.IsSuccess()) {
                UNIT_ASSERT_C(streamPart.EOS(), streamPart.GetIssues().ToString());
                break;
            }

            if (streamPart.HasResultSet()) {
                auto resultSet = streamPart.ExtractResultSet();
                auto columns = resultSet.GetColumnsMeta();
                NYdb::TResultSetParser parser(resultSet);
                while (parser.TryNextRow()) {
                    std::vector<std::string> row;
                    row.reserve(columns.size());
                    for (ui32 i = 0; i < columns.size(); ++i) {
                        const TString value = FormatLogColumnValueYson(parser.GetValue(i));
                        row.emplace_back(value.data(), value.size());
                    }
                    rows.push_back(std::move(row));
                }
            }
        }

        return rows;
    }

    TString GetFetchQuery() {
        TStringBuilder selectList;
        TStringBuilder orderBy;
        for (const auto& column : Columns) {
            if (!selectList.empty()) {
                selectList << ", ";
            }
            selectList << "`" << column->Name << "`";
            if (column->Settings.IsPK) {
                if (!orderBy.empty()) {
                    orderBy << ", ";
                }
                orderBy << "`" << column->Name << "`";
            }
        }

        TStringBuilder query;
        query << "--!syntax_v1\n";
        query << "\n";
        query << "SELECT " << selectList << " FROM `/Root/" << Settings.StoreName << "/" << Settings.TableName << "`";
        if (!orderBy.empty()) {
            query << " ORDER BY " << orderBy;
        }
        query << "\n";
        return query;
    }

    void Dump(const TQueryResult& result) {
        for (const auto& row : result) {
            for (size_t i = 0; i < row.size(); ++i) {
                if (i) {
                    Cerr << "; ";
                }
                Cerr << row[i];
            }
            Cerr << Endl;
        }
    }

    void CheckWrittenLogContent(const TQueryResult& requiredResult, unsigned existedRecordCount = 0) {
        // Wait log completely written
        for(unsigned i=0; WrittenCount + existedRecordCount < requiredResult.size() && i < 100; i++) {
            Sleep(TDuration::MilliSeconds(100));
        }
        UNIT_ASSERT(WrittenCount + existedRecordCount >= requiredResult.size()); // WrittenCount can be great in flush tests

        // Build query
        auto query = GetFetchQuery();

        // Execute query
        auto client = GetRunner().GetTableClient();
        auto it = client.StreamExecuteScanQuery(query).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        // Fetch result
        auto result = FetchStreamData(it);

        // Dump on error
        if (true /*result != requiredResult*/) {
            Cerr << " " << Endl;
            Cerr << "QUERY:" << Endl << query << Endl;

            Cerr << " " << Endl;
            Cerr << "RESULT:" << Endl;
            Dump(result);

            Cerr << " " << Endl;
            Cerr << "REQUIRED:" << Endl;
            Dump(requiredResult);
        }

        UNIT_ASSERT_EQUAL(result, requiredResult);
    }
};

class TEmitTestLog : public NActors::TActorBootstrapped<TEmitTestLog> {
public:
    using TLogWriteFunc = const std::function<void()>;

    TLogWriteFunc WriteFunc;
    TEmitTestLog(const TLogWriteFunc& writeFunc) : WriteFunc(writeFunc) {}

    void Bootstrap() {
        if (WriteFunc) {
            WriteFunc();
        }
        PassAway();
    }
};

struct TEnvironment {
    static constexpr int Component = NActorsServices::TEST;

    TKikimrRunner Kikimr;
    std::shared_ptr<TBaseTestExampleLogWriter> Writer;

    TEnvironment(const TVector<std::shared_ptr<TSchematizedLogColumn>>& columns, std::optional<ui32> maxBatchSize = {})
        : Kikimr(TKikimrSettings().SetWithSampleTables(false)) {
        Writer = std::make_shared<TBaseTestExampleLogWriter>(Kikimr, TEnvironment::Component, columns, maxBatchSize);
    }

    void RecreateWriter(const TVector<std::shared_ptr<TSchematizedLogColumn>>& columns, std::optional<ui32> maxBatchSize = {}) {
        Writer = std::make_shared<TBaseTestExampleLogWriter>(Kikimr, TEnvironment::Component, columns, maxBatchSize);
    }

    void WriteLog(const TEmitTestLog::TLogWriteFunc& writeFunc) {
        auto* runtime = Kikimr.GetTestServer().GetRuntime();
        for (ui32 i = 0; i < runtime->GetNodeCount(); ++i) {
            runtime->GetLogSettings(i)->FlushSinksTimeout = (Writer->GetDatabaseSettings().MaxBatchSize.has_value())?1000000:0;
            runtime->GetLogSettings(i)->Sinks = {Writer};
        }
        runtime->SetLogPriority(TEnvironment::Component, NActors::NLog::PRI_TRACE);

        runtime->Register(new TEmitTestLog(writeFunc));
    }
};

Y_UNIT_TEST_SUITE(KqpOlapWriteLog) {
    Y_UNIT_TEST(WriteSimple) {

        TEnvironment env({
            std::make_shared<TDBLogMessageIdColumn>(1),
            std::make_shared<TDBLogMessagePrioColumn>(),
            std::make_shared<TDBLogMessageTextColumn>(),
            std::make_shared<TDBLogMessageLocationColumn>(),
            std::make_shared<TDBLogMessageStringValueColumn>("string_value", std::vector<TKeyName>{"value"}),
            std::make_shared<TDBLogColumnUint64>("ui64_value", std::vector<TKeyName>{"value"})
        });
        env.WriteLog([](){
            YDB_LOG_INFO_COMP(TEnvironment::Component, "Test info message",
                {"value", 3});
            YDB_LOG_NOTICE_COMP(TEnvironment::Component, "Test notice message",
                {"value", 7});
            YDB_LOG_WARN_COMP(TEnvironment::Component, "Test warn message",
                {"value", "ace"});
            YDB_LOG_ERROR_COMP(TEnvironment::Component, "Test error message");
        });

        // Fetch and check data
        env.Writer->CheckWrittenLogContent({
            {"1u", "6u", R"("Test info message")",   R"("write_ut.cpp:235")", R"(["3"])",  "[3u]"},
            {"2u", "5u", R"("Test notice message")", R"("write_ut.cpp:237")", R"(["7"])",   "[7u]"},
            {"3u", "4u", R"("Test warn message")",   R"("write_ut.cpp:239")", R"(["ace"])", "#"},
            {"4u", "3u", R"("Test error message")",  R"("write_ut.cpp:240")", R"(#)",       "#"}});
    }

    Y_UNIT_TEST(WriteVaryValues) {

        TEnvironment env({
            std::make_shared<TDBLogMessageIdColumn>(1),
            std::make_shared<TDBLogColumnUint64>("value1", std::vector<TKeyName>{"value1"}),
            std::make_shared<TDBLogColumnUint64>("value2", std::vector<TKeyName>{"value2"}),
            std::make_shared<TDBLogColumnUint64>("value3", std::vector<TKeyName>{"value3"})});
        env.WriteLog([](){
            YDB_LOG_INFO_COMP(TEnvironment::Component, "Write 0 values");
            YDB_LOG_INFO_COMP(TEnvironment::Component, "Write 1 values",
                {"value1", 1});
            YDB_LOG_INFO_COMP(TEnvironment::Component, "Write 2 values",
                {"value1", 1},
                {"value2", 2});
            YDB_LOG_INFO_COMP(TEnvironment::Component, "Write 3 values",
                {"value1", 1},
                {"value2", 2},
                {"value3", 3});
        });

        // Fetch and check data
        env.Writer->CheckWrittenLogContent({
            {"1u", "#", "#", "#"},
            {"2u", "[1u]", "#", "#"},
            {"3u", "[1u]", "[2u]", "#"},
            {"4u", "[1u]", "[2u]", "[3u]"}});
    }

    Y_UNIT_TEST(WriteMessageTime) {

        TEnvironment env({
            std::make_shared<TDBLogMessageIdColumn>(1),
            std::make_shared<TDBLogMessageTimeColumn>()
        });

        // Write data
        NActors::NStructuredLog::TLogMessage message;
        message.Component = TEnvironment::Component;
        message.Time = TInstant::MicroSeconds(1789233327128336);
        env.Writer->Write(message);
        message.Time = TInstant::MicroSeconds(1789233327128337);
        env.Writer->Write(message);
        message.Time = TInstant::MicroSeconds(1789233327128338);
        env.Writer->Write(message);
        env.Writer->Flush();

        // Fetch and check data
        env.Writer->CheckWrittenLogContent({
            {"1u", R"("2026-09-12T17:15:27.128336Z")"},
            {"2u", R"("2026-09-12T17:15:27.128337Z")"},
            {"3u", R"("2026-09-12T17:15:27.128338Z")"}});
    }

    Y_UNIT_TEST(WriteMessageNodeId) {

        TEnvironment env({
            std::make_shared<TDBLogMessageIdColumn>(1),
            std::make_shared<TDBLogMessageNodeIdColumn>()
        });

        // Write data
        NActors::NStructuredLog::TLogMessage message;
        message.Component = TEnvironment::Component;
        message.NodeId = 1;
        env.Writer->Write(message);
        message.NodeId = 2;
        env.Writer->Write(message);
        message.NodeId = 3;
        env.Writer->Write(message);

        env.Writer->Flush();

        // Fetch and check data
        env.Writer->CheckWrittenLogContent({
            {"1u", "1u"},
            {"2u", "2u"},
            {"3u", "3u"}});
    }

    Y_UNIT_TEST(WriteMessageErrors) {

        TEnvironment env({
            std::make_shared<TDBLogMessageIdColumn>(1),
            std::make_shared<TDBLogColumnUint64>("value1", std::vector<TKeyName>{"value1"}, TSchematizedLogColumn::TDatabaseSettings::NotNull()),
            std::make_shared<TDBLogColumnUint64>("value2", std::vector<TKeyName>{"value2"}),
            std::make_shared<TDBLogMessageErrorColumn>()
        });
        env.WriteLog([](){
            YDB_LOG_INFO_COMP(TEnvironment::Component, "",
                {"value1", 1});
            YDB_LOG_INFO_COMP(TEnvironment::Component, "",
                {"value1", 1},
                {"value2", 2});

            // TWriteResultKind::DummyValueInsteadOfNull
            YDB_LOG_INFO_COMP(TEnvironment::Component, "",
                {"value2", 1});

            // TWriteResultKind::DummyValueInsteadOfCastError
            YDB_LOG_INFO_COMP(TEnvironment::Component, "",
                {"value1", "string value"});

            // TWriteResultKind::NullInsteadOfCastError
            YDB_LOG_INFO_COMP(TEnvironment::Component, "",
                {"value1", 1},
                {"value2", "string_value"});

            // Two errors
            YDB_LOG_INFO_COMP(TEnvironment::Component, "",
                {"value2", "string-value"});
        });

        // Fetch and check data
        env.Writer->CheckWrittenLogContent({
            {"1u", "1u", "#", "#"},
            {"2u", "1u", "[2u]", "#"},
            {"3u", "0u", "[1u]", R"(["Dummy \"value1\" instead of null"])"},
            {"4u", "0u", "#", R"(["Dummy \"value1\" instead of not casted value \"string value\""])"},
            {"5u", "1u", "#", R"(["Null \"value2\" instead of not casted value string_value"])"},
            {"6u", "0u", "#", R"(["Dummy \"value1\" instead of null; Null \"value2\" instead of not casted value string-value"])"}});
    }

    Y_UNIT_TEST(ManualFlush) {

        TEnvironment env({
            std::make_shared<TDBLogMessageIdColumn>(1),
            std::make_shared<TDBLogMessageNodeIdColumn>()
        });

        // Write data
        NActors::NStructuredLog::TLogMessage message;
        message.Component = TEnvironment::Component;

        // First chunk
        message.NodeId = 1;
        env.Writer->Write(message);
        message.NodeId = 2;
        env.Writer->Write(message);
        env.Writer->Flush();

        // Check
        env.Writer->CheckWrittenLogContent({
            {"1u", "1u"},
            {"2u", "2u"}});

        // Second chunk
        message.NodeId = 3;
        env.Writer->Write(message);
        message.NodeId = 4;
        env.Writer->Write(message);
        env.Writer->Flush();

        // Check
        env.Writer->CheckWrittenLogContent({
            {"1u", "1u"},
            {"2u", "2u"},
            {"3u", "3u"},
            {"4u", "4u"}});
    }

    Y_UNIT_TEST(AutoFlush) {

        TEnvironment env({
            std::make_shared<TDBLogMessageIdColumn>(1),
            std::make_shared<TDBLogMessageNodeIdColumn>()
        }, 2);

        // Write data
        NActors::NStructuredLog::TLogMessage message;
        message.Component = TEnvironment::Component;

        // Trigger first auto flush
        message.NodeId = 1;
        env.Writer->Write(message);
        message.NodeId = 2;
        env.Writer->Write(message);         // auto flush here
        message.NodeId = 3;
        env.Writer->Write(message);

        // Check
        env.Writer->CheckWrittenLogContent({
            {"1u", "1u"},
            {"2u", "2u"}});

        // Trigger second auto flush
        message.NodeId = 4;
        env.Writer->Write(message);         // auto flush here
        message.NodeId = 5;
        env.Writer->Write(message);

        // Check
        env.Writer->CheckWrittenLogContent({
            {"1u", "1u"},
            {"2u", "2u"},
            {"3u", "3u"},
            {"4u", "4u"}});

        // Manual flush and check
        env.Writer->Flush();
        env.Writer->CheckWrittenLogContent({
            {"1u", "1u"},
            {"2u", "2u"},
            {"3u", "3u"},
            {"4u", "4u"},
            {"5u", "5u"}});
    }
}

Y_UNIT_TEST_SUITE(KqpOlapWriteLogSchema) {

    Y_UNIT_TEST(ExistsTable) {
        TEnvironment env({
            std::make_shared<TDBLogMessageIdColumn>(1),
            std::make_shared<TDBLogMessageTextColumn>()
        });
        // First log chunk
        env.WriteLog([](){
            YDB_LOG_INFO_COMP(TEnvironment::Component, "Test message 1");
            YDB_LOG_INFO_COMP(TEnvironment::Component, "Test message 2");
            YDB_LOG_INFO_COMP(TEnvironment::Component, "Test message 3");
        });

        // Fetch and check data
        env.Writer->CheckWrittenLogContent({
            {"1u", R"("Test message 1")"},
            {"2u", R"("Test message 2")"},
            {"3u", R"("Test message 3")"}});

        // Recreate writer (emulate YDB restart)
        env.RecreateWriter({
            std::make_shared<TDBLogMessageIdColumn>(11),
            std::make_shared<TDBLogMessageTextColumn>()
        });
        // Second log chunk
        env.WriteLog([](){
            YDB_LOG_INFO_COMP(TEnvironment::Component, "Test message 11");
            YDB_LOG_INFO_COMP(TEnvironment::Component, "Test message 12");
            YDB_LOG_INFO_COMP(TEnvironment::Component, "Test message 13");
        });

        // Fetch and check data
        env.Writer->CheckWrittenLogContent({
            {"1u", R"("Test message 1")"},
            {"2u", R"("Test message 2")"},
            {"3u", R"("Test message 3")"},
            {"11u", R"("Test message 11")"},
            {"12u", R"("Test message 12")"},
            {"13u", R"("Test message 13")"}
        }, 3);
    }

    Y_UNIT_TEST(AddColumn) {
        // @todo
    }

    Y_UNIT_TEST(RemoveColumn) {
        // @todo
    }
}

template <typename T>
void AppendYdbValue(NYdb::TValueBuilder& builder, const T& value) {
    if constexpr (std::is_same_v<T, bool>) {
        builder.Bool(value);
    } else if constexpr (std::is_same_v<T, i8>) {
        builder.Int8(value);
    } else if constexpr (std::is_same_v<T, ui8>) {
        builder.Uint8(value);
    } else if constexpr (std::is_same_v<T, i16>) {
        builder.Int16(value);
    } else if constexpr (std::is_same_v<T, ui16>) {
        builder.Uint16(value);
    } else if constexpr (std::is_same_v<T, i32>) {
        builder.Int32(value);
    } else if constexpr (std::is_same_v<T, ui32>) {
        builder.Uint32(value);
    } else if constexpr (std::is_same_v<T, i64>) {
        builder.Int64(value);
    } else if constexpr (std::is_same_v<T, ui64>) {
        builder.Uint64(value);
    } else if constexpr (std::is_same_v<T, float>) {
        builder.Float(value);
    } else if constexpr (std::is_same_v<T, double>) {
        builder.Double(value);
    } else if constexpr (std::is_same_v<T, TString>) {
        builder.Utf8(std::string(value.data(), value.size()));
    } else if constexpr (std::is_same_v<T, TInstant>) {
        builder.Timestamp(value);
    } else {
        static_assert(!sizeof(T*), "Unsupported type for ValueToYson");
    }
}

std::string FormatYdbValueToYson(const NYdb::TValue& value) {
    const TString yson = NYdb::FormatValueYson(value);
    return std::string(yson.data(), yson.size());
}

template <typename T>
std::string ValueToYsonString(const T& value) {
    if constexpr (std::is_same_v<T, TInstant>) {
        const TString yson = TStringBuilder() << "\"" << value.ToString() << "\"";
        return std::string(yson.data(), yson.size());
    }
    NYdb::TValueBuilder builder;
    AppendYdbValue(builder, value);
    return FormatYdbValueToYson(builder.Build());
}

template <typename TValueType, typename TInvalidValueType>
void TestType(const TValueType& value, const std::optional<TInvalidValueType>& invalidValue = {}) {
    TEnvironment env({
        std::make_shared<TDBLogMessageIdColumn>(1),
        std::make_shared<TDBLogMessageStringValueColumn>("string_value", std::vector<TKeyName>{"value"}),
        std::make_shared<TDBLogMessageTypedValueColumn<TValueType>>("native_value", std::vector<TKeyName>{"value"})
    });
    env.WriteLog([&](){
        YDB_LOG_INFO_COMP(TEnvironment::Component, "Write valid value",
            {"value", value});
        YDB_LOG_INFO_COMP(TEnvironment::Component, "Write invalid value",
            {"value", invalidValue});
        YDB_LOG_INFO_COMP(TEnvironment::Component, "Write no value");
    });

    TStringBuilder stringValue;
    stringValue << TString(R"([")") << TTypesMapping::ToString(value) << TString(R"("])");

    TStringBuilder stringYsonValue;
    stringYsonValue << TString(R"([)") << ValueToYsonString(value) << TString(R"(])");

    TStringBuilder stringInvalidValue;
    if (invalidValue.has_value()) {
        stringInvalidValue << TString(R"([")") << TTypesMapping::ToString(invalidValue.value()) << TString(R"("])");
    } else {
        stringInvalidValue << "#";
    }

    env.Writer->CheckWrittenLogContent(
        {{"1u", stringValue, stringYsonValue},
         {"2u", stringInvalidValue, "#"},
         {"3u", "#", "#"}});
}

Y_UNIT_TEST_SUITE(KqpOlapWriteLogTypes) {

    /* Y_UNIT_TEST(Bool) {
        TestType<bool, TString>(true, TString("s"));
    } */
    Y_UNIT_TEST(Int8) {
        TestType<i8, TString>(i8(-8), TString("s"));
    }

    Y_UNIT_TEST(UInt8) {
        TestType<ui8, TString>(ui8(8), TString("s"));
    }

    Y_UNIT_TEST(Int16) {
        TestType<i16, TString>(i16(-16), TString("s"));
    }

    Y_UNIT_TEST(UInt16) {
        TestType<ui16, TString>(ui16(16), TString("s"));
    }

    Y_UNIT_TEST(Int32) {
        TestType<i32, TString>(-32, TString("s"));
    }

    Y_UNIT_TEST(UInt32) {
        TestType<ui32, TString>(32, TString("s"));
    }

    Y_UNIT_TEST(Int64) {
        TestType<i64, TString>(i64(-64), TString("s"));
    }

    Y_UNIT_TEST(UInt64) {
        TestType<ui64, TString>(1, TString("s"));
    }

    Y_UNIT_TEST(Float) {
        TestType<float, TString>(1.5f, TString("s"));
    }

    Y_UNIT_TEST(Double) {
        TestType<double, TString>(2.5, TString("s"));
    }

    Y_UNIT_TEST(String) {
        TestType<TString, ui64>(TString("s"), 1);
    }

    Y_UNIT_TEST(Instant) {
        TestType<TInstant, TString>(TInstant::Now(), TString("s"));
    }

}

}   // namespace NKikimr::NKqp
