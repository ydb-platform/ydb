#include <ydb/core/kqp/event_log/audit_event_log_writer.h>
#include <ydb/core/kqp/event_log/column_shard_log_writer.h>
#include <ydb/core/kqp/event_log/kqp_event_log_writer.h>
#include <ydb/core/kqp/event_log/log_column.h>
#include <ydb/core/kqp/event_log/tli_event_log_writer.h>

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

using namespace NKikimr::NKqp::NEventLog;

namespace {

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
    std::optional<TQueryResult> FetchStreamData(NYdb::NTable::TScanQueryPartIterator& it, bool unitAssert) {
        if (unitAssert) {
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        }
        if (!it.IsSuccess()) {
            return {};
        }

        auto streamPart = it.ReadNext().GetValueSync();
        if (!streamPart.IsSuccess()) {
            if (unitAssert) {
                UNIT_ASSERT_C(streamPart.EOS(), streamPart.GetIssues().ToString());
            }
            if (!streamPart.EOS()) {
                return {};
            }
        }

        TQueryResult rows;
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
        return rows;
    }

    std::optional<TQueryResult> ExecuteQueryAndFetchData(TKikimrRunner& kikimr, const TString& query) {
        for(unsigned i = 10;i > 0;i--) {
            auto client = kikimr.GetTableClient();
            auto it = client.StreamExecuteScanQuery(query).GetValueSync();
            if (!it.IsSuccess()) {
                Sleep(TDuration::Seconds(1));
                continue;
            }
            auto result = FetchStreamData(it, i == 1);
            if (!result.has_value()) {
                Sleep(TDuration::Seconds(1));
                continue;
            }
            return result.value();
        }
        return {};
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

    template <typename C>
    bool WaitCondition(const C& cond) {
        for(unsigned i=0; i < 100; i++) {
            if (cond()) {
                return true;
            }
            Sleep(TDuration::MilliSeconds(100));
        }
        return false;
    }
}

class TBaseTestExampleLogWriter : public TColumnShardLogWriter {
public:
    TKikimrRunner& Runner;
    NLog::EComponent Component;
    unsigned WrittenCount{0};

    TBaseTestExampleLogWriter(TKikimrRunner& runner, NLog::EComponent component, TVector<std::shared_ptr<TEventLogColumn>> columns,
            std::optional<ui32> maxBatchSize = {})
        : TColumnShardLogWriter(TColumnShardLogWriter::TDatabaseSettings {
            .Path = "/Root",
            .StoreName = "olapStore",
            .TableName = "olapTable",
            .MaxBatchSize = maxBatchSize
        }, columns),
        Runner(runner),
        Component(component)
    {
    }

    bool Filter(const NActors::NStructuredLog::TLogMessage& message) override {
        return Component == message.Component;
    }

    bool Write(const NActors::NStructuredLog::TLogMessage& message) override {
        if (!TColumnShardLogWriter::Write(message)) {
            return false;
        }
        WrittenCount++;
        return true;
    }

    void Stop() override {
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
        query << "SELECT " << selectList << " FROM `" << Settings.Path << "/" << Settings.StoreName << "/" << Settings.TableName << "`";
        if (!orderBy.empty()) {
            query << " ORDER BY " << orderBy;
        }
        query << "\n";
        return query;
    }

    void CheckWrittenLogContent(const TQueryResult& requiredResult, unsigned existedRecordCount = 0) {
        WaitCondition([&](){
            return WrittenCount + existedRecordCount >= requiredResult.size();
        });

        // Build query
        auto query = GetFetchQuery();

        // Execute query
        auto result = ExecuteQueryAndFetchData(Runner, query);

        // Dump on error
        if (true /*result != requiredResult*/) {
            Cerr << " " << Endl;
            Cerr << "QUERY:" << Endl << query << Endl;

            Cerr << " " << Endl;
            Cerr << "RESULT:" << Endl;
            Dump(result.value());

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
    std::vector<NStructuredLog::ILogSinkSPtr> AddSinks;

    TEnvironment(const TVector<std::shared_ptr<TEventLogColumn>>& columns, std::optional<ui32> maxBatchSize = 0)
        : Kikimr(TKikimrSettings().SetWithSampleTables(false)) {
        Writer = std::make_shared<TBaseTestExampleLogWriter>(Kikimr, TEnvironment::Component, columns, maxBatchSize);
    }

    void RecreateWriter(const TVector<std::shared_ptr<TEventLogColumn>>& columns, std::optional<ui32> maxBatchSize = 0) {
        Writer = std::make_shared<TBaseTestExampleLogWriter>(Kikimr, TEnvironment::Component, columns, maxBatchSize);
    }

    void UpdateSinks() {
        auto* runtime = Kikimr.GetTestServer().GetRuntime();
        for (ui32 i = 0; i < runtime->GetNodeCount(); ++i) {
            auto settings = runtime->GetLogSettings(i);
            settings->DefPriority = NActors::NLog::PRI_TRACE;
            auto sinks = std::make_shared<NLog::TSettings::TLogSinkMap>();
            (*sinks)[""] = Writer;

            ui32 j = 0;
            for (auto& sink: AddSinks) {
                TStringBuilder key;
                key << j++;
                (*sinks)[key] = sink;
            }
            settings->Sinks = sinks;
        }
        runtime->SetLogPriority(TEnvironment::Component, NActors::NLog::PRI_TRACE);
    }

    void WriteLog(const TEmitTestLog::TLogWriteFunc& writeFunc) {
        UpdateSinks();

        auto* runtime = Kikimr.GetTestServer().GetRuntime();
        runtime->Register(new TEmitTestLog(writeFunc));
    }

    void ExecuteQuery(const TString& query) {
        auto kikimrPtr = &Kikimr;
        WriteLog([query, kikimrPtr](){
            auto client = kikimrPtr->GetTableClient();
            auto it = client.StreamExecuteScanQuery(query).GetValueSync();
        });
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
        env.WriteLog([&](){
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
            {"1u", "6u", R"("Test info message")",   R"("write_ut.cpp:304")", R"(["3"])",  "[3u]"},
            {"2u", "5u", R"("Test notice message")", R"("write_ut.cpp:306")", R"(["7"])",   "[7u]"},
            {"3u", "4u", R"("Test warn message")",   R"("write_ut.cpp:308")", R"(["ace"])", "#"},
            {"4u", "3u", R"("Test error message")",  R"("write_ut.cpp:309")", R"(#)",       "#"}});
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
        env.WriteLog([&](){
            NActors::NStructuredLog::TLogMessage message;
            message.Component = TEnvironment::Component;
            message.Time = TInstant::MicroSeconds(1789233327128336);
            env.Writer->Write(message);
            message.Time = TInstant::MicroSeconds(1789233327128337);
            env.Writer->Write(message);
            message.Time = TInstant::MicroSeconds(1789233327128338);
            env.Writer->Write(message);
        });

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
        env.WriteLog([&](){
            NActors::NStructuredLog::TLogMessage message;
            message.Component = TEnvironment::Component;
            message.NodeId = 1;
            env.Writer->Write(message);
            message.NodeId = 2;
            env.Writer->Write(message);
            message.NodeId = 3;
            env.Writer->Write(message);
        });

        // Fetch and check data
        env.Writer->CheckWrittenLogContent({
            {"1u", "1u"},
            {"2u", "2u"},
            {"3u", "3u"}});
    }

    Y_UNIT_TEST(WriteMessageErrors) {

        TEnvironment env({
            std::make_shared<TDBLogMessageIdColumn>(1),
            std::make_shared<TDBLogColumnUint64>("value1", std::vector<TKeyName>{"value1"}, TEventLogColumn::TDatabaseSettings::NotNull()),
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
        }, 100 /* Disable autoflush by size */);

        // Write single message to create table
        env.WriteLog([&](){
            NActors::NStructuredLog::TLogMessage message;
            message.Component = TEnvironment::Component;

            // First chunk
            message.NodeId = 1;
            env.Writer->Write(message);
            env.Writer->Flush();
        });
        // Wait flush complete
        UNIT_ASSERT(
            WaitCondition([&](){
                return env.Writer->GetCurrentBatchSize() == 0;
            }));
        // Check content
        env.Writer->CheckWrittenLogContent({
            {"1u", "1u"}});

        // Write data
        env.WriteLog([&](){
            NActors::NStructuredLog::TLogMessage message;
            message.Component = TEnvironment::Component;

            // First chunk
            message.NodeId = 2;
            env.Writer->Write(message);
            message.NodeId = 3;
            env.Writer->Write(message);
        });

        // Check
        UNIT_ASSERT(
            WaitCondition([&](){
                return env.Writer->GetCurrentBatchSize() == 2;
            }));
        env.Writer->CheckWrittenLogContent({
            {"1u", "1u"}});

        // Flush
        env.WriteLog([&](){
            env.Writer->Flush();
        });

        // Check
        UNIT_ASSERT(
            WaitCondition([&](){
                return env.Writer->GetCurrentBatchSize() == 0;
            }));
        env.Writer->CheckWrittenLogContent({
            {"1u", "1u"},
            {"2u", "2u"},
            {"3u", "3u"}});
    }

    Y_UNIT_TEST(AutoFlushBySize) {

        TEnvironment env({
            std::make_shared<TDBLogMessageIdColumn>(1),
            std::make_shared<TDBLogMessageNodeIdColumn>()
        }, 2);

        // Write single message to create table
        env.WriteLog([&](){
            NActors::NStructuredLog::TLogMessage message;
            message.Component = TEnvironment::Component;

            // First chunk
            message.NodeId = 1;
            env.Writer->Write(message);
            env.Writer->Flush();
        });
        // Wait flush complete
        UNIT_ASSERT(
            WaitCondition([&](){
                return env.Writer->GetCurrentBatchSize() == 0;
            }));
        // Check content
        env.Writer->CheckWrittenLogContent({
            {"1u", "1u"}});

        // Write data (3 messages)
        env.WriteLog([&](){
            NActors::NStructuredLog::TLogMessage message;
            message.Component = TEnvironment::Component;

            // First chunk
            message.NodeId = 2;
            env.Writer->Write(message);
            message.NodeId = 3;
            env.Writer->Write(message);
            message.NodeId = 4;
            env.Writer->Write(message);
        });

        // Check (saved 2 messages only)
        UNIT_ASSERT(
            WaitCondition([&](){
                return env.Writer->GetCurrentBatchSize() == 1;
            }));
        env.Writer->CheckWrittenLogContent({
            {"1u", "1u"},
            {"2u", "2u"},
            {"3u", "3u"}});

        // Write data
        env.WriteLog([&](){
            NActors::NStructuredLog::TLogMessage message;
            message.Component = TEnvironment::Component;

            message.NodeId = 5;
            env.Writer->Write(message);
            message.NodeId = 6;
        });

        // Check flushed
        UNIT_ASSERT(
            WaitCondition([&](){
                return env.Writer->GetCurrentBatchSize() == 1;
            }));
        env.Writer->CheckWrittenLogContent({
            {"1u", "1u"},
            {"2u", "2u"},
            {"3u", "3u"},
            {"4u", "4u"},
            {"5u", "5u"}});
    }

    /* Y_UNIT_TEST(AutoFlush) {

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
    } */

    Y_UNIT_TEST(KqpRequestLog) {
        TEnvironment env({
            std::make_shared<TDBLogMessageIdColumn>(1)
        });

        TKqpEventLogWriter::TDatabaseSettings settings;
        env.AddSinks.push_back(std::make_shared<TKqpEventLogWriter>(
            TKqpEventLogWriter::TDatabaseSettings{
                .Path = "/Root",
                .StoreName = "kqp_requests",
                .TableName = "kqp_requests",
                .MaxBatchSize = 0}));

        // Query with error - must be in log
        TString query = "SELECT A B C D E";
        env.ExecuteQuery(query);

        // Wait
        Sleep(TDuration::Seconds(5));

        // Select from system table
        Cerr << "DEBUG: Dump" << Endl;
        TStringBuilder selectQuery;
        selectQuery << "SELECT database, request, action, status FROM `/Root/kqp_requests/kqp_requests` WHERE request='" << query << "'";

        auto result = ExecuteQueryAndFetchData(env.Kikimr, selectQuery);
        Cerr << "KQP_RESULT:" << Endl;
        Dump(result.value());

        UNIT_ASSERT(result.has_value());
        UNIT_ASSERT(result.value().size() > 0);
        UNIT_ASSERT(result.value()[0].size() == 4);
        UNIT_ASSERT_EQUAL(result.value()[0][0], R"(["/Root"])");
        UNIT_ASSERT_EQUAL(result.value()[0][1], R"("SELECT A B C D E")");
        UNIT_ASSERT_EQUAL(result.value()[0][2], R"(["QUERY_ACTION_EXECUTE"])");
        UNIT_ASSERT_EQUAL(result.value()[0][3], R"(["CANCELLED"])");
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
