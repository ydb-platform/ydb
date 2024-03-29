#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/ut/common/columnshard.h>

#include <ydb/core/sys_view/service/query_history.h>
#include <ydb/core/tx/columnshard/columnshard_ut_common.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/writer.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/core/formats/arrow/simple_builder/filler.h>
#include <ydb/core/formats/arrow/simple_builder/array.h>
#include <ydb/core/formats/arrow/simple_builder/batch.h>
#include <ydb/core/formats/arrow/ssa_runtime_version.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/tx/columnshard/data_sharing/destination/events/control.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/datashard/datashard_ut_common_kqp.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/testlib/cs_helper.h>
#include <ydb/core/wrappers/fake_storage.h>
#include <util/system/sanitizers.h>

#include <fmt/format.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type_traits.h>

namespace NKikimr {
namespace NKqp {

using namespace NKikimr::NDataShard::NKqpHelpers;
using namespace NSchemeShard;
using namespace NActors;
using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NScheme;

using TEvBulkUpsertRequest = NGRpcService::TGrpcRequestOperationCall<Ydb::Table::BulkUpsertRequest,
    Ydb::Table::BulkUpsertResponse>;

Y_UNIT_TEST_SUITE(KqpOlap) {
    void PrintValue(IOutputStream& out, const NYdb::TValue& v) {
        NYdb::TValueParser value(v);

        while (value.GetKind() == NYdb::TTypeParser::ETypeKind::Optional) {
            if (value.IsNull()) {
                out << "<NULL>";
                return;
            } else {
                value.OpenOptional();
            }
        }

        if (value.IsNull()) {
            out << "<NULL>";
            return;
        }

        switch (value.GetPrimitiveType()) {
            case NYdb::EPrimitiveType::Uint32:
            {
                out << value.GetUint32();
                break;
            }
            case NYdb::EPrimitiveType::Uint64:
            {
                out << value.GetUint64();
                break;
            }
            case NYdb::EPrimitiveType::Int64:
            {
                out << value.GetInt64();
                break;
            }
            case NYdb::EPrimitiveType::Utf8:
            {
                out << value.GetUtf8();
                break;
            }
            case NYdb::EPrimitiveType::Timestamp:
            {
                out << value.GetTimestamp();
                break;
            }
            case NYdb::EPrimitiveType::Bool:
            {
                out << value.GetBool();
                break;
            }
            default:
            {
                UNIT_ASSERT_C(false, "PrintValue not iplemented for this type");
            }
        }
    }

    void PrintRow(IOutputStream& out, const THashMap<TString, NYdb::TValue>& fields) {
        for (const auto& f : fields) {
            out << f.first << ": ";
            PrintValue(out, f.second);
            out << " ";
        }
    }

    void PrintRows(IOutputStream& out, const TVector<THashMap<TString, NYdb::TValue>>& rows) {
        for (const auto& r : rows) {
            PrintRow(out, r);
            out << "\n";
        }
    }

    TVector<THashMap<TString, NYdb::TValue>> CollectRows(NYdb::NTable::TScanQueryPartIterator& it, NJson::TJsonValue* statInfo = nullptr, NJson::TJsonValue* diagnostics = nullptr) {
        TVector<THashMap<TString, NYdb::TValue>> rows;
        if (statInfo) {
            *statInfo = NJson::JSON_NULL;
        }
        if (diagnostics) {
            *diagnostics = NJson::JSON_NULL;
        }
        for (;;) {
            auto streamPart = it.ReadNext().GetValueSync();
            if (!streamPart.IsSuccess()) {
                UNIT_ASSERT_C(streamPart.EOS(), streamPart.GetIssues().ToString());
                break;
            }

            UNIT_ASSERT_C(streamPart.HasResultSet() || streamPart.HasQueryStats(),
                "Unexpected empty scan query response.");

            if (streamPart.HasQueryStats()) {
                auto plan = streamPart.GetQueryStats().GetPlan();
                if (plan && statInfo) {
                    UNIT_ASSERT(NJson::ReadJsonFastTree(*plan, statInfo));
                }
            }

            if (streamPart.HasDiagnostics()) {
                TString diagnosticsString = streamPart.GetDiagnostics();
                if (!diagnosticsString.empty() && diagnostics) {
                    UNIT_ASSERT(NJson::ReadJsonFastTree(diagnosticsString, diagnostics));
                }
            }

            if (streamPart.HasResultSet()) {
                auto resultSet = streamPart.ExtractResultSet();
                NYdb::TResultSetParser rsParser(resultSet);
                while (rsParser.TryNextRow()) {
                    THashMap<TString, NYdb::TValue> row;
                    for (size_t ci = 0; ci < resultSet.ColumnsCount(); ++ci) {
                        row.emplace(resultSet.GetColumnsMeta()[ci].Name, rsParser.GetValue(ci));
                    }
                    rows.emplace_back(std::move(row));
                }
            }
        }
        return rows;
    }

    TVector<THashMap<TString, NYdb::TValue>> ExecuteScanQuery(NYdb::NTable::TTableClient& tableClient, const TString& query, const bool verbose = true) {
        if (verbose) {
            Cerr << "====================================\n"
                << "QUERY:\n" << query
                << "\n\nRESULT:\n";
        }

        TStreamExecScanQuerySettings scanSettings;
        auto it = tableClient.StreamExecuteScanQuery(query, scanSettings).GetValueSync();
        auto rows = CollectRows(it);
        if (verbose) {
            PrintRows(Cerr, rows);
            Cerr << "\n";
        }

        return rows;
    }

    ui64 GetUint32(const NYdb::TValue& v) {
        NYdb::TValueParser value(v);
        if (value.GetKind() == NYdb::TTypeParser::ETypeKind::Optional) {
            return *value.GetOptionalUint32();
        } else {
            return value.GetUint32();
        }
    }

    ui64 GetUint64(const NYdb::TValue& v) {
        NYdb::TValueParser value(v);
        if (value.GetKind() == NYdb::TTypeParser::ETypeKind::Optional) {
            return *value.GetOptionalUint64();
        } else {
            return value.GetUint64();
        }
    }

    TString GetUtf8(const NYdb::TValue& v) {
        NYdb::TValueParser value(v);
        if (value.GetKind() == NYdb::TTypeParser::ETypeKind::Optional) {
            return *value.GetOptionalUtf8();
        } else {
            return value.GetUtf8();
        }
    }

    TInstant GetTimestamp(const NYdb::TValue& v) {
        NYdb::TValueParser value(v);
        if (value.GetKind() == NYdb::TTypeParser::ETypeKind::Optional) {
            return *value.GetOptionalTimestamp();
        } else {
            return value.GetTimestamp();
        }
    }

    class TTypedLocalHelper: public Tests::NCS::THelper {
    private:
        using TBase = Tests::NCS::THelper;
        const TString TypeName;
        TKikimrRunner& KikimrRunner;
        const TString TablePath;
        const TString TableName;
        const TString StoreName;
    protected:
        virtual TString GetTestTableSchema() const override {
            TString result;
            if (TypeName) {
                result = R"(Columns { Name: "field" Type: ")" + TypeName + "\"}";
            }
            result += R"(
                Columns { Name: "pk_int" Type: "Int64" NotNull: true }
                Columns { Name: "ts" Type: "Timestamp" }
                KeyColumnNames: "pk_int"
                Engine: COLUMN_ENGINE_REPLACING_TIMESERIES
            )";
            return result;
        }
        virtual std::vector<TString> GetShardingColumns() const override {
            return { "pk_int" };
        }
    public:
        TTypedLocalHelper(const TString& typeName, TKikimrRunner& kikimrRunner, const TString& tableName = "olapTable", const TString& storeName = "olapStore")
            : TBase(kikimrRunner.GetTestServer())
            , TypeName(typeName)
            , KikimrRunner(kikimrRunner)
            , TablePath("/Root/" + storeName + "/" + tableName)
            , TableName(tableName)
            , StoreName(storeName)
        {
            SetShardingMethod("HASH_FUNCTION_CONSISTENCY_64");
        }

        void ExecuteSchemeQuery(const TString& alterQuery, const EStatus expectedStatus = EStatus::SUCCESS) const {
            auto session = KikimrRunner.GetTableClient().CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), expectedStatus, alterResult.GetIssues().ToString());
        }

        TString GetQueryResult(const TString& request) const {
                auto db = KikimrRunner.GetQueryClient();
                auto result = db.ExecuteQuery(request, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
                const TString output = FormatResultSetYson(result.GetResultSet(0));
                Cout << output << Endl;
                return output;
        }

        void PrintCount() {
            const TString selectQuery = "SELECT COUNT(*), MAX(pk_int), MIN(pk_int) FROM `" + TablePath + "`";

            auto tableClient = KikimrRunner.GetTableClient();
            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            for (auto&& r : rows) {
                for (auto&& c : r) {
                    Cerr << c.first << ":" << Endl << c.second.GetProto().DebugString() << Endl;
                }
            }
        }

        class TDistribution {
        private:
            YDB_READONLY(ui32, Count, 0);
            YDB_READONLY(ui32, MinCount, 0);
            YDB_READONLY(ui32, MaxCount, 0);
            YDB_READONLY(ui32, GroupsCount, 0);
        public:
            TDistribution(const ui32 count, const ui32 minCount, const ui32 maxCount, const ui32 groupsCount)
                : Count(count)
                , MinCount(minCount)
                , MaxCount(maxCount)
                , GroupsCount(groupsCount)
            {

            }

            TString DebugString() const {
                return TStringBuilder()
                    << "count=" << Count << ";"
                    << "min_count=" << MinCount << ";"
                    << "max_count=" << MaxCount << ";"
                    << "groups_count=" << GroupsCount << ";";
            }
        };

        TDistribution GetDistribution(const bool verbose = false) {
            const TString selectQuery = "PRAGMA Kikimr.OptUseFinalizeByKey='true';SELECT COUNT(*) as c, field FROM `" + TablePath + "` GROUP BY field ORDER BY field";

            auto tableClient = KikimrRunner.GetTableClient();
            auto rows = ExecuteScanQuery(tableClient, selectQuery, verbose);
            ui32 count = 0;
            std::optional<ui32> minCount;
            std::optional<ui32> maxCount;
            std::set<TString> groups;
            for (auto&& r : rows) {
                for (auto&& c : r) {
                    if (c.first == "c") {
                        const ui64 v = GetUint64(c.second);
                        count += v;
                        if (!minCount || *minCount > v) {
                            minCount = v;
                        }
                        if (!maxCount || *maxCount < v) {
                            maxCount = v;
                        }
                    } else if (c.first == "field") {
                        Y_ABORT_UNLESS(groups.emplace(c.second.GetProto().DebugString()).second);
                    }
                    if (verbose) {
                        Cerr << c.first << ":" << Endl << c.second.GetProto().DebugString() << Endl;
                    }
                }
            }
            Y_ABORT_UNLESS(maxCount);
            Y_ABORT_UNLESS(minCount);
            return TDistribution(count, *minCount, *maxCount, groups.size());
        }

        void GetVolumes(ui64& rawBytes, ui64& bytes, const bool verbose = false, const std::vector<TString> columnNames = {}) {
            TString selectQuery = "SELECT * FROM `" + TablePath + "/.sys/primary_index_stats` WHERE Activity = true";
            if (columnNames.size()) {
                selectQuery += " AND EntityName IN ('" + JoinSeq("','", columnNames) + "')";
            }

            auto tableClient = KikimrRunner.GetTableClient();

            std::optional<ui64> rawBytesPred;
            std::optional<ui64> bytesPred;
            while (true) {
                auto rows = ExecuteScanQuery(tableClient, selectQuery);
                rawBytes = 0;
                bytes = 0;
                for (auto&& r : rows) {
                    if (verbose) {
                        Cerr << "-------" << Endl;
                    }
                    for (auto&& c : r) {
                        if (c.first == "RawBytes") {
                            rawBytes += GetUint64(c.second);
                        }
                        if (c.first == "BlobRangeSize") {
                            bytes += GetUint64(c.second);
                        }
                        if (verbose) {
                            Cerr << c.first << ":" << Endl << c.second.GetProto().DebugString() << Endl;
                        }
                    }
                }
                if (rawBytesPred && *rawBytesPred == rawBytes && bytesPred && *bytesPred == bytes) {
                    break;
                } else {
                    rawBytesPred = rawBytes;
                    bytesPred = bytes;
                    Cerr << "Wait changes: " << bytes << "/" << rawBytes << Endl;
                    Sleep(TDuration::Seconds(5));
                }
            }
            Cerr << bytes << "/" << rawBytes << Endl;
        }

        void GetStats(std::vector<NKikimrColumnShardStatisticsProto::TPortionStorage>& stats, const bool verbose = false) {
            TString selectQuery = "SELECT * FROM `" + TablePath + "/.sys/primary_index_portion_stats` WHERE Activity = true";
            auto tableClient = KikimrRunner.GetTableClient();
            auto rows = ExecuteScanQuery(tableClient, selectQuery, verbose);
            for (auto&& r : rows) {
                for (auto&& c : r) {
                    if (c.first == "Stats") {
                        NKikimrColumnShardStatisticsProto::TPortionStorage store;
                        AFL_VERIFY(google::protobuf::TextFormat::ParseFromString(GetUtf8(c.second), &store));
                        stats.emplace_back(store);
                    }
                }
            }

        }

        void GetCount(ui64& count) {
            const TString selectQuery = "SELECT COUNT(*) as a FROM `" + TablePath + "`";

            auto tableClient = KikimrRunner.GetTableClient();

            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            for (auto&& r : rows) {
                for (auto&& c : r) {
                    if (c.first == "a") {
                        count = GetUint64(c.second);
                    }
                }
            }
        }

        template <class TFiller>
        void FillTable(const TFiller& fillPolicy, const ui32 pkKff = 0, const ui32 numRows = 800000) const {
            std::vector<NArrow::NConstruction::IArrayBuilder::TPtr> builders;
            builders.emplace_back(NArrow::NConstruction::TSimpleArrayConstructor<NArrow::NConstruction::TIntSeqFiller<arrow::Int64Type>>::BuildNotNullable("pk_int", numRows * pkKff));
            builders.emplace_back(std::make_shared<NArrow::NConstruction::TSimpleArrayConstructor<TFiller>>("field", fillPolicy));
            NArrow::NConstruction::TRecordBatchConstructor batchBuilder(builders);
            std::shared_ptr<arrow::RecordBatch> batch = batchBuilder.BuildBatch(numRows);
            TBase::SendDataViaActorSystem(TablePath, batch);
        }

        void FillPKOnly(const double pkKff = 0, const ui32 numRows = 800000) const {
            std::vector<NArrow::NConstruction::IArrayBuilder::TPtr> builders;
            builders.emplace_back(NArrow::NConstruction::TSimpleArrayConstructor<NArrow::NConstruction::TIntSeqFiller<arrow::Int64Type>>::BuildNotNullable("pk_int", numRows * pkKff));
            NArrow::NConstruction::TRecordBatchConstructor batchBuilder(builders);
            std::shared_ptr<arrow::RecordBatch> batch = batchBuilder.BuildBatch(numRows);
            TBase::SendDataViaActorSystem(TablePath, batch);
        }

        void CreateTestOlapTable(ui32 storeShardsCount = 4, ui32 tableShardsCount = 3) {
            CreateOlapTableWithStore(TableName, StoreName, storeShardsCount, tableShardsCount);
        }
    };

    class TLocalHelper: public Tests::NCS::THelper {
    private:
        using TBase = Tests::NCS::THelper;
    public:
        TLocalHelper& SetShardingMethod(const TString& value) {
            TBase::SetShardingMethod(value);
            return *this;
        }

        void CreateTestOlapTable(TString tableName = "olapTable", TString storeName = "olapStore",
            ui32 storeShardsCount = 4, ui32 tableShardsCount = 3) {
            CreateOlapTableWithStore(tableName, storeName, storeShardsCount, tableShardsCount);
        }
        using TBase::TBase;

        TLocalHelper(TKikimrRunner& runner)
            : TBase(runner.GetTestServer()) {

        }
    };

    class TExtLocalHelper: public TLocalHelper {
    private:
        using TBase = TLocalHelper;
        TKikimrRunner& KikimrRunner;
    public:
        bool TryCreateTable(const TString& storeName, const TString& tableName, const ui32 shardsCount) {
            auto tableClient = KikimrRunner.GetTableClient();
            auto session = tableClient.CreateSession().GetValueSync().GetSession();

            auto query = TStringBuilder() << Sprintf(R"(
            --!syntax_v1
            CREATE TABLE `/Root/%s/%s`
            (
                timestamp Timestamp NOT NULL,
                resource_id Utf8,
                uid Utf8,
                level Int32,
                message Utf8,
                PRIMARY KEY (timestamp, uid)
            )
            PARTITION BY HASH(timestamp)
            WITH (
                STORE = COLUMN,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = %d
                )
            )", storeName.data(), tableName.data(), shardsCount);
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            if (result.GetStatus() != EStatus::SUCCESS) {
                Cerr << result.GetIssues().ToOneLineString() << Endl;
            }
            return result.GetStatus() == EStatus::SUCCESS;
        }

        bool DropTable(const TString& storeName, const TString& tableName) {
            auto tableClient = KikimrRunner.GetTableClient();
            auto session = tableClient.CreateSession().GetValueSync().GetSession();

            auto query = TStringBuilder() << Sprintf(R"(
            --!syntax_v1
            DROP TABLE `/Root/%s/%s`
            )", storeName.data(), tableName.data());
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            if (result.GetStatus() != EStatus::SUCCESS) {
                Cerr << result.GetIssues().ToOneLineString() << Endl;
            }
            return result.GetStatus() == EStatus::SUCCESS;
        }

        TExtLocalHelper(TKikimrRunner& runner)
            : TBase(runner.GetTestServer())
            , KikimrRunner(runner) {

        }
    };

    class TClickHelper : public Tests::NCS::TCickBenchHelper {
    private:
        using TBase = Tests::NCS::TCickBenchHelper;
    public:
        using TBase::TBase;

        TClickHelper(TKikimrRunner& runner)
            : TBase(runner.GetTestServer())
        {}

        void CreateClickBenchTable(TString tableName = "benchTable", ui32 shardsCount = 4) {
            TActorId sender = Server.GetRuntime()->AllocateEdgeActor();

            TBase::CreateTestOlapTable(sender, "", Sprintf(R"(
                Name: "%s"
                ColumnShardCount: %d
                Schema {
                    %s
                }
                Sharding {
                    HashSharding {
                        Function: HASH_FUNCTION_CONSISTENCY_64
                        Columns: "EventTime"
                    }
                })", tableName.c_str(), shardsCount, PROTO_SCHEMA));
        }
    };

    class TTableWithNullsHelper : public Tests::NCS::TTableWithNullsHelper {
    private:
        using TBase = Tests::NCS::TTableWithNullsHelper;
    public:
        using TBase::TBase;

        TTableWithNullsHelper(TKikimrRunner& runner)
            : TBase(runner.GetTestServer())
        {}

        void CreateTableWithNulls(TString tableName = "tableWithNulls", ui32 shardsCount = 4) {
            TActorId sender = Server.GetRuntime()->AllocateEdgeActor();

            TBase::CreateTestOlapTable(sender, "", Sprintf(R"(
                Name: "%s"
                ColumnShardCount: %d
                Schema {
                    %s
                }
                Sharding {
                    HashSharding {
                        Function: HASH_FUNCTION_CONSISTENCY_64
                        Columns: "id"
                    }
                })", tableName.c_str(), shardsCount, PROTO_SCHEMA));
        }
    };

    void WriteTestData(TKikimrRunner& kikimr, TString testTable, ui64 pathIdBegin, ui64 tsBegin, size_t rowCount, bool withSomeNulls = false) {
        UNIT_ASSERT(testTable != "/Root/benchTable"); // TODO: check schema instead
        TLocalHelper lHelper(kikimr);
        if (withSomeNulls) {
            lHelper.WithSomeNulls();
        }
        auto batch = lHelper.TestArrowBatch(pathIdBegin, tsBegin, rowCount);
        lHelper.SendDataViaActorSystem(testTable, batch);
    }

    void WriteTestDataForClickBench(TKikimrRunner& kikimr, TString testTable, ui64 pathIdBegin, ui64 tsBegin, size_t rowCount) {
        UNIT_ASSERT(testTable == "/Root/benchTable"); // TODO: check schema instead
        TClickHelper lHelper(kikimr.GetTestServer());
        auto batch = lHelper.TestArrowBatch(pathIdBegin, tsBegin, rowCount);
        lHelper.SendDataViaActorSystem(testTable, batch);
    }

    void WriteTestDataForTableWithNulls(TKikimrRunner& kikimr, TString testTable) {
        UNIT_ASSERT(testTable == "/Root/tableWithNulls"); // TODO: check schema instead
        TTableWithNullsHelper lHelper(kikimr.GetTestServer());
        auto batch = lHelper.TestArrowBatch();
        lHelper.SendDataViaActorSystem(testTable, batch);
    }

    void CreateTableOfAllTypes(TKikimrRunner& kikimr) {
        auto& legacyClient = kikimr.GetTestClient();

        legacyClient.CreateOlapStore("/Root", R"(
                                     Name: "olapStore"
                                     ColumnShardCount: 1
                                     SchemaPresets {
                                         Name: "default"
                                         Schema {
                                             Columns { Name: "key" Type: "Int32" NotNull: true }
                                             #Columns { Name: "Bool_column" Type: "Bool" }
                                             # Int8, Int16, UInt8, UInt16 is not supported by engine
                                             Columns { Name: "Int8_column" Type: "Int32" }
                                             Columns { Name: "Int16_column" Type: "Int32" }
                                             Columns { Name: "Int32_column" Type: "Int32" }
                                             Columns { Name: "Int64_column" Type: "Int64" }
                                             Columns { Name: "UInt8_column" Type: "Uint32" }
                                             Columns { Name: "UInt16_column" Type: "Uint32" }
                                             Columns { Name: "UInt32_column" Type: "Uint32" }
                                             Columns { Name: "UInt64_column" Type: "Uint64" }
                                             Columns { Name: "Double_column" Type: "Double" }
                                             Columns { Name: "Float_column" Type: "Float" }
                                             #Columns { Name: "Decimal_column" Type: "Decimal" }
                                             Columns { Name: "String_column" Type: "String" }
                                             Columns { Name: "Utf8_column" Type: "Utf8" }
                                             Columns { Name: "Json_column" Type: "Json" }
                                             Columns { Name: "Yson_column" Type: "Yson" }
                                             Columns { Name: "Timestamp_column" Type: "Timestamp" }
                                             Columns { Name: "Date_column" Type: "Date" }
                                             Columns { Name: "Datetime_column" Type: "Datetime" }
                                             #Columns { Name: "Interval_column" Type: "Interval" }
                                             KeyColumnNames: "key"
                                             Engine: COLUMN_ENGINE_REPLACING_TIMESERIES
                                         }
                                     }
        )");

        legacyClient.CreateColumnTable("/Root/olapStore", R"(
            Name: "OlapParametersTable"
            ColumnShardCount: 1
        )");
        legacyClient.Ls("/Root");
        legacyClient.Ls("/Root/olapStore");
        legacyClient.Ls("/Root/olapStore/OlapParametersTable");
    }

    std::map<std::string, TParams> CreateParametersOfAllTypes(NYdb::NTable::TTableClient& tableClient) {
         return {
#if 0
            {
                "Bool",
                tableClient.GetParamsBuilder().AddParam("$in_value").Bool(false).Build().Build()
            },
#endif
            {
                "Int8",
                tableClient.GetParamsBuilder().AddParam("$in_value").Int8(0).Build().Build()
            },
            {
                "Int16",
                tableClient.GetParamsBuilder().AddParam("$in_value").Int16(0).Build().Build()
            },
            {
                "Int32",
                tableClient.GetParamsBuilder().AddParam("$in_value").Int32(0).Build().Build()
            },
            {
                "Int64",
                tableClient.GetParamsBuilder().AddParam("$in_value").Int64(0).Build().Build()
            },
            {
                "UInt8",
                tableClient.GetParamsBuilder().AddParam("$in_value").Uint8(0).Build().Build()
            },
            {
                "UInt16",
                tableClient.GetParamsBuilder().AddParam("$in_value").Uint16(0).Build().Build()
            },
            {
                "UInt32",
                tableClient.GetParamsBuilder().AddParam("$in_value").Uint32(0).Build().Build()
            },
            {
                "UInt64",
                tableClient.GetParamsBuilder().AddParam("$in_value").Uint64(0).Build().Build()
            },
            {
                "Float",
                tableClient.GetParamsBuilder().AddParam("$in_value").Float(0).Build().Build()
            },
            {
                "Double",
                tableClient.GetParamsBuilder().AddParam("$in_value").Double(0).Build().Build()
            },
            {
                "String",
                tableClient.GetParamsBuilder().AddParam("$in_value").String("XX").Build().Build()
            },
            {
                "Utf8",
                tableClient.GetParamsBuilder().AddParam("$in_value").Utf8("XX").Build().Build()
            },
            {
                "Timestamp",
                tableClient.GetParamsBuilder().AddParam("$in_value").Timestamp(TInstant::Now()).Build().Build()
            },
            {
                "Date",
                tableClient.GetParamsBuilder().AddParam("$in_value").Date(TInstant::Now()).Build().Build()
            },
            {
                "Datetime",
                tableClient.GetParamsBuilder().AddParam("$in_value").Datetime(TInstant::Now()).Build().Build()
            },
#if 0
            {
                "Interval",
                tableClient.GetParamsBuilder().AddParam("$in_value").Interval(1010).Build().Build()
            },
            {
                "Decimal(12,9)",
                tableClient.GetParamsBuilder().AddParam("$in_value").Decimal(TDecimalValue("10.123456789", 12, 9)).Build().Build()
            },
            {
                "Json",
                tableClient.GetParamsBuilder().AddParam("$in_value").Json(R"({"XX":"YY"})").Build().Build()
            },
            {
                "Yson",
                tableClient.GetParamsBuilder().AddParam("$in_value").Yson("[[[]]]").Build().Build()
            },
#endif
        };
    }

    template <typename TClient>
    auto StreamExplainQuery(const TString& query, TClient& client) {
        if constexpr (std::is_same_v<NYdb::NTable::TTableClient, TClient>) {
            TStreamExecScanQuerySettings scanSettings;
            scanSettings.Explain(true);
            return client.StreamExecuteScanQuery(query, scanSettings).GetValueSync();
        } else {
            NYdb::NQuery::TExecuteQuerySettings scanSettings;
            scanSettings.ExecMode(NYdb::NQuery::EExecMode::Explain);
            return client.StreamExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), scanSettings).GetValueSync();
        }
    }

    template <typename TClient>
    void CheckPlanForAggregatePushdown(
        const TString& query,
        TClient& client,
        const std::vector<std::string>& expectedPlanNodes,
        const std::string& readNodeType)
    {
        auto res = StreamExplainQuery(query, client);
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

        auto planRes = CollectStreamResult(res);
        auto ast = planRes.QueryStats->Getquery_ast();
        Cerr << "JSON Plan:" << Endl;
        Cerr << planRes.PlanJson.GetOrElse("NO_PLAN") << Endl;
        Cerr << "AST:" << Endl;
        Cerr << ast << Endl;
        for (auto planNode : expectedPlanNodes) {
            UNIT_ASSERT_C(ast.find(planNode) != std::string::npos,
                TStringBuilder() << planNode << " was not found. Query: " << query);
        }
        UNIT_ASSERT_C(ast.find("SqueezeToDict") == std::string::npos, TStringBuilder() << "SqueezeToDict denied for aggregation requests. Query: " << query);

        if (!readNodeType.empty()) {
            NJson::TJsonValue planJson;
            NJson::ReadJsonTree(*planRes.PlanJson, &planJson, true);
            auto readNode = FindPlanNodeByKv(planJson, "Node Type", readNodeType.c_str());
            UNIT_ASSERT(readNode.IsDefined());

            auto& operators = readNode.GetMapSafe().at("Operators").GetArraySafe();
            for (auto& op : operators) {
                if (op.GetMapSafe().at("Name") == "TableFullScan") {
                    auto ssaProgram = op.GetMapSafe().at("SsaProgram");
                    UNIT_ASSERT(ssaProgram.IsDefined());
                    UNIT_ASSERT(FindPlanNodes(ssaProgram, "Projection").size());
                    break;
                }
            }
        }
    }

    Y_UNIT_TEST(SimpleQueryOlap) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();

        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 2);

        auto client = kikimr.GetTableClient();

        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        {
            auto it = client.StreamExecuteScanQuery(R"(
                --!syntax_v1

                SELECT `resource_id`, `timestamp`
                FROM `/Root/olapStore/olapTable`
                ORDER BY `resource_id`, `timestamp`
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;
            CompareYson(result, R"([[["0"];1000000u];[["1"];1000001u]])");
        }
    }

    Y_UNIT_TEST(SimpleQueryOlapStats) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();

        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 2);

        auto client = kikimr.GetTableClient();

        {
            TStreamExecScanQuerySettings settings;
            settings.CollectQueryStats(ECollectQueryStatsMode::Full);
            auto it = client.StreamExecuteScanQuery(R"(
                --!syntax_v1
                SELECT `resource_id`, `timestamp`
                FROM `/Root/olapStore/olapTable`
                ORDER BY `resource_id`, `timestamp`
            )", settings).GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            NJson::TJsonValue jsonStat;
            CollectRows(it, &jsonStat);
            UNIT_ASSERT(!jsonStat.IsNull());
            const TString plan = jsonStat.GetStringRobust();
            Cerr << plan << Endl;
            UNIT_ASSERT(plan.find("NodesScanShards") == TString::npos);
        }

        {
            TStreamExecScanQuerySettings settings;
            settings.CollectQueryStats(ECollectQueryStatsMode::Profile);
            auto it = client.StreamExecuteScanQuery(R"(
                --!syntax_v1
                SELECT `resource_id`, `timestamp`
                FROM `/Root/olapStore/olapTable`
                ORDER BY `resource_id`, `timestamp`
            )", settings).GetValueSync();
            NJson::TJsonValue jsonStat;
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            CollectRows(it, &jsonStat);
            const TString plan = jsonStat.GetStringRobust();
            Cerr << plan << Endl;
            UNIT_ASSERT(plan.find("NodesScanShards") != TString::npos);
        }
    }

    Y_UNIT_TEST(SimpleQueryOlapDiagnostics) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();

        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 2);

        auto client = kikimr.GetTableClient();

        {
            TStreamExecScanQuerySettings settings;
            settings.CollectQueryStats(ECollectQueryStatsMode::Full);
            auto it = client.StreamExecuteScanQuery(R"(
                --!syntax_v1
                SELECT `resource_id`, `timestamp`
                FROM `/Root/olapStore/olapTable`
                ORDER BY `resource_id`, `timestamp`
            )", settings).GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            NJson::TJsonValue jsonDiagnostics;
            CollectRows(it, nullptr, &jsonDiagnostics);
            UNIT_ASSERT_C(!jsonDiagnostics.IsDefined(), "Query result diagnostics should be empty, but it's not");
        }

        {
            TStreamExecScanQuerySettings settings;
            settings.CollectQueryStats(ECollectQueryStatsMode::Full);
            settings.CollectFullDiagnostics(true);
            auto it = client.StreamExecuteScanQuery(R"(
                --!syntax_v1
                SELECT `resource_id`, `timestamp`
                FROM `/Root/olapStore/olapTable`
                ORDER BY `resource_id`, `timestamp`
            )", settings).GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            NJson::TJsonValue jsonDiagnostics;
            CollectRows(it, nullptr, &jsonDiagnostics);
            UNIT_ASSERT(!jsonDiagnostics.IsNull());

            UNIT_ASSERT_C(jsonDiagnostics.IsMap(), "Incorrect Diagnostics");
            UNIT_ASSERT_C(jsonDiagnostics.Has("query_id"), "Incorrect Diagnostics");
            UNIT_ASSERT_C(jsonDiagnostics.Has("version"), "Incorrect Diagnostics");
            UNIT_ASSERT_C(jsonDiagnostics.Has("query_text"), "Incorrect Diagnostics");
            UNIT_ASSERT_C(jsonDiagnostics.Has("query_parameter_types"), "Incorrect Diagnostics");
            UNIT_ASSERT_C(jsonDiagnostics.Has("table_metadata"), "Incorrect Diagnostics");
            UNIT_ASSERT_C(jsonDiagnostics.Has("created_at"), "Incorrect Diagnostics");
            UNIT_ASSERT_C(jsonDiagnostics.Has("query_syntax"), "Incorrect Diagnostics");
            UNIT_ASSERT_C(jsonDiagnostics.Has("query_database"), "Incorrect Diagnostics");
            UNIT_ASSERT_C(jsonDiagnostics.Has("query_cluster"), "Incorrect Diagnostics");
            UNIT_ASSERT_C(jsonDiagnostics.Has("query_plan"), "Incorrect Diagnostics");
            UNIT_ASSERT_C(jsonDiagnostics.Has("query_type"), "Incorrect Diagnostics");
        }
    }

    Y_UNIT_TEST(SimpleLookupOlap) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();

        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 2);

        auto client = kikimr.GetTableClient();

        {
            auto it = client.StreamExecuteScanQuery(R"(
                --!syntax_v1

                SELECT `resource_id`, `timestamp`
                FROM `/Root/olapStore/olapTable`
                WHERE `timestamp` == CAST(1000000 AS Timestamp)
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;

            CompareYson(result, R"([[["0"];1000000u]])");
        }
    }

    Y_UNIT_TEST(SimpleRangeOlap) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();

        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 2);

        auto client = kikimr.GetTableClient();

        {
            auto it = client.StreamExecuteScanQuery(R"(
                --!syntax_v1

                SELECT `resource_id`, `timestamp`
                FROM `/Root/olapStore/olapTable`
                WHERE `timestamp` >= CAST(1000000 AS Timestamp)
                  AND `timestamp` <= CAST(2000000 AS Timestamp)
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;

            CompareYson(result, R"([[["0"];1000000u];[["1"];1000001u]])");
        }
    }

    Y_UNIT_TEST(CompositeRangeOlap) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();

        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 2);

        auto client = kikimr.GetTableClient();

        {
            auto it = client.StreamExecuteScanQuery(R"(
                --!syntax_v1

                SELECT `resource_id`, `timestamp`
                FROM `/Root/olapStore/olapTable`
                WHERE `timestamp` >= CAST(1000000 AS Timestamp)
                    AND `timestamp` < CAST(1000001 AS Timestamp)
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;

            CompareYson(result, R"([[["0"];1000000u]])");
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                --!syntax_v1

                SELECT `resource_id`, `timestamp`
                FROM `/Root/olapStore/olapTable`
                WHERE `timestamp` >= CAST(1000000 AS Timestamp)
                    AND `timestamp` <= CAST(1000001 AS Timestamp)
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;

            CompareYson(result, R"([[["0"];1000000u];[["1"];1000001u]])");
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                --!syntax_v1

                SELECT `resource_id`, `timestamp`
                FROM `/Root/olapStore/olapTable`
                WHERE `timestamp` > CAST(1000000 AS Timestamp)
                    AND `timestamp` <= CAST(1000001 AS Timestamp)
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;

            CompareYson(result, R"([[["1"];1000001u]])");
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                --!syntax_v1

                SELECT `resource_id`, `timestamp`
                FROM `/Root/olapStore/olapTable`
                WHERE `timestamp` >= CAST(1000000 AS Timestamp)
                    AND `resource_id` == "0"
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;

            CompareYson(result, R"([[["0"];1000000u]])");
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                --!syntax_v1

                SELECT `resource_id`, `timestamp`
                FROM `/Root/olapStore/olapTable`
                WHERE `timestamp` <= CAST(1000001 AS Timestamp)
                    AND `resource_id` == "1"
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;

            CompareYson(result, R"([[["1"];1000001u]])");
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                --!syntax_v1

                SELECT `resource_id`, `timestamp`
                FROM `/Root/olapStore/olapTable`
                WHERE `timestamp` > CAST(1000000 AS Timestamp)
                    AND `resource_id` == "1"
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;

            CompareYson(result, R"([[["1"];1000001u]])");
        }

        {
            auto it = client.StreamExecuteScanQuery(R"(
                --!syntax_v1

                SELECT `resource_id`, `timestamp`
                FROM `/Root/olapStore/olapTable`
                WHERE `timestamp` < CAST(1000001 AS Timestamp)
                    AND `resource_id` == "0"
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;

            CompareYson(result, R"([[["0"];1000000u]])");
        }
    }

    void CreateSampleOltpTable(TKikimrRunner& kikimr) {
        kikimr.GetTestClient().CreateTable("/Root", R"(
            Name: "OltpTable"
            Columns { Name: "Key", Type: "Uint64" }
            Columns { Name: "Value1", Type: "String" }
            Columns { Name: "Value2", Type: "String" }
            KeyColumnNames: ["Key"]
        )");

        TTableClient tableClient{kikimr.GetDriver()};
        auto session = tableClient.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            REPLACE INTO `/Root/OltpTable` (Key, Value1, Value2) VALUES
                (1u,   "Value-001",  "1"),
                (2u,   "Value-002",  "2"),
                (42u,  "Value-002",  "2"),
                (101u, "Value-101",  "101")
        )", TTxControl::BeginTx().CommitTx()).GetValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        session.Close();
    }

    Y_UNIT_TEST(ScanQueryOltpAndOlap) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto client = kikimr.GetTableClient();

        TLocalHelper(kikimr).CreateTestOlapTable();

        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 3);

        CreateSampleOltpTable(kikimr);

        {
            auto it = client.StreamExecuteScanQuery(R"(
                --!syntax_v1

                SELECT a.`resource_id`, a.`timestamp`, t.*
                FROM `/Root/OltpTable` AS t
                JOIN `/Root/olapStore/olapTable` AS a ON CAST(t.Key AS Utf8) = a.resource_id
                ORDER BY a.`resource_id`, a.`timestamp`
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;
            CompareYson(result, R"([[[1u];["Value-001"];["1"];["1"];1000001u];[[2u];["Value-002"];["2"];["2"];1000002u]])");
        }
    }

    Y_UNIT_TEST(YqlScriptOltpAndOlap) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 3);

        CreateSampleOltpTable(kikimr);

        {
            NScripting::TScriptingClient client(kikimr.GetDriver());
            auto it = client.ExecuteYqlScript(R"(
                --!syntax_v1

                SELECT a.`resource_id`, a.`timestamp`, t.*
                FROM `/Root/OltpTable` AS t
                JOIN `/Root/olapStore/olapTable` AS a ON CAST(t.Key AS Utf8) = a.resource_id
                ORDER BY a.`resource_id`, a.`timestamp`
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = FormatResultSetYson(it.GetResultSet(0));
            Cout << result << Endl;
            CompareYson(result, R"([[[1u];["Value-001"];["1"];["1"];1000001u];[[2u];["Value-002"];["2"];["2"];1000002u]])");
        }
    }

    Y_UNIT_TEST(EmptyRange) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000000, 1000);

        auto tableClient = kikimr.GetTableClient();

        auto it = tableClient.StreamExecuteScanQuery(R"(
            --!syntax_v1

            SELECT *
            FROM `/Root/olapStore/olapTable`
            WHERE `timestamp` < CAST(3000001 AS Timestamp) AND `timestamp` > CAST(3000005 AS Timestamp)
        )").GetValueSync();

        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        CompareYson(StreamResultToYson(it), "[]");
    }

    Y_UNIT_TEST(Aggregation) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();

        auto tableClient = kikimr.GetTableClient();

        {
            auto it = tableClient.StreamExecuteScanQuery(R"(
                --!syntax_v1

                SELECT
                    COUNT(*)
                FROM `/Root/olapStore/olapTable`
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;
            CompareYson(result, R"([[0u;]])");
        }

        {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 11000, 3001000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 12000, 3002000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 13000, 3003000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 14000, 3004000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 20000, 2000000, 7000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 30000, 1000000, 11000);
        }

        {
            auto it = tableClient.StreamExecuteScanQuery(R"(
                --!syntax_v1

                SELECT
                    COUNT(*), MAX(`resource_id`), MAX(`timestamp`), MIN(LENGTH(`message`))
                FROM `/Root/olapStore/olapTable`
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;
            CompareYson(result, R"([[23000u;["40999"];[3004999u];[1036u]]])");
        }

        {
            auto it = tableClient.StreamExecuteScanQuery(R"(
                --!syntax_v1

                SELECT
                    COUNT(*)
                FROM `/Root/olapStore/olapTable`
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;
            CompareYson(result, R"([[23000u;]])");
        }
    }

    Y_UNIT_TEST(IndexesActualization) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->SetPeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        csController->SetLagForCompactionBeforeTierings(TDuration::Seconds(1));

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();

        Tests::NCommon::TLoggerInit(kikimr).SetComponents({NKikimrServices::TX_COLUMNSHARD}, "CS").SetPriority(NActors::NLog::PRI_DEBUG).Initialize();

        std::vector<TString> uids;
        std::vector<TString> resourceIds;
        std::vector<ui32> levels;

        {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 1000000, 300000000, 10000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 1100000, 300100000, 10000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 1200000, 300200000, 10000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 1300000, 300300000, 10000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 1400000, 300400000, 10000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 2000000, 200000000, 70000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 3000000, 100000000, 110000);

            const auto filler = [&](const ui32 startRes, const ui32 startUid, const ui32 count) {
                for (ui32 i = 0; i < count; ++i) {
                    uids.emplace_back("uid_" + ::ToString(startUid + i));
                    resourceIds.emplace_back(::ToString(startRes + i));
                    levels.emplace_back(i % 5);
                }
            };

            filler(1000000, 300000000, 10000);
            filler(1100000, 300100000, 10000);
            filler(1200000, 300200000, 10000);
            filler(1300000, 300300000, 10000);
            filler(1400000, 300400000, 10000);
            filler(2000000, 200000000, 70000);
            filler(3000000, 100000000, 110000);

        }

        {
            auto alterQuery = TStringBuilder() <<
                R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_uid, TYPE=BLOOM_FILTER, 
                    FEATURES=`{"column_names" : ["uid"], "false_positive_probability" : 0.05}`);
                )";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }
        {
            auto alterQuery = TStringBuilder() <<
                R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_resource_id, TYPE=BLOOM_FILTER, 
                    FEATURES=`{"column_names" : ["resource_id", "level"], "false_positive_probability" : 0.05}`);
                )";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }

        {
            auto alterQuery = TStringBuilder() <<
                "ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`);";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }
        csController->WaitActualization(TDuration::Seconds(10));
        {
            auto it = tableClient.StreamExecuteScanQuery(R"(
                --!syntax_v1

                SELECT
                    COUNT(*)
                FROM `/Root/olapStore/olapTable`
                WHERE ((resource_id = '2' AND level = 222222) OR (resource_id = '1' AND level = 111111) OR (resource_id LIKE '%11dd%')) AND uid = '222'
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cerr << result << Endl;
            Cerr << csController->GetIndexesSkippingOnSelect().Val() << " / " << csController->GetIndexesApprovedOnSelect().Val() << Endl;
            CompareYson(result, R"([[0u;]])");
            AFL_VERIFY(csController->GetIndexesSkippedNoData().Val() == 0);
            AFL_VERIFY(csController->GetIndexesApprovedOnSelect().Val() < csController->GetIndexesSkippingOnSelect().Val() * 0.4)
                ("approve", csController->GetIndexesApprovedOnSelect().Val())("skip", csController->GetIndexesSkippingOnSelect().Val());
        }
    }

    Y_UNIT_TEST(IndexesActualizationRebuildScheme) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->SetPeriodicWakeupActivationPeriod(TDuration::Seconds(1));

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();

        std::vector<TString> uids;
        std::vector<TString> resourceIds;
        std::vector<ui32> levels;

        {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 1000000, 300000000, 10000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 1100000, 300100000, 10000);

            const auto filler = [&](const ui32 startRes, const ui32 startUid, const ui32 count) {
                for (ui32 i = 0; i < count; ++i) {
                    uids.emplace_back("uid_" + ::ToString(startUid + i));
                    resourceIds.emplace_back(::ToString(startRes + i));
                    levels.emplace_back(i % 5);
                }
            };

            filler(1000000, 300000000, 10000);
            filler(1100000, 300100000, 10000);

        }

        for (ui32 i = 0; i < 10; ++i) {
            auto alterQuery = TStringBuilder() <<
                "ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`);";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }
        const ui64 startCount = csController->GetActualizationRefreshSchemeCount().Val();
        AFL_VERIFY(startCount == 30);

        for (auto&& i : csController->GetShardActualIds()) {
            kikimr.GetTestServer().GetRuntime()->Send(MakePipePeNodeCacheID(false), NActors::TActorId(), new TEvPipeCache::TEvForward(
                new TEvents::TEvPoisonPill(), i, false));
        }

        {
            auto it = tableClient.StreamExecuteScanQuery(R"(
                --!syntax_v1
                SELECT
                    COUNT(*)
                FROM `/Root/olapStore/olapTable`
            )").GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;
            CompareYson(result, R"([[20000u;]])");
        }

        AFL_VERIFY(startCount + 3 /*tables count*/ * 2 /*normalizers + main_load*/ == 
            (ui64)csController->GetActualizationRefreshSchemeCount().Val())("start", startCount)("count", csController->GetActualizationRefreshSchemeCount().Val());
    }

    Y_UNIT_TEST(Indexes) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();

//        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();

        {
            auto alterQuery = TStringBuilder() <<
                R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_uid, TYPE=BLOOM_FILTER, 
                    FEATURES=`{"column_names" : ["uid"], "false_positive_probability" : 0.05}`);
                )";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }
        {
            auto alterQuery = TStringBuilder() <<
                R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_resource_id, TYPE=BLOOM_FILTER, 
                    FEATURES=`{"column_names" : ["resource_id", "level"], "false_positive_probability" : 0.05}`);
                )";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }

        std::vector<TString> uids;
        std::vector<TString> resourceIds;
        std::vector<ui32> levels;

        {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 1000000, 300000000, 10000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 1100000, 300100000, 10000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 1200000, 300200000, 10000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 1300000, 300300000, 10000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 1400000, 300400000, 10000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 2000000, 200000000, 70000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 3000000, 100000000, 110000);

            const auto filler = [&](const ui32 startRes, const ui32 startUid, const ui32 count) {
                for (ui32 i = 0; i < count; ++i) {
                    uids.emplace_back("uid_" + ::ToString(startUid + i));
                    resourceIds.emplace_back(::ToString(startRes + i));
                    levels.emplace_back(i % 5);
                }
            };

            filler(1000000, 300000000, 10000);
            filler(1100000, 300100000, 10000);
            filler(1200000, 300200000, 10000);
            filler(1300000, 300300000, 10000);
            filler(1400000, 300400000, 10000);
            filler(2000000, 200000000, 70000);
            filler(3000000, 100000000, 110000);
            
        }

        {
            auto it = tableClient.StreamExecuteScanQuery(R"(
                --!syntax_v1

                SELECT
                    COUNT(*)
                FROM `/Root/olapStore/olapTable`
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;
            CompareYson(result, R"([[230000u;]])");
        }

        AFL_VERIFY(csController->GetIndexesSkippingOnSelect().Val() == 0);
        AFL_VERIFY(csController->GetIndexesApprovedOnSelect().Val() == 0);
        TInstant start = Now();
        ui32 compactionsStart = csController->GetCompactions().Val();
        while (Now() - start < TDuration::Seconds(10)) {
            if (compactionsStart != csController->GetCompactions().Val()) {
                compactionsStart = csController->GetCompactions().Val();
                start = Now();
            }
            Cerr << "WAIT_COMPACTION: " << csController->GetCompactions().Val() << Endl;
            Sleep(TDuration::Seconds(1));
        }

        {
            auto it = tableClient.StreamExecuteScanQuery(R"(
                --!syntax_v1

                SELECT
                    COUNT(*)
                FROM `/Root/olapStore/olapTable`
                WHERE ((resource_id = '2' AND level = 222222) OR (resource_id = '1' AND level = 111111) OR (resource_id LIKE '%11dd%')) AND uid = '222'
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;
            Cout << csController->GetIndexesSkippingOnSelect().Val() << " / " << csController->GetIndexesApprovedOnSelect().Val() << Endl;
            CompareYson(result, R"([[0u;]])");
            AFL_VERIFY(csController->GetIndexesSkippedNoData().Val() == 0);
            AFL_VERIFY(csController->GetIndexesApprovedOnSelect().Val() < csController->GetIndexesSkippingOnSelect().Val() * 0.3);
        }
        ui32 requestsCount = 100;
        for (ui32 i = 0; i < requestsCount; ++i) {
            const ui32 idx = RandomNumber<ui32>(uids.size());
            const auto query = [](const TString& res, const TString& uid, const ui32 level) {
                TStringBuilder sb;
                sb << "SELECT" << Endl;
                sb << "COUNT(*)" << Endl;
                sb << "FROM `/Root/olapStore/olapTable`" << Endl;
                sb << "WHERE(" << Endl;
                sb << "resource_id = '" << res << "' AND" << Endl;
                sb << "uid= '" << uid << "' AND" << Endl;
                sb << "level= " << level << Endl;
                sb << ")";
                return sb;
            };
            auto it = tableClient.StreamExecuteScanQuery(query(resourceIds[idx], uids[idx], levels[idx])).GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << csController->GetIndexesSkippingOnSelect().Val() << " / " << csController->GetIndexesApprovedOnSelect().Val() << " / " << csController->GetIndexesSkippedNoData().Val() << Endl;
            CompareYson(result, R"([[1u;]])");
        }

        AFL_VERIFY(csController->GetIndexesApprovedOnSelect().Val() < 0.20 * csController->GetIndexesSkippingOnSelect().Val());

    }

    Y_UNIT_TEST(IndexesModificationError) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();

        //        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();

        {
            auto alterQuery = TStringBuilder() <<
                R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_uid, TYPE=BLOOM_FILTER, 
                    FEATURES=`{"column_names" : ["uid"], "false_positive_probability" : 0.05}`);
                )";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }

        {
            auto alterQuery = TStringBuilder() <<
                R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_uid, TYPE=BLOOM_FILTER, 
                    FEATURES=`{"column_names" : ["uid", "resource_id"], "false_positive_probability" : 0.05}`);
                )";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_UNEQUAL(alterResult.GetStatus(), EStatus::SUCCESS);
        }

        {
            auto alterQuery = TStringBuilder() <<
                R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_uid, TYPE=BLOOM_FILTER, 
                    FEATURES=`{"column_names" : ["uid"], "false_positive_probability" : 0.005}`);
                )";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_UNEQUAL(alterResult.GetStatus(), EStatus::SUCCESS);
        }

        {
            auto alterQuery = TStringBuilder() <<
                R"(ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_INDEX, NAME=index_uid, TYPE=BLOOM_FILTER, 
                    FEATURES=`{"column_names" : ["uid"], "false_positive_probability" : 0.01}`);
                )";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }

        {
            auto alterQuery = TStringBuilder() << "ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=DROP_INDEX, NAME=index_uid);";
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::SUCCESS, alterResult.GetIssues().ToString());
        }

    }

    Y_UNIT_TEST(PushdownFilter) {
        static bool enableLog = false;

        auto doTest = [](std::optional<bool> viaPragma, bool pushdownPresent) {
            auto settings = TKikimrSettings()
                .SetWithSampleTables(false);

            if (enableLog) {
                Cerr << "Run test:" << Endl;
                Cerr << "viaPragma is " << (viaPragma.has_value() ? "" : "not ") << "present.";
                if (viaPragma.has_value()) {
                    Cerr << " Value: " << viaPragma.value();
                }
                Cerr << Endl;
                Cerr << "Expected result: " << pushdownPresent << Endl;
            }

            TKikimrRunner kikimr(settings);
            kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NActors::NLog::PRI_DEBUG);

            auto client = kikimr.GetTableClient();

            TLocalHelper(kikimr).CreateTestOlapTable();
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 10);

            TStreamExecScanQuerySettings scanSettings;
            scanSettings.Explain(true);

            {
                TString query = TString(R"(
                    --!syntax_v1
                    SELECT * FROM `/Root/olapStore/olapTable` WHERE resource_id = "5"u;
                )");

                if (viaPragma.has_value() && !viaPragma.value()) {
                    TString pragma = TString(R"(
                        PRAGMA Kikimr.OptEnableOlapPushdown = "false";
                    )");
                    query = pragma + query;
                }

                auto it = client.StreamExecuteScanQuery(query).GetValueSync();

                UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
                TString result = StreamResultToYson(it);

                CompareYson(result, R"([[
                    [0];
                    ["some prefix xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"];
                    ["5"];
                    1000005u;
                    "uid_1000005"
                    ]])");
            }
        };

        TVector<std::tuple<std::optional<bool>, bool>> testData = {
            {std::nullopt, true},
            {false, false},
            {true, true},
        };

        for (auto &data: testData) {
            doTest(std::get<0>(data), std::get<1>(data));
        }
    }

    Y_UNIT_TEST(PKDescScan) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TStreamExecScanQuerySettings scanSettings;
        scanSettings.Explain(true);

        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        TLocalHelper(kikimr).CreateTestOlapTable();
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 128);

        auto tableClient = kikimr.GetTableClient();
        auto selectQueryWithSort = TString(R"(
            --!syntax_v1
            SELECT `timestamp` FROM `/Root/olapStore/olapTable` ORDER BY `timestamp` DESC LIMIT 4;
        )");
        auto selectQuery = TString(R"(
            --!syntax_v1
            SELECT `timestamp` FROM `/Root/olapStore/olapTable` ORDER BY `timestamp` LIMIT 4;
        )");

        auto it = tableClient.StreamExecuteScanQuery(selectQuery, scanSettings).GetValueSync();
        auto result = CollectStreamResult(it);

        NJson::TJsonValue plan, node, reverse, limit, pushedLimit;
        NJson::ReadJsonTree(*result.PlanJson, &plan, true);
        Cerr << *result.PlanJson << Endl;
        Cerr << result.QueryStats->query_plan() << Endl;
        Cerr << result.QueryStats->query_ast() << Endl;

        node = FindPlanNodeByKv(plan, "Node Type", "TopSort-TableFullScan");
        UNIT_ASSERT(node.IsDefined());
        reverse = FindPlanNodeByKv(node, "Reverse", "false");
        UNIT_ASSERT(!reverse.IsDefined());
        pushedLimit = FindPlanNodeByKv(node, "ReadLimit", "4");
        UNIT_ASSERT(pushedLimit.IsDefined());
        limit = FindPlanNodeByKv(node, "Limit", "4");
        UNIT_ASSERT(limit.IsDefined());

        // Check that Reverse flag is set in query plan
        it = tableClient.StreamExecuteScanQuery(selectQueryWithSort, scanSettings).GetValueSync();
        result = CollectStreamResult(it);

        NJson::ReadJsonTree(*result.PlanJson, &plan, true);
        Cerr << "==============================" << Endl;
        Cerr << *result.PlanJson << Endl;
        Cerr << result.QueryStats->query_plan() << Endl;
        Cerr << result.QueryStats->query_ast() << Endl;

        node = FindPlanNodeByKv(plan, "Node Type", "TopSort-TableFullScan");
        UNIT_ASSERT(node.IsDefined());
        reverse = FindPlanNodeByKv(node, "Reverse", "true");
        UNIT_ASSERT(reverse.IsDefined());
        limit = FindPlanNodeByKv(node, "Limit", "4");
        UNIT_ASSERT(limit.IsDefined());
        pushedLimit = FindPlanNodeByKv(node, "ReadLimit", "4");
        UNIT_ASSERT(pushedLimit.IsDefined());

        // Run actual request in case explain did not execute anything
        it = tableClient.StreamExecuteScanQuery(selectQueryWithSort).GetValueSync();

        UNIT_ASSERT(it.IsSuccess());

        auto ysonResult = CollectStreamResult(it).ResultSetYson;

        auto expectedYson = TString(R"([
            [1000127u];
            [1000126u];
            [1000125u];
            [1000124u]
        ])");

        CompareYson(expectedYson, ysonResult);
    }

    Y_UNIT_TEST(CheckEarlyFilterOnEmptySelect) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        ui32 rowsCount = 0;
        {
            ui32 i = 0;
            const ui32 rowsPack = 20;
            const TInstant start = Now();
            while (!csController->HasCompactions() && Now() - start < TDuration::Seconds(100)) {
                WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000 + i * rowsPack, rowsPack);
                ++i;
                rowsCount += rowsPack;
            }
        }
        Sleep(TDuration::Seconds(10));
        auto tableClient = kikimr.GetTableClient();
        auto selectQuery = TString(R"(
            SELECT * FROM `/Root/olapStore/olapTable`
            WHERE uid='dsfdfsd'
            LIMIT 10;
        )");

        auto rows = ExecuteScanQuery(tableClient, selectQuery);
        Cerr << csController->GetFilteredRecordsCount().Val() << Endl;
        Y_ABORT_UNLESS(csController->GetFilteredRecordsCount().Val() * 10 <= rowsCount);
        UNIT_ASSERT(rows.size() == 0);
    }

    Y_UNIT_TEST(ExtractRanges) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 2000);

        auto tableClient = kikimr.GetTableClient();
        auto selectQuery = TString(R"(
            SELECT `timestamp` FROM `/Root/olapStore/olapTable`
                WHERE
                    (`timestamp` < CAST(1000100 AS Timestamp) AND `timestamp` > CAST(1000095 AS Timestamp)) OR
                    (`timestamp` <= CAST(1001000 AS Timestamp) AND `timestamp` >= CAST(1000999 AS Timestamp)) OR
                    (`timestamp` > CAST(1002000 AS Timestamp))
                ORDER BY `timestamp`
                LIMIT 1000;
        )");

        auto rows = ExecuteScanQuery(tableClient, selectQuery);

        TInstant tsPrev = TInstant::MicroSeconds(1000000);

        std::set<ui64> results = { 1000096, 1000097, 1000098, 1000099, 1000999, 1001000 };
        for (const auto& r : rows) {
            TInstant ts = GetTimestamp(r.at("timestamp"));
            UNIT_ASSERT_GE_C(ts, tsPrev, "result is not sorted in ASC order");
            UNIT_ASSERT(results.erase(ts.GetValue()));
            tsPrev = ts;
        }
        UNIT_ASSERT(rows.size() == 6);
    }

    Y_UNIT_TEST(ExtractRangesReverse) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 2000);

        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto tableClient = kikimr.GetTableClient();
        auto selectQuery = TString(R"(
            SELECT `timestamp` FROM `/Root/olapStore/olapTable`
                WHERE
                    (`timestamp` < CAST(1000100 AS Timestamp) AND `timestamp` > CAST(1000095 AS Timestamp)) OR
                    (`timestamp` < CAST(1000300 AS Timestamp) AND `timestamp` >= CAST(1000295 AS Timestamp)) OR
                    (`timestamp` <= CAST(1000400 AS Timestamp) AND `timestamp` > CAST(1000395 AS Timestamp)) OR

                    (`timestamp` <= CAST(1000500 AS Timestamp) AND `timestamp` >= CAST(1000495 AS Timestamp)) OR
                    (`timestamp` <= CAST(1000505 AS Timestamp) AND `timestamp` >= CAST(1000499 AS Timestamp)) OR
                    (`timestamp` < CAST(1000510 AS Timestamp) AND `timestamp` >= CAST(1000505 AS Timestamp)) OR

                    (`timestamp` <= CAST(1001000 AS Timestamp) AND `timestamp` >= CAST(1000999 AS Timestamp)) OR
                    (`timestamp` > CAST(1002000 AS Timestamp))
                ORDER BY `timestamp` DESC
                LIMIT 1000;
        )");

        auto rows = ExecuteScanQuery(tableClient, selectQuery);

        TInstant tsPrev = TInstant::MicroSeconds(2000000);
        std::set<ui64> results = { 1000096, 1000097, 1000098, 1000099,
            1000999, 1001000,
            1000295, 1000296, 1000297, 1000298, 1000299,
            1000396, 1000397, 1000398, 1000399, 1000400,
            1000495, 1000496, 1000497, 1000498, 1000499, 1000500, 1000501, 1000502, 1000503, 1000504, 1000505, 1000506, 1000507, 1000508, 1000509 };
        const ui32 expectedCount = results.size();
        for (const auto& r : rows) {
            TInstant ts = GetTimestamp(r.at("timestamp"));
            UNIT_ASSERT_LE_C(ts, tsPrev, "result is not sorted in DESC order");
            UNIT_ASSERT(results.erase(ts.GetValue()));
            tsPrev = ts;
        }
        UNIT_ASSERT(rows.size() == expectedCount);
    }

    TString BuildQuery(const TString& predicate, bool pushEnabled) {
        TStringBuilder qBuilder;
        qBuilder << "--!syntax_v1" << Endl;
        qBuilder << "PRAGMA Kikimr.OptEnableOlapPushdown = '" << (pushEnabled ? "true" : "false") << "';" << Endl;
        qBuilder << "SELECT `timestamp` FROM `/Root/olapStore/olapTable` WHERE ";
        qBuilder << predicate;
        qBuilder << " ORDER BY `timestamp`";
        return qBuilder;
    };

    Y_UNIT_TEST(PredicatePushdown) {
        constexpr bool logQueries = false;
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TStreamExecScanQuerySettings scanSettings;
        scanSettings.Explain(true);

        TLocalHelper(kikimr).CreateTestOlapTable();
#if SSA_RUNTIME_VERSION >= 4U
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000000, 5, true);
#else
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000000, 5, false);
#endif
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto tableClient = kikimr.GetTableClient();

        // TODO: Add support for DqPhyPrecompute push-down: Cast((2+2) as Uint64)
        std::vector<TString> testData = {
            R"(`resource_id` = `uid`)",
            R"(`resource_id` != `uid`)",
            R"(`resource_id` = "10001")",
            R"(`resource_id` != "10001")",
            R"(`level` = 1)",
            R"(`level` = Int8("1"))",
            R"(`level` = Int16("1"))",
            R"(`level` = Int32("1"))",
            R"((`level`, `uid`, `resource_id`) = (Int32("1"), "uid_3000001", "10001"))",
            R"(`level` > Int32("3"))",
            R"(`level` < Int32("1"))",
            R"(`level` >= Int32("4"))",
            R"(`level` <= Int32("0"))",
            R"(`level` != Int32("0"))",
            R"((`level`, `uid`, `resource_id`) > (Int32("1"), "uid_3000001", "10001"))",
            R"((`level`, `uid`, `resource_id`) > (Int32("1"), "uid_3000000", "10001"))",
            R"((`level`, `uid`, `resource_id`) < (Int32("1"), "uid_3000002", "10001"))",
            R"((`level`, `uid`, `resource_id`) >= (Int32("2"), "uid_3000001", "10001"))",
            R"((`level`, `uid`, `resource_id`) >= (Int32("1"), "uid_3000002", "10001"))",
            R"((`level`, `uid`, `resource_id`) >= (Int32("1"), "uid_3000001", "10002"))",
            R"((`level`, `uid`, `resource_id`) >= (Int32("1"), "uid_3000001", "10001"))",
            R"((`level`, `uid`, `resource_id`) <= (Int32("2"), "uid_3000001", "10001"))",
            R"((`level`, `uid`, `resource_id`) <= (Int32("1"), "uid_3000002", "10001"))",
            R"((`level`, `uid`, `resource_id`) <= (Int32("1"), "uid_3000001", "10002"))",
            R"((`level`, `uid`, `resource_id`) <= (Int32("1"), "uid_3000001", "10001"))",
            R"((`level`, `uid`, `resource_id`) != (Int32("1"), "uid_3000001", "10001"))",
            R"((`level`, `uid`, `resource_id`) != (Int32("0"), "uid_3000001", "10011"))",
            R"(`level` = 0 OR `level` = 2 OR `level` = 1)",
            R"(`level` = 0 OR (`level` = 2 AND `uid` = "uid_3000002"))",
            R"(`level` = 0 OR NOT(`level` = 2 AND `uid` = "uid_3000002"))",
            R"(`level` = 0 AND (`uid` = "uid_3000000" OR `uid` = "uid_3000002"))",
            R"(`level` = 0 AND NOT(`uid` = "uid_3000000" OR `uid` = "uid_3000002"))",
            R"(`level` = 0 OR `uid` = "uid_3000003")",
            R"(`level` = 0 AND `uid` = "uid_3000003")",
            R"(`level` = 0 AND `uid` = "uid_3000000")",
            R"((`level`, `uid`) > (Int32("2"), "uid_3000004") OR (`level`, `uid`) < (Int32("1"), "uid_3000002"))",
            R"(Int32("3") > `level`)",
            R"((Int32("1"), "uid_3000001", "10001") = (`level`, `uid`, `resource_id`))",
            R"((Int32("1"), `uid`, "10001") = (`level`, "uid_3000001", `resource_id`))",
            R"(`level` = 0 AND "uid_3000000" = `uid`)",
            R"(`uid` > `resource_id`)",
            R"(`level` IS NULL)",
            R"(`level` IS NOT NULL)",
            R"(`message` IS NULL)",
            R"(`message` IS NOT NULL)",
            R"((`level`, `uid`) > (Int32("1"), NULL))",
            R"((`level`, `uid`) != (Int32("1"), NULL))",
            R"(`level` >= CAST("2" As Int32))",
            R"(CAST("2" As Int32) >= `level`)",
#if SSA_RUNTIME_VERSION >= 2U
            R"(`uid` LIKE "%30000%")",
            R"(`uid` LIKE "uid%")",
            R"(`uid` LIKE "%001")",
            R"(`uid` LIKE "uid%001")",
#endif
#if SSA_RUNTIME_VERSION >= 4U
            R"(`level` + 2 < 5)",
            R"(`level` - 2 >= 1)",
            R"(`level` * 3 > 4)",
            R"(`level` / 2 <= 1)",
            R"(`level` % 3 != 1)",
            R"(-`level` < -2)",
            R"(Abs(`level` - 3) >= 1)",
            R"(LENGTH(`message`) > 1037)",
            R"(LENGTH(`uid`) > 1 OR `resource_id` = "10001")",
            R"((LENGTH(`uid`) > 2 AND `resource_id` = "10001") OR `resource_id` = "10002")",
            R"((LENGTH(`uid`) > 3 OR `resource_id` = "10002") AND (LENGTH(`uid`) < 15 OR `resource_id` = "10001"))",
            R"(NOT(LENGTH(`uid`) > 0 AND `resource_id` = "10001"))",
            R"(NOT(LENGTH(`uid`) > 0 OR `resource_id` = "10001"))",
            R"(`level` IS NULL OR `message` IS NULL)",
            R"(`level` IS NOT NULL AND `message` IS NULL)",
            R"(`level` IS NULL AND `message` IS NOT NULL)",
            R"(`level` IS NOT NULL AND `message` IS NOT NULL)",
            R"(`level` IS NULL XOR `message` IS NOT NULL)",
            R"(`level` IS NULL XOR `message` IS NULL)",
            R"(`level` + 2. < 5.f)",
            R"(`level` - 2.f >= 1.)",
            R"(`level` * 3. > 4.f)",
            R"(`level` / 2.f <= 1.)",
            R"(`level` % 3. != 1.f)",
            //R"(`timestamp` >= Timestamp("1970-01-01T00:00:00.000001Z"))",
            R"(`timestamp` >= Timestamp("1970-01-01T00:00:00.000001Z") AND `level` > 3)",
            R"((`timestamp`, `level`) >= (Timestamp("1970-01-01T00:00:00.000001Z"), 3))",
#endif
        };

        for (const auto& predicate: testData) {
            auto normalQuery = BuildQuery(predicate, false);
            auto pushQuery = BuildQuery(predicate, true);

            Cerr << "--- Run normal query ---\n";
            Cerr << normalQuery << Endl;
            auto it = tableClient.StreamExecuteScanQuery(normalQuery).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            auto goodResult = CollectStreamResult(it);

            Cerr << "--- Run pushed down query ---\n";
            Cerr << pushQuery << Endl;
            it = tableClient.StreamExecuteScanQuery(pushQuery).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            auto pushResult = CollectStreamResult(it);

            if (logQueries) {
                Cerr << "Query: " << normalQuery << Endl;
                Cerr << "Expected: " << goodResult.ResultSetYson << Endl;
                Cerr << "Received: " << pushResult.ResultSetYson << Endl;
            }

            CompareYson(goodResult.ResultSetYson, pushResult.ResultSetYson);

            it = tableClient.StreamExecuteScanQuery(pushQuery, scanSettings).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            auto result = CollectStreamResult(it);
            auto ast = result.QueryStats->Getquery_ast();

            UNIT_ASSERT_C(ast.find("KqpOlapFilter") != std::string::npos,
                          TStringBuilder() << "Predicate not pushed down. Query: " << pushQuery);
        }
    }

    Y_UNIT_TEST(PredicateDoNotPushdown) {
        constexpr bool logQueries = false;
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TStreamExecScanQuerySettings scanSettings;
        scanSettings.Explain(true);

        TLocalHelper(kikimr).CreateTestOlapTable();
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000000, 5);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto tableClient = kikimr.GetTableClient();

        std::vector<TString> testDataNoPush = {
            R"(`level` != NULL)",
            R"(`level` > NULL)",
            R"(`timestamp` >= CAST(3000001U AS Timestamp))",
            R"(`level` >= CAST("2" As Uint32))",
            R"(`level` = NULL)",
            R"(`level` > NULL)",
#if SSA_RUNTIME_VERSION < 2U
            R"(`uid` LIKE "%30000%")",
            R"(`uid` LIKE "uid%")",
            R"(`uid` LIKE "%001")",
            R"(`uid` LIKE "uid%001")",
#endif
#if SSA_RUNTIME_VERSION < 4U
            R"(`level` * 3.14 > 4)",
            R"(LENGTH(`uid`) > 0 OR `resource_id` = "10001")",
            R"((LENGTH(`uid`) > 0 AND `resource_id` = "10001") OR `resource_id` = "10002")",
            R"((LENGTH(`uid`) > 0 OR `resource_id` = "10002") AND (LENGTH(`uid`) < 15 OR `resource_id` = "10001"))",
            R"(NOT(LENGTH(`uid`) > 0 AND `resource_id` = "10001"))",
            R"(Unwrap(`level`/1) = `level` AND `resource_id` = "10001")",
            R"(NOT(LENGTH(`uid`) > 0 OR `resource_id` = "10001"))",
            R"(`level` + 2 < 5)",
            R"(`level` - 2 >= 1)",
            R"(`level` * 3 > 4)",
            R"(`level` / 2 <= 1)",
            R"(`level` % 3 != 1)",
            R"(`timestamp` >= Timestamp("1970-01-01T00:00:00.000001Z"))",
#endif
        };

        for (const auto& predicate: testDataNoPush) {
            auto pushQuery = BuildQuery(predicate, true);

            if (logQueries) {
                Cerr << "Query: " << pushQuery << Endl;
            }

            auto it = tableClient.StreamExecuteScanQuery(pushQuery, scanSettings).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            auto result = CollectStreamResult(it);
            auto ast = result.QueryStats->Getquery_ast();

            UNIT_ASSERT_C(ast.find("KqpOlapFilter") == std::string::npos,
                          TStringBuilder() << "Predicate pushed down. Query: " << pushQuery);
        }
    }

    Y_UNIT_TEST(PredicatePushdownPartial) {
        constexpr bool logQueries = false;
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TStreamExecScanQuerySettings scanSettings;
        scanSettings.Explain(true);

        TLocalHelper(kikimr).CreateTestOlapTable();
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000000, 5);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto tableClient = kikimr.GetTableClient();

        std::vector<TString> testDataPartialPush = {
            R"(LENGTH(`uid`) > 0 AND `resource_id` = "10001")",
            R"(`resource_id` = "10001" AND `level` > 1 AND LENGTH(`uid`) > 0)",
            R"(`resource_id` >= "10001" AND LENGTH(`uid`) > 0 AND `level` >= 1 AND `level` < 3)",
            R"(LENGTH(`uid`) > 0 AND (`resource_id` >= "10001" OR `level`>= 1 AND `level` <= 3))",
            R"(NOT(`resource_id` = "10001" OR `level` >= 1) AND LENGTH(`uid`) > 0)",
            R"(NOT(`resource_id` = "10001" AND `level` != 1) AND LENGTH(`uid`) > 0)",
            R"(`resource_id` = "10001" AND Unwrap(`level`/1) = `level`)",
            R"(`resource_id` = "10001" AND Unwrap(`level`/1) = `level` AND `level` > 1)",
        };

        for (const auto& predicate: testDataPartialPush) {
            auto normalQuery = BuildQuery(predicate, false);
            auto pushQuery = BuildQuery(predicate, true);

            Cerr << "--- Run normal query ---\n";
            Cerr << normalQuery << Endl;
            auto it = tableClient.StreamExecuteScanQuery(normalQuery).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            auto goodResult = CollectStreamResult(it);

            Cerr << "--- Run pushed down query ---\n";
            Cerr << pushQuery << Endl;
            it = tableClient.StreamExecuteScanQuery(pushQuery).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            auto pushResult = CollectStreamResult(it);

            if (logQueries) {
                Cerr << "Query: " << normalQuery << Endl;
                Cerr << "Expected: " << goodResult.ResultSetYson << Endl;
                Cerr << "Received: " << pushResult.ResultSetYson << Endl;
            }

            CompareYson(goodResult.ResultSetYson, pushResult.ResultSetYson);

            it = tableClient.StreamExecuteScanQuery(pushQuery, scanSettings).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            auto result = CollectStreamResult(it);
            auto ast = result.QueryStats->Getquery_ast();

            UNIT_ASSERT_C(ast.find("KqpOlapFilter") != std::string::npos,
                          TStringBuilder() << "Predicate not pushed down. Query: " << pushQuery);
            UNIT_ASSERT_C(ast.find("NarrowMap") != std::string::npos,
                          TStringBuilder() << "NarrowMap was removed. Query: " << pushQuery);
        }
    }
#if SSA_RUNTIME_VERSION >= 2U
    Y_UNIT_TEST(PredicatePushdown_DifferentLvlOfFilters) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TStreamExecScanQuerySettings scanSettings;
        scanSettings.Explain(true);

        TLocalHelper(kikimr).CreateTestOlapTable();
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000000, 5);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto tableClient = kikimr.GetTableClient();

        std::vector< std::pair<TString, TString> > secondLvlFilters = {
            { R"(`uid` LIKE "%30000%")", "TableFullScan" },
            { R"(`uid` NOT LIKE "%30000%")", "TableFullScan" },
            { R"(`uid` LIKE "uid%")", "TableFullScan" },
            { R"(`uid` LIKE "%001")", "TableFullScan" },
#if SSA_RUNTIME_VERSION >= 4U
            { R"(`uid` LIKE "uid%001")", "TableFullScan" },
#else
            { R"(`uid` LIKE "uid%001")", "Filter-TableFullScan" }, // We have filter (Size >= 6)
#endif
        };
        std::string query = R"(
            SELECT `timestamp` FROM `/Root/olapStore/olapTable` WHERE
                `level` >= 1 AND
        )";

        for (auto filter : secondLvlFilters) {
            auto it = tableClient.StreamExecuteScanQuery(query + filter.first, scanSettings).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            auto result = CollectStreamResult(it);
            NJson::TJsonValue plan;
            NJson::ReadJsonTree(*result.PlanJson, &plan, true);

            auto readNode = FindPlanNodeByKv(plan, "Node Type", filter.second);
            UNIT_ASSERT(readNode.IsDefined());

            auto& operators = readNode.GetMapSafe().at("Operators").GetArraySafe();
            for (auto& op : operators) {
                if (op.GetMapSafe().at("Name") == "TableFullScan") {
                    UNIT_ASSERT(op.GetMapSafe().at("SsaProgram").IsDefined());
                    const auto ssa = op.GetMapSafe().at("SsaProgram").GetStringRobust();
                    UNIT_ASSERT(ssa.Contains(R"("Filter":{)"));
                }
            }
        }
    }
#endif

#if SSA_RUNTIME_VERSION >= 3U
    Y_UNIT_TEST(PredicatePushdown_LikePushedDownForStringType) {
#else
    Y_UNIT_TEST(PredicatePushdown_LikeNotPushedDownForStringType) {
#endif
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TStreamExecScanQuerySettings scanSettings;
        scanSettings.Explain(true);

        TTableWithNullsHelper(kikimr).CreateTableWithNulls();
        WriteTestDataForTableWithNulls(kikimr, "/Root/tableWithNulls");
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto tableClient = kikimr.GetTableClient();
        auto query = R"(SELECT id, binary_str FROM `/Root/tableWithNulls` WHERE binary_str LIKE "5%")";
        auto it = tableClient.StreamExecuteScanQuery(query, scanSettings).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        auto result = CollectStreamResult(it);
        auto ast = result.QueryStats->Getquery_ast();
#if SSA_RUNTIME_VERSION >= 3U
        UNIT_ASSERT_C(ast.find("KqpOlapFilter") != std::string::npos,
                        TStringBuilder() << "Predicate wasn't pushed down. Query: " << query);
#else
        UNIT_ASSERT_C(ast.find("KqpOlapFilter") == std::string::npos,
                        TStringBuilder() << "Predicate was pushed down. Query: " << query);
#endif
    }

    Y_UNIT_TEST(PredicatePushdown_LikeNotPushedDownIfAnsiLikeDisabled) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TStreamExecScanQuerySettings scanSettings;
        scanSettings.Explain(true);

        TTableWithNullsHelper(kikimr).CreateTableWithNulls();
        WriteTestDataForTableWithNulls(kikimr, "/Root/tableWithNulls");
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto tableClient = kikimr.GetTableClient();
        auto query = R"(
            PRAGMA DisableAnsiLike;
            SELECT id, resource_id FROM `/Root/tableWithNulls` WHERE resource_id LIKE "%5%"
        )";
        auto it = tableClient.StreamExecuteScanQuery(query, scanSettings).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        auto result = CollectStreamResult(it);
        auto ast = result.QueryStats->Getquery_ast();
        UNIT_ASSERT_C(ast.find("KqpOlapFilter") == std::string::npos,
                        TStringBuilder() << "Predicate pushed down. Query: " << query);
    }

    Y_UNIT_TEST(PredicatePushdown_MixStrictAndNotStrict) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TStreamExecScanQuerySettings scanSettings;
        scanSettings.Explain(true);

        TLocalHelper(kikimr).CreateTestOlapTable();
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000000, 5);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto tableClient = kikimr.GetTableClient();
        auto query = R"(
            SELECT `timestamp` FROM `/Root/olapStore/olapTable` WHERE
                `resource_id` = "10001" AND Unwrap(`level`/1) = `level` AND `level` > 1;
        )";

        auto it = tableClient.StreamExecuteScanQuery(query, scanSettings).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        auto result = CollectStreamResult(it);
        auto ast = result.QueryStats->Getquery_ast();
        UNIT_ASSERT_C(ast.find(R"(('eq '"resource_id")") != std::string::npos,
                          TStringBuilder() << "Predicate not pushed down. Query: " << query);
        UNIT_ASSERT_C(ast.find(R"(('gt '"level")") == std::string::npos,
                          TStringBuilder() << "Predicate pushed down. Query: " << query);
        UNIT_ASSERT_C(ast.find("NarrowMap") != std::string::npos,
                          TStringBuilder() << "NarrowMap was removed. Query: " << query);
    }

    Y_UNIT_TEST(AggregationCountPushdown) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetForceColumnTablesCompositeMarks(true);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();

        {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 11000, 3001000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 12000, 3002000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 13000, 3003000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 14000, 3004000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 20000, 2000000, 7000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 30000, 1000000, 11000);
        }
        while (csController->GetIndexations().Val() == 0) {
            Cout << "Wait indexation..." << Endl;
            Sleep(TDuration::Seconds(2));
        }
        AFL_VERIFY(Singleton<NWrappers::NExternalStorage::TFakeExternalStorage>()->GetSize());

        {
            TString query = R"(
                --!syntax_v1
                SELECT
                    COUNT(level)
                FROM `/Root/olapStore/olapTable`
            )";
            auto opStartTime = Now();
            auto it = tableClient.StreamExecuteScanQuery(query).GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cerr << "!!!\nPushdown query execution time: " << (Now() - opStartTime).MilliSeconds() << "\n!!!\n";
            Cout << result << Endl;
            CompareYson(result, R"([[23000u;]])");

            // Check plan
#if SSA_RUNTIME_VERSION >= 2U
            CheckPlanForAggregatePushdown(query, tableClient, { "TKqpOlapAgg" }, "TableFullScan");
#else
            CheckPlanForAggregatePushdown(query, tableClient, { "CombineCore" }, "");
#endif
        }
    }

    Y_UNIT_TEST(AggregationCountGroupByPushdown) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetForceColumnTablesCompositeMarks(true);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();

        {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 11000, 3001000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 12000, 3002000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 13000, 3003000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 14000, 3004000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 20000, 2000000, 7000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 30000, 1000000, 11000);
        }

        {
            TString query = R"(
                --!syntax_v1
                PRAGMA Kikimr.OptUseFinalizeByKey;
                SELECT
                    level, COUNT(level)
                FROM `/Root/olapStore/olapTable`
                GROUP BY level
                ORDER BY level
            )";
            auto it = tableClient.StreamExecuteScanQuery(query).GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;
            CompareYson(result, R"([[[0];4600u];[[1];4600u];[[2];4600u];[[3];4600u];[[4];4600u]])");

            // Check plan
#if SSA_RUNTIME_VERSION >= 2U
            CheckPlanForAggregatePushdown(query, tableClient, { "WideCombiner" }, "Aggregate-TableFullScan");
//            CheckPlanForAggregatePushdown(query, tableClient, { "TKqpOlapAgg" }, "TableFullScan");
#else
            CheckPlanForAggregatePushdown(query, tableClient, { "CombineCore" }, "");
#endif
        }
    }

    Y_UNIT_TEST_TWIN(CountAllPushdown, UseLlvm) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetForceColumnTablesCompositeMarks(true);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();

        {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 11000, 3001000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 12000, 3002000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 13000, 3003000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 14000, 3004000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 20000, 2000000, 7000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 30000, 1000000, 11000);
        }

        {
            TString query = fmt::format(R"(
                --!syntax_v1
                PRAGMA ydb.UseLlvm = "{}";

                SELECT
                    COUNT(*)
                FROM `/Root/olapStore/olapTable`
            )", UseLlvm ? "true" : "false");
            auto it = tableClient.StreamExecuteScanQuery(query).GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;
            CompareYson(result, R"([[23000u;]])");

            // Check plan
#if SSA_RUNTIME_VERSION >= 2U
            CheckPlanForAggregatePushdown(query, tableClient, { "TKqpOlapAgg" }, "TableFullScan");
#else
            CheckPlanForAggregatePushdown(query, tableClient, { "Condense" }, "");
#endif
        }
    }

    Y_UNIT_TEST_TWIN(CountAllPushdownBackwardCompatibility, EnableLlvm) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetForceColumnTablesCompositeMarks(true);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();

        {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 11000, 3001000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 12000, 3002000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 13000, 3003000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 14000, 3004000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 20000, 2000000, 7000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 30000, 1000000, 11000);
        }

        {
            TString query = fmt::format(R"(
                --!syntax_v1
                PRAGMA Kikimr.EnableLlvm = "{}";

                SELECT
                    COUNT(*)
                FROM `/Root/olapStore/olapTable`
            )", EnableLlvm ? "true" : "false");
            auto it = tableClient.StreamExecuteScanQuery(query).GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;
            CompareYson(result, R"([[23000u;]])");

            // Check plan
#if SSA_RUNTIME_VERSION >= 2U
            CheckPlanForAggregatePushdown(query, tableClient, { "TKqpOlapAgg" }, "TableFullScan");
#else
            CheckPlanForAggregatePushdown(query, tableClient, { "Condense" }, "");
#endif
        }
    }

    Y_UNIT_TEST(CountAllNoPushdown) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetForceColumnTablesCompositeMarks(true);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();

        {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 11000, 3001000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 12000, 3002000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 13000, 3003000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 14000, 3004000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 20000, 2000000, 7000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 30000, 1000000, 11000);
        }

        {
            auto it = tableClient.StreamExecuteScanQuery(R"(
                --!syntax_v1
                SELECT
                    COUNT(*)
                FROM `/Root/olapStore/olapTable`
            )").GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;
            CompareYson(result, R"([[23000u;]])");
        }
    }

    class TExpectedLimitChecker {
    private:
        std::optional<ui32> ExpectedLimit;
        std::optional<ui32> ExpectedResultCount;
        ui32 CheckScanData = 0;
        ui32 CheckScanTask = 0;
    public:
        TExpectedLimitChecker& SetExpectedLimit(const ui32 value) {
            ExpectedLimit = value;
            ExpectedResultCount = value;
            return *this;
        }
        TExpectedLimitChecker& SetExpectedResultCount(const ui32 value) {
            ExpectedResultCount = value;
            return *this;
        }
        bool CheckExpectedLimitOnScanData(const ui32 resultCount) {
            if (!ExpectedResultCount) {
                return true;
            }
            ++CheckScanData;
            UNIT_ASSERT_LE(resultCount, *ExpectedResultCount);
            return true;
        }
        bool CheckExpectedLimitOnScanTask(const ui32 taskLimit) {
            if (!ExpectedLimit) {
                return true;
            }
            ++CheckScanTask;
            UNIT_ASSERT_EQUAL(taskLimit, *ExpectedLimit);
            return true;
        }
        bool CheckFinish() const {
            if (!ExpectedLimit) {
                return true;
            }
            return CheckScanData && CheckScanTask;
        }
    };

    class TExpectedRecordChecker {
    private:
        std::optional<ui32> ExpectedColumnsCount;
        ui32 CheckScanData = 0;
    public:
        TExpectedRecordChecker& SetExpectedColumnsCount(const ui32 value) {
            ExpectedColumnsCount = value;
            return *this;
        }
        bool CheckExpectedOnScanData(const ui32 columnsCount) {
            if (!ExpectedColumnsCount) {
                return true;
            }
            ++CheckScanData;
            UNIT_ASSERT_EQUAL(columnsCount, *ExpectedColumnsCount);
            return true;
        }
        bool CheckFinish() const {
            if (!ExpectedColumnsCount) {
                return true;
            }
            return CheckScanData;
        }
    };

    class TAggregationTestCase {
    private:
        TString Query;
        TString ExpectedReply;
        std::vector<std::string> ExpectedPlanOptions;
        bool Pushdown = true;
        std::string ExpectedReadNodeType;
        TExpectedLimitChecker LimitChecker;
        TExpectedRecordChecker RecordChecker;
        bool UseLlvm = true;
    public:
        void FillExpectedAggregationGroupByPlanOptions() {
#if SSA_RUNTIME_VERSION >= 2U
//            AddExpectedPlanOptions("TKqpOlapAgg");
            AddExpectedPlanOptions("WideCombiner");
#else
            AddExpectedPlanOptions("CombineCore");
#endif
        }
        TString GetFixedQuery() const {
            TStringBuilder queryFixed;
            queryFixed << "--!syntax_v1" << Endl;
            if (!Pushdown) {
                queryFixed << "PRAGMA Kikimr.OptEnableOlapPushdown = \"false\";" << Endl;
            }
            if (!UseLlvm) {
                queryFixed << "PRAGMA Kikimr.UseLlvm = \"false\";" << Endl;
            }
            queryFixed << "PRAGMA Kikimr.OptUseFinalizeByKey;" << Endl;

            queryFixed << Query << Endl;
            Cerr << "REQUEST:\n" << queryFixed << Endl;
            return queryFixed;
        }
        TAggregationTestCase() = default;
        TExpectedLimitChecker& MutableLimitChecker() {
            return LimitChecker;
        }
        TExpectedRecordChecker& MutableRecordChecker() {
            return RecordChecker;
        }
        bool GetPushdown() const {
            return Pushdown;
        }
        TAggregationTestCase& SetPushdown(const bool value = true) {
            Pushdown = value;
            return *this;
        }
        bool CheckFinished() const {
            return LimitChecker.CheckFinish();
        }

        const TString& GetQuery() const {
            return Query;
        }
        TAggregationTestCase& SetQuery(const TString& value) {
            Query = value;
            return *this;
        }
        TAggregationTestCase& SetUseLlvm(const bool value) {
            UseLlvm = value;
            return *this;
        }
        const TString& GetExpectedReply() const {
            return ExpectedReply;
        }
        TAggregationTestCase& SetExpectedReply(const TString& value) {
            ExpectedReply = value;
            return *this;
        }

        TAggregationTestCase& AddExpectedPlanOptions(const std::string& value) {
            ExpectedPlanOptions.emplace_back(value);
            return *this;
        }

        const std::vector<std::string>& GetExpectedPlanOptions() const {
            return ExpectedPlanOptions;
        }

        TAggregationTestCase& SetExpectedReadNodeType(const std::string& value) {
            ExpectedReadNodeType = value;
            return *this;
        }

        const std::string& GetExpectedReadNodeType() const {
            return ExpectedReadNodeType;
        }
    };

    void TestAggregationsBase(const std::vector<TAggregationTestCase>& cases) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetForceColumnTablesCompositeMarks(true);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable();
        auto tableClient = kikimr.GetTableClient();

        {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 11000, 3001000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 12000, 3002000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 13000, 3003000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 14000, 3004000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 20000, 2000000, 7000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 30000, 1000000, 11000);
        }

        for (auto&& i : cases) {
            const TString queryFixed = i.GetFixedQuery();
            {
                auto it = tableClient.StreamExecuteScanQuery(queryFixed).GetValueSync();
                UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
                TString result = StreamResultToYson(it);
                if (!i.GetExpectedReply().empty()) {
                    CompareYson(result, i.GetExpectedReply());
                }
            }
            CheckPlanForAggregatePushdown(queryFixed, tableClient, i.GetExpectedPlanOptions(), i.GetExpectedReadNodeType());
        }
    }

    void TestAggregationsInternal(const std::vector<TAggregationTestCase>& cases) {
        TPortManager tp;
        ui16 mbusport = tp.GetPort(2134);
        auto settings = Tests::TServerSettings(mbusport)
            .SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetNodeCount(2);

        Tests::TServer::TPtr server = new Tests::TServer(settings);

        auto runtime = server->GetRuntime();
        auto sender = runtime->AllocateEdgeActor();

        InitRoot(server, sender);
        Tests::NCommon::TLoggerInit(runtime).Initialize();

        ui32 numShards = 1;
        ui32 numIterations = 10;
        TLocalHelper(*server).CreateTestOlapTable("olapTable", "olapStore", numShards, numShards);
        const ui32 iterationPackSize = 2000;
        for (ui64 i = 0; i < numIterations; ++i) {
            TLocalHelper(*server).SendDataViaActorSystem("/Root/olapStore/olapTable", 0, 1000000 + i * 1000000, iterationPackSize);
        }

        TAggregationTestCase currentTest;
        auto captureEvents = [&](TAutoPtr<IEventHandle>& ev) -> auto {
            switch (ev->GetTypeRewrite()) {
                case NKqp::TKqpComputeEvents::EvScanData:
                {
                    auto* msg = ev->Get<NKqp::TEvKqpCompute::TEvScanData>();
                    Y_ABORT_UNLESS(currentTest.MutableLimitChecker().CheckExpectedLimitOnScanData(msg->ArrowBatch ? msg->ArrowBatch->num_rows() : 0));
                    Y_ABORT_UNLESS(currentTest.MutableRecordChecker().CheckExpectedOnScanData(msg->ArrowBatch ? msg->ArrowBatch->num_columns() : 0));
                    break;
                }
                case TEvDataShard::EvKqpScan:
                {
                    auto* msg = ev->Get<TEvDataShard::TEvKqpScan>();
                    Y_ABORT_UNLESS(currentTest.MutableLimitChecker().CheckExpectedLimitOnScanTask(msg->Record.GetItemsLimit()));
                    break;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime->SetObserverFunc(captureEvents);

        for (auto&& i : cases) {
            const TString queryFixed = i.GetFixedQuery();
            currentTest = i;
            auto streamSender = runtime->AllocateEdgeActor();
            SendRequest(*runtime, streamSender, MakeStreamRequest(streamSender, queryFixed, false));
            auto ev = runtime->GrabEdgeEventRethrow<NKqp::TEvKqpCompute::TEvScanData>(streamSender, TDuration::Seconds(10));
            Y_ABORT_UNLESS(currentTest.CheckFinished());
        }
    }

    void TestAggregations(const std::vector<TAggregationTestCase>& cases) {
        TestAggregationsBase(cases);
        TestAggregationsInternal(cases);
    }

    template <typename TClient>
    auto StreamExecuteQuery(const TAggregationTestCase& testCase, TClient& client) {
        if constexpr (std::is_same_v<NYdb::NTable::TTableClient, TClient>) {
            return client.StreamExecuteScanQuery(testCase.GetFixedQuery()).GetValueSync();
        } else {
            return client.StreamExecuteQuery(
                testCase.GetFixedQuery(),
                NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
        }
    }

    template <typename TClient>
    void RunTestCaseWithClient(const TAggregationTestCase& testCase, TClient& client) {
        auto it = StreamExecuteQuery(testCase, client);
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        TString result = StreamResultToYson(it);
        if (!testCase.GetExpectedReply().empty()) {
            CompareYson(result, testCase.GetExpectedReply());
        }
    }

    void TestClickBenchBase(const std::vector<TAggregationTestCase>& cases, const bool genericQuery) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetForceColumnTablesCompositeMarks(true);
        TKikimrRunner kikimr(settings);

        TClickHelper(kikimr).CreateClickBenchTable();
        auto tableClient = kikimr.GetTableClient();


        ui32 numIterations = 10;
        const ui32 iterationPackSize = NSan::PlainOrUnderSanitizer(2000, 20);
        for (ui64 i = 0; i < numIterations; ++i) {
            WriteTestDataForClickBench(kikimr, "/Root/benchTable", 0, 1000000 + i * 1000000, iterationPackSize);
        }

        if (!genericQuery) {
            auto tableClient = kikimr.GetTableClient();
            for (auto&& i : cases) {
                const TString queryFixed = i.GetFixedQuery();
                RunTestCaseWithClient(i, tableClient);
                CheckPlanForAggregatePushdown(queryFixed, tableClient, i.GetExpectedPlanOptions(), i.GetExpectedReadNodeType());
            }
        } else {
            auto queryClient = kikimr.GetQueryClient();
            for (auto&& i : cases) {
                const TString queryFixed = i.GetFixedQuery();
                RunTestCaseWithClient(i, queryClient);
                CheckPlanForAggregatePushdown(queryFixed, queryClient, i.GetExpectedPlanOptions(), i.GetExpectedReadNodeType());
            }
        }
    }

    void TestClickBenchInternal(const std::vector<TAggregationTestCase>& cases) {
        TPortManager tp;
        ui16 mbusport = tp.GetPort(2134);
        auto settings = Tests::TServerSettings(mbusport)
            .SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetNodeCount(2);

        Tests::TServer::TPtr server = new Tests::TServer(settings);

        auto runtime = server->GetRuntime();
        auto sender = runtime->AllocateEdgeActor();

        InitRoot(server, sender);

        TClickHelper(*server).CreateClickBenchTable();

        // write data

        ui32 numIterations = 10;
        const ui32 iterationPackSize = NSan::PlainOrUnderSanitizer(2000, 20);
        for (ui64 i = 0; i < numIterations; ++i) {
            TClickHelper(*server).SendDataViaActorSystem("/Root/benchTable", 0, 1000000 + i * 1000000,
                                                         iterationPackSize);
        }

        TAggregationTestCase currentTest;
        auto captureEvents = [&](TAutoPtr<IEventHandle>& ev) -> auto {
            switch (ev->GetTypeRewrite()) {
                case NKqp::TKqpComputeEvents::EvScanData:
                {
                    auto* msg = ev->Get<NKqp::TEvKqpCompute::TEvScanData>();
                    Y_ABORT_UNLESS(currentTest.MutableLimitChecker().CheckExpectedLimitOnScanData(msg->ArrowBatch ? msg->ArrowBatch->num_rows() : 0));
                    Y_ABORT_UNLESS(currentTest.MutableRecordChecker().CheckExpectedOnScanData(msg->ArrowBatch ? msg->ArrowBatch->num_columns() : 0));
                    break;
                }
                case TEvDataShard::EvKqpScan:
                {
                    auto* msg = ev->Get<TEvDataShard::TEvKqpScan>();
                    Y_ABORT_UNLESS(currentTest.MutableLimitChecker().CheckExpectedLimitOnScanTask(msg->Record.GetItemsLimit()));
                    break;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime->SetObserverFunc(captureEvents);

        // selects

        for (auto&& i : cases) {
            const TString queryFixed = i.GetFixedQuery();
            currentTest = i;
            auto streamSender = runtime->AllocateEdgeActor();
            SendRequest(*runtime, streamSender, MakeStreamRequest(streamSender, queryFixed, false));
            auto ev = runtime->GrabEdgeEventRethrow<NKqp::TEvKqpCompute::TEvScanData>(streamSender, TDuration::Seconds(10));
            Y_ABORT_UNLESS(currentTest.CheckFinished());
        }
    }

    void TestClickBench(const std::vector<TAggregationTestCase>& cases, const bool genericQuery = false) {
        TestClickBenchBase(cases, genericQuery);
        if (!genericQuery) {
            TestClickBenchInternal(cases);
        }
    }

    void TestTableWithNulls(const std::vector<TAggregationTestCase>& cases, const bool genericQuery = false) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetForceColumnTablesCompositeMarks(true);
        TKikimrRunner kikimr(settings);

        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        TTableWithNullsHelper(kikimr).CreateTableWithNulls();
        auto tableClient = kikimr.GetTableClient();

        {
            WriteTestDataForTableWithNulls(kikimr, "/Root/tableWithNulls");
        }

        if (!genericQuery) {
            auto tableClient = kikimr.GetTableClient();
            for (auto&& i : cases) {
                RunTestCaseWithClient(i, tableClient);
                CheckPlanForAggregatePushdown(i.GetFixedQuery(), tableClient,
                    i.GetExpectedPlanOptions(), i.GetExpectedReadNodeType());
            }
        } else {
            auto queryClient = kikimr.GetQueryClient();
            for (auto&& i : cases) {
                RunTestCaseWithClient(i, queryClient);
                CheckPlanForAggregatePushdown(i.GetFixedQuery(), queryClient,
                i.GetExpectedPlanOptions(), i.GetExpectedReadNodeType());
            }
        }
    }

    Y_UNIT_TEST(Filter_NotAllUsedFieldsInResultSet) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT id, resource_id FROM `/Root/tableWithNulls`
                WHERE
                    level = 5;
            )")
            .SetExpectedReply("[[5;#]]")
            .AddExpectedPlanOptions("KqpOlapFilter");

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_ResultDistinctCountRI_GroupByL) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    level, COUNT(DISTINCT resource_id)
                FROM `/Root/olapStore/olapTable`
                GROUP BY level
                ORDER BY level
            )")
            .SetExpectedReply("[[[0];4600u];[[1];4600u];[[2];4600u];[[3];4600u];[[4];4600u]]")
            ;
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_ResultCountAll_FilterL) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                    SELECT
                        COUNT(*)
                    FROM `/Root/olapStore/olapTable`
                    WHERE level = 2
                )")
            .SetExpectedReply("[[4600u;]]")
            .AddExpectedPlanOptions("KqpOlapFilter")
#if SSA_RUNTIME_VERSION >= 2U
            .AddExpectedPlanOptions("TKqpOlapAgg")
            .MutableLimitChecker().SetExpectedResultCount(1)
#else
            .AddExpectedPlanOptions("Condense")
#endif
            ;

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_ResultCountL_FilterL) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    COUNT(level)
                FROM `/Root/olapStore/olapTable`
                WHERE level = 2
            )")
            .SetExpectedReply("[[4600u;]]")
            .AddExpectedPlanOptions("KqpOlapFilter")
#if SSA_RUNTIME_VERSION >= 2U
            .AddExpectedPlanOptions("TKqpOlapAgg")
            .MutableLimitChecker().SetExpectedResultCount(1)
#else
            .AddExpectedPlanOptions("CombineCore")
#endif
            ;

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_ResultCountT_FilterL) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    COUNT(timestamp)
                FROM `/Root/olapStore/olapTable`
                WHERE level = 2
            )")
            .SetExpectedReply("[[4600u;]]")
            .AddExpectedPlanOptions("KqpOlapFilter")
#if SSA_RUNTIME_VERSION >= 2U
            .AddExpectedPlanOptions("TKqpOlapAgg")
            .MutableLimitChecker().SetExpectedResultCount(1)
#else
            .AddExpectedPlanOptions("CombineCore")
            .AddExpectedPlanOptions("KqpOlapFilter")
#endif
            ;

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_ResultTL_FilterL_Limit2) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    timestamp, level
                FROM `/Root/olapStore/olapTable`
                WHERE level = 2
                LIMIT 2
            )")
            .AddExpectedPlanOptions("KqpOlapFilter")
            .MutableLimitChecker().SetExpectedLimit(2);
        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_ResultTL_FilterL_OrderT_Limit2) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    timestamp, level
                FROM `/Root/olapStore/olapTable`
                WHERE level = 2
                ORDER BY timestamp
                LIMIT 2
            )")
            .AddExpectedPlanOptions("KqpOlapFilter")
            .MutableLimitChecker().SetExpectedLimit(2);

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_ResultT_FilterL_Limit2) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    timestamp
                FROM `/Root/olapStore/olapTable`
                WHERE level = 2
                LIMIT 2
            )")
            .AddExpectedPlanOptions("KqpOlapFilter")
            .AddExpectedPlanOptions("KqpOlapExtractMembers")
            .MutableLimitChecker().SetExpectedLimit(2);

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_ResultT_FilterL_OrderT_Limit2) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    timestamp
                FROM `/Root/olapStore/olapTable`
                WHERE level = 2
                ORDER BY timestamp
                LIMIT 2
            )")
            .AddExpectedPlanOptions("KqpOlapFilter")
            .AddExpectedPlanOptions("KqpOlapExtractMembers")
            .MutableLimitChecker().SetExpectedLimit(2);

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_ResultL_FilterL_OrderL_Limit2) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    timestamp, level
                FROM `/Root/olapStore/olapTable`
                WHERE level > 1
                ORDER BY level
                LIMIT 2
            )")
            .AddExpectedPlanOptions("KqpOlapFilter");

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_ResultCountExpr) {
        auto g = NColumnShard::TLimits::MaxBlobSizeGuard(10000);
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                    SELECT
                        COUNT(level + 2)
                    FROM `/Root/olapStore/olapTable`
                )")
            .SetExpectedReply("[[23000u;]]")
            .AddExpectedPlanOptions("Condense1");

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Count_Null) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    COUNT(level)
                FROM `/Root/tableWithNulls`
                WHERE id > 5;
            )")
            .SetExpectedReply("[[0u]]")
#if SSA_RUNTIME_VERSION >= 2U
            .AddExpectedPlanOptions("TKqpOlapAgg");
#else
            .AddExpectedPlanOptions("CombineCore");
#endif

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Count_NullMix) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    COUNT(level)
                FROM `/Root/tableWithNulls`;
            )")
            .SetExpectedReply("[[5u]]")
#if SSA_RUNTIME_VERSION >= 2U
            .AddExpectedPlanOptions("TKqpOlapAgg");
#else
            .AddExpectedPlanOptions("CombineCore");
#endif

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Count_GroupBy) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    id, COUNT(level)
                FROM `/Root/tableWithNulls`
                WHERE id BETWEEN 4 AND 5
                GROUP BY id
                ORDER BY id;
            )")
            .SetExpectedReply("[[4;1u];[5;1u]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Count_NullGroupBy) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    id, COUNT(level)
                FROM `/Root/tableWithNulls`
                WHERE id BETWEEN 6 AND 7
                GROUP BY id
                ORDER BY id;
            )")
            .SetExpectedReply("[[6;0u];[7;0u]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Count_NullMixGroupBy) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    id, COUNT(level)
                FROM `/Root/tableWithNulls`
                WHERE id > 4 AND id < 7
                GROUP BY id
                ORDER BY id;
            )")
            .SetExpectedReply("[[5;1u];[6;0u]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Count_GroupByNull) {
        // Wait for KIKIMR-16940 fix
        return;
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    level, COUNT(id), COUNT(level), COUNT(*)
                FROM `/Root/tableWithNulls`
                WHERE id > 5
                GROUP BY level
                ORDER BY level;
            )")
            .SetExpectedReply("[[#;5u;0u;5u]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Count_GroupByNullMix) {
        // Wait for KIKIMR-16940 fix
        return;
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    level, COUNT(id), COUNT(level), COUNT(*)
                FROM `/Root/tableWithNulls`
                WHERE id >= 5
                GROUP BY level
                ORDER BY level;
            )")
            .SetExpectedReply("[[#;5u;0u;5u];[[5];1u;1u;1u]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_NoPushdownOnDisabledEmitAggApply) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                    PRAGMA DisableEmitAggApply;
                    SELECT
                        COUNT(level)
                    FROM `/Root/olapStore/olapTable`
                )")
            .SetExpectedReply("[[23000u;]]")
            .AddExpectedPlanOptions("Condense1");

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(AggregationAndFilterPushdownOnDiffCols) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    COUNT(`timestamp`)
                FROM `/Root/olapStore/olapTable`
                WHERE level = 2
            )")
            .SetExpectedReply("[[4600u;]]")
#if SSA_RUNTIME_VERSION >= 2U
            .AddExpectedPlanOptions("TKqpOlapAgg")
#else
            .AddExpectedPlanOptions("CombineCore")
#endif
            .AddExpectedPlanOptions("KqpOlapFilter");

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Avg) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    AVG(level), MIN(level)
                FROM `/Root/olapStore/olapTable`
            )")
            .SetExpectedReply("[[[2.];[0]]]")
#if SSA_RUNTIME_VERSION >= 2U
            .AddExpectedPlanOptions("TKqpOlapAgg");
#else
            .AddExpectedPlanOptions("CombineCore");
#endif

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Avg_Null) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    AVG(level)
                FROM `/Root/tableWithNulls`
                WHERE id > 5;
            )")
            .SetExpectedReply("[[#]]")
#if SSA_RUNTIME_VERSION >= 2U
            .AddExpectedPlanOptions("TKqpOlapAgg");
#else
            .AddExpectedPlanOptions("CombineCore");
#endif

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Avg_NullMix) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    AVG(level)
                FROM `/Root/tableWithNulls`;
            )")
            .SetExpectedReply("[[[3.]]]")
#if SSA_RUNTIME_VERSION >= 2U
            .AddExpectedPlanOptions("TKqpOlapAgg");
#else
            .AddExpectedPlanOptions("CombineCore");
#endif

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Avg_GroupBy) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    id, AVG(level)
                FROM `/Root/tableWithNulls`
                WHERE id BETWEEN 4 AND 5
                GROUP BY id
                ORDER BY id;
            )")
            .SetExpectedReply("[[4;[4.]];[5;[5.]]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Avg_NullGroupBy) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    id, AVG(level)
                FROM `/Root/tableWithNulls`
                WHERE id BETWEEN 6 AND 7
                GROUP BY id
                ORDER BY id;
            )")
            .SetExpectedReply("[[6;#];[7;#]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Avg_NullMixGroupBy) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    id, AVG(level)
                FROM `/Root/tableWithNulls`
                WHERE id > 4 AND id < 7
                GROUP BY id
                ORDER BY id;
            )")
            .SetExpectedReply("[[5;[5.]];[6;#]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Avg_GroupByNull) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    level, AVG(id), AVG(level)
                FROM `/Root/tableWithNulls`
                WHERE id > 5
                GROUP BY level
                ORDER BY level;
            )")
            .SetExpectedReply("[[#;8.;#]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Avg_GroupByNullMix) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    level, AVG(id), AVG(level)
                FROM `/Root/tableWithNulls`
                WHERE id >= 5
                GROUP BY level
                ORDER BY level;
            )")
            .SetExpectedReply("[[#;8.;#];[[5];5.;[5.]]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Sum) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    SUM(level)
                FROM `/Root/olapStore/olapTable`
            )")
            .SetExpectedReply("[[[46000;]]]")
#if SSA_RUNTIME_VERSION >= 2U
            .AddExpectedPlanOptions("TKqpOlapAgg");
#else
            .AddExpectedPlanOptions("CombineCore");
#endif

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Sum_Null) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    SUM(level)
                FROM `/Root/tableWithNulls`
                WHERE id > 5;
            )")
            .SetExpectedReply("[[#]]")
#if SSA_RUNTIME_VERSION >= 2U
            .AddExpectedPlanOptions("TKqpOlapAgg");
#else
            .AddExpectedPlanOptions("CombineCore");
#endif

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Sum_NullMix) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    SUM(level)
                FROM `/Root/tableWithNulls`;
            )")
            .SetExpectedReply("[[[15]]]")
#if SSA_RUNTIME_VERSION >= 2U
            .AddExpectedPlanOptions("TKqpOlapAgg");
#else
            .AddExpectedPlanOptions("CombineCore");
#endif

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Sum_GroupBy) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    id, SUM(level)
                FROM `/Root/tableWithNulls`
                WHERE id BETWEEN 4 AND 5
                GROUP BY id
                ORDER BY id;
            )")
            .SetExpectedReply("[[4;[4]];[5;[5]]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Sum_NullGroupBy) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    id, SUM(level)
                FROM `/Root/tableWithNulls`
                WHERE id BETWEEN 6 AND 7
                GROUP BY id
                ORDER BY id;
            )")
            .SetExpectedReply("[[6;#];[7;#]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Sum_NullMixGroupBy) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    id, SUM(level)
                FROM `/Root/tableWithNulls`
                WHERE id > 4 AND id < 7
                GROUP BY id
                ORDER BY id;
            )")
            .SetExpectedReply("[[5;[5]];[6;#]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Sum_GroupByNull) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    level, SUM(id), SUM(level)
                FROM `/Root/tableWithNulls`
                WHERE id > 5
                GROUP BY level
                ORDER BY level;
            )")
            .SetExpectedReply("[[#;40;#]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Sum_GroupByNullMix) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    level, SUM(id), SUM(level)
                FROM `/Root/tableWithNulls`
                WHERE id >= 5
                GROUP BY level
                ORDER BY level;
            )")
            .SetExpectedReply("[[#;40;#];[[5];5;[5]]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_SumL_GroupL_OrderL) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    level, SUM(level)
                FROM `/Root/olapStore/olapTable`
                GROUP BY level
                ORDER BY level
            )")
            .SetExpectedReply("[[[0];[0]];[[1];[4600]];[[2];[9200]];[[3];[13800]];[[4];[18400]]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_MinL) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    MIN(level)
                FROM `/Root/olapStore/olapTable`
            )")
            .SetExpectedReply("[[[0]]]")
#if SSA_RUNTIME_VERSION >= 2U
            .AddExpectedPlanOptions("TKqpOlapAgg");
#else
            .AddExpectedPlanOptions("CombineCore");
#endif

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_MaxL) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    MAX(level)
                FROM `/Root/olapStore/olapTable`
            )")
            .SetExpectedReply("[[[4]]]")
#if SSA_RUNTIME_VERSION >= 2U
            .AddExpectedPlanOptions("TKqpOlapAgg");
#else
            .AddExpectedPlanOptions("CombineCore");
#endif

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_MinR_GroupL_OrderL) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    level, MIN(resource_id)
                FROM `/Root/olapStore/olapTable`
                GROUP BY level
                ORDER BY level
            )")
            .SetExpectedReply("[[[0];[\"10000\"]];[[1];[\"10001\"]];[[2];[\"10002\"]];[[3];[\"10003\"]];[[4];[\"10004\"]]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_MaxR_GroupL_OrderL) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    level, MAX(resource_id)
                FROM `/Root/olapStore/olapTable`
                GROUP BY level
                ORDER BY level
            )")
            .SetExpectedReply("[[[0];[\"40995\"]];[[1];[\"40996\"]];[[2];[\"40997\"]];[[3];[\"40998\"]];[[4];[\"40999\"]]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_ProjectionOrder) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    resource_id, level, count(*) as c
                FROM `/Root/olapStore/olapTable`
                GROUP BY resource_id, level
                ORDER BY c, resource_id DESC LIMIT 3
            )")
            .SetExpectedReply("[[[\"40999\"];[4];1u];[[\"40998\"];[3];1u];[[\"40997\"];[2];1u]]")
            .SetExpectedReadNodeType("Aggregate-TableFullScan");
        testCase.FillExpectedAggregationGroupByPlanOptions();
        TestAggregations({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Some) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT SOME(level) FROM `/Root/tableWithNulls` WHERE id=1
            )")
            .SetExpectedReply("[[[1]]]")
#if SSA_RUNTIME_VERSION >= 2U
            .AddExpectedPlanOptions("TKqpOlapAgg");
#else
            .AddExpectedPlanOptions("CombineCore");
#endif
        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Some_Null) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT SOME(level) FROM `/Root/tableWithNulls` WHERE id > 5
            )")
            .SetExpectedReply("[[#]]")
#if SSA_RUNTIME_VERSION >= 2U
            .AddExpectedPlanOptions("TKqpOlapAgg");
#else
            .AddExpectedPlanOptions("CombineCore");
#endif
        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Some_GroupBy) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    id, SOME(level)
                FROM `/Root/tableWithNulls`
                WHERE id BETWEEN 4 AND 5
                GROUP BY id
                ORDER BY id;
            )")
            .SetExpectedReply("[[4;[4]];[5;[5]]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Some_NullGroupBy) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    id, SOME(level)
                FROM `/Root/tableWithNulls`
                WHERE id BETWEEN 6 AND 7
                GROUP BY id
                ORDER BY id;
            )")
            .SetExpectedReply("[[6;#];[7;#]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Some_NullMixGroupBy) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    id, SOME(level)
                FROM `/Root/tableWithNulls`
                WHERE id > 4 AND id < 7
                GROUP BY id
                ORDER BY id;
            )")
            .SetExpectedReply("[[5;[5]];[6;#]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Some_GroupByNullMix) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    level, SOME(id), SOME(level)
                FROM `/Root/tableWithNulls`
                WHERE id BETWEEN 5 AND 6
                GROUP BY level
                ORDER BY level;
            )")
            .SetExpectedReply("[[#;6;#];[[5];5;[5]]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Aggregation_Some_GroupByNull) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    level, SOME(id), SOME(level)
                FROM `/Root/tableWithNulls`
                WHERE id = 6
                GROUP BY level
                ORDER BY level;
            )")
            .SetExpectedReply("[[#;6;#]]");
        testCase.FillExpectedAggregationGroupByPlanOptions();

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(ClickBenchSmoke) {
        TAggregationTestCase q7;
        q7.SetQuery(R"(
                SELECT
                    AdvEngineID, COUNT(*) as c
                FROM `/Root/benchTable`
                WHERE AdvEngineID != 0
                GROUP BY AdvEngineID
                ORDER BY c DESC
            )")
            //.SetExpectedReply("[[[\"40999\"];[4];1u];[[\"40998\"];[3];1u];[[\"40997\"];[2];1u]]")
            // Should be fixed in https://st.yandex-team.ru/KIKIMR-17009
            // .SetExpectedReadNodeType("TableFullScan");
            .SetExpectedReadNodeType("Aggregate-TableFullScan");
        q7.FillExpectedAggregationGroupByPlanOptions();

        TAggregationTestCase q9;
        q9.SetQuery(R"(
                SELECT
                    RegionID, SUM(AdvEngineID), COUNT(*) AS c, avg(ResolutionWidth), COUNT(DISTINCT UserID)
                FROM `/Root/benchTable`
                GROUP BY RegionID
                ORDER BY c DESC
                LIMIT 10
            )")
            //.SetExpectedReply("[[[\"40999\"];[4];1u];[[\"40998\"];[3];1u];[[\"40997\"];[2];1u]]")
            // Should be fixed in https://st.yandex-team.ru/KIKIMR-17009
            // .SetExpectedReadNodeType("TableFullScan");
            .SetExpectedReadNodeType("Aggregate-TableFullScan");
        q9.FillExpectedAggregationGroupByPlanOptions();

        TAggregationTestCase q12;
        q12.SetQuery(R"(
                SELECT
                    SearchPhrase, count(*) AS c
                FROM `/Root/benchTable`
                WHERE SearchPhrase != ''
                GROUP BY SearchPhrase
                ORDER BY c DESC
                LIMIT 10;
            )")
            //.SetExpectedReply("[[[\"40999\"];[4];1u];[[\"40998\"];[3];1u];[[\"40997\"];[2];1u]]")
            // Should be fixed in https://st.yandex-team.ru/KIKIMR-17009
            // .SetExpectedReadNodeType("TableFullScan");
            .SetExpectedReadNodeType("Aggregate-TableFullScan");
        q12.FillExpectedAggregationGroupByPlanOptions();

        TAggregationTestCase q14;
        q14.SetQuery(R"(
                SELECT
                    SearchEngineID, SearchPhrase, count(*) AS c
                FROM `/Root/benchTable`
                WHERE SearchPhrase != ''
                GROUP BY SearchEngineID, SearchPhrase
                ORDER BY c DESC
                LIMIT 10;
            )")
            //.SetExpectedReply("[[[\"40999\"];[4];1u];[[\"40998\"];[3];1u];[[\"40997\"];[2];1u]]")
            // Should be fixed in https://st.yandex-team.ru/KIKIMR-17009
            // .SetExpectedReadNodeType("TableFullScan");
            .SetExpectedReadNodeType("Aggregate-TableFullScan");
        q14.FillExpectedAggregationGroupByPlanOptions();

        TAggregationTestCase q22;
        q22.SetQuery(R"(
                SELECT
                    SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID)
                FROM `/Root/benchTable`
                WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> ''
                GROUP BY SearchPhrase
                ORDER BY c DESC
                LIMIT 10;
            )")
            .AddExpectedPlanOptions("KqpOlapFilter")
            .SetExpectedReadNodeType("Aggregate-TableFullScan");
        q22.FillExpectedAggregationGroupByPlanOptions();

        TAggregationTestCase q39;
        q39.SetQuery(R"(
                SELECT TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst, COUNT(*) AS PageViews
                FROM `/Root/benchTable`
                WHERE CounterID = 62 AND EventDate >= Date('2013-07-01') AND EventDate <= Date('2013-07-31') AND IsRefresh == 0
                GROUP BY
                    TraficSourceID, SearchEngineID, AdvEngineID, IF (SearchEngineID = 0 AND AdvEngineID = 0, Referer, '') AS Src,
                    URL AS Dst
                ORDER BY PageViews DESC
                LIMIT 10;
            )")
            .AddExpectedPlanOptions("KqpOlapFilter")
            .SetExpectedReadNodeType("Aggregate-Filter-TableFullScan");
        q39.FillExpectedAggregationGroupByPlanOptions();

        std::vector<TAggregationTestCase> cases = {q7, q9, q12, q14, q22, q39};
        for (auto&& c : cases) {
            c.SetUseLlvm(NSan::PlainOrUnderSanitizer(true, false));
        }

        TestClickBench(cases);
    }

    Y_UNIT_TEST(StatsSysView) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        static ui32 numKinds = 2;

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        TLocalHelper(kikimr).CreateTestOlapTable();
        for (ui64 i = 0; i < 100; ++i) {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000 + i * 10000, 1000);
        }

        auto tableClient = kikimr.GetTableClient();
        auto selectQuery = TString(R"(
            SELECT PathId, Kind, TabletId, Sum(Rows) as Rows
            FROM `/Root/olapStore/.sys/store_primary_index_stats`
            GROUP BY PathId, Kind, TabletId
            ORDER BY TabletId, Kind, PathId
        )");

        auto rows = ExecuteScanQuery(tableClient, selectQuery);

        UNIT_ASSERT_VALUES_EQUAL(rows.size(), numKinds*3);
        UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("PathId")), 3ull);
        UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[0].at("Kind")), "INSERTED");
        UNIT_ASSERT_GE(GetUint64(rows[0].at("TabletId")), 72075186224037888ull);
        UNIT_ASSERT_GE(GetUint64(rows[2].at("TabletId")), 72075186224037889ull);
        UNIT_ASSERT_GE(GetUint64(rows[4].at("TabletId")), 72075186224037890ull);
        UNIT_ASSERT_GE(GetUint64(rows[1].at("TabletId")), GetUint64(rows[0].at("TabletId")));
        UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[2].at("Kind")), "INSERTED");
        UNIT_ASSERT_GE(GetUint64(rows[2].at("TabletId")), GetUint64(rows[1].at("TabletId")));
        UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[3].at("PathId")), 3ull);
        UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[3].at("Kind")), "SPLIT_COMPACTED");
        UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[3].at("TabletId")), GetUint64(rows[2].at("TabletId")));
        UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[4].at("Kind")), "INSERTED");
        UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[4].at("TabletId")), GetUint64(rows[5].at("TabletId")));
        UNIT_ASSERT_GE(
            GetUint64(rows[0].at("Rows")) + GetUint64(rows[1].at("Rows")) + GetUint64(rows[2].at("Rows")) +
            GetUint64(rows[3].at("Rows")) + GetUint64(rows[4].at("Rows")) + GetUint64(rows[5].at("Rows")),
            0.3*0.9*100*1000); // >= 90% of 100K inserted rows
    }

    Y_UNIT_TEST(StatsSysViewTable) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        static ui32 numKinds = 5;

        TLocalHelper(kikimr).CreateTestOlapTable("olapTable_1");
        TLocalHelper(kikimr).CreateTestOlapTable("olapTable_2");
        for (ui64 i = 0; i < 10; ++i) {
            WriteTestData(kikimr, "/Root/olapStore/olapTable_1", 0, 1000000 + i*10000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable_2", 0, 1000000 + i*10000, 2000);
        }

        auto tableClient = kikimr.GetTableClient();
        {
            auto selectQuery = TString(R"(
                SELECT PathId, Kind, TabletId
                FROM `/Root/olapStore/olapTable_1/.sys/primary_index_stats`
                GROUP BY PathId, TabletId, Kind
                ORDER BY PathId, TabletId, Kind
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);

            UNIT_ASSERT_GT(rows.size(), 1*numKinds);
            UNIT_ASSERT_LE(rows.size(), 3*numKinds);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows.front().at("PathId")), 3ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows.back().at("PathId")), 3ull);
        }
        {
            auto selectQuery = TString(R"(
                SELECT PathId, Kind, TabletId
                FROM `/Root/olapStore/olapTable_2/.sys/primary_index_stats`
                GROUP BY PathId, TabletId, Kind
                ORDER BY PathId, TabletId, Kind
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);

            UNIT_ASSERT_GT(rows.size(), 1*numKinds);
            UNIT_ASSERT_LE(rows.size(), 3*numKinds);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows.front().at("PathId")), 4ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows.back().at("PathId")), 4ull);
        }
        {
            auto selectQuery = TString(R"(
                SELECT *
                FROM `/Root/olapStore/olapTable_1/.sys/primary_index_stats`
                WHERE
                    PathId > UInt64("3")
                ORDER BY PathId, Kind, TabletId
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);

            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 0);
        }
    }

    Y_UNIT_TEST(StatsSysViewEnumStringBytes) {
        ui64 rawBytesPK1;
        ui64 bytesPK1;
        {
            auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
            csController->SetPeriodicWakeupActivationPeriod(TDuration::Seconds(1));
            auto settings = TKikimrSettings()
                .SetWithSampleTables(false);
            TKikimrRunner kikimr(settings);
            Tests::NCommon::TLoggerInit(kikimr).Initialize();
            TTypedLocalHelper helper("", kikimr, "olapTable", "olapStore12");
            helper.CreateTestOlapTable();
            helper.FillPKOnly(0, 800000);
            helper.GetVolumes(rawBytesPK1, bytesPK1, false);
        }

        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        ui64 rawBytesUnpack1PK = 0;
        ui64 bytesUnpack1PK = 0;
        ui64 rawBytesPackAndUnpack2PK;
        ui64 bytesPackAndUnpack2PK;
        const ui32 rowsCount = 800000;
        const ui32 groupsCount = 512;
        {
            auto settings = TKikimrSettings()
                .SetWithSampleTables(false);
            TKikimrRunner kikimr(settings);
            Tests::NCommon::TLoggerInit(kikimr).Initialize();
            TTypedLocalHelper helper("Utf8", kikimr);
            helper.CreateTestOlapTable();
            NArrow::NConstruction::TStringPoolFiller sPool(groupsCount, 52);
            helper.FillTable(sPool, 0, rowsCount);
            helper.PrintCount();
            {
                auto d = helper.GetDistribution();
                Y_ABORT_UNLESS(d.GetCount() == rowsCount);
                Y_ABORT_UNLESS(d.GetGroupsCount() == groupsCount);
                Y_ABORT_UNLESS(d.GetMaxCount() - d.GetMinCount() <= 1);
            }
            helper.GetVolumes(rawBytesUnpack1PK, bytesUnpack1PK, false);
            Sleep(TDuration::Seconds(5));
            auto tableClient = kikimr.GetTableClient();
            helper.ExecuteSchemeQuery(TStringBuilder() << "ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field, `ENCODING.DICTIONARY.ENABLED`=`true`);");
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field1, `ENCODING.DICTIONARY.ENABLED`=`true`);", EStatus::SCHEME_ERROR);
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field, `ENCODING.DICTIONARY.ENABLED1`=`true`);", EStatus::GENERIC_ERROR);
            Sleep(TDuration::Seconds(5));
            helper.FillTable(sPool, 1, rowsCount);
            Sleep(TDuration::Seconds(5));
            {
                helper.GetVolumes(rawBytesPackAndUnpack2PK, bytesPackAndUnpack2PK, false);
                helper.PrintCount();
                {
                    auto d = helper.GetDistribution();
                    Cerr << d.DebugString() << Endl;
                    Y_ABORT_UNLESS(d.GetCount() == 2 * rowsCount);
                    Y_ABORT_UNLESS(d.GetGroupsCount() == groupsCount);
                    Y_ABORT_UNLESS(d.GetMaxCount() - d.GetMinCount() <= 2);
                }
            }
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field, `SERIALIZER.CLASS_NAME`=`ARROW_SERIALIZER`, `COMPRESSION.TYPE`=`zstd`);");
        }
        const ui64 rawBytesUnpack = rawBytesUnpack1PK - rawBytesPK1;
        const ui64 bytesUnpack = bytesUnpack1PK - bytesPK1;
        const ui64 rawBytesPack = rawBytesPackAndUnpack2PK - rawBytesUnpack1PK - rawBytesPK1;
        const ui64 bytesPack = bytesPackAndUnpack2PK - bytesUnpack1PK - bytesPK1;
        TStringBuilder result;
        result << "unpacked data: " << rawBytesUnpack << " / " << bytesUnpack << Endl;
        result << "packed data: " << rawBytesPack << " / " << bytesPack << Endl;
        result << "frq_diff: " << 1.0 * bytesPack / bytesUnpack << Endl;
        result << "frq_compression: " << 1.0 * bytesPack / rawBytesPack << Endl;
        result << "pk_size : " << rawBytesPK1 << " / " << bytesPK1 << Endl;
        Cerr << result << Endl;
        Y_ABORT_UNLESS(bytesPack / bytesUnpack < 0.1);
    }

    Y_UNIT_TEST(StatsSysViewBytesPackActualization) {
        ui64 rawBytesPK1;
        ui64 bytesPK1;
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->SetPeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        TTypedLocalHelper helper("", kikimr, "olapTable", "olapStore");
        helper.CreateTestOlapTable();
        helper.FillPKOnly(0, 800000);
        helper.GetVolumes(rawBytesPK1, bytesPK1, false, {"pk_int"});
        auto tableClient = kikimr.GetTableClient();
        {
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=pk_int, `SERIALIZER.CLASS_NAME`=`ARROW_SERIALIZER`, `COMPRESSION.TYPE`=`zstd`);");
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`);");
            csController->WaitActualization(TDuration::Seconds(10));
            ui64 rawBytesPK2;
            ui64 bytesPK2;
            helper.GetVolumes(rawBytesPK2, bytesPK2, false, {"pk_int"});
            AFL_VERIFY(rawBytesPK2 == rawBytesPK1)("pk1", rawBytesPK1)("pk2", rawBytesPK2);
            AFL_VERIFY(bytesPK2 < bytesPK1 / 3)("pk1", bytesPK1)("pk2", bytesPK2);
        }
        {
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=pk_int, `SERIALIZER.CLASS_NAME`=`ARROW_SERIALIZER`, `COMPRESSION.TYPE`=`lz4`);");
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`);");
            csController->WaitActualization(TDuration::Seconds(10));
            ui64 rawBytesPK2;
            ui64 bytesPK2;
            helper.GetVolumes(rawBytesPK2, bytesPK2, false, {"pk_int"});
            AFL_VERIFY(rawBytesPK2 == rawBytesPK1)("pk1", rawBytesPK1)("pk2", rawBytesPK2);
            AFL_VERIFY(bytesPK2 < bytesPK1 * 1.01 && bytesPK1 < bytesPK2 * 1.01)("pk1", bytesPK1)("pk2", bytesPK2);
        }
    }

    Y_UNIT_TEST(StatsSysViewBytesColumnActualization) {
        ui64 rawBytes1;
        ui64 bytes1;
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->SetPeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        TTypedLocalHelper helper("Utf8", kikimr);
        helper.CreateTestOlapTable();
        NArrow::NConstruction::TStringPoolFiller sPool(3, 52);
        helper.FillTable(sPool, 0, 800000);
        helper.GetVolumes(rawBytes1, bytes1, false, {"new_column_ui64"});
        AFL_VERIFY(rawBytes1 == 0);
        AFL_VERIFY(bytes1 == 0);
        auto tableClient = kikimr.GetTableClient();
        {
            helper.ExecuteSchemeQuery("ALTER TABLESTORE `/Root/olapStore` ADD COLUMN new_column_ui64 Uint64;");
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`);");
            csController->WaitActualization(TDuration::Seconds(10));
            ui64 rawBytes2;
            ui64 bytes2;
            helper.GetVolumes(rawBytes2, bytes2, false, {"new_column_ui64"});
            AFL_VERIFY(rawBytes2 == 6500041)("real", rawBytes2);
            AFL_VERIFY(bytes2 == 45360)("b", bytes2);
        }
    }

    Y_UNIT_TEST(StatsSysViewBytesDictActualization) {
        ui64 rawBytes1;
        ui64 bytes1;
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->SetPeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        TTypedLocalHelper helper("Utf8", kikimr);
        helper.CreateTestOlapTable();
        NArrow::NConstruction::TStringPoolFiller sPool(3, 52);
        helper.FillTable(sPool, 0, 800000);
        helper.GetVolumes(rawBytes1, bytes1, false, {"field"});
        auto tableClient = kikimr.GetTableClient();
        {
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field, `ENCODING.DICTIONARY.ENABLED`=`true`);");
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`);");
            csController->WaitActualization(TDuration::Seconds(10));
            ui64 rawBytes2;
            ui64 bytes2;
            helper.GetVolumes(rawBytes2, bytes2, false, {"field"});
            AFL_VERIFY(rawBytes2 == rawBytes1)("f1", rawBytes1)("f2", rawBytes2);
            AFL_VERIFY(bytes2 < bytes1 * 0.5)("f1", bytes1)("f2", bytes2);
        }
        {
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field, `ENCODING.DICTIONARY.ENABLED`=`false`);");
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`);");
            csController->WaitActualization(TDuration::Seconds(10));
            ui64 rawBytes2;
            ui64 bytes2;
            helper.GetVolumes(rawBytes2, bytes2, false, {"field"});
            AFL_VERIFY(rawBytes2 == rawBytes1)("f1", rawBytes1)("f2", rawBytes2);
            AFL_VERIFY(bytes2 < bytes1 * 1.01 && bytes1 < bytes2 * 1.01)("f1", bytes1)("f2", bytes2);
        }
    }

    Y_UNIT_TEST(StatsSysViewBytesDictStatActualization) {
        ui64 rawBytes1;
        ui64 bytes1;
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->SetPeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        TTypedLocalHelper helper("Utf8", kikimr);
        helper.CreateTestOlapTable();
        NArrow::NConstruction::TStringPoolFiller sPool(3, 52);
        helper.FillTable(sPool, 0, 800000);
        helper.GetVolumes(rawBytes1, bytes1, false, {"field"});
        auto tableClient = kikimr.GetTableClient();
        {
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=field, `ENCODING.DICTIONARY.ENABLED`=`true`);");
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_STAT, NAME=field_var, TYPE=variability, FEATURES=`{\"column_name\" : \"field\"}`);");
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_STAT, NAME=pk_int_max, TYPE=max, FEATURES=`{\"column_name\" : \"pk_int\"}`);");
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`);");
            csController->WaitActualization(TDuration::Seconds(10));
            ui64 rawBytes2;
            ui64 bytes2;
            helper.GetVolumes(rawBytes2, bytes2, false, {"field"});
            AFL_VERIFY(rawBytes2 == rawBytes1)("f1", rawBytes1)("f2", rawBytes2);
            AFL_VERIFY(bytes2 < bytes1 * 0.5)("f1", bytes1)("f2", bytes2);
            std::vector<NKikimrColumnShardStatisticsProto::TPortionStorage> stats;
            helper.GetStats(stats, true);
            for (auto&& i : stats) {
                AFL_VERIFY(i.ScalarsSize() == 2);
                AFL_VERIFY(i.GetScalars()[0].GetUint32() == 3);
            }
        }
        {
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=DROP_STAT, NAME=pk_int_max);");
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`);");
            csController->WaitActualization(TDuration::Seconds(10));
            std::vector<NKikimrColumnShardStatisticsProto::TPortionStorage> stats;
            helper.GetStats(stats, true);
            for (auto&& i : stats) {
                AFL_VERIFY(i.ScalarsSize() == 1);
                AFL_VERIFY(i.GetScalars()[0].GetUint32() == 3);
            }
        }
        {
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_STAT, NAME=pk_int_max, TYPE=max, FEATURES=`{\"column_name\" : \"pk_int\"}`);");
            helper.ExecuteSchemeQuery("ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`);");
            csController->WaitActualization(TDuration::Seconds(10));
            std::vector<NKikimrColumnShardStatisticsProto::TPortionStorage> stats;
            helper.GetStats(stats, true);
            for (auto&& i : stats) {
                AFL_VERIFY(i.ScalarsSize() == 2);
                AFL_VERIFY(i.GetScalars()[0].GetUint32() == 3);
            }
        }
    }

    Y_UNIT_TEST(StatsUsage) {
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        {
            auto settings = TKikimrSettings().SetWithSampleTables(false);
            TKikimrRunner kikimr(settings);
            Tests::NCommon::TLoggerInit(kikimr).Initialize();
            TTypedLocalHelper helper("Utf8", kikimr);
            helper.CreateTestOlapTable();
            auto tableClient = kikimr.GetTableClient();
            {
                auto alterQuery = TStringBuilder() << "ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_STAT, TYPE=max, NAME=max_pk_int, FEATURES=`{\"column_name\": \"pk_int\"}`);";
                auto session = tableClient.CreateSession().GetValueSync().GetSession();
                auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::SUCCESS, alterResult.GetIssues().ToString());
            }
            {
                auto alterQuery = TStringBuilder() << "ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_STAT, TYPE=max, NAME=max_field, FEATURES=`{\"column_name\": \"field\"}`);";
                auto session = tableClient.CreateSession().GetValueSync().GetSession();
                auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
                UNIT_ASSERT_VALUES_UNEQUAL_C(alterResult.GetStatus(), EStatus::SUCCESS, alterResult.GetIssues().ToString());
            }
            {
                auto alterQuery = TStringBuilder() << "ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_STAT, TYPE=max, NAME=max_pk_int, FEATURES=`{\"column_name\": \"pk_int\"}`);";
                auto session = tableClient.CreateSession().GetValueSync().GetSession();
                auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
                UNIT_ASSERT_VALUES_UNEQUAL_C(alterResult.GetStatus(), EStatus::SUCCESS, alterResult.GetIssues().ToString());
            }
            {
                auto alterQuery = TStringBuilder() << "ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=DROP_STAT, NAME=max_pk_int);";
                auto session = tableClient.CreateSession().GetValueSync().GetSession();
                auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::SUCCESS, alterResult.GetIssues().ToString());
            }
        }
    }

    Y_UNIT_TEST(StatsUsageWithTTL) {
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        {
            auto settings = TKikimrSettings().SetWithSampleTables(false);
            TKikimrRunner kikimr(settings);
            Tests::NCommon::TLoggerInit(kikimr).Initialize();
            TTypedLocalHelper helper("Utf8", kikimr);
            helper.CreateTestOlapTable();
            auto tableClient = kikimr.GetTableClient();
            {
                auto alterQuery = TStringBuilder() << "ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=UPSERT_STAT, TYPE=max, NAME=max_ts, FEATURES=`{\"column_name\": \"ts\"}`);";
                auto session = tableClient.CreateSession().GetValueSync().GetSession();
                auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::SUCCESS, alterResult.GetIssues().ToString());
            }
            {
                auto alterQuery = TStringBuilder() << "ALTER TABLE `/Root/olapStore/olapTable` SET (TTL = Interval(\"P1D\") ON ts);";
                auto session = tableClient.CreateSession().GetValueSync().GetSession();
                auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::SUCCESS, alterResult.GetIssues().ToString());
            }
            {
                auto alterQuery = TStringBuilder() << "ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=DROP_STAT, NAME=max_ts);";
                auto session = tableClient.CreateSession().GetValueSync().GetSession();
                auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
                UNIT_ASSERT_VALUES_UNEQUAL_C(alterResult.GetStatus(), EStatus::SUCCESS, alterResult.GetIssues().ToString());
            }
        }
    }

    namespace {
    class TTransferStatus {
    private:
        YDB_ACCESSOR(bool, Proposed, false);
        YDB_ACCESSOR(bool, Confirmed, false);
        YDB_ACCESSOR(bool, Finished, false);
    public:
        void Reset() {
            Confirmed = false;
            Proposed = false;
            Finished = false;
        }
    };

    static TMutex CSTransferStatusesMutex;
    static std::shared_ptr<TTransferStatus> CSTransferStatus = std::make_shared<TTransferStatus>();
    }

    class TTestController: public NOlap::NDataSharing::IInitiatorController {
    private:
        static const inline auto Registrator = TFactory::TRegistrator<TTestController>("test");
    protected:
        virtual void DoProposeError(const TString& sessionId, const TString& message) const override {
            AFL_VERIFY(false)("session_id", sessionId)("message", message);
        }
        virtual void DoProposeSuccess(const TString& sessionId) const override {
            CSTransferStatus->SetProposed(true);
            AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("event", "sharing_proposed")("session_id", sessionId);
        }
        virtual void DoConfirmSuccess(const TString& sessionId) const override {
            CSTransferStatus->SetConfirmed(true);
            AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("event", "sharing_confirmed")("session_id", sessionId);
        }
        virtual void DoFinished(const TString& sessionId) const override {
            CSTransferStatus->SetFinished(true);
            AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("event", "sharing_finished")("session_id", sessionId);
        }
        virtual void DoStatus(const NOlap::NDataSharing::TStatusContainer& status) const override {
            AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("event", "status")("info", status.SerializeToProto().DebugString());
        }
        virtual TConclusionStatus DoDeserializeFromProto(const NKikimrColumnShardDataSharingProto::TInitiator::TController& /*proto*/) override {
            return TConclusionStatus::Success();
        }
        virtual void DoSerializeToProto(NKikimrColumnShardDataSharingProto::TInitiator::TController& /*proto*/) const override {
            
        }

        virtual TString GetClassName() const override {
            return "test";
        }
    };

    class TSharingDataTestCase {
    private:
        const ui32 ShardsCount;
        TKikimrRunner& Kikimr;
        TTypedLocalHelper Helper;
        NYDBTest::TControllers::TGuard<NYDBTest::NColumnShard::TController> Controller;
        std::vector<ui64> ShardIds;
        std::vector<ui64> PathIds;
        YDB_ACCESSOR(bool, RebootTablet, false);
    public:
        const TTypedLocalHelper& GetHelper() const {
            return Helper;
        }

        void AddRecords(const ui32 recordsCount, const double kff = 0) {
            Helper.FillPKOnly(kff, recordsCount);
        }

        TSharingDataTestCase(const ui32 shardsCount, TKikimrRunner& kikimr)
            : ShardsCount(shardsCount)
            , Kikimr(kikimr)
            , Helper("", Kikimr, "olapTable", "olapStore12")
            , Controller(NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>())
        {
            Controller->SetCompactionControl(NYDBTest::EOptimizerCompactionWeightControl::Disable);
            Controller->SetExpectedShardsCount(ShardsCount);
            Controller->SetPeriodicWakeupActivationPeriod(TDuration::Seconds(1));
            Controller->SetReadTimeoutClean(TDuration::Seconds(1));

            Tests::NCommon::TLoggerInit(Kikimr).SetComponents({NKikimrServices::TX_COLUMNSHARD}, "CS").Initialize();

            Helper.CreateTestOlapTable(ShardsCount, ShardsCount);
            ShardIds = Controller->GetShardActualIds();
            AFL_VERIFY(ShardIds.size() == ShardsCount)("count", ShardIds.size())("ids", JoinSeq(",", ShardIds));
            std::set<ui64> pathIdsSet;
            for (auto&& i : ShardIds) {
                auto pathIds = Controller->GetPathIds(i);
                pathIdsSet.insert(pathIds.begin(), pathIds.end());
            }
            PathIds = std::vector<ui64>(pathIdsSet.begin(), pathIdsSet.end());
            AFL_VERIFY(PathIds.size() == 1)("count", PathIds.size())("ids", JoinSeq(",", PathIds));
        }

        void WaitNormalization() {
            Controller->SetCompactionControl(NYDBTest::EOptimizerCompactionWeightControl::Force);
            const auto start = TInstant::Now();
            while (!Controller->IsTrivialLinks() && TInstant::Now() - start < TDuration::Seconds(30)) {
                Cerr << "WAIT_TRIVIAL_LINKS..." << Endl;
                Sleep(TDuration::Seconds(1));
            }
            AFL_VERIFY(Controller->IsTrivialLinks());
            Controller->CheckInvariants();
        }

        void Execute(const ui64 destinationIdx, const std::vector<ui64>& sourceIdxs, const bool move, const NOlap::TSnapshot& snapshot, const std::set<ui64>& pathIdxs) {
            AFL_VERIFY(destinationIdx < ShardIds.size());
            const ui64 destination = ShardIds[destinationIdx];
            std::vector<ui64> sources;
            for (auto&& i : sourceIdxs) {
                AFL_VERIFY(i < ShardIds.size());
                sources.emplace_back(ShardIds[i]);
            }
            std::set<ui64> pathIds;
            for (auto&& i : pathIdxs) {
                AFL_VERIFY(i < PathIds.size());
                AFL_VERIFY(pathIds.emplace(PathIds[i]).second);
            }
            Cerr << "SHARING: " << JoinSeq(",", sources) << "->" << destination << Endl;
            THashMap<ui64, ui64> pathIdsRemap;
            for (auto&& i : pathIds) {
                pathIdsRemap.emplace(i, i);
            }
            THashSet<NOlap::TTabletId> sourceTablets;
            for (auto&& i : sources) {
                AFL_VERIFY(sourceTablets.emplace((NOlap::TTabletId)i).second);
            }
            const TString sessionId = TGUID::CreateTimebased().AsUuidString();
            NOlap::NDataSharing::TTransferContext transferContext((NOlap::TTabletId)destination, sourceTablets, snapshot, move);
            NOlap::NDataSharing::TDestinationSession session(std::make_shared<TTestController>(), pathIdsRemap, sessionId, transferContext);
            Kikimr.GetTestServer().GetRuntime()->Send(MakePipePeNodeCacheID(false), NActors::TActorId(), new TEvPipeCache::TEvForward(
                new NOlap::NDataSharing::NEvents::TEvProposeFromInitiator(session), destination, false));
            {
                const TInstant start = TInstant::Now();
                while (!CSTransferStatus->GetProposed() && TInstant::Now() - start < TDuration::Seconds(10)) {
                    Sleep(TDuration::Seconds(1));
                    Cerr << "WAIT_PROPOSING..." << Endl;
                }
                AFL_VERIFY(CSTransferStatus->GetProposed());
            }
            if (RebootTablet) {
                Kikimr.GetTestServer().GetRuntime()->Send(MakePipePeNodeCacheID(false), NActors::TActorId(), new TEvPipeCache::TEvForward(
                    new TEvents::TEvPoisonPill(), destination, false));
            }
            {
                const TInstant start = TInstant::Now();
                while (!CSTransferStatus->GetConfirmed() && TInstant::Now() - start < TDuration::Seconds(10)) {
                    Kikimr.GetTestServer().GetRuntime()->Send(MakePipePeNodeCacheID(false), NActors::TActorId(), new TEvPipeCache::TEvForward(
                        new NOlap::NDataSharing::NEvents::TEvConfirmFromInitiator(sessionId), destination, false));
                    Sleep(TDuration::Seconds(1));
                    Cerr << "WAIT_CONFIRMED..." << Endl;
                }
                AFL_VERIFY(CSTransferStatus->GetConfirmed());
            }
            if (RebootTablet) {
                Kikimr.GetTestServer().GetRuntime()->Send(MakePipePeNodeCacheID(false), NActors::TActorId(), new TEvPipeCache::TEvForward(
                    new TEvents::TEvPoisonPill(), destination, false));
                for (auto&& i : sources) {
                    Kikimr.GetTestServer().GetRuntime()->Send(MakePipePeNodeCacheID(false), NActors::TActorId(), new TEvPipeCache::TEvForward(
                        new TEvents::TEvPoisonPill(), i, false));
                }
            }
            {
                const TInstant start = TInstant::Now();
                while (!CSTransferStatus->GetFinished() && TInstant::Now() - start < TDuration::Seconds(10)) {
                    Sleep(TDuration::Seconds(1));
                    Cerr << "WAIT_FINISHED..." << Endl;
                }
                AFL_VERIFY(CSTransferStatus->GetFinished());
            }
            CSTransferStatus->Reset();
            AFL_VERIFY(!Controller->IsTrivialLinks());
        }
    };

    Y_UNIT_TEST(BlobsSharingSplit1_1) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        TSharingDataTestCase tester(4, kikimr);
        tester.AddRecords(800000);
        Sleep(TDuration::Seconds(1));
        tester.Execute(0, {1}, false, NOlap::TSnapshot(TInstant::Now().MilliSeconds(), 1232123), {0});
    }

    Y_UNIT_TEST(BlobsSharingSplit1_1_clean) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        TSharingDataTestCase tester(2, kikimr);
        tester.AddRecords(80000);
        CompareYson(tester.GetHelper().GetQueryResult("SELECT COUNT(*) FROM `/Root/olapStore12/olapTable`"), R"([[80000u;]])");
        Sleep(TDuration::Seconds(1));
        tester.Execute(0, {1}, false, NOlap::TSnapshot(TInstant::Now().MilliSeconds(), 1232123), {0});
        CompareYson(tester.GetHelper().GetQueryResult("SELECT COUNT(*) FROM `/Root/olapStore12/olapTable`"), R"([[119928u;]])");
        tester.AddRecords(80000, 0.8);
        tester.WaitNormalization();
        CompareYson(tester.GetHelper().GetQueryResult("SELECT COUNT(*) FROM `/Root/olapStore12/olapTable`"), R"([[183928u;]])");
    }

    Y_UNIT_TEST(BlobsSharingSplit1_1_clean_with_restarts) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        TSharingDataTestCase tester(2, kikimr);
        tester.SetRebootTablet(true);
        tester.AddRecords(80000);
        CompareYson(tester.GetHelper().GetQueryResult("SELECT COUNT(*) FROM `/Root/olapStore12/olapTable`"), R"([[80000u;]])");
        Sleep(TDuration::Seconds(1));
        tester.Execute(0, {1}, false, NOlap::TSnapshot(TInstant::Now().MilliSeconds(), 1232123), {0});
        CompareYson(tester.GetHelper().GetQueryResult("SELECT COUNT(*) FROM `/Root/olapStore12/olapTable`"), R"([[119928u;]])");
        tester.AddRecords(80000, 0.8);
        tester.WaitNormalization();
        CompareYson(tester.GetHelper().GetQueryResult("SELECT COUNT(*) FROM `/Root/olapStore12/olapTable`"), R"([[183928u;]])");
    }

    Y_UNIT_TEST(BlobsSharingSplit3_1) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        TSharingDataTestCase tester(4, kikimr);
        tester.AddRecords(800000);
        Sleep(TDuration::Seconds(1));
        tester.Execute(0, {1, 2, 3}, false, NOlap::TSnapshot(TInstant::Now().MilliSeconds(), 1232123), {0});
    }

    Y_UNIT_TEST(BlobsSharingSplit1_3_1) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        TSharingDataTestCase tester(4, kikimr);
        tester.AddRecords(800000);
        Sleep(TDuration::Seconds(1));
        tester.Execute(1, {0}, false, NOlap::TSnapshot(TInstant::Now().MilliSeconds(), 1232123), {0});
        tester.Execute(2, {0}, false, NOlap::TSnapshot(TInstant::Now().MilliSeconds(), 1232123), {0});
        tester.Execute(3, {0}, false, NOlap::TSnapshot(TInstant::Now().MilliSeconds(), 1232123), {0});
        tester.Execute(0, {1, 2, 3}, false, NOlap::TSnapshot(TInstant::Now().MilliSeconds(), 1232123), {0});
    }

    Y_UNIT_TEST(BlobsSharingSplit1_3_2_1_clean) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        TSharingDataTestCase tester(4, kikimr);
        tester.AddRecords(800000);
        Sleep(TDuration::Seconds(1));
        tester.Execute(1, {0}, false, NOlap::TSnapshot(TInstant::Now().MilliSeconds(), 1232123), {0});
        tester.Execute(2, {0}, false, NOlap::TSnapshot(TInstant::Now().MilliSeconds(), 1232123), {0});
        tester.Execute(3, {0}, false, NOlap::TSnapshot(TInstant::Now().MilliSeconds(), 1232123), {0});
        tester.AddRecords(800000, 0.9);
        Sleep(TDuration::Seconds(1));
        tester.Execute(3, {2}, false, NOlap::TSnapshot(TInstant::Now().MilliSeconds(), 1232123), {0});
        tester.Execute(0, {1, 2}, false, NOlap::TSnapshot(TInstant::Now().MilliSeconds(), 1232123), {0});
        tester.WaitNormalization();
    }

    Y_UNIT_TEST(SelectLimit1ManyShards) {
        TPortManager tp;
        ui16 mbusport = tp.GetPort(2134);
        auto settings = Tests::TServerSettings(mbusport)
            .SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetNodeCount(2);


        Tests::TServer::TPtr server = new Tests::TServer(settings);
        auto runtime = server->GetRuntime();
        auto sender = runtime->AllocateEdgeActor();

        InitRoot(server, sender);
        Tests::NCommon::TLoggerInit(runtime).Initialize();

        const ui32 numShards = 10;
        const ui32 numIterations = 10;
        TLocalHelper(*server).CreateTestOlapTable("selectTable", "selectStore", numShards, numShards);
        for(ui64 i = 0; i < numIterations; ++i) {
            TLocalHelper(*server).SendDataViaActorSystem("/Root/selectStore/selectTable", 0, 1000000 + i*1000000, 2000);
        }

        ui64 result = 0;

        std::vector<TAutoPtr<IEventHandle>> evs;
        ui32 num = 0;
        auto captureEvents = [&](TAutoPtr<IEventHandle>& ev) -> auto {
            switch (ev->GetTypeRewrite()) {
                case NKqp::TKqpExecuterEvents::EvShardsResolveStatus: {

                    auto* msg = ev->Get<NKqp::TEvKqpExecuter::TEvShardsResolveStatus>();
                    for (auto& [shardId, nodeId]: msg->ShardNodes) {
                        Cerr << "-- nodeId: " << nodeId << Endl;
                        nodeId = runtime->GetNodeId(num);
                        ++num;
                        num = num % 2;
                    }
                    break;
                }

                case NYql::NDq::TDqComputeEvents::EvChannelData: {
                    auto& record = ev->Get<NYql::NDq::TEvDqCompute::TEvChannelData>()->Record;
                    if (record.GetChannelData().GetChannelId() == 2) {
                        Cerr << (TStringBuilder() << "captured event for the second channel" << Endl);
                        Cerr.Flush();
                    }

                    Cerr << (TStringBuilder() << "-- EvChannelData: " << record.AsJSON() << Endl);
                    Cerr.Flush();

                    if (record.GetChannelData().GetChannelId() == 2) {
                        Cerr << (TStringBuilder() << "captured event for the second channel" << Endl);
                        evs.push_back(ev);
                        return TTestActorRuntime::EEventAction::DROP;
                    } else {
                        return TTestActorRuntime::EEventAction::PROCESS;
                    }
                }

                case NKqp::TKqpExecuterEvents::EvStreamData: {
                    auto& record = ev->Get<NKqp::TEvKqpExecuter::TEvStreamData>()->Record;

                    Cerr << (TStringBuilder() << "-- EvStreamData: " << record.AsJSON() << Endl);
                    Cerr.Flush();

                    Y_ASSERT(record.GetResultSet().rows().size() == 1);
                    result = 1;

                    auto resp = MakeHolder<NKqp::TEvKqpExecuter::TEvStreamDataAck>();
                    resp->Record.SetEnough(false);
                    resp->Record.SetSeqNo(ev->Get<NKqp::TEvKqpExecuter::TEvStreamData>()->Record.GetSeqNo());
                    resp->Record.SetFreeSpace(100);
                    runtime->Send(new IEventHandle(ev->Sender, sender, resp.Release()));
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };

        runtime->SetObserverFunc(captureEvents);
        auto streamSender = runtime->AllocateEdgeActor();
        SendRequest(*runtime, streamSender, MakeStreamRequest(streamSender, "SELECT * FROM `/Root/selectStore/selectTable` LIMIT 1;", false));
        auto ev = runtime->GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(streamSender);
        UNIT_ASSERT_VALUES_EQUAL(result, 1);
    }

    Y_UNIT_TEST(ManyColumnShards) {
        TPortManager tp;
        ui16 mbusport = tp.GetPort(2134);
        auto settings = Tests::TServerSettings(mbusport)
            .SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetNodeCount(2);

        Tests::TServer::TPtr server = new Tests::TServer(settings);

        auto runtime = server->GetRuntime();
        auto sender = runtime->AllocateEdgeActor();

        InitRoot(server, sender);
        Tests::NCommon::TLoggerInit(runtime).Initialize();

        ui32 numShards = NSan::PlainOrUnderSanitizer(1000, 10);
        ui32 numIterations = NSan::PlainOrUnderSanitizer(50, 10);
        TLocalHelper(*server).SetShardingMethod("HASH_FUNCTION_CLOUD_LOGS").CreateTestOlapTable("largeOlapTable", "largeOlapStore", numShards, numShards);
        ui32 insertRows = 0;
        for(ui64 i = 0; i < numIterations; ++i) {
            TLocalHelper(*server).SendDataViaActorSystem("/Root/largeOlapStore/largeOlapTable", 0, 1000000 + i * 1000000, 2000);
            insertRows += 2000;
        }

        ui64 result = 0;
        auto captureEvents = [&](TAutoPtr<IEventHandle>& ev) -> auto {
            switch (ev->GetTypeRewrite()) {
                case NKqp::TKqpExecuterEvents::EvShardsResolveStatus: {

                    auto* msg = ev->Get<NKqp::TEvKqpExecuter::TEvShardsResolveStatus>();
                    for (auto& [shardId, nodeId]: msg->ShardNodes) {
                        Cerr << "-- nodeId: " << nodeId << Endl;
                        nodeId = runtime->GetNodeId(0);
                    }
                    break;
                }

                case NKqp::TKqpExecuterEvents::EvStreamData: {
                    auto& record = ev->Get<NKqp::TEvKqpExecuter::TEvStreamData>()->Record;

                    Cerr << (TStringBuilder() << "-- EvStreamData: " << record.AsJSON() << Endl);
                    Cerr.Flush();

                    Y_ASSERT(record.GetResultSet().rows().size() == 1);
                    Y_ASSERT(record.GetResultSet().rows().at(0).items().size() == 1);
                    result = record.GetResultSet().rows().at(0).items().at(0).uint64_value();

                    auto resp = MakeHolder<NKqp::TEvKqpExecuter::TEvStreamDataAck>();
                    resp->Record.SetEnough(false);
                    resp->Record.SetSeqNo(ev->Get<NKqp::TEvKqpExecuter::TEvStreamData>()->Record.GetSeqNo());
                    resp->Record.SetFreeSpace(100);
                    runtime->Send(new IEventHandle(ev->Sender, sender, resp.Release()));
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };

        runtime->SetObserverFunc(captureEvents);
        auto streamSender = runtime->AllocateEdgeActor();
        SendRequest(*runtime, streamSender, MakeStreamRequest(streamSender, "SELECT COUNT(*) FROM `/Root/largeOlapStore/largeOlapTable`;", false));
        runtime->GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(streamSender);
        UNIT_ASSERT_VALUES_EQUAL(result, insertRows);
    }

    Y_UNIT_TEST(ManyColumnShardsFilterPushdownEmptySet) {
        TPortManager tp;
        ui16 mbusport = tp.GetPort(2134);
        auto settings = Tests::TServerSettings(mbusport)
            .SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetNodeCount(2);

        Tests::TServer::TPtr server = new Tests::TServer(settings);

        auto runtime = server->GetRuntime();
        auto sender = runtime->AllocateEdgeActor();

        InitRoot(server, sender);
        Tests::NCommon::TLoggerInit(runtime).Initialize();

        const ui32 numShards = 10;
        const ui32 numIterations = 50;
        TLocalHelper(*server).CreateTestOlapTable("largeOlapTable", "largeOlapStore", numShards, numShards);
        for(ui64 i = 0; i < numIterations; ++i) {
            TLocalHelper(*server).SendDataViaActorSystem("/Root/largeOlapStore/largeOlapTable", 0, 1000000 + i*1000000, 2000);
        }

        bool hasResult = false;
        auto captureEvents = [&](TAutoPtr<IEventHandle>& ev) -> auto {
            switch (ev->GetTypeRewrite()) {
                case NKqp::TKqpExecuterEvents::EvShardsResolveStatus: {

                    auto* msg = ev->Get<NKqp::TEvKqpExecuter::TEvShardsResolveStatus>();
                    for (auto& [shardId, nodeId]: msg->ShardNodes) {
                        Cerr << "-- nodeId: " << nodeId << Endl;
                        nodeId = runtime->GetNodeId(0);
                    }
                    break;
                }

                case NKqp::TKqpExecuterEvents::EvStreamData: {
                    auto& record = ev->Get<NKqp::TEvKqpExecuter::TEvStreamData>()->Record;

                    Cerr << (TStringBuilder() << "-- EvStreamData: " << record.AsJSON() << Endl);
                    Cerr.Flush();

                    Y_ASSERT(record.GetResultSet().rows().size() == 0);
                    hasResult = true;

                    auto resp = MakeHolder<NKqp::TEvKqpExecuter::TEvStreamDataAck>();
                    resp->Record.SetEnough(false);
                    resp->Record.SetSeqNo(ev->Get<NKqp::TEvKqpExecuter::TEvStreamData>()->Record.GetSeqNo());
                    resp->Record.SetFreeSpace(100);
                    runtime->Send(new IEventHandle(ev->Sender, sender, resp.Release()));
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };

        runtime->SetObserverFunc(captureEvents);
        auto streamSender = runtime->AllocateEdgeActor();
        SendRequest(*runtime, streamSender, MakeStreamRequest(streamSender, "SELECT * FROM `/Root/largeOlapStore/largeOlapTable` where resource_id = Utf8(\"notfound\");", false));
        auto ev = runtime->GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(streamSender);
        UNIT_ASSERT(hasResult);
    }

    Y_UNIT_TEST(ManyColumnShardsWithRestarts) {
        TPortManager tp;
        ui16 mbusport = tp.GetPort(2134);
        auto settings = Tests::TServerSettings(mbusport)
            .SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetNodeCount(2);

        Tests::TServer::TPtr server = new Tests::TServer(settings);
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();

        auto runtime = server->GetRuntime();
        auto sender = runtime->AllocateEdgeActor();

        InitRoot(server, sender);
        Tests::NCommon::TLoggerInit(runtime).Initialize();

        ui32 numShards = NSan::PlainOrUnderSanitizer(100, 10);
        ui32 numIterations = NSan::PlainOrUnderSanitizer(100, 10);
        TLocalHelper(*server).SetShardingMethod("HASH_FUNCTION_CLOUD_LOGS").CreateTestOlapTable("largeOlapTable", "largeOlapStore", numShards, numShards);
        ui32 insertRows = 0;

        for(ui64 i = 0; i < numIterations; ++i) {
            TLocalHelper(*server).SendDataViaActorSystem("/Root/largeOlapStore/largeOlapTable", 0, 1000000 + i*1000000, 2000);
            insertRows += 2000;
        }

        ui64 result = 0;
        THashSet<TActorId> columnShardScans;
        bool prevIsFinished = false;

        auto captureEvents = [&](TAutoPtr<IEventHandle>& ev) -> auto {
            switch (ev->GetTypeRewrite()) {
                case NKqp::TKqpExecuterEvents::EvShardsResolveStatus: {

                    auto* msg = ev->Get<NKqp::TEvKqpExecuter::TEvShardsResolveStatus>();
                    for (auto& [shardId, nodeId]: msg->ShardNodes) {
                        Cerr << "-- nodeId: " << nodeId << Endl;
                        nodeId = runtime->GetNodeId(0);
                    }
                    break;
                }

                case NKqp::TKqpExecuterEvents::EvStreamData: {
                    auto& record = ev->Get<NKqp::TEvKqpExecuter::TEvStreamData>()->Record;

                    Cerr << (TStringBuilder() << "-- EvStreamData: " << record.AsJSON() << Endl);
                    Cerr.Flush();

                    Y_ASSERT(record.GetResultSet().rows().size() == 1);
                    Y_ASSERT(record.GetResultSet().rows().at(0).items().size() == 1);
                    result = record.GetResultSet().rows().at(0).items().at(0).uint64_value();

                    auto resp = MakeHolder<NKqp::TEvKqpExecuter::TEvStreamDataAck>();
                    resp->Record.SetEnough(false);
                    resp->Record.SetSeqNo(ev->Get<NKqp::TEvKqpExecuter::TEvStreamData>()->Record.GetSeqNo());
                    resp->Record.SetFreeSpace(100);
                    runtime->Send(new IEventHandle(ev->Sender, sender, resp.Release()));
                    return TTestActorRuntime::EEventAction::DROP;
                }

                case NKqp::TKqpComputeEvents::EvScanData: {
                    auto [it, success] = columnShardScans.emplace(ev->Sender);
                    auto* msg = ev->Get<NKqp::TEvKqpCompute::TEvScanData>();
                    if (success) {
                        // first scan response.
                        prevIsFinished = msg->Finished;
                        return TTestActorRuntime::EEventAction::PROCESS;
                    } else {
                        if (prevIsFinished) {
                            Cerr << (TStringBuilder() << "-- EvScanData from " << ev->Sender << ": hijack event");
                            Cerr.Flush();
                            auto resp = std::make_unique<NKqp::TEvKqpCompute::TEvScanError>(msg->Generation, 0);
                            runtime->Send(new IEventHandle(ev->Recipient, ev->Sender, resp.release()));
                        } else {
                            prevIsFinished = msg->Finished;
                        }
                        return TTestActorRuntime::EEventAction::PROCESS;
                    }
                    break;
                }

                default:
                    break;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };

        runtime->SetObserverFunc(captureEvents);
        auto streamSender = runtime->AllocateEdgeActor();
        SendRequest(*runtime, streamSender, MakeStreamRequest(streamSender, "SELECT COUNT(*) FROM `/Root/largeOlapStore/largeOlapTable`;", false));
        auto ev = runtime->GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(streamSender);
        UNIT_ASSERT_VALUES_EQUAL(result, insertRows);
    }

    Y_UNIT_TEST(StatsSysViewColumns) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr.GetTestServer()).CreateTestOlapTable();
        for (ui64 i = 0; i < 10; ++i) {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000 + i*10000, 2000);
        }

        auto tableClient = kikimr.GetTableClient();

        {
            auto selectQuery = TString(R"(
                SELECT TabletId, PathId, Kind
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                ORDER BY PathId, Kind, TabletId
                LIMIT 4;
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);

            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 4);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("PathId")), 3ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[0].at("Kind")), "INSERTED");
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[3].at("PathId")), 3ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[3].at("Kind")), "INSERTED");
        }
        {
            auto selectQuery = TString(R"(
                SELECT SUM(BlobRangeSize) as Bytes, SUM(Rows) as Rows, PathId, TabletId
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                GROUP BY PathId, TabletId
                ORDER BY Bytes
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);

            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 3);
            UNIT_ASSERT_LE(GetUint64(rows[0].at("Bytes")), GetUint64(rows[1].at("Bytes")));
        }
        {
            auto selectQuery = TString(R"(
                SELECT Sum(Rows) as Rows, Kind, Sum(RawBytes) as RawBytes, Sum(Rows) as Rows2, Sum(Rows) as Rows3, PathId
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                GROUP BY Kind, PathId
                ORDER BY PathId, Kind, Rows3
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("Rows2")), GetUint64(rows[0].at("Rows3")));
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[1].at("Rows")), GetUint64(rows[1].at("Rows3")));
        }
    }

    Y_UNIT_TEST(StatsSysViewRanges) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        csController->SetCompactionControl(NYDBTest::EOptimizerCompactionWeightControl::Disable);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        TLocalHelper(kikimr).CreateTestOlapTable("olapTable_1");
        TLocalHelper(kikimr).CreateTestOlapTable("olapTable_2");
        TLocalHelper(kikimr).CreateTestOlapTable("olapTable_3");

        for (ui64 i = 0; i < 10; ++i) {
            WriteTestData(kikimr, "/Root/olapStore/olapTable_1", 0, 1000000 + i*10000, 2000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable_2", 0, 1000000 + i*10000, 3000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable_3", 0, 1000000 + i*10000, 5000);
        }

        auto tableClient = kikimr.GetTableClient();

        {
            auto selectQuery = TString(R"(
                SELECT *
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);
        }

        {
            auto selectQuery = TString(R"(
                SELECT PathId, Kind, TabletId
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                WHERE
                    PathId == UInt64("3") AND Activity = true
                GROUP BY TabletId, PathId, Kind
                ORDER BY TabletId, Kind
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);

            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("PathId")), 3ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[0].at("Kind")), "INSERTED");
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[1].at("PathId")), 3ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[2].at("Kind")), "INSERTED");
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[2].at("PathId")), 3ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[1].at("Kind")), "INSERTED");
        }

        {
            auto selectQuery = TString(R"(
                SELECT PathId, Kind, TabletId
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                GROUP BY PathId, Kind, TabletId
                ORDER BY PathId DESC, Kind DESC, TabletId DESC
                ;
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);

            ui32 numExpected = 3*3;
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), numExpected);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("PathId")), 5ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[0].at("Kind")), "INSERTED");
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[numExpected-1].at("PathId")), 3ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[numExpected-1].at("Kind")), "INSERTED");
        }

        {
            auto selectQuery = TString(R"(
                SELECT PathId, Kind, TabletId
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                WHERE
                    PathId > UInt64("0") AND PathId < UInt32("4")
                    OR PathId > UInt64("4") AND PathId <= UInt64("5")
                GROUP BY PathId, Kind, TabletId
                ORDER BY
                    PathId DESC, Kind DESC, TabletId DESC
                ;
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);

            ui32 numExpected = 2*3;
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), numExpected);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("PathId")), 5ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[0].at("Kind")), "INSERTED");
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[numExpected-1].at("PathId")), 3ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUtf8(rows[numExpected-1].at("Kind")), "INSERTED");
        }
    }

    Y_UNIT_TEST(StatsSysViewFilter) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();

        TLocalHelper(kikimr.GetTestServer()).CreateTestOlapTable();
        for (ui64 i = 0; i < 10; ++i) {
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000 + i*10000, 2000);
        }

        auto tableClient = kikimr.GetTableClient();

        {
            auto selectQuery = TString(R"(
                SELECT PathId, Kind, TabletId, Sum(BlobRangeSize) as Bytes
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                GROUP BY PathId, Kind, TabletId
                ORDER BY PathId, Kind, TabletId;
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);

            UNIT_ASSERT_GE(rows.size(), 3);
        }

        {
            auto selectQuery = TString(R"(
                SELECT PathId, Kind, TabletId, Sum(BlobRangeSize) as Bytes
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                GROUP BY PathId, Kind, TabletId
                ORDER BY PathId, Kind, TabletId;
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);

            UNIT_ASSERT_GE(rows.size(), 3);
        }

        {
            auto selectQuery = TString(R"(
                SELECT *
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                WHERE Kind == 'EVICTED'
                ORDER BY PathId, Kind, TabletId;
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);

            UNIT_ASSERT_GE(rows.size(), 0);
        }

        {
            auto selectQuery = TString(R"(
                SELECT PathId, Kind, TabletId
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                WHERE Kind IN ('SPLIT_COMPACTED', 'INACTIVE', 'EVICTED')
                GROUP BY PathId, Kind, TabletId
                ORDER BY PathId, Kind, TabletId;
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);

            UNIT_ASSERT_GE(rows.size(), 3);
        }
    }

    Y_UNIT_TEST(StatsSysViewAggregation) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();

        TLocalHelper(kikimr.GetTestServer()).CreateTestOlapTable("olapTable_1");
        TLocalHelper(kikimr.GetTestServer()).CreateTestOlapTable("olapTable_2");
        TLocalHelper(kikimr.GetTestServer()).CreateTestOlapTable("olapTable_3");

        for (ui64 i = 0; i < 100; ++i) {
            WriteTestData(kikimr, "/Root/olapStore/olapTable_1", 0, 1000000 + i*10000, 1000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable_2", 0, 1000000 + i*10000, 2000);
            WriteTestData(kikimr, "/Root/olapStore/olapTable_3", 0, 1000000 + i*10000, 3000);
        }

        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto tableClient = kikimr.GetTableClient();

        {
            auto selectQuery = TString(R"(
                SELECT
                    SUM(Rows) as rows,
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                WHERE
                    Kind != 'INACTIVE'
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1ull);
        }

        {
            auto selectQuery = TString(R"(
                SELECT
                    PathId,
                    SUM(Rows) as rows,
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                WHERE
                    Kind != 'INACTIVE'
                GROUP BY
                    PathId
                ORDER BY
                    PathId
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 3ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("PathId")), 3);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[1].at("PathId")), 4);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[2].at("PathId")), 5);
        }

        {
            auto selectQuery = TString(R"(
                SELECT
                    PathId,
                    SUM(Rows) as rows,
                    SUM(BlobRangeSize) as bytes,
                    SUM(RawBytes) as bytes_raw
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                WHERE
                    Kind IN ('INSERTED', 'SPLIT_COMPACTED', 'COMPACTED')
                GROUP BY PathId
                ORDER BY rows DESC
                LIMIT 10
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 3ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("PathId")), 5);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[1].at("PathId")), 4);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[2].at("PathId")), 3);
        }

        {
            auto selectQuery = TString(R"(
                SELECT
                    PathId,
                    SUM(Rows) as rows,
                    SUM(BlobRangeSize) as bytes,
                    SUM(RawBytes) as bytes_raw
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                WHERE
                    PathId == UInt64("3") AND Kind IN ('INSERTED', 'SPLIT_COMPACTED', 'COMPACTED')
                GROUP BY PathId
                ORDER BY rows DESC
                LIMIT 10
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 1ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("PathId")), 3);
        }

        {
            auto selectQuery = TString(R"(
                SELECT
                    PathId,
                    SUM(Rows) as rows,
                    SUM(BlobRangeSize) as bytes,
                    SUM(RawBytes) as bytes_raw
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                WHERE
                    PathId >= UInt64("4") AND Kind IN ('INSERTED', 'SPLIT_COMPACTED', 'COMPACTED')
                GROUP BY PathId
                ORDER BY rows DESC
                LIMIT 10
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 2ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("PathId")), 5);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[1].at("PathId")), 4);
        }

        {
            auto selectQuery = TString(R"(
                SELECT PathId, TabletId, Kind
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                GROUP BY PathId, TabletId, Kind
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            // 3 Tables with 3 Shards each and 2 KindId-s of stats
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 3 * 3 * 2);
        }

        {
            auto selectQuery = TString(R"(
                SELECT
                    count(distinct(PathId)),
                    count(distinct(Kind)),
                    count(distinct(TabletId))
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("column0")), 3ull);
            UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[0].at("column1")), 2);
            UNIT_ASSERT_GE(GetUint64(rows[0].at("column2")), 3ull);
        }

        {
            auto selectQuery = TString(R"(
                SELECT PathId, count(*), sum(Rows), sum(BlobRangeSize), sum(RawBytes)
                FROM `/Root/olapStore/.sys/store_primary_index_stats`
                GROUP BY PathId
                ORDER BY PathId
            )");

            auto rows = ExecuteScanQuery(tableClient, selectQuery);
            UNIT_ASSERT_VALUES_EQUAL(rows.size(), 3ull);
            for (ui64 pathId = 3, row = 0; pathId <= 5; ++pathId, ++row) {
                UNIT_ASSERT_VALUES_EQUAL(GetUint64(rows[row].at("PathId")), pathId);
            }
        }
    }

    Y_UNIT_TEST(PredicatePushdownWithParameters) {
        constexpr bool logQueries = true;
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TStreamExecScanQuerySettings scanSettings;
        scanSettings.Explain(true);

        TLocalHelper(kikimr.GetTestServer()).CreateTestOlapTable();
        WriteTestData(kikimr, "/Root/olapStore/olapTable", 10000, 3000000, 1000);

        auto tableClient = kikimr.GetTableClient();

        auto buildQuery = [](bool pushEnabled) {
            TStringBuilder builder;

            builder << "--!syntax_v1" << Endl;

            if (!pushEnabled) {
                builder << "PRAGMA Kikimr.OptEnableOlapPushdown = \"false\";" << Endl;
            }

            builder << R"(
                DECLARE $in_uid AS Utf8;
                DECLARE $in_level AS Int32;

                SELECT `timestamp` FROM `/Root/olapStore/olapTable` WHERE
                    uid > $in_uid AND level > $in_level
                ORDER BY `timestamp`;
            )" << Endl;

            return builder;
        };

        auto normalQuery = buildQuery(false);
        auto pushQuery = buildQuery(true);

        auto params = tableClient.GetParamsBuilder()
            .AddParam("$in_uid")
                .Utf8("uid_3000980")
                .Build()
            .AddParam("$in_level")
                .Int32(2)
                .Build()
            .Build();

        auto it = tableClient.StreamExecuteScanQuery(normalQuery, params).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        auto goodResult = CollectStreamResult(it);

        it = tableClient.StreamExecuteScanQuery(pushQuery, params).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        auto pushResult = CollectStreamResult(it);

        if (logQueries) {
            Cerr << "Query: " << normalQuery << Endl;
            Cerr << "Expected: " << goodResult.ResultSetYson << Endl;
            Cerr << "Received: " << pushResult.ResultSetYson << Endl;
        }

        CompareYson(goodResult.ResultSetYson, pushResult.ResultSetYson);

        it = tableClient.StreamExecuteScanQuery(pushQuery, scanSettings).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        auto result = CollectStreamResult(it);
        auto ast = result.QueryStats->Getquery_ast();

        UNIT_ASSERT_C(ast.find("KqpOlapFilter") != std::string::npos,
                      TStringBuilder() << "Predicate not pushed down. Query: " << pushQuery);

        NJson::TJsonValue plan, readRange;
        NJson::ReadJsonTree(*result.PlanJson, &plan, true);

        readRange = FindPlanNodeByKv(plan, "Name", "TableFullScan");
        UNIT_ASSERT(readRange.IsDefined());
    }

    Y_UNIT_TEST(PredicatePushdownParameterTypesValidation) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TStreamExecScanQuerySettings scanSettings;
        scanSettings.Explain(true);

        CreateTableOfAllTypes(kikimr);

        auto tableClient = kikimr.GetTableClient();

        std::map<std::string, TParams> testData = CreateParametersOfAllTypes(tableClient);

        const TString queryTemplate = R"(
            --!syntax_v1
            DECLARE $in_value AS <--TYPE-->;
            SELECT `key` FROM `/Root/olapStore/OlapParametersTable` WHERE <--NAME-->_column > $in_value;
        )";

        for (auto& [type, parameter]: testData) {
            TString query(queryTemplate);
            std::string clearType = type;

            size_t pos = clearType.find('(');

            if (std::string::npos != pos) {
                clearType = clearType.substr(0, pos);
            }

            SubstGlobal(query, "<--TYPE-->", type);
            SubstGlobal(query, "<--NAME-->", clearType);

            TStringBuilder b;

            b << "----------------------------" << Endl;
            b << query << Endl;
            b << "----------------------------" << Endl;

            auto it = tableClient.StreamExecuteScanQuery(query, parameter).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString() << Endl << b);
            auto goodResult = CollectStreamResult(it);

            it = tableClient.StreamExecuteScanQuery(query, scanSettings).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString() << Endl << b);

            auto result = CollectStreamResult(it);
            auto ast = result.QueryStats->Getquery_ast();

            UNIT_ASSERT_C(ast.find("KqpOlapFilter") != std::string::npos,
                          TStringBuilder() << "Predicate not pushed down. Query: " << query);
        }
    }

    Y_UNIT_TEST(OlapLayout) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        settings.AppConfig.MutableColumnShardConfig()->MutableTablesStorageLayoutPolicy()->MutableIdentityGroups();
        Y_ABORT_UNLESS(settings.AppConfig.GetColumnShardConfig().GetTablesStorageLayoutPolicy().HasIdentityGroups());
        TKikimrRunner kikimr(settings);
        TLocalHelper(kikimr).CreateTestOlapTable("olapTable", "olapStore1", 20, 4);
        UNIT_ASSERT(TExtLocalHelper(kikimr).TryCreateTable("olapStore1", "olapTable_1", 5));
        UNIT_ASSERT(TExtLocalHelper(kikimr).TryCreateTable("olapStore1", "olapTable_2", 5));
        UNIT_ASSERT(TExtLocalHelper(kikimr).TryCreateTable("olapStore1", "olapTable_3", 5));
        UNIT_ASSERT(TExtLocalHelper(kikimr).TryCreateTable("olapStore1", "olapTable_4", 5));

        TLocalHelper(kikimr).CreateTestOlapTable("olapTable", "olapStore", 16, 4);
        UNIT_ASSERT(TExtLocalHelper(kikimr).TryCreateTable("olapStore", "olapTable_1", 4));
        UNIT_ASSERT(TExtLocalHelper(kikimr).TryCreateTable("olapStore", "olapTable_2", 4));
        UNIT_ASSERT(TExtLocalHelper(kikimr).TryCreateTable("olapStore", "olapTable_4", 2));
        UNIT_ASSERT(TExtLocalHelper(kikimr).TryCreateTable("olapStore", "olapTable_5", 1));
        UNIT_ASSERT(TExtLocalHelper(kikimr).TryCreateTable("olapStore", "olapTable_6", 1));
        UNIT_ASSERT(!TExtLocalHelper(kikimr).TryCreateTable("olapStore", "olapTable_7", 5));
        UNIT_ASSERT(!TExtLocalHelper(kikimr).TryCreateTable("olapStore", "olapTable_8", 3));
        UNIT_ASSERT(!TExtLocalHelper(kikimr).TryCreateTable("olapStore", "olapTable_9", 8));
        UNIT_ASSERT(!TExtLocalHelper(kikimr).TryCreateTable("olapStore", "olapTable_10", 17));
        UNIT_ASSERT(TExtLocalHelper(kikimr).TryCreateTable("olapStore", "olapTable_11", 2));
        UNIT_ASSERT(TExtLocalHelper(kikimr).TryCreateTable("olapStore", "olapTable_12", 1));

        UNIT_ASSERT(TExtLocalHelper(kikimr).DropTable("olapStore", "olapTable_1"));
        UNIT_ASSERT(TExtLocalHelper(kikimr).DropTable("olapStore", "olapTable_2"));
        UNIT_ASSERT(TExtLocalHelper(kikimr).DropTable("olapStore", "olapTable_4"));
        UNIT_ASSERT(TExtLocalHelper(kikimr).DropTable("olapStore", "olapTable_5"));
        UNIT_ASSERT(TExtLocalHelper(kikimr).DropTable("olapStore", "olapTable_6"));

        UNIT_ASSERT(TExtLocalHelper(kikimr).TryCreateTable("olapStore", "olapTable_7", 5));
        UNIT_ASSERT(TExtLocalHelper(kikimr).TryCreateTable("olapStore", "olapTable_8", 2));
        UNIT_ASSERT(!TExtLocalHelper(kikimr).TryCreateTable("olapStore", "olapTable_9", 9));

        UNIT_ASSERT(TExtLocalHelper(kikimr).DropTable("olapStore", "olapTable"));
        UNIT_ASSERT(TExtLocalHelper(kikimr).DropTable("olapStore", "olapTable_12"));
        UNIT_ASSERT(TExtLocalHelper(kikimr).DropTable("olapStore", "olapTable_11"));
        UNIT_ASSERT(TExtLocalHelper(kikimr).TryCreateTable("olapStore", "olapTable_9", 9));
    }

    void TestOlapUpsert(ui32 numShards) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto tableClient = kikimr.GetTableClient();
        auto session = tableClient.CreateSession().GetValueSync().GetSession();

        auto query = TStringBuilder() << R"(
            --!syntax_v1
            CREATE TABLE `/Root/test_table`
            (
                WatchID Int64 NOT NULL,
                CounterID Int32 NOT NULL,
                URL Text NOT NULL,
                Age Int16 NOT NULL,
                Sex Int16 NOT NULL,
                PRIMARY KEY (CounterID, WatchID)
            )
            PARTITION BY HASH(WatchID)
            WITH (
                STORE = COLUMN,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT =)" << numShards
            << ")";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session.ExecuteDataQuery(R"(
            UPSERT INTO `/Root/test_table` (WatchID, CounterID, URL, Age, Sex) VALUES
                (0, 15, 'aaaaaaa', 23, 1),
                (0, 15, 'bbbbbbb', 23, 1),
                (1, 15, 'ccccccc', 23, 1);
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync(); // TODO: snapshot isolation?

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        {
            TString query = R"(
                --!syntax_v1
                SELECT CounterID, WatchID
                FROM `/Root/test_table`
                ORDER BY CounterID, WatchID
            )";

            auto it = tableClient.StreamExecuteScanQuery(query).GetValueSync();

            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            TString result = StreamResultToYson(it);
            Cout << result << Endl;
            //CompareYson(result, R"([[0;15];[1;15]])");
            CompareYson(result, R"([])"); // FIXME
        }
    }

    Y_UNIT_TEST(OlapUpsertImmediate) {
        // Should be fixed in KIKIMR-17646
        return;

        TestOlapUpsert(1);
    }

    Y_UNIT_TEST(OlapUpsert) {
        // Should be fixed in KIKIMR-17646
        return;

        TestOlapUpsert(2);
    }

    Y_UNIT_TEST(OlapDeleteImmediate) {
        // Should be fixed in KIKIMR-17582
        return;

        TPortManager pm;
        ui32 grpcPort = pm.GetPort();
        ui32 msgbPort = pm.GetPort();

        Tests::TServerSettings serverSettings(msgbPort);
        serverSettings.Port = msgbPort;
        serverSettings.GrpcPort = grpcPort;
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableMetadataProvider(true)
            .SetForceColumnTablesCompositeMarks(true);
        ;

        Tests::TServer::TPtr server = new Tests::TServer(serverSettings);
        server->EnableGRpc(grpcPort);
        //        server->SetupDefaultProfiles();
        Tests::TClient client(serverSettings);

        auto& runtime = *server->GetRuntime();
        Tests::NCommon::TLoggerInit(runtime).Initialize();

        auto sender = runtime.AllocateEdgeActor();
        server->SetupRootStoragePools(sender);
        Tests::NCommon::THelper lHelper(*server);
        lHelper.StartSchemaRequest(
            R"(
                CREATE TABLE `/Root/test_table`
                (
                    WatchID Int64 NOT NULL,
                    CounterID Int32 NOT NULL,
                    URL Text NOT NULL,
                    Age Int16 NOT NULL,
                    Sex Int16 NOT NULL,
                    PRIMARY KEY (CounterID, WatchID)
                )
                PARTITION BY HASH(WatchID)
                WITH (
                    STORE = COLUMN,
                    AUTO_PARTITIONING_BY_SIZE = ENABLED,
                    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1
                );
            )"
        );

        lHelper.StartDataRequest(
            R"(
                DELETE FROM `/Root/test_table` WHERE WatchID = 1;
            )"
        );

    }

    Y_UNIT_TEST(OlapDeleteImmediatePK) {
        // Should be fixed in KIKIMR-17582
        return;

        TPortManager pm;
        ui32 grpcPort = pm.GetPort();
        ui32 msgbPort = pm.GetPort();

        Tests::TServerSettings serverSettings(msgbPort);
        serverSettings.Port = msgbPort;
        serverSettings.GrpcPort = grpcPort;
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableMetadataProvider(true)
            .SetForceColumnTablesCompositeMarks(true);
        ;

        Tests::TServer::TPtr server = new Tests::TServer(serverSettings);
        server->EnableGRpc(grpcPort);
        //        server->SetupDefaultProfiles();
        Tests::TClient client(serverSettings);

        auto& runtime = *server->GetRuntime();
        Tests::NCommon::TLoggerInit(runtime).Initialize();

        auto sender = runtime.AllocateEdgeActor();
        server->SetupRootStoragePools(sender);
        Tests::NCommon::THelper lHelper(*server);
        lHelper.StartSchemaRequest(
            R"(
                CREATE TABLE `/Root/test_table`
                (
                    WatchID Int64 NOT NULL,
                    CounterID Int32 NOT NULL,
                    URL Text NOT NULL,
                    Age Int16 NOT NULL,
                    Sex Int16 NOT NULL,
                    PRIMARY KEY (CounterID, WatchID)
                )
                PARTITION BY HASH(WatchID)
                WITH (
                    STORE = COLUMN,
                    AUTO_PARTITIONING_BY_SIZE = ENABLED,
                    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1
                );
            )"
        );

        lHelper.StartDataRequest(
            R"(
                DELETE FROM `/Root/test_table` WHERE WatchID = 1 AND CounterID = 1;
            )"
        );

    }
/*
    Y_UNIT_TEST(OlapDeletePlanned) {
        TPortManager pm;

        ui32 grpcPort = pm.GetPort();
        ui32 msgbPort = pm.GetPort();

        Tests::TServerSettings serverSettings(msgbPort);
        serverSettings.Port = msgbPort;
        serverSettings.GrpcPort = grpcPort;
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableMetadataProvider(true)
            .SetForceColumnTablesCompositeMarks(true);
        ;

        Tests::TServer::TPtr server = new Tests::TServer(serverSettings);
        server->EnableGRpc(grpcPort);
        //        server->SetupDefaultProfiles();
        Tests::TClient client(serverSettings);

        auto& runtime = *server->GetRuntime();

        auto sender = runtime.AllocateEdgeActor();
        server->SetupRootStoragePools(sender);
        Tests::NCommon::THelper lHelper(*server);
        lHelper.StartSchemaRequest(
            R"(
                CREATE TABLE `/Root/test_table`
                (
                    WatchID Int64 NOT NULL,
                    CounterID Int32 NOT NULL,
                    URL Text NOT NULL,
                    Age Int16 NOT NULL,
                    Sex Int16 NOT NULL,
                    PRIMARY KEY (CounterID, WatchID)
                )
                PARTITION BY HASH(WatchID)
                WITH (
                    STORE = COLUMN,
                    AUTO_PARTITIONING_BY_SIZE = ENABLED,
                    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 8
                );
            )"
        );

        lHelper.StartDataRequest(
            R"(
                DELETE FROM `/Root/test_table` WHERE WatchID = 0;
            )"
#if 1 // TODO
            , false
#endif
        );
    }
*/

    Y_UNIT_TEST(PredicatePushdownCastErrors) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TStreamExecScanQuerySettings scanSettings;
        scanSettings.Explain(true);

        CreateTableOfAllTypes(kikimr);

        auto tableClient = kikimr.GetTableClient();

#if SSA_RUNTIME_VERSION >= 4U
        const std::set<std::string> numerics = {"Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64", "Float", "Double"};
        const std::map<std::string, std::set<std::string>> exceptions = {
            {"Int8", numerics},
            {"Int16", numerics},
            {"Int32", numerics},
            {"Int64", numerics},
            {"UInt8", numerics},
            {"UInt16", numerics},
            {"UInt32", numerics},
            {"UInt64", numerics},
            {"Float", numerics},
            {"Double", numerics},
            {"String", {"Utf8"}},
            {"Utf8", {"String"}},
        };
#else
        std::map<std::string, std::set<std::string>> exceptions = {
            {"Int8", {"Int16", "Int32"}},
            {"Int16", {"Int8", "Int32"}},
            {"Int32", {"Int8", "Int16"}},
            {"UInt8", {"UInt16", "UInt32"}},
            {"UInt16", {"UInt8", "UInt32"}},
            {"UInt32", {"UInt8", "UInt16"}},
            {"String", {"Utf8"}},
            {"Utf8", {"String", "Json", "Yson"}},
        };
#endif

        std::vector<std::string> allTypes = {
            //"Bool",
            "Int8",
            "Int16",
            "Int32",
            "Int64",
            "UInt8",
            "UInt16",
            "UInt32",
            "UInt64",
            "Double",
            "Float",
            //"Decimal(12,9)",
            "String",
            "Utf8",
            "Timestamp",
            "Date",
            "Datetime",
            //"Interval"
        };

        std::map<std::string, TParams> parameters = CreateParametersOfAllTypes(tableClient);

        const std::vector<std::string> predicates = {
            "<--NAME-->_column > $in_value",
            "<--NAME-->_column = $in_value",
            "$in_value > <--NAME-->_column",
            "$in_value = <--NAME-->_column",
        };

        const TString queryBegin = R"(
            --!syntax_v1
            DECLARE $in_value AS <--TYPE-->;
            SELECT `key` FROM `/Root/olapStore/OlapParametersTable` WHERE
        )";

        std::vector<std::string> falsePositive;
        std::vector<std::string> falseNegative;

        for (const auto& predicateTemplate: predicates) {
            for (const auto& type: allTypes) {
                for (const auto& checkType: allTypes) {
                    bool error = true;

                    auto exc = exceptions.find(checkType);

                    if (exc != exceptions.end() && exc->second.contains(type)) {
                        error = false;
                    } else if (type == checkType) {
                        error = false;
                    }

                    std::string clearType = type;

                    size_t pos = clearType.find('(');

                    if (std::string::npos != pos) {
                        clearType = clearType.substr(0, pos);
                    }

                    TString query(queryBegin);
                    TString predicate(predicateTemplate);
                    SubstGlobal(query, "<--TYPE-->", checkType);
                    SubstGlobal(predicate, "<--NAME-->", clearType);

                    auto parameter = parameters.find(checkType);

                    UNIT_ASSERT_C(parameter != parameters.end(), "No type " << checkType << " in parameters");

                    Cerr << "Test query:\n" << query + predicate << Endl;

                    auto it = tableClient.StreamExecuteScanQuery(query + predicate, parameter->second).GetValueSync();
                    // Check for successful execution
                    auto streamPart = it.ReadNext().GetValueSync();

                    bool pushdown;

                    if (streamPart.IsSuccess()) {
                        it = tableClient.StreamExecuteScanQuery(
                            query + predicate, parameter->second, scanSettings
                        ).GetValueSync();

                        auto result = CollectStreamResult(it);
                        auto ast = result.QueryStats->Getquery_ast();

                        pushdown = ast.find("KqpOlapFilter") != std::string::npos;
                    } else {
                        // Error means that predicate not pushed down
                        pushdown = false;
                    }

                    if (error && pushdown) {
                        falsePositive.emplace_back(
                            TStringBuilder() << type << " vs " << checkType << " at " << predicate
                        );
                        continue;
                    }

                    if (!error && !pushdown) {
                        falseNegative.emplace_back(
                            TStringBuilder() << type << " vs " << checkType << " at " << predicate
                        );
                    }
                }
            }
        }

        TStringBuilder b;
        b << "Errors found:" << Endl;
        b << "------------------------------------------------" << Endl;
        b << "False positive" << Endl;

        for (const auto& txt: falsePositive) {
            b << txt << Endl;
        }

        b << "False negative" << Endl;
        for (const auto& txt: falseNegative) {
            b << txt << Endl;
        }

        b << "------------------------------------------------" << Endl;
        UNIT_ASSERT_C(falsePositive.empty() && falseNegative.empty(), b);
    }

    Y_UNIT_TEST(NoErrorOnLegacyPragma) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                PRAGMA Kikimr.KqpPushOlapProcess = "false";
                SELECT id, resource_id FROM `/Root/tableWithNulls`
                WHERE
                    level = 5;
            )")
            .SetExpectedReply("[[5;#]]")
            .AddExpectedPlanOptions("KqpOlapFilter");

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(BlocksRead) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                PRAGMA UseBlocks;
                PRAGMA Kikimr.OptEnableOlapPushdown = "false";

                SELECT
                    id, resource_id
                FROM `/Root/tableWithNulls`
                WHERE
                    level = 5;
            )")
            .SetExpectedReply("[[5;#]]");

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Blocks_NoAggPushdown) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                PRAGMA UseBlocks;
                SELECT
                    COUNT(DISTINCT id)
                FROM `/Root/tableWithNulls`;
            )")
            .SetExpectedReply("[[10u]]");

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Json_GetValue) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT id, JSON_VALUE(jsonval, "$.col1"), JSON_VALUE(jsondoc, "$.col1") FROM `/Root/tableWithNulls`
                WHERE JSON_VALUE(jsonval, "$.col1") = "val1" AND id = 1;
            )")
#if SSA_RUNTIME_VERSION >= 3U
            .AddExpectedPlanOptions("KqpOlapJsonValue")
#else
            .AddExpectedPlanOptions("Udf")
#endif
            .SetExpectedReply(R"([[1;["val1"];#]])");

        TestTableWithNulls({testCase});
    }

    Y_UNIT_TEST(Json_GetValue_Minus) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT id, JSON_VALUE(jsonval, "$.'col-abc'"), JSON_VALUE(jsondoc, "$.'col-abc'") FROM `/Root/tableWithNulls`
                WHERE JSON_VALUE(jsonval, "$.'col-abc'") = "val-abc" AND id = 1;
            )")
#if SSA_RUNTIME_VERSION >= 3U
            .AddExpectedPlanOptions("KqpOlapJsonValue")
#else
            .AddExpectedPlanOptions("Udf")
#endif
            .SetExpectedReply(R"([[1;["val-abc"];#]])");

        TestTableWithNulls({testCase});
    }

    Y_UNIT_TEST(Json_GetValue_ToString) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT id, JSON_VALUE(jsonval, "$.col1" RETURNING String), JSON_VALUE(jsondoc, "$.col1") FROM `/Root/tableWithNulls`
                WHERE JSON_VALUE(jsonval, "$.col1" RETURNING String) = "val1" AND id = 1;
            )")
#if SSA_RUNTIME_VERSION >= 3U
            .AddExpectedPlanOptions("KqpOlapJsonValue")
#else
            .AddExpectedPlanOptions("Udf")
#endif
            .SetExpectedReply(R"([[1;["val1"];#]])");

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Json_GetValue_ToInt) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT id, JSON_VALUE(jsonval, "$.obj.obj_col2_int" RETURNING Int), JSON_VALUE(jsondoc, "$.obj.obj_col2_int" RETURNING Int) FROM `/Root/tableWithNulls`
                WHERE JSON_VALUE(jsonval, "$.obj.obj_col2_int" RETURNING Int) = 16 AND id = 1;
            )")
#if SSA_RUNTIME_VERSION >= 3U
            .AddExpectedPlanOptions("KqpOlapJsonValue")
#else
            .AddExpectedPlanOptions("Udf")
#endif
            .SetExpectedReply(R"([[1;[16];#]])");

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(JsonDoc_GetValue) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT id, JSON_VALUE(jsonval, "$.col1"), JSON_VALUE(jsondoc, "$.col1") FROM `/Root/tableWithNulls`
                WHERE JSON_VALUE(jsondoc, "$.col1") = "val1" AND id = 6;
            )")
#if SSA_RUNTIME_VERSION >= 3U
            .AddExpectedPlanOptions("KqpOlapJsonValue")
#else
            .AddExpectedPlanOptions("Udf")
#endif
            .SetExpectedReply(R"([[6;#;["val1"]]])");

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(JsonDoc_GetValue_ToString) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT id, JSON_VALUE(jsonval, "$.col1"), JSON_VALUE(jsondoc, "$.col1" RETURNING String) FROM `/Root/tableWithNulls`
                WHERE JSON_VALUE(jsondoc, "$.col1" RETURNING String) = "val1" AND id = 6;
            )")
#if SSA_RUNTIME_VERSION >= 3U
            .AddExpectedPlanOptions("KqpOlapJsonValue")
#else
            .AddExpectedPlanOptions("Udf")
#endif
            .SetExpectedReply(R"([[6;#;["val1"]]])");

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(JsonDoc_GetValue_ToInt) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT id, JSON_VALUE(jsonval, "$.obj.obj_col2_int"), JSON_VALUE(jsondoc, "$.obj.obj_col2_int" RETURNING Int) FROM `/Root/tableWithNulls`
                WHERE JSON_VALUE(jsondoc, "$.obj.obj_col2_int" RETURNING Int) = 16 AND id = 6;
            )")
#if SSA_RUNTIME_VERSION >= 3U
            .AddExpectedPlanOptions("KqpOlapJsonValue")
#else
            .AddExpectedPlanOptions("Udf")
#endif
            .SetExpectedReply(R"([[6;#;[16]]])");

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Json_Exists) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT id, JSON_EXISTS(jsonval, "$.col1"), JSON_EXISTS(jsondoc, "$.col1") FROM `/Root/tableWithNulls`
                WHERE
                    JSON_EXISTS(jsonval, "$.col1") AND level = 1;
            )")
#if SSA_RUNTIME_VERSION >= 3U
            .AddExpectedPlanOptions("KqpOlapJsonExists")
#else
            .AddExpectedPlanOptions("Udf")
#endif
            .SetExpectedReply(R"([[1;[%true];#]])");

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(JsonDoc_Exists) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT id, JSON_EXISTS(jsonval, "$.col1"), JSON_EXISTS(jsondoc, "$.col1") FROM `/Root/tableWithNulls`
                WHERE
                    JSON_EXISTS(jsondoc, "$.col1") AND id = 6;
            )")
#if SSA_RUNTIME_VERSION >= 3U
            .AddExpectedPlanOptions("KqpOlapJsonExists")
#else
            .AddExpectedPlanOptions("Udf")
#endif
            .SetExpectedReply(R"([[6;#;[%true]]])");

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Json_Query) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT id, JSON_QUERY(jsonval, "$.col1" WITH UNCONDITIONAL WRAPPER),
                    JSON_QUERY(jsondoc, "$.col1" WITH UNCONDITIONAL WRAPPER)
                FROM `/Root/tableWithNulls`
                WHERE
                    level = 1;
            )")
            .AddExpectedPlanOptions("Udf")
            .SetExpectedReply(R"([[1;["[\"val1\"]"];#]])");

        TestTableWithNulls({ testCase });
    }

    Y_UNIT_TEST(Olap_InsertFailsOnDataQuery) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetForceColumnTablesCompositeMarks(true);
        TKikimrRunner kikimr(settings);

        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        TTableWithNullsHelper(kikimr).CreateTableWithNulls();

        auto tableClient = kikimr.GetTableClient();

        auto session = tableClient.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            INSERT INTO `/Root/tableWithNulls`(id, resource_id, level) VALUES(1, "1", 1);
        )", TTxControl::BeginTx().CommitTx()).GetValueSync();

        UNIT_ASSERT(!result.IsSuccess());
        std::string errorMsg = result.GetIssues().ToString();
        UNIT_ASSERT_C(errorMsg.find("Write mode 'insert_abort' is not supported for olap tables.") != std::string::npos, errorMsg);
    }

    Y_UNIT_TEST(Olap_InsertFailsOnGenericQuery) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetForceColumnTablesCompositeMarks(true);
        TKikimrRunner kikimr(settings);

        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        TTableWithNullsHelper(kikimr).CreateTableWithNulls();

        auto db = kikimr.GetQueryClient();

        auto result = db.ExecuteQuery(R"(
            INSERT INTO `/Root/tableWithNulls`(id, resource_id, level) VALUES(1, "1", 1);
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();

        UNIT_ASSERT(!result.IsSuccess());
        std::string errorMsg = result.GetIssues().ToString();
        UNIT_ASSERT_C(errorMsg.find("Write mode 'insert_abort' is not supported for olap tables.") != std::string::npos, errorMsg);
    }

    Y_UNIT_TEST(OlapRead_FailsOnDataQuery) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetForceColumnTablesCompositeMarks(true);
        TKikimrRunner kikimr(settings);

        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        TTableWithNullsHelper(kikimr).CreateTableWithNulls();
        TLocalHelper(kikimr).CreateTestOlapTable();

        auto tableClient = kikimr.GetTableClient();

        {
            WriteTestDataForTableWithNulls(kikimr, "/Root/tableWithNulls");
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 2);
        }

        auto session = tableClient.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/tableWithNulls`;
        )", TTxControl::BeginTx().CommitTx()).GetValueSync();

        UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
    }

    Y_UNIT_TEST(OlapRead_UsesScanOnJoin) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetForceColumnTablesCompositeMarks(true);
        TKikimrRunner kikimr(settings);

        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        TTableWithNullsHelper(kikimr).CreateTableWithNulls();
        TLocalHelper(kikimr).CreateTestOlapTable();

        {
            WriteTestDataForTableWithNulls(kikimr, "/Root/tableWithNulls");
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 2);
        }

        NScripting::TScriptingClient client(kikimr.GetDriver());
        auto result = client.ExecuteYqlScript(R"(
            SELECT * FROM `/Root/olapStore/olapTable` WHERE resource_id IN (SELECT CAST(id AS Utf8) FROM `/Root/tableWithNulls`);
        )").GetValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    Y_UNIT_TEST(OlapRead_UsesScanOnJoinWithDataShardTable) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetForceColumnTablesCompositeMarks(true);
        TKikimrRunner kikimr(settings);

        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        TTableWithNullsHelper(kikimr).CreateTableWithNulls();
        TLocalHelper(kikimr).CreateTestOlapTable();

        {
            WriteTestDataForTableWithNulls(kikimr, "/Root/tableWithNulls");
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 2);
        }

        NScripting::TScriptingClient client(kikimr.GetDriver());
        auto result = client.ExecuteYqlScript(R"(
            SELECT * FROM `/Root/olapStore/olapTable` WHERE resource_id IN (SELECT CAST(id AS Utf8) FROM `/Root/tableWithNulls`);
        )").GetValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    Y_UNIT_TEST(OlapRead_UsesGenericQueryOnJoinWithDataShardTable) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetForceColumnTablesCompositeMarks(true);
        TKikimrRunner kikimr(settings);

        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        TTableWithNullsHelper(kikimr).CreateTableWithNulls();
        TLocalHelper(kikimr).CreateTestOlapTable();

        {
            WriteTestDataForTableWithNulls(kikimr, "/Root/tableWithNulls");
            WriteTestData(kikimr, "/Root/olapStore/olapTable", 0, 1000000, 2);
        }

        auto db = kikimr.GetQueryClient();
        auto result = db.ExecuteQuery(R"(
            SELECT timestamp, resource_id, uid, level FROM `/Root/olapStore/olapTable` WHERE resource_id IN (SELECT CAST(id AS Utf8) FROM `/Root/tableWithNulls`);
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        TString output = FormatResultSetYson(result.GetResultSet(0));
        Cout << output << Endl;
        CompareYson(output, R"([[1000001u;["1"];"uid_1000001";[1]]])");
    }

    Y_UNIT_TEST(OlapRead_GenericQuery) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetForceColumnTablesCompositeMarks(true);
        TKikimrRunner kikimr(settings);

        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        TTableWithNullsHelper(kikimr).CreateTableWithNulls();
        TLocalHelper(kikimr).CreateTestOlapTable();

        {
            WriteTestDataForTableWithNulls(kikimr, "/Root/tableWithNulls");
        }

        auto db = kikimr.GetQueryClient();

        auto result = db.ExecuteQuery(R"(
            SELECT COUNT(*) FROM `/Root/tableWithNulls`;
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        TString output = FormatResultSetYson(result.GetResultSet(0));
        Cout << output << Endl;
        CompareYson(output, R"([[10u;]])");
    }

    Y_UNIT_TEST(OlapRead_StreamGenericQuery) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetForceColumnTablesCompositeMarks(true);
        TKikimrRunner kikimr(settings);

        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        TTableWithNullsHelper(kikimr).CreateTableWithNulls();
        TLocalHelper(kikimr).CreateTestOlapTable();

        {
            WriteTestDataForTableWithNulls(kikimr, "/Root/tableWithNulls");
        }

        auto db = kikimr.GetQueryClient();

        auto it = db.StreamExecuteQuery(R"(
            SELECT COUNT(*) FROM `/Root/tableWithNulls`;
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
        TString output = StreamResultToYson(it);
        Cout << output << Endl;
        CompareYson(output, R"([[10u;]])");
    }

    Y_UNIT_TEST(OlapRead_ScanQuery) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetForceColumnTablesCompositeMarks(true);
        TKikimrRunner kikimr(settings);

        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        TTableWithNullsHelper(kikimr).CreateTableWithNulls();
        TLocalHelper(kikimr).CreateTestOlapTable();

        {
            WriteTestDataForTableWithNulls(kikimr, "/Root/tableWithNulls");
        }

        NScripting::TScriptingClient client(kikimr.GetDriver());
        auto result = client.ExecuteYqlScript(R"(
            SELECT COUNT(*) FROM `/Root/tableWithNulls`;
        )").GetValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        TString output = FormatResultSetYson(result.GetResultSet(0));
        Cout << output << Endl;
        CompareYson(output, R"([[10u;]])");
    }

    Y_UNIT_TEST(DuplicatesInIncomingBatch) {
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        TTestHelper testHelper(runnerSettings);
        Tests::NCommon::TLoggerInit(testHelper.GetRuntime()).Initialize();
        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("id_second").SetType(NScheme::NTypeIds::Utf8).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("resource_id").SetType(NScheme::NTypeIds::Utf8),
            TTestHelper::TColumnSchema().SetName("level").SetType(NScheme::NTypeIds::Int32)
        };
        TTestHelper::TColumnTable testTable;

        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({"id", "id_second"}).SetSharding({"id"}).SetSchema(schema);
        testHelper.CreateTable(testTable);

        {
            TTestHelper::TUpdatesBuilder tableInserter(testTable.GetArrowSchema(schema));
            tableInserter.AddRow().Add(1).Add("test_res_1").AddNull().AddNull();
            tableInserter.AddRow().Add(2).Add("test_res_2").Add("val1").AddNull();
            tableInserter.AddRow().Add(3).Add("test_res_3").Add("val3").AddNull();
            tableInserter.AddRow().Add(2).Add("test_res_2").Add("val2").AddNull();
            testHelper.BulkUpsert(testTable, tableInserter);
        }
        while (csController->GetIndexations().Val() == 0) {
            Cout << "Wait indexation..." << Endl;
            Sleep(TDuration::Seconds(2));
        }
        testHelper.ReadData("SELECT * FROM `/Root/ColumnTableTest` WHERE id=2", "[[2;\"test_res_2\";#;[\"val1\"]]]");
    }

    Y_UNIT_TEST(BulkUpsertUpdate) {
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        TTestHelper testHelper(runnerSettings);

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int64).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("value").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
        };

        TTestHelper::TColumnTable testTable;
        testTable.SetName("/Root/ColumnTableTest").SetPrimaryKey({ "id" }).SetSharding({ "id" }).SetSchema(schema);
        testHelper.CreateTable(testTable);
        {
            TTestHelper::TUpdatesBuilder tableInserter(testTable.GetArrowSchema(schema));
            tableInserter.AddRow().Add(1).Add(10);
            testHelper.BulkUpsert(testTable, tableInserter);
        }
        while (csController->GetIndexations().Val() < 1) {
            Cout << "Wait indexation..." << Endl;
            Sleep(TDuration::Seconds(2));
        }
        testHelper.ReadData("SELECT value FROM `/Root/ColumnTableTest` WHERE id = 1", "[[10]]");
        {
            TTestHelper::TUpdatesBuilder tableInserter(testTable.GetArrowSchema(schema));
            tableInserter.AddRow().Add(1).Add(110);
            testHelper.BulkUpsert(testTable, tableInserter);
        }
        testHelper.ReadData("SELECT value FROM `/Root/ColumnTableTest` WHERE id = 1", "[[110]]");
        while (csController->GetIndexations().Val() < 2) {
            Cout << "Wait indexation..." << Endl;
            Sleep(TDuration::Seconds(2));
        }
        testHelper.ReadData("SELECT value FROM `/Root/ColumnTableTest` WHERE id = 1", "[[110]]");
    }

    Y_UNIT_TEST(OlapReplace_FromSelectSimple) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        auto settings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        TTableWithNullsHelper(kikimr).CreateTableWithNulls();

        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        const TString query = R"(
            CREATE TABLE `/Root/ColumnSource` (
                Col1 Uint64 NOT NULL,
                Col2 String,
                Col3 Int32 NOT NULL,
                PRIMARY KEY (Col1)
            )
            PARTITION BY HASH(Col1)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10);

            CREATE TABLE `/Root/DataShard1` (
                Col1 Uint64 NOT NULL,
                Col2 String,
                Col3 Int32 NOT NULL,
                PRIMARY KEY (Col1)
            )
            WITH (UNIFORM_PARTITIONS = 2, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2);

            CREATE TABLE `/Root/ColumnShard1` (
                Col1 Uint64 NOT NULL,
                Col2 String,
                Col3 Int32 NOT NULL,
                PRIMARY KEY (Col1)
            )
            PARTITION BY HASH(Col1)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 3);

            CREATE TABLE `/Root/ColumnShard2` (
                Col1 Uint64 NOT NULL,
                Col2 String,
                Col3 Int32 NOT NULL,
                PRIMARY KEY (Col1)
            )
            PARTITION BY HASH(Col1)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 16);

            CREATE TABLE `/Root/ColumnShard3` (
                Col1 Uint64 NOT NULL,
                Col2 String,
                Col3 Int32 NOT NULL,
                PRIMARY KEY (Col1)
            )
            PARTITION BY HASH(Col1)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 16);

            CREATE TABLE `/Root/DataShard2` (
                Col1 Uint64 NOT NULL,
                Col2 String,
                Col3 Int32 NOT NULL,
                PRIMARY KEY (Col1)
            )
            WITH (UNIFORM_PARTITIONS = 3, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 3);
        )";

        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        auto client = kikimr.GetQueryClient();
        {
            auto prepareResult = client.ExecuteQuery(R"(
                REPLACE INTO `/Root/ColumnSource` (Col1, Col2, Col3) VALUES
                    (1u, "test1", 10), (2u, "test2", 11), (3u, "test3", 12), (4u, NULL, 13);
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        }

        {
            // Missing Nullable column
            const TString sql = R"(
                REPLACE INTO `/Root/ColumnShard1`
                SELECT 10u + Col1 AS Col1, 100 + Col3 AS Col3 FROM `/Root/ColumnSource`
            )";
            auto insertResult = client.ExecuteQuery(sql, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(insertResult.IsSuccess(), insertResult.GetIssues().ToString());

            auto it = client.StreamExecuteQuery(R"(
                SELECT * FROM `/Root/ColumnShard1` ORDER BY Col1, Col2, Col3;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(
                output,
                R"([[11u;#;110];[12u;#;111];[13u;#;112];[14u;#;113]])");
        }

        {
            // column -> column
            const TString sql = R"(
                REPLACE INTO `/Root/ColumnShard2`
                SELECT * FROM `/Root/ColumnSource`
            )";
            auto insertResult = client.ExecuteQuery(sql, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(insertResult.IsSuccess(), insertResult.GetIssues().ToString());

            auto it = client.StreamExecuteQuery(R"(
                SELECT * FROM `/Root/ColumnShard2` ORDER BY Col1, Col2, Col3;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(
                output,
                R"([[1u;["test1"];10];[2u;["test2"];11];[3u;["test3"];12];[4u;#;13]])");
        }
    }

    Y_UNIT_TEST(OlapReplace_BadTransactions) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        auto settings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        TTableWithNullsHelper(kikimr).CreateTableWithNulls();

        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        const TString query = R"(
            CREATE TABLE `/Root/ColumnShard` (
                Col1 Uint64 NOT NULL,
                Col2 String,
                Col3 Int32 NOT NULL,
                PRIMARY KEY (Col1)
            )
            PARTITION BY HASH(Col1)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10);

            CREATE TABLE `/Root/DataShard` (
                Col1 Uint64 NOT NULL,
                Col2 String,
                Col3 Int32 NOT NULL,
                PRIMARY KEY (Col1)
            )
            WITH (UNIFORM_PARTITIONS = 2, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2);
        )";

        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        auto client = kikimr.GetQueryClient();
        {
            auto prepareResult = client.ExecuteQuery(R"(
                REPLACE INTO `/Root/ColumnShard` (Col1, Col2, Col3) VALUES
                    (1u, "test1", 10), (2u, "test2", 11), (3u, "test3", 12), (4u, NULL, 13);
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        }
        {
            auto prepareResult = client.ExecuteQuery(R"(
                REPLACE INTO `/Root/DataShard` (Col1, Col2, Col3) VALUES
                    (10u, "test1", 10), (20u, "test2", 11), (30u, "test3", 12), (40u, NULL, 13);
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        }

        {
            // column -> row
            const TString sql = R"(
                REPLACE INTO `/Root/DataShard`
                SELECT * FROM `/Root/ColumnShard`
            )";
            auto insertResult = client.ExecuteQuery(sql, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT(!insertResult.IsSuccess());
            UNIT_ASSERT_C(
                insertResult.GetIssues().ToString().Contains("Transactions between column and row tables are disabled at current time"),
                insertResult.GetIssues().ToString());
        }

        {
            // row -> column
            const TString sql = R"(
                REPLACE INTO `/Root/ColumnShard`
                SELECT * FROM `/Root/DataShard`
            )";
            auto insertResult = client.ExecuteQuery(sql, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT(!insertResult.IsSuccess());
            UNIT_ASSERT_C(
                insertResult.GetIssues().ToString().Contains("Transactions between column and row tables are disabled at current time"),
                insertResult.GetIssues().ToString());
        }

        {
            // column & row read
            const TString sql = R"(
                SELECT * FROM `/Root/DataShard`;
                SELECT * FROM `/Root/ColumnShard`;
            )";
            auto insertResult = client.ExecuteQuery(sql, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT(!insertResult.IsSuccess());
            UNIT_ASSERT_C(
                insertResult.GetIssues().ToString().Contains("Transactions between column and row tables are disabled at current time"),
                insertResult.GetIssues().ToString());
        }

        {
            // column & row write
            const TString sql = R"(
                REPLACE INTO `/Root/DataShard` (Col1, Col2, Col3) VALUES
                    (1u, "test1", 10), (2u, "test2", 11), (3u, "test3", 12), (4u, NULL, 13);
                REPLACE INTO `/Root/ColumnShard` (Col1, Col2, Col3) VALUES
                    (1u, "test1", 10), (2u, "test2", 11), (3u, "test3", 12), (4u, NULL, 13);
            )";
            auto insertResult = client.ExecuteQuery(sql, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT(!insertResult.IsSuccess());
            UNIT_ASSERT_C(
                insertResult.GetIssues().ToString().Contains("Transactions between column and row tables are disabled at current time"),
                insertResult.GetIssues().ToString());
        }

        {
            // column read & row write
            const TString sql = R"(
                REPLACE INTO `/Root/DataShard` (Col1, Col2, Col3) VALUES
                    (1u, "test1", 10), (2u, "test2", 11), (3u, "test3", 12), (4u, NULL, 13);
                SELECT * FROM `/Root/ColumnShard`;
            )";
            auto insertResult = client.ExecuteQuery(sql, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT(!insertResult.IsSuccess());
            UNIT_ASSERT_C(
                insertResult.GetIssues().ToString().Contains("Transactions between column and row tables are disabled at current time"),
                insertResult.GetIssues().ToString());
        }

        {
            // column write & row read
            const TString sql = R"(
                REPLACE INTO `/Root/ColumnShard` (Col1, Col2, Col3) VALUES
                    (1u, "test1", 10), (2u, "test2", 11), (3u, "test3", 12), (4u, NULL, 13);
                SELECT * FROM `/Root/DataShard`;
            )";
            auto insertResult = client.ExecuteQuery(sql, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT(!insertResult.IsSuccess());
            UNIT_ASSERT_C(
                insertResult.GetIssues().ToString().Contains("Transactions between column and row tables are disabled at current time"),
                insertResult.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(OlapReplace_FromSelectLarge) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        auto settings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetWithSampleTables(false);

        TTestHelper testHelper(settings);

        TKikimrRunner& kikimr = testHelper.GetKikimr();
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("Col1").SetType(NScheme::NTypeIds::Int64).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("Col2").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
        };

        TTestHelper::TColumnTable testTable1;
        testTable1.SetName("/Root/ColumnShard1").SetPrimaryKey({ "Col1" }).SetSharding({ "Col1" }).SetSchema(schema).SetMinPartitionsCount(1000);
        testHelper.CreateTable(testTable1);

        TTestHelper::TColumnTable testTable2;
        testTable2.SetName("/Root/ColumnShard2").SetPrimaryKey({ "Col1" }).SetSharding({ "Col1" }).SetSchema(schema).SetMinPartitionsCount(1000);
        testHelper.CreateTable(testTable2);

        {
            TTestHelper::TUpdatesBuilder tableInserter(testTable1.GetArrowSchema(schema));
            for (size_t index = 0; index < 10000; ++index) {
                tableInserter.AddRow().Add(index).Add(index * 10);
            }
            testHelper.BulkUpsert(testTable1, tableInserter);
        }

        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto client = kikimr.GetQueryClient();

        {
            const TString sql = R"(
                REPLACE INTO `/Root/ColumnShard2`
                SELECT * FROM `/Root/ColumnShard1`
            )";
            auto insertResult = client.ExecuteQuery(sql, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(insertResult.IsSuccess(), insertResult.GetIssues().ToString());

            auto it = client.StreamExecuteQuery(R"(
                SELECT COUNT(*) FROM `/Root/ColumnShard2`;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(
                output,
                R"([[10000u]])");
        }
    }

    Y_UNIT_TEST(OlapReplace_Simple) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        auto settings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        TTableWithNullsHelper(kikimr).CreateTableWithNulls();

        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        const TString query = R"(
            CREATE TABLE `/Root/ColumnShard` (
                Col1 Uint64 NOT NULL,
                Col2 Int32,
                Col4 String,
                Col3 String NOT NULL,
                PRIMARY KEY (Col1, Col3)
            )
            PARTITION BY HASH(Col1)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10);
        )";

        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        // Shuffled
        auto client = kikimr.GetQueryClient();
        { 
            auto prepareResult = client.ExecuteQuery(R"(
                REPLACE INTO `/Root/ColumnShard` (Col3, Col4, Col2, Col1) VALUES
                    ("test100", "100", 1000, 100u);
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        }

        {
            auto prepareResult = client.ExecuteQuery(R"(
                REPLACE INTO `/Root/ColumnShard` (Col1, Col3, Col2, Col4) VALUES
                    (1u, "test1", 10, "1"), (2u, "test2", NULL, "2"), (3u, "test3", 12, NULL), (4u, "test4", NULL, NULL);
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        }

        auto it = client.StreamExecuteQuery(R"(
            SELECT Col1, Col3, Col2, Col4 FROM `/Root/ColumnShard` ORDER BY Col1, Col3, Col2, Col4;
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
        TString output = StreamResultToYson(it);
        CompareYson(output, R"([[1u;"test1";[10];["1"]];[2u;"test2";#;["2"]];[3u;"test3";[12];#];[4u;"test4";#;#];[100u;"test100";[1000];["100"]]])");
    }

    Y_UNIT_TEST(OlapReplace_InsertUpsertError) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        auto settings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetWithSampleTables(false);

        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        TTableWithNullsHelper(kikimr).CreateTableWithNulls();

        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        const TString query = R"(
            CREATE TABLE `/Root/ColumnShard` (
                Col1 Uint64 NOT NULL,
                Col2 Int32,
                PRIMARY KEY (Col1)
            )
            PARTITION BY HASH(Col1)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 16);
        )";

        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        auto client = kikimr.GetQueryClient();
        { 
            auto prepareResult = client.ExecuteQuery(R"(
                UPSERT INTO `/Root/ColumnShard` (Col1, Col2) VALUES (1u, 1)
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!prepareResult.IsSuccess());
            UNIT_ASSERT_C(
                prepareResult.GetIssues().ToString().Contains("is not supported for olap tables"),
                prepareResult.GetIssues().ToString());
        }

        { 
            auto prepareResult = client.ExecuteQuery(R"(
                INSERT INTO `/Root/ColumnShard` (Col1, Col2) VALUES (2u, 2)
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!prepareResult.IsSuccess());
            UNIT_ASSERT_C(
                prepareResult.GetIssues().ToString().Contains("is not supported for olap tables"),
                prepareResult.GetIssues().ToString());
        }

        {
            auto it = client.StreamExecuteQuery(R"(
                SELECT * FROM `/Root/ColumnShard`;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(output, R"([])");
        }
    }

    Y_UNIT_TEST(OlapReplace_Duplicates) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        auto settings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetWithSampleTables(false);

        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        TTableWithNullsHelper(kikimr).CreateTableWithNulls();

        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        const TString query = R"(
            CREATE TABLE `/Root/ColumnShard` (
                Col1 Uint64 NOT NULL,
                Col2 Int32,
                PRIMARY KEY (Col1)
            )
            PARTITION BY HASH(Col1)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10);
        )";

        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        auto client = kikimr.GetQueryClient();
        { 
            auto prepareResult = client.ExecuteQuery(R"(
                REPLACE INTO `/Root/ColumnShard` (Col1, Col2) VALUES
                    (100u, 1000), (100u, 1000);
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        }

        { 
            auto prepareResult = client.ExecuteQuery(R"(
                REPLACE INTO `/Root/ColumnShard` (Col1, Col2) VALUES
                    (100u, 1000), (100u, 1000);
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        }

        auto it = client.StreamExecuteQuery(R"(
            SELECT * FROM `/Root/ColumnShard` ORDER BY Col1, Col2;
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
        TString output = StreamResultToYson(it);
        CompareYson(output, R"([[100u;[1000]]])");
    }

    Y_UNIT_TEST(OlapReplace_DisableOlapSink) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableOlapSink(false);
        auto settings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetWithSampleTables(false);

        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        TTableWithNullsHelper(kikimr).CreateTableWithNulls();

        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        const TString query = R"(
            CREATE TABLE `/Root/ColumnShard` (
                Col1 Uint64 NOT NULL,
                Col2 Int32,
                PRIMARY KEY (Col1)
            )
            PARTITION BY HASH(Col1)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 16);
        )";

        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        auto client = kikimr.GetQueryClient();
        { 
            auto prepareResult = client.ExecuteQuery(R"(
                REPLACE INTO `/Root/ColumnShard` (Col1, Col2) VALUES (1u, 1)
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!prepareResult.IsSuccess());
            UNIT_ASSERT_C(
                prepareResult.GetIssues().ToString().Contains("Data manipulation queries do not support column shard tables."),
                prepareResult.GetIssues().ToString());
        }

        {
            auto it = client.StreamExecuteQuery(R"(
                SELECT * FROM `/Root/ColumnShard`;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(output, R"([])");
        }
    }

    Y_UNIT_TEST(BlockGenericWithDistinct) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    COUNT(DISTINCT id)
                FROM `/Root/tableWithNulls`
                WHERE level = 5 AND Cast(id AS String) = "5";
            )")
            .AddExpectedPlanOptions("KqpBlockReadOlapTableRanges")
            .AddExpectedPlanOptions("WideFromBlocks")
            .SetExpectedReply("[[1u]]");
        TestTableWithNulls({ testCase }, /* generic */ true);
    }

    Y_UNIT_TEST(BlockGenericSimpleAggregation) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    level, COUNT(*), SUM(id)
                FROM `/Root/tableWithNulls`
                WHERE level = 5
                GROUP BY level
                ORDER BY level;
            )")
            .AddExpectedPlanOptions("KqpBlockReadOlapTableRanges")
            .AddExpectedPlanOptions("WideFromBlocks")
            .SetExpectedReply(R"([[[5];1u;5]])");

        TestTableWithNulls({ testCase }, /* generic */ true);
    }

    Y_UNIT_TEST(BlockGenericSelectAll) {
        TAggregationTestCase testCase;
        testCase.SetQuery(R"(
                SELECT
                    id, resource_id, level
                FROM `/Root/tableWithNulls`
                WHERE level != 5 OR level IS NULL
                ORDER BY id, resource_id, level;
            )")
            .AddExpectedPlanOptions("KqpBlockReadOlapTableRanges")
            .AddExpectedPlanOptions("WideFromBlocks")
            .SetExpectedReply(R"([[1;#;[1]];[2;#;[2]];[3;#;[3]];[4;#;[4]];[6;["6"];#];[7;["7"];#];[8;["8"];#];[9;["9"];#];[10;["10"];#]])");

        TestTableWithNulls({ testCase }, /* generic */ true);
    }
}

} // namespace NKqp
} // namespace NKikimr
