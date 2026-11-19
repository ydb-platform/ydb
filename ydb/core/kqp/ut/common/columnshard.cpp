#include "columnshard.h"

#include <util/string/printf.h>

#include <vector>

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/formats/arrow/serializer/native.h>
#include <ydb/core/formats/arrow/serializer/parsing.h>
#include <ydb/core/testlib/cs_helper.h>
#include <ydb/core/tx/columnshard/engines/scheme/objects_cache.h>

extern "C" {
#include <yql/essentials/parser/pg_wrapper/postgresql/src/include/catalog/pg_type_d.h>
}

namespace NKikimr {
namespace NKqp {
    using namespace NYdb;

    TTestHelper::TTestHelper(const TKikimrSettings& settings) {
        TKikimrSettings kikimrSettings(settings);
        if (!kikimrSettings.FeatureFlags.HasDisableOldSecretCreation()) {
            kikimrSettings.FeatureFlags.SetDisableOldSecretCreation(false);
        }
        if (!kikimrSettings.FeatureFlags.HasEnableTieringInColumnShard()) {
            kikimrSettings.SetEnableTieringInColumnShard(true);
        }
        if (!kikimrSettings.FeatureFlags.HasEnableExternalDataSources()) {
            kikimrSettings.SetEnableExternalDataSources(true);
            kikimrSettings.AppConfig.MutableQueryServiceConfig()->AddAvailableExternalDataSources("ObjectStorage");
        }

        if (!kikimrSettings.AppConfig.GetColumnShardConfig().HasStatistics()) {
            kikimrSettings.AppConfig.MutableColumnShardConfig()->MutableStatistics()->SetReportBaseStatisticsPeriodMs(1000);
            kikimrSettings.AppConfig.MutableColumnShardConfig()->MutableStatistics()->SetReportExecutorStatisticsPeriodMs(1000);
        }

        Kikimr = std::make_unique<TKikimrRunner>(kikimrSettings);
        TableClient =
            std::make_unique<NYdb::NTable::TTableClient>(Kikimr->GetTableClient(NYdb::NTable::TClientSettings().AuthToken("root@builtin")));
        QueryClient =
            std::make_unique<NYdb::NQuery::TQueryClient>(Kikimr->GetQueryClient(NYdb::NQuery::TClientSettings().AuthToken("root@builtin")));
        Session = std::make_unique<NYdb::NTable::TSession>(TableClient->CreateSession().GetValueSync().GetSession());

        NOlap::TSchemaCachesManager::DropCaches();
    }

    NKikimr::NKqp::TKikimrRunner& TTestHelper::GetKikimr() {
        return *Kikimr;
    }

    TTestActorRuntime& TTestHelper::GetRuntime() {
        return *Kikimr->GetTestServer().GetRuntime();
    }

    NYdb::NTable::TSession& TTestHelper::GetSession() {
        return *Session;
    }

    void TTestHelper::CreateTable(const TColumnTableBase& table, const EStatus expectedStatus) {
        std::cerr << (table.BuildQuery()) << std::endl;
        auto result = GetSession().ExecuteSchemeQuery(table.BuildQuery()).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), expectedStatus, result.GetIssues().ToString());
    }

    void TTestHelper::CreateTableQuery(const TColumnTableBase& table, const EStatus expectedStatus) {
        std::cerr << (table.BuildQuery()) << std::endl;
        auto it = QueryClient->ExecuteQuery(table.BuildQuery(), NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), expectedStatus, it.GetIssues().ToString());
    }

    void TTestHelper::CreateTier(const TString& tierName) {
        auto result = GetSession().ExecuteSchemeQuery(R"(
            UPSERT OBJECT `accessKey` (TYPE SECRET) WITH (value = `secretAccessKey`);
            UPSERT OBJECT `secretKey` (TYPE SECRET) WITH (value = `fakeSecret`);
            CREATE EXTERNAL DATA SOURCE `)" + tierName + R"(` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="http://fake.fake/olap-)" + tierName + R"(",
                AUTH_METHOD="AWS",
                AWS_ACCESS_KEY_ID_SECRET_NAME="accessKey",
                AWS_SECRET_ACCESS_KEY_SECRET_NAME="secretKey",
                AWS_REGION="ru-central1"
        );
        )").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    void TTestHelper::SetTiering(const TString& tableName, const TString& tierName, const TString& columnName) {
        auto alterQuery = TStringBuilder() << "ALTER TABLE `" << tableName <<  "` SET TTL Interval(\"P10D\") TO EXTERNAL DATA SOURCE `" << tierName << "` ON `" << columnName << "`;";
        auto result = GetSession().ExecuteSchemeQuery(alterQuery).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    void TTestHelper::ResetTiering(const TString& tableName) {
        auto alterQuery = TStringBuilder() << "ALTER TABLE `" << tableName <<  "` RESET (TTL)";
        auto result = GetSession().ExecuteSchemeQuery(alterQuery).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    void TTestHelper::DropTable(const TString& tableName) {
        auto result = GetSession().DropTable(tableName).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    void TTestHelper::BulkUpsert(const TColumnTable& table, TTestHelper::TUpdatesBuilder& updates,
        const Ydb::StatusIds_StatusCode& opStatus /*= Ydb::StatusIds::SUCCESS*/, const TString& expectedIssuePrefix /*= ""*/) {
        NKikimr::Tests::NCS::THelper helper(GetKikimr().GetTestServer());
        auto batch = updates.BuildArrow();
        helper.SendDataViaActorSystem(table.GetName(), batch, opStatus, expectedIssuePrefix);
    }

    void TTestHelper::BulkUpsert(const TColumnTable& table, std::shared_ptr<arrow::RecordBatch> batch, const Ydb::StatusIds_StatusCode& opStatus /*= Ydb::StatusIds::SUCCESS*/) {
        NKikimr::Tests::NCS::THelper helper(GetKikimr().GetTestServer());
        helper.SendDataViaActorSystem(table.GetName(), batch, opStatus);
    }

    void TTestHelper::ReadData(const TString& query, const TString& expected, const EStatus opStatus /*= EStatus::SUCCESS*/) const {
        auto it = TableClient->StreamExecuteScanQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString()); // Means stream successfully get
        TString result = StreamResultToYson(it, false, opStatus);
        if (opStatus == EStatus::SUCCESS) {
            UNIT_ASSERT_NO_DIFF(ReformatYson(result), ReformatYson(expected));
        }
    }

    void TTestHelper::ReadDataExecQuery(const TString& query, const TString& expected, const EStatus opStatus /*= EStatus::SUCCESS*/) const {
        auto it = QueryClient->StreamExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
        TString result = StreamResultToYson(it, false, opStatus, "");
        if (opStatus == EStatus::SUCCESS) {
            UNIT_ASSERT_NO_DIFF(ReformatYson(result), ReformatYson(expected));
        }
    }

    void TTestHelper::ExecuteQuery(const TString& query) const {
        auto it = QueryClient->ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString()); // Means stream successfully get
    }

    void TTestHelper::RebootTablets(const TString& tableName) {
        auto runtime = GetKikimr().GetTestServer().GetRuntime();
        TActorId sender = runtime->AllocateEdgeActor();
        TVector<ui64> shards;
        {
            auto describeResult = DescribeTable(&GetKikimr().GetTestServer(), sender, tableName);
            for (auto shard : describeResult.GetPathDescription().GetColumnTableDescription().GetSharding().GetColumnShards()) {
                shards.push_back(shard);
            }
        }
        for (auto shard : shards) {
            GetKikimr().GetTestServer().GetRuntime()->Send(MakePipePerNodeCacheID(false), NActors::TActorId(), new TEvPipeCache::TEvForward(
                    new TEvents::TEvPoisonPill(), shard, false));
        }
    }

    void TTestHelper::WaitTabletDeletionInHive(ui64 tabletId, TDuration duration) {
        auto deadline = TInstant::Now() + duration;
        while (GetKikimr().GetTestClient().TabletExistsInHive(&GetRuntime(), tabletId) && TInstant::Now() <= deadline) {
            Cerr << "WaitTabletDeletionInHive: wait until " << tabletId << " is deleted" << Endl;
            Sleep(TDuration::Seconds(1));
        }
    }

    TString TTestHelper::TColumnSchema::BuildQuery() const {
        TStringBuilder str;
        str << Name << ' ';
        switch (TypeInfo.GetTypeId()) {
        case NScheme::NTypeIds::Pg:
            str << NPg::PgTypeNameFromTypeDesc(TypeInfo.GetPgTypeDesc());
            break;
        case NScheme::NTypeIds::Decimal: {
            TTypeBuilder builder;
            builder.Decimal(TDecimalType(TypeInfo.GetDecimalType().GetPrecision(), TypeInfo.GetDecimalType().GetScale()));
            str << builder.Build();
            break;
        }
        default:
            str << NScheme::GetTypeName(TypeInfo.GetTypeId());
        }
        if (!NullableFlag) {
            str << " NOT NULL";
        }
        if (DictionaryEncodingFlag) {
            str << " ENCODING(DICT)";
        }
        if (ColumnCompression) {
            str << " COMPRESSION(";
            bool haveSome = false;
            for (const auto& [key, value] : *ColumnCompression) {
                if (haveSome) {
                    str << ',';
                }
                str << key << '=' << value;
                haveSome = true;
            }
            str << ")";
        }
        return str;
    }

    TTestHelper::TColumnSchema& TTestHelper::TColumnSchema::SetType(const NScheme::TTypeInfo& typeInfo) {
        TypeInfo = typeInfo;
        return *this;
    }

    TTestHelper::TColumnSchema& TTestHelper::TColumnSchema::SetCompression() {
        ColumnCompression = std::map<TString, TString>();
        return *this;
    }

    TTestHelper::TColumnSchema& TTestHelper::TColumnSchema::SetCompressionSetting(const TString& key, const TString& value) {
        if (!ColumnCompression) {
            ColumnCompression = std::map<TString, TString>();
        }
        (*ColumnCompression)[key] = value;
        return *this;
    }

    TString TTestHelper::TColumnTableBase::BuildQuery() const {
        auto str = TStringBuilder() << "CREATE " << GetObjectType() << " `" << Name << "`";
        str << " (" << BuildColumnsStr(Schema) << ", PRIMARY KEY (" << JoinStrings(PrimaryKey, ", ") << ")";
        str << ")";
        if (!Sharding.empty()) {
            str << " PARTITION BY HASH(" << JoinStrings(Sharding, ", ") << ")";
        }
        str << " WITH (STORE = COLUMN";
        str << ", AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = " << MinPartitionsCount;
        if (TTLConf) {
            str << ", TTL = " << TTLConf->second << " ON " << TTLConf->first;
        }
        str << ");";
        return str;
    }

    std::shared_ptr<arrow::Schema> TTestHelper::TColumnTableBase::GetArrowSchema(const TVector<TColumnSchema>& columns) {
        std::vector<std::shared_ptr<arrow::Field>> result;
        for (auto&& col : columns) {
            result.push_back(BuildField(col.GetName(), col.GetTypeInfo(), col.IsNullable()));
        }
        return std::make_shared<arrow::Schema>(result);
    }

    TString TTestHelper::TColumnTableBase::BuildColumnsStr(const TVector<TColumnSchema>& clumns) const {
        TVector<TString> columnStr;
        for (auto&& c : clumns) {
            columnStr.push_back(c.BuildQuery());
        }
        return JoinStrings(columnStr, ", ");
    }

    std::shared_ptr<arrow::Field> TTestHelper::TColumnTableBase::BuildField(const TString name, const NScheme::TTypeInfo& typeInfo, bool nullable) const {
        switch (typeInfo.GetTypeId()) {
        case NScheme::NTypeIds::Bool:
            return arrow::field(name, arrow::uint8(), nullable);
        case NScheme::NTypeIds::Int8:
            return arrow::field(name, arrow::int8(), nullable);
        case NScheme::NTypeIds::Int16:
            return arrow::field(name, arrow::int16(), nullable);
        case NScheme::NTypeIds::Int32:
            return arrow::field(name, arrow::int32(), nullable);
        case NScheme::NTypeIds::Int64:
            return arrow::field(name, arrow::int64(), nullable);
        case NScheme::NTypeIds::Uint8:
            return arrow::field(name, arrow::uint8(), nullable);
        case NScheme::NTypeIds::Uint16:
            return arrow::field(name, arrow::uint16(), nullable);
        case NScheme::NTypeIds::Uint32:
            return arrow::field(name, arrow::uint32(), nullable);
        case NScheme::NTypeIds::Uint64:
            return arrow::field(name, arrow::uint64(), nullable);
        case NScheme::NTypeIds::Float:
            return arrow::field(name, arrow::float32(), nullable);
        case NScheme::NTypeIds::Double:
            return arrow::field(name, arrow::float64(), nullable);
        case NScheme::NTypeIds::String:
            return arrow::field(name, arrow::binary(), nullable);
        case NScheme::NTypeIds::Utf8:
            return arrow::field(name, arrow::utf8(), nullable);
        case NScheme::NTypeIds::Json:
            return arrow::field(name, arrow::utf8(), nullable);
        case NScheme::NTypeIds::Yson:
            return arrow::field(name, arrow::binary(), nullable);
        case NScheme::NTypeIds::Date:
            return arrow::field(name, arrow::uint16(), nullable);
        case NScheme::NTypeIds::Datetime:
            return arrow::field(name, arrow::uint32(), nullable);
        case NScheme::NTypeIds::Timestamp:
            return arrow::field(name, arrow::timestamp(arrow::TimeUnit::TimeUnit::MICRO), nullable);
        case NScheme::NTypeIds::Interval:
            return arrow::field(name, arrow::int64(), nullable);
        case NScheme::NTypeIds::Date32:
            return arrow::field(name, arrow::int32(), nullable);
        case NScheme::NTypeIds::Datetime64:
        case NScheme::NTypeIds::Timestamp64:
        case NScheme::NTypeIds::Interval64:
            return arrow::field(name, arrow::int64(), nullable);
        case NScheme::NTypeIds::JsonDocument:
            return arrow::field(name, arrow::binary(), nullable);
        case NScheme::NTypeIds::DyNumber:
            return arrow::field(name, arrow::binary(), nullable);
        case NScheme::NTypeIds::Decimal:
        case NScheme::NTypeIds::Uuid:
            return arrow::field(name, std::make_shared<arrow::FixedSizeBinaryType>(NScheme::FSB_SIZE), nullable);
        case NScheme::NTypeIds::Pg:
            switch (NPg::PgTypeIdFromTypeDesc(typeInfo.GetPgTypeDesc())) {
                case INT2OID:
                    return arrow::field(name, arrow::int16(), true);
                case INT4OID:
                    return arrow::field(name, arrow::int32(), true);
                case INT8OID:
                    return arrow::field(name, arrow::int64(), true);
                case FLOAT4OID:
                    return arrow::field(name, arrow::float32(), true);
                case FLOAT8OID:
                    return arrow::field(name, arrow::float64(), true);
                case BYTEAOID:
                    return arrow::field(name, arrow::binary(), true);
                case TEXTOID:
                    return arrow::field(name, arrow::utf8(), true);
                default:
                    Y_FAIL("TODO: support pg");
            }
        }
        return nullptr;
    }

    TString TTestHelper::TColumnTable::GetObjectType() const {
        return "TABLE";
    }


    TString TTestHelper::TColumnTableStore::GetObjectType() const {
        return "TABLESTORE";
    }

}
}

namespace NKikimr::NKqp {

namespace {

void CreateOlapJsonValueAggregationRows(TKikimrRunner& kikimr) {
    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();
    auto create = session.ExecuteSchemeQuery(R"(
        CREATE TABLE `/Root/docs` (id Uint64 NOT NULL, labels JsonDocument, PRIMARY KEY (id))
            PARTITION BY HASH(id) WITH (STORE = COLUMN);
    )").GetValueSync();
    UNIT_ASSERT_C(create.IsSuccess(), create.GetIssues().ToString());

    NYdb::TValueBuilder rows;
    rows.BeginList();
    for (const auto& [id, labels] : std::vector<std::pair<ui64, TString>>{
             {1, R"({"host":"a1"})"}, {2, R"({"host":"a1"})"}, {3, R"({"host":"a2"})"}, {4, "{}"}}) {
        rows.AddListItem()
            .BeginStruct()
            .AddMember("id")
            .Uint64(id)
            .AddMember("labels")
            .JsonDocument(labels)
            .EndStruct();
    }
    rows.EndList();
    auto insert = db.BulkUpsert("/Root/docs", rows.Build()).GetValueSync();
    UNIT_ASSERT_C(insert.IsSuccess(), insert.GetIssues().ToString());
}

void CreateOlapJsonValueTopHostsRows(TKikimrRunner& kikimr) {
    auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
    auto create = session.ExecuteSchemeQuery(R"(
        CREATE TABLE `/Root/foo` (
            timestamp Timestamp NOT NULL,
            id Uint64 NOT NULL,
            labels JsonDocument,
            PRIMARY KEY (timestamp, id)
        ) WITH (STORE = COLUMN);
    )")
                      .GetValueSync();
    UNIT_ASSERT_C(create.IsSuccess(), create.GetIssues().ToString());

    auto querySession = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
    auto insert = querySession.ExecuteQuery(R"(
        INSERT INTO `/Root/foo` (timestamp, id, labels) VALUES
            (Timestamp("1970-01-01T00:00:03.000001Z"), 1, JsonDocument('{"host":"a1"}')),
            (Timestamp("1970-01-01T00:00:03.000002Z"), 2, JsonDocument('{"host":"a1"}')),
            (Timestamp("1970-01-01T00:00:03.000003Z"), 3, JsonDocument('{"host":"a2"}')),
            (Timestamp("1970-01-01T00:00:03.000004Z"), 4, JsonDocument('{}'));
    )", NYdb::NQuery::TTxControl::NoTx())
                      .GetValueSync();
    UNIT_ASSERT_C(insert.IsSuccess(), insert.GetIssues().ToString());
}

const TString OlapJsonValueQueryPrefix = R"(
    PRAGMA YqlSelect = 'force';
    PRAGMA YqlSelectAllowUnnamedGroupByExpr;
    PRAGMA Kikimr.OptEnableOlapPushdownProjections = 'true';
    $rows = SELECT id, labels FROM `/Root/docs`;
    $by_host = SELECT JSON_VALUE(labels, '$.host') AS host, COUNT(*) AS cnt
        FROM $rows GROUP BY JSON_VALUE(labels, '$.host');
)";

TString BuildOlapJsonValueAggregationQuery(bool threeBranches, bool nestedJsonValue) {
    const char* hostExpr = nestedJsonValue ? "COALESCE(JSON_VALUE(labels, '$.host'), '')" : "JSON_VALUE(labels, '$.host')";
    const TString prefix = OlapJsonValueQueryPrefix + Sprintf(R"(
        $by_host_id = SELECT %s AS host, id, COUNT(*) AS cnt
            FROM $rows GROUP BY %s, id;
    )", hostExpr, hostExpr);
    const TString suffix = threeBranches ? R"(
        $by_host_id3 = SELECT JSON_VALUE(labels, '$.host') AS host, id / 3 AS bucket, COUNT(*) AS cnt
            FROM $rows GROUP BY JSON_VALUE(labels, '$.host'), id / 3;
        SELECT a.host, a.cnt, b.cnt, c.cnt FROM $by_host AS a
            INNER JOIN $by_host_id AS b ON a.host = b.host
            INNER JOIN $by_host_id3 AS c ON a.host = c.host
            ORDER BY a.host, b.id, c.bucket;
    )" : R"(
        SELECT a.host, a.cnt, b.cnt FROM $by_host AS a
            INNER JOIN $by_host_id AS b ON a.host = b.host
            ORDER BY a.host, b.id;
    )";
    return prefix + suffix;
}

TString BuildOlapJsonValueDifferentPathsQuery() {
    return OlapJsonValueQueryPrefix + R"(
        $by_zone = SELECT JSON_VALUE(labels, '$.zone') AS zone, id, COUNT(*) AS cnt
            FROM $rows GROUP BY JSON_VALUE(labels, '$.zone'), id;
        SELECT a.host, b.zone FROM $by_host AS a
            INNER JOIN $by_zone AS b ON a.host = b.zone;
    )";
}

TString BuildOlapJsonValueRawColumnQuery() {
    return OlapJsonValueQueryPrefix + R"(
        $by_host_id = SELECT id, JSON_VALUE(labels, '$.host') AS host, labels AS raw_labels
            FROM $rows;
        SELECT b.id, b.raw_labels, a.host FROM $by_host AS a
            INNER JOIN $by_host_id AS b ON a.host = b.host
            ORDER BY b.id;
    )";
}

} // namespace

TString BuildOlapJsonValueTopHostsQuery(bool useRbo) {
    if (!useRbo) {
        return R"(
        $from = Timestamp("1970-01-01T00:00:03Z");
        $to = Timestamp("1970-01-01T00:00:04Z");
        SELECT min(`timestamp`) AS bucket, column, count(*) AS cnt
        FROM `/Root/foo`
        WHERE `timestamp` >= $from AND `timestamp` < $to
            AND JSON_VALUE(labels, '$.host') IN (
                SELECT column FROM (
                    SELECT column, count(*) AS total
                    FROM `/Root/foo`
                    WHERE `timestamp` >= $from AND `timestamp` < $to
                    GROUP BY JSON_VALUE(labels, '$.host') AS column
                    ORDER BY total DESC
                    LIMIT 10
                )
            )
        GROUP BY CAST(`timestamp` AS Int64) / 300 / 1000000, JSON_VALUE(labels, '$.host') AS column
        ORDER BY bucket, column;
        )";
    } else {
        // RBO does not support `JSON_VALUE(labels, '$.host') IN (SELECT ...)` form
        // so rewrite the query with named rows
        return R"(
        PRAGMA YqlSelectAllowUnnamedGroupByExpr;
        $from = Timestamp("1970-01-01T00:00:03Z");
        $to = Timestamp("1970-01-01T00:00:04Z");
        $rows = SELECT `timestamp`, labels
            FROM `/Root/foo`
            WHERE `timestamp` >= $from AND `timestamp` < $to;
        $top_rows = SELECT JSON_VALUE(labels, '$.host') AS column FROM $rows;
        $top = SELECT column, count(*) AS total FROM $top_rows
            GROUP BY column ORDER BY total DESC LIMIT 10;
        $bucket_rows = SELECT `timestamp`, JSON_VALUE(labels, '$.host') AS column FROM $rows;
        SELECT min(`timestamp`) AS bucket, column, count(*) AS cnt FROM $bucket_rows
            WHERE column IN (SELECT column FROM $top)
            GROUP BY CAST(`timestamp` AS Int64) / 300 / 1000000, column
            ORDER BY bucket, column;
        )";
    }
}

namespace {

void AssertOlapJsonValueRawColumnResult(const NYdb::TResultSet& result) {
    NYdb::TResultSetParser parser(result);
    for (const auto& [id, host] : std::vector<std::pair<ui64, TString>>{{1, "a1"}, {2, "a1"}, {3, "a2"}}) {
        UNIT_ASSERT(parser.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser(0).GetUint64(), id);
        UNIT_ASSERT_VALUES_EQUAL(*parser.ColumnParser(1).GetOptionalJsonDocument(),
                                 Sprintf(R"({"host":"%s"})", host.c_str()));
        UNIT_ASSERT_VALUES_EQUAL(*parser.ColumnParser(2).GetOptionalUtf8(), host);
    }
    UNIT_ASSERT(!parser.TryNextRow());
}

void AssertOlapJsonValueProjectionPushdown(const TString& ast) {
    UNIT_ASSERT_C(ast.find("KqpOlapProjections") != TString::npos, ast);
    UNIT_ASSERT_C(ast.find("KqpOlapJsonValue") != TString::npos, ast);
    UNIT_ASSERT_C(ast.find("Json2.JsonDocumentSqlValueConvertToUtf8") == TString::npos, ast);
}

void AssertOlapJsonValueProjectionNotPushedDown(TStringBuf ast) {
    UNIT_ASSERT_C(ast.find("KqpOlapProjections") == TStringBuf::npos, ast);
    UNIT_ASSERT_C(ast.find("Json2.JsonDocumentSqlValueConvertToUtf8") != TStringBuf::npos, ast);
}

// This is a test precondition to ensure that setup is correct for Replicate tests.
void AssertSingleRead(const TString& ast) {
    constexpr TStringBuf read = "KqpBlockReadOlapTableRanges";
    const auto first = ast.find(read);
    UNIT_ASSERT_C(first != TString::npos, ast);
    UNIT_ASSERT_C(ast.find(read, first + 1) == TString::npos, ast);
}

} // namespace

void TOlapJsonValueProjectionSuite::CheckQuery(const TString& query, void (*createRows)(TKikimrRunner&), bool expectPushdown, bool expectSingleRead,
                                               const std::function<void(const NYdb::TResultSet&)>& assertResult) const {
    TKikimrRunner kikimr(CreateSettings());
    createRows(kikimr);
    auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();

    auto execution = ExecuteQuery(kikimr, session, query);
    UNIT_ASSERT_C(execution.IsSuccess(), execution.GetIssues().ToString());
    assertResult(execution.GetResultSet(0));

    auto explain =
        session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain))
            .GetValueSync();
    UNIT_ASSERT_C(explain.IsSuccess(), explain.GetIssues().ToString());
    const TString ast(*explain.GetStats()->GetAst());
    if (expectSingleRead) {
        AssertSingleRead(ast);
    }
    if (expectPushdown) {
        AssertOlapJsonValueProjectionPushdown(ast);
    } else {
        AssertOlapJsonValueProjectionNotPushedDown(ast);
    }
}

void TOlapJsonValueProjectionSuite::CheckTopHostsSemiJoin() const {
    CheckQuery(CreateTopHostsQuery(), CreateOlapJsonValueTopHostsRows, true, false,
               [](const NYdb::TResultSet& result) { CompareYson(FormatResultSetYson(result), R"([[3000001u;["a1"];2u];[3000003u;["a2"];1u]])"); });
}

void TOlapJsonValueProjectionSuite::CheckAggregationBranches(bool threeBranches, bool nestedJsonValue) const {
    const TString expectedResult =
        threeBranches ? R"([[["a1"];2u;1u;2u];[["a1"];2u;1u;2u];[["a2"];1u;1u;1u]])" : R"([[["a1"];2u;1u];[["a1"];2u;1u];[["a2"];1u;1u]])";
    CheckQuery(BuildOlapJsonValueAggregationQuery(threeBranches, nestedJsonValue), CreateOlapJsonValueAggregationRows, true, true,
               [&expectedResult](const NYdb::TResultSet& result) { CompareYson(FormatResultSetYson(result), expectedResult); });
}

void TOlapJsonValueProjectionSuite::CheckDifferentSharedPaths() const {
    // A shared read currently pushes only one JSON_VALUE path.
    CheckQuery(BuildOlapJsonValueDifferentPathsQuery(), CreateOlapJsonValueAggregationRows, false, true,
               [](const NYdb::TResultSet& result) { CompareYson(FormatResultSetYson(result), "[]"); });
}

void TOlapJsonValueProjectionSuite::CheckRawColumn() const {
    CheckQuery(BuildOlapJsonValueRawColumnQuery(), CreateOlapJsonValueAggregationRows, false, true, AssertOlapJsonValueRawColumnResult);
}

} // namespace NKikimr::NKqp
