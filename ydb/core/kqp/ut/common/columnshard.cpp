#include "columnshard.h"

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

    void TTestHelper::BulkUpsert(const TColumnTable& table, std::shared_ptr<arrow20::RecordBatch> batch, const Ydb::StatusIds_StatusCode& opStatus /*= Ydb::StatusIds::SUCCESS*/) {
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

    std::shared_ptr<arrow20::Schema> TTestHelper::TColumnTableBase::GetArrowSchema(const TVector<TColumnSchema>& columns) {
        std::vector<std::shared_ptr<arrow20::Field>> result;
        for (auto&& col : columns) {
            result.push_back(BuildField(col.GetName(), col.GetTypeInfo(), col.IsNullable()));
        }
        return std::make_shared<arrow20::Schema>(result);
    }

    TString TTestHelper::TColumnTableBase::BuildColumnsStr(const TVector<TColumnSchema>& clumns) const {
        TVector<TString> columnStr;
        for (auto&& c : clumns) {
            columnStr.push_back(c.BuildQuery());
        }
        return JoinStrings(columnStr, ", ");
    }

    std::shared_ptr<arrow20::Field> TTestHelper::TColumnTableBase::BuildField(const TString name, const NScheme::TTypeInfo& typeInfo, bool nullable) const {
        switch (typeInfo.GetTypeId()) {
        case NScheme::NTypeIds::Bool:
            return arrow20::field(name, arrow20::uint8(), nullable);
        case NScheme::NTypeIds::Int8:
            return arrow20::field(name, arrow20::int8(), nullable);
        case NScheme::NTypeIds::Int16:
            return arrow20::field(name, arrow20::int16(), nullable);
        case NScheme::NTypeIds::Int32:
            return arrow20::field(name, arrow20::int32(), nullable);
        case NScheme::NTypeIds::Int64:
            return arrow20::field(name, arrow20::int64(), nullable);
        case NScheme::NTypeIds::Uint8:
            return arrow20::field(name, arrow20::uint8(), nullable);
        case NScheme::NTypeIds::Uint16:
            return arrow20::field(name, arrow20::uint16(), nullable);
        case NScheme::NTypeIds::Uint32:
            return arrow20::field(name, arrow20::uint32(), nullable);
        case NScheme::NTypeIds::Uint64:
            return arrow20::field(name, arrow20::uint64(), nullable);
        case NScheme::NTypeIds::Float:
            return arrow20::field(name, arrow20::float32(), nullable);
        case NScheme::NTypeIds::Double:
            return arrow20::field(name, arrow20::float64(), nullable);
        case NScheme::NTypeIds::String:
            return arrow20::field(name, arrow20::binary(), nullable);
        case NScheme::NTypeIds::Utf8:
            return arrow20::field(name, arrow20::utf8(), nullable);
        case NScheme::NTypeIds::Json:
            return arrow20::field(name, arrow20::utf8(), nullable);
        case NScheme::NTypeIds::Yson:
            return arrow20::field(name, arrow20::binary(), nullable);
        case NScheme::NTypeIds::Date:
            return arrow20::field(name, arrow20::uint16(), nullable);
        case NScheme::NTypeIds::Datetime:
            return arrow20::field(name, arrow20::uint32(), nullable);
        case NScheme::NTypeIds::Timestamp:
            return arrow20::field(name, arrow20::timestamp(arrow20::TimeUnit::TimeUnit::MICRO), nullable);
        case NScheme::NTypeIds::Interval:
            return arrow20::field(name, arrow20::duration(arrow20::TimeUnit::TimeUnit::MICRO), nullable);
        case NScheme::NTypeIds::Date32:
            return arrow20::field(name, arrow20::int32(), nullable);
        case NScheme::NTypeIds::Datetime64:
        case NScheme::NTypeIds::Timestamp64:
        case NScheme::NTypeIds::Interval64:
            return arrow20::field(name, arrow20::int64(), nullable);
        case NScheme::NTypeIds::JsonDocument:
            return arrow20::field(name, arrow20::binary(), nullable);
        case NScheme::NTypeIds::Decimal:
            return arrow20::field(name, std::make_shared<arrow20::FixedSizeBinaryType>(NScheme::FSB_SIZE), nullable);
        case NScheme::NTypeIds::Pg:
            switch (NPg::PgTypeIdFromTypeDesc(typeInfo.GetPgTypeDesc())) {
                case INT2OID:
                    return arrow20::field(name, arrow20::int16(), true);
                case INT4OID:
                    return arrow20::field(name, arrow20::int32(), true);
                case INT8OID:
                    return arrow20::field(name, arrow20::int64(), true);
                case FLOAT4OID:
                    return arrow20::field(name, arrow20::float32(), true);
                case FLOAT8OID:
                    return arrow20::field(name, arrow20::float64(), true);
                case BYTEAOID:
                    return arrow20::field(name, arrow20::binary(), true);
                case TEXTOID:
                    return arrow20::field(name, arrow20::utf8(), true);
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
