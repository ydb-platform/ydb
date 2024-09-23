#include "columnshard.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/formats/arrow/serializer/parsing.h>
#include <ydb/core/testlib/cs_helper.h>

extern "C" {
#include <ydb/library/yql/parser/pg_wrapper/postgresql/src/include/catalog/pg_type_d.h>
}

namespace NKikimr {
namespace NKqp {

    TString GetConfigProtoWithName(const TString & tierName) {
        return TStringBuilder() << "Name : \"" << tierName << "\"\n" <<
        R"(
            ObjectStorage : {
                Endpoint: "fake"
                Bucket: "fake"
                SecretableAccessKey: {
                    Value: {
                        Data: "secretAccessKey"
                    }
                }
                SecretableSecretKey: {
                    Value: {
                        Data: "fakeSecret"
                    }
                }
            }
        )";
    }

    using namespace NYdb;

    TTestHelper::TTestHelper(const TKikimrSettings& settings) {
        TKikimrSettings kikimrSettings(settings);
        if (!kikimrSettings.FeatureFlags.HasEnableTieringInColumnShard()) {
            kikimrSettings.SetEnableTieringInColumnShard(true);
        }

        Kikimr = std::make_unique<TKikimrRunner>(kikimrSettings);
        TableClient = std::make_unique<NYdb::NTable::TTableClient>(Kikimr->GetTableClient());
        Session = std::make_unique<NYdb::NTable::TSession>(TableClient->CreateSession().GetValueSync().GetSession());
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

    void TTestHelper::CreateTier(const TString& tierName) {
        auto result = GetSession().ExecuteSchemeQuery("CREATE OBJECT " + tierName + " (TYPE TIER) WITH tierConfig = `" + GetConfigProtoWithName(tierName) + "`").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    TString TTestHelper::CreateTieringRule(const TString& tierName, const TString& columnName) {
        const TString ruleName = tierName + "_" + columnName;
        const TString configTieringStr = TStringBuilder() <<  R"({
            "rules" : [
                {
                    "tierName" : ")" << tierName << R"(",
                    "durationForEvict" : "10d"
                }
            ]
        })";
        auto result = GetSession().ExecuteSchemeQuery("CREATE OBJECT IF NOT EXISTS " + ruleName + " (TYPE TIERING_RULE) WITH (defaultColumn = " + columnName + ", description = `" + configTieringStr + "`)").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        return ruleName;
    }

    void TTestHelper::SetTiering(const TString& tableName, const TString& ruleName) {
        auto alterQuery = TStringBuilder() << "ALTER TABLE `" << tableName <<  "` SET (TIERING = '" << ruleName << "')";
        auto result = GetSession().ExecuteSchemeQuery(alterQuery).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    void TTestHelper::ResetTiering(const TString& tableName) {
        auto alterQuery = TStringBuilder() << "ALTER TABLE `" << tableName <<  "` RESET (TIERING)";
        auto result = GetSession().ExecuteSchemeQuery(alterQuery).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    void TTestHelper::DropTable(const TString& tableName) {
        auto result = GetSession().DropTable(tableName).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    void TTestHelper::BulkUpsert(const TColumnTable& table, TTestHelper::TUpdatesBuilder& updates, const Ydb::StatusIds_StatusCode& opStatus /*= Ydb::StatusIds::SUCCESS*/) {
        Y_UNUSED(opStatus);
        NKikimr::Tests::NCS::THelper helper(GetKikimr().GetTestServer());
        auto batch = updates.BuildArrow();
        helper.SendDataViaActorSystem(table.GetName(), batch, opStatus);
    }

    void TTestHelper::BulkUpsert(const TColumnTable& table, std::shared_ptr<arrow::RecordBatch> batch, const Ydb::StatusIds_StatusCode& opStatus /*= Ydb::StatusIds::SUCCESS*/) {
        Y_UNUSED(opStatus);
        NKikimr::Tests::NCS::THelper helper(GetKikimr().GetTestServer());
        helper.SendDataViaActorSystem(table.GetName(), batch, opStatus);
    }

    void TTestHelper::ReadData(const TString& query, const TString& expected, const EStatus opStatus /*= EStatus::SUCCESS*/) {
        auto it = TableClient->StreamExecuteScanQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString()); // Means stream successfully get
        TString result = StreamResultToYson(it, false, opStatus);
        if (opStatus == EStatus::SUCCESS) {
            UNIT_ASSERT_NO_DIFF(ReformatYson(result), ReformatYson(expected));
        }
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

    void TTestHelper::SetCompression(
        const TColumnTableBase& columnTable, const TString& columnName, const TCompression& compression, const NYdb::EStatus expectedStatus) {
        auto alterQuery = columnTable.BuildAlterCompressionQuery(columnName, compression);
        auto result = GetSession().ExecuteSchemeQuery(alterQuery).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), expectedStatus, result.GetIssues().ToString());
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
            builder.Decimal(TDecimalType(22, 9));
            str << builder.Build();
            break;
        }
        default:
            str << NScheme::GetTypeName(TypeInfo.GetTypeId());
        }
        if (!NullableFlag) {
            str << " NOT NULL";
        }
        return str;
    }

    TTestHelper::TColumnSchema& TTestHelper::TColumnSchema::SetType(NScheme::TTypeId typeId) {
        TypeInfo = NScheme::TTypeInfo(typeId);
        return *this;
    }

    TString TTestHelper::TColumnTableBase::BuildQuery() const {
        auto str = TStringBuilder() << "CREATE " << GetObjectType() << " `" << Name << "`";
        str << " (" << BuildColumnsStr(Schema) << ", PRIMARY KEY (" << JoinStrings(PrimaryKey, ", ") << "))";
        if (!Sharding.empty()) {
            str << " PARTITION BY HASH(" << JoinStrings(Sharding, ", ") << ")";
        }
        str << " WITH (STORE = COLUMN";
        str << ", AUTO_PARTITIONING_MIN_PARTITIONS_COUNT =" << MinPartitionsCount;
        if (TTLConf) {
            str << ", TTL = " << TTLConf->second << " ON " << TTLConf->first;
        }
        str << ");";
        return str;
    }

    TString TTestHelper::TColumnTableBase::BuildAlterCompressionQuery(const TString& columnName, const TCompression& compression) const {
        auto str = TStringBuilder() << "ALTER OBJECT `" << Name << "` (TYPE " << GetObjectType() << ") SET";
        str << " (ACTION=ALTER_COLUMN, NAME=" << columnName << ", `SERIALIZER.CLASS_NAME`=`" << compression.GetSerializerName() << "`,";
        str << " `COMPRESSION.TYPE`=`" << NArrow::CompressionToString(compression.GetType()) << "`";
        if (compression.GetCompressionLevel() != Max<i32>()) {
            str << "`COMPRESSION.LEVEL`=" << compression.GetCompressionLevel();
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
            return arrow::field(name, arrow::boolean(), nullable);
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
            return arrow::field(name, arrow::duration(arrow::TimeUnit::TimeUnit::MICRO), nullable);
        case NScheme::NTypeIds::Date32:
            return arrow::field(name, arrow::int32(), nullable);
        case NScheme::NTypeIds::Datetime64:
        case NScheme::NTypeIds::Timestamp64:
        case NScheme::NTypeIds::Interval64:
            return arrow::field(name, arrow::int64(), nullable);
        case NScheme::NTypeIds::JsonDocument:
            return arrow::field(name, arrow::binary(), nullable);
        case NScheme::NTypeIds::Decimal:
            return arrow::field(name, arrow::decimal(22, 9));
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
