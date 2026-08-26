#pragma once

#include "column_type_test_enums.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/kqp/ut/common/arrow_builders.h>
#include <ydb/core/kqp/ut/common/columnshard.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/common_helper.h>
#include <ydb/core/testlib/cs_helper.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_replication.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <library/cpp/threading/local_executor/local_executor.h>
#include <util/generic/serialized_enum.h>
#include <util/string/printf.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

template <typename TValue>
struct TTypedRow {
    i32 Id;
    i64 IntVal;
    std::optional<TValue> TypedVal;
};

inline void CheckOrExec(TTestHelper& helper, const TString& query, const TString& expected, EQueryMode scanMode) {
    if (scanMode == EQueryMode::SCAN_QUERY) {
        helper.ReadData(query, expected);
    } else {
        helper.ReadDataExecQuery(query, expected);
    }
}

template <typename TConfigure>
TKikimrSettings CreateColumnshardSettings(TConfigure&& configure) {
    NKikimrConfig::TFeatureFlags featureFlags;
    configure(featureFlags);
    return TKikimrSettings().SetWithSampleTables(false).SetFeatureFlags(featureFlags);
}

// TTraits contract:
//   using TValue = ...;
//   static constexpr const char* ColumnName;
//   static constexpr const char* SqlTypeName;
//   static TKikimrSettings CreateSettings();
//   static NScheme::TTypeId GetTypeId();
//   static void AppendYdbValue(TValueBuilder& builder, const std::optional<TValue>& val);
//   static void AppendCsvValue(TStringBuilder& builder, const std::optional<TValue>& val);
//   static std::shared_ptr<arrow::Array> MakeArrowArray(const TVector<TTypedRow<TValue>>& rows);
//   static std::shared_ptr<arrow::DataType> ArrowType();
template <typename TTraits>
struct TColumnTypeTestBase {
    using TValue = typename TTraits::TValue;
    using TRow = TTypedRow<TValue>;

    static TString GetSqlTypeName() {
        if constexpr (requires { { TTraits::BuildSqlTypeName() } -> std::convertible_to<TString>; }) {
            return TTraits::BuildSqlTypeName();
        } else {
            return TTraits::SqlTypeName;
        }
    }

    static void CreateDataShardTable(TTestHelper& helper, const TString& name) {
        auto& session = helper.GetSession();
        auto res = session
                       .ExecuteSchemeQuery(TStringBuilder() << R"(
                CREATE TABLE `)" << name << R"(` (
                    id Int32 NOT NULL,
                    int Int64,
                    )" << TTraits::ColumnName << " " << GetSqlTypeName() << R"(,
                    PRIMARY KEY (id)
                );
            )")
                       .ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), NYdb::EStatus::SUCCESS);
    }

    static void BulkUpsertRowTableYdbValue(TTestHelper& helper, const TString& name, const TVector<TRow>& rows) {
        TValueBuilder builder;
        builder.BeginList();
        for (auto&& r : rows) {
            builder.AddListItem().BeginStruct()
                .AddMember("id").Int32(r.Id)
                .AddMember("int").Int64(r.IntVal)
                .AddMember(TTraits::ColumnName);
            TTraits::AppendYdbValue(builder, r.TypedVal);
            builder.EndStruct();
        }

        builder.EndList();
        auto result = helper.GetKikimr().GetTableClient().BulkUpsert(name, builder.Build()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    static void BulkUpsertRowTableCSV(TTestHelper& helper, const TString& name, const TVector<TRow>& rows) {
        BulkUpsertRowTableCSVWithColumnName(helper, name, rows, "int");
    }

    static void BulkUpsertRowTableCSVWithColumnName(
        TTestHelper& helper, const TString& name, const TVector<TRow>& rows, const TString& columnName) {
        Y_UNUSED(columnName);
        TStringBuilder builder;
        for (auto&& r : rows) {
            builder << r.Id << "," << r.IntVal << ",";
            TTraits::AppendCsvValue(builder, r.TypedVal);
            builder << '\n';
        }

        auto result = helper.GetKikimr().GetTableClient().BulkUpsert(name, EDataFormat::CSV, builder).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    static std::shared_ptr<arrow::RecordBatch> MakeArrowBatch(const TVector<TRow>& rows) {
        using namespace NKikimr::NKqp::NTestArrow;
        std::vector<int32_t> ids;
        std::vector<int64_t> vals;
        ids.reserve(rows.size());
        vals.reserve(rows.size());
        for (auto&& r : rows) {
            ids.push_back(r.Id);
            vals.push_back(r.IntVal);
        }

        auto idArr = MakeInt32Array(ids);
        auto intArr = MakeInt64Array(vals);
        auto typedArr = TTraits::MakeArrowArray(rows);
        return MakeBatch(
            { arrow::field("id", arrow::int32(), false),
              arrow::field("int", arrow::int64()),
              arrow::field(TTraits::ColumnName, TTraits::ArrowType()) },
            { idArr, intArr, typedArr });
    }

    static void BulkUpsertRowTableArrow(TTestHelper& helper, const TString& name, const TVector<TRow>& rows) {
        auto batch = MakeArrowBatch(rows);
        TString strBatch = NArrow::SerializeBatchNoCompression(batch);
        TString strSchema = NArrow::SerializeSchema(*batch->schema());
        auto result = helper.GetKikimr().GetTableClient().BulkUpsert(
            name, NYdb::NTable::EDataFormat::ApacheArrow, strBatch, strSchema).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    static void LoadData(TTestHelper& helper, ETableKind table, ELoadKind load,
        const TString& name, const TVector<TRow>& rows,
        TTestHelper::TColumnTable* col = nullptr,
        const TVector<TTestHelper::TColumnSchema>* schema = nullptr) {
        switch (table) {
            case ETableKind::COLUMNSHARD: {
                Y_ABORT_UNLESS(col && schema);
                TString columnName = "int";
                if (schema->size() >= 2) {
                    columnName = (*schema)[1].GetName();
                }

                if (load == ELoadKind::ARROW) {
                    auto batch = MakeArrowBatchWithColumnName(rows, columnName);
                    helper.BulkUpsert(*col, batch);
                } else if (load == ELoadKind::YDB_VALUE) {
                    BulkUpsertRowTableYdbValueWithColumnName(helper, name, rows, columnName);
                } else {
                    BulkUpsertRowTableCSVWithColumnName(helper, name, rows, columnName);
                }

                break;
            }
            case ETableKind::DATASHARD: {
                if (load == ELoadKind::ARROW) {
                    BulkUpsertRowTableArrow(helper, name, rows);
                } else if (load == ELoadKind::YDB_VALUE) {
                    BulkUpsertRowTableYdbValue(helper, name, rows);
                } else {
                    BulkUpsertRowTableCSV(helper, name, rows);
                }

                break;
            }
        }
    }

    static void PrepareBase(TTestHelper& helper, ETableKind tableKind, const TString& tableName,
        TTestHelper::TColumnTable* colTableOut, TVector<TTestHelper::TColumnSchema>* schemaOut) {
        if (tableKind == ETableKind::COLUMNSHARD) {
            TVector<TTestHelper::TColumnSchema> schema = {
                TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
                TTestHelper::TColumnSchema().SetName("int").SetType(NScheme::NTypeIds::Int64),
                TTestHelper::TColumnSchema().SetName(TTraits::ColumnName).SetType(TTraits::GetTypeId()),
            };

            *schemaOut = schema;
            TTestHelper::TColumnTable col;
            col.SetName(tableName).SetPrimaryKey({ "id" }).SetSharding({ "id" }).SetSchema(schema);
            helper.CreateTable(col);
            *colTableOut = col;
        } else {
            CreateDataShardTable(helper, tableName);
        }
    }

    static void CreateDataShardTableWithSecondColumn(TTestHelper& helper, const TString& name, const TString& secondName) {
        auto& session = helper.GetSession();
        auto res = session
                       .ExecuteSchemeQuery(TStringBuilder() << R"(
                CREATE TABLE `)" << name << R"(` (
                    id Int32 NOT NULL,
                    )" << secondName << R"( Int64,
                    )" << TTraits::ColumnName << " " << GetSqlTypeName() << R"(,
                    PRIMARY KEY (id)
                );
            )")
                       .ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), NYdb::EStatus::SUCCESS);
    }

    static void BulkUpsertRowTableYdbValueWithColumnName(
        TTestHelper& helper, const TString& name, const TVector<TRow>& rows, const TString& columnName) {
        TValueBuilder builder;
        builder.BeginList();
        for (auto&& r : rows) {
            builder.AddListItem().BeginStruct()
                .AddMember("id").Int32(r.Id)
                .AddMember(columnName).Int64(r.IntVal)
                .AddMember(TTraits::ColumnName);
            TTraits::AppendYdbValue(builder, r.TypedVal);
            builder.EndStruct();
        }

        builder.EndList();
        auto result = helper.GetKikimr().GetTableClient().BulkUpsert(name, builder.Build()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    static void BulkUpsertRowTableYdbValueWithSecondColumn(
        TTestHelper& helper, const TString& name, const TVector<TRow>& rows, const TString& secondName) {
        BulkUpsertRowTableYdbValueWithColumnName(helper, name, rows, secondName);
    }

    static std::shared_ptr<arrow::RecordBatch> MakeArrowBatchWithColumnName(const TVector<TRow>& rows, const TString& columnName) {
        using namespace NKikimr::NKqp::NTestArrow;
        std::vector<int32_t> ids;
        std::vector<int64_t> vals;
        ids.reserve(rows.size());
        vals.reserve(rows.size());
        for (auto&& r : rows) {
            ids.push_back(r.Id);
            vals.push_back(r.IntVal);
        }

        auto idArr = MakeInt32Array(ids);
        auto intArr = MakeInt64Array(vals);
        auto typedArr = TTraits::MakeArrowArray(rows);
        return MakeBatch(
            { arrow::field("id", arrow::int32(), false),
              arrow::field(std::string(columnName), arrow::int64()),
              arrow::field(TTraits::ColumnName, TTraits::ArrowType()) },
            { idArr, intArr, typedArr });
    }

    static std::shared_ptr<arrow::RecordBatch> MakeArrowBatchWithSecondColumn(const TVector<TRow>& rows, const TString& secondName) {
        return MakeArrowBatchWithColumnName(rows, secondName);
    }

    static void BulkUpsertRowTableArrowWithSecondColumn(
        TTestHelper& helper, const TString& name, const TVector<TRow>& rows, const TString& secondName) {
        auto batch = MakeArrowBatchWithSecondColumn(rows, secondName);
        TString strBatch = NArrow::SerializeBatchNoCompression(batch);
        TString strSchema = NArrow::SerializeSchema(*batch->schema());
        auto result = helper.GetKikimr().GetTableClient().BulkUpsert(
            name, NYdb::NTable::EDataFormat::ApacheArrow, strBatch, strSchema).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }
};

template <typename TTraits>
struct TColumnTypeTestContext {
    using Traits = TTraits;
    using Base = TColumnTypeTestBase<TTraits>;
    using TRow = TTypedRow<typename TTraits::TValue>;
    using TValue = typename TTraits::TValue;

    static TKikimrSettings Settings() { return TTraits::CreateSettings(); }
};

#define COLUMN_TYPE_TEST_USING(Traits)          \
    using TTraits = Traits;                     \
    using Ctx = TColumnTypeTestContext<TTraits>; \
    using Base = Ctx::Base;                     \
    using TRow = Ctx::TRow;

}   // namespace NKikimr::NKqp
