#pragma once

#include "kqp_ut_common.h"

#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/formats/arrow/simple_builder/array.h>
#include <ydb/library/formats/arrow/simple_builder/batch.h>
#include <ydb/library/formats/arrow/simple_builder/filler.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status_codes.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

namespace NKikimr {
namespace NKqp {
class TTestHelper {
public:
    class TCompression {
        YDB_ACCESSOR(TString, SerializerClassName, "ARROW_SERIALIZER");
        YDB_OPT(NKikimrSchemeOp::EColumnCodec, CompressionType);
        YDB_ACCESSOR_DEF(std::optional<i32>, CompressionLevel);

    public:
        bool DeserializeFromProto(const NKikimrSchemeOp::TOlapColumn::TSerializer& serializer);
        TString BuildQuery() const;

        bool IsEqual(const TCompression& rhs, TString& errorMessage) const;

        TString ToString() const;
    };

    class TColumnFamily {
        YDB_ACCESSOR(ui32, Id, 0);
        YDB_ACCESSOR_DEF(TString, FamilyName);
        YDB_ACCESSOR_DEF(TString, Data);
        YDB_ACCESSOR_DEF(TCompression, Compression);

    public:
        bool DeserializeFromProto(const NKikimrSchemeOp::TFamilyDescription& family);
        TString BuildQuery() const;

        bool IsEqual(const TColumnFamily& rhs, TString& errorMessage) const;

        TString ToString() const;
    };

    class TColumnSchema {
        YDB_ACCESSOR_DEF(TString, Name);
        YDB_ACCESSOR_DEF(NScheme::TTypeInfo, TypeInfo);
        YDB_FLAG_ACCESSOR(Nullable, true);
        YDB_ACCESSOR_DEF(TString, ColumnFamilyName);

    public:
        TString BuildQuery() const;

        TColumnSchema& SetType(const NScheme::TTypeInfo& typeInfo);
    };

    using TUpdatesBuilder = NColumnShard::TTableUpdatesBuilder;

    class TColumnTableBase {
        YDB_ACCESSOR_DEF(TString, Name);
        YDB_ACCESSOR_DEF(TVector<TColumnSchema>, Schema);
        YDB_ACCESSOR_DEF(TVector<TString>, PrimaryKey);
        YDB_ACCESSOR_DEF(TVector<TString>, Sharding);
        YDB_ACCESSOR(ui32, MinPartitionsCount, 1);
        YDB_ACCESSOR_DEF(TVector<TColumnFamily>, ColumnFamilies);

        std::optional<std::pair<TString, TString>> TTLConf;

    public:
        TString BuildQuery() const;
        TString BuildAlterCompressionQuery(const TString& columnName, const TCompression& compression) const;
        std::shared_ptr<arrow::Schema> GetArrowSchema(const TVector<TColumnSchema>& columns);

        TColumnTableBase& SetTTL(const TString& columnName, const TString& ttlConf) {
            TTLConf = std::make_pair(columnName, ttlConf);
            return *this;
        }

    private:
        virtual TString GetObjectType() const = 0;
        TString BuildColumnsStr(const TVector<TColumnSchema>& clumns) const;
        std::shared_ptr<arrow::Field> BuildField(const TString name, const NScheme::TTypeInfo& typeInfo, bool nullable) const;
    };

    class TColumnTable: public TColumnTableBase {
    private:
        TString GetObjectType() const override;
    };

    class TColumnTableStore: public TColumnTableBase {
    private:
        TString GetObjectType() const override;
    };

private:
    std::unique_ptr<TKikimrRunner> Kikimr;
    std::unique_ptr<NYdb::NTable::TTableClient> TableClient;
    std::unique_ptr<NYdb::NTable::TSession> Session;

public:
    TTestHelper(const TKikimrSettings& settings);
    TKikimrRunner& GetKikimr();
    TTestActorRuntime& GetRuntime();
    NYdb::NTable::TSession& GetSession();
    void CreateTable(const TColumnTableBase& table, const NYdb::EStatus expectedStatus = NYdb::EStatus::SUCCESS);
    void DropTable(const TString& tableName);
    void EnsureSecret(const TString& name, const TString& value);
    void CreateTier(const TString& tierName);
    TString CreateTieringRule(const TString& tierName, const TString& columnName);
    void SetTiering(const TString& tableName, const TString& tierName, const TString& columnName);
    void ResetTiering(const TString& tableName);
    void BulkUpsert(
        const TColumnTable& table, TTestHelper::TUpdatesBuilder& updates, const Ydb::StatusIds_StatusCode& opStatus = Ydb::StatusIds::SUCCESS);
    void BulkUpsert(const TColumnTable& table, std::shared_ptr<arrow::RecordBatch> batch,
        const Ydb::StatusIds_StatusCode& opStatus = Ydb::StatusIds::SUCCESS);
    void ReadData(const TString& query, const TString& expected, const NYdb::EStatus opStatus = NYdb::EStatus::SUCCESS) const;
    void RebootTablets(const TString& tableName);
    void WaitTabletDeletionInHive(ui64 tabletId, TDuration duration);
    void SetCompression(const TColumnTableBase& columnTable, const TString& columnName, const TCompression& compression,
        const NYdb::EStatus expectedStatus = NYdb::EStatus::SUCCESS);
};
}
}
