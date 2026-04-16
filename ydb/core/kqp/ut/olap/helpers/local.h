#pragma once
#include <ydb/core/testlib/cs_helper.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr::NKqp {

class TTableWithNullsHelper: public Tests::NCS::TTableWithNullsHelper {
private:
    using TBase = Tests::NCS::TTableWithNullsHelper;
public:
    using TBase::TBase;

    TTableWithNullsHelper(TKikimrRunner& runner)
        : TBase(runner.GetTestServer()) {
    }

    void CreateTableWithNulls(TString tableName = "tableWithNulls", ui32 shardsCount = 4);
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
        CreateOlapTablesWithStore({tableName}, storeName, storeShardsCount, tableShardsCount);
    }

    void CreateTestOlapTables(TVector<TString> tableNames = {"olapTable0", "olapTable1"}, TString storeName = "olapStore",
        ui32 storeShardsCount = 4, ui32 tableShardsCount = 3) {
        CreateOlapTablesWithStore(tableNames, storeName, storeShardsCount, tableShardsCount);
    }

    void CreateTestOlapStandaloneTable(TString tableName = "olapTable", ui32 tableShardsCount = 3) {
        CreateOlapTables({tableName}, tableShardsCount);
    }

    using TBase::TBase;

    TLocalHelper(TKikimrRunner& runner)
        : TBase(runner.GetTestServer()) {

    }
};

/// Same as TLocalHelper but HashSharding uses only `timestamp`, matching a single-column PK (no `uid` in sharding).
class TLocalHelperSinglePkShard : public TLocalHelper {
public:
    using TLocalHelper::TLocalHelper;

    std::vector<TString> GetShardingColumns() const override {
        return {"timestamp"};
    }

    /// OLAP table with PK = (timestamp) only.
    void CreateTestOlapTableSinglePkColumn(TString tableName = "onePkOlap", TString storeName = "olapStoreOnePk",
        ui32 storeShardsCount = 4, ui32 tableShardsCount = 3)
    {
        const TString schema = R"(
            Columns { Name: "timestamp" Type: "Timestamp" NotNull: true }
            Columns { Name: "payload" Type: "Int32" }
            KeyColumnNames: "timestamp"
        )";
        CreateSchemaOlapTablesWithStore(schema, {tableName}, storeName, storeShardsCount, tableShardsCount);
    }
};

}
