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
        CreateOlapTableWithStore(tableName, storeName, storeShardsCount, tableShardsCount);
    }
    using TBase::TBase;

    TLocalHelper(TKikimrRunner& runner)
        : TBase(runner.GetTestServer()) {

    }
};

}