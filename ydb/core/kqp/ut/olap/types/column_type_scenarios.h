#pragma once

#include "column_type_test_base.h"

namespace NKikimr::NKqp {

struct TQueryCheck {
    TString Query;
    TString Expected;
};

template <typename TValue>
struct TScenarioLoad {
    TVector<TTypedRow<TValue>> Data;
    TVector<TQueryCheck> ChecksAfter;
};

template <typename TValue>
struct TScenario {
    TString TableName;
    TVector<TTypedRow<TValue>> Data;
    TVector<TQueryCheck> Checks;
    TVector<TScenarioLoad<TValue>> ExtraLoads{};
};

template <typename TValue>
struct TJoinScenario {
    TString Table1Name;
    TString Table2Name;
    TVector<TTypedRow<TValue>> Table1Data;
    TVector<TTypedRow<TValue>> Table2Data;
    TVector<TQueryCheck> Checks;
};

struct TPkLookupScenario {
    TString TableName;
    TVector<TQueryCheck> Checks;
};

struct TCsvScenario {
    TString TableName;
    TString CsvData;
    TVector<TQueryCheck> Checks;
};

template <typename TTraits>
void RunScenario(const TScenario<typename TTraits::TValue>& scenario,
    EQueryMode scan, ETableKind table, ELoadKind load) {
    using Base = TColumnTypeTestBase<TTraits>;

    TTestHelper helper(TTraits::CreateSettings());
    TTestHelper::TColumnTable col;
    TVector<TTestHelper::TColumnSchema> schema;
    Base::PrepareBase(helper, table, scenario.TableName, &col, &schema);
    Base::LoadData(helper, table, load, scenario.TableName, scenario.Data, &col, &schema);

    for (auto&& check : scenario.Checks) {
        CheckOrExec(helper, check.Query, check.Expected, scan);
    }

    for (auto&& extra : scenario.ExtraLoads) {
        Base::LoadData(helper, table, load, scenario.TableName, extra.Data, &col, &schema);
        for (auto&& check : extra.ChecksAfter) {
            CheckOrExec(helper, check.Query, check.Expected, scan);
        }
    }
}

template <typename TTraits>
void RunJoinScenario(const TJoinScenario<typename TTraits::TValue>& scenario,
    EQueryMode scan, ETableKind table, ELoadKind load) {
    using Base = TColumnTypeTestBase<TTraits>;

    TTestHelper helper(TTraits::CreateSettings());
    TTestHelper::TColumnTable col1;
    TTestHelper::TColumnTable col2;
    TVector<TTestHelper::TColumnSchema> s1;
    TVector<TTestHelper::TColumnSchema> s2;

    if (table == ETableKind::COLUMNSHARD) {
        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("id").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("int").SetType(NScheme::NTypeIds::Int64),
            TTestHelper::TColumnSchema().SetName(TTraits::ColumnName).SetType(TTraits::GetTypeId()),
        };

        s1 = schema;
        col1.SetName(scenario.Table1Name).SetPrimaryKey({ "id" }).SetSharding({ "id" }).SetSchema(schema);
        helper.CreateTable(col1);
        s2 = schema;
        col2.SetName(scenario.Table2Name).SetPrimaryKey({ "id" }).SetSharding({ "id" }).SetSchema(schema);
        helper.CreateTable(col2);
    } else {
        Base::CreateDataShardTable(helper, scenario.Table1Name);
        Base::CreateDataShardTable(helper, scenario.Table2Name);
    }

    Base::LoadData(helper, table, load, scenario.Table1Name, scenario.Table1Data, &col1, &s1);
    Base::LoadData(helper, table, load, scenario.Table2Name, scenario.Table2Data, &col2, &s2);

    for (auto&& check : scenario.Checks) {
        CheckOrExec(helper, check.Query, check.Expected, scan);
    }
}

template <typename TTraits>
void RunPkLookupScenario(const TPkLookupScenario& scenario, EQueryMode scan, ELoadKind load) {
    TTestHelper helper(TTraits::CreateSettings());
    TVector<TTestHelper::TColumnSchema> schema = {
        TTestHelper::TColumnSchema().SetName(TTraits::ColumnName).SetType(TTraits::GetTypeId()).SetNullable(false),
        TTestHelper::TColumnSchema().SetName("val").SetType(NScheme::NTypeIds::Int64),
    };

    TTestHelper::TColumnTable testTable;
    testTable.SetName(scenario.TableName)
        .SetPrimaryKey({ TTraits::ColumnName })
        .SetSharding({ TTraits::ColumnName })
        .SetSchema(schema);
    helper.CreateTable(testTable);
    TTraits::LoadPkTable(helper, load, scenario.TableName, testTable);

    for (auto&& check : scenario.Checks) {
        CheckOrExec(helper, check.Query, check.Expected, scan);
    }
}

template <typename TTraits>
void RunCsvScenario(const TCsvScenario& scenario, EQueryMode scan, ETableKind table) {
    using Base = TColumnTypeTestBase<TTraits>;

    TTestHelper helper(TTraits::CreateSettings());
    TTestHelper::TColumnTable col;
    TVector<TTestHelper::TColumnSchema> schema;
    Base::PrepareBase(helper, table, scenario.TableName, &col, &schema);

    auto result = helper.GetKikimr().GetTableClient().BulkUpsert(
        scenario.TableName, EDataFormat::CSV, scenario.CsvData).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    for (auto&& check : scenario.Checks) {
        CheckOrExec(helper, check.Query, check.Expected, scan);
    }
}

#define Y_UNIT_TEST_SCENARIO(Name, ScenarioFn) \
    Y_UNIT_TEST(Name, EQueryMode, ETableKind, ELoadKind) { \
        RunScenario<TTraits>(ScenarioFn(), Arg<0>(), Arg<1>(), Arg<2>()); \
    }

#define Y_UNIT_TEST_JOIN_SCENARIO(Name, ScenarioFn) \
    Y_UNIT_TEST(Name, EQueryMode, ETableKind, ELoadKind) { \
        RunJoinScenario<TTraits>(ScenarioFn(), Arg<0>(), Arg<1>(), Arg<2>()); \
    }

#define Y_UNIT_TEST_PK_SCENARIO(Name, ScenarioFn) \
    Y_UNIT_TEST(Name, EQueryMode, ELoadKind) { \
        RunPkLookupScenario<TTraits>(ScenarioFn(), Arg<0>(), Arg<1>()); \
    }

#define Y_UNIT_TEST_CSV_SCENARIO(Name, ScenarioFn) \
    Y_UNIT_TEST(Name, EQueryMode, ETableKind) { \
        RunCsvScenario<TTraits>(ScenarioFn(), Arg<0>(), Arg<1>()); \
    }

}   // namespace NKikimr::NKqp
