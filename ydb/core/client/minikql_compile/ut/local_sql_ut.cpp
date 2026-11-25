#include <ydb/core/client/minikql_compile/local_sql.h>
#include <ydb/core/client/minikql_compile/yql_expr_minikql.h>
#include <ydb/core/engine/mkql_keys.h>
#include <ydb/core/testlib/minikql_compile.h>

#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/mkql_node_visitor.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql {
namespace {

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;
using TTypeInfo = NScheme::TTypeInfo;

const TDuration TIME_LIMIT = TDuration::Seconds(60);

struct TServices {
    TIntrusivePtr<IFunctionRegistry> FunctionRegistry;
    TScopedAlloc Alloc;
    TTypeEnvironment TypeEnv;
    TMockDbSchemeResolver DbSchemeResolver;
    TVector<THolder<TKeyDesc>> DescList;

    TServices()
        : FunctionRegistry(CreateFunctionRegistry(CreateBuiltinRegistry()))
        , Alloc(__LOCATION__)
        , TypeEnv(Alloc)
    {
        Alloc.Release();
    }

    ~TServices() {
        Alloc.Acquire();
    }

    void ExtractKeys(TRuntimeNode program) {
        Alloc.Acquire();
        TExploringNodeVisitor explorer;
        explorer.Walk(program.GetNode(), TypeEnv);
        DescList = ExtractTableKeys(explorer, TypeEnv);
        Alloc.Release();
    }
};

TLocalSqlSchema SampleSchema() {
    TLocalSqlSchema schema;

    TLocalSqlTable table;
    table.Name = "table1";
    table.Columns.push_back({"key", TTypeInfo(NUdf::TDataType<ui32>::Id), false, 0});
    table.Columns.push_back({"value", TTypeInfo(NUdf::TDataType<char*>::Id), false, -1});
    schema.Tables.emplace(table.Name, table);

    TLocalSqlTable composite;
    composite.Name = "composite";
    composite.Columns.push_back({"a", TTypeInfo(NUdf::TDataType<ui32>::Id), false, 0});
    composite.Columns.push_back({"b", TTypeInfo(NUdf::TDataType<ui32>::Id), false, 1});
    composite.Columns.push_back({"value", TTypeInfo(NUdf::TDataType<char*>::Id), false, -1});
    schema.Tables.emplace(composite.Name, composite);
    return schema;
}

void RegisterSampleTables(TServices& services) {
    using TColumn = IDbSchemeResolver::TTableResult::TColumn;
    IDbSchemeResolver::TTableResult table(IDbSchemeResolver::TTableResult::Ok);
    table.Table.TableName = "table1";
    table.Table.ColumnNames = {"key", "value"};
    table.TableId.Reset(new TTableId(1, 2));
    table.KeyColumnCount = 1;
    table.Columns.insert(std::make_pair("key", TColumn{34, 0, TTypeInfo(NUdf::TDataType<ui32>::Id), 0, EColumnTypeConstraint::Nullable}));
    table.Columns.insert(std::make_pair("value", TColumn{56, -1, TTypeInfo(NUdf::TDataType<char*>::Id), 0, EColumnTypeConstraint::Nullable}));
    services.DbSchemeResolver.AddTable(table);

    IDbSchemeResolver::TTableResult composite(IDbSchemeResolver::TTableResult::Ok);
    composite.Table.TableName = "composite";
    composite.Table.ColumnNames = {"a", "b", "value"};
    composite.TableId.Reset(new TTableId(3, 4));
    composite.KeyColumnCount = 2;
    composite.Columns.insert(std::make_pair("a", TColumn{1, 0, TTypeInfo(NUdf::TDataType<ui32>::Id), 0, EColumnTypeConstraint::Nullable}));
    composite.Columns.insert(std::make_pair("b", TColumn{2, 1, TTypeInfo(NUdf::TDataType<ui32>::Id), 0, EColumnTypeConstraint::Nullable}));
    composite.Columns.insert(std::make_pair("value", TColumn{3, -1, TTypeInfo(NUdf::TDataType<char*>::Id), 0, EColumnTypeConstraint::Nullable}));
    services.DbSchemeResolver.AddTable(composite);
}

TRuntimeNode Compile(const TString& sql, TServices& services) {
    const auto translated = TranslateLocalSql(sql, SampleSchema());
    UNIT_ASSERT_C(translated.IsOk(), translated.Error);

    auto expr = ParseText(translated.Program);
    auto future = ConvertToMiniKQL(expr, services.FunctionRegistry.Get(), &services.TypeEnv, &services.DbSchemeResolver);
    auto result = future.GetValue(TIME_LIMIT);
    result.Errors.PrintTo(Cerr);
    UNIT_ASSERT_C(result.Node.GetNode(), translated.Program);
    return result.Node;
}

TString TranslationError(const TString& sql) {
    const auto translated = TranslateLocalSql(sql, SampleSchema());
    UNIT_ASSERT_C(!translated.IsOk(), translated.Program);
    return translated.Error;
}

} // namespace

Y_UNIT_TEST_SUITE(TLocalSql) {
    Y_UNIT_TEST(SExprIsNotSql) {
        UNIT_ASSERT(IsLocalMiniKqlSExpr(" (let x '1) "));
        UNIT_ASSERT(IsLocalMiniKqlSExpr(""));
        UNIT_ASSERT(!IsLocalMiniKqlSExpr("SELECT value FROM table1 WHERE key = 1"));
        UNIT_ASSERT(!IsLocalMiniKqlSExpr("-- comment\nSELECT value FROM table1"));
    }

    Y_UNIT_TEST(PointSelect) {
        TServices services;
        RegisterSampleTables(services);
        auto program = Compile("SELECT value FROM table1 WHERE key = 23", services);
        services.ExtractKeys(program);
        UNIT_ASSERT_VALUES_EQUAL(services.DescList.size(), 1);
        const auto& key = *services.DescList[0];
        UNIT_ASSERT_EQUAL(key.RowOperation, TKeyDesc::ERowOperation::Read);
        UNIT_ASSERT(key.Range.Point);
        UNIT_ASSERT_VALUES_EQUAL(key.Range.From.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(key.Range.From[0].AsValue<ui32>(), 23u);
    }

    Y_UNIT_TEST(RangeSelect) {
        TServices services;
        RegisterSampleTables(services);
        auto program = Compile("SELECT value FROM table1 WHERE key >= 1 AND key < 10 ORDER BY key DESC LIMIT 5", services);
        services.ExtractKeys(program);
        UNIT_ASSERT_VALUES_EQUAL(services.DescList.size(), 1);
        const auto& key = *services.DescList[0];
        UNIT_ASSERT(!key.Range.Point);
        UNIT_ASSERT(key.Range.InclusiveFrom);
        UNIT_ASSERT(!key.Range.InclusiveTo);
        UNIT_ASSERT(key.Reverse);
        UNIT_ASSERT_VALUES_EQUAL(key.RangeLimits.ItemsLimit, 5u);
        UNIT_ASSERT_VALUES_EQUAL(key.Range.From.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(key.Range.To.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(key.Range.From[0].AsValue<ui32>(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(key.Range.To[0].AsValue<ui32>(), 10u);
    }

    Y_UNIT_TEST(FullScan) {
        TServices services;
        RegisterSampleTables(services);
        auto program = Compile("SELECT value FROM table1", services);
        services.ExtractKeys(program);
        UNIT_ASSERT_VALUES_EQUAL(services.DescList.size(), 1);
        const auto& key = *services.DescList[0];
        UNIT_ASSERT(!key.Range.Point);
        UNIT_ASSERT(key.Range.InclusiveFrom);
        UNIT_ASSERT(key.Range.InclusiveTo);
        UNIT_ASSERT_VALUES_EQUAL(key.Range.From.size(), 1);
        UNIT_ASSERT(key.Range.From[0].IsNull());
        UNIT_ASSERT(key.Range.To.empty());
        UNIT_ASSERT(!key.Range.IsAmbiguous(1));
    }

    Y_UNIT_TEST(UpperBoundSelect) {
        TServices services;
        RegisterSampleTables(services);
        auto program = Compile("SELECT value FROM table1 WHERE key < 10", services);
        services.ExtractKeys(program);
        const auto& key = *services.DescList[0];
        UNIT_ASSERT(key.Range.InclusiveFrom);
        UNIT_ASSERT(!key.Range.InclusiveTo);
        UNIT_ASSERT_VALUES_EQUAL(key.Range.From.size(), 1);
        UNIT_ASSERT(key.Range.From[0].IsNull());
        UNIT_ASSERT_VALUES_EQUAL(key.Range.To.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(key.Range.To[0].AsValue<ui32>(), 10u);
        UNIT_ASSERT(!key.Range.IsAmbiguous(1));
    }

    Y_UNIT_TEST(CompositePrefix) {
        TServices services;
        RegisterSampleTables(services);
        auto program = Compile("SELECT value FROM composite WHERE a = 7 AND b >= 3", services);
        services.ExtractKeys(program);
        UNIT_ASSERT_VALUES_EQUAL(services.DescList.size(), 1);
        const auto& key = *services.DescList[0];
        UNIT_ASSERT(!key.Range.Point);
        UNIT_ASSERT(key.Range.InclusiveFrom);
        UNIT_ASSERT(key.Range.InclusiveTo);
        UNIT_ASSERT_VALUES_EQUAL(key.Range.From.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(key.Range.To.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(key.Range.From[0].AsValue<ui32>(), 7u);
        UNIT_ASSERT_VALUES_EQUAL(key.Range.From[1].AsValue<ui32>(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(key.Range.To[0].AsValue<ui32>(), 7u);
        UNIT_ASSERT(!key.Range.IsAmbiguous(2));
    }

    Y_UNIT_TEST(UpdateDeleteAndUpsert) {
        TServices services;
        RegisterSampleTables(services);
        auto update = Compile("UPDATE table1 SET value = 'a b' WHERE key = 4", services);
        services.ExtractKeys(update);
        UNIT_ASSERT_EQUAL(services.DescList[0]->RowOperation, TKeyDesc::ERowOperation::Update);
        UNIT_ASSERT_VALUES_EQUAL(services.DescList[0]->Range.From[0].AsValue<ui32>(), 4u);

        auto erase = Compile("DELETE FROM table1 WHERE key = 5", services);
        services.ExtractKeys(erase);
        UNIT_ASSERT_EQUAL(services.DescList[0]->RowOperation, TKeyDesc::ERowOperation::Erase);

        auto upsert = Compile("UPSERT INTO table1 (key, value) VALUES (6, 'abc')", services);
        services.ExtractKeys(upsert);
        UNIT_ASSERT_EQUAL(services.DescList[0]->RowOperation, TKeyDesc::ERowOperation::Update);
        UNIT_ASSERT_VALUES_EQUAL(services.DescList[0]->Range.From[0].AsValue<ui32>(), 6u);
    }

    Y_UNIT_TEST(ParameterCompiles) {
        TServices services;
        RegisterSampleTables(services);
        const auto translated = TranslateLocalSql("SELECT value FROM table1 WHERE key = $id", SampleSchema());
        UNIT_ASSERT_C(translated.IsOk(), translated.Error);
        auto expr = ParseText(translated.Program);
        auto result = ConvertToMiniKQL(expr, services.FunctionRegistry.Get(), &services.TypeEnv, &services.DbSchemeResolver).GetValue(TIME_LIMIT);
        result.Errors.PrintTo(Cerr);
        UNIT_ASSERT_C(result.Node.GetNode(), translated.Program);
    }

    Y_UNIT_TEST(RejectsWhatTheEngineCannotSee) {
        auto contains = [](const TString& error, const char* text) {
            UNIT_ASSERT_C(error.find(text) != TString::npos, error);
        };
        contains(TranslationError("SELECT value FROM table1 WHERE value = 'x'"), "not a key");
        contains(TranslationError("DELETE FROM table1 WHERE key > 1"), "equality");
        contains(TranslationError("SELECT value FROM composite WHERE b = 1"), "prefix");
        contains(TranslationError("SELECT value FROM composite WHERE a < 1"), "ambiguous");
        contains(TranslationError("SELECT value FROM missing WHERE key = 1"), "unknown table");
    }
};

} // namespace NYql
