#include <ydb/core/kqp/host/new_parser/kqp_name_resolver.h>
#include <ydb/core/kqp/host/new_parser/kqp_new_parser.h>

#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>

namespace NKikimr::NKqp {
namespace {

using namespace NNewYql;

// Use an explicit catalog and record every lookup so tests can distinguish
// lexical CTE binding from physical table lookup. Keep Ast alive because the
// resolution result indexes its nodes by address.
struct TFixture {
    THashMap<std::pair<TString, TString>, TTableMetadataPtr> Catalog;
    TVector<std::pair<TString, TString>> Lookups;
    std::unique_ptr<TScript> Ast;
    TNameResolutionResult Result;

    TFixture() {
        AddTable("local", "/Root/t", {{"id", 1, "Int64", true}, {"value", 2, "String", false}});
        AddTable("local", "/Root/u", {{"id", 3, "Int64", true}, {"other", 4, "Int64", false}});
    }

    void AddTable(TString cluster, TString path, TVector<TColumnMetadata> columns) {
        auto table = std::make_shared<TTableMetadata>();
        table->Cluster = cluster;
        table->Path = path;
        table->OwnerId = 42;
        table->TableId = 100 + Catalog.size();
        table->SchemaVersion = 7;
        table->Columns = std::move(columns);
        Catalog[std::make_pair(cluster, path)] = std::move(table);
    }

    void Resolve(const TString& text, TNameResolutionSettings settings = {"local", "/Root", {}}) {
        auto parsed = ParseQueryWithNewParser(text, {}, false);
        UNIT_ASSERT_C(parsed.IsOk(), parsed.Issues.ToString());
        Ast = std::move(parsed.Ast);
        Result = ResolveNames(*Ast, std::move(settings), [&](const TString& cluster, const TString& path) {
            Lookups.emplace_back(cluster, path);
            const auto* table = Catalog.FindPtr(std::make_pair(cluster, path));
            return table ? *table : TTableMetadataPtr{};
        });
    }

    void Ok(const TString& text) {
        Resolve(text);
        UNIT_ASSERT_C(Result.IsOk(), Result.Issues.ToString());
    }

    const TQuery& Query(size_t index = 0) const {
        return *std::get<TQueryStatement>(Ast->Statements[index].Value).Query;
    }

    const TSelect& Select(size_t index = 0) const {
        return std::get<TSelect>(Query(index).Body);
    }

    const TResolvedColumn& Column(const TExpression& expression) const {
        return Result.Names->Columns.at(Result.Names->ColumnReferences.at(&expression));
    }
};

} // namespace

Y_UNIT_TEST_SUITE(KqpNewYqlNameResolution) {
    Y_UNIT_TEST(CollectsPhysicalTablesBeforeColumnResolution) {
        TFixture f;
        f.Ok("$q = (SELECT id FROM t); "
            "WITH c AS (SELECT id FROM u), d AS (SELECT id FROM c) "
            "SELECT q.id FROM $q AS q JOIN d ON q.id = d.id "
            "WHERE EXISTS (SELECT id FROM t WHERE id IN (SELECT id FROM u)); "
            "SELECT id FROM (SELECT id FROM t UNION ALL SELECT id FROM u) AS s;");
        const auto tables = CollectTableReferences(*f.Ast, {"local", "/Root", {}});
        UNIT_ASSERT_C(tables.IsOk(), tables.Issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(tables.Tables.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(tables.Tables[0].second, "/Root/t");
        UNIT_ASSERT_VALUES_EQUAL(tables.Tables[1].second, "/Root/u");
        for (const auto& key : f.Lookups) {
            UNIT_ASSERT(std::find(tables.Tables.begin(), tables.Tables.end(), key) != tables.Tables.end());
        }
    }

    Y_UNIT_TEST(TableCollectionKeepsCteScopeAndPathPragmaOrder) {
        auto parsed = ParseQueryWithNewParser(
            "PRAGMA TablePathPrefix = '/first'; $q = (SELECT * FROM t); "
            "PRAGMA TablePathPrefix = '/second'; "
            "WITH t AS (SELECT * FROM t), u AS (SELECT * FROM t) "
            "SELECT * FROM u UNION ALL SELECT * FROM $q; "
            "SELECT * FROM u; SELECT * FROM other.t; SELECT * FROM `/absolute`;", {}, false);
        UNIT_ASSERT_C(parsed.IsOk(), parsed.Issues.ToString());
        const auto tables = CollectTableReferences(*parsed.Ast, {"local", "/Root", {{"other", "/remote"}}});
        UNIT_ASSERT_C(tables.IsOk(), tables.Issues.ToString());
        const TVector<std::pair<TString, TString>> expected = {
            {"local", "/first/t"}, {"local", "/second/t"}, {"local", "/second/u"},
            {"other", "/remote/t"}, {"local", "/absolute"},
        };
        UNIT_ASSERT(tables.Tables == expected);
    }

    Y_UNIT_TEST(TableCollectionPreservesQuotedNamesAndFallback) {
        auto parsed = ParseQueryWithNewParser(
            "--!ansi_lexer\nWITH \"c\" AS (SELECT * FROM \"t\") SELECT * FROM \"c\";", {}, false);
        UNIT_ASSERT_C(parsed.IsOk(), parsed.Issues.ToString());
        const auto tables = CollectTableReferences(*parsed.Ast, {"local", "/Root", {}});
        UNIT_ASSERT_C(tables.IsOk(), tables.Issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(tables.Tables.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(tables.Tables.front().second, "/Root/t");
        parsed = ParseQueryWithNewParser("PRAGMA TablePathPrefix = $prefix; SELECT * FROM t;", {}, false);
        UNIT_ASSERT_C(parsed.IsOk(), parsed.Issues.ToString());
        UNIT_ASSERT_EXCEPTION(CollectTableReferences(*parsed.Ast, {"local", "/Root", {}}), TNewYqlParserFallback);
    }

    Y_UNIT_TEST(SetOrderingUsesCompoundOutputScope) {
        TFixture f;
        f.Ok("SELECT value AS z, id AS a FROM t UNION ALL SELECT value AS z, id AS a FROM t ORDER BY a, z;");
        const auto& outputs = f.Result.Names->QueryOutputs.at(&f.Query());
        UNIT_ASSERT_VALUES_EQUAL(f.Result.Names->Columns[outputs[0]].Name, "z");
        UNIT_ASSERT_VALUES_EQUAL(f.Result.Names->Columns[outputs[1]].Name, "a");
        UNIT_ASSERT_VALUES_EQUAL(f.Result.Names->ColumnReferences.at(f.Query().OrderBy[0].Expression.get()), outputs[1]);
        UNIT_ASSERT_VALUES_EQUAL(f.Result.Names->ColumnReferences.at(f.Query().OrderBy[1].Expression.get()), outputs[0]);
        for (const TString key : {"id", "t.id", "value"}) {
            f.Resolve("SELECT id AS n FROM t UNION ALL SELECT id AS n FROM t ORDER BY " + key + ";");
            UNIT_ASSERT_C(!f.Result.IsOk(), key);
            UNIT_ASSERT_STRING_CONTAINS(f.Result.Issues.ToString(), "Unknown column");
        }
    }

    Y_UNIT_TEST(NativeResultColumnNames) {
        TFixture f;
        f.Ok("SELECT l.id, r.id, 1 AS column2, 2 FROM t AS l JOIN u AS r ON l.id = r.id;");
        const auto& outputs = f.Result.Names->QueryOutputs.at(&f.Query());
        const TVector<TString> expected = {"l.id", "r.id", "column2", "column3"};
        for (size_t i = 0; i < expected.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(f.Result.Names->Columns[outputs[i]].Name, expected[i]);
        }
        f.Ok("SELECT 1, 2 AS column0, 3;");
        const auto& synthetic = f.Result.Names->QueryOutputs.at(&f.Query());
        UNIT_ASSERT_VALUES_EQUAL(f.Result.Names->Columns[synthetic[0]].Name, "column1");
        UNIT_ASSERT_VALUES_EQUAL(f.Result.Names->Columns[synthetic[2]].Name, "column2");
        f.Resolve("SELECT id AS x, value AS x FROM t;");
        UNIT_ASSERT(!f.Result.IsOk());
        UNIT_ASSERT_STRING_CONTAINS(f.Result.Issues.ToString(), "Duplicate output column 'x'");
    }

    // Verify schema identity and physical column provenance, not just names.
    Y_UNIT_TEST(BindsCatalogIdentityAndColumns) {
        TFixture f;
        f.Ok("SELECT t.id, value FROM t WHERE id > $min ORDER BY t.id;");
        UNIT_ASSERT_VALUES_EQUAL(f.Lookups.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Lookups[0].first, "local");
        UNIT_ASSERT_VALUES_EQUAL(f.Lookups[0].second, "/Root/t");
        const auto& column = f.Column(*f.Select().Columns[0].Expression);
        UNIT_ASSERT_VALUES_EQUAL(column.Name, "id");
        UNIT_ASSERT_VALUES_EQUAL(*column.MetadataColumnId, 1);
        UNIT_ASSERT(column.Source == f.Select().From.get());
        UNIT_ASSERT_VALUES_EQUAL(column.Table->OwnerId, 42);
        UNIT_ASSERT_VALUES_EQUAL(column.Table->SchemaVersion, 7);
        UNIT_ASSERT_VALUES_EQUAL(column.Table->Columns[0].Type, "Int64");
        UNIT_ASSERT(column.Table->Columns[0].NotNull);
        UNIT_ASSERT_VALUES_EQUAL(f.Result.Names->QueryOutputs.at(&f.Query()).size(), 2);
    }

    // Equal table/column spellings on two inputs must not share a logical ID.
    Y_UNIT_TEST(SelfJoinHasDistinctColumnBindings) {
        TFixture f;
        f.Ok("SELECT l.id, r.id AS rid FROM ANY t AS l JOIN ANY t AS r ON l.id = r.id;");
        const auto& l = f.Column(*f.Select().Columns[0].Expression);
        const auto& r = f.Column(*f.Select().Columns[1].Expression);
        UNIT_ASSERT(l.Source != r.Source);
        UNIT_ASSERT(l.Table == r.Table);
        UNIT_ASSERT_VALUES_EQUAL(l.MetadataColumnId, r.MetadataColumnId);
        const auto& join = std::get<TJoin>(f.Select().From->Value);
        UNIT_ASSERT(join.LeftAny && join.RightAny);
        const auto& on = std::get<TBinary>(join.On->Value);
        UNIT_ASSERT(f.Column(*on.Left).Source == l.Source);
        UNIT_ASSERT(f.Column(*on.Right).Source == r.Source);
    }

    // Missing names and ambiguous nearest-scope matches are user errors,
    // so neither may become a successful partial resolution or fallback.
    Y_UNIT_TEST(RejectsMissingAndAmbiguousNames) {
        for (const auto& [query, message] : TVector<std::pair<TString, TString>>{
            {"SELECT id FROM absent;", "does not exist"},
            {"SELECT absent FROM t;", "Unknown column 'absent'"},
            {"SELECT x.absent FROM t AS x;", "Unknown column 'x.absent'"},
            {"SELECT id FROM t AS l JOIN u AS r ON l.id = r.id;", "Ambiguous column 'id'"},
            {"SELECT l.id FROM t AS l JOIN u AS l ON l.id = l.id;", "Duplicate table alias 'l'"},
            {"SELECT t.id FROM t AS hidden;", "Unknown column"},
            {"SELECT id FROM t WHERE absent > 0;", "Unknown column 'absent'"},
            {"SELECT COUNT(*) FROM t GROUP BY absent;", "Unknown column 'absent'"},
            {"SELECT id FROM t ORDER BY absent;", "Unknown column 'absent'"},
            {"SELECT missing.* FROM t;", "Unknown table alias 'missing'"},
            {"SELECT l.id FROM t AS l JOIN u AS r ON l.absent = r.id;", "Unknown column 'l.absent'"},
        }) {
            TFixture f;
            f.Resolve(query);
            UNIT_ASSERT_C(!f.Result.IsOk(), query);
            UNIT_ASSERT_STRING_CONTAINS(f.Result.Issues.ToString(), message);
            UNIT_ASSERT(!f.Result.Names); // No partial binding result escapes.
        }
    }

    // Decode names before catalog lookup and use the prefix for the selected
    // cluster rather than always applying the default prefix.
    Y_UNIT_TEST(QuotedNamesAndClusterPrefixes) {
        TFixture f;
        f.AddTable("remote", "/Data/strange`table", {{"some`column", 9, "String", false}});
        f.Resolve("SELECT a.`some``column` FROM remote.`strange``table` AS a;",
            {"local", "/Root", {{"remote", "/Data"}}});
        UNIT_ASSERT_C(f.Result.IsOk(), f.Result.Issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(f.Lookups[0].second, "/Data/strange`table");
        UNIT_ASSERT_VALUES_EQUAL(f.Column(*f.Select().Columns[0].Expression).Name, "some`column");
    }

    // ANSI escaping must not erase exact-case identifier matching.
    Y_UNIT_TEST(AnsiQuotedAliasesAndExactCase) {
        TFixture f;
        f.AddTable("local", "/Root/Mixed", {{"Id", 1, "Int64", false}});
        f.Ok("--!ansi_lexer\nSELECT \"a\"\"b\".Id FROM \"Mixed\" AS \"a\"\"b\";");
        UNIT_ASSERT_VALUES_EQUAL(f.Column(*f.Select().Columns[0].Expression).Name, "Id");
        f.Resolve("SELECT id FROM Mixed;");
        UNIT_ASSERT(!f.Result.IsOk());
    }

    // Expansion order is a catalog contract, independent of hash-map iteration.
    Y_UNIT_TEST(StarUsesCatalogColumnOrder) {
        TFixture f;
        f.Ok("SELECT t.* FROM t;");
        const auto& expansion = f.Result.Names->Stars.at(f.Select().Columns[0].Expression.get());
        UNIT_ASSERT_VALUES_EQUAL(expansion.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(f.Result.Names->Columns[expansion[0]].Name, "id");
        UNIT_ASSERT_VALUES_EQUAL(f.Result.Names->Columns[expansion[1]].Name, "value");
    }

    // Renamed outputs retain Input provenance while exposing the new names.
    Y_UNIT_TEST(DerivedTablesAndProjectionAliases) {
        TFixture f;
        f.Ok("SELECT d.key FROM (SELECT id AS key FROM t) AS d ORDER BY key;");
        const auto& derived = f.Column(*f.Select().Columns[0].Expression);
        UNIT_ASSERT_VALUES_EQUAL(derived.Name, "key");
        UNIT_ASSERT(derived.Input);
        const auto& projection = f.Result.Names->Columns[*derived.Input];
        UNIT_ASSERT(projection.Input);
        UNIT_ASSERT_VALUES_EQUAL(*f.Result.Names->Columns[*projection.Input].MetadataColumnId, 1);
        f.Resolve("SELECT d.value FROM (SELECT id AS key FROM t) AS d;");
        UNIT_ASSERT(!f.Result.IsOk());
    }

    // Output aliases take precedence in ORDER BY but are unavailable in WHERE.
    Y_UNIT_TEST(OrderByAliasesDoNotLeakIntoWhere) {
        TFixture f;
        f.Ok("SELECT id + 1 AS value FROM t ORDER BY value;");
        const auto id = f.Result.Names->ColumnReferences.at(f.Select().OrderBy[0].Expression.get());
        UNIT_ASSERT_VALUES_EQUAL(id, f.Result.Names->QueryOutputs.at(&f.Query())[0]);
        f.Resolve("SELECT id AS new_name FROM t WHERE new_name > 0;");
        UNIT_ASSERT(!f.Result.IsOk());
    }

    // A CTE name is resolved lexically, while each reference gets fresh IDs.
    Y_UNIT_TEST(CteShadowsCatalogAndCopiesBindings) {
        TFixture f;
        f.Ok("WITH t(key) AS (SELECT id FROM u) SELECT l.key, r.key AS other FROM t AS l JOIN t AS r ON l.key = r.key;");
        UNIT_ASSERT_VALUES_EQUAL(f.Lookups.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Lookups[0].second, "/Root/u");
        UNIT_ASSERT(f.Column(*f.Select().Columns[0].Expression).Source != f.Column(*f.Select().Columns[1].Expression).Source);
        f.Resolve("WITH c(a,b) AS (SELECT id FROM t) SELECT a FROM c;");
        UNIT_ASSERT(!f.Result.IsOk());
    }

    // Nearest relations shadow outer ones even when correlation would otherwise
    // find a similarly named column in the parent scope.
    Y_UNIT_TEST(CorrelatedSubqueryAndLocalShadowing) {
        TFixture f;
        f.Ok("SELECT l.id FROM t AS l WHERE EXISTS (SELECT r.id FROM u AS r WHERE r.id = l.id);");
        const auto& query = *std::get<TExists>(f.Select().Where->Value).Query;
        const auto& predicate = std::get<TBinary>(std::get<TSelect>(query.Body).Where->Value);
        UNIT_ASSERT(f.Column(*predicate.Right).Source == f.Select().From.get());
        f.Resolve("SELECT l.value FROM t AS l WHERE EXISTS (SELECT l.value FROM u AS l);");
        UNIT_ASSERT(!f.Result.IsOk());
        UNIT_ASSERT_STRING_CONTAINS(f.Result.Issues.ToString(), "Unknown column 'l.value'");
    }

    // Both sides participate in ON; only the preserved side is visible afterward.
    Y_UNIT_TEST(SemiJoinExposesOnlyPreservedSide) {
        TFixture f;
        f.Ok("SELECT l.id FROM t AS l LEFT SEMI JOIN u AS r ON l.id = r.id;");
        f.Resolve("SELECT r.id FROM t AS l LEFT SEMI JOIN u AS r ON l.id = r.id;");
        UNIT_ASSERT(!f.Result.IsOk());
    }

    // Function names inhabit a different namespace from their column arguments;
    // window references also need their own binding table.
    Y_UNIT_TEST(FunctionsAndWindowsResolveTheirArguments) {
        TFixture f;
        f.Ok("SELECT SUM(id) FILTER(WHERE id > 0) OVER w, Math::Abs(id) FROM t WINDOW w AS (PARTITION BY value ORDER BY id);");
        const auto& call = std::get<TCall>(f.Select().Columns[0].Expression->Value);
        UNIT_ASSERT(f.Result.Names->WindowReferences.at(&*call.Over) == &f.Select().Windows[0]);
        f.Resolve("SELECT SUM(absent) OVER () FROM t;");
        UNIT_ASSERT(!f.Result.IsOk());
        f.Resolve("SELECT SUM(id) OVER absent FROM t;");
        UNIT_ASSERT(!f.Result.IsOk());
        f.Resolve("SELECT SUM(id) OVER (ORDER BY absent) FROM t;");
        UNIT_ASSERT(!f.Result.IsOk());
    }

    // A later path-prefix pragma must not retroactively change an earlier query.
    Y_UNIT_TEST(PrefixPragmasAreAppliedInStatementOrder) {
        TFixture f;
        f.AddTable("local", "/Other/t", {{"different", 1, "String", true}});
        f.Ok("SELECT id FROM t; PRAGMA TablePathPrefix = '/Other'; SELECT different FROM t; SELECT id FROM `/Root/t`;");
        UNIT_ASSERT_VALUES_EQUAL(f.Lookups[0].second, "/Root/t");
        UNIT_ASSERT_VALUES_EQUAL(f.Lookups[1].second, "/Other/t");
        UNIT_ASSERT_VALUES_EQUAL(f.Lookups[2].second, "/Root/t");
    }

    Y_UNIT_TEST(CostBasedOptimizationLevelPragmas) {
        TFixture f;
        f.Ok("PRAGMA ydb.Cost_Based_Optimization_Level('\\x30'); "
            "PRAGMA kikimr.CostBasedOptimizationLevel = '4'; SELECT id FROM t;");
        const auto& settings = f.Result.Names->CostBasedOptimizationPragmas;
        UNIT_ASSERT_VALUES_EQUAL(settings.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(*settings.at(&std::get<TPragmaStatement>(f.Ast->Statements[0].Value)), "0");
        UNIT_ASSERT_VALUES_EQUAL(*settings.at(&std::get<TPragmaStatement>(f.Ast->Statements[1].Value)), "4");
        for (const TString sql : {"PRAGMA ydb.CostBasedOptimizationLevel = 0; SELECT id FROM t;",
            "PRAGMA ydb.CostBasedOptimizationLevel('0', '4'); SELECT id FROM t;"}) {
            f.Resolve(sql);
            UNIT_ASSERT(!f.Result.IsOk());
            UNIT_ASSERT(!f.Result.Issues.Empty());
        }
        UNIT_ASSERT_EXCEPTION(f.Resolve("$level = '0'; PRAGMA ydb.CostBasedOptimizationLevel = $level; SELECT id FROM t;"),
            TNewYqlParserFallback);
        UNIT_ASSERT_EXCEPTION(f.Resolve("PRAGMA yt.CostBasedOptimizationLevel = '0'; SELECT id FROM t;"),
            TNewYqlParserFallback);
    }

    // Diagnostics should retain SQL file/line/column information after CST release.
    Y_UNIT_TEST(ErrorsHaveSourcePositions) {
        TFixture f;
        f.Resolve("SELECT\n  missing FROM t;");
        UNIT_ASSERT(!f.Result.IsOk());
        const auto& issue = *f.Result.Issues.begin();
        UNIT_ASSERT_VALUES_EQUAL(issue.Position.Row, 2);
        UNIT_ASSERT_VALUES_EQUAL(issue.Position.Column, 3);
    }

    // Representable syntax with unimplemented binding rules requests fallback
    // without publishing bindings accumulated before the unsupported construct.
    Y_UNIT_TEST(UnsupportedBindingDoesNotReturnPartialNames) {
        for (const TString query : {
            "SELECT * FROM t JOIN u USING(id);",
            "SELECT * FROM t WITH SCHEMA (id String);",
            "SELECT value.field FROM t;",
            "PRAGMA AnsiQuotedIdentifiers; SELECT id FROM t;",
            "PRAGMA AnsiImplicitCrossJoin = 'false'; SELECT id FROM t;",
        }) {
            TFixture f;
            UNIT_ASSERT_EXCEPTION(f.Resolve(query), TNewYqlParserFallback);
        }
    }

    // A closure captures the preceding definition, not whichever definition
    // happens to have the same spelling when the closure is later applied.
    Y_UNIT_TEST(PreambleCapturesAndLambdaShadowing) {
        TFixture f;
        f.Ok("$n = 1; $f = ($x) -> { $n = $n + $x; return $n; }; "
            "$n = 100; SELECT $f(id), $n FROM t;");
        const auto& n = std::get<TNamedBinding>(f.Ast->Statements[0].Value);
        const auto& fn = std::get<TNamedBinding>(f.Ast->Statements[1].Value);
        const auto& lambda = std::get<TLambda>(fn.Value->Value);
        const auto& local = lambda.Bindings[0];
        const auto& sum = std::get<TBinary>(local.Value->Value);
        const auto& references = f.Result.Names->BindingReferences;
        UNIT_ASSERT_EQUAL(references.at(sum.Left.get()), &n.Name);
        UNIT_ASSERT_EQUAL(references.at(sum.Right.get()), &lambda.Parameters[0]);
        UNIT_ASSERT_EQUAL(references.at(lambda.Body.get()), &local.Name);
        const auto& projection = f.Select(3).Columns;
        UNIT_ASSERT_EQUAL(references.at(std::get<TCall>(projection[0].Expression->Value).Callable.get()), &fn.Name);
        UNIT_ASSERT_EQUAL(references.at(projection[1].Expression.get()),
            &std::get<TNamedBinding>(f.Ast->Statements[2].Value).Name);
    }

    Y_UNIT_TEST(NamedQueriesBindOnceAndExposeIndependentColumns) {
        TFixture f;
        f.Ok("$q = (SELECT id FROM t); $alias = $q; "
            "SELECT l.id, r.id FROM $q AS l JOIN $alias AS r ON l.id = r.id;");
        UNIT_ASSERT_VALUES_EQUAL(f.Lookups.size(), 1);
        const auto& join = std::get<TJoin>(f.Select(2).From->Value);
        const auto& names = *f.Result.Names;
        UNIT_ASSERT_EQUAL(names.NamedSources.at(join.Left.get()), names.NamedSources.at(join.Right.get()));
        UNIT_ASSERT(names.ColumnReferences.at(f.Select(2).Columns[0].Expression.get())
            != names.ColumnReferences.at(f.Select(2).Columns[1].Expression.get()));
    }

    Y_UNIT_TEST(PreambleBindingErrorsHaveNoPartialResult) {
        for (const TString sql : {"$f = ($x, $x) -> ($x); SELECT $f(1, 2);",
            "$q = (SELECT id FROM $later); $later = (SELECT id FROM t); SELECT * FROM $q;"}) {
            TFixture f;
            f.Resolve(sql);
            UNIT_ASSERT(!f.Result.IsOk());
            UNIT_ASSERT(!f.Result.Issues.Empty());
        }
    }

}

} // namespace NKikimr::NKqp
