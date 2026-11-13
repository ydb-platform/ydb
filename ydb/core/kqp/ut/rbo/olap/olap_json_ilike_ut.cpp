#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/ut/olap/helpers/local.h>
#include <ydb/core/kqp/ut/olap/helpers/test_case.h>
#include <ydb/core/tx/columnshard/engines/scheme/objects_cache.h>
#include <ydb/public/lib/yson_value/ydb_yson_value.h>

#include <yql/essentials/ast/yql_ast.h>

#include <util/string/join.h>

#include <algorithm>
#include <cctype>

namespace NKikimr::NKqp {

using namespace NYdb;

namespace {

// The OLAP program of the first column read of a query AST: its KqpOlapFilter, or with `withProjections` its KqpOlapProjections
// when it has them, with the let bindings inlined, atoms printed by their content and the arguments named by their order,
// so that both optimizers print the same program the same way.
class TOlapProgramPrinter {
public:
    explicit TOlapProgramPrinter(const TString& ast)
        : Ast(NYql::ParseAst(ast))
    {
        UNIT_ASSERT_C(Ast.IsOk(), ast);
        CollectLets(*Ast.Root);
    }

    TString Print(bool withProjections) {
        const auto* program = withProjections ? FindCallable(*Ast.Root, "KqpOlapProjections") : nullptr;
        if (!program) {
            program = FindCallable(*Ast.Root, "KqpOlapFilter");
        }
        if (!program) {
            return {};
        }
        TStringBuilder out;
        Print(*program, out);
        return out;
    }

    // The columns the first column read fetches, and those its program returns to compute.
    TString PrintReadColumns() const {
        const auto* read = FindCallable(*Ast.Root, "KqpBlockReadOlapTableRanges");
        if (!read) {
            read = FindCallable(*Ast.Root, "KqpReadOlapTableRanges");
        }
        if (!read) {
            return {};
        }
        // (KqpBlockReadOlapTableRanges table ranges columns settings explainPrompt process)
        const auto columns = PrintColumns(*read->GetChild(3));
        const auto* extractMembers = FindInlined(*read->GetChild(6), "TKqpOlapExtractMembers");
        return TStringBuilder() << "read " << columns << ", output " << (extractMembers ? PrintColumns(*extractMembers->GetChild(2)) : columns);
    }

private:
    static bool IsAtom(const NYql::TAstNode& node, TStringBuf content) {
        return node.IsAtom() && node.GetContent() == content;
    }

    // A bare $N atom names a let binding or a lambda argument. A quoted atom, such as a JSON path, is a value.
    static bool IsReference(const NYql::TAstNode& node) {
        const auto content = node.GetContent();
        return node.GetFlags() == NYql::TNodeFlags::Default && content.size() > 1 && content[0] == '$' &&
            std::all_of(content.begin() + 1, content.end(), [](char c) { return std::isdigit(static_cast<unsigned char>(c)); });
    }

    void CollectLets(const NYql::TAstNode& node) {
        if (!node.IsList()) {
            return;
        }
        if (node.GetChildrenCount() == 3 && IsAtom(*node.GetChild(0), "let") && node.GetChild(1)->IsAtom()) {
            Lets[node.GetChild(1)->GetContent()] = node.GetChild(2);
        }
        for (ui32 i = 0; i < node.GetChildrenCount(); ++i) {
            CollectLets(*node.GetChild(i));
        }
    }

    static const NYql::TAstNode* FindCallable(const NYql::TAstNode& node, TStringBuf name) {
        if (!node.IsList()) {
            return nullptr;
        }
        if (node.GetChildrenCount() > 0 && IsAtom(*node.GetChild(0), name)) {
            return &node;
        }
        for (ui32 i = 0; i < node.GetChildrenCount(); ++i) {
            if (const auto* found = FindCallable(*node.GetChild(i), name)) {
                return found;
            }
        }
        return nullptr;
    }

    // `node` without its quotes, with a let reference replaced by its binding.
    const NYql::TAstNode& Unwrap(const NYql::TAstNode& node) const {
        if (const auto* let = node.IsAtom() && IsReference(node) ? Lets.FindPtr(node.GetContent()) : nullptr) {
            return Unwrap(**let);
        }
        if (node.IsList() && node.GetChildrenCount() == 2 && IsAtom(*node.GetChild(0), "quote")) {
            return Unwrap(*node.GetChild(1));
        }
        return node;
    }

    // The first `name` callable in `node`, following its let references.
    const NYql::TAstNode* FindInlined(const NYql::TAstNode& node, TStringBuf name) const {
        const auto& current = Unwrap(node);
        if (!current.IsList()) {
            return nullptr;
        }
        if (current.GetChildrenCount() > 0 && IsAtom(*current.GetChild(0), name)) {
            return &current;
        }
        for (ui32 i = 0; i < current.GetChildrenCount(); ++i) {
            if (const auto* found = FindInlined(*current.GetChild(i), name)) {
                return found;
            }
        }
        return nullptr;
    }

    // The columns of a quoted list, sorted, as the optimizers list them in different orders.
    TString PrintColumns(const NYql::TAstNode& node) const {
        const auto& list = Unwrap(node);
        std::vector<TString> columns;
        for (ui32 i = 0; i < list.GetChildrenCount(); ++i) {
            columns.emplace_back(Unwrap(*list.GetChild(i)).GetContent());
        }
        std::sort(columns.begin(), columns.end());
        return TStringBuilder() << "(" << JoinSeq(" ", columns) << ")";
    }

    TString ArgumentName(TStringBuf argument) {
        return TStringBuilder() << "$" << Arguments.emplace(argument, Arguments.size()).first->second;
    }

    void Print(const NYql::TAstNode& node, TStringBuilder& out) {
        if (node.IsAtom()) {
            if (!IsReference(node)) {
                out << node.GetContent();
            } else if (const auto* let = Lets.FindPtr(node.GetContent())) {
                Print(**let, out);
            } else {
                out << ArgumentName(node.GetContent());
            }
            return;
        }
        // 'x is (quote x).
        if (node.GetChildrenCount() == 2 && IsAtom(*node.GetChild(0), "quote")) {
            Print(*node.GetChild(1), out);
            return;
        }
        // Both optimizers drop the columns compute does not need from the program output: the old one before the
        // projections, the new one after them. PrintReadColumns compares the columns, so the program skips it.
        if (node.GetChildrenCount() == 3 && (IsAtom(*node.GetChild(0), "TKqpOlapExtractMembers") || IsAtom(*node.GetChild(0), "KqpOlapExtractMembers"))) {
            Print(*node.GetChild(1), out);
            return;
        }
        // (block '((let ...) ... (return x))) is x, its lets are inlined.
        if (node.GetChildrenCount() == 2 && IsAtom(*node.GetChild(0), "block")) {
            const auto* body = node.GetChild(1);
            body = body->IsList() && body->GetChildrenCount() == 2 && IsAtom(*body->GetChild(0), "quote") ? body->GetChild(1) : body;
            for (ui32 i = 0; i < body->GetChildrenCount(); ++i) {
                const auto* item = body->GetChild(i);
                if (item->IsList() && item->GetChildrenCount() == 2 && IsAtom(*item->GetChild(0), "return")) {
                    Print(*item->GetChild(1), out);
                    return;
                }
            }
        }
        out << "(";
        for (ui32 i = 0; i < node.GetChildrenCount(); ++i) {
            out << (i ? " " : "");
            Print(*node.GetChild(i), out);
        }
        out << ")";
    }

    NYql::TAstParseResult Ast;
    THashMap<TStringBuf, const NYql::TAstNode*> Lets;
    THashMap<TStringBuf, ui32> Arguments;
};

// The rows of a result, each with its columns by name, in a fixed order, so that a different column order of SELECT * does not matter.
std::vector<TString> CanonicalRows(const TResultSet& resultSet) {
    std::vector<TString> rows;
    TResultSetParser parser(resultSet);
    while (parser.TryNextRow()) {
        std::vector<TString> columns;
        for (size_t i = 0; i < resultSet.ColumnsCount(); ++i) {
            columns.push_back(TStringBuilder() << resultSet.GetColumnsMeta()[i].Name << "=" << FormatValueYson(parser.GetValue(i)));
        }
        std::sort(columns.begin(), columns.end());
        rows.push_back(JoinSeq(";", columns));
    }
    std::sort(rows.begin(), rows.end());
    return rows;
}

struct TQueryOutcome {
    TString Program;
    TString ReadColumns;
    std::vector<TString> Rows;
};

std::vector<TQueryOutcome> RunQueries(bool newRbo, const std::function<void(TKikimrRunner&)>& prepare, const std::vector<TString>& queries,
                                      bool withProjections) {
    // Column-shard schema caches are process-wide and keyed by path ID, which every new runner reuses.
    NOlap::TSchemaCachesManager::DropCaches();
    auto settings = TKikimrSettings().SetColumnShardAlterObjectEnabled(true).SetWithSampleTables(false);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableNewRBO(newRbo);
    // A query the new optimizer cannot compile must fail, not run with the old one.
    settings.AppConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
    TKikimrRunner kikimr(settings);
    prepare(kikimr);

    std::vector<TQueryOutcome> outcomes;
    auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
    for (const auto& query : queries) {
        auto result = session.ExecuteQuery(query, NQuery::TTxControl::NoTx(), NQuery::TExecuteQuerySettings().StatsMode(NQuery::EStatsMode::Full))
            .ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), (newRbo ? "new" : "old") << " optimizer, " << query << ": " << result.GetIssues().ToString());
        TOlapProgramPrinter printer(TString{*result.GetStats()->GetAst()});
        outcomes.push_back({printer.Print(withProjections), printer.PrintReadColumns(), CanonicalRows(result.GetResultSet(0))});
    }
    return outcomes;
}

// The new optimizer gives the column shard the same program as the old one, the read fetches and returns the same
// columns, and the query returns the same rows. The projections are compared only with `withProjections`: the new
// optimizer pushes some projections the old one does not.
void CheckSameAsOldOptimizer(const std::function<void(TKikimrRunner&)>& prepare, const std::vector<TString>& queries,
                             bool withProjections = false) {
    const auto oldOutcomes = RunQueries(false, prepare, queries, withProjections);
    const auto newOutcomes = RunQueries(true, prepare, queries, withProjections);
    for (size_t i = 0; i < queries.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL_C(newOutcomes[i].Program, oldOutcomes[i].Program, queries[i]);
        UNIT_ASSERT_VALUES_EQUAL_C(newOutcomes[i].ReadColumns, oldOutcomes[i].ReadColumns, queries[i]);
        UNIT_ASSERT_VALUES_EQUAL_C(JoinSeq("\n", newOutcomes[i].Rows), JoinSeq("\n", oldOutcomes[i].Rows), queries[i]);
    }
}

void ExecuteSchemeQueries(TKikimrRunner& kikimr, const std::vector<TString>& queries) {
    auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
    for (const auto& query : queries) {
        const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), query << ": " << result.GetIssues().ToString());
    }
}

void BulkUpsertJson(TKikimrRunner& kikimr, const std::vector<std::pair<ui64, std::optional<std::string>>>& rows) {
    TValueBuilder builder;
    builder.BeginList();
    for (const auto& [key, json] : rows) {
        builder.AddListItem().BeginStruct().AddMember("Col1").Uint64(key).AddMember("Col2").OptionalJsonDocument(json).EndStruct();
    }
    builder.EndList();
    const auto result = kikimr.GetTableClient().BulkUpsert("/Root/ColumnTable", builder.Build()).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

const TString CreateColumnTable = R"(
    CREATE TABLE `/Root/ColumnTable` (
        Col1 Uint64 NOT NULL,
        Col2 JsonDocument,
        PRIMARY KEY (Col1)
    )
    PARTITION BY HASH(Col1)
    WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2);
)";

const TString UseSubColumns = R"(
    ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`,
        `DATA_EXTRACTOR_CLASS_NAME`=`JSON_SCANNER`)
)";

} // namespace

// Ported from ut/olap/types/json_ut.cpp and ut/olap/kqp_olap_ut.cpp: the selects of the JSON and simple ILIKE tests.
// Their scripts write data with REPLACE INTO ... VALUES, which the new optimizer does not support, so instead of running
// them the tests write the same data with bulk upserts and check each select against the old optimizer.
Y_UNIT_TEST_SUITE(KqpRboOlapCompat) {

    // ComplexPathVariants.
    Y_UNIT_TEST(JsonPathsSameAsOldOptimizer) {
        CheckSameAsOldOptimizer([](TKikimrRunner& kikimr) {
            ExecuteSchemeQueries(kikimr, {CreateColumnTable, UseSubColumns});
            BulkUpsertJson(kikimr, {{1, R"({"a":{"b":"nested"},"a.b":"flat","a[0]":{"b":"literal"},"a":[{"b":"array"}],"":{"x":"empty"},)"
                                        R"("q\"u":{"b":"quote"},"slash/key":{"b":"slash"},"?":{"x":"question"}})"}});
            BulkUpsertJson(kikimr, {{1, R"({"a":{"b":"merged_nested"},"a.b":"merged_flat","a[0]":{"b":"merged_literal"},)"
                                        R"("a":[{"b":"merged_array"}],"":{"x":"merged_empty"},"q\"u":{"b":"merged_quote"},)"
                                        R"("slash\/key":{"b":"merged_slash"},"?":{"x":"merged_question"}})"}});
        }, {
            R"q(SELECT Col1 FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.a.b") = "merged_nested")q",
            R"q(SELECT Col1 FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"a\".\"b\"") = "merged_nested")q",
            R"q(SELECT Col1 FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.'a'.'b'") = "merged_nested")q",
            R"q(SELECT Col1 FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"a.b\"") = "merged_flat")q",
            R"q(SELECT Col1 FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"a[0]\".b") = "merged_literal")q",
            R"q(SELECT Col1 FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.a[0].b") = "merged_array")q",
            R"q(SELECT Col1 FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"\".x") = "merged_empty")q",
            R"q(SELECT Col1 FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, @@$."q\"u".b@@) = "merged_quote")q",
            R"q(SELECT Col1 FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, @@$."slash/key".b@@) = "merged_slash")q",
            R"q(SELECT Col1 FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"?\".x") = "merged_question")q",
        });
    }

    // BloomMixIndexesVariants, BloomCategoryIndexesVariants and BloomNGrammIndexesVariants.
    Y_UNIT_TEST(JsonIndexesSameAsOldOptimizer) {
        CheckSameAsOldOptimizer([](TKikimrRunner& kikimr) {
            ExecuteSchemeQueries(kikimr, {CreateColumnTable, UseSubColumns});
            BulkUpsertJson(kikimr, {{1, R"({"a.b.c" : "a1"})"}, {2, R"({"a.b.c" : "a2"})"}, {3, R"({"b.c.d" : "b3"})"},
                                    {4, R"({"b.c.d" : "b4", "a" : "a4"})"}});
            BulkUpsertJson(kikimr, {{11, R"({"a.b.c" : "1a1"})"}, {12, R"({"a.b.c" : "1a2"})"}, {13, R"({"b.c.d" : "1b3"})"},
                                    {14, R"({"b.c.d" : "1b4", "a" : "a4"})"}});
            ExecuteSchemeQueries(kikimr, {
                R"(ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=a_index, TYPE=CATEGORY_BLOOM_FILTER,
                    FEATURES=`{"column_name" : "Col2", "false_positive_probability" : 0.01}`))",
                R"(ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=index_ngramm_b, TYPE=BLOOM_NGRAMM_FILTER,
                    FEATURES=`{"column_name" : "Col2", "ngramm_size" : 3, "hashes_count" : 2, "filter_size_bytes" : 4096,
                        "records_count" : 1024, "case_sensitive" : false, "data_extractor" : {"class_name" : "SUB_COLUMN", "sub_column_name" : '"b.c.d"'}}`))",
                R"(ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=index_ngramm_a, TYPE=BLOOM_NGRAMM_FILTER,
                    FEATURES=`{"column_name" : "Col2", "ngramm_size" : 3, "hashes_count" : 2, "filter_size_bytes" : 4096,
                        "records_count" : 1024, "case_sensitive" : true, "data_extractor" : {"class_name" : "SUB_COLUMN", "sub_column_name" : '"a"'}}`))",
            });
            BulkUpsertJson(kikimr, {{10, std::nullopt}});
            ExecuteSchemeQueries(kikimr, {
                R"(ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=b_index, TYPE=CATEGORY_BLOOM_FILTER,
                    FEATURES=`{"column_name" : "Col2", "false_positive_probability" : 0.01}`))",
            });
        }, {
            R"q(SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"a.b.c\"") = "a1" ORDER BY Col1)q",
            R"q(SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"a.b.c\"") = "a1" AND JSON_VALUE(Col2, "$.\"a\"") = "a4" ORDER BY Col1)q",
            R"q(SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"b.c.d\"") = "1b4" AND JSON_VALUE(Col2, "$.\"a\"") = "a4" ORDER BY Col1)q",
            R"q(SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"a.b.c\"") = "1a1" ORDER BY Col1)q",
            R"q(SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"b.c.d\"") = "1b4" ORDER BY Col1)q",
            R"q(SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"b.c.d\"") like "%1b4%" ORDER BY Col1)q",
            R"q(SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"b.c.d\"") ilike "%1b4%" ORDER BY Col1)q",
            R"q(SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"b.c.d\"") ilike "%1B4" ORDER BY Col1)q",
            R"q(SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"b.c.d\"") ilike "1b5" ORDER BY Col1)q",
            R"q(SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.a") = "1b5" ORDER BY Col1)q",
            R"q(SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.a") = "a4" ORDER BY Col1)q",
            R"q(SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"a\"") = "a4" ORDER BY Col1)q",
            R"q(SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"b.c.d111\"") = "1b5" ORDER BY Col1)q",
            R"q(SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"b.c.d\"") like "1b3" ORDER BY Col1)q",
            R"q(SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"b.c.d\"") like "1b5" ORDER BY Col1)q",
            R"q(SELECT * FROM `/Root/ColumnTable` ORDER BY Col1)q",
            R"q(SELECT * FROM `/Root/ColumnTable` WHERE JSON_VALUE(Col2, "$.\"b.c.d\"") like "1B3" ORDER BY Col1)q",
        });
    }

    // Json_GetValue*, JsonDoc_GetValue*, Json_Exists, JsonDoc_Exists, Json_Query, MixedJsonAndOlapApply,
    // JsonValuePushedIntoOlapApply, JsonValueUsedBySeveralPushedPredicates, JsonValueWithReturningTypeIsNotPushedAsOlapJsonValue
    // and JsonValueExternalArgsDoNotWidenPushdown.
    Y_UNIT_TEST(JsonFunctionsSameAsOldOptimizer) {
        CheckSameAsOldOptimizer([](TKikimrRunner& kikimr) {
            TTableWithNullsHelper(kikimr).CreateTableWithNulls();
            WriteTestDataForTableWithNulls(kikimr, "/Root/tableWithNulls");
        }, {
            R"q(SELECT id, JSON_VALUE(jsonval, "$.col1"), JSON_VALUE(jsondoc, "$.col1") FROM `/Root/tableWithNulls` WHERE JSON_VALUE(jsonval, "$.col1") = "val1" AND id = 1)q",
            R"q(SELECT id, JSON_VALUE(jsonval, "$.'col-abc'"), JSON_VALUE(jsondoc, "$.'col-abc'") FROM `/Root/tableWithNulls` WHERE JSON_VALUE(jsonval, "$.'col-abc'") = "val-abc" AND id = 1)q",
            R"q(SELECT id, JSON_VALUE(jsonval, "$.col1" RETURNING String), JSON_VALUE(jsondoc, "$.col1") FROM `/Root/tableWithNulls` WHERE JSON_VALUE(jsonval, "$.col1" RETURNING String) = "val1" AND id = 1)q",
            R"q(SELECT id, JSON_VALUE(jsonval, "$.obj.obj_col2_int" RETURNING Int), JSON_VALUE(jsondoc, "$.obj.obj_col2_int" RETURNING Int) FROM `/Root/tableWithNulls` WHERE JSON_VALUE(jsonval, "$.obj.obj_col2_int" RETURNING Int) = 16 AND id = 1)q",
            R"q(SELECT id, JSON_VALUE(jsonval, "$.col1"), JSON_VALUE(jsondoc, "$.col1") FROM `/Root/tableWithNulls` WHERE JSON_VALUE(jsondoc, "$.col1") = "val1" AND id = 6)q",
            R"q(SELECT id, JSON_VALUE(jsonval, "$.col1"), JSON_VALUE(jsondoc, "$.col1" RETURNING String) FROM `/Root/tableWithNulls` WHERE JSON_VALUE(jsondoc, "$.col1" RETURNING String) = "val1" AND id = 6)q",
            R"q(SELECT id, JSON_VALUE(jsonval, "$.obj.obj_col2_int"), JSON_VALUE(jsondoc, "$.obj.obj_col2_int" RETURNING Int) FROM `/Root/tableWithNulls` WHERE JSON_VALUE(jsondoc, "$.obj.obj_col2_int" RETURNING Int) = 16 AND id = 6)q",
            R"q(SELECT id, JSON_EXISTS(jsonval, "$.col1"), JSON_EXISTS(jsondoc, "$.col1") FROM `/Root/tableWithNulls` WHERE JSON_EXISTS(jsonval, "$.col1") AND level = 1)q",
            R"q(SELECT id, JSON_EXISTS(jsonval, "$.col1"), JSON_EXISTS(jsondoc, "$.col1") FROM `/Root/tableWithNulls` WHERE JSON_EXISTS(jsondoc, "$.col1") AND id = 6)q",
            R"q(SELECT id, JSON_QUERY(jsonval, "$.col1" WITH UNCONDITIONAL WRAPPER), JSON_QUERY(jsondoc, "$.col1" WITH UNCONDITIONAL WRAPPER) FROM `/Root/tableWithNulls` WHERE level = 1)q",
            R"q(SELECT id, JSON_VALUE(jsonval, "$.\"col-abc\"") FROM `/Root/tableWithNulls` WHERE id = 1 AND JSON_VALUE(jsonval, "$.col1") = "val1" AND JSON_VALUE(jsonval, "$.\"col-abc\"") ilike "%A%b%" AND JSON_EXISTS(jsonval, "$.obj.obj_col2_int"))q",
            R"q(SELECT id FROM `/Root/tableWithNulls` WHERE JSON_VALUE(jsonval, "$.\"col-abc\"") ILIKE "%A%b%" ORDER BY id)q",
            R"q(SELECT id FROM `/Root/tableWithNulls` WHERE JSON_VALUE(jsondoc, "$.\"col-abc\"") ILIKE "%A%b%" ORDER BY id)q",
            R"q(SELECT id FROM `/Root/tableWithNulls` WHERE CAST(JSON_VALUE(jsonval, "$.obj.obj_col2_int") AS Int32) + 1 > 16 AND id < 3 ORDER BY id)q",
            R"q(SELECT id FROM `/Root/tableWithNulls` WHERE JSON_VALUE(jsonval, "$.col1") || JSON_VALUE(jsonval, "$.\"col-abc\"") LIKE "val1%abc" AND id > 3 ORDER BY id)q",
            R"q(SELECT id FROM `/Root/tableWithNulls` WHERE COALESCE(JSON_VALUE(jsonval, "$.missing"), "dflt") = "dflt" ORDER BY id)q",
            R"q(SELECT id FROM `/Root/tableWithNulls` WHERE IF(JSON_VALUE(jsonval, "$.col1") = "val1", 1, 0) = 1 ORDER BY id)q",
            R"q(SELECT id FROM `/Root/tableWithNulls` WHERE JSON_VALUE(jsonval, "$.col1") || JSON_VALUE(jsonval, "$.\"col-abc\"") = "val1val-abc" ORDER BY id)q",
            R"q(SELECT id FROM `/Root/tableWithNulls` WHERE JSON_VALUE(jsonval, "$.col1") ILIKE "%V%1%" AND JSON_VALUE(jsonval, "$.col1") ILIKE "%a%l%" ORDER BY id)q",
            R"q(SELECT id FROM `/Root/tableWithNulls` WHERE JSON_VALUE(jsonval, "$.col1") ILIKE "%V%1%" AND CAST(JSON_VALUE(jsonval, "$.col1") AS String) ILIKE "%a%l%" AND JSON_VALUE(jsonval, "$.\"col-abc\"") ILIKE "%A%b%" ORDER BY id)q",
            R"q(SELECT id FROM `/Root/tableWithNulls` WHERE JSON_VALUE(jsonval, "$.col1") = "val1" AND JSON_VALUE(jsonval, "$.col1") ILIKE "%a%l%" ORDER BY id)q",
            R"q(SELECT id FROM `/Root/tableWithNulls` WHERE JSON_VALUE(jsonval, "$.col1") = "val1" OR JSON_VALUE(jsonval, "$.col1") = "val2" ORDER BY id)q",
            R"q(SELECT id FROM `/Root/tableWithNulls` WHERE JSON_VALUE(jsonval, "$.col1") = JSON_VALUE(jsonval, "$.col1") ORDER BY id)q",
            R"q(SELECT id FROM `/Root/tableWithNulls` WHERE JSON_VALUE(jsonval, "$.col1") || JSON_VALUE(jsonval, "$.col1") ILIKE "VAL1val1" ORDER BY id)q",
            R"q(SELECT id FROM `/Root/tableWithNulls` WHERE JSON_VALUE(jsonval, "$.obj.obj_col2_int" RETURNING String) ILIKE "%1%6%" ORDER BY id)q",
            R"q(SELECT id FROM `/Root/tableWithNulls` WHERE JSON_VALUE(jsonval, "$.obj.obj_col2_int" RETURNING Utf8) ILIKE "%1%6%" ORDER BY id)q",
            R"q(SELECT id FROM `/Root/tableWithNulls` WHERE CAST(JSON_VALUE(jsonval, "$.obj.obj_col2_int" RETURNING Date) AS Utf8) LIKE "1970%17" ORDER BY id)q",
            R"q(SELECT id FROM `/Root/tableWithNulls` WHERE CAST(JSON_VALUE(jsonval, "$.obj.obj_col2_int" RETURNING Int32) AS Utf8) ILIKE "%1%6%" ORDER BY id)q",
            R"q(SELECT id FROM `/Root/tableWithNulls` WHERE JSON_VALUE(jsonval, "$.obj.obj_col2_int" RETURNING Utf8) LIKE "%16%" ORDER BY id)q",
            R"q(SELECT id FROM `/Root/tableWithNulls` WHERE JSON_VALUE(jsondoc, "$.obj.obj_col2_int" RETURNING String) = "16" ORDER BY id)q",
            R"q(SELECT id FROM `/Root/tableWithNulls` WHERE JSON_VALUE(jsonval, "$.obj.obj_col2_int" RETURNING Int32) = 16 ORDER BY id)q",
            R"q(SELECT id FROM `/Root/tableWithNulls` WHERE JSON_VALUE(jsonval, "$.missing" DEFAULT "dflt" ON EMPTY) = "dflt" AND id <= 5 ORDER BY id)q",
            R"q(SELECT id FROM `/Root/tableWithNulls` WHERE JSON_VALUE(jsonval, "$.col1" PASSING "unused"u AS v) = "val1" ORDER BY id)q",
            R"q(SELECT JSON_VALUE(jsonval, "$.obj.obj_col2_int" RETURNING Utf8) FROM `/Root/tableWithNulls` WHERE id = 1)q",
            R"q(SELECT id FROM `/Root/tableWithNulls` WHERE JSON_VALUE(jsonval, "$.obj" DEFAULT "none" ON ERROR) = "none" AND id <= 5 ORDER BY id)q",
            R"q(SELECT id FROM `/Root/tableWithNulls` WHERE JSON_VALUE(jsonval, "$.col1") IS NULL AND id < 8 ORDER BY id)q",
            R"q(SELECT id FROM `/Root/tableWithNulls` WHERE JSON_VALUE(jsondoc, "$.col1") IS NOT NULL ORDER BY id)q",
            R"q(SELECT id FROM `/Root/tableWithNulls` WHERE JSON_VALUE(jsonval, "$.col1") IN ("val1", "val2") ORDER BY id)q",
        });
    }

    // JSON_VALUE projections over a filter the column shard cannot run, which reads other columns.
    // Not ported: a filter which reads the JSON column itself. The old optimizer then pushes the projection
    // as a new column named by its alias, while the new optimizer only pushes projections in place of their source column.
    Y_UNIT_TEST(JsonProjectionsOverRemainingFilterSameAsOldOptimizer) {
        CheckSameAsOldOptimizer([](TKikimrRunner& kikimr) {
            TTableWithNullsHelper(kikimr).CreateTableWithNulls();
            WriteTestDataForTableWithNulls(kikimr, "/Root/tableWithNulls");
        }, {
            R"q(SELECT id, JSON_VALUE(jsonval, "$.col1") AS x FROM `/Root/tableWithNulls` WHERE resource_id || "x" != "x" ORDER BY id)q",
            R"q(SELECT id, JSON_VALUE(jsondoc, "$.col1") AS x FROM `/Root/tableWithNulls` WHERE resource_id || "x" != "x" ORDER BY id)q",
            R"q(SELECT id, JSON_VALUE(jsonval, "$.col1") AS x FROM `/Root/tableWithNulls` WHERE level = 1 AND resource_id || "x" != "x" ORDER BY id)q",
            R"q(SELECT id, JSON_VALUE(jsonval, "$.col1") AS a, JSON_VALUE(jsondoc, "$.col1") AS b FROM `/Root/tableWithNulls`
                WHERE resource_id || "x" != "x" ORDER BY id)q",
            R"q(SELECT id, JSON_VALUE(jsondoc, "$.obj.obj_col2_int") || "!" AS x FROM `/Root/tableWithNulls` WHERE resource_id || "x" != "x" ORDER BY id)q",
            R"q(SELECT id, JSON_VALUE(jsonval, "$.col1") AS x FROM `/Root/tableWithNulls` WHERE resource_id || "x" != "x" OR level = 2 ORDER BY id)q",
        }, /*withProjections=*/true);
    }

    // PredicatePushdown_FastAsciiContainsIgnoreCase, PredicatePushdown_IgnoreCaseUdfDifferentStringTypes and
    // PredicatePushdown_IgnoreCaseUdfUsesIndex.
    Y_UNIT_TEST(SimpleILikeSameAsOldOptimizer) {
        CheckSameAsOldOptimizer([](TKikimrRunner& kikimr) {
            ExecuteSchemeQueries(kikimr, {R"(
                CREATE TABLE `/Root/foo` (
                    id Int64 NOT NULL,
                    str String,
                    u_str Utf8,
                    PRIMARY KEY (id),
                    INDEX idx_u_str LOCAL USING bloom_ngram_filter ON (u_str)
                        WITH (ngram_size = 3, false_positive_probability = 0.01, case_sensitive = false)
                )
                PARTITION BY HASH(id)
                WITH (STORE = COLUMN, PARTITION_COUNT = 1);
            )"});
            TValueBuilder rows;
            rows.BeginList();
            for (const auto& [id, value] : std::vector<std::pair<i64, std::optional<std::string>>>{
                     {1, "foobar"}, {2, "baz"}, {3, "fooqux"}, {4, std::nullopt}}) {
                rows.AddListItem().BeginStruct().AddMember("id").Int64(id).AddMember("str").OptionalString(value)
                    .AddMember("u_str").OptionalUtf8(value).EndStruct();
            }
            rows.EndList();
            const auto result = kikimr.GetTableClient().BulkUpsert("/Root/foo", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }, {
            R"q(PRAGMA OptimizeSimpleILike; PRAGMA AnsiLike; SELECT id FROM `/Root/foo` WHERE str ILIKE '%foo%' ORDER BY id)q",
            R"q(PRAGMA OptimizeSimpleILike; PRAGMA AnsiLike; SELECT id FROM `/Root/foo` WHERE str ILIKE '%FOO%' ORDER BY id)q",
            R"q(PRAGMA OptimizeSimpleILike; PRAGMA AnsiLike; SELECT id FROM `/Root/foo` WHERE str ILIKE 'foo%' ORDER BY id)q",
            R"q(PRAGMA OptimizeSimpleILike; PRAGMA AnsiLike; SELECT id FROM `/Root/foo` WHERE str ILIKE '%QUX' ORDER BY id)q",
            R"q(PRAGMA OptimizeSimpleILike; PRAGMA AnsiLike; SELECT id FROM `/Root/foo` WHERE str ILIKE 'BAZ' ORDER BY id)q",
            R"q(PRAGMA OptimizeSimpleILike; PRAGMA AnsiLike; SELECT id FROM `/Root/foo` WHERE str ILIKE '%nomatch%' ORDER BY id)q",
            R"q(PRAGMA OptimizeSimpleILike; PRAGMA AnsiLike; SELECT id FROM `/Root/foo` WHERE str NOT ILIKE '%foo%' ORDER BY id)q",
            R"q(PRAGMA OptimizeSimpleILike; PRAGMA AnsiLike; SELECT id FROM `/Root/foo` WHERE u_str ILIKE '%foo%' ORDER BY id)q",
            R"q(PRAGMA OptimizeSimpleILike; PRAGMA AnsiLike; SELECT id FROM `/Root/foo` WHERE u_str ILIKE '%FOO%' ORDER BY id)q",
            R"q(PRAGMA OptimizeSimpleILike; PRAGMA AnsiLike; SELECT id FROM `/Root/foo` WHERE u_str ILIKE 'foo%' ORDER BY id)q",
            R"q(PRAGMA OptimizeSimpleILike; PRAGMA AnsiLike; SELECT id FROM `/Root/foo` WHERE u_str ILIKE '%QUX' ORDER BY id)q",
            R"q(PRAGMA OptimizeSimpleILike; PRAGMA AnsiLike; SELECT id FROM `/Root/foo` WHERE u_str ILIKE 'BAZ' ORDER BY id)q",
            R"q(PRAGMA OptimizeSimpleILike; PRAGMA AnsiLike; SELECT id FROM `/Root/foo` WHERE u_str ILIKE '%nomatch%' ORDER BY id)q",
            R"q(PRAGMA OptimizeSimpleILike; PRAGMA AnsiLike; SELECT id FROM `/Root/foo` WHERE u_str NOT ILIKE '%foo%' ORDER BY id)q",
            R"q(PRAGMA OptimizeSimpleILike; PRAGMA AnsiLike; PRAGMA kikimr.OptEnableOlapFastAsciiIgnoreCase = "true"; SELECT id FROM `/Root/foo` WHERE str ILIKE '%foo%' ORDER BY id)q",
            R"q(PRAGMA OptimizeSimpleILike; PRAGMA AnsiLike; PRAGMA kikimr.OptEnableOlapFastAsciiIgnoreCase = "true"; SELECT id FROM `/Root/foo` WHERE str ILIKE '%FOO%' ORDER BY id)q",
            R"q(PRAGMA OptimizeSimpleILike; PRAGMA AnsiLike; PRAGMA kikimr.OptEnableOlapFastAsciiIgnoreCase = "true"; SELECT id FROM `/Root/foo` WHERE str ILIKE 'foo%' ORDER BY id)q",
            R"q(PRAGMA OptimizeSimpleILike; PRAGMA AnsiLike; PRAGMA kikimr.OptEnableOlapFastAsciiIgnoreCase = "true"; SELECT id FROM `/Root/foo` WHERE str ILIKE '%QUX' ORDER BY id)q",
            R"q(PRAGMA OptimizeSimpleILike; PRAGMA AnsiLike; PRAGMA kikimr.OptEnableOlapFastAsciiIgnoreCase = "true"; SELECT id FROM `/Root/foo` WHERE str ILIKE 'BAZ' ORDER BY id)q",
            R"q(PRAGMA OptimizeSimpleILike; PRAGMA AnsiLike; PRAGMA kikimr.OptEnableOlapFastAsciiIgnoreCase = "true"; SELECT id FROM `/Root/foo` WHERE str ILIKE '%nomatch%' ORDER BY id)q",
            R"q(PRAGMA OptimizeSimpleILike; PRAGMA AnsiLike; PRAGMA kikimr.OptEnableOlapFastAsciiIgnoreCase = "true"; SELECT id FROM `/Root/foo` WHERE str NOT ILIKE '%foo%' ORDER BY id)q",
            R"q(PRAGMA OptimizeSimpleILike; PRAGMA AnsiLike; PRAGMA kikimr.OptEnableOlapFastAsciiIgnoreCase = "true"; SELECT id FROM `/Root/foo` WHERE u_str ILIKE '%foo%' ORDER BY id)q",
            R"q(PRAGMA OptimizeSimpleILike; PRAGMA AnsiLike; PRAGMA kikimr.OptEnableOlapFastAsciiIgnoreCase = "true"; SELECT id FROM `/Root/foo` WHERE u_str ILIKE '%FOO%' ORDER BY id)q",
            R"q(PRAGMA OptimizeSimpleILike; PRAGMA AnsiLike; PRAGMA kikimr.OptEnableOlapFastAsciiIgnoreCase = "true"; SELECT id FROM `/Root/foo` WHERE u_str ILIKE 'foo%' ORDER BY id)q",
            R"q(PRAGMA OptimizeSimpleILike; PRAGMA AnsiLike; PRAGMA kikimr.OptEnableOlapFastAsciiIgnoreCase = "true"; SELECT id FROM `/Root/foo` WHERE u_str ILIKE '%QUX' ORDER BY id)q",
            R"q(PRAGMA OptimizeSimpleILike; PRAGMA AnsiLike; PRAGMA kikimr.OptEnableOlapFastAsciiIgnoreCase = "true"; SELECT id FROM `/Root/foo` WHERE u_str ILIKE 'BAZ' ORDER BY id)q",
            R"q(PRAGMA OptimizeSimpleILike; PRAGMA AnsiLike; PRAGMA kikimr.OptEnableOlapFastAsciiIgnoreCase = "true"; SELECT id FROM `/Root/foo` WHERE u_str ILIKE '%nomatch%' ORDER BY id)q",
            R"q(PRAGMA OptimizeSimpleILike; PRAGMA AnsiLike; PRAGMA kikimr.OptEnableOlapFastAsciiIgnoreCase = "true"; SELECT id FROM `/Root/foo` WHERE u_str NOT ILIKE '%foo%' ORDER BY id)q",
        });
    }

    // As in the old optimizer, column shards do not return the columns that only the program or the ranges need.
    Y_UNIT_TEST(ReadColumnsSameAsOldOptimizer) {
        CheckSameAsOldOptimizer([](TKikimrRunner& kikimr) {
            ExecuteSchemeQueries(kikimr, {R"(
                CREATE TABLE `/Root/foo` (
                    id Int64 NOT NULL,
                    level Int32,
                    message String,
                    payload JsonDocument,
                    PRIMARY KEY (id)
                )
                PARTITION BY HASH(id)
                WITH (STORE = COLUMN, PARTITION_COUNT = 1);
            )"});
            using TRow = std::tuple<i64, std::optional<i32>, std::optional<std::string>, std::optional<std::string>>;
            TValueBuilder rows;
            rows.BeginList();
            for (const auto& [id, level, message, payload] : std::vector<TRow>{
                     {1, 1, "foo", R"({"a":"x"})"}, {2, 2, "bar", R"({"a":"y"})"}, {3, 3, "foobar", std::nullopt},
                     {4, std::nullopt, std::nullopt, R"({"b":"z"})"}}) {
                rows.AddListItem().BeginStruct().AddMember("id").Int64(id).AddMember("level").OptionalInt32(level)
                    .AddMember("message").OptionalString(message).AddMember("payload").OptionalJsonDocument(payload).EndStruct();
            }
            rows.EndList();
            const auto result = kikimr.GetTableClient().BulkUpsert("/Root/foo", rows.Build()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }, {
            // Filter columns.
            R"q(SELECT id FROM `/Root/foo` WHERE level > 1 ORDER BY id)q",
            R"q(SELECT id, level FROM `/Root/foo` WHERE message LIKE "%foo%" ORDER BY id)q",
            R"q(SELECT id FROM `/Root/foo` WHERE level > 1 ORDER BY id LIMIT 2)q",
            // A filter column that the remaining compute filter reads too.
            R"q(SELECT id FROM `/Root/foo` WHERE level > 1 AND message || "x" = "barx")q",
            // No column but the row count.
            R"q(SELECT COUNT(*) FROM `/Root/foo` WHERE level > 1)q",
            R"q(SELECT 1 FROM `/Root/foo`)q",
            R"q(SELECT 1 FROM `/Root/foo` WHERE level > 1)q",
            R"q(SELECT 1 FROM `/Root/foo` WHERE level > 1 AND message LIKE "%foo%")q",
            R"q(SELECT 1 FROM `/Root/foo` WHERE id = 2)q",
            // A key column the ranges read.
            R"q(SELECT message FROM `/Root/foo` WHERE id = 2)q",
            // A filter column under a projection.
            R"q(SELECT JSON_VALUE(payload, "$.a") AS a FROM `/Root/foo` WHERE level > 1 ORDER BY a)q",
        }, /*withProjections=*/true);
    }
}

} // namespace NKikimr::NKqp
