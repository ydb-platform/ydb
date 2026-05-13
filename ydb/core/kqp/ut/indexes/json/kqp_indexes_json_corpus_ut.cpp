#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/core/kqp/ut/indexes/json/kqp_json_index_corpus.h>
#include <ydb/core/kqp/ut/indexes/json/kqp_json_index_predicate.h>

namespace NKikimr::NKqp {

using namespace NYdb::NQuery;
using namespace NYdb;

namespace {

TKikimrRunner Kikimr(bool enableJsonIndex = true) {
    NKikimrConfig::TFeatureFlags featureFlags;
    featureFlags.SetEnableJsonIndex(enableJsonIndex);
    auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
    return TKikimrRunner(settings);
}

void CreateTestTable(TQueryClient& db, const std::string& type = "Json", bool withIndex = false) {
    const auto query = std::format(R"(
        CREATE TABLE TestTable (
            Key Uint64,
            Text {0},
            Data Utf8,
            PRIMARY KEY (Key)
            {1}
        );
    )", type, withIndex ? ", INDEX `json_idx` GLOBAL USING json ON (Text)" : "");
    auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

struct TTestJsonCorpusOptions {
    bool IsJsonDocument = false;
    bool IsStrict = false;
    size_t RowCount = 1000;
    size_t MaxPredicates = 500;
    ui64 Seed = 0xC0DE;
};

void TestJsonCorpus(TTestJsonCorpusOptions tOpts, TPredicateBuilderOptions pOpts) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();

    const auto jsonType = std::string(tOpts.IsJsonDocument ? "JsonDocument" : "Json");
    CreateTestTable(db, jsonType, /* withIndex */ false);

    TJsonCorpus corpus(TCorpusOptions{.RowCount = tOpts.RowCount, .Seed = tOpts.Seed});

    corpus.UpsertRange(db, "TestTable", jsonType, 0, tOpts.RowCount / 2);
    {
        auto result = db.ExecuteQuery(R"(
                ALTER TABLE TestTable ADD INDEX json_idx GLOBAL USING json ON (Text)
            )", TTxControl::NoTx())
                          .ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }
    corpus.UpsertRange(db, "TestTable", jsonType, tOpts.RowCount / 2, tOpts.RowCount / 2);

    auto execQ = [&](const std::string& sql, const std::optional<TParams>& params) {
        if (params) {
            return db.ExecuteQuery(sql, TTxControl::NoTx(), *params).ExtractValueSync();
        }
        return db.ExecuteQuery(sql, TTxControl::NoTx()).ExtractValueSync();
    };

    size_t okCount = 0;
    size_t errCount = 0;

    auto predicates = TPredicateBuilder().BuildBatch(corpus, tOpts.IsStrict, tOpts.MaxPredicates, tOpts.Seed, pOpts);
    for (const auto& p : predicates) {
        const auto sqlMain = std::format("SELECT Key FROM TestTable VIEW PRIMARY KEY WHERE {} ORDER BY Key", p.Sql);
        const auto sqlIndex = std::format("SELECT Key FROM TestTable VIEW json_idx WHERE {} ORDER BY Key", p.Sql);

        auto idxResult = execQ(sqlIndex, p.Params);
        auto mainResult = execQ(sqlMain, p.Params);

        if (p.ExpectExtractError) {
            UNIT_ASSERT_C(!idxResult.IsSuccess(), "Expected extract error for predicate: " << p.Sql);
            UNIT_ASSERT_STRING_CONTAINS_C(idxResult.GetIssues().ToString(), p.ExpectedErrorSubstr, "for predicate: " << p.Sql);
            UNIT_ASSERT_C(mainResult.IsSuccess(), "Main query failed for predicate: " << p.Sql << " err: " << mainResult.GetIssues().ToString());
            ++errCount;

            Cout << p.Sql << ", err";
        } else {
            UNIT_ASSERT_C(idxResult.IsSuccess(), "INDEX query failed for predicate: " << p.Sql << " err: " << idxResult.GetIssues().ToString());
            UNIT_ASSERT_C(mainResult.IsSuccess(), "MAIN query failed for predicate: " << p.Sql << " err: " << mainResult.GetIssues().ToString());
            CompareYson(FormatResultSetYson(mainResult.GetResultSet(0)), FormatResultSetYson(idxResult.GetResultSet(0)));
            ++okCount;

            Cout << p.Sql << ", size: " << idxResult.GetResultSet(0).RowsCount() << Endl;
        }
    }

    Cerr << "JsonIndexCorpus: ok=" << okCount << " err=" << errCount << " total=" << predicates.size() << Endl;
    UNIT_ASSERT_GE(okCount, predicates.size() / 2);
}

} // namespace

Y_UNIT_TEST_SUITE(KqpJsonIndexesCorpus_JE) {
    Y_UNIT_TEST_TWIN(Basic, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = false,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xC0DE,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Ranges, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = false,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = true,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xBABE,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Methods, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = false,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xC0DE,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Predicates, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = false,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = true,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xC0DE,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Predicates_Ranges, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = false,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = true,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = true,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xC0DE,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Methods_Predicates, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = false,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = true,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xC0DE,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Methods_Predicates_Ranges, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = false,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = true,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = true,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xC0DE,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_JsonLiteral, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = false,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = true,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xBABE,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_YqlParameter, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = false,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xCAFE,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_Ranges, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = false,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = true,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xCAFE,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_Methods, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = false,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xC0DE,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_Predicates, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = false,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xC0DE,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_Methods_Predicates, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = false,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xC0DE,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Literals, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = false,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = true,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xBACE,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Literals_Ranges, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = false,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = true,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = true,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xBACE,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Literals_Ranges_Variables, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = false,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = true,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = true,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xBACE,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Arithmetic, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = false,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = true,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = true,
            .EnableArithmeticOperators = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xCACE,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_Indexable, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = false,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = true,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xDEAD,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_NonIndexable, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = false,
            .EnableNonJsonFilters = true,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = true,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xBABA,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_Ranges, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = false,
            .EnableNonJsonFilters = true,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = true,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = true,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xBABA,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_Variables, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = false,
            .EnableNonJsonFilters = true,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = true,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xBABA,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_Literals, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = false,
            .EnableNonJsonFilters = true,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = true,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = true,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xBABA,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_Arithmetic, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = false,
            .EnableNonJsonFilters = true,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = true,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = true,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = true,
            .EnableArithmeticOperators = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xBABA,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_Indexable, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = false,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xBEEF,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_NonIndexable, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = false,
            .EnableNonJsonFilters = true,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xCACA,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_Ranges, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = false,
            .EnableNonJsonFilters = true,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = true,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xCACA,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_Variables, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = false,
            .EnableNonJsonFilters = true,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xCACA,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_Literals, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = false,
            .EnableNonJsonFilters = true,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = true,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xCACA,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_Arithmetic, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = false,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = true,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = true,
            .EnableArithmeticOperators = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xCACA,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Indexable, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = false,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAAAA,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_NonIndexable, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = false,
            .EnableNonJsonFilters = true,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xCCCC,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Ranges, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = false,
            .EnableNonJsonFilters = true,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = true,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xCCCC,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Variables, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = false,
            .EnableNonJsonFilters = true,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xCCCC,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Literals, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = false,
            .EnableNonJsonFilters = true,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = true,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xCCCC,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Arithmetic, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = false,
            .EnableNonJsonFilters = true,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = true,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = true,
            .EnableArithmeticOperators = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xCCCC,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(All, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = false,
            .EnableNonJsonFilters = true,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = true,
            .EnableBetween = true,
            .EnableInList = true,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = true,
            .EnableArithmeticOperators = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xBABE,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }
}

Y_UNIT_TEST_SUITE(KqpJsonIndexesCorpus_JV) {
    Y_UNIT_TEST_TWIN(Basic, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = false,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0x1DE0,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Ranges, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = false,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = true,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0x2DE1,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Methods, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = false,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xBEEF,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Predicates, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = false,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = true,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xBEEF,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Predicates_Ranges, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = false,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = true,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = true,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xBEEF,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Methods_Predicates, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = false,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = true,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xBEEF,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Methods_Predicates_Ranges, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = false,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = true,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = true,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xBEEF,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Between, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = false,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = true,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0x3DE2,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(InList, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = false,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = true,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0x4DE3,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_JsonLiteral, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = false,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = true,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0x5DE4,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_YqlParameter, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = false,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0x5DE5,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_Ranges, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = false,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = true,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0x5DE5,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_Methods, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = false,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xBEEF,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_Predicates, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = false,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xBEEF,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_Methods_Predicates, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = false,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = true,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xBEEF,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_Between, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = false,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = false,
            .EnableBetween = true,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0x3DE2,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_InList, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = false,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = true,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0x4DE3,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Literals, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = false,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = true,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xCEEB,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Arithmetic, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = false,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = true,
            .EnableBetween = true,
            .EnableInList = true,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xCEEB,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_Indexable, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = false,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = true,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0x6DE5,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_NonIndexable, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = false,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = true,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0x7DE6,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_Ranges, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = false,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = true,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = true,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0x7DE6,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_Variables, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = false,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = true,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0x7DE6,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_Literals, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = false,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = true,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = true,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0x7DE6,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_Arithmetic, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = false,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = true,
            .EnableBetween = true,
            .EnableInList = true,
            .EnableAndCombinations = true,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = true,
            .EnableArithmeticOperators = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0x7DE6,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_Indexable, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = false,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0x8DE7,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_NonIndexable, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = false,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0x9DE8,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_Ranges, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = false,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = true,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0x9DE8,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_Variables, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = false,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0x9DE8,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_Literals, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = false,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = true,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0x9DE8,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_Arithmetic, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = false,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = true,
            .EnableBetween = true,
            .EnableInList = true,
            .EnableAndCombinations = false,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = true,
            .EnableArithmeticOperators = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0x9DE8,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Indexable, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = false,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xADE9,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_NonIndexable, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = false,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xBDEA,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Ranges, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = false,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = true,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xBDEA,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Variables, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = false,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xBDEA,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Literals, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = false,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = true,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xBDEA,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Arithmetic, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = false,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = true,
            .EnableBetween = true,
            .EnableInList = true,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = true,
            .EnableArithmeticOperators = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xBDEA,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(All, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = false,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = true,
            .EnableBetween = true,
            .EnableInList = true,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = true,
            .EnableArithmeticOperators = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xCDEB,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }
}

Y_UNIT_TEST_SUITE(KqpJsonIndexesCorpus_JEJV) {
    Y_UNIT_TEST_TWIN(Basic, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xF001,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Ranges, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = true,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xF002,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Methods, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xF0A3,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Predicates, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = true,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xF0A3,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Predicates_Ranges, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = true,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = true,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xF0A3,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Methods_Predicates, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = true,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xF0A3,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Methods_Predicates_Ranges, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = true,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = true,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xF0A3,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Between, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = true,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xF003,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(InList, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = true,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xF004,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_JsonLiteral, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = true,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xF005,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_YqlParameter, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xF006,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_Ranges, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = true,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xF006,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_Methods, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xF0A3,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_Predicates, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xF0A3,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_Methods_Predicates, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xF0A3,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_Between, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = false,
            .EnableBetween = true,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xF003,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_InList, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = true,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xF004,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Literals, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = true,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xF11D,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Arithmetic, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = true,
            .EnableBetween = true,
            .EnableInList = true,
            .EnableAndCombinations = false,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xF11D,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_Indexable, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = true,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xF007,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_NonIndexable, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = true,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xF008,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_Ranges, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = true,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = true,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xF008,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_Variables, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = true,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xF008,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_Literals, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = true,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = true,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xF008,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_Arithmetic, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = true,
            .EnableBetween = true,
            .EnableInList = false,
            .EnableAndCombinations = true,
            .EnableOrCombinations = false,
            .EnableJsonIsLiteral = true,
            .EnableArithmeticOperators = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xF008,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_Indexable, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xF009,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_NonIndexable, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xF00A,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_Ranges, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = true,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xF00A,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_Variables, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xF00A,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_Literals, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = false,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = true,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xF00A,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_Arithmetic, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = true,
            .EnableBetween = true,
            .EnableInList = true,
            .EnableAndCombinations = false,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = true,
            .EnableArithmeticOperators = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xF00A,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Indexable, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = false,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xF00B,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_NonIndexable, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xF00C,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Ranges, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = true,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xF00C,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Variables, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = false,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xF00C,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Literals, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableJsonPathMethods = false,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = false,
            .EnableSqlParameters = false,
            .EnableRangeComparisons = false,
            .EnableBetween = false,
            .EnableInList = false,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = true,
            .EnableArithmeticOperators = false,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xF00C,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Arithmetic, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = false,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = true,
            .EnableBetween = true,
            .EnableInList = true,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = true,
            .EnableArithmeticOperators = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xF00C,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(All, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = true,
            .EnableBetween = true,
            .EnableInList = true,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = true,
            .EnableArithmeticOperators = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .IsStrict = false,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xF00D,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }
}
}  // namespace NKikimr::NKqp
