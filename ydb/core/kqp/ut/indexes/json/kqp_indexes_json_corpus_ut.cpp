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
}

} // namespace

Y_UNIT_TEST_SUITE(KqpJsonIndexesCorpus_JE) {
    Y_UNIT_TEST_TWIN(Basic, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xE001,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Ranges, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableRangeComparisons = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xE002,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Methods, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonPathMethods = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xE003,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Predicates, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonPathPredicates = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xE004,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Predicates_Ranges, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonPathPredicates = true,
            .EnableRangeComparisons = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xE005,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Methods_Predicates, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xE006,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Methods_Predicates_Ranges, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = true,
            .EnableRangeComparisons = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xE007,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_JsonLiteral, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnablePassingVariables = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xE008,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_YqlParameter, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xE009,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_Ranges, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xE00A,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_Methods, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonPathMethods = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xE00B,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_Predicates, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonPathPredicates = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xE00C,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_Methods_Predicates, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xE00D,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_Literals, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableJsonIsLiteral = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xE00E,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Literals, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonIsLiteral = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xE00F,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Literals_Ranges, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableRangeComparisons = true,
            .EnableJsonIsLiteral = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xE010,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Literals_Ranges_Variables, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = true,
            .EnableJsonIsLiteral = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xE011,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Arithmetic, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonPathMethods = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = true,
            .EnableJsonIsLiteral = true,
            .EnableArithmeticOperators = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xE012,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(SqlNullChecks, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = true,
            .EnableSqlNullChecks = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xE013,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(DistinctFrom, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableDistinctFrom = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xE029,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(DistinctFrom_Variables, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableDistinctFrom = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xE02B,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_Indexable, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableAndCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xE013,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_NonIndexable, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableNonJsonFilters = true,
            .EnableAndCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xE014,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_Ranges, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableNonJsonFilters = true,
            .EnableRangeComparisons = true,
            .EnableAndCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xE015,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_Predicates, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableNonJsonFilters = true,
            .EnableJsonPathPredicates = true,
            .EnableAndCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xE016,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_Variables, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableNonJsonFilters = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableAndCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xE017,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_Literals, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableNonJsonFilters = true,
            .EnableAndCombinations = true,
            .EnableJsonIsLiteral = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xE018,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_Arithmetic, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableNonJsonFilters = true,
            .EnableJsonPathMethods = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = true,
            .EnableAndCombinations = true,
            .EnableJsonIsLiteral = true,
            .EnableArithmeticOperators = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xE019,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_Indexable, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xE01A,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_NonIndexable, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableNonJsonFilters = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xE01B,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_Ranges, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableNonJsonFilters = true,
            .EnableRangeComparisons = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xE01C,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_Predicates, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableNonJsonFilters = true,
            .EnableJsonPathPredicates = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xE01D,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_Variables, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableNonJsonFilters = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xE01E,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_Literals, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableNonJsonFilters = true,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xE01F,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_Arithmetic, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonPathMethods = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = true,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = true,
            .EnableArithmeticOperators = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xE020,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Indexable, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xE021,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_NonIndexable, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableNonJsonFilters = true,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xE022,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Ranges, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableNonJsonFilters = true,
            .EnableRangeComparisons = true,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xE023,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Predicates, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableNonJsonFilters = true,
            .EnableJsonPathPredicates = true,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xE024,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Variables, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableNonJsonFilters = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xE025,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Literals, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableNonJsonFilters = true,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xE026,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Arithmetic, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableNonJsonFilters = true,
            .EnableJsonPathMethods = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = true,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = true,
            .EnableArithmeticOperators = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xE027,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(ComplexFilters, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableComplexJsonPathFilters = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xE02C,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(ComplexFilters_Methods, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonPathMethods = true,
            .EnablePassingVariables = true,
            .EnableComplexJsonPathFilters = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xE02D,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(All, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
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
            .EnableSqlNullChecks = true,
            .EnableDistinctFrom = true,
            .EnableComplexJsonPathFilters = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xE028,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }
}

Y_UNIT_TEST_SUITE(KqpJsonIndexesCorpus_JV) {
    Y_UNIT_TEST_TWIN(Basic, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE01,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Ranges, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableRangeComparisons = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE02,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Methods, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableJsonPathMethods = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE03,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Predicates, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableJsonPathPredicates = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE04,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Predicates_Ranges, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableJsonPathPredicates = true,
            .EnableRangeComparisons = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE05,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Methods_Predicates, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE06,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Methods_Predicates_Ranges, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = true,
            .EnableRangeComparisons = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE07,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Between, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableBetween = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE08,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(InList, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableInList = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE09,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Between_InList, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableBetween = true,
            .EnableInList = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE0A,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_JsonLiteral, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnablePassingVariables = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE0B,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_YqlParameter, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE0C,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_Ranges, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE0D,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_Methods, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableJsonPathMethods = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE0E,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_Predicates, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableJsonPathPredicates = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE0F,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_Methods_Predicates, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE10,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_Between, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableBetween = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE11,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_InList, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableInList = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE12,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_Between_InList, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableBetween = true,
            .EnableInList = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE13,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_Literals, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableJsonIsLiteral = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE14,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Literals, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableJsonIsLiteral = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE15,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Arithmetic, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableJsonPathMethods = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = true,
            .EnableBetween = true,
            .EnableInList = true,
            .EnableArithmeticOperators = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE16,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(SqlNullChecks, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = true,
            .EnableSqlNullChecks = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xE013,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(DistinctFrom, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableDistinctFrom = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE2D,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(DistinctFrom_Variables, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableDistinctFrom = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE2F,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(ComplexFilters, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableComplexJsonPathFilters = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE30,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(ComplexFilters_Methods, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableJsonPathMethods = true,
            .EnablePassingVariables = true,
            .EnableComplexJsonPathFilters = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE31,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_Indexable, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableAndCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE17,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_NonIndexable, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableAndCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE18,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_Ranges, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableRangeComparisons = true,
            .EnableAndCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE19,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_Predicates, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableJsonPathPredicates = true,
            .EnableAndCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE1A,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_Variables, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableAndCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE1B,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_Literals, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableAndCombinations = true,
            .EnableJsonIsLiteral = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE1C,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_Arithmetic, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableJsonPathMethods = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = true,
            .EnableBetween = true,
            .EnableInList = true,
            .EnableAndCombinations = true,
            .EnableJsonIsLiteral = true,
            .EnableArithmeticOperators = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE1D,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_Indexable, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE1E,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_NonIndexable, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE1F,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_Ranges, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableRangeComparisons = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE20,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_Predicates, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableJsonPathPredicates = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE21,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_Variables, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE22,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_Literals, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE23,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_Arithmetic, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableJsonPathMethods = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = true,
            .EnableBetween = true,
            .EnableInList = true,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = true,
            .EnableArithmeticOperators = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE24,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Indexable, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE25,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_NonIndexable, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE26,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Ranges, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableRangeComparisons = true,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE27,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Predicates, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableJsonPathPredicates = true,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE28,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Variables, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE29,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Literals, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE2A,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Arithmetic, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableJsonPathMethods = true,
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
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE2B,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(All, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
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
            .EnableSqlNullChecks = true,
            .EnableDistinctFrom = true,
            .EnableComplexJsonPathFilters = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE2C,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }
}

Y_UNIT_TEST_SUITE(KqpJsonIndexesCorpus_JEJV) {
    Y_UNIT_TEST_TWIN(Basic, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xEE01,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Ranges, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableRangeComparisons = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xEE02,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Methods, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableJsonPathMethods = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xEE03,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Predicates, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableJsonPathPredicates = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xEE04,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Predicates_Ranges, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableJsonPathPredicates = true,
            .EnableRangeComparisons = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xEE05,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Methods_Predicates, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xEE06,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Methods_Predicates_Ranges, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = true,
            .EnableRangeComparisons = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xEE07,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Between, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableBetween = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xEE08,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(InList, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableInList = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xEE09,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Between_InList, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableBetween = true,
            .EnableInList = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xEE0A,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_JsonLiteral, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnablePassingVariables = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xEE0B,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_YqlParameter, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xEE0C,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_Ranges, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xEE0D,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_Methods, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableJsonPathMethods = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xEE0E,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_Predicates, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableJsonPathPredicates = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xEE0F,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_Methods_Predicates, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xEE10,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_Between, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableBetween = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xEE11,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_InList, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableInList = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xEE12,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_Between_InList, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableBetween = true,
            .EnableInList = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xEE13,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_Literals, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableJsonIsLiteral = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xEE14,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Literals, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableJsonIsLiteral = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xEE15,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Arithmetic, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableJsonPathMethods = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = true,
            .EnableBetween = true,
            .EnableInList = true,
            .EnableArithmeticOperators = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xEE16,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(SqlNullChecks, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = true,
            .EnableSqlNullChecks = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xE013,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(DistinctFrom, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableDistinctFrom = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xEE2D,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(DistinctFrom_Variables, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableDistinctFrom = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xEE2F,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(ComplexFilters, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableComplexJsonPathFilters = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE30,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(ComplexFilters_Methods, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableJsonPathMethods = true,
            .EnablePassingVariables = true,
            .EnableComplexJsonPathFilters = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xAE31,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_Indexable, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableAndCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xEE17,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_NonIndexable, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableAndCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xEE18,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_Ranges, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableRangeComparisons = true,
            .EnableAndCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xEE19,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_Predicates, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableJsonPathPredicates = true,
            .EnableAndCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xEE1A,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_Variables, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableAndCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xEE1B,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_Literals, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableAndCombinations = true,
            .EnableJsonIsLiteral = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xEE1C,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_Arithmetic, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableJsonPathMethods = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = true,
            .EnableBetween = true,
            .EnableAndCombinations = true,
            .EnableJsonIsLiteral = true,
            .EnableArithmeticOperators = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xEE1D,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_Indexable, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xEE1E,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_NonIndexable, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xEE1F,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_Ranges, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableRangeComparisons = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xEE20,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_Predicates, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableJsonPathPredicates = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xEE21,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_Variables, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xEE22,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_Literals, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xEE23,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_Arithmetic, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableJsonPathMethods = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = true,
            .EnableBetween = true,
            .EnableInList = true,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = true,
            .EnableArithmeticOperators = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xEE24,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Indexable, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xEE25,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_NonIndexable, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xEE26,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Ranges, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableRangeComparisons = true,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xEE27,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Predicates, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableJsonPathPredicates = true,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xEE28,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Variables, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xEE29,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Literals, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xEE2A,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Arithmetic, IsJsonDocument) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableJsonPathMethods = true,
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
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xEE2B,
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
            .EnableSqlNullChecks = true,
            .EnableDistinctFrom = true,
            .EnableComplexJsonPathFilters = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = IsJsonDocument,
            .RowCount = 1000,
            .MaxPredicates = 50,
            .Seed = 0xEE2C,
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }
}
}  // namespace NKikimr::NKqp
