#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/core/kqp/ut/indexes/json/kqp_json_index_corpus.h>
#include <ydb/core/kqp/ut/indexes/json/kqp_json_index_predicate.h>

namespace NKikimr::NKqp {

using namespace NYdb::NQuery;
using namespace NYdb;

namespace {

constexpr ui64 CorpusSeed(ui32 index) noexcept {
    constexpr ui64 base = 0x4A504A4A4A500000ULL;
    return base + static_cast<ui64>(index) * 2u;
}

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

            Cerr << p.Sql << ", err" << Endl;
        } else {
            UNIT_ASSERT_C(idxResult.IsSuccess(), "INDEX query failed for predicate: " << p.Sql << " err: " << idxResult.GetIssues().ToString());
            UNIT_ASSERT_C(mainResult.IsSuccess(), "MAIN query failed for predicate: " << p.Sql << " err: " << mainResult.GetIssues().ToString());
            CompareYson(FormatResultSetYson(mainResult.GetResultSet(0)), FormatResultSetYson(idxResult.GetResultSet(0)));
            ++okCount;

            Cerr << p.Sql << ", size: " << idxResult.GetResultSet(0).RowsCount() << Endl;
        }
    }

    Cerr << "JsonIndexCorpus: ok=" << okCount << " err=" << errCount << " total=" << predicates.size() << Endl;
}

} // namespace

Y_UNIT_TEST_SUITE(KqpJsonIndexesCorpus_JE) {
    Y_UNIT_TEST_TWIN(Basic, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(0) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(SqlParameters, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableSqlParameters = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 120,
            .Seed = CorpusSeed(1) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Ranges, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableRangeComparisons = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(2) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Methods, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonPathMethods = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(3) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Predicates, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonPathPredicates = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(4) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Predicates_Ranges, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonPathPredicates = true,
            .EnableRangeComparisons = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(5) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Methods_Predicates, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(6) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Methods_Predicates_Ranges, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = true,
            .EnableRangeComparisons = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(7) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Methods_Ranges_Passing, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonPathMethods = true,
            .EnablePassingVariables = true,
            .EnableRangeComparisons = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(8) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Methods_Predicates_Passing, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = true,
            .EnablePassingVariables = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(9) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_JsonLiteral, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnablePassingVariables = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(10) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_YqlParameter, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(11) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_Ranges, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(12) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_Methods, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonPathMethods = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(13) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_Predicates, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonPathPredicates = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(14) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_Methods_Predicates, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(15) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_Literals, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableJsonIsLiteral = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(16) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Literals, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonIsLiteral = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(17) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Literals_Predicates, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonPathPredicates = true,
            .EnableJsonIsLiteral = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(18) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Literals_Ranges, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableRangeComparisons = true,
            .EnableJsonIsLiteral = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(19) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Literals_Predicates_Ranges, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonPathPredicates = true,
            .EnableRangeComparisons = true,
            .EnableJsonIsLiteral = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(20) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Literals_Ranges_Variables, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = true,
            .EnableJsonIsLiteral = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(21) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Arithmetic, IsStrict) {
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
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(22) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(SqlNullChecks, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = true,
            .EnableSqlNullChecks = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(23) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(DistinctFrom, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableDistinctFrom = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(24) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(DistinctFrom_Variables, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableDistinctFrom = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(25) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_Indexable, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableAndCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(26) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_NonIndexable, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableNonJsonFilters = true,
            .EnableAndCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(27) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_Ranges, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableNonJsonFilters = true,
            .EnableRangeComparisons = true,
            .EnableAndCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(28) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_Predicates, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableNonJsonFilters = true,
            .EnableJsonPathPredicates = true,
            .EnableAndCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(29) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_Variables, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableNonJsonFilters = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableAndCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(30) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_Literals, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableNonJsonFilters = true,
            .EnableAndCombinations = true,
            .EnableJsonIsLiteral = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(31) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_Arithmetic, IsStrict) {
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
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(32) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_Indexable, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(33) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_NonIndexable, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableNonJsonFilters = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(34) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_Ranges, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableNonJsonFilters = true,
            .EnableRangeComparisons = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(35) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_Predicates, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableNonJsonFilters = true,
            .EnableJsonPathPredicates = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(36) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_Variables, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableNonJsonFilters = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(37) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_Literals, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableNonJsonFilters = true,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(38) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_Arithmetic, IsStrict) {
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
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(39) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Indexable, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(40) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_NonIndexable, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableNonJsonFilters = true,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(41) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Ranges, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableNonJsonFilters = true,
            .EnableRangeComparisons = true,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(42) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Predicates, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableNonJsonFilters = true,
            .EnableJsonPathPredicates = true,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(43) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Variables, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableNonJsonFilters = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(44) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Literals, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableNonJsonFilters = true,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(45) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Arithmetic, IsStrict) {
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
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(46) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(ComplexFilters, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableComplexJsonPathFilters = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(47) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(ComplexFilters_Extensions, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = true,
            .EnableComplexJsonPathFilters = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 200,
            .Seed = CorpusSeed(48) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(ComplexFilters_Methods, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonPathMethods = true,
            .EnablePassingVariables = true,
            .EnableComplexJsonPathFilters = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(49) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(All, IsStrict) {
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
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(50) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }
}

Y_UNIT_TEST_SUITE(KqpJsonIndexesCorpus_JV) {
    Y_UNIT_TEST_TWIN(Basic, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(51) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(SqlParameters, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableSqlParameters = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 120,
            .Seed = CorpusSeed(52) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Ranges, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableRangeComparisons = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(53) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Methods, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableJsonPathMethods = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(54) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Predicates, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableJsonPathPredicates = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(55) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Predicates_Ranges, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableJsonPathPredicates = true,
            .EnableRangeComparisons = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(56) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Methods_Predicates, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(57) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Methods_Predicates_Ranges, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = true,
            .EnableRangeComparisons = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(58) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Methods_Ranges_Passing, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableJsonPathMethods = true,
            .EnablePassingVariables = true,
            .EnableRangeComparisons = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(59) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Methods_Predicates_Passing, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = true,
            .EnablePassingVariables = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(60) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Between, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableBetween = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(61) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(InList, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableInList = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(62) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Between_InList, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableBetween = true,
            .EnableInList = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(63) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_JsonLiteral, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnablePassingVariables = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(64) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_YqlParameter, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(65) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_Ranges, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(66) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_Methods, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableJsonPathMethods = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(67) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_Predicates, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableJsonPathPredicates = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(68) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_Methods_Predicates, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(69) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_Between, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableBetween = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(70) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_InList, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableInList = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(71) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_Between_InList, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableBetween = true,
            .EnableInList = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(72) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Variables_Literals, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableJsonIsLiteral = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(73) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Literals, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableJsonIsLiteral = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(74) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Literals_Predicates, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableJsonPathPredicates = true,
            .EnableJsonIsLiteral = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(75) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Literals_Ranges, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableRangeComparisons = true,
            .EnableJsonIsLiteral = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(76) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Literals_Predicates_Ranges, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableJsonPathPredicates = true,
            .EnableRangeComparisons = true,
            .EnableJsonIsLiteral = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(77) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Literals_Ranges_Variables, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = true,
            .EnableJsonIsLiteral = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(78) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(Arithmetic, IsStrict) {
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
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(79) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(SqlNullChecks, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = true,
            .EnableSqlNullChecks = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(80) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(DistinctFrom, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableDistinctFrom = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(81) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(DistinctFrom_Variables, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableDistinctFrom = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(82) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(ComplexFilters, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableComplexJsonPathFilters = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(83) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(ComplexFilters_Extensions, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableJsonPathMethods = true,
            .EnableJsonPathPredicates = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableRangeComparisons = true,
            .EnableComplexJsonPathFilters = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 200,
            .Seed = CorpusSeed(84) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(ComplexFilters_Methods, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableJsonPathMethods = true,
            .EnablePassingVariables = true,
            .EnableComplexJsonPathFilters = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(85) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_Indexable, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableAndCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(86) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_NonIndexable, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableAndCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(87) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_Ranges, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableRangeComparisons = true,
            .EnableAndCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(88) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_Predicates, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableJsonPathPredicates = true,
            .EnableAndCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(89) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_Variables, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableAndCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(90) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_Literals, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableAndCombinations = true,
            .EnableJsonIsLiteral = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(91) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndCombinations_Arithmetic, IsStrict) {
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
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(92) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_Indexable, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(93) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_NonIndexable, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(94) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_Ranges, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableRangeComparisons = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(95) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_Predicates, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableJsonPathPredicates = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(96) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_Variables, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(97) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_Literals, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(98) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(OrCombinations_Arithmetic, IsStrict) {
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
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(99) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Indexable, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(100) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_NonIndexable, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(101) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Ranges, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableRangeComparisons = true,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(102) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Predicates, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableJsonPathPredicates = true,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(103) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Variables, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnablePassingVariables = true,
            .EnableSqlParameters = true,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(104) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Literals, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(105) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Arithmetic, IsStrict) {
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
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(106) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(All, IsStrict) {
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
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(107) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }
}

Y_UNIT_TEST_SUITE(KqpJsonIndexesCorpus_JEJV) {
    Y_UNIT_TEST_TWIN(AndOrCombinations_Indexable, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(108) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_NonIndexable, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(109) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Ranges, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableRangeComparisons = true,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(110) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Predicates, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableJsonPathPredicates = true,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(111) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Variables, IsStrict) {
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
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(112) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Literals, IsStrict) {
        TPredicateBuilderOptions pOpts = {
            .EnableJsonExists = true,
            .EnableJsonValue = true,
            .EnableNonJsonFilters = true,
            .EnableAndCombinations = true,
            .EnableOrCombinations = true,
            .EnableJsonIsLiteral = true,
        };

        TTestJsonCorpusOptions tOpts = {
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(113) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(AndOrCombinations_Arithmetic, IsStrict) {
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
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(114) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }

    Y_UNIT_TEST_TWIN(All, IsStrict) {
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
            .IsJsonDocument = true,
            .IsStrict = IsStrict,
            .RowCount = 1000,
            .MaxPredicates = 75,
            .Seed = CorpusSeed(115) + static_cast<ui64>(IsStrict),
        };

        TestJsonCorpus(std::move(tOpts), std::move(pOpts));
    }
}
}  // namespace NKikimr::NKqp
