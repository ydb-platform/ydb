#include <ydb/core/kqp/ut/indexes/json/common/kqp_indexes_json_ut_common.h>

namespace NKikimr::NKqp {

using namespace NYdb::NQuery;
using namespace NYdb;

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
