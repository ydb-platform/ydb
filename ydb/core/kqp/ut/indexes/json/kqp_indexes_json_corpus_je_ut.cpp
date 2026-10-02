#include <ydb/core/kqp/ut/indexes/json/common/kqp_indexes_json_ut_common.h>

namespace NKikimr::NKqp {

using namespace NYdb::NQuery;
using namespace NYdb;

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

}  // namespace NKikimr::NKqp
