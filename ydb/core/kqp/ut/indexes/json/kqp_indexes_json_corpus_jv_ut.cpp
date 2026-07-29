#include <ydb/core/kqp/ut/indexes/json/common/kqp_indexes_json_ut_common.h>

namespace NKikimr::NKqp {

using namespace NYdb::NQuery;
using namespace NYdb;

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

}  // namespace NKikimr::NKqp
