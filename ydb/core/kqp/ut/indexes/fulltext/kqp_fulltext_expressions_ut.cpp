#include "kqp_fulltext_ut_common.h"

namespace NKikimr::NKqp {
namespace {

struct TExpressionTestOptions {
    bool Compact = false;
    bool Utf8 = true;
    bool NotNull = false;
    bool Prefixed = false;
    bool FilterScope = false;
    bool Ngram = false;
    bool Relevance = false;
    bool Empty = false;
};

TKikimrRunner ExpressionKikimr(bool compact) {
    NKikimrConfig::TFeatureFlags flags;
    flags.SetEnableFulltextIndex(true);
    flags.SetEnableCompactFulltextIndex(compact);
    flags.SetEnableFulltextIndexPrefix(true);
    return Kikimr(std::move(flags));
}

struct TExpressionFixture {
    const TExpressionTestOptions Options;
    TKikimrRunner Runner;
    NYdb::NQuery::TQueryClient Db;

    explicit TExpressionFixture(TExpressionTestOptions options)
        : Options(options)
        , Runner(ExpressionKikimr(options.Compact))
        , Db(Runner.GetQueryClient())
    {
        Execute(Sprintf(R"sql(
            CREATE TABLE `/Root/Docs` (
                Id Uint64 NOT NULL,
                Scope Utf8,
                Text %s %s,
                OtherText %s,
                PRIMARY KEY (Id),
                INDEX fulltext_idx GLOBAL USING %s ON (%sText)
                WITH (tokenizer=standard, use_filter_lowercase=true%s)
            );
            CREATE TABLE `/Root/Patterns` (
                Id Uint64 NOT NULL,
                Pattern %s,
                PRIMARY KEY (Id),
                INDEX pattern_idx GLOBAL ON (Pattern)
            );
            CREATE TABLE `/Root/Choices` (
                Id Uint64 NOT NULL,
                PatternId Uint64,
                PRIMARY KEY (Id)
            );
        )sql", Type(), Options.NotNull ? "NOT NULL" : "", Type(),
            Options.Relevance ? "fulltext_relevance" : "fulltext_plain",
            Options.Prefixed ? "Scope, " : "",
            Options.Ngram ? ", use_filter_ngram=true, filter_ngram_min_length=2, filter_ngram_max_length=3" : "",
            Type()));

        Execute(R"sql(
            UPSERT INTO `/Root/Patterns` (Id, Pattern) VALUES
                (1ul, "alpha"), (2ul, "beta"), (3ul, "absent"), (4ul, ""), (5ul, NULL);
            UPSERT INTO `/Root/Choices` (Id, PatternId) VALUES (1ul, 1ul), (2ul, 2ul);
        )sql");
        if (!Options.Empty) {
            Execute(R"sql(
                UPSERT INTO `/Root/Docs` (Id, Scope, Text, OtherText) VALUES
                    (1ul, "p1"u, "alpha", "beta"),
                    (2ul, "p1"u, "beta", "alpha"),
                    (3ul, "p2"u, "alpha", "beta"),
                    (4ul, "p1"u, "alphabet", "alpha"),
                    (5ul, "p1"u, "alpha beta", "alpha"),
                    (6ul, "p1"u, "", "alpha"),
                    (8ul, "p1"u, "alp lph pha", "alpha"),
                    (9ul, "p1"u, "Альфа", "beta");
            )sql");
            if (!Options.NotNull) {
                Execute(R"sql(UPSERT INTO `/Root/Docs` (Id, Scope, Text) VALUES (7ul, "p1"u, NULL);)sql");
            }
        }
    }

    const char* Type() const {
        return Options.Utf8 ? "Utf8" : "String";
    }

    TString Declarations() const {
        return Sprintf(R"sql(
            DECLARE $q AS %s;
            DECLARE $other AS %s;
            DECLARE $choose AS Bool;
            DECLARE $lo AS Uint64;
            DECLARE $hi AS Uint64;
            DECLARE $scope AS Utf8;
        )sql", Type(), Type());
    }

    NYdb::TParams Parameters(const TString& q = "alpha", const TString& other = "beta",
        bool choose = true, ui64 lo = 1, ui64 hi = 2, const TString& scope = "p1") const
    {
        NYdb::TParamsBuilder builder;
        if (Options.Utf8) {
            builder.AddParam("$q").Utf8(q).Build();
            builder.AddParam("$other").Utf8(other).Build();
        } else {
            builder.AddParam("$q").String(q).Build();
            builder.AddParam("$other").String(other).Build();
        }
        return builder
            .AddParam("$choose").Bool(choose).Build()
            .AddParam("$lo").Uint64(lo).Build()
            .AddParam("$hi").Uint64(hi).Build()
            .AddParam("$scope").Utf8(scope).Build()
            .Build();
    }

    TString Select(const TString& expression, bool score = false, const TString& namedArgs = "",
        const TString& extraPredicate = "") const
    {
        const TString fulltext = Sprintf("%s(Text, %s%s)",
            score ? "FulltextScore" : "FulltextMatch", expression.c_str(), namedArgs.c_str());
        return Sprintf(R"sql(
            SELECT Id%s FROM `/Root/Docs` VIEW fulltext_idx
            WHERE %s%s%s%s ORDER BY Id;
        )sql", score ? (", " + fulltext + " AS Score").c_str() : "",
            Options.Prefixed || Options.FilterScope ? "Scope = $scope AND " : "", fulltext.c_str(),
            score ? " > 0" : "", extraPredicate.empty() ? "" : (" AND (" + extraPredicate + ")").c_str());
    }

    TString Query(const TString& expression, const TString& setup = "", bool score = false,
        const TString& namedArgs = "", const TString& extraPredicate = "") const
    {
        return Declarations() + setup + Select(expression, score, namedArgs, extraPredicate);
    }

    void Execute(const TString& query) {
        const auto result = Db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, query << "\n" << result.GetIssues().ToString());
    }

    static void CheckIds(const NYdb::TResultSet& resultSet, const TVector<ui64>& expected, const TString& query) {
        NYdb::TResultSetParser parser(resultSet);
        TVector<ui64> actual;
        while (parser.TryNextRow()) {
            actual.push_back(parser.ColumnParser("Id").GetUint64());
            if (resultSet.ColumnsCount() == 2) {
                UNIT_ASSERT_C(parser.ColumnParser("Score").GetDouble() > 0, query);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL_C(actual, expected, query);
    }

    TString ExpectIds(const TString& query, const TVector<ui64>& expected, const NYdb::TParams& params) {
        const auto result = Db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), params).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, query << "\n" << result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetResultSets().size(), 1, query);
        CheckIds(result.GetResultSet(0), expected, query);
        return NYdb::FormatResultSetYson(result.GetResultSet(0));
    }

    TString ExpectIds(const TString& query, const TVector<ui64>& expected) {
        return ExpectIds(query, expected, Parameters());
    }

    NYdb::EStatus ExpectError(const TString& query, const TString& message, const NYdb::TParams& params) {
        const auto result = Db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), params).ExtractValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::BAD_REQUEST || result.GetStatus() == NYdb::EStatus::GENERIC_ERROR,
            query << "\n" << result.GetStatus() << "\n" << result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), message, query);
        return result.GetStatus();
    }

    NYdb::EStatus ExpectError(const TString& query, const TString& message) {
        return ExpectError(query, message, Parameters());
    }
};

} // namespace

Y_UNIT_TEST_SUITE(KqpFulltextExpressions) {

Y_UNIT_TEST_QUAD(ScalarExpressions, Compact, Utf8) {
    TExpressionFixture fixture({.Compact = Compact, .Utf8 = Utf8});
    const TVector<TString> expressions = {
        "$q",
        "IF($choose, $q, $other)",
        "CASE WHEN $choose THEN $q ELSE $other END",
        "CAST($q AS String) || SUBSTRING(CAST($other AS String), 0, 0)",
        "COALESCE(IF($choose, Just($q), Nothing(" + TString(fixture.Type()) + "?)), $other)",
        "Unwrap(ListHead(AsList($q, $other)))",
        "$record.pattern",
        "$tuple.0",
        "$identity($q)",
        "COALESCE(CAST(CAST($q AS String) AS Utf8), \"\"u)",
        "String::AsciiToLower(CAST($q AS String))",
    };
    const TString setup = R"sql(
        $record = AsStruct($q AS pattern);
        $tuple = AsTuple($q, $other);
        $identity = ($x) -> { RETURN CAST($x AS String) || SUBSTRING(CAST($other AS String), 0, 0); };
    )sql";
    for (const auto& expression : expressions) {
        fixture.ExpectIds(fixture.Query(expression, setup), {1, 3, 5});
        fixture.ExpectIds(fixture.Query(expression, setup), {}, fixture.Parameters("absent"));
    }
    fixture.ExpectIds(fixture.Query("IF($choose, $q, $other)"), {2, 5}, fixture.Parameters("alpha", "beta", false));
    fixture.ExpectIds(fixture.Query("CAST($lo AS String)"), {});
    // A nullable value becomes valid after an explicit default or unwrap.
    fixture.ExpectIds(fixture.Query("COALESCE(Nothing(String?), CAST($q AS String))"), {1, 3, 5});
    fixture.ExpectIds(fixture.Query("COALESCE(CAST($q AS Utf8), \"\"u)"), {1, 3, 5});
    fixture.ExpectIds(fixture.Query("IF($choose, $q, $other)"), {9}, fixture.Parameters("альфа"));
}

Y_UNIT_TEST_QUAD(SubqueryRanges, Compact, Utf8) {
    TExpressionFixture fixture({.Compact = Compact, .Utf8 = Utf8, .Prefixed = true});
    const TString setup = R"sql(
        $use_fixed = (SELECT COUNT(*) > 0 FROM `/Root/Patterns` WHERE Id >= $lo AND Id < $hi);
        $search = IF($use_fixed, $q, $other);
    )sql";
    const TString query = fixture.Query("$search", setup);
    fixture.ExpectIds(query, {1, 5});
    fixture.ExpectIds(query, {2, 5}, fixture.Parameters("alpha", "beta", true, 10, 20));
    fixture.ExpectIds(query, {2, 5}, fixture.Parameters("alpha", "beta", true, 1, 1));
    fixture.ExpectIds(query, {2, 5}, fixture.Parameters("alpha", "beta", true, 2, 1));
    fixture.ExpectIds(query, {}, fixture.Parameters("absent"));
    fixture.ExpectIds(query, {}, fixture.Parameters("alpha", "beta", true, 1, 2, "missing"));
    fixture.ExpectIds(query, {3}, fixture.Parameters("alpha", "beta", true, 1, 2, "p2"));
    fixture.ExpectIds(fixture.Query("$search", setup, false, "", "Id >= $hi AND Id < $lo"), {});

    const TString scalar = fixture.Query("COALESCE($search, $other)", R"sql(
        $search = (SELECT Pattern FROM `/Root/Patterns` WHERE Id = $lo);
    )sql");
    fixture.ExpectIds(scalar, {1, 5});
    fixture.ExpectIds(scalar, {2, 5}, fixture.Parameters("alpha", "beta", true, 20));
    fixture.ExpectIds(scalar, {2, 5}, fixture.Parameters("alpha", "beta", true, 5));
    fixture.ExpectIds(scalar, {}, fixture.Parameters("alpha", "beta", true, 3));
}

Y_UNIT_TEST_QUAD(DependentReads, Compact, Utf8) {
    TExpressionFixture fixture({.Compact = Compact, .Utf8 = Utf8, .Prefixed = true});
    const TVector<TString> setups = {
        R"sql(
            $id = (SELECT PatternId FROM `/Root/Choices` WHERE Id = $lo);
            $search = (SELECT Pattern FROM `/Root/Patterns` WHERE Id = $id);
        )sql",
        R"sql(
            $search = (SELECT p.Pattern FROM `/Root/Patterns` AS p
                INNER JOIN `/Root/Choices` AS c ON p.Id = c.PatternId WHERE c.Id = $lo);
        )sql",
        R"sql(
            $patterns = SELECT Id, Pattern FROM `/Root/Patterns` WHERE Id >= $lo AND Id < $hi;
            $search = (SELECT MIN(Pattern) FROM $patterns);
        )sql",
        R"sql(
            $present = (SELECT COUNT(*) > 0 FROM `/Root/Docs` WHERE Id = $lo);
            $first = (SELECT Pattern FROM `/Root/Patterns` WHERE Id = $lo);
            $second = (SELECT Pattern FROM `/Root/Patterns` WHERE Id = $hi);
            $search = IF($present, $first, $second);
        )sql",
    };
    for (const auto& setup : setups) {
        const TString query = fixture.Query("COALESCE($search, $other)", setup);
        fixture.ExpectIds(query, {1, 5});
        fixture.ExpectIds(query, {2, 5}, fixture.Parameters("alpha", "beta", true, 2, 3));
        fixture.ExpectIds(query, {2, 5}, fixture.Parameters("alpha", "beta", true, 20, 21));
    }

    // The same read is used by both the index query and an ordinary predicate.
    fixture.ExpectIds(fixture.Query("IF($count > 0, $q, $other)", R"sql(
        $count = (SELECT COUNT(*) FROM `/Root/Patterns` WHERE Id >= $lo AND Id < $hi);
    )sql", false, "", "Id >= $count"), {1, 5});

    const TString indexedRead = fixture.Query("COALESCE($search, $other)", R"sql(
        $search = (SELECT Pattern FROM `/Root/Patterns` VIEW pattern_idx WHERE Pattern = $q AND Id = $lo);
    )sql");
    fixture.ExpectIds(indexedRead, {1, 5});
    fixture.ExpectIds(indexedRead, {2, 5}, fixture.Parameters("beta", "absent", true, 2));
    fixture.ExpectIds(indexedRead, {}, fixture.Parameters("missing", "absent"));
}

Y_UNIT_TEST_QUAD(NestedFulltextReads, Compact, Score) {
    TExpressionFixture fixture({.Compact = Compact, .Prefixed = Compact || !Score, .FilterScope = true, .Relevance = Score});
    const TString query = fixture.Query("COALESCE($search, $other)", R"sql(
        $search = (SELECT OtherText FROM `/Root/Docs` VIEW fulltext_idx
            WHERE Scope = $scope AND FulltextMatch(Text, $q) AND Id = $lo);
    )sql", Score);
    fixture.ExpectIds(query, {2, 5});
    fixture.ExpectIds(query, {1, 5}, fixture.Parameters("beta", "absent", true, 2));
    fixture.ExpectIds(query, {}, fixture.Parameters("missing", "absent"));
}

Y_UNIT_TEST_QUAD(RowsetExpressions, Compact, Score) {
    for (bool utf8 : {false, true}) {
        TExpressionFixture fixture({.Compact = Compact, .Utf8 = utf8, .Prefixed = Compact || !Score, .FilterScope = true, .Relevance = Score});
        const TString setup = R"sql(
            $patterns = SELECT Pattern FROM `/Root/Patterns` WHERE Id >= $lo AND Id < $hi ORDER BY Id;
            $values = (SELECT AGGREGATE_LIST(CAST(Pattern AS String)) FROM $patterns);
            $joined = COALESCE(String::JoinFromList($values, " "), "");
            $search = IF(LENGTH($joined) > 0, $joined, CAST($other AS String));
        )sql";
        const TString query = fixture.Query("$search", setup, Score, ", \"and\" AS DefaultOperator");
        fixture.ExpectIds(query, {1, 5});
        fixture.ExpectIds(query, {5}, fixture.Parameters("alpha", "beta", true, 1, 3));
        fixture.ExpectIds(query, {2, 5}, fixture.Parameters("alpha", "beta", true, 2, 3));
        fixture.ExpectIds(query, {}, fixture.Parameters("alpha", "beta", true, 3, 4));
        // Empty and NULL strings, empty ranges, and reversed ranges all use the default.
        for (const auto& [lo, hi] : TVector<std::pair<ui64, ui64>>{{4, 5}, {5, 6}, {10, 20}, {1, 1}, {2, 1}}) {
            fixture.ExpectIds(query, {2, 5}, fixture.Parameters("alpha", "beta", true, lo, hi));
        }
        fixture.ExpectIds(query, {}, fixture.Parameters("alpha", "absent", true, 20, 21));

        // Both the fulltext source and a residual predicate consume the same rowset.
        fixture.ExpectIds(fixture.Query("$search", setup, Score, ", \"and\" AS DefaultOperator",
            "Id >= ListLength($values)"), {5}, fixture.Parameters("alpha", "beta", true, 1, 3));

        const TString shared = fixture.Declarations() + setup
            + "SELECT Pattern FROM $patterns ORDER BY Pattern;"
            + fixture.Select("$search", Score, ", \"and\" AS DefaultOperator");
        const auto result = fixture.Db.ExecuteQuery(shared, NYdb::NQuery::TTxControl::NoTx(),
            fixture.Parameters("alpha", "beta", true, 1, 3)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 2);
        CompareYson(R"([[["alpha"]];[["beta"]]])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
        TExpressionFixture::CheckIds(result.GetResultSet(1), {5}, shared);
    }
}

Y_UNIT_TEST_TWIN(MultipleSearchExpressions, Compact) {
    TExpressionFixture fixture({.Compact = Compact, .Prefixed = true});
    const TString query = fixture.Declarations() + R"sql(
        $first = COALESCE((SELECT Pattern FROM `/Root/Patterns` WHERE Id = $lo), $q);
        $second = COALESCE((SELECT Pattern FROM `/Root/Patterns` WHERE Id = $hi), $other);
        SELECT Id FROM `/Root/Docs` VIEW fulltext_idx
        WHERE Scope = $scope AND FulltextMatch(Text, $first) AND FulltextMatch(Text, $second)
        ORDER BY Id;
    )sql";
    fixture.ExpectIds(query, {5});
    fixture.ExpectIds(query, {1, 5}, fixture.Parameters("alpha", "beta", true, 1, 1));
    fixture.ExpectIds(query, {}, fixture.Parameters("absent", "beta", true, 20, 2));
}

Y_UNIT_TEST_QUAD(WildcardPostfilter, Compact, Utf8) {
    TExpressionFixture fixture({.Compact = Compact, .Utf8 = Utf8, .Prefixed = true, .Ngram = true});
    const TString setup = R"sql(
        $use_fixed = (SELECT COUNT(*) > 0 FROM `/Root/Docs` WHERE Id >= $lo AND Id < $hi);
        $search = IF($use_fixed, $q, $other);
    )sql";
    const TString query = fixture.Query("$search", setup, false, ", \"Wildcard\" AS Mode");
    // Id=8 has all the query trigrams in different words; the postfilter must reject it.
    fixture.ExpectIds(query, {1, 4, 5}, fixture.Parameters("%alpha%", "%beta%"));
    fixture.ExpectIds(query, {2, 5}, fixture.Parameters("%alpha%", "%beta%", true, 20, 21));
    fixture.ExpectIds(query, {}, fixture.Parameters("%absent%", "%beta%"));
    fixture.ExpectIds(query, {}, fixture.Parameters("%alpha%", "%beta%", true, 1, 2, "missing"));
    fixture.ExpectIds(query, {3}, fixture.Parameters("%alpha%", "%beta%", true, 1, 2, "p2"));
    fixture.ExpectIds(fixture.Query("CAST($q AS String) || CAST($other AS String)", "", false,
        ", \"Wildcard\" AS Mode"), {1, 4, 5}, fixture.Parameters("%alp", "ha%"));

    // LIKE shares the search-expression extraction and must keep its own postfilter.
    const TString like = fixture.Declarations() + setup + R"sql(
        SELECT Id FROM `/Root/Docs` VIEW fulltext_idx
        WHERE Scope = $scope AND Text LIKE $search ORDER BY Id;
    )sql";
    fixture.ExpectIds(like, {1, 4, 5}, fixture.Parameters("%alpha%", "%beta%"));
    fixture.ExpectIds(like, {2, 5}, fixture.Parameters("%alpha%", "%beta%", true, 20, 21));
    fixture.ExpectIds(query, {9}, fixture.Parameters("%альф%", "%beta%"));
}

Y_UNIT_TEST_QUAD(ImplicitStringPredicates, Compact, Utf8) {
    TExpressionFixture fixture({.Compact = Compact, .Utf8 = Utf8, .Prefixed = true, .Ngram = true});
    const TString setup = R"sql(
        $search = COALESCE((SELECT Pattern FROM `/Root/Patterns` WHERE Id = $lo), $other);
    )sql";
    const TVector<std::pair<TString, TVector<ui64>>> cases = {
        {"StartsWith(Text, $search)", {1, 4, 5}},
        {"EndsWith(Text, $search)", {1}},
        {"YQL::StringContains(Text, $search)", {1, 4, 5}},
        {"Text = $search", {1}},
    };
    for (const auto& [predicate, expected] : cases) {
        const TString query = fixture.Declarations() + setup
            + "SELECT Id FROM `/Root/Docs` VIEW fulltext_idx WHERE Scope = $scope AND " + predicate + " ORDER BY Id;";
        fixture.ExpectIds(query, expected);
        fixture.ExpectIds(query, {}, fixture.Parameters("alpha", "absent", true, 20));
    }
}

Y_UNIT_TEST_QUAD(ScoreExpressions, Compact, Utf8) {
    TExpressionFixture fixture({.Compact = Compact, .Utf8 = Utf8, .Relevance = true});
    const TString expected = fixture.ExpectIds(fixture.Query("\"alpha\"", "", true), {1, 3, 5});
    for (const TString& expression : {TString("IF($choose, $q, $other)"), TString("COALESCE($search, $other)")}) {
        const TString setup = "$search = (SELECT Pattern FROM `/Root/Patterns` WHERE Id = $lo);";
        const TString actual = fixture.ExpectIds(fixture.Query(expression, setup, true), {1, 3, 5});
        CompareYson(expected, actual);
    }
    const TString query = fixture.Query("IF($use_fixed, $q, $other)", R"sql(
        $use_fixed = (SELECT COUNT(*) > 0 FROM `/Root/Patterns` WHERE Id >= $lo AND Id < $hi);
    )sql", true);
    fixture.ExpectIds(query, {2, 5}, fixture.Parameters("alpha", "beta", true, 20, 21));
    fixture.ExpectIds(query, {}, fixture.Parameters("absent"));
    fixture.ExpectIds(query, {}, fixture.Parameters("alpha", "absent", true, 20, 21));
    fixture.ExpectIds(fixture.Query("IF($choose, $q, $other)", "", true,
        ", 0.75f AS B, 1.2 AS K1, \"or\" AS DefaultOperator, \"1\" AS MinimumShouldMatch"), {1, 3, 5});
}

Y_UNIT_TEST_QUAD(ExplicitPredicateKeepsStringFilters, Compact, Score) {
    TExpressionFixture fixture({.Compact = Compact, .Ngram = true, .Relevance = Score});
    const TVector<std::pair<TString, TVector<ui64>>> filters = {
        {"Text = IF($choose, $q, $other)", {1, 3}},
        {"StartsWith(Text, IF($choose, $q, $other))", {1, 3, 4, 5}},
        {"EndsWith(Text, IF($choose, $q, $other))", {1, 3}},
        {"Text LIKE IF($choose, $q, $other)", {1, 3}},
    };
    for (const auto& [filter, expected] : filters) {
        fixture.ExpectIds(fixture.Query("$q", "", Score, "", filter), expected);
        fixture.ExpectIds(fixture.Query("$q", "", Score, "", filter), {},
            fixture.Parameters("alpha", "absent", false));
    }
}

Y_UNIT_TEST_TWIN(WildcardExpressionEvaluatedOnce, Compact) {
    TExpressionFixture fixture({.Compact = Compact, .Ngram = true});
    const TString setup = R"sql(
        $search = IF(Random($choose) < 0.5, $q, $other);
    )sql";
    for (const TString& predicate : {TString("FulltextMatch(Text, $search, \"Wildcard\" AS Mode)"),
            TString("Text LIKE $search")}) {
        const TString query = fixture.Declarations() + setup
            + "SELECT Id FROM `/Root/Docs` VIEW fulltext_idx WHERE " + predicate + " ORDER BY Id;";
        for (ui32 iteration = 0; iteration < 16; ++iteration) {
            const auto result = fixture.Db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
                fixture.Parameters()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
            NYdb::TResultSetParser parser(result.GetResultSet(0));
            TVector<ui64> ids;
            while (parser.TryNextRow()) {
                ids.push_back(parser.ColumnParser("Id").GetUint64());
            }
            // Either scalar value must be used consistently by the index and the postfilter.
            UNIT_ASSERT_C(ids == TVector<ui64>({1, 3}) || ids == TVector<ui64>({2}),
                query << "\n" << NYdb::FormatResultSetYson(result.GetResultSet(0)));
        }
    }
}

Y_UNIT_TEST_QUAD(ColumnNullability, Utf8, NotNull) {
    TExpressionFixture fixture({.Utf8 = Utf8, .NotNull = NotNull});
    fixture.ExpectIds(fixture.Query("IF($choose, CAST($q AS String), CAST($other AS String))"), {1, 3, 5});
    fixture.ExpectIds(fixture.Query("COALESCE(IF($choose, CAST($q AS Utf8), CAST($other AS Utf8)), \"\"u)"), {1, 3, 5});
}

Y_UNIT_TEST_QUAD(EmptyTable, Compact, Score) {
    TExpressionFixture fixture({.Compact = Compact, .Prefixed = Compact || !Score, .FilterScope = true, .Relevance = Score, .Empty = true});
    const TString query = fixture.Query("IF($use_fixed, $q, $other)", R"sql(
        $use_fixed = (SELECT COUNT(*) > 0 FROM `/Root/Patterns` WHERE Id >= $lo AND Id < $hi);
    )sql", Score);
    fixture.ExpectIds(query, {});
    fixture.ExpectIds(query, {}, fixture.Parameters("alpha", "beta", true, 20, 21));
}

Y_UNIT_TEST_QUAD(EmptySearchExpression, Compact, Score) {
    TExpressionFixture fixture({.Compact = Compact, .Relevance = Score});
    const TString query = fixture.Query("IF($choose, $q, $other)", "", Score);
    UNIT_ASSERT_VALUES_EQUAL(fixture.ExpectError(query, "Expected non-empty query", fixture.Parameters("")),
        NYdb::EStatus::BAD_REQUEST);
    UNIT_ASSERT_VALUES_EQUAL(fixture.ExpectError(query, "No search terms were extracted", fixture.Parameters("   ")),
        NYdb::EStatus::BAD_REQUEST);
    const TString fromRead = fixture.Query("COALESCE($search, $other)", R"sql(
        $search = (SELECT Pattern FROM `/Root/Patterns` WHERE Id = $lo);
    )sql", Score);
    UNIT_ASSERT_VALUES_EQUAL(fixture.ExpectError(fromRead, "Expected non-empty query", fixture.Parameters("alpha", "beta", true, 4)),
        NYdb::EStatus::BAD_REQUEST);
    UNIT_ASSERT_VALUES_EQUAL(fixture.ExpectError(fromRead, "Expected non-empty query", fixture.Parameters("alpha", "", true, 20)),
        NYdb::EStatus::BAD_REQUEST);
}

Y_UNIT_TEST_QUAD(CachedQueriesObserveParametersAndReads, Compact, Score) {
    TExpressionFixture fixture({.Compact = Compact, .Prefixed = Compact || !Score, .FilterScope = true, .Relevance = Score});
    const TString query = fixture.Query("COALESCE($search, $other)", R"sql(
        $search = (SELECT Pattern FROM `/Root/Patterns` WHERE Id = $lo);
    )sql", Score);
    fixture.ExpectIds(query, {1, 5});
    fixture.ExpectIds(query, {2, 5}, fixture.Parameters("alpha", "beta", true, 2));
    fixture.ExpectIds(query, {1, 5});
    fixture.Execute(R"sql(UPSERT INTO `/Root/Patterns` (Id, Pattern) VALUES (1ul, "beta");)sql");
    fixture.ExpectIds(query, {2, 5});
    fixture.Execute(R"sql(DELETE FROM `/Root/Patterns` WHERE Id = 1ul;)sql");
    fixture.ExpectIds(query, {}, fixture.Parameters("alpha", "absent"));
    fixture.ExpectIds(query, {1, 5}, fixture.Parameters("beta", "alpha"));

    const TString pureQuery = fixture.Query("IF($choose, $q, $other)", "", Score);
    fixture.ExpectIds(pureQuery, {1, 5});
    fixture.ExpectIds(pureQuery, {2, 5}, fixture.Parameters("alpha", "beta", false));
    fixture.ExpectIds(pureQuery, {}, fixture.Parameters("absent"));
    fixture.ExpectIds(pureQuery, {1, 5});
}

Y_UNIT_TEST_QUAD(SharedReadAndMultipleResults, Compact, Score) {
    TExpressionFixture fixture({.Compact = Compact, .Prefixed = Compact || !Score, .FilterScope = true, .Relevance = Score});
    const TString query = fixture.Declarations() + R"sql(
        $search = COALESCE((SELECT Pattern FROM `/Root/Patterns` WHERE Id = $lo), $other);
        SELECT $search AS Pattern;
    )sql" + fixture.Select("$search", Score) + fixture.Select("$search", Score, "", "Id = 1ul");
    const auto result = fixture.Db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), fixture.Parameters()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
    UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 3);
    CompareYson(R"([["alpha"]])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    TExpressionFixture::CheckIds(result.GetResultSet(1), {1, 5}, query);
    TExpressionFixture::CheckIds(result.GetResultSet(2), {1}, query);
}

Y_UNIT_TEST_QUAD(InvalidSearchTypes, Utf8, Score) {
    TExpressionFixture fixture({.Utf8 = Utf8, .Relevance = Score});
    // Each expression has a valid YQL type, but not the required non-optional String/Utf8 type.
    const TVector<TString> invalid = {
        "NULL", "$choose", "$lo", "CAST($lo AS Int8)", "CAST($lo AS Uint8)",
        "CAST($lo AS Int16)", "CAST($lo AS Uint16)", "CAST($lo AS Int32)",
        "CAST($lo AS Uint32)", "CAST($lo AS Int64)", "CAST($lo AS Float)",
        "CAST($lo AS Double)", "CAST($lo AS Decimal(22, 9))",
        "Date(\"2020-01-01\")", "Datetime(\"2020-01-01T00:00:00Z\")",
        "Timestamp(\"2020-01-01T00:00:00Z\")", "Interval(\"P1D\")",
        "Uuid(\"01234567-89ab-cdef-0123-456789abcdef\")", "Json(\"{}\")", "Yson(\"{}\")",
        "Just($q)", "Just(Just($q))", "Nothing(String?)", "Nothing(Utf8?)",
        "AsList($q)", "AsTuple($q, $other)", "AsStruct($q AS pattern)",
        "AsDict(AsTuple($q, $other))", "[]", "{}", "AsTagged($q, \"pattern\")",
        "AsVariant($q, \"text\")", "AsEnum(\"alpha\")",
        "(SELECT Pattern FROM `/Root/Patterns` WHERE Id = $lo)",
    };
    for (const auto& expression : invalid) {
        fixture.ExpectError(fixture.Query(expression, "", Score), "Search string must be String or Utf8");
    }
}

Y_UNIT_TEST_QUAD(QueryParameterTypes, Utf8, Score) {
    TExpressionFixture fixture({.Utf8 = Utf8, .Relevance = Score});
    for (bool parameterUtf8 : {false, true}) {
        const TString type = parameterUtf8 ? "Utf8" : "String";
        const TString query = "DECLARE $value AS " + type + ";" + fixture.Select("$value", Score);
        NYdb::TParamsBuilder builder;
        if (parameterUtf8) {
            builder.AddParam("$value").Utf8("alpha").Build();
        } else {
            builder.AddParam("$value").String("alpha").Build();
        }
        fixture.ExpectIds(query, {1, 3, 5}, builder.Build());

        for (bool present : {false, true}) {
            const std::optional<std::string> value = present ? std::make_optional<std::string>("alpha") : std::nullopt;
            NYdb::TParamsBuilder optionalBuilder;
            if (parameterUtf8) {
                optionalBuilder.AddParam("$value").OptionalUtf8(value).Build();
            } else {
                optionalBuilder.AddParam("$value").OptionalString(value).Build();
            }
            const auto params = optionalBuilder.Build();
            const TString declarations = "DECLARE $value AS " + type + "?;";
            fixture.ExpectError(declarations + fixture.Select("$value", Score),
                "Search string must be String or Utf8", params);
            fixture.ExpectIds(declarations + fixture.Select("COALESCE($value, \"beta\")", Score),
                present ? TVector<ui64>{1, 3, 5} : TVector<ui64>{2, 5}, params);
        }
    }

    const TString query = "DECLARE $filter AS Struct<pattern: String, choose: Bool>;"
        + fixture.Select("IF($filter.choose, $filter.pattern, \"beta\")", Score);
    for (bool choose : {true, false}) {
        const auto params = NYdb::TParamsBuilder().AddParam("$filter").BeginStruct()
            .AddMember("pattern").String("alpha")
            .AddMember("choose").Bool(choose)
            .EndStruct().Build().Build();
        fixture.ExpectIds(query, choose ? TVector<ui64>{1, 3, 5} : TVector<ui64>{2, 5}, params);
    }
}

Y_UNIT_TEST_TWIN(InvalidTextTypesAndColumns, Score) {
    TExpressionFixture fixture({.Relevance = Score});
    const TVector<TString> invalidColumns = {
        "Id", "$choose", "NULL", "Just(Id)", "CAST(Id AS Int64)", "CAST(Id AS Float)",
        "CAST(Id AS Double)", "CAST(Id AS Decimal(22, 9))", "Date(\"2020-01-01\")",
        "Datetime(\"2020-01-01T00:00:00Z\")", "Timestamp(\"2020-01-01T00:00:00Z\")",
        "Interval(\"P1D\")", "Uuid(\"01234567-89ab-cdef-0123-456789abcdef\")",
        "Json(\"{}\")", "Yson(\"{}\")", "AsList($q)", "AsStruct($q AS pattern)",
        "AsTuple($q, $other)", "AsDict(AsTuple($q, $other))", "AsTagged($q, \"text\")",
        "AsVariant($q, \"text\")", "AsEnum(\"alpha\")",
    };
    for (const auto& column : invalidColumns) {
        const TString call = Sprintf("%s(%s, IF($choose, $q, $other), \"or\" AS DefaultOperator)",
            Score ? "FulltextScore" : "FulltextMatch", column.c_str());
        fixture.ExpectError(fixture.Declarations() + "SELECT Id FROM `/Root/Docs` VIEW fulltext_idx WHERE "
            + call + (Score ? " > 0;" : ";"), "Expected String or Utf8");
    }
    // Correct type alone is insufficient: the first argument must identify the indexed column.
    for (const TString& column : {TString("OtherText"), TString("$q"), TString("Text || $q")}) {
        const TString call = Sprintf("%s(%s, $q)", Score ? "FulltextScore" : "FulltextMatch", column.c_str());
        fixture.ExpectError(fixture.Declarations() + "SELECT Id FROM `/Root/Docs` VIEW fulltext_idx WHERE "
            + call + (Score ? " > 0;" : ";"), "Unsupported index access");
    }
}

Y_UNIT_TEST_TWIN(RowDependentSearchRejected, Score) {
    TExpressionFixture fixture({.Relevance = Score});
    for (const TString& expression : {TString("COALESCE(OtherText, $q)"),
            TString("IF(Id = $lo, $q, $other)"), TString("CAST(Id AS String)"),
            TString("$q || COALESCE(Text, $other)")}) {
        fixture.ExpectError(fixture.Query(expression, "", Score), "search expression must not depend on the table row");
    }
    // A residual row-dependent string comparison must not invalidate the usable fulltext predicate.
    fixture.ExpectIds(fixture.Query("IF($choose, $q, $other)", "", Score, "", "Text != OtherText"), {1, 3, 5});
    fixture.ExpectIds(fixture.Query("IF($choose, $q, $other)", "", Score, "", "Text = OtherText"), {});
}

Y_UNIT_TEST_TWIN(InvalidPredicatePlacement, Score) {
    TExpressionFixture fixture({.Relevance = Score});
    const TString call = Score ? "FulltextScore(Text, IF($choose, $q, $other)) > 0"
                              : "FulltextMatch(Text, IF($choose, $q, $other))";
    for (const TString& predicate : {"(" + call + ") OR Id = $lo", "NOT (" + call + ")"}) {
        fixture.ExpectError(fixture.Declarations() + "SELECT Id FROM `/Root/Docs` VIEW fulltext_idx WHERE "
            + predicate + ";", "not reachable by conjunctions");
    }
    if (!Score) {
        fixture.ExpectError(fixture.Declarations() + R"sql(
            SELECT Id FROM `/Root/Docs` VIEW fulltext_idx
            WHERE IF($choose, FulltextMatch(Text, $q), FulltextMatch(Text, $other));
        )sql", "not reachable by conjunctions");
    }
}

Y_UNIT_TEST_TWIN(NamedArgumentsWithExpression, Score) {
    TExpressionFixture fixture({.Relevance = Score});
    const TString expression = "IF($choose, $q, $other)";
    fixture.ExpectIds(fixture.Query(expression, "", Score,
        ", Just(0.75f) AS B, Just(1.2) AS K1, Just(\"or\"u) AS DefaultOperator, Just(\"1\"u) AS MinimumShouldMatch"),
        {1, 2, 3, 5}, fixture.Parameters("alpha beta"));

    for (bool floatB : {false, true}) {
        for (bool utf8Options : {false, true}) {
            const TString declarations = Sprintf(R"sql(
                DECLARE $words AS String;
                DECLARE $choose AS Bool;
                DECLARE $b AS %s;
                DECLARE $k1 AS %s;
                DECLARE $operator AS %s;
                DECLARE $minimum AS %s;
            )sql", floatB ? "Float" : "Double", floatB ? "Double" : "Float",
                utf8Options ? "Utf8" : "String", utf8Options ? "Utf8" : "String");
            const TString query = declarations + fixture.Select("IF($choose, $words, \"absent\")", Score,
                ", $b AS B, $k1 AS K1, $operator AS DefaultOperator, $minimum AS MinimumShouldMatch");
            for (bool choose : {false, true}) {
                NYdb::TParamsBuilder builder;
                builder.AddParam("$words").String("alpha beta").Build();
                builder.AddParam("$choose").Bool(choose).Build();
                if (floatB) {
                    builder.AddParam("$b").Float(0.75f).Build();
                    builder.AddParam("$k1").Double(1.2).Build();
                } else {
                    builder.AddParam("$b").Double(0.75).Build();
                    builder.AddParam("$k1").Float(1.2f).Build();
                }
                if (utf8Options) {
                    builder.AddParam("$operator").Utf8("or").Build();
                    builder.AddParam("$minimum").Utf8("1").Build();
                } else {
                    builder.AddParam("$operator").String("or").Build();
                    builder.AddParam("$minimum").String("1").Build();
                }
                fixture.ExpectIds(query, choose ? TVector<ui64>{1, 2, 3, 5} : TVector<ui64>{}, builder.Build());
            }
        }
    }
}

Y_UNIT_TEST_TWIN(InvalidNamedArgumentTypes, Score) {
    TExpressionFixture fixture({.Ngram = !Score, .Relevance = Score});
    const TVector<TString> invalidValues = {"$lo", "$choose", "AsList($q)", "AsStruct($q AS pattern)", "NULL"};
    for (const auto& value : invalidValues) {
        for (const TString& option : {TString("B"), TString("K1"), TString("DefaultOperator"), TString("MinimumShouldMatch")}) {
            fixture.ExpectError(fixture.Query("IF($choose, $q, $other)", "", Score,
                ", " + value + " AS " + option), "Unsupported index access");
        }
        fixture.ExpectError(fixture.Query("IF($choose, $q, $other)", "", Score,
            ", " + value + " AS Mode"), "Mode must be a String literal");
    }
    for (const TString& value : {TString("$q"), TString("\"Wildcard\"u"), TString("Just(\"Wildcard\")"), TString("0.5")}) {
        fixture.ExpectError(fixture.Query("IF($choose, $q, $other)", "", Score,
            ", " + value + " AS Mode"), "Mode must be a String literal");
    }
    for (const TString& value : {TString("$q"), TString("Just($q)"), TString("AsTuple($q, $other)")}) {
        for (const TString& option : {TString("B"), TString("K1")}) {
            fixture.ExpectError(fixture.Query("IF($choose, $q, $other)", "", Score,
                ", " + value + " AS " + option), "must be a Float or Double literal or parameter");
        }
    }
}

} // Y_UNIT_TEST_SUITE(KqpFulltextExpressions)
} // namespace NKikimr::NKqp
