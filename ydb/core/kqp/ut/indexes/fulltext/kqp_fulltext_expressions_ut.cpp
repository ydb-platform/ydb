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
    bool EdgeNgram = false;
    bool Relevance = false;
    bool Empty = false;
    const char* Tokenizer = "standard";
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
        const TString ngramSettings = Options.Ngram || Options.EdgeNgram
            ? Sprintf(", use_filter_ngram=%s, use_filter_edge_ngram=%s, filter_ngram_min_length=2, filter_ngram_max_length=3",
                Options.Ngram ? "true" : "false", Options.EdgeNgram ? "true" : "false")
            : "";
        Execute(Sprintf(R"sql(
            CREATE TABLE `/Root/Docs` (
                Id Uint64 NOT NULL,
                Scope Utf8,
                Text %s %s,
                OtherText %s,
                PRIMARY KEY (Id),
                INDEX fulltext_idx GLOBAL USING %s ON (%sText)
                WITH (tokenizer=%s, use_filter_lowercase=true%s)
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
            Options.Tokenizer,
            ngramSettings.c_str(),
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

    TString ExpectIds(const TString& query, const TVector<ui64>& expected, const NYdb::TParams& params,
        std::optional<bool> fromCache = std::nullopt)
    {
        NYdb::NQuery::TExecuteQuerySettings settings;
        if (fromCache) {
            settings.StatsMode(NYdb::NQuery::EStatsMode::Basic);
        }
        const auto result = Db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), params, settings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, query << "\n" << result.GetIssues().ToString());
        if (fromCache) {
            UNIT_ASSERT(result.GetStats());
            const auto compilation = result.GetStats()->GetCompilation();
            UNIT_ASSERT(compilation);
            UNIT_ASSERT_VALUES_EQUAL_C(compilation->IsFromCache(), *fromCache, query);
        }
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetResultSets().size(), 1, query);
        CheckIds(result.GetResultSet(0), expected, query);
        return NYdb::FormatResultSetYson(result.GetResultSet(0));
    }

    TString ExpectIds(const TString& query, const TVector<ui64>& expected, std::optional<bool> fromCache = std::nullopt) {
        return ExpectIds(query, expected, Parameters(), fromCache);
    }

    NYdb::EStatus ExpectError(const TString& query, const TString& message, const NYdb::TParams& params,
        NYdb::EStatus expectedStatus = NYdb::EStatus::BAD_REQUEST)
    {
        const auto result = Db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), params).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), expectedStatus,
            query << "\n" << result.GetStatus() << "\n" << result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), message, query);
        return result.GetStatus();
    }

    NYdb::EStatus ExpectError(const TString& query, const TString& message,
        NYdb::EStatus expectedStatus = NYdb::EStatus::BAD_REQUEST)
    {
        return ExpectError(query, message, Parameters(), expectedStatus);
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

Y_UNIT_TEST_QUAD(SharedRowsetFeedsAggregateAndJoin, Compact, Score) {
    TExpressionFixture fixture({.Compact = Compact, .Prefixed = Compact || !Score, .FilterScope = true, .Relevance = Score});
    // Like JoinWithPrecompute, the same rowset supplies an aggregate and a join.
    // Losing either dependency changes the query text or the residual bound.
    const TString query = fixture.Query("$search", R"sql(
        $data = SELECT Id, Pattern FROM `/Root/Patterns` WHERE Id >= $lo AND Id < $hi;
        $max = (SELECT MAX(Id) FROM $data);
        $joined = SELECT p.Pattern AS Pattern FROM $data AS p
            INNER JOIN `/Root/Choices` AS c ON p.Id = c.PatternId;
        $search = COALESCE((SELECT MIN(Pattern) FROM $joined), $other);
    )sql", Score, "", "Id >= COALESCE($max, 0ul)");
    fixture.ExpectIds(query, {5}, fixture.Parameters("absent", "beta", true, 1, 3));
    fixture.ExpectIds(query, {1, 5});
    fixture.ExpectIds(query, {2, 5}, fixture.Parameters("absent", "beta", true, 2, 3));
    fixture.ExpectIds(query, {5}, fixture.Parameters("absent", "beta", true, 3, 4));
    fixture.ExpectIds(query, {2, 5}, fixture.Parameters("absent", "beta", true, 20, 21));
}

Y_UNIT_TEST_QUAD(FulltextRowsetFeedsJoinAndPrecompute, Compact, Score) {
    TExpressionFixture fixture({.Compact = Compact, .Prefixed = Compact || !Score, .FilterScope = true, .Relevance = Score});
    // FlatMapLambdaInnerPrecompute also compares counts of a shared source and
    // its joined result. Here the shared source itself has a fulltext precompute.
    const TString query = fixture.Declarations() + R"sql(
        $search = COALESCE((SELECT Pattern FROM `/Root/Patterns` WHERE Id = $lo), $other);
        $rows = SELECT Id FROM `/Root/Docs` VIEW fulltext_idx
            WHERE Scope = $scope AND
    )sql" + (Score ? "FulltextScore(Text, $search) > 0;" : "FulltextMatch(Text, $search);") + R"sql(
        $cnt = (SELECT COUNT(*) FROM $rows);
        $joined = SELECT r.Id AS Id FROM $rows AS r
            LEFT JOIN `/Root/Choices` AS c ON r.Id = c.PatternId;
        $check = (SELECT COUNT(*) FROM $joined);
        SELECT Id FROM $rows WHERE $check = $cnt ORDER BY Id;
    )sql";
    fixture.ExpectIds(query, {1, 5});
    fixture.ExpectIds(query, {2, 5}, fixture.Parameters("alpha", "beta", true, 2));
    fixture.ExpectIds(query, {}, fixture.Parameters("alpha", "beta", true, 3));
    fixture.ExpectIds(query, {2, 5}, fixture.Parameters("alpha", "beta", true, 20));
    // Duplicate the non-key join value: removing the join or its count would now
    // incorrectly retain the fulltext rows.
    fixture.Execute("UPSERT INTO `/Root/Choices` (Id, PatternId) VALUES (3ul, 1ul);");
    fixture.ExpectIds(query, {});
}

Y_UNIT_TEST_QUAD(FulltextPrecomputeFeedsLookupKey, Compact, Score) {
    TExpressionFixture fixture({.Compact = Compact, .Relevance = Score});
    const TString query = fixture.Declarations() + R"sql(
        $search = COALESCE((SELECT Pattern FROM `/Root/Patterns` WHERE Id = $lo), $other);
        $key = (SELECT MIN(Id) FROM `/Root/Docs` VIEW fulltext_idx WHERE
    )sql" + (Score ? "FulltextScore(Text, $search) > 0);" : "FulltextMatch(Text, $search));") + R"sql(
        SELECT Id FROM `/Root/Choices` WHERE Id = $key ORDER BY Id;
    )sql";
    fixture.ExpectIds(query, {1});
    fixture.ExpectIds(query, {2}, fixture.Parameters("alpha", "beta", true, 2));
    fixture.ExpectIds(query, {}, fixture.Parameters("alpha", "beta", true, 3));
    fixture.ExpectIds(query, {2}, fixture.Parameters("alpha", "beta", true, 20));
}

Y_UNIT_TEST_QUAD(OrderedScalarSubquery, Compact, Score) {
    TExpressionFixture fixture({.Compact = Compact, .Prefixed = Compact || !Score, .FilterScope = true, .Relevance = Score});
    const TString query = fixture.Query("COALESCE($search, $other)", R"sql(
        $search = (SELECT Pattern FROM `/Root/Patterns`
            WHERE Id >= $lo AND Id < $hi ORDER BY Id DESC LIMIT 1);
    )sql", Score);
    fixture.ExpectIds(query, {2, 5}, fixture.Parameters("alpha", "beta", true, 1, 3));
    fixture.ExpectIds(query, {1, 5});
    fixture.ExpectIds(query, {}, fixture.Parameters("alpha", "beta", true, 3, 4));
    fixture.ExpectIds(query, {2, 5}, fixture.Parameters("alpha", "beta", true, 5, 6));
    fixture.ExpectIds(query, {2, 5}, fixture.Parameters("alpha", "beta", true, 20, 21));
    fixture.ExpectError(query, "Expected non-empty query", fixture.Parameters("alpha", "beta", true, 4, 5));
}

Y_UNIT_TEST_QUAD(PrecomputeExecutionModes, Compact, Score) {
    for (bool utf8 : {false, true}) {
        TExpressionFixture fixture({.Compact = Compact, .Utf8 = utf8, .Prefixed = Compact || !Score,
            .FilterScope = true, .Relevance = Score});
        auto tableClient = fixture.Runner.GetTableClient();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();
        const TVector<TString> expressions = {
            "\"alpha\"",
            "$q",
            "IF($choose, $q, $other)",
            "COALESCE((SELECT Pattern FROM `/Root/Patterns` WHERE Id = $lo), $other)",
        };
        for (const auto& expression : expressions) {
            const TString query = fixture.Query(expression, "", Score);
            const auto initial = fixture.Parameters();
            const auto changed = fixture.Parameters("beta", "beta", false, 2);
            const TVector<ui64> changedIds = expression == "\"alpha\""
                ? TVector<ui64>{1, 5} : TVector<ui64>{2, 5};
            const TString initialYson = fixture.ExpectIds(query, {1, 5}, initial);
            const TString changedYson = fixture.ExpectIds(query, changedIds, changed);

            for (const auto& txSettings : {NYdb::NQuery::TTxSettings::SerializableRW(), NYdb::NQuery::TTxSettings::SnapshotRO()}) {
                const auto result = fixture.Db.ExecuteQuery(query,
                    NYdb::NQuery::TTxControl::BeginTx(txSettings).CommitTx(), changed).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, query << result.GetIssues().ToString());
                TExpressionFixture::CheckIds(result.GetResultSet(0), changedIds, query);
                CompareYson(changedYson, NYdb::FormatResultSetYson(result.GetResultSet(0)), query);
            }

            // Execute the same prepared plan with different parameters, as in
            // ordinary data-query precompute/cache tests.
            const auto prepared = tableSession.PrepareDataQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(prepared.GetStatus(), NYdb::EStatus::SUCCESS, query << prepared.GetIssues().ToString());
            auto dataQuery = prepared.GetQuery();
            for (const auto& txSettings : {NYdb::NTable::TTxSettings::SerializableRW(), NYdb::NTable::TTxSettings::SnapshotRO()}) {
                const auto first = dataQuery.Execute(NYdb::NTable::TTxControl::BeginTx(txSettings).CommitTx(), initial).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(first.GetStatus(), NYdb::EStatus::SUCCESS, query << first.GetIssues().ToString());
                TExpressionFixture::CheckIds(first.GetResultSet(0), {1, 5}, query);
                CompareYson(initialYson, NYdb::FormatResultSetYson(first.GetResultSet(0)), query);
                const auto second = dataQuery.Execute(NYdb::NTable::TTxControl::BeginTx(txSettings).CommitTx(), changed).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(second.GetStatus(), NYdb::EStatus::SUCCESS, query << second.GetIssues().ToString());
                TExpressionFixture::CheckIds(second.GetResultSet(0), changedIds, query);
                CompareYson(changedYson, NYdb::FormatResultSetYson(second.GetResultSet(0)), query);
            }
            if (expression.StartsWith("COALESCE(")) {
                fixture.Execute("UPSERT INTO `/Root/Patterns` (Id, Pattern) VALUES (1ul, \"beta\");");
                const auto reread = dataQuery.Execute(NYdb::NTable::TTxControl::BeginTx().CommitTx(), initial).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(reread.GetStatus(), NYdb::EStatus::SUCCESS, query << reread.GetIssues().ToString());
                TExpressionFixture::CheckIds(reread.GetResultSet(0), {2, 5}, query);
                fixture.Execute("UPSERT INTO `/Root/Patterns` (Id, Pattern) VALUES (1ul, \"alpha\");");
            }

            // Scan executes pure and table-reading precompute phases separately.
            for (bool useChanged : {false, true}) {
                auto iterator = tableClient.StreamExecuteScanQuery(query, useChanged ? changed : initial).GetValueSync();
                UNIT_ASSERT_C(iterator.IsSuccess(), query << iterator.GetIssues().ToString());
                const auto result = CollectStreamResult(iterator);
                CompareYson(useChanged ? changedYson : initialYson, result.ResultSetYson, query);
            }
        }
    }
}

Y_UNIT_TEST_QUAD(FulltextPrecomputeFeedsWrite, Compact, Score) {
    TExpressionFixture fixture({.Compact = Compact, .Prefixed = Compact || !Score, .FilterScope = true, .Relevance = Score});
    auto tableClient = fixture.Runner.GetTableClient();
    auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();
    const TVector<TString> expressions = {
        "\"alpha\"",
        "$q",
        "COALESCE((SELECT Pattern FROM `/Root/Patterns` WHERE Id = $lo), $other)",
    };
    struct TCase {
        TString Term;
        ui64 PatternId;
        TVector<ui64> Expected;
    };
    const TVector<TCase> cases = {
        {"alpha", 1, {1, 5}}, {"beta", 2, {2, 5}},
        {"absent", 3, {}}, {"beta", 20, {2, 5}},
    };
    for (const auto& expression : expressions) {
        const TString predicate = Score ? "FulltextScore(Text, " + expression + ") > 0"
            : "FulltextMatch(Text, " + expression + ")";
        const TString query = fixture.Declarations() + R"sql(
            $hits = SELECT Id FROM `/Root/Docs` VIEW fulltext_idx
                WHERE Scope = $scope AND
        )sql" + predicate + R"sql(;
            UPSERT INTO `/Root/Choices` (Id, PatternId)
                SELECT Id, Id AS PatternId FROM $hits;
            SELECT Id FROM $hits ORDER BY Id;
            SELECT Id FROM `/Root/Choices` ORDER BY Id;
        )sql";
        for (const auto& testCase : cases) {
            const auto params = fixture.Parameters(testCase.Term, "beta", true, testCase.PatternId);
            const TVector<ui64> expected = expression == "\"alpha\"" ? TVector<ui64>{1, 5} : testCase.Expected;
            for (bool dataQuery : {false, true}) {
                fixture.Execute("DELETE FROM `/Root/Choices`;");
                if (dataQuery) {
                    const auto result = tableSession.ExecuteDataQuery(query,
                        NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx(), params).ExtractValueSync();
                    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, query << result.GetIssues().ToString());
                    UNIT_ASSERT_VALUES_EQUAL_C(result.GetResultSets().size(), 2, query);
                    TExpressionFixture::CheckIds(result.GetResultSet(0), expected, query);
                    TExpressionFixture::CheckIds(result.GetResultSet(1), expected, query);
                } else {
                    const auto result = fixture.Db.ExecuteQuery(query,
                        NYdb::NQuery::TTxControl::NoTx(), params).ExtractValueSync();
                    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, query << result.GetIssues().ToString());
                    UNIT_ASSERT_VALUES_EQUAL_C(result.GetResultSets().size(), 2, query);
                    TExpressionFixture::CheckIds(result.GetResultSet(0), expected, query);
                    TExpressionFixture::CheckIds(result.GetResultSet(1), expected, query);
                }
                // Check the committed sink independently of the query's shared rowset.
                fixture.ExpectIds("SELECT Id FROM `/Root/Choices` ORDER BY Id;", expected);
            }
        }
    }
}

Y_UNIT_TEST_QUAD(PrecomputeReadsTransactionLocalChanges, Compact, Score) {
    TExpressionFixture fixture({.Compact = Compact, .Prefixed = Compact || !Score, .FilterScope = true, .Relevance = Score});
    const TString query = fixture.Query("COALESCE($search, $other)", R"sql(
        $search = (SELECT Pattern FROM `/Root/Patterns` WHERE Id = $lo);
    )sql", Score);
    fixture.ExpectIds(query, {1, 5});
    auto session = fixture.Db.GetSession().GetValueSync().GetSession();
    const auto update = session.ExecuteQuery(
        "UPSERT INTO `/Root/Patterns` (Id, Pattern) VALUES (1ul, \"beta\");",
        NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::SerializableRW())).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(update.GetStatus(), NYdb::EStatus::SUCCESS, update.GetIssues().ToString());
    UNIT_ASSERT(update.GetTransaction());
    auto tx = *update.GetTransaction();
    const auto result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::Tx(tx), fixture.Parameters()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, query << result.GetIssues().ToString());
    TExpressionFixture::CheckIds(result.GetResultSet(0), {2, 5}, query);
    const auto rollback = tx.Rollback().ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(rollback.GetStatus(), NYdb::EStatus::SUCCESS, rollback.GetIssues().ToString());
    fixture.ExpectIds(query, {1, 5});
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

    const TString mixedEmpty = fixture.Declarations() + R"sql(
        $first = IF($choose, "", $q);
        SELECT Id FROM `/Root/Docs` VIEW fulltext_idx
        WHERE Scope = $scope AND FulltextMatch(Text, $first) AND FulltextMatch(Text, $other)
        ORDER BY Id;
    )sql";
    fixture.ExpectError(mixedEmpty, "Expected non-empty query");
}

Y_UNIT_TEST_TWIN(DependentSearchExpressionsSharePrecompute, Compact) {
    TExpressionFixture fixture({.Compact = Compact, .Prefixed = true});
    const TString setup = R"sql(
        $search = IF(Random(true) < 0.5, "alpha", "beta");
    )sql";
    const TVector<TString> predicates = {
        "FulltextMatch(Text, $search) AND FulltextMatch(Text, $search || \" beta\")",
        "FulltextMatch(Text, $search || \" beta\") AND FulltextMatch(Text, $search)",
    };
    for (const auto& predicate : predicates) {
        const TString query = fixture.Declarations() + setup
            + "SELECT Id, $search AS Search FROM `/Root/Docs` VIEW fulltext_idx "
              "WHERE Scope = $scope AND " + predicate + " ORDER BY Id;";
        for (ui32 iteration = 0; iteration < 32; ++iteration) {
            const auto result = fixture.Db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
                fixture.Parameters()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

            NYdb::TResultSetParser parser(result.GetResultSet(0));
            TVector<ui64> ids;
            TString search;
            while (parser.TryNextRow()) {
                ids.push_back(parser.ColumnParser("Id").GetUint64());
                const TString current(parser.ColumnParser("Search").GetString());
                if (search.empty()) {
                    search = current;
                }
                UNIT_ASSERT_VALUES_EQUAL(current, search);
            }
            UNIT_ASSERT_C(search == "alpha" || search == "beta", NYdb::FormatResultSetYson(result.GetResultSet(0)));
            const TVector<ui64> expected = search == "alpha" ? TVector<ui64>{5} : TVector<ui64>{2, 5};
            UNIT_ASSERT_VALUES_EQUAL_C(ids, expected, query << "\n" << NYdb::FormatResultSetYson(result.GetResultSet(0)));
        }
    }
}

Y_UNIT_TEST_QUAD(WildcardPostfilter, Compact, Utf8) {
    for (bool edge : {false, true}) {
        TExpressionFixture fixture({.Compact = Compact, .Utf8 = Utf8, .Prefixed = true, .Ngram = !edge, .EdgeNgram = edge});
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

        fixture.Execute(R"sql(
            UPSERT INTO `/Root/Docs` (Id, Scope, Text) VALUES
                (10ul, "p1"u, "alpha_beta"), (11ul, "p1"u, "alpha%beta");
            UPSERT INTO `/Root/Patterns` (Id, Pattern) VALUES
                (1ul, "%alpha!_beta%"), (2ul, "%alpha!%beta%");
        )sql");
        // Escaped wildcard characters must survive both pure and table-reading
        // query precomputes, including the LIKE postfilter's escape character.
        const TString escaped = fixture.Declarations() + R"sql(
            $search = IF($choose, $q, $other);
            SELECT Id FROM `/Root/Docs` VIEW fulltext_idx
            WHERE Scope = $scope AND Text LIKE $search ESCAPE '!' ORDER BY Id;
        )sql";
        fixture.ExpectIds(escaped, {10}, fixture.Parameters("%alpha!_beta%", "%alpha!%beta%"));
        fixture.ExpectIds(escaped, {11}, fixture.Parameters("%alpha!_beta%", "%alpha!%beta%", false));
        const TString escapedRead = fixture.Declarations() + R"sql(
            $search = COALESCE((SELECT Pattern FROM `/Root/Patterns` WHERE Id = $lo), $other);
            SELECT Id FROM `/Root/Docs` VIEW fulltext_idx
            WHERE Scope = $scope AND Text LIKE $search ESCAPE '!' ORDER BY Id;
        )sql";
        fixture.ExpectIds(escapedRead, {10}, false);
        fixture.ExpectIds(escapedRead, {11}, fixture.Parameters("alpha", "beta", true, 2), true);
        fixture.ExpectIds(escapedRead, {10}, fixture.Parameters("alpha", "%alpha!_beta%", true, 20), true);
    }
}

Y_UNIT_TEST_QUAD(CachedWildcardPrecomputeSwitchesBetweenIndexAndScan, Compact, Utf8) {
    for (bool edge : {false, true}) {
        TExpressionFixture fixture({.Compact = Compact, .Utf8 = Utf8, .Prefixed = true, .Ngram = !edge, .EdgeNgram = edge});
        const TString query = fixture.Query("$search", R"sql(
            $search = COALESCE((SELECT Pattern FROM `/Root/Patterns` WHERE Id = $lo), $other);
        )sql", false, ", \"Wildcard\" AS Mode");
        // Mirror the literal/parameter fallback cases with a table-reading precompute.
        // Reusing the plan must not retain either a previous pattern or its scan mode.
        const TVector<std::pair<TString, TVector<ui64>>> cases = {
            {"alpha", {1}},
            {"%alpha%", {1, 4, 5}},
            {"%a%", {1, 2, 4, 5, 8}},
            {"%", {1, 2, 4, 5, 6, 8, 9}},
            {"", {6}},
            {"%альф%", {9}},
            {"_eta", {2}},
            {"%absent%", {}},
            {"%alpha%", {1, 4, 5}},
        };
        bool first = true;
        for (const auto& [pattern, expected] : cases) {
            fixture.Execute(Sprintf("UPSERT INTO `/Root/Patterns` (Id, Pattern) VALUES (1ul, \"%s\");", pattern.c_str()));
            fixture.ExpectIds(query, expected, !first);
            first = false;
        }
        fixture.Execute("DELETE FROM `/Root/Patterns` WHERE Id = 1ul;");
        fixture.ExpectIds(query, {2}, true);
        fixture.Execute("UPSERT INTO `/Root/Patterns` (Id, Pattern) VALUES (1ul, NULL);");
        fixture.ExpectIds(query, {2}, true);
    }
}

Y_UNIT_TEST_QUAD(WildcardPrecomputeLimitFollowsPostfilter, Compact, Utf8) {
    TExpressionFixture fixture({.Compact = Compact, .Utf8 = Utf8, .Prefixed = true, .Ngram = true});
    fixture.Execute(R"sql(
        UPSERT INTO `/Root/Docs` (Id, Scope, Text) VALUES (0ul, "p1"u, "alp lph pha");
    )sql");
    // Id=0 is an earlier posting candidate containing every query ngram, but it
    // fails the wildcard. Applying LIMIT before the postfilter loses Id=1.
    const TString setup = fixture.Declarations() + R"sql(
        $pattern = (SELECT Pattern FROM `/Root/Patterns` WHERE Id = $lo);
        $search = "%" || CAST(COALESCE($pattern, $other) AS String) || "%";
        $limit = IF($choose, 1ul, 2ul);
    )sql";
    for (const TString& predicate : {TString("FulltextMatch(Text, $search, \"Wildcard\" AS Mode)"),
            TString("Text ILIKE $search")}) {
        const TString query = setup + "SELECT Id FROM `/Root/Docs` VIEW fulltext_idx WHERE Scope = $scope AND "
            + predicate + " ORDER BY Id LIMIT $limit;";
        fixture.ExpectIds(query, {1});
        fixture.ExpectIds(query, {1, 4}, fixture.Parameters("alpha", "beta", false));
        fixture.ExpectIds(query, {2}, fixture.Parameters("alpha", "beta", true, 2));
    }
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

Y_UNIT_TEST_QUAD(ScoreOptionsFollowComputedQuery, Compact, Utf8) {
    TExpressionFixture fixture({.Compact = Compact, .Utf8 = Utf8, .Relevance = true});
    const TString declarations = R"sql(
        DECLARE $value AS String;
        DECLARE $b AS Double;
        DECLARE $k1 AS Double;
    )sql";
    const TVector<TString> expressions = {
        "$value",
        "String::AsciiToLower($value)",
        "COALESCE(CAST((SELECT Pattern FROM `/Root/Patterns` WHERE Id = 1ul) AS String), $value)",
    };
    const TVector<std::pair<double, double>> factors = {{0.2, 0.5}, {0.8, 0.5}, {0.8, 2.0}};
    TString previous;
    for (ui32 index = 0; index < factors.size(); ++index) {
        const auto [b, k1] = factors[index];
        const auto params = NYdb::TParamsBuilder()
            .AddParam("$value").String("alpha").Build()
            .AddParam("$b").Double(b).Build()
            .AddParam("$k1").Double(k1).Build()
            .Build();
        const TString literal = fixture.ExpectIds(declarations + fixture.Select("\"alpha\"", true,
            Sprintf(", %.1f AS B, %.1f AS K1", b, k1)), {1, 3, 5}, params);
        // Change one factor at a time: comparing only matching IDs would miss
        // ignored score options or values retained by a cached source.
        UNIT_ASSERT_C(previous.empty() || previous != literal, literal);
        previous = literal;
        for (const auto& expression : expressions) {
            const TString query = declarations + fixture.Select(expression, true, ", $b AS B, $k1 AS K1");
            CompareYson(literal, fixture.ExpectIds(query, {1, 3, 5}, params, index != 0), query);
        }
    }
}

Y_UNIT_TEST_QUAD(PrecomputeLimitWithoutOrdering, Compact, Score) {
    TExpressionFixture fixture({.Compact = Compact, .Relevance = Score});
    for (const TString& limit : {TString("IF($choose, $hi, $lo)"),
            TString("COALESCE((SELECT COUNT(*) FROM `/Root/Patterns` WHERE Id <= $hi), 0ul)")}) {
        const TString query = fixture.Declarations() + R"sql(
            $search = COALESCE((SELECT Pattern FROM `/Root/Patterns` WHERE Id = $lo), $other);
            SELECT COUNT(*) AS Cnt FROM (
                SELECT Id FROM `/Root/Docs` VIEW fulltext_idx WHERE
        )sql" + (Score ? "FulltextScore(Text, $search) > 0" : "FulltextMatch(Text, $search)")
            + " LIMIT " + limit + ");";
        for (const auto& [patternId, matchingRows] : TVector<std::pair<ui64, ui64>>{{1, 3}, {2, 2}, {3, 0}}) {
            for (ui64 count : {0ul, 1ul, 2ul, 3ul}) {
                const auto params = fixture.Parameters("alpha", "absent", true, patternId, count);
                const auto result = fixture.Db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), params).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, query << result.GetIssues().ToString());
                NYdb::TResultSetParser parser(result.GetResultSet(0));
                UNIT_ASSERT(parser.TryNextRow());
                UNIT_ASSERT_VALUES_EQUAL_C(parser.ColumnParser("Cnt").GetUint64(),
                    count < matchingRows ? count : matchingRows, query);
                UNIT_ASSERT(!parser.TryNextRow());
            }
        }
    }
}

Y_UNIT_TEST_QUAD(RelevancePrecomputeOrderingAndLimits, Compact, Utf8) {
    TExpressionFixture fixture({.Compact = Compact, .Utf8 = Utf8, .Relevance = true});
    fixture.Execute(R"sql(
        UPSERT INTO `/Root/Docs` (Id, Text) VALUES
            (1ul, "alpha alpha filler"), (2ul, "beta beta filler"),
            (3ul, "alpha alpha alpha"), (5ul, "alpha beta filler");
    )sql");
    // Equal document lengths and distinct term frequencies give the strict
    // orders alpha: 3,1,5 and beta: 2,5, including inside pushed TopK.
    const auto query = [&](const TString& expression, const TString& suffix, const TString& predicate = "") {
        const TString score = "FulltextScore(Text, " + expression + ", 0.5 AS B)";
        return fixture.Declarations() + R"sql(
            $search = IF($choose, $q, $other);
            $limit = (SELECT COUNT(*) FROM `/Root/Patterns` WHERE Id < $hi);
        )sql" + "SELECT Id, " + score + " AS Score FROM `/Root/Docs` VIEW fulltext_idx WHERE "
            + (predicate.empty() ? score + " > 0" : "0 < " + score + " AND (" + predicate + ")")
            + " ORDER BY Score DESC" + suffix + ";";
    };
    struct TLimitCase {
        TString Suffix;
        TVector<ui64> Alpha;
        TVector<ui64> Beta;
    };
    const TVector<TLimitCase> limits = {
        {" LIMIT 1", {3}, {2}},
        {" LIMIT 1 OFFSET 1", {1}, {5}},
        {" LIMIT $hi", {3, 1}, {2, 5}},
        {" LIMIT $hi OFFSET $lo", {1, 5}, {}},
        {" LIMIT 1 OFFSET COALESCE($limit, 0ul)", {1}, {5}},
        {" LIMIT COALESCE($limit, 0ul) OFFSET 1", {1}, {5}},
        {" LIMIT COALESCE($limit, 0ul) OFFSET COALESCE($limit, 0ul)", {1}, {5}},
    };
    const TVector<TString> expressions = {
        "$q", "$search", "COALESCE((SELECT Pattern FROM `/Root/Patterns` WHERE Id = $lo), $other)",
    };
    for (const auto& [term, id] : TVector<std::pair<TString, ui64>>{{"alpha", 1}, {"beta", 2}, {"absent", 3}}) {
        const auto params = fixture.Parameters(term, "absent", true, id);
        for (const auto& limit : limits) {
            const TVector<ui64> expected = term == "alpha" ? limit.Alpha : term == "beta" ? limit.Beta : TVector<ui64>{};
            const TString literal = fixture.ExpectIds(query("\"" + term + "\"", limit.Suffix), expected, params);
            for (const auto& expression : expressions) {
                CompareYson(literal, fixture.ExpectIds(query(expression, limit.Suffix), expected, params));
            }
        }
    }
    fixture.ExpectIds(query("$search", " LIMIT $hi"), {2, 5}, fixture.Parameters("alpha", "beta", false));
    fixture.ExpectIds(query("$search", " LIMIT $hi"), {}, fixture.Parameters("alpha", "beta", true, 1, 0));
    fixture.ExpectIds(query("$search", " LIMIT COALESCE($limit, 0ul)"), {3, 1},
        fixture.Parameters("alpha", "beta", true, 1, 3));
    for (const auto& [predicate, expected] : TVector<std::pair<TString, TVector<ui64>>>{
            {"Id = 1ul", {1}}, {"Id = 3ul", {3}}, {"Id >= 2ul AND Id < 6ul", {3, 5}},
            {"Id >= 6ul AND Id < 2ul", {}}, {"Id >= CAST(LENGTH($search) AS Uint64)", {5}}}) {
        fixture.ExpectIds(query("$search", "", predicate), expected);
    }
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

Y_UNIT_TEST_TWIN(FallbackBoundsUseSharedExpression, Compact) {
    TExpressionFixture fixture({.Compact = Compact, .Ngram = true});
    const TString query = fixture.Declarations() + R"sql(
        $search = IF(Random($choose) < 0.5, "%", "%a%");
        SELECT Id, $search AS Search FROM `/Root/Docs` VIEW fulltext_idx
        WHERE Text LIKE $search AND Id >= CAST(LENGTH($search) AS Uint64)
        ORDER BY Id;
    )sql";
    // Neither pattern yields a two-character ngram: both must use the fallback scan.
    // Its PK bounds, residual predicate, and projection must use the same random value.
    for (ui32 iteration = 0; iteration < 32; ++iteration) {
        const auto result = fixture.Db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
            fixture.Parameters()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        NYdb::TResultSetParser parser(result.GetResultSet(0));
        TVector<ui64> ids;
        TString search;
        while (parser.TryNextRow()) {
            ids.push_back(parser.ColumnParser("Id").GetUint64());
            const TString current(parser.ColumnParser("Search").GetString());
            if (search.empty()) {
                search = current;
            }
            UNIT_ASSERT_VALUES_EQUAL(current, search);
        }
        UNIT_ASSERT_C(search == "%" || search == "%a%", search);
        const TVector<ui64> expected = search == "%"
            ? TVector<ui64>{1, 2, 3, 4, 5, 6, 8, 9}
            : TVector<ui64>{3, 4, 5, 8};
        UNIT_ASSERT_VALUES_EQUAL_C(ids, expected, search);
    }
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
    fixture.ExpectIds(query, {1, 5}, false);
    fixture.ExpectIds(query, {2, 5}, fixture.Parameters("alpha", "beta", true, 2), true);
    fixture.ExpectIds(query, {1, 5}, true);
    fixture.Execute(R"sql(UPSERT INTO `/Root/Patterns` (Id, Pattern) VALUES (1ul, "beta");)sql");
    fixture.ExpectIds(query, {2, 5}, true);
    fixture.Execute(R"sql(DELETE FROM `/Root/Patterns` WHERE Id = 1ul;)sql");
    fixture.ExpectIds(query, {}, fixture.Parameters("alpha", "absent"), true);
    fixture.ExpectIds(query, {1, 5}, fixture.Parameters("beta", "alpha"), true);

    const TString pureQuery = fixture.Query("IF($choose, $q, $other)", "", Score);
    fixture.ExpectIds(pureQuery, {1, 5}, false);
    fixture.ExpectIds(pureQuery, {2, 5}, fixture.Parameters("alpha", "beta", false), true);
    fixture.ExpectIds(pureQuery, {}, fixture.Parameters("absent"), true);
    fixture.ExpectIds(pureQuery, {1, 5}, true);
    fixture.ExpectError(pureQuery, "Expected non-empty query", fixture.Parameters(""));
    fixture.ExpectIds(pureQuery, {2, 5}, fixture.Parameters("beta"), true);
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
        fixture.ExpectError(fixture.Query(expression, "", Score), "Search string must be String or Utf8", NYdb::EStatus::GENERIC_ERROR);
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
                "Search string must be String or Utf8", params, NYdb::EStatus::GENERIC_ERROR);
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
            + call + (Score ? " > 0;" : ";"), "Expected String or Utf8", NYdb::EStatus::GENERIC_ERROR);
    }
    // Correct type alone is insufficient: the first argument must identify the indexed column.
    for (const TString& column : {TString("OtherText"), TString("$q"), TString("Text || $q")}) {
        const TString call = Sprintf("%s(%s, $q)", Score ? "FulltextScore" : "FulltextMatch", column.c_str());
        fixture.ExpectError(fixture.Declarations() + "SELECT Id FROM `/Root/Docs` VIEW fulltext_idx WHERE "
            + call + (Score ? " > 0;" : ";"), "Unsupported index access");
    }
}

Y_UNIT_TEST_TWIN(SideEffectSearchRejected, Score) {
    TExpressionFixture fixture({.Relevance = Score});
    fixture.ExpectError(fixture.Query("WithSideEffectsMode($q, 'General')", "", Score),
        "search expression must not have side effects");
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

Y_UNIT_TEST_TWIN(ComputedPredicateCombinationValidation, Compact) {
    TExpressionFixture fixture({.Compact = Compact, .Relevance = true});
    const TString setup = fixture.Declarations() + R"sql(
        $first = IF($choose, $q, $other);
        $second = COALESCE((SELECT Pattern FROM `/Root/Patterns` WHERE Id = $hi), $other);
    )sql";
    struct TCase {
        TString First;
        TString Second;
        TString Error;
    };
    const TVector<TCase> cases = {
        {"FulltextMatch(Text, $first)", "FulltextScore(Text, $second) > 0", "Multiple fulltext predicates"},
        {"FulltextScore(Text, $first) > 0", "FulltextScore(Text, $second) > 0", "Multiple fulltext score predicates"},
        {"FulltextMatch(Text, $first, \"or\" AS DefaultOperator)", "FulltextMatch(Text, $second)",
            "Multiple fulltext match predicates with named options"},
    };
    for (const auto& test : cases) {
        for (bool reverse : {false, true}) {
            fixture.ExpectError(setup + "SELECT Id FROM `/Root/Docs` VIEW fulltext_idx WHERE "
                + (reverse ? test.Second : test.First) + " AND " + (reverse ? test.First : test.Second) + ";", test.Error);
        }
    }
    fixture.ExpectError(setup + R"sql(
        SELECT Id, FulltextScore(Text, $first) AS Score FROM `/Root/Docs` VIEW fulltext_idx ORDER BY Score DESC;
    )sql", "Score restriction is not found");
    fixture.ExpectIds(setup + R"sql(
        SELECT Id FROM `/Root/Docs` VIEW fulltext_idx
        WHERE FulltextMatch(Text, $first) AND FulltextMatch(Text, $second) ORDER BY Id;
    )sql", {5});
}

Y_UNIT_TEST_TWIN(KeywordTokenizerPrecompute, Compact) {
    TExpressionFixture fixture({.Compact = Compact, .Tokenizer = "keyword"});
    fixture.ExpectIds(fixture.Query("IF($choose, $q, $other)"), {1, 3});
    fixture.ExpectIds(fixture.Query("IF($choose, $q, $other)"), {2}, fixture.Parameters("alpha", "beta", false));
    fixture.ExpectIds(fixture.Query("CAST($q AS String) || \" \" || CAST($other AS String)"), {5});
    fixture.ExpectIds(fixture.Query("COALESCE($search, $other)", R"sql(
        $search = (SELECT Pattern FROM `/Root/Patterns` WHERE Id = $lo);
    )sql"), {1, 3});
    fixture.ExpectError(fixture.Declarations() + R"sql(
        SELECT Id FROM `/Root/Docs` VIEW fulltext_idx
        WHERE FulltextMatch(Text, IF($choose, $q, $other))
            AND FulltextMatch(Text, COALESCE((SELECT Pattern FROM `/Root/Patterns` WHERE Id = $hi), $other));
    )sql", "Multiple fulltext match predicates with the keyword tokenizer");
}

Y_UNIT_TEST_TWIN(NamedArgumentsWithExpression, Score) {
    TExpressionFixture fixture({.Relevance = Score});
    const TString expression = "IF($choose, $q, $other)";
    const TString scoreOptions = Score ? ", Just(0.75f) AS B, Just(1.2) AS K1" : "";
    fixture.ExpectIds(fixture.Query(expression, "", Score, scoreOptions
        + ", Just(\"or\"u) AS DefaultOperator, Just(\"1\"u) AS MinimumShouldMatch"),
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
            const TString namedOptions = (Score ? TString(", $b AS B, $k1 AS K1") : TString())
                + ", $operator AS DefaultOperator, $minimum AS MinimumShouldMatch";
            const TString query = declarations + fixture.Select("IF($choose, $words, \"absent\")", Score, namedOptions);
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

Y_UNIT_TEST_TWIN(FunctionSpecificNamedArguments, Score) {
    TExpressionFixture fixture({.Ngram = !Score, .Relevance = Score});
    const TString expression = "IF($choose, $q, $other)";
    if (Score) {
        for (const TString& mode : {TString("Keywords"), TString("Wildcard"), TString("unknown")}) {
            fixture.ExpectError(fixture.Query(expression, "", true, ", \"" + mode + "\" AS Mode"),
                "unsupported FullTextScore named argument 'Mode'");
        }
    } else {
        for (const TString& option : {TString("B"), TString("K1")}) {
            fixture.ExpectError(fixture.Query(expression, "", false, ", 0.75 AS " + option),
                "unsupported FullTextMatch named argument '" + option + "'");
        }
        for (const TString& mode : {TString("unknown"), TString("")}) {
            fixture.ExpectError(fixture.Query(expression, "", false, ", \"" + mode + "\" AS Mode"),
                "Mode must be 'Keywords' or 'Wildcard'");
        }
        fixture.ExpectIds(fixture.Query(expression, "", false, ", \"kEyWoRdS\" AS Mode"), {1, 3, 4, 5, 8});
        fixture.ExpectIds(fixture.Query(expression, "", false, ", \"wIlDcArD\" AS Mode"), {1, 3},
            fixture.Parameters("alpha"));
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
            ", " + value + " AS Mode"), Score ? "unsupported FullTextScore named argument 'Mode'"
                                              : "Mode must be a String literal");
    }
    for (const TString& value : {TString("$q"), TString("\"Wildcard\"u"), TString("Just(\"Wildcard\")"), TString("0.5"),
            TString("IF($choose, \"Keywords\", \"Wildcard\")")}) {
        fixture.ExpectError(fixture.Query("IF($choose, $q, $other)", "", Score,
            ", " + value + " AS Mode"), Score ? "unsupported FullTextScore named argument 'Mode'"
                                              : "Mode must be a String literal");
    }
    for (const TString& value : {TString("$q"), TString("Just($q)"), TString("AsTuple($q, $other)"),
            TString("IF($choose, 0.75, 0.5)")}) {
        for (const TString& option : {TString("B"), TString("K1")}) {
            fixture.ExpectError(fixture.Query("IF($choose, $q, $other)", "", Score,
                ", " + value + " AS " + option), Score ? "must be a Float or Double literal or parameter"
                    : "unsupported FullTextMatch named argument '" + option + "'");
        }
    }
    // Supporting computed query text does not widen the named-option contract.
    for (const TString& option : {TString("DefaultOperator"), TString("MinimumShouldMatch")}) {
        fixture.ExpectError(fixture.Query("IF($choose, $q, $other)", "", Score,
            ", IF($choose, $q, $other) AS " + option), "must be a String or Utf8 literal or parameter");
    }
}

} // Y_UNIT_TEST_SUITE(KqpFulltextExpressions)
} // namespace NKikimr::NKqp
