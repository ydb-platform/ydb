#include "digest_test_common.h"

#include <util/digest/city.h>
#include <util/string/hex.h>

#include <openssl/sha.h>

namespace NKikimr::NKqp {
namespace {

using namespace NYdb;
using namespace NYdb::NTable;
using namespace NDigestTest;
namespace NDigest = NYql::NDigest;

void AddPickle(NDigest::TState& state, TStringBuf pickle) {
    switch (state.GetAlgorithm()) {
        case NDigest::EAlgorithm::CityHash64:
            state.AddHash(NDigest::TWords{CityHash64(pickle.data(), pickle.size())});
            break;
        case NDigest::EAlgorithm::CityHash128: {
            const auto hash = CityHash128(pickle.data(), pickle.size());
            state.AddHash(NDigest::TWords{hash.first, hash.second});
            break;
        }
        case NDigest::EAlgorithm::Sha256: {
            unsigned char hash[SHA256_DIGEST_LENGTH];
            SHA256(reinterpret_cast<const unsigned char*>(pickle.data()), pickle.size(), hash);
            state.AddHash(TStringBuf(reinterpret_cast<const char*>(hash), sizeof(hash)));
            break;
        }
    }
}

NDigest::TState ExpectedDigest(NDigest::EAlgorithm algorithm, bool range = false) {
    NDigest::TState state(algorithm);
    // Fixed StablePickle(TableRow()) outputs for the three fixture rows.
    if (!range) {
        AddPickle(state, HexDecode("0d0002066f6e65"));
    }
    AddPickle(state, HexDecode("07020116"));
    if (!range) {
        AddPickle(state, HexDecode("08000000002a0a7468726565"));
    }
    return state;
}

void PrepareTables(TSession& session) {
    auto result = session.ExecuteSchemeQuery(R"(
        CREATE TABLE digest_a (key Uint64 NOT NULL, value Utf8, PRIMARY KEY (key))
            WITH (PARTITION_AT_KEYS = (10, 20));
        CREATE TABLE digest_b (value Utf8, key Uint64 NOT NULL, PRIMARY KEY (key));
        CREATE TABLE digest_empty (key Uint64 NOT NULL, value Utf8, PRIMARY KEY (key));
    )").GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    auto data = session.ExecuteDataQuery(R"(
        UPSERT INTO digest_a (key, value) VALUES (1, "one"u), (11, NULL), (21, "three"u);
        UPSERT INTO digest_b (value, key) VALUES ("three"u, 21), (NULL, 11), ("one"u, 1);
    )", TTxControl::BeginTx().CommitTx()).GetValueSync();
    UNIT_ASSERT_C(data.IsSuccess(), data.GetIssues().ToString());
}

TString DigestExpression(TStringBuf row, TStringBuf algorithm) {
    return TStringBuilder() << "AGGREGATE_BY(" << row << ", $DigestFactory(\"" << algorithm << "\"))";
}

TDataQueryResult QueryDigest(TSession& session, TStringBuf table, TStringBuf algorithm, TStringBuf where = {}) {
    auto result = session.ExecuteDataQuery(
        TStringBuilder() << DigestFactorySql()
                         << "SELECT " << DigestExpression("TableRow()", algorithm)
                         << " AS digest FROM " << table << " " << where << ";",
        TTxControl::BeginTx(TTxSettings::SnapshotRO()).CommitTx()).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    return result;
}

TString ChecksumStatement(TStringBuf table, TStringBuf algorithm = {}) {
    TStringBuilder query;
    query << "CHECKSUM TABLE " << table;
    if (algorithm) {
        query << " WITH (ALGORITHM = \"" << algorithm << "\")";
    }
    return query << ";";
}

TDataQueryResult QueryChecksum(TSession& session, TStringBuf table,
                               TStringBuf algorithm = {},
                               TStringBuf prefix = {}) {
    auto result =
        session
            .ExecuteDataQuery(
                TStringBuilder() << prefix << ChecksumStatement(table, algorithm),
                TTxControl::BeginTx(TTxSettings::SnapshotRO()).CommitTx())
            .GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
    return result;
}

} // namespace

Y_UNIT_TEST_SUITE(Digest) {
    Y_UNIT_TEST(AlgorithmsPartitionsAndRanges) {
        TKikimrRunner kikimr(TKikimrSettings().SetWithSampleTables(false));
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        PrepareTables(session);
        for (auto algorithm : {NDigest::EAlgorithm::CityHash64, NDigest::EAlgorithm::CityHash128, NDigest::EAlgorithm::Sha256}) {
            const auto name = NDigest::GetAlgorithmName(algorithm);
            const auto expected = ExpectedDigest(algorithm);
            for (TStringBuf table : {"digest_a", "digest_b"}) {
                const auto result = QueryDigest(session, table, name);
                AssertDigest(ReadDigest(result.GetResultSet(0)), expected);
            }
            const auto range = QueryDigest(session, "digest_a", name, "WHERE key >= 10 AND key < 20");
            AssertDigest(ReadDigest(range.GetResultSet(0)), ExpectedDigest(algorithm, true));
            const auto empty = QueryDigest(session, "digest_empty", name);
            AssertDigest(ReadDigest(empty.GetResultSet(0)), NDigest::TState(algorithm));
            const auto filtered = QueryDigest(session, "digest_a", name, "WHERE key < 0");
            AssertDigest(ReadDigest(filtered.GetResultSet(0)), NDigest::TState(algorithm));
        }
    }

    Y_UNIT_TEST(GroupedDigestsKeepIndependentStates) {
        TKikimrRunner kikimr(TKikimrSettings().SetWithSampleTables(false));
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        PrepareTables(session);
        // Independent StablePickle(AsStruct(x AS x)) fixtures for x = 1, 11, 21.
        const TStringBuf pickles[] = {"0302", "0316", "032a"};
        for (auto algorithm : {NDigest::EAlgorithm::CityHash64, NDigest::EAlgorithm::CityHash128, NDigest::EAlgorithm::Sha256}) {
            for (bool constant : {false, true}) {
                const auto expression = DigestExpression(
                    constant ? "AsStruct(1ul AS x)" : "AsStruct(x AS x)", NDigest::GetAlgorithmName(algorithm));
                const TString query = TStringBuilder()
                                      << DigestFactorySql()
                                      << "$rows = (SELECT key AS x FROM digest_a);\n"
                                      << "SELECT g, COUNT(*) AS n, " << expression
                                      << " AS digest FROM $rows GROUP BY x > 1ul AS g;";
                const auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                TResultSetParser rows(result.GetResultSet(0));
                size_t groups = 0;
                while (rows.TryNextRow()) {
                    const bool group = rows.ColumnParser("g").GetBool();
                    UNIT_ASSERT_VALUES_EQUAL(rows.ColumnParser("n").GetUint64(), group ? 2 : 1);
                    NDigest::TState expected(algorithm);
                    for (size_t i = 0; i < 3; ++i) {
                        if ((i > 0) == group) {
                            AddPickle(expected, HexDecode(pickles[constant ? 0 : i]));
                        }
                    }
                    AssertDigest(ReadDigest(rows.ColumnParser("digest")), expected);
                    ++groups;
                }
                UNIT_ASSERT_VALUES_EQUAL(groups, 2);
            }
        }
    }

    Y_UNIT_TEST(EmptyInputStillValidatesAlgorithm) {
        TKikimrRunner kikimr(TKikimrSettings().SetWithSampleTables(false));
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        PrepareTables(session);
        const auto result = session.ExecuteDataQuery(
            TStringBuilder() << DigestFactorySql()
                             << "SELECT AGGREGATE_BY(TableRow(), $DigestFactory(\"Sha512\")) FROM digest_empty;",
            TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Unsupported Digest algorithm");
    }

    Y_UNIT_TEST(SnapshotRemainsStableAcrossWrites) {
        TKikimrRunner kikimr(TKikimrSettings().SetWithSampleTables(false));
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        PrepareTables(session);
        const TString query = TStringBuilder() << DigestFactorySql()
                                              << "SELECT AGGREGATE_BY(TableRow(), $DigestFactory(\"Sha256\")) FROM digest_a;";
        auto first = session.ExecuteDataQuery(query, TTxControl::BeginTx(TTxSettings::SnapshotRO())).GetValueSync();
        UNIT_ASSERT_C(first.IsSuccess(), first.GetIssues().ToString());
        const auto transaction = first.GetTransaction();
        UNIT_ASSERT(transaction);
        auto write = session.ExecuteDataQuery(
            "UPSERT INTO digest_a (key, value) VALUES (1, \"changed\"u);", TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(write.IsSuccess(), write.GetIssues().ToString());
        auto second = session.ExecuteDataQuery(query, TTxControl::Tx(*transaction).CommitTx()).GetValueSync();
        UNIT_ASSERT_C(second.IsSuccess(), second.GetIssues().ToString());
        AssertDigest(ReadDigest(first.GetResultSet(0)), ExpectedDigest(NDigest::EAlgorithm::Sha256));
        AssertDigest(ReadDigest(second.GetResultSet(0)), ExpectedDigest(NDigest::EAlgorithm::Sha256));
        const auto latest = QueryDigest(session, "digest_a", "Sha256");
        UNIT_ASSERT(ReadDigest(latest.GetResultSet(0)).Hash != ReadDigest(first.GetResultSet(0)).Hash);
    }
    Y_UNIT_TEST(CommandAlgorithmsPartitionsAndEmptyTables) {
        TKikimrRunner kikimr(TKikimrSettings().SetWithSampleTables(false));
        auto session =
            kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        PrepareTables(session);

        const auto defaultResult = QueryChecksum(session, "digest_a");
        AssertDigest(ReadChecksum(defaultResult.GetResultSet(0)),
                     ExpectedDigest(NDigest::EAlgorithm::CityHash64));
        for (auto algorithm :
             {NDigest::EAlgorithm::CityHash64, NDigest::EAlgorithm::CityHash128,
              NDigest::EAlgorithm::Sha256}) {
            const auto name = NDigest::GetAlgorithmName(algorithm);
            for (TStringBuf table : {"digest_a", "digest_b"}) {
                const auto result = QueryChecksum(session, table, name);
                AssertDigest(ReadChecksum(result.GetResultSet(0)),
                             ExpectedDigest(algorithm));
            }
            const auto empty = QueryChecksum(session, "digest_empty", name);
            AssertDigest(ReadChecksum(empty.GetResultSet(0)),
                         NDigest::TState(algorithm));
        }
    }

    Y_UNIT_TEST(CanonicalizationIsTypeDirectedInPhysicalPlan) {
        TKikimrRunner kikimr(TKikimrSettings().SetWithSampleTables(false));
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        PrepareTables(session);
        const auto schema = session.ExecuteSchemeQuery(R"(
            CREATE TABLE digest_json_doc (
                key Uint64 NOT NULL, doc JsonDocument, PRIMARY KEY (key)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(schema.IsSuccess(), schema.GetIssues().ToString());
        const auto data = session.ExecuteDataQuery(R"(
            UPSERT INTO digest_json_doc (key, doc)
            VALUES (1, JsonDocument('[1,"alpha",2,"beta"]')), (2, NULL);
        )", TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(data.IsSuccess(), data.GetIssues().ToString());

        for (auto algorithm : {NDigest::EAlgorithm::CityHash64, NDigest::EAlgorithm::CityHash128, NDigest::EAlgorithm::Sha256}) {
            const auto name = NDigest::GetAlgorithmName(algorithm);
            auto direct = [&](TStringBuf row, TStringBuf table) -> TString {
                return TStringBuilder() << DigestFactorySql() << "SELECT "
                                        << DigestExpression(row, name) << " FROM " << table << ";";
            };
            const TVector<std::pair<TString, bool>> cases = {
                {ChecksumStatement("digest_a", name), false},
                {direct("TableRow()", "digest_a"), false},
                {direct("AsStruct(AsList(value) AS `JsonDocument`)", "digest_a"), false},
                {ChecksumStatement("digest_json_doc", name), true},
                {direct("AsStruct(AsList(doc) AS nested)", "digest_json_doc"), true},
            };
            for (const auto& [query, needsCanonicalization] : cases) {
                const auto result = session.ExplainDataQuery(query).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                const auto& ast = result.GetAst();
                UNIT_ASSERT(!ast.empty());
                UNIT_ASSERT_STRING_CONTAINS(ast, "DigestAggregate.AggregateAddValue");
                UNIT_ASSERT_VALUES_EQUAL_C(
                    ast.find("DigestAggregate.Canonicalize") != std::string::npos,
                    needsCanonicalization, TStringBuilder() << query << "\n" << ast);
            }
        }
    }

    Y_UNIT_TEST(CommandResolvesTablePaths) {
        TKikimrRunner kikimr(TKikimrSettings().SetWithSampleTables(false));
        auto session =
            kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        PrepareTables(session);
        const auto expected = ExpectedDigest(NDigest::EAlgorithm::CityHash64);
        for (TStringBuf table : {"`/Root/digest_a`", "db.digest_a"}) {
            const auto result = QueryChecksum(session, table);
            AssertDigest(ReadChecksum(result.GetResultSet(0)), expected);
        }
        const auto use = QueryChecksum(session, "digest_a", {}, "USE db;\n");
        AssertDigest(ReadChecksum(use.GetResultSet(0)), expected);
        const auto bound = QueryChecksum(session, "$table", {}, "$table = 'digest_a';\n");
        AssertDigest(ReadChecksum(bound.GetResultSet(0)), expected);

        auto directory =
            kikimr.GetSchemeClient().MakeDirectory("/Root/checksum").GetValueSync();
        UNIT_ASSERT_C(directory.IsSuccess(), directory.GetIssues().ToString());
        auto schema = session
                          .ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/checksum/select` (
                key Uint64 NOT NULL, value Utf8, PRIMARY KEY (key)
            );
        )")
                          .GetValueSync();
        UNIT_ASSERT_C(schema.IsSuccess(), schema.GetIssues().ToString());
        auto data = session
                        .ExecuteDataQuery(R"(
            UPSERT INTO `/Root/checksum/select` (key, value)
            VALUES (1, "one"u), (11, NULL), (21, "three"u);
        )", TTxControl::BeginTx().CommitTx())
                        .GetValueSync();
        UNIT_ASSERT_C(data.IsSuccess(), data.GetIssues().ToString());
        const auto prefixed =
            QueryChecksum(session, "`select`", {},
                          "PRAGMA TablePathPrefix(\"/Root/checksum\");\n");
        AssertDigest(ReadChecksum(prefixed.GetResultSet(0)), expected);
        const auto absolute =
            QueryChecksum(session, "`/Root/checksum/select`");
        AssertDigest(ReadChecksum(absolute.GetResultSet(0)), expected);

        const auto missing = session
                                 .ExecuteDataQuery("CHECKSUM TABLE missing_digest;",
                                                   TTxControl::BeginTx().CommitTx())
                                 .GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(missing.GetStatus(), EStatus::SCHEME_ERROR,
                                   missing.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(missing.GetIssues().ToString(),
                                    "missing_digest");
    }

    Y_UNIT_TEST(CommandValidatesAlgorithmOnEmptyInput) {
        TKikimrRunner kikimr(TKikimrSettings().SetWithSampleTables(false));
        auto session =
            kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        PrepareTables(session);
        for (TStringBuf table : {"digest_a", "digest_empty"}) {
            for (TStringBuf algorithm : {"Sha512", "sha256", ""}) {
                const auto result = session.ExecuteDataQuery(
                    TStringBuilder() << "CHECKSUM TABLE " << table
                                     << " WITH (ALGORITHM = \"" << algorithm << "\");",
                    TTxControl::BeginTx().CommitTx()).GetValueSync();
                UNIT_ASSERT(!result.IsSuccess());
                UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(),
                    TStringBuilder() << "Unsupported CHECKSUM TABLE algorithm: " << algorithm);
            }
        }
    }

    Y_UNIT_TEST(CommandQueryService) {
        TKikimrRunner kikimr(TKikimrSettings().SetWithSampleTables(false));
        auto session =
            kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        PrepareTables(session);
        auto client = kikimr.GetQueryClient();
        for (auto algorithm :
             {NDigest::EAlgorithm::CityHash64, NDigest::EAlgorithm::CityHash128,
              NDigest::EAlgorithm::Sha256}) {
            const auto result =
                client
                    .ExecuteQuery(
                        ChecksumStatement("digest_a",
                                          NDigest::GetAlgorithmName(algorithm)),
                        NYdb::NQuery::TTxControl::BeginTx(
                            NYdb::NQuery::TTxSettings::SnapshotRO())
                            .CommitTx())
                    .GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
            AssertDigest(ReadChecksum(result.GetResultSet(0)),
                         ExpectedDigest(algorithm));
        }
        const auto batch =
            client
                .ExecuteQuery(
                    "CHECKSUM TABLE digest_a; CHECKSUM TABLE digest_empty;",
                    NYdb::NQuery::TTxControl::BeginTx().CommitTx())
                .GetValueSync();
        UNIT_ASSERT_C(batch.IsSuccess(), batch.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(batch.GetResultSets().size(), 2);
        AssertDigest(ReadChecksum(batch.GetResultSet(0)),
                     ExpectedDigest(NDigest::EAlgorithm::CityHash64));
        AssertDigest(ReadChecksum(batch.GetResultSet(1)),
                     NDigest::TState(NDigest::EAlgorithm::CityHash64));
    }

    Y_UNIT_TEST(CommandResultRowsLimitDoesNotLimitInput) {
        TKikimrRunner kikimr(TKikimrSettings().SetWithSampleTables(false));
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        PrepareTables(session);
        auto client = kikimr.GetQueryClient();
        const TStringBuf prefix = "PRAGMA ResultRowsLimit = \"1\";\n";

        for (auto algorithm : {NDigest::EAlgorithm::CityHash64, NDigest::EAlgorithm::CityHash128, NDigest::EAlgorithm::Sha256}) {
            const auto name = NDigest::GetAlgorithmName(algorithm);
            for (TStringBuf table : {"digest_a", "digest_b", "digest_empty"}) {
                const auto expected = table == "digest_empty" ? NDigest::TState(algorithm) : ExpectedDigest(algorithm);
                const auto dataResult = QueryChecksum(session, table, name, prefix);
                AssertDigest(ReadChecksum(dataResult.GetResultSet(0)), expected);

                const auto queryResult = client.ExecuteQuery(
                    TStringBuilder() << prefix << ChecksumStatement(table, name),
                    NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::SnapshotRO()).CommitTx())
                    .GetValueSync();
                UNIT_ASSERT_C(queryResult.IsSuccess(), queryResult.GetIssues().ToString());
                UNIT_ASSERT_VALUES_EQUAL(queryResult.GetResultSets().size(), 1);
                AssertDigest(ReadChecksum(queryResult.GetResultSet(0)), expected);
            }
        }
    }

    Y_UNIT_TEST(CommandIgnoresSystemColumns) {
        TKikimrRunner kikimr(TKikimrSettings().SetWithSampleTables(false));
        auto session =
            kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        PrepareTables(session);
        for (TStringBuf prefix : {
                 "",
                 "PRAGMA EnableSystemColumns = \"false\";\n",
                 "PRAGMA kikimr.EnableSystemColumns = \"true\";\n"}) {
            for (auto algorithm :
                 {NDigest::EAlgorithm::CityHash64, NDigest::EAlgorithm::CityHash128,
                  NDigest::EAlgorithm::Sha256}) {
                for (TStringBuf table : {"digest_a", "digest_b"}) {
                    const auto result =
                        QueryChecksum(session, table, NDigest::GetAlgorithmName(algorithm), prefix);
                    AssertDigest(ReadChecksum(result.GetResultSet(0)),
                                 ExpectedDigest(algorithm));
                }
            }
        }
    }

    Y_UNIT_TEST(CommandIgnoresExtraSystemColumnPrefixes) {
        TKikimrRunner kikimr(TKikimrSettings().SetWithSampleTables(false));
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        PrepareTables(session);
        const auto schema = session.ExecuteSchemeQuery(R"(
            CREATE TABLE digest_extra_system_columns (
                key Uint64 NOT NULL, value Utf8, __ydb_row_id Uint64,
                PRIMARY KEY (key)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(schema.IsSuccess(), schema.GetIssues().ToString());
        const auto data = session.ExecuteDataQuery(R"(
            UPSERT INTO digest_extra_system_columns (key, value)
            SELECT key, value FROM digest_a;
        )", TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(data.IsSuccess(), data.GetIssues().ToString());
        // KQP configures __ydb_row_id as an extra system-column prefix.
        const auto visible = session.ExecuteDataQuery("SELECT * FROM digest_extra_system_columns;",
            TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(visible.IsSuccess(), visible.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(visible.GetResultSet(0).ColumnsCount(), 2);
        for (auto algorithm : {NDigest::EAlgorithm::CityHash64, NDigest::EAlgorithm::CityHash128, NDigest::EAlgorithm::Sha256}) {
            const auto result = QueryChecksum(session, "digest_extra_system_columns", NDigest::GetAlgorithmName(algorithm));
            AssertDigest(ReadChecksum(result.GetResultSet(0)), ExpectedDigest(algorithm));
        }
    }

    Y_UNIT_TEST(CommandSnapshotRemainsStableAcrossWrites) {
        TKikimrRunner kikimr(TKikimrSettings().SetWithSampleTables(false));
        auto session =
            kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        PrepareTables(session);
        const auto query = ChecksumStatement("digest_a", "Sha256");
        const auto first =
            session
                .ExecuteDataQuery(query,
                                  TTxControl::BeginTx(TTxSettings::SnapshotRO()))
                .GetValueSync();
        UNIT_ASSERT_C(first.IsSuccess(), first.GetIssues().ToString());
        const auto transaction = first.GetTransaction();
        UNIT_ASSERT(transaction);
        const auto write =
            session
                .ExecuteDataQuery(
                    "UPSERT INTO digest_a (key, value) VALUES (1, \"changed\"u);",
                    TTxControl::BeginTx().CommitTx())
                .GetValueSync();
        UNIT_ASSERT_C(write.IsSuccess(), write.GetIssues().ToString());
        const auto second =
            session.ExecuteDataQuery(query, TTxControl::Tx(*transaction).CommitTx())
                .GetValueSync();
        UNIT_ASSERT_C(second.IsSuccess(), second.GetIssues().ToString());
        AssertDigest(ReadChecksum(first.GetResultSet(0)),
                     ExpectedDigest(NDigest::EAlgorithm::Sha256));
        AssertDigest(ReadChecksum(second.GetResultSet(0)),
                     ExpectedDigest(NDigest::EAlgorithm::Sha256));
        const auto latest = QueryChecksum(session, "digest_a", "Sha256");
        UNIT_ASSERT(ReadChecksum(latest.GetResultSet(0)).Hash !=
                    ReadChecksum(first.GetResultSet(0)).Hash);
    }

    Y_UNIT_TEST(CommandRequiresTableReadPermission) {
        TKikimrRunner kikimr(
            TKikimrSettings().SetWithSampleTables(false).SetAuthToken(
                "root@builtin"));
        auto session =
            kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        PrepareTables(session);
        const TString subject = "checksum_reader@builtin";
        const auto connect =
            kikimr.GetSchemeClient()
                .ModifyPermissions(
                    "/Root",
                    NYdb::NScheme::TModifyPermissionsSettings().AddGrantPermissions(
                        NYdb::NScheme::TPermissions(subject,
                                                    {"ydb.database.connect"})))
                .GetValueSync();
        UNIT_ASSERT_C(connect.IsSuccess(), connect.GetIssues().ToString());
        WaitForProxy(kikimr, subject);
        auto client = kikimr.GetTableClient(
            NYdb::NTable::TClientSettings().AuthToken(subject));
        const auto created = client.CreateSession().GetValueSync();
        UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());
        auto reader = created.GetSession();

        for (const TString& query :
             {"SELECT * FROM digest_a;", "CHECKSUM TABLE digest_a;"}) {
            const auto denied =
                reader.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx())
                    .GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(denied.GetStatus(), EStatus::SCHEME_ERROR,
                                       denied.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(denied.GetIssues().ToString(),
                                        "access permissions");
        }

        const auto granted =
            kikimr.GetSchemeClient()
                .ModifyPermissions(
                    "/Root/digest_a",
                    NYdb::NScheme::TModifyPermissionsSettings().AddGrantPermissions(
                        NYdb::NScheme::TPermissions(subject, {"ydb.generic.read"})))
                .GetValueSync();
        UNIT_ASSERT_C(granted.IsSuccess(), granted.GetIssues().ToString());
        Tests::TClient::RefreshPathCache(kikimr.GetTestServer().GetRuntime(),
                                         "/Root/digest_a");
        const auto allowed = QueryChecksum(reader, "digest_a");
        AssertDigest(ReadChecksum(allowed.GetResultSet(0)),
                     ExpectedDigest(NDigest::EAlgorithm::CityHash64));
    }
} // Y_UNIT_TEST_SUITE(Digest)

} // namespace NKikimr::NKqp
