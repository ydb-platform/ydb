#include "sql_ut.h"

#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/translation/sql.h>
#include <yql/essentials/sql/v1/translation/sql_translation.h>

#include <yql/essentials/providers/common/provider/yql_provider_names.h>

using namespace NSQLTranslationV1;

Y_UNIT_TEST_SUITE(RelativeTablePathPrefix) {

NYql::TAstParseResult Translate(const TString& query, const TString& provider = "kikimr", bool enableRelativePaths = true) {
    NSQLTranslation::TTranslationSettings settings;
    settings.PathPrefix = "/Root/database";
    settings.EnableTablePathPrefixRelativePaths = enableRelativePaths;
    return SqlToYqlWithMode("USE plato; " + query, NSQLTranslation::ESqlMode::QUERY,
        10, provider, EDebugOutput::None, false, settings);
}

const NYql::TAstNode* GetKeyPath(const NYql::TAstParseResult& result) {
    UNIT_ASSERT_C(result.IsOk(), Err2Str(result));
    const auto* key = FindNodeByChildAtomContent(result.Root, 0, "Key");
    UNIT_ASSERT_C(key && key->GetChildrenCount() >= 2, GetPrettyPrint(result));
    const auto* quotedEntry = key->GetChild(1);
    UNIT_ASSERT(quotedEntry->IsListOfSize(2));
    UNIT_ASSERT(quotedEntry->GetChild(0)->IsAtom());
    UNIT_ASSERT_VALUES_EQUAL(quotedEntry->GetChild(0)->GetContent(), "quote");
    const auto* entry = quotedEntry->GetChild(1);
    UNIT_ASSERT(entry->IsListOfSize(2));
    const auto* string = entry->GetChild(1);
    UNIT_ASSERT(string->IsListOfSize(2));
    UNIT_ASSERT(string->GetChild(0)->IsAtom());
    UNIT_ASSERT_VALUES_EQUAL(string->GetChild(0)->GetContent(), "String");
    return string->GetChild(1);
}

void AssertQuotedAtom(const NYql::TAstNode* node, const TString& value) {
    UNIT_ASSERT(node->IsListOfSize(2));
    UNIT_ASSERT(node->GetChild(0)->IsAtom());
    UNIT_ASSERT_VALUES_EQUAL(node->GetChild(0)->GetContent(), "quote");
    UNIT_ASSERT(node->GetChild(1)->IsAtom());
    UNIT_ASSERT_VALUES_EQUAL(node->GetChild(1)->GetContent(), value);
}

void AssertPath(const NYql::TAstParseResult& result, const TString& path) {
    AssertQuotedAtom(GetKeyPath(result), path);
}

Y_UNIT_TEST(GenericDefaultsPreserveLegacyPaths) {
    NSQLTranslation::TTranslationSettings settings;
    UNIT_ASSERT(!settings.EnableTablePathPrefixRelativePaths);
    settings.PathPrefix = "/Root/database";
    for (const TString& provider : {TString("kikimr"), TString("yt"), TString("rtmr"), TString("ydb")}) {
        AssertPath(SqlToYqlWithMode("USE plato; PRAGMA TablePathPrefix = './folder'; SELECT * FROM users;",
            NSQLTranslation::ESqlMode::QUERY, 10, provider, EDebugOutput::None, false, settings), "folder/users");
    }
}

Y_UNIT_TEST(LegacyAccessorAndTopicFactoryRemainAvailable) {
    using TLegacyPrefixAccessor = TStringBuf (TContext::*)(const TString&, const TDeferredAtom&) const;
    const TLegacyPrefixAccessor getPrefix = &TContext::GetPrefixPath;
    using TLegacyTopicFactory = TNodePtr (*)(TPosition, const TDeferredAtom&, const TDeferredAtom&);
    const TLegacyTopicFactory buildTopic = &BuildTopicKey;

    for (bool enabled : {false, true}) {
        NSQLTranslation::TTranslationSettings settings;
        settings.PathPrefix = "/Root/database";
        settings.EnableTablePathPrefixRelativePaths = enabled;
        settings.ClusterMapping["plato"] = "kikimr";
        NYql::TIssues issues;
        TContext ctx({}, {}, settings, {}, issues);
        const TDeferredAtom cluster(TPosition{}, "plato");
        UNIT_ASSERT(ctx.SetPathPrefix("./folder"));
        const TContext& legacy = ctx;
        const TStringBuf prefix = (legacy.*getPrefix)("kikimr", cluster);
        UNIT_ASSERT_VALUES_EQUAL(prefix, enabled ? "/Root/database/folder" : "./folder");
        UNIT_ASSERT(buildTopic(TPosition{}, cluster, TDeferredAtom(TPosition{}, "users")));
    }
}

Y_UNIT_TEST(PrefixStoragePreservesProviderAndEmptyFallbacks) {
    for (bool enabled : {false, true}) {
        NSQLTranslation::TTranslationSettings settings;
        settings.EnableTablePathPrefixRelativePaths = enabled;
        settings.PathPrefix = "/Root/database";
        settings.ClusterMapping["plato"] = "kikimr";
        settings.ClusterMapping["yt_cluster"] = "yt";
        settings.ClusterPathPrefixes["plato"] = "initial";
        settings.ClusterPathPrefixes["yt_cluster"] = "./remote";
        NYql::TIssues issues;
        TContext ctx({}, {}, settings, {}, issues);
        const TDeferredAtom cluster(TPosition{}, "plato");
        const TDeferredAtom remote(TPosition{}, "yt_cluster");
        UNIT_ASSERT_VALUES_EQUAL(ctx.GetPrefixPath("kikimr", cluster), enabled ? "/Root/database/initial" : "initial");
        UNIT_ASSERT_VALUES_EQUAL(ctx.GetPrefixPath("yt", remote), enabled ? "/Root/database/remote" : "./remote");

        UNIT_ASSERT(ctx.SetPathPrefix("folder"));
        UNIT_ASSERT(ctx.SetPathPrefix("", TString("plato")));
        UNIT_ASSERT(ctx.SetPathPrefix("provider", TString("kikimr")));
        UNIT_ASSERT_VALUES_EQUAL(ctx.GetPrefixPath("kikimr", cluster), enabled ? "/Root/database/provider" : "provider");
        UNIT_ASSERT(ctx.SetPathPrefix("", TString("kikimr")));
        UNIT_ASSERT_VALUES_EQUAL(ctx.GetPrefixPath("kikimr", cluster), enabled ? "/Root/database/folder" : "folder");
        UNIT_ASSERT(ctx.SetPathPrefix("", TString("yt_cluster")));
        UNIT_ASSERT_VALUES_EQUAL(ctx.GetPrefixPath("yt", remote), enabled ? "/Root/database/folder" : "folder");
        UNIT_ASSERT(ctx.SetPathPrefix("remote_provider", TString("yt")));
        UNIT_ASSERT_VALUES_EQUAL(ctx.GetPrefixPath("yt", remote), enabled ? "/Root/database/remote_provider" : "remote_provider");
        UNIT_ASSERT(ctx.SetPathPrefix("", TString("yt")));
        UNIT_ASSERT_VALUES_EQUAL(ctx.GetPrefixPath("yt", remote), enabled ? "/Root/database/folder" : "folder");

        UNIT_ASSERT(ctx.SetPathPrefix(""));
        UNIT_ASSERT_VALUES_EQUAL(ctx.GetPrefixPath("kikimr", cluster), enabled ? "/Root/database" : "");
        UNIT_ASSERT_VALUES_EQUAL(ctx.GetPrefixPath("yt", remote), enabled ? "/Root/database" : "");
    }
}

Y_UNIT_TEST(ResolveFromDatabase) {
    const struct {
        TString Prefix;
        TString Table;
        TString Expected;
    } cases[] = {
        {"folder", "users", "/Root/database/folder/users"},
        {"./folder", "users", "/Root/database/folder/users"},
        {"folder/", "users", "/Root/database/folder/users"},
        {"folder/child/..", "users", "/Root/database/folder/users"},
        {"folder/child", "../users", "/Root/database/folder/users"},
        {".", "users", "/Root/database/users"},
        {"", "users", "/Root/database/users"},
        {"../sibling", "users", "/Root/sibling/users"},
        {"/Other/folder", "users", "/Other/folder/users"},
        {"folder", "/Other/users", "/Other/users"},
        {"./folder", "/Other/users", "/Other/users"},
        {"/Other/folder", "/Root/database/users", "/Root/database/users"},
    };
    for (const auto& test : cases) {
        const TString query = TStringBuilder() << "PRAGMA TablePathPrefix = '" << test.Prefix
            << "'; SELECT * FROM `" << test.Table << "`;";
        AssertPath(Translate(query), test.Expected);
    }
    AssertPath(Translate("SELECT * FROM users;"), "/Root/database/users");
}

Y_UNIT_TEST(DisabledPreservesLegacyResolution) {
    const struct {
        TString Pragma;
        TString TablePath;
    } cases[] = {
        {"PRAGMA TablePathPrefix = 'folder';", "folder/users"},
        {"PRAGMA TablePathPrefix = './folder';", "folder/users"},
        {"PRAGMA TablePathPrefix = '';", "users"},
        {"PRAGMA TablePathPrefix = '/Other';", "/Other/users"},
        {"PRAGMA TablePathPrefix('kikimr', './folder');", "folder/users"},
        {"PRAGMA TablePathPrefix('plato', './folder');", "folder/users"},
    };
    for (const auto& test : cases) {
        AssertPath(Translate(test.Pragma + "SELECT * FROM users;", "kikimr", false), test.TablePath);
    }
    AssertPath(Translate("PRAGMA TablePathPrefix = 'folder'; SELECT * FROM `/Other/users`;", "kikimr", false), "/Other/users");
    AssertPath(Translate("PRAGMA TablePathPrefix = './folder'; CREATE TOPIC users;", "kikimr", false), "folder/users");
    // Topics historically ignored provider prefixes because their service was empty.
    AssertPath(Translate("PRAGMA TablePathPrefix('kikimr', './folder'); CREATE TOPIC users;", "kikimr", false), "/Root/database/users");
}

Y_UNIT_TEST(RepeatedPrefixesUseDatabaseRoot) {
    AssertPath(Translate(R"(
        PRAGMA TablePathPrefix = 'folder1';
        PRAGMA TablePathPrefix = './folder2';
        SELECT * FROM users;
    )"), "/Root/database/folder2/users");
}

Y_UNIT_TEST(ProviderAndClusterPrefixes) {
    for (const TString& scope : {TString("kikimr"), TString("plato")}) {
        AssertPath(Translate(TStringBuilder()
            << "PRAGMA TablePathPrefix = '/Other';"
            << "PRAGMA TablePathPrefix('" << scope << "', './folder');"
            << "SELECT * FROM users;"), "/Root/database/folder/users");
    }
    AssertPath(Translate(R"(
        PRAGMA TablePathPrefix = '/Other';
        PRAGMA TablePathPrefix('kikimr', 'provider');
        PRAGMA TablePathPrefix('plato', 'cluster');
        SELECT * FROM users;
    )"), "/Root/database/cluster/users");
}

Y_UNIT_TEST(OptInAppliesToEveryProviderInTheContext) {
    for (const TString& provider : {TString("kikimr"), TString("yt"), TString("rtmr"), TString("ydb")}) {
        AssertPath(Translate("PRAGMA TablePathPrefix = './folder'; SELECT * FROM users;", provider), "/Root/database/folder/users");
        AssertPath(Translate("PRAGMA TablePathPrefix('plato', './folder'); SELECT * FROM users;", provider), "/Root/database/folder/users");
        if (provider != "ydb") {
            AssertPath(Translate(TStringBuilder() << "PRAGMA TablePathPrefix('" << provider
                << "', './folder'); SELECT * FROM users;", provider), "/Root/database/folder/users");
        }
    }
}

Y_UNIT_TEST(OptInRequiresAnAbsoluteBase) {
    for (const TString& base : {TString(), TString("relative/base")}) {
        NSQLTranslation::TTranslationSettings settings;
        settings.EnableTablePathPrefixRelativePaths = true;
        settings.PathPrefix = base;
        settings.ClusterMapping["plato"] = "kikimr";
        settings.ClusterPathPrefixes["plato"] = "./initial";
        NYql::TIssues issues;
        TContext ctx({}, {}, settings, {}, issues);
        const TDeferredAtom cluster(TPosition{}, "plato");
        UNIT_ASSERT_VALUES_EQUAL(ctx.GetPrefixPath("kikimr", cluster), "./initial");
        UNIT_ASSERT(ctx.SetPathPrefix("./folder"));
        UNIT_ASSERT(ctx.SetPathPrefix("", TString("plato")));
        UNIT_ASSERT_VALUES_EQUAL(ctx.GetPrefixPath("kikimr", cluster), "./folder");
    }
}

Y_UNIT_TEST(TopicPrefixesKeepLegacyProviderSelection) {
    for (bool enabled : {false, true}) {
        AssertPath(Translate("PRAGMA TablePathPrefix('plato', './folder'); CREATE TOPIC users;", "kikimr", enabled),
            enabled ? "/Root/database/folder/users" : "folder/users");
        AssertPath(Translate("PRAGMA TablePathPrefix('kikimr', './folder'); CREATE TOPIC users;", "kikimr", enabled),
            "/Root/database/users");
    }
}

Y_UNIT_TEST(DynamicClustersKeepUnprefixedPaths) {
    NSQLTranslation::TTranslationSettings settings;
    settings.PathPrefix = "/Root/database";
    settings.EnableTablePathPrefixRelativePaths = true;
    settings.DynamicClusterProvider = NYql::KikimrProviderName;
    const auto result = SqlToYqlWithMode(R"(
        PRAGMA TablePathPrefix = './folder';
        SELECT * FROM extcluster.`nested/users`;
    )", NSQLTranslation::ESqlMode::QUERY, 10, "kikimr", EDebugOutput::None, false, settings);
    AssertPath(result, "nested/users");
}

Y_UNIT_TEST(DeferredTablePathsUseDatabaseRoot) {
    const auto result = Translate(R"(
        PRAGMA TablePathPrefix = './folder';
        $table = 'us' || 'ers';
        SELECT * FROM $table;
    )");
    const auto* buildPath = FindNodeByChildAtomContent(GetKeyPath(result), 0, "BuildTablePath");
    UNIT_ASSERT_C(buildPath && buildPath->IsListOfSize(3), GetPrettyPrint(result));
    const auto* prefix = buildPath->GetChild(1);
    UNIT_ASSERT(prefix->IsListOfSize(2));
    UNIT_ASSERT(prefix->GetChild(0)->IsAtom());
    UNIT_ASSERT_VALUES_EQUAL(prefix->GetChild(0)->GetContent(), "String");
    AssertQuotedAtom(prefix->GetChild(1), "/Root/database/folder");
}

Y_UNIT_TEST(ObjectPathsUseDatabaseRoot) {
    for (const TString& statement : {
        TString("CREATE TABLE users (id Uint64, PRIMARY KEY (id));"),
        TString("DROP TABLE users;"),
        TString("CREATE TOPIC users;"),
        TString("DROP TOPIC users;"),
        TString("CREATE EXTERNAL DATA SOURCE users WITH (SOURCE_TYPE='ObjectStorage', LOCATION='bucket', AUTH_METHOD='NONE');"),
    }) {
        AssertPath(Translate("PRAGMA TablePathPrefix = './folder'; " + statement), "/Root/database/folder/users");
    }
}

} // Y_UNIT_TEST_SUITE(RelativeTablePathPrefix)

Y_UNIT_TEST_SUITE(QuerySplit) {

TVector<TString> Statements(const TString& query) {
    google::protobuf::Arena Arena;

    NSQLTranslation::TTranslationSettings settings;
    settings.AnsiLexer = false;
    settings.Arena = &Arena;

    TVector<TString> statements;
    NYql::TIssues issues;

    NSQLTranslationV1::TLexers lexers;
    lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
    NSQLTranslationV1::TParsers parsers;
    parsers.Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory();

    UNIT_ASSERT(NSQLTranslationV1::SplitQueryToStatements(lexers, parsers, query, statements, issues, settings));

    return statements;
}

Y_UNIT_TEST(Simple) {
    TString query = R"(
        ;
        -- Comment 1
        SELECT * From Input; -- Comment 2
        -- Comment 3
        $a = "a";

        -- Comment 9
        ;

        -- Comment 10

        -- Comment 8

        $b = ($x) -> {
        -- comment 4
        return /* Comment 5 */ $x;
        -- Comment 6
        };

        // Comment 7



        )";

    auto statements = Statements(query);

    UNIT_ASSERT_VALUES_EQUAL(statements.size(), 3);

    UNIT_ASSERT_VALUES_EQUAL(statements[0], "-- Comment 1\n        SELECT * From Input; -- Comment 2\n");
    UNIT_ASSERT_VALUES_EQUAL(statements[1], R"(-- Comment 3
        $a = "a";)");
    UNIT_ASSERT_VALUES_EQUAL(statements[2], R"(-- Comment 10

        -- Comment 8

        $b = ($x) -> {
        -- comment 4
        return /* Comment 5 */ $x;
        -- Comment 6
        };)");
}

Y_UNIT_TEST(Bad1Bad2) {
    TString query = " select1; select2;";

    auto statements = Statements(query);
    UNIT_ASSERT_VALUES_EQUAL(statements.size(), 0);
}

} // Y_UNIT_TEST_SUITE(QuerySplit)

Y_UNIT_TEST_SUITE(TestGetQueryPosition) {

Y_UNIT_TEST(TestTokenFinding) {
    const TString query = TStringBuilder() << R"(
    )" << "\r" << R"(BEGIN)" << "\r\n" << R"(
       )" << "\n\r" << R"(END
    $b = ()" << "\r\r" << R"($x) -> {

    )" << "\n" << R"(
    -- comment A
    return /*Комментарий*/ $x;
    -- Comment B
    };
    )";

    NSQLTranslationV1::TLexers lexers;
    lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();

    ui64 lexerPosition = 0;
    const auto onNextToken = [&](NSQLTranslation::TParsedToken&& token) {
        NSQLv1Generated::TToken tokenProto;
        tokenProto.SetLine(token.Line);
        tokenProto.SetColumn(token.LinePos);
        UNIT_ASSERT_VALUES_EQUAL_C(lexerPosition, NSQLTranslationV1::GetQueryPosition(query, tokenProto), token.Line << ":" << token.LinePos << ":'" << token.Content << "'");

        lexerPosition += token.Content.size();
    };

    const auto lexer = NSQLTranslationV1::MakeLexer(lexers, /*ansi=*/false);

    NYql::TIssues issues;
    const bool result = lexer->Tokenize(query, {}, onNextToken, issues, NSQLTranslation::SQL_MAX_PARSER_ERRORS);
    UNIT_ASSERT_C(result, issues.ToOneLineString());
}

Y_UNIT_TEST(TestTokenMissing) {
    const TString query = "BEGIN /*Комментарий*/ \nEND";
    NSQLv1Generated::TToken tokenProto;

    tokenProto.SetLine(3);
    tokenProto.SetColumn(0);
    UNIT_ASSERT_VALUES_EQUAL(std::string::npos, NSQLTranslationV1::GetQueryPosition(query, tokenProto));

    tokenProto.SetLine(2);
    tokenProto.SetColumn(4);
    UNIT_ASSERT_VALUES_EQUAL(std::string::npos, NSQLTranslationV1::GetQueryPosition(query, tokenProto));

    tokenProto.SetLine(1);
    tokenProto.SetColumn(34);
    UNIT_ASSERT_VALUES_EQUAL(std::string::npos, NSQLTranslationV1::GetQueryPosition(query, tokenProto));

    tokenProto.SetLine(1);
    tokenProto.SetColumn(0);
    UNIT_ASSERT_VALUES_EQUAL(0, NSQLTranslationV1::GetQueryPosition(query, tokenProto));
}
} // Y_UNIT_TEST_SUITE(TestGetQueryPosition)
