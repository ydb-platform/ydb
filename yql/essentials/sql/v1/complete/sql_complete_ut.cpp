#include "sql_complete.h"

#include <yql/essentials/sql/v1/complete/name/fallback/name_service.h>
#include <yql/essentials/sql/v1/complete/name/static/name_service.h>

#include <yql/essentials/sql/v1/lexer/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_pure/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_pure_ansi/lexer.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NSQLComplete;

class TDummyException: public std::runtime_error {
public:
    TDummyException()
        : std::runtime_error("T_T") {
    }
};

class TFailingNameService: public INameService {
public:
    TFuture<TNameResponse> Lookup(TNameRequest) override {
        auto e = std::make_exception_ptr(TDummyException());
        return NThreading::MakeErrorFuture<TNameResponse>(e);
    }
};

class TSilentNameService: public INameService {
public:
    TFuture<TNameResponse> Lookup(TNameRequest) override {
        auto promise = NThreading::NewPromise<TNameResponse>();
        return promise.GetFuture();
    }
};

Y_UNIT_TEST_SUITE(SqlCompleteTests) {
    using ECandidateKind::Keyword;
    using ECandidateKind::TypeName;

    TLexerSupplier MakePureLexerSupplier() {
        NSQLTranslationV1::TLexers lexers;
        lexers.Antlr4Pure = NSQLTranslationV1::MakeAntlr4PureLexerFactory();
        lexers.Antlr4PureAnsi = NSQLTranslationV1::MakeAntlr4PureAnsiLexerFactory();
        return [lexers = std::move(lexers)](bool ansi) {
            return NSQLTranslationV1::MakeLexer(
                lexers, ansi, /* antlr4 = */ true, 
                NSQLTranslationV1::ELexerFlavor::Pure);
        };
    }

    ISqlCompletionEngine::TPtr MakeSqlCompletionEngineUT() {
        TLexerSupplier lexer = MakePureLexerSupplier();
        INameService::TPtr names = MakeStaticNameService({
            .Types = {"Uint64"},
        });
        return MakeSqlCompletionEngine(std::move(lexer), std::move(names));
    }

    TVector<TCandidate> Complete(ISqlCompletionEngine::TPtr& engine, TStringBuf prefix) {
        return engine->Complete({prefix}).Candidates;
    }

    Y_UNIT_TEST(Beginning) {
        TVector<TCandidate> expected = {
            {Keyword, "ALTER"},
            {Keyword, "ANALYZE"},
            {Keyword, "BACKUP"},
            {Keyword, "BATCH"},
            {Keyword, "COMMIT"},
            {Keyword, "CREATE"},
            {Keyword, "DECLARE"},
            {Keyword, "DEFINE"},
            {Keyword, "DELETE"},
            {Keyword, "DISCARD"},
            {Keyword, "DO"},
            {Keyword, "DROP"},
            {Keyword, "EVALUATE"},
            {Keyword, "EXPLAIN"},
            {Keyword, "EXPORT"},
            {Keyword, "FOR"},
            {Keyword, "FROM"},
            {Keyword, "GRANT"},
            {Keyword, "IF"},
            {Keyword, "IMPORT"},
            {Keyword, "INSERT"},
            {Keyword, "PARALLEL"},
            {Keyword, "PRAGMA"},
            {Keyword, "PROCESS"},
            {Keyword, "REDUCE"},
            {Keyword, "REPLACE"},
            {Keyword, "RESTORE"},
            {Keyword, "REVOKE"},
            {Keyword, "ROLLBACK"},
            {Keyword, "SELECT"},
            {Keyword, "SHOW"},
            {Keyword, "UPDATE"},
            {Keyword, "UPSERT"},
            {Keyword, "USE"},
            {Keyword, "VALUES"},
        };

        auto engine = MakeSqlCompletionEngineUT();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {""}), expected);
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {" "}), expected);
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"  "}), expected);
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {";"}), expected);
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"; "}), expected);
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {" ; "}), expected);
    }

    Y_UNIT_TEST(Alter) {
        TVector<TCandidate> expected = {
            {Keyword, "ASYNC"},
            {Keyword, "BACKUP"},
            {Keyword, "DATABASE"},
            {Keyword, "EXTERNAL"},
            {Keyword, "GROUP"},
            {Keyword, "OBJECT"},
            {Keyword, "RESOURCE"},
            {Keyword, "SEQUENCE"},
            {Keyword, "TABLE"},
            {Keyword, "TABLESTORE"},
            {Keyword, "TOPIC"},
            {Keyword, "TRANSFER"},
            {Keyword, "USER"},
        };

        auto engine = MakeSqlCompletionEngineUT();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"ALTER "}), expected);
    }

    Y_UNIT_TEST(Create) {
        TVector<TCandidate> expected = {
            {Keyword, "ASYNC"},
            {Keyword, "BACKUP"},
            {Keyword, "EXTERNAL"},
            {Keyword, "GROUP"},
            {Keyword, "OBJECT"},
            {Keyword, "OR"},
            {Keyword, "RESOURCE"},
            {Keyword, "TABLE"},
            {Keyword, "TABLESTORE"},
            {Keyword, "TEMP"},
            {Keyword, "TEMPORARY"},
            {Keyword, "TOPIC"},
            {Keyword, "TRANSFER"},
            {Keyword, "USER"},
            {Keyword, "VIEW"},
        };

        auto engine = MakeSqlCompletionEngineUT();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"CREATE "}), expected);
    }

    Y_UNIT_TEST(Delete) {
        TVector<TCandidate> expected = {
            {Keyword, "FROM"},
        };

        auto engine = MakeSqlCompletionEngineUT();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"DELETE "}), expected);
    }

    Y_UNIT_TEST(Drop) {
        TVector<TCandidate> expected = {
            {Keyword, "ASYNC"},
            {Keyword, "BACKUP"},
            {Keyword, "EXTERNAL"},
            {Keyword, "GROUP"},
            {Keyword, "OBJECT"},
            {Keyword, "RESOURCE"},
            {Keyword, "TABLE"},
            {Keyword, "TABLESTORE"},
            {Keyword, "TOPIC"},
            {Keyword, "TRANSFER"},
            {Keyword, "USER"},
            {Keyword, "VIEW"},
        };

        auto engine = MakeSqlCompletionEngineUT();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"DROP "}), expected);
    }

    Y_UNIT_TEST(Explain) {
        TVector<TCandidate> expected = {
            {Keyword, "ALTER"},
            {Keyword, "ANALYZE"},
            {Keyword, "BACKUP"},
            {Keyword, "BATCH"},
            {Keyword, "COMMIT"},
            {Keyword, "CREATE"},
            {Keyword, "DECLARE"},
            {Keyword, "DEFINE"},
            {Keyword, "DELETE"},
            {Keyword, "DISCARD"},
            {Keyword, "DO"},
            {Keyword, "DROP"},
            {Keyword, "EVALUATE"},
            {Keyword, "EXPORT"},
            {Keyword, "FOR"},
            {Keyword, "FROM"},
            {Keyword, "GRANT"},
            {Keyword, "IF"},
            {Keyword, "IMPORT"},
            {Keyword, "INSERT"},
            {Keyword, "PARALLEL"},
            {Keyword, "PRAGMA"},
            {Keyword, "PROCESS"},
            {Keyword, "QUERY"},
            {Keyword, "REDUCE"},
            {Keyword, "REPLACE"},
            {Keyword, "RESTORE"},
            {Keyword, "REVOKE"},
            {Keyword, "ROLLBACK"},
            {Keyword, "SELECT"},
            {Keyword, "SHOW"},
            {Keyword, "UPDATE"},
            {Keyword, "UPSERT"},
            {Keyword, "USE"},
            {Keyword, "VALUES"},
        };

        auto engine = MakeSqlCompletionEngineUT();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"EXPLAIN "}), expected);
    }

    Y_UNIT_TEST(Grant) {
        TVector<TCandidate> expected = {
            {Keyword, "ALL"},
            {Keyword, "ALTER"},
            {Keyword, "CONNECT"},
            {Keyword, "CREATE"},
            {Keyword, "DESCRIBE"},
            {Keyword, "DROP"},
            {Keyword, "ERASE"},
            {Keyword, "FULL"},
            {Keyword, "GRANT"},
            {Keyword, "INSERT"},
            {Keyword, "LIST"},
            {Keyword, "MANAGE"},
            {Keyword, "MODIFY"},
            {Keyword, "REMOVE"},
            {Keyword, "SELECT"},
            {Keyword, "UPDATE"},
            {Keyword, "USE"},
        };

        auto engine = MakeSqlCompletionEngineUT();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"GRANT "}), expected);
    }

    Y_UNIT_TEST(Insert) {
        TVector<TCandidate> expected = {
            {Keyword, "INTO"},
            {Keyword, "OR"},
        };

        auto engine = MakeSqlCompletionEngineUT();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"INSERT "}), expected);
    }

    Y_UNIT_TEST(Pragma) {
        TVector<TCandidate> expected = {
            {Keyword, "ANSI"},
            {Keyword, "CALLABLE"},
            {Keyword, "DICT"},
            {Keyword, "ENUM"},
            {Keyword, "FLOW"},
            {Keyword, "LIST"},
            {Keyword, "OPTIONAL"},
            {Keyword, "RESOURCE"},
            {Keyword, "SET"},
            {Keyword, "STRUCT"},
            {Keyword, "TAGGED"},
            {Keyword, "TUPLE"},
            {Keyword, "VARIANT"},
        };

        auto engine = MakeSqlCompletionEngineUT();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"PRAGMA "}), expected);
    }

    Y_UNIT_TEST(Select) {
        TVector<TCandidate> expected = {
            {Keyword, "ALL"},
            {Keyword, "BITCAST"},
            {Keyword, "CALLABLE"},
            {Keyword, "CASE"},
            {Keyword, "CAST"},
            {Keyword, "CURRENT_DATE"},
            {Keyword, "CURRENT_TIME"},
            {Keyword, "CURRENT_TIMESTAMP"},
            {Keyword, "DICT"},
            {Keyword, "DISTINCT"},
            {Keyword, "EMPTY_ACTION"},
            {Keyword, "ENUM"},
            {Keyword, "EXISTS"},
            {Keyword, "FALSE"},
            {Keyword, "FLOW"},
            {Keyword, "JSON_EXISTS"},
            {Keyword, "JSON_QUERY"},
            {Keyword, "JSON_VALUE"},
            {Keyword, "LIST"},
            {Keyword, "NOT"},
            {Keyword, "NULL"},
            {Keyword, "OPTIONAL"},
            {Keyword, "RESOURCE"},
            {Keyword, "SET"},
            {Keyword, "STREAM"},
            {Keyword, "STRUCT"},
            {Keyword, "TAGGED"},
            {Keyword, "TRUE"},
            {Keyword, "TUPLE"},
            {Keyword, "VARIANT"},
        };

        auto engine = MakeSqlCompletionEngineUT();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"SELECT "}), expected);
    }

    Y_UNIT_TEST(Upsert) {
        TVector<TCandidate> expected = {
            {Keyword, "INTO"},
            {Keyword, "OBJECT"},
        };

        auto engine = MakeSqlCompletionEngineUT();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"UPSERT "}), expected);
    }

    Y_UNIT_TEST(TypeName) {
        TVector<TCandidate> expected = {
            {Keyword, "CALLABLE"},
            {Keyword, "DECIMAL"},
            {Keyword, "DICT"},
            {Keyword, "ENUM"},
            {Keyword, "FLOW"},
            {Keyword, "LIST"},
            {Keyword, "OPTIONAL"},
            {Keyword, "RESOURCE"},
            {Keyword, "SET"},
            {Keyword, "STREAM"},
            {Keyword, "STRUCT"},
            {Keyword, "TAGGED"},
            {Keyword, "TUPLE"},
            {Keyword, "VARIANT"},
            {TypeName, "Uint64"},
        };

        auto engine = MakeSqlCompletionEngineUT();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"CREATE TABLE table (id "}), expected);
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"SELECT CAST (1 AS "}), expected);
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"SELECT OPTIONAL<"}), expected);
    }

    Y_UNIT_TEST(UTF8Wide) {
        auto engine = MakeSqlCompletionEngineUT();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"\xF0\x9F\x98\x8A"}).size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"编码"}).size(), 0);
    }

    Y_UNIT_TEST(WordBreak) {
        auto engine = MakeSqlCompletionEngineUT();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"SELECT ("}).size(), 28);
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"SELECT (1)"}).size(), 30);
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"SELECT 1;"}).size(), 35);
    }

    Y_UNIT_TEST(Typing) {
        const auto queryUtf16 = TUtf16String::FromUtf8(
            "SELECT \n"
            "  123467, \"Hello, {name}! 编码\"}, \n"
            "  (1 + (5 * 1 / 0)), MIN(identifier), \n"
            "  Bool(field), Math::Sin(var) \n"
            "FROM `local/test/space/table` JOIN test;");

        auto engine = MakeSqlCompletionEngineUT();

        for (std::size_t size = 0; size <= queryUtf16.size(); ++size) {
            const TWtringBuf prefixUtf16(queryUtf16, 0, size);
            auto completion = engine->Complete({TString::FromUtf16(prefixUtf16)});
            Y_DO_NOT_OPTIMIZE_AWAY(completion);
        }
    }

    Y_UNIT_TEST(CaseInsensitivity) {
        TVector<TCandidate> expected = {
            {Keyword, "SELECT"},
        };

        auto engine = MakeSqlCompletionEngineUT();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "se"), expected);
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "sE"), expected);
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "Se"), expected);
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SE"), expected);
    }

    Y_UNIT_TEST(InvalidStatementsRecovery) {
        auto engine = MakeSqlCompletionEngineUT();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "select select; ").size(), 35);
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "select select;").size(), 35);
        UNIT_ASSERT_VALUES_EQUAL_C(Complete(engine, "!;").size(), 0, "Lexer failing");
    }

    Y_UNIT_TEST(DefaultNameSet) {
        auto set = MakeDefaultNameSet();
        auto service = MakeStaticNameService(std::move(set));
        auto engine = MakeSqlCompletionEngine(MakePureLexerSupplier(), std::move(service));
        {
            TVector<TCandidate> expected = {
                {TypeName, "Uint16"},
                {TypeName, "Uint32"},
                {TypeName, "Uint64"},
                {TypeName, "Uint8"},
                {TypeName, "Utf8"},
                {TypeName, "Uuid"},
            };
            UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"SELECT OPTIONAL<U"}), expected);
        }
    }

    Y_UNIT_TEST(OnFailingNameService) {
        auto service = MakeHolder<TFailingNameService>();
        auto engine = MakeSqlCompletionEngine(MakePureLexerSupplier(), std::move(service));
        UNIT_ASSERT_NO_EXCEPTION(Complete(engine, {""}));
        UNIT_ASSERT_EXCEPTION(Complete(engine, {"SELECT OPTIONAL<U"}), TDummyException);
        UNIT_ASSERT_EXCEPTION(Complete(engine, {"SELECT CAST (1 AS "}).size(), TDummyException);
    }

    Y_UNIT_TEST(OnSilentNameService) {
        auto silent = MakeHolder<TSilentNameService>();
        auto deadlined = MakeDeadlinedNameService(std::move(silent), TDuration::MilliSeconds(1));

        auto engine = MakeSqlCompletionEngine(MakePureLexerSupplier(), std::move(deadlined));
        UNIT_ASSERT_EXCEPTION(Complete(engine, {"SELECT OPTIONAL<U"}), NThreading::TFutureException);
        UNIT_ASSERT_EXCEPTION(Complete(engine, {"SELECT OPTIONAL<"}), NThreading::TFutureException);
    }

    Y_UNIT_TEST(OnFallbackNameService) {
        auto silent = MakeHolder<TSilentNameService>();
        auto primary = MakeDeadlinedNameService(std::move(silent), TDuration::MilliSeconds(1));

        auto standby = MakeStaticNameService(MakeDefaultNameSet());

        auto fallback = MakeFallbackNameService(std::move(primary), std::move(standby));

        auto engine = MakeSqlCompletionEngine(MakePureLexerSupplier(), std::move(fallback));
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"SELECT CAST (1 AS U"}).size(), 6);
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"SELECT CAST (1 AS "}).size(), 47);
    }

} // Y_UNIT_TEST_SUITE(SqlCompleteTests)
