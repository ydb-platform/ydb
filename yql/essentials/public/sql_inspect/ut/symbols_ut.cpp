#include <yql/essentials/public/sql_inspect/symbols.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql::NSqlInspect {
namespace {

TSymbols Inspect(const TString& sql, const TSymbolSettings& settings = {}) {
    const auto result = GetSymbols(sql, settings);
    if (const auto* issues = std::get_if<TIssues>(&result)) {
        UNIT_FAIL(issues->ToString());
    }
    return std::get<TSymbols>(result);
}

void AssertFailure(const TString& sql, const TSymbolSettings& settings = {}) {
    const auto result = GetSymbols(sql, settings);
    UNIT_ASSERT(std::holds_alternative<TIssues>(result));
    UNIT_ASSERT(!std::get<TIssues>(result).Empty());
}

void AssertAnsiIdentifiers(const TString& prefix, const TSymbolSettings& settings) {
    const auto symbols = Inspect(prefix + R"(
        IMPORT "module" SYMBOLS $"foo" AS $"hidden";
        IMPORT module SYMBOLS $foo;
        EXPORT $"foo", $foo;
        DECLARE $"foo" AS Int32;
        DECLARE $foo AS Int32;
    )", settings);
    UNIT_ASSERT_VALUES_EQUAL(symbols.Imports.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(symbols.Imports[0].Module, (TVector<TString>{"module"}));
    UNIT_ASSERT_VALUES_EQUAL(symbols.Imports[0].Name, "$foo");
    UNIT_ASSERT_VALUES_EQUAL(symbols.Exports.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(symbols.Exports[0].Name, "$foo");
    UNIT_ASSERT_VALUES_EQUAL(symbols.Parameters.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(symbols.Parameters[0].Name, "$foo");
}

} // namespace

Y_UNIT_TEST_SUITE(SqlInspect) {
Y_UNIT_TEST(ImportsAndExports) {
    const auto symbols = Inspect("IMPORT foo.bar SYMBOLS $original AS $alias, $other; EXPORT $out, $next;");
    UNIT_ASSERT_VALUES_EQUAL(symbols.Imports.size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(symbols.Imports[0].Name, "$original");
    UNIT_ASSERT_VALUES_EQUAL(symbols.Imports[0].Module, (TVector<TString>{"foo", "bar"}));
    UNIT_ASSERT_VALUES_EQUAL(symbols.Imports[1].Name, "$other");
    UNIT_ASSERT_VALUES_EQUAL(symbols.Imports[1].Module, (TVector<TString>{"foo", "bar"}));
    UNIT_ASSERT_VALUES_EQUAL(symbols.Exports.size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(symbols.Exports[0].Name, "$next");
    UNIT_ASSERT_VALUES_EQUAL(symbols.Exports[1].Name, "$out");
}

Y_UNIT_TEST(Filters) {
    for (const bool imports : {false, true}) {
        for (const bool exports : {false, true}) {
            for (const bool parameters : {false, true}) {
                TSymbolSettings settings{.Filter = TSet<ESymbolType>{}};
                if (imports) {
                    settings.Filter->insert(ESymbolType::Import);
                }
                if (exports) {
                    settings.Filter->insert(ESymbolType::Export);
                }
                if (parameters) {
                    settings.Filter->insert(ESymbolType::Parameter);
                }
                const auto symbols = Inspect("IMPORT foo SYMBOLS $x; EXPORT $y; DECLARE $z AS Int32;", settings);
                UNIT_ASSERT_VALUES_EQUAL(symbols.Imports.size(), imports ? 1 : 0);
                UNIT_ASSERT_VALUES_EQUAL(symbols.Exports.size(), exports ? 1 : 0);
                UNIT_ASSERT_VALUES_EQUAL(symbols.Parameters.size(), parameters ? 1 : 0);
                AssertFailure("IMPORT foo SYMBOLS;", settings);
                AssertFailure("DECLARE $z AS;", settings);
            }
        }
    }
}

Y_UNIT_TEST(EmptyAndUnrelatedSql) {
    for (const TString sql : {"", "-- comment", ";;;", "SELECT $undefined FROM unknown;", "SELECT 'IMPORT m SYMBOLS $x; EXPORT $y;';"}) {
        const auto symbols = Inspect(sql);
        UNIT_ASSERT(symbols.Imports.empty());
        UNIT_ASSERT(symbols.Exports.empty());
        UNIT_ASSERT(symbols.Parameters.empty());
    }
}

Y_UNIT_TEST(OrderDuplicatesAndNestedImports) {
    const auto symbols = Inspect(
        "IMPORT first SYMBOLS $x; "
        "$f = () -> { IMPORT second SYMBOLS $y AS $local; RETURN $local; }; "
        "IMPORT first SYMBOLS $x AS $differentAlias; IMPORT third SYMBOLS $x; "
        "IMPORT first SYMBOLS $y, $x; EXPORT $z, $z; EXPORT $next, $z, $Z, $`next`;");
    UNIT_ASSERT_VALUES_EQUAL(symbols.Imports.size(), 4);
    UNIT_ASSERT_VALUES_EQUAL(symbols.Imports[0].Module, (TVector<TString>{"first"}));
    UNIT_ASSERT_VALUES_EQUAL(symbols.Imports[0].Name, "$x");
    UNIT_ASSERT_VALUES_EQUAL(symbols.Imports[1].Module, (TVector<TString>{"first"}));
    UNIT_ASSERT_VALUES_EQUAL(symbols.Imports[1].Name, "$y");
    UNIT_ASSERT_VALUES_EQUAL(symbols.Imports[2].Name, "$y");
    UNIT_ASSERT_VALUES_EQUAL(symbols.Imports[2].Module, (TVector<TString>{"second"}));
    UNIT_ASSERT_VALUES_EQUAL(symbols.Imports[3].Name, "$x");
    UNIT_ASSERT_VALUES_EQUAL(symbols.Imports[3].Module, (TVector<TString>{"third"}));
    UNIT_ASSERT_VALUES_EQUAL(symbols.Exports.size(), 3);
    UNIT_ASSERT_VALUES_EQUAL(symbols.Exports[0].Name, "$Z");
    UNIT_ASSERT_VALUES_EQUAL(symbols.Exports[1].Name, "$next");
    UNIT_ASSERT_VALUES_EQUAL(symbols.Exports[2].Name, "$z");
}

Y_UNIT_TEST(QuotedNames) {
    const auto symbols = Inspect("IMPORT .`my module`.`file.sql` SYMBOLS $`a``b` AS $`hidden`; EXPORT $``, $`a\\x62`;");
    UNIT_ASSERT_VALUES_EQUAL(symbols.Imports.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(symbols.Imports[0].Module, (TVector<TString>{"my module", "file.sql"}));
    UNIT_ASSERT(symbols.Imports[0].IsAbsolute);
    UNIT_ASSERT_VALUES_EQUAL(symbols.Imports[0].Name, "$a`b");
    UNIT_ASSERT_VALUES_EQUAL(symbols.Exports.size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(symbols.Exports[0].Name, "$");
    UNIT_ASSERT_VALUES_EQUAL(symbols.Exports[1].Name, "$ab");
}

Y_UNIT_TEST(DistinctModulePaths) {
    const auto symbols = Inspect("IMPORT foo.bar SYMBOLS $x; IMPORT `foo.bar` SYMBOLS $x;");
    UNIT_ASSERT_VALUES_EQUAL(symbols.Imports.size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(symbols.Imports[0].Module, (TVector<TString>{"foo", "bar"}));
    UNIT_ASSERT_VALUES_EQUAL(symbols.Imports[1].Module, (TVector<TString>{"foo.bar"}));
    UNIT_ASSERT_VALUES_EQUAL(symbols.Imports[0].Name, "$x");
    UNIT_ASSERT_VALUES_EQUAL(symbols.Imports[1].Name, "$x");
}

Y_UNIT_TEST(DollarInQuotedNames) {
    const auto symbols = Inspect("IMPORT `$module` SYMBOLS $`$name`; EXPORT $`$name`; DECLARE $`$name` AS Int32;");
    UNIT_ASSERT_VALUES_EQUAL(symbols.Imports[0].Module, (TVector<TString>{"$module"}));
    UNIT_ASSERT_VALUES_EQUAL(symbols.Imports[0].Name, "$$name");
    UNIT_ASSERT_VALUES_EQUAL(symbols.Exports[0].Name, "$$name");
    UNIT_ASSERT_VALUES_EQUAL(symbols.Parameters[0].Name, "$$name");
}

Y_UNIT_TEST(ModuleComponentsAndDeduplication) {
    const auto symbols = Inspect(
        "IMPORT foo.bar SYMBOLS $x; IMPORT `foo`.`bar` SYMBOLS $x; "
        "IMPORT .foo.bar SYMBOLS $x; IMPORT foo.``.bar SYMBOLS $x; "
        "IMPORT `foo/bar` SYMBOLS $x;");
    UNIT_ASSERT_VALUES_EQUAL(symbols.Imports.size(), 4);
    UNIT_ASSERT_VALUES_EQUAL(symbols.Imports[0].Module, (TVector<TString>{"foo", "", "bar"}));
    UNIT_ASSERT_VALUES_EQUAL(symbols.Imports[1].Module, (TVector<TString>{"foo", "bar"}));
    UNIT_ASSERT_VALUES_EQUAL(symbols.Imports[2].Module, (TVector<TString>{"foo/bar"}));
    UNIT_ASSERT_VALUES_EQUAL(symbols.Imports[3].Module, (TVector<TString>{"foo", "bar"}));
    UNIT_ASSERT(!symbols.Imports[0].IsAbsolute);
    UNIT_ASSERT(!symbols.Imports[1].IsAbsolute);
    UNIT_ASSERT(!symbols.Imports[2].IsAbsolute);
    UNIT_ASSERT(symbols.Imports[3].IsAbsolute);
    for (const auto& imported : symbols.Imports) {
        UNIT_ASSERT_VALUES_EQUAL(imported.Name, "$x");
    }
}

Y_UNIT_TEST(AbsoluteModuleFlag) {
    const auto symbols = Inspect(
        "IMPORT .foo SYMBOLS $x; IMPORT foo SYMBOLS $x; IMPORT .`foo` SYMBOLS $x; "
        "IMPORT ``.foo SYMBOLS $x; IMPORT .``.foo SYMBOLS $x;");
    UNIT_ASSERT_VALUES_EQUAL(symbols.Imports.size(), 4);
    UNIT_ASSERT_VALUES_EQUAL(symbols.Imports[0].Module, (TVector<TString>{"", "foo"}));
    UNIT_ASSERT_VALUES_EQUAL(symbols.Imports[1].Module, (TVector<TString>{"foo"}));
    UNIT_ASSERT_VALUES_EQUAL(symbols.Imports[2].Module, (TVector<TString>{"", "foo"}));
    UNIT_ASSERT_VALUES_EQUAL(symbols.Imports[3].Module, (TVector<TString>{"foo"}));
    UNIT_ASSERT(!symbols.Imports[0].IsAbsolute);
    UNIT_ASSERT(!symbols.Imports[1].IsAbsolute);
    UNIT_ASSERT(symbols.Imports[2].IsAbsolute);
    UNIT_ASSERT(symbols.Imports[3].IsAbsolute);
}

Y_UNIT_TEST(AnsiModuleComponents) {
    const TString sql = R"(IMPORT foo.bar SYMBOLS $x; IMPORT "foo"."bar" SYMBOLS $x; IMPORT "foo.bar" SYMBOLS $x;)";
    for (const bool explicitAnsi : {false, true}) {
        const auto symbols = Inspect(explicitAnsi ? sql : "--!ansi_lexer\n" + sql, {.Ansi = explicitAnsi});
        UNIT_ASSERT_VALUES_EQUAL(symbols.Imports.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(symbols.Imports[0].Module, (TVector<TString>{"foo", "bar"}));
        UNIT_ASSERT_VALUES_EQUAL(symbols.Imports[1].Module, (TVector<TString>{"foo.bar"}));
    }
}

Y_UNIT_TEST(AnsiFlagAndInference) {
    const TString sql = R"(IMPORT "my""module" SYMBOLS $"na""me" AS $alias; EXPORT $"out";)";
    for (const bool explicitAnsi : {false, true}) {
        const auto symbols = Inspect(explicitAnsi ? sql : "--!ansi_lexer\n" + sql, {.Ansi = explicitAnsi});
        UNIT_ASSERT_VALUES_EQUAL(symbols.Imports.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(symbols.Imports[0].Name, "$na\"me");
        UNIT_ASSERT_VALUES_EQUAL(symbols.Imports[0].Module, (TVector<TString>{"my\"module"}));
        UNIT_ASSERT_VALUES_EQUAL(symbols.Exports.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(symbols.Exports[0].Name, "$out");
    }
    AssertFailure(sql);
}

Y_UNIT_TEST(AnsiIdentifiersViaFirstLine) {
    AssertAnsiIdentifiers("--!ansi_lexer\n", {});
}

Y_UNIT_TEST(AnsiIdentifiersViaOption) {
    AssertAnsiIdentifiers("", {.Ansi = true});
}

Y_UNIT_TEST(RejectDoubleQuotesWithoutAnsi) {
    for (const TString sql : {"EXPORT $\"foo\";", "DECLARE $\"foo\" AS Int32;", "IMPORT \"module\" SYMBOLS $x;", "IMPORT module SYMBOLS $\"foo\";"}) {
        AssertFailure(sql);
        AssertFailure(sql, {.Filter = TSet<ESymbolType>{}});
    }
}

Y_UNIT_TEST(RejectAnsiIdentifierSuffix) {
    AssertFailure("EXPORT $\"foo\"u;", {.Ansi = true});
}

Y_UNIT_TEST(RejectStringSuffixesInIdentifiers) {
    for (const TString suffix : {"s", "u", "y", "j", "p", "pt", "pb", "pv", "U"}) {
        const TString identifier = "\"foo\"" + suffix;
        for (const TString& sql : {
                 "EXPORT $" + identifier + ";",
                 "IMPORT module SYMBOLS $" + identifier + ";",
                 "IMPORT " + identifier + " SYMBOLS $foo;",
                 "DECLARE $" + identifier + " AS Int32;"}) {
            AssertFailure(sql, {.Ansi = true});
            AssertFailure("--!ansi_lexer\n" + sql, {.Filter = TSet<ESymbolType>{}});
        }
    }
}

Y_UNIT_TEST(EmptyAnsiIdentifier) {
    const auto symbols = Inspect("EXPORT $\"\";", {.Ansi = true});
    UNIT_ASSERT_VALUES_EQUAL(symbols.Exports.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(symbols.Exports[0].Name, "$");
}

Y_UNIT_TEST(RejectSingleQuotedIdentifier) {
    AssertFailure("EXPORT $'foo';");
}

Y_UNIT_TEST(RejectMultilineIdentifier) {
    AssertFailure("EXPORT $@@foo@@;");
}

Y_UNIT_TEST(RejectStringLiteralNames) {
    for (const TString literal : {"'foo'", "@@foo@@", "''", "@@@@", "'foo'u", "@@foo@@u"}) {
        for (const TString& sql : {
                 "EXPORT $" + literal + ";",
                 "DECLARE $" + literal + " AS Int32;",
                 "IMPORT " + literal + " SYMBOLS $x;",
                 "IMPORT module SYMBOLS $" + literal + ";"}) {
            for (const bool ansi : {false, true}) {
                AssertFailure(sql, {.Ansi = ansi});
                AssertFailure(sql, {.Ansi = ansi, .Filter = TSet<ESymbolType>{}});
            }
            AssertFailure("--!ansi_lexer\n" + sql);
        }
    }
}

Y_UNIT_TEST(StringLiteralsRemainValidValues) {
    for (const bool ansi : {false, true}) {
        const auto symbols = Inspect("DECLARE $foo AS String = 'value'; SELECT 'foo', @@bar@@;", {.Ansi = ansi});
        UNIT_ASSERT(symbols.Imports.empty());
        UNIT_ASSERT(symbols.Exports.empty());
        UNIT_ASSERT_VALUES_EQUAL(symbols.Parameters.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(symbols.Parameters[0].Name, "$foo");
    }
}

Y_UNIT_TEST(EmptyNamesAndNormalizedDuplicates) {
    const auto symbols = Inspect("IMPORT `` SYMBOLS $``, $``, $x; IMPORT `` SYMBOLS $`x`; EXPORT $``, $``, $x, $`x`;");
    UNIT_ASSERT_VALUES_EQUAL(symbols.Imports.size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(symbols.Imports[0].Module, (TVector<TString>{""}));
    UNIT_ASSERT_VALUES_EQUAL(symbols.Imports[0].Name, "$");
    UNIT_ASSERT_VALUES_EQUAL(symbols.Imports[1].Module, (TVector<TString>{""}));
    UNIT_ASSERT_VALUES_EQUAL(symbols.Imports[1].Name, "$x");
    UNIT_ASSERT_VALUES_EQUAL(symbols.Exports.size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(symbols.Exports[0].Name, "$");
    UNIT_ASSERT_VALUES_EQUAL(symbols.Exports[1].Name, "$x");
}

Y_UNIT_TEST(InvalidIdentifierEscape) {
    AssertFailure("EXPORT $`\\xQQ`;");
    AssertFailure("IMPORT `\\xQQ` SYMBOLS $x;");
    AssertFailure("DECLARE $`\\xQQ` AS Int32;");
}

Y_UNIT_TEST(InvalidExportWithFilters) {
    AssertFailure("EXPORT $`\\xQQ`;", {.Filter = TSet<ESymbolType>{}});
    AssertFailure("EXPORT $`\\xQQ`;", {.Filter = TSet<ESymbolType>{ESymbolType::Import}});
}

Y_UNIT_TEST(InvalidImportWithFilters) {
    AssertFailure("IMPORT foo SYMBOLS $`\\xQQ`;", {.Filter = TSet<ESymbolType>{}});
    AssertFailure("IMPORT `\\xQQ` SYMBOLS $foo;", {.Filter = TSet<ESymbolType>{ESymbolType::Export}});
}

Y_UNIT_TEST(InvalidParameterWithFilters) {
    AssertFailure("DECLARE $`\\xQQ` AS Int32;", {.Filter = TSet<ESymbolType>{}});
    AssertFailure("DECLARE $`\\xQQ` AS Int32;", {.Filter = TSet<ESymbolType>{ESymbolType::Export}});
}

Y_UNIT_TEST(DeclaredParameters) {
    const auto symbols = Inspect(R"(
        DECLARE $z AS Optional<Int32>;
        DECLARE $ab AS String = "default";
        DECLARE $`a\x62` AS String;
        DECLARE $`` AS Int32;
        DECLARE $`` AS Int32;
        DECLARE $A AS List<String>;
        $local = 1;
        SELECT $reference, $local, $ab;
    )");
    UNIT_ASSERT(symbols.Imports.empty());
    UNIT_ASSERT(symbols.Exports.empty());
    UNIT_ASSERT_VALUES_EQUAL(symbols.Parameters.size(), 4);
    UNIT_ASSERT_VALUES_EQUAL(symbols.Parameters[0].Name, "$");
    UNIT_ASSERT_VALUES_EQUAL(symbols.Parameters[1].Name, "$A");
    UNIT_ASSERT_VALUES_EQUAL(symbols.Parameters[2].Name, "$ab");
    UNIT_ASSERT_VALUES_EQUAL(symbols.Parameters[3].Name, "$z");
}

Y_UNIT_TEST(InternalVariablesAreNotParameters) {
    const auto symbols = Inspect(R"($`a\x62` = 1; SELECT $ab;)");
    UNIT_ASSERT(symbols.Imports.empty());
    UNIT_ASSERT(symbols.Exports.empty());
    UNIT_ASSERT(symbols.Parameters.empty());
}

Y_UNIT_TEST(LanguageVersionSettings) {
    UNIT_ASSERT_VALUES_EQUAL(TSymbolSettings{}.LangVer, MinLangVersion);
    for (const TLangVersion version : {MinLangVersion, GetMaxLangVersion()}) {
        const TSymbolSettings settings{.LangVer = version};
        const auto symbols = Inspect("DECLARE $foo AS Int32;", settings);
        UNIT_ASSERT_VALUES_EQUAL(symbols.Parameters.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(symbols.Parameters[0].Name, "$foo");
    }
}

Y_UNIT_TEST(SyntaxFailures) {
    for (const TString sql : {"IMPORT foo SYMBOLS;", "EXPORT;", "EXPORT $x; SELECT (", "/*", "--!syntax_v0\nSELECT 1;", "--!syntax_pg\nSELECT 1;"}) {
        AssertFailure(sql);
    }
    UNIT_ASSERT_VALUES_EQUAL(Inspect("--!syntax_v1\nEXPORT $x;").Exports.size(), 1);
}

Y_UNIT_TEST(RejectSyntaxV2) {
    AssertFailure("--!syntax_v2\nEXPORT $x;");
}

Y_UNIT_TEST(RejectCustomSyntax) {
    AssertFailure("--!syntax_mock\nEXPORT $x;");
}

Y_UNIT_TEST(IssuePositions) {
    const auto result = GetSymbols("IMPORT foo SYMBOLS $x;\nEXPORT;");
    UNIT_ASSERT(std::holds_alternative<TIssues>(result));
    const auto& issues = std::get<TIssues>(result);
    UNIT_ASSERT(!issues.Empty());
    UNIT_ASSERT_VALUES_EQUAL(issues.begin()->Position.Row, 2);
}

} // Y_UNIT_TEST_SUITE(SqlInspect)

} // namespace NYql::NSqlInspect
