#include <library/cpp/testing/unittest/registar.h>

#include "yql_library_compiler.h"
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/translation/sql.h>

namespace NYql {

Y_UNIT_TEST_SUITE(TLibraryCompilerTests) {

Y_UNIT_TEST(ProjectSettingsAreInheritedByChildResolver) {
    auto settings = std::make_shared<TProjectSettings>();
    settings->AddPragma("yson.Strict", "false");
    TModuleResolver resolver(NSQLTranslation::TTranslators(nullptr, nullptr, nullptr), TModulesTable{}, 0, {}, {});
    resolver.SetProjectSettings(settings);

    auto child = resolver.CreateMutableChild();
    UNIT_ASSERT(child->GetProjectSettings() == settings);
    UNIT_ASSERT(child->CreateMutableChild()->GetProjectSettings() == settings);
    child->SetProjectSettings({});
    UNIT_ASSERT(!child->GetProjectSettings());
    UNIT_ASSERT(resolver.GetProjectSettings() == settings);
}

Y_UNIT_TEST(ChildResolverWithoutProjectSettings) {
    TModuleResolver resolver(NSQLTranslation::TTranslators(nullptr, nullptr, nullptr), TModulesTable{}, 0, {}, {});
    UNIT_ASSERT(!resolver.CreateMutableChild()->GetProjectSettings());
}

NSQLTranslation::TTranslators MakeSqlTranslators() {
    NSQLTranslationV1::TLexers lexers;
    lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
    NSQLTranslationV1::TParsers parsers;
    parsers.Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory(/*isAmbiguityError=*/true, /*isAmbiguityDebugging=*/false);
    return NSQLTranslation::TTranslators(nullptr, NSQLTranslationV1::MakeTranslator(lexers, parsers), nullptr);
}

Y_UNIT_TEST(SqlLibraryUsesOnlyScopedProjectSettings) {
    const auto translators = MakeSqlTranslators();
    TProjectSettings settings;
    settings.AddPragma("Unicode_Literals", TVector<TString>{});
    settings.AddPragma("ErrorMsg", "must not affect library compilation");
    settings.AddPragma("yson.Strict", "invalid");
    settings.AddPragma("config.flags", TVector<TString>{"LLVM_OFF"});
    TExprContext ctx;
    TLibraryCohesion library;
    UNIT_ASSERT_C(CompileLibrary(translators, "/lib/scoped.sql", "$value = 'text'; EXPORT $value;", ctx, library, /*optimize=*/true, settings),
                  ctx.IssueManager.GetIssues().ToString());
    UNIT_ASSERT(library.Exports.Symbols().at("$value")->IsCallable("Utf8"));
}

Y_UNIT_TEST(DynamicSqlLibraryUsesOnlyScopedProjectSettings) {
    const auto translators = MakeSqlTranslators();
    for (bool useProjectSettings : {false, true}) {
        TModuleResolver resolver(translators, TModulesTable{}, 0, {}, {});
        if (useProjectSettings) {
            auto settings = std::make_shared<TProjectSettings>();
            settings->AddPragma("RelativePathPrefix", "folder");
            settings->AddPragma("Unicode_Literals", TVector<TString>{});
            settings->AddPragma("ErrorMsg", "must not affect dynamic libraries");
            resolver.SetProjectSettings(settings);
        }
        TExprContext ctx;
        TString moduleName;
        const bool added = resolver.AddFromMemory("/lib/dynamic.sql", "$value = 'text'; EXPORT $value;", ctx, 1, 0, {}, moduleName);
        UNIT_ASSERT_C(added, ctx.IssueManager.GetIssues().ToString());
        UNIT_ASSERT_C(resolver.Link(ctx), ctx.IssueManager.GetIssues().ToString());
        const auto* module = resolver.GetModule(moduleName);
        UNIT_ASSERT(module);
        UNIT_ASSERT(module->Symbols().at("$value")->IsCallable(useProjectSettings ? "Utf8" : "String"));
    }
}

const char* alias = "/lib/ut.yqls";

bool CompileAndLink(const THashMap<TString, TString>& libs, TExprContext& ctx) {
    NSQLTranslation::TTranslators translators(
        nullptr,
        nullptr,
        nullptr);

    THashMap<TString, TLibraryCohesion> compiled;
    for (const auto& lib : libs) {
        if (!CompileLibrary(translators, alias, lib.second, ctx, compiled[lib.first])) {
            return false;
        }
    }

    return LinkLibraries(compiled, ctx, ctx);
}

Y_UNIT_TEST(OnlyExportsTest) {
    const auto s = "(\n"
                   "(let X 'Y)\n"
                   "(let ex '42)\n"
                   "(export ex)\n"
                   "(export X)\n"
                   ")\n";

    NSQLTranslation::TTranslators translators(
        nullptr,
        nullptr,
        nullptr);

    TExprContext ctx;
    TLibraryCohesion cohesion;
    UNIT_ASSERT(CompileLibrary(translators, alias, s, ctx, cohesion));
    UNIT_ASSERT_VALUES_EQUAL(2, cohesion.Exports.Symbols().size());
    UNIT_ASSERT(cohesion.Imports.empty());
}

Y_UNIT_TEST(ExportAndImportsTest) {
    const auto s = "(\n"
                   "(import math_module '"
                   "/lib/yql/math.yqls"
                   ")\n"
                   "(let mySqr2 (bind math_module 'sqr2))\n"
                   "(let mySqr3 (bind math_module 'sqr3))\n"
                   "(let ex '42)\n"
                   "(export ex)\n"
                   ")\n";

    NSQLTranslation::TTranslators translators(
        nullptr,
        nullptr,
        nullptr);

    TExprContext ctx;
    TLibraryCohesion cohesion;
    UNIT_ASSERT(CompileLibrary(translators, alias, s, ctx, cohesion));
    UNIT_ASSERT_VALUES_EQUAL(1, cohesion.Exports.Symbols().size());
    UNIT_ASSERT_VALUES_EQUAL(2, cohesion.Imports.size());
}

Y_UNIT_TEST(TestImportSelf) {
    const auto xxx = "(\n"
                     "(import math_module '"
                     "/lib/yql/xxx.yqls"
                     ")\n"
                     "(let myXxx (bind math_module 'xxx))\n"
                     "(let sqr (lambda '(x) (Apply myXxx x)))\n"
                     "(export sqr)\n"
                     ")\n";

    const THashMap<TString, TString> libs = {
        {"/lib/yql/xxx.yqls", xxx}};

    TExprContext ctx;
    UNIT_ASSERT(!CompileAndLink(libs, ctx));
    UNIT_ASSERT_VALUES_EQUAL("/lib/ut.yqls:3:13: Error: Library '/lib/yql/xxx.yqls' tries to import itself.\n", ctx.IssueManager.GetIssues().ToString());
}

Y_UNIT_TEST(TestMissedModule) {
    const auto aaa = "(\n"
                     "(import math_module '"
                     "/lib/yql/xxx.yqls"
                     ")\n"
                     "(let myXxx (bind math_module 'xxx))\n"
                     "(let sqr (lambda '(x) (Apply x myXxx)))\n"
                     "(export sqr)\n"
                     ")\n";

    const THashMap<TString, TString> libs = {
        {"/lib/yql/aaa.yqls", aaa}};

    TExprContext ctx;
    UNIT_ASSERT(!CompileAndLink(libs, ctx));
    UNIT_ASSERT_VALUES_EQUAL("/lib/ut.yqls:3:13: Error: Library '/lib/yql/aaa.yqls' has unresolved dependency from '/lib/yql/xxx.yqls'.\n", ctx.IssueManager.GetIssues().ToString());
}

Y_UNIT_TEST(TestUnresolvedSymbol) {
    const auto one = "(\n"
                     "(import math_module '"
                     "/lib/yql/two.yqls"
                     ")\n"
                     "(let myTwo (bind math_module 'zzz))\n"
                     "(let one (lambda '(x) (Apply myTwo x)))\n"
                     "(export one)\n"
                     ")\n";

    const auto two = "(\n"
                     "(let two (lambda '(x) (+ x x)))\n"
                     "(export two)\n"
                     ")\n";

    const THashMap<TString, TString> libs = {
        {"/lib/yql/one.yqls", one},
        {"/lib/yql/two.yqls", two}};

    TExprContext ctx;
    UNIT_ASSERT(!CompileAndLink(libs, ctx));
    UNIT_ASSERT_VALUES_EQUAL("/lib/ut.yqls:3:13: Error: Library '/lib/yql/one.yqls' has unresolved symbol 'zzz' from '/lib/yql/two.yqls'.\n", ctx.IssueManager.GetIssues().ToString());
}

Y_UNIT_TEST(TestCrossReference) {
    const auto one = "(\n"
                     "(import math_module '"
                     "/lib/yql/two.yqls"
                     ")\n"
                     "(let myTwo (bind math_module 'two))\n"
                     "(let one (lambda '(x) (Apply myTwo x)))\n"
                     "(export one)\n"
                     ")\n";

    const auto two = "(\n"
                     "(import math_module '"
                     "/lib/yql/one.yqls"
                     ")\n"
                     "(let myOne (bind math_module 'one))\n"
                     "(let two (lambda '(x) (Apply myOne x)))\n"
                     "(export two)\n"
                     ")\n";

    const THashMap<TString, TString> libs = {
        {"/lib/yql/one.yqls", one},
        {"/lib/yql/two.yqls", two}};

    TExprContext ctx;
    UNIT_ASSERT(!CompileAndLink(libs, ctx));
    UNIT_ASSERT(ctx.IssueManager.GetIssues().ToString().Contains("Cross reference detected"));
}

Y_UNIT_TEST(TestCrorssDependencyWithoutCrossReference) {
    const auto one = "(\n"
                     "(import math_module '"
                     "/lib/yql/two.yqls"
                     ")\n"
                     "(let myTwo (bind math_module 'two))\n"
                     "(let one (lambda '(x) (Apply myTwo x)))\n"
                     "(export one)\n"
                     ")\n";

    const auto two = "(\n"
                     "(import math_module '"
                     "/lib/yql/one.yqls"
                     ")\n"
                     "(let myOne (bind math_module 'one))\n"
                     "(let two (lambda '(x) (+ x x)))\n"
                     "(export two)\n"
                     "(let exp (lambda '(x) (Apply myOne x)))\n"
                     "(export exp)\n"
                     ")\n";

    const THashMap<TString, TString> libs = {
        {"/lib/yql/one.yqls", one},
        {"/lib/yql/two.yqls", two}};

    TExprContext ctx;
    UNIT_ASSERT(CompileAndLink(libs, ctx));
}

Y_UNIT_TEST(TestCircleReference) {
    const auto one = "(\n"
                     "(import math_module '"
                     "/lib/yql/two.yqls"
                     ")\n"
                     "(let myTwo (bind math_module 'two))\n"
                     "(let one (lambda '(x) (Apply myTwo x)))\n"
                     "(export one)\n"
                     ")\n";

    const auto two = "(\n"
                     "(import math_module '"
                     "/lib/yql/xxx.yqls"
                     ")\n"
                     "(let myXxx (bind math_module 'xxx))\n"
                     "(let two (lambda '(x) (Apply myXxx x)))\n"
                     "(export two)\n"
                     ")\n";

    const auto xxx = "(\n"
                     "(import math_module '"
                     "/lib/yql/one.yqls"
                     ")\n"
                     "(let myOne (bind math_module 'one))\n"
                     "(let xxx (lambda '(x) (Apply myOne x)))\n"
                     "(export xxx)\n"
                     ")\n";

    const THashMap<TString, TString> libs = {
        {"/lib/yql/one.yqls", one},
        {"/lib/yql/two.yqls", two},
        {"/lib/yql/xxx.yqls", xxx}};

    TExprContext ctx;
    UNIT_ASSERT(!CompileAndLink(libs, ctx));
    UNIT_ASSERT(ctx.IssueManager.GetIssues().ToString().Contains("Cross reference detected"));
}

Y_UNIT_TEST(TestForwarding) {
    const auto one = "(\n"
                     "(let one '1)\n"
                     "(export one)\n"
                     ")\n";

    const auto two = "(\n"
                     "(import math_module '"
                     "/lib/yql/one.yqls"
                     ")\n"
                     "(let myOne (bind math_module 'one))\n"
                     "(export myOne)\n"
                     ")\n";

    const auto xxx = "(\n"
                     "(import math_module '"
                     "/lib/yql/two.yqls"
                     ")\n"
                     "(let xxx (bind math_module 'myOne))\n"
                     "(export xxx)\n"
                     ")\n";

    const THashMap<TString, TString> libs = {
        {"/lib/yql/one.yqls", one},
        {"/lib/yql/two.yqls", two},
        {"/lib/yql/xxx.yqls", xxx}};

    TExprContext ctx;
    UNIT_ASSERT(CompileAndLink(libs, ctx));
}
} // Y_UNIT_TEST_SUITE(TLibraryCompilerTests)

} // namespace NYql
