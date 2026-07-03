#include <library/cpp/testing/unittest/registar.h>

#include "yql_library_compiler.h"

namespace NYql {

Y_UNIT_TEST_SUITE(TLibraryCompilerTests) {

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
