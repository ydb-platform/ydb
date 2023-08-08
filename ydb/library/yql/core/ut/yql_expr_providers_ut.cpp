#include <ydb/library/yql/core/type_ann/type_ann_core.h>
#include <ydb/library/yql/core/type_ann/type_ann_expr.h>

#include <ydb/library/yql/ast/yql_ast_annotation.h>
#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/ut_common/yql_ut_common.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/facade/yql_facade.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_provider.h>
#include <ydb/library/yql/providers/yt/gateway/file/yql_yt_file.h>
#include <ydb/library/yql/providers/yt/gateway/file/yql_yt_file_services.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/hex.h>
#include <util/random/random.h>
#include <util/system/sanitizers.h>

namespace NYql {

Y_UNIT_TEST_SUITE(TCompileWithProvidersYqlExpr) {
    static TAstParseResult ParseAstWithCheck(const TStringBuf& s) {
        TAstParseResult res = ParseAst(s);
        res.Issues.PrintTo(Cout);
        UNIT_ASSERT(res.IsOk());
        return res;
    }

    static void CompileExprWithCheck(TAstNode& root, TExprNode::TPtr& exprRoot, TExprContext& exprCtx,
        ui32 annotationFlags = TExprAnnotationFlags::None)
    {
        const bool success = CompileExpr(root, exprRoot, exprCtx, nullptr, nullptr, annotationFlags);
        exprCtx.IssueManager.GetIssues().PrintTo(Cout);

        UNIT_ASSERT(success);
        UNIT_ASSERT_VALUES_EQUAL(exprRoot->GetState(), annotationFlags != TExprAnnotationFlags::None
            ? TExprNode::EState::TypeComplete
            : TExprNode::EState::Initial);
    }

    static void AnnotateExprWithCheck(TExprNode::TPtr& root, TExprContext& ctx, bool wholeProgram, TTypeAnnotationContextPtr typeAnnotationContext) {
        const bool success = SyncAnnotateTypes(root, ctx, wholeProgram, *typeAnnotationContext);
        ctx.IssueManager.GetIssues().PrintTo(Cout);
        UNIT_ASSERT(success);
    }

    static void VerifyProgram(const TString& s) {
        TAstParseResult astRes = ParseAstWithCheck(s);
        TExprContext exprCtx;
        TExprNode::TPtr exprRoot;
        CompileExprWithCheck(*astRes.Root, exprRoot, exprCtx);
        UNIT_ASSERT(!exprRoot->GetTypeAnn());

        auto functionRegistry = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry());
        TTestTablesMapping testTables;

        auto yqlNativeServices = NFile::TYtFileServices::Make(functionRegistry.Get(), testTables);
        auto ytGateway = CreateYtFileGateway(yqlNativeServices);
        auto typeAnnotationContext = MakeIntrusive<TTypeAnnotationContext>();
        auto ytState = MakeIntrusive<TYtState>();
        ytState->Gateway = ytGateway;
        ytState->Types = typeAnnotationContext.Get();

        InitializeYtGateway(ytGateway, ytState);
        typeAnnotationContext->AddDataSink(YtProviderName, CreateYtDataSink(ytState));
        typeAnnotationContext->AddDataSource(YtProviderName, CreateYtDataSource(ytState));
        auto randomProvider = CreateDeterministicRandomProvider(1);
        typeAnnotationContext->RandomProvider = randomProvider;
        AnnotateExprWithCheck(exprRoot, exprCtx, true, typeAnnotationContext);
        UNIT_ASSERT(exprRoot->GetTypeAnn());
        //PrintExpr(Cout, *exprRoot, exprRoot, true);

        auto convertedAst = ConvertToAst(*exprRoot, exprCtx, TExprAnnotationFlags::None, true);
        //PrintAst(Cout, *convertedAst, TAstPrintFlags::PerLine | TAstPrintFlags::ShortQuote);
        TStringStream strOrig;
        convertedAst.Root->PrettyPrintTo(strOrig, TAstPrintFlags::PerLine | TAstPrintFlags::ShortQuote);

        //PrintAst(Cout, *AnnotatePositions(*convertedAst), TAstPrintFlags::PerLine | TAstPrintFlags::ShortQuote);

        auto convertedAstWithAnnotations1 = ConvertToAst(*exprRoot, exprCtx, TExprAnnotationFlags::Position, true);
        TString strAnn1res = convertedAstWithAnnotations1.Root->ToString(TAstPrintFlags::ShortQuote);
        TAstParseResult astResAnn1 = ParseAstWithCheck(strAnn1res);
        UNIT_ASSERT(astResAnn1.IsOk());

        TMemoryPool pool(4096);
        auto cleanAst1 = RemoveAnnotations(*convertedAstWithAnnotations1.Root, pool);
        UNIT_ASSERT(cleanAst1);
        TStringStream strClean1;
        cleanAst1->PrettyPrintTo(strClean1, TAstPrintFlags::PerLine | TAstPrintFlags::ShortQuote);
        UNIT_ASSERT_VALUES_EQUAL(strClean1.Str(), strOrig.Str());

        auto convertedAstWithAnnotations2 = ConvertToAst(*exprRoot, exprCtx, TExprAnnotationFlags::Types, true);
        TString strAnn2res = convertedAstWithAnnotations2.Root->ToString(TAstPrintFlags::ShortQuote);

        TAstParseResult astResAnn2 = ParseAstWithCheck(strAnn2res);
        UNIT_ASSERT(astResAnn2.IsOk());

        auto cleanAst2 = RemoveAnnotations(*convertedAstWithAnnotations2.Root, pool);
        UNIT_ASSERT(cleanAst2);
        TStringStream strClean2;
        cleanAst2->PrettyPrintTo(strClean2, TAstPrintFlags::PerLine | TAstPrintFlags::ShortQuote);
        UNIT_ASSERT_VALUES_EQUAL(strClean2.Str(), strOrig.Str());

        TExprContext exprCtxAnnTypes;
        TExprNode::TPtr exprRootAnnTypes;
        CompileExprWithCheck(*astResAnn2.Root, exprRootAnnTypes, exprCtxAnnTypes, (ui32)TExprAnnotationFlags::Types);
        UNIT_ASSERT(exprRootAnnTypes->GetTypeAnn());

        auto convertedAstWithAnnotations3 = ConvertToAst(*exprRootAnnTypes, exprCtxAnnTypes, TExprAnnotationFlags::Types, true);
        TString strAnn3res = convertedAstWithAnnotations3.Root->ToString(TAstPrintFlags::ShortQuote);
        UNIT_ASSERT_EQUAL(strAnn2res, strAnn3res);

        const ui32 flagsToParseBack[] = {
                0,
                TAstPrintFlags::PerLine,
                // TAstPrintFlags::ShortQuote, // -- generates invalid AST
                TAstPrintFlags::PerLine | TAstPrintFlags::ShortQuote
        };

        for (ui32 index = 0; index < Y_ARRAY_SIZE(flagsToParseBack); ++index) {
            auto flag = flagsToParseBack[index];
            auto s2 = convertedAst.Root->ToString(flag);

            TAstParseResult res2 = ParseAst(s2);
            res2.Issues.PrintTo(Cout);
            UNIT_ASSERT(res2.IsOk());

            auto s3 = res2.Root->ToString(flag);
            UNIT_ASSERT_STRINGS_EQUAL(s2, s3);

            TExprContext exprCtx2;
            TExprNode::TPtr exprRoot2;
            CompileExprWithCheck(*res2.Root, exprRoot2, exprCtx2);

            auto convertedAst2 = ConvertToAst(*exprRoot2, exprCtx2, TExprAnnotationFlags::None, true);
            auto s4 = convertedAst2.Root->ToString(flag);
            UNIT_ASSERT_STRINGS_EQUAL(s2, s4);
        }
    }

    Y_UNIT_TEST(TestComplexProgramWithLamda) {
        auto s = "(\n"
            "#comment\n"
            "(let mr_source (DataSource 'yt 'plato))\n"
            "(let x (Read! world mr_source (Key '('table (String 'Input))) '('key 'subkey 'value) '()))\n"
            "(let world (Left! x))\n"
            "(let table1 (Right! x))\n"
            "(let tresh (String '100))\n"
            "(let table1low (Filter table1 (lambda '(item) (< (Member item 'key) tresh))))\n"
            "(let mr_sink (DataSink 'yt (quote plato)))\n"
            "(let world (Write! world mr_sink (Key '('table (String 'Output))) table1low '('('mode 'append))))\n"
            "(let world (Commit! world mr_sink))\n"
            "(return world)\n"
            ")\n";

        VerifyProgram(s);
    }

    Y_UNIT_TEST(TestSerializeAtomAsPartialExpression) {
        auto s = "(\n"
            "(let x 'a)\n"
            "(let y 'b)\n"
            "(let z (quote (x y)))\n"
            "(return z)\n"
            ")\n";

        TAstParseResult astRes = ParseAstWithCheck(s);
        UNIT_ASSERT(astRes.IsOk());

        TExprContext exprCtx;
        TExprNode::TPtr exprRoot;
        CompileExprWithCheck(*astRes.Root, exprRoot, exprCtx);

        auto typeAnnotationContext = MakeIntrusive<TTypeAnnotationContext>();
        auto randomProvider = CreateDeterministicRandomProvider(1);
        typeAnnotationContext->RandomProvider = randomProvider;
        AnnotateExprWithCheck(exprRoot, exprCtx, false, typeAnnotationContext);
        UNIT_ASSERT(exprRoot->GetTypeAnn());
        UNIT_ASSERT_VALUES_EQUAL(exprRoot->Child(0)->Content(), "a");

        auto convertedAstWithTypes = ConvertToAst(*exprRoot->Child(0), exprCtx, TExprAnnotationFlags::Types, true);
        TString strAnnRes = convertedAstWithTypes.Root->ToString(TAstPrintFlags::ShortQuote);
        TAstParseResult astRes2 = ParseAstWithCheck(strAnnRes);
        UNIT_ASSERT(astRes2.IsOk());

        TExprContext exprCtx2;
        TExprNode::TPtr exprRoot2;
        CompileExprWithCheck(*astRes2.Root, exprRoot2, exprCtx2, (ui32)TExprAnnotationFlags::Types);
        UNIT_ASSERT(exprRoot2->GetTypeAnn());
        UNIT_ASSERT_VALUES_EQUAL(exprRoot2->Content(), "a");

        ui32 annotationFlags = TExprAnnotationFlags::Types | TExprAnnotationFlags::Position;
        auto convertedAstWithTypesAndPos = ConvertToAst(*exprRoot->Child(0), exprCtx, annotationFlags, true);
        TString strPosAnnRes = convertedAstWithTypesAndPos.Root->ToString(TAstPrintFlags::ShortQuote);
        TAstParseResult astRes3 = ParseAstWithCheck(strPosAnnRes);
        UNIT_ASSERT(astRes3.IsOk());

        TExprContext exprCtx3;
        TExprNode::TPtr exprRoot3;
        CompileExprWithCheck(*astRes3.Root, exprRoot3, exprCtx3, annotationFlags);
        UNIT_ASSERT(exprRoot2->GetTypeAnn());
        UNIT_ASSERT_VALUES_EQUAL(exprRoot2->Content(), "a");
    }

    Y_UNIT_TEST(TestComplexProgramWithLamdaAsBlock) {
        auto s = "(\n"
            "#comment\n"
            "(let mr_source (DataSource 'yt 'plato))\n"
            "(let x (Read! world mr_source (Key '('table (String 'Input))) '('key 'subkey 'value) '()))\n"
            "(let world (Left! x))\n"
            "(let table1 (Right! x))\n"
            "(let table1low (Filter table1 (lambda '(item) (block '(\n"
            "   (let tresh (String '100))\n"
            "   (let predicate (< (Member item 'key) tresh))\n"
            "   (return predicate)\n"
            ")))))\n"
            "(let mr_sink (DataSink 'yt (quote plato)))\n"
            "(let world (Write! world mr_sink (Key '('table (String 'Output))) table1low '('('mode 'append))))\n"
            "(let world (Commit! world mr_sink))\n"
            "(return world)\n"
            ")\n";

        VerifyProgram(s);
    }

    Y_UNIT_TEST(TestComplexProgramWithBlockAndLambdaAsBlock) {
        auto s = "(\n"
            "#comment\n"
            "(let mr_source (DataSource 'yt 'plato))\n"
            "(let data (block '(\n"
            "  (let x (Read! world mr_source (Key '('table (String 'Input))) '('key 'subkey 'value) '()))\n"
            "  (let world (Left! x))\n"
            "  (let table1 (Right! x))\n"
            "  (let table1low (Filter table1 (lambda '(item) (block '(\n"
            "     (let tresh (String '100))\n"
            "     (let predicate (< (Member item 'key) tresh))\n"
            "     (return predicate)\n"
            "  )))))\n"
            "  (return table1low)\n"
            ")))\n"
            "(let mr_sink (DataSink 'yt (quote plato)))\n"
            "(let world (Write! world mr_sink (Key '('table (String 'Output))) data '('('mode 'append))))\n"
            "(let world (Commit! world mr_sink))\n"
            "(return world)\n"
            ")\n";

        VerifyProgram(s);
    }

    Y_UNIT_TEST(TestPerfCompile) {
        auto s = "(\n"
            "#comment\n"
            "(let mr_source (DataSource 'yt 'plato))\n"
            "(let data (block '(\n"
            "  (let x (Read! world mr_source (Key '('table (String 'Input))) '('key 'subkey 'value) '()))\n"
            "  (let world (Left! x))\n"
            "  (let table1 (Right! x))\n"
            "  (let table1low (Filter table1 (lambda '(item) (block '(\n"
            "     (let tresh (String '100))\n"
            "     (let predicate (< (Member item 'key) tresh))\n"
            "     (return predicate)\n"
            "  )))))\n"
            "  (return table1low)\n"
            ")))\n"
            "(let mr_sink (DataSink 'yt (quote plato)))\n"
            "(let world (Write! world mr_sink (Key '('table (String 'Output))) data '('('mode 'append))))\n"
            "(let world (Commit! world mr_sink))\n"
            "(return world)\n"
            ")\n";

        TAstParseResult astRes = ParseAstWithCheck(s);
        const ui32 n = NSan::PlainOrUnderSanitizer(10000, 1000);
        auto t1 = TInstant::Now();
        for (ui32 i = 0; i < n; ++i) {
            TExprContext exprCtx;
            TExprNode::TPtr exprRoot;
            CompileExprWithCheck(*astRes.Root, exprRoot, exprCtx);
        }

        auto t2 = TInstant::Now();
        Cout << t2 - t1 << Endl;
    }
}

} // namespace NYql
