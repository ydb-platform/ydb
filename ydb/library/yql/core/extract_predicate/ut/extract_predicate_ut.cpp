#include "extract_predicate_impl.h"

#include <ydb/library/yql/providers/yt/provider/yql_yt_provider.h>
#include <ydb/library/yql/providers/yt/gateway/file/yql_yt_file.h>
#include <ydb/library/yql/providers/yt/gateway/file/yql_yt_file_services.h>
#include <ydb/library/yql/providers/config/yql_config_provider.h>
#include <ydb/library/yql/providers/result/provider/yql_result_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/sql/settings/translation_settings.h>
#include <ydb/library/yql/sql/sql.h>
#include <ydb/library/yql/ast/yql_ast_annotation.h>
#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/type_ann/type_ann_core.h>
#include <ydb/library/yql/core/type_ann/type_ann_expr.h>
#include <ydb/library/yql/core/ut_common/yql_ut_common.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/facade/yql_facade.h>
#include <ydb/library/yql/core/services/yql_transform_pipeline.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/string/cast.h>
#include <util/system/user.h>

namespace NYql {

Y_UNIT_TEST_SUITE(TYqlExtractPredicate) {

    TExprNode::TPtr ParseAndOptimize(const TString& program, TExprContext& exprCtx, TTypeAnnotationContextPtr& typesCtx) {
        NSQLTranslation::TTranslationSettings settings;
        settings.ClusterMapping["plato"] = YtProviderName;
        settings.SyntaxVersion = 1;

        TAstParseResult astRes = SqlToYql(program, settings);
        UNIT_ASSERT(astRes.IsOk());
        TExprNode::TPtr exprRoot;
        UNIT_ASSERT(CompileExpr(*astRes.Root, exprRoot, exprCtx, nullptr, nullptr));

        auto functionRegistry = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry());
        TTestTablesMapping testTables;
        auto yqlNativeServices = NFile::TYtFileServices::Make(functionRegistry.Get(), testTables);
        auto ytGateway = CreateYtFileGateway(yqlNativeServices);
        typesCtx = MakeIntrusive<TTypeAnnotationContext>();
        typesCtx->RandomProvider = CreateDeterministicRandomProvider(1);
        auto ytState = MakeIntrusive<TYtState>();
        ytState->Gateway = ytGateway;
        ytState->Types = typesCtx.Get();

        InitializeYtGateway(ytGateway, ytState);
        typesCtx->AddDataSink(YtProviderName, CreateYtDataSink(ytState));
        typesCtx->AddDataSource(YtProviderName, CreateYtDataSource(ytState));

        typesCtx->AddDataSource(ConfigProviderName, CreateConfigProvider(*typesCtx, nullptr, ""));

        auto transformer = TTransformationPipeline(typesCtx)
            .AddServiceTransformers()
            .AddPreTypeAnnotation()
            .AddExpressionEvaluation(*functionRegistry)
            .AddIOAnnotation()
            .AddTypeAnnotation()
            .AddPostTypeAnnotation()
            .AddOptimization()
            .Build();

        auto status = SyncTransform(*transformer, exprRoot, exprCtx);
        if (status != IGraphTransformer::TStatus::Ok) {
            auto issues = exprCtx.IssueManager.GetIssues();
            issues.PrintTo(Cerr);
        }
        UNIT_ASSERT(status == IGraphTransformer::TStatus::Ok);

        return exprRoot;
    }

    TString DumpNode(const TExprNode& node, TExprContext& exprCtx) {
        auto ast = ConvertToAst(node, exprCtx, TExprAnnotationFlags::None, true);
        return ast.Root->ToString(TAstPrintFlags::PerLine | TAstPrintFlags::ShortQuote);
    }

    TExprNode::TPtr LocateFilterLambda(const TExprNode::TPtr& root) {
        TExprNode::TPtr filterLambda;
        VisitExpr(root, [&filterLambda] (const TExprNode::TPtr& node) {
            if (node->IsCallable({"FlatMap", "OrderedFlatMap"})) {
                auto lambda = node->Child(1);
                if (lambda->Tail().IsCallable("OptionalIf")) {
                    filterLambda = lambda;
                }
            }
            return !filterLambda;
        });

        UNIT_ASSERT(filterLambda);
        return filterLambda;
    }

    void CheckRanges(const TExprNode::TPtr& exprRoot, const TVector<TString>& keyColumns, TExprContext& exprCtx, TTypeAnnotationContext& typesCtx, const TString& expectedRanges,
        const THashSet<TString>& expectedColumns, const TString& expectedComputeRanges = "", bool verbose = false)
    {
        if (verbose) {
            Cerr << "Input: " << DumpNode(*exprRoot, exprCtx) << "\n";
        }

        TExprNode::TPtr filterLambda = LocateFilterLambda(exprRoot);

        THashSet<TString> usedColumns;
        using NDetail::TPredicateRangeExtractor;

        THolder<TPredicateRangeExtractor> extractor{new TPredicateRangeExtractor};

        UNIT_ASSERT(extractor->Prepare(filterLambda, *filterLambda->Head().Head().GetTypeAnn(), usedColumns, exprCtx, typesCtx));
        auto range = extractor->GetPreparedRange();
        UNIT_ASSERT(range);
        auto rangeDump = DumpNode(*range, exprCtx);

        TString computeRangeDump;
        if (expectedComputeRanges) {
            auto buildResult = extractor->BuildComputeNode(keyColumns, exprCtx, typesCtx);
            if (buildResult.ComputeNode) {
                computeRangeDump = DumpNode(*buildResult.ComputeNode, exprCtx);
            }
        }

        if (verbose) {
            Cerr << "Ranges: " << rangeDump << "\n";
            Cerr << "ComputeRanges: " << computeRangeDump << "\n";
        }

        UNIT_ASSERT_NO_DIFF(expectedRanges, rangeDump);
        UNIT_ASSERT(expectedColumns == usedColumns);
        if (expectedComputeRanges) {
            UNIT_ASSERT_NO_DIFF(expectedComputeRanges, computeRangeDump);
        }
    }

    struct TRunSingleProgram {
        TString Src;
        TString TmpDir;
        TString Parameters;
        IOutputStream& Err;
        TVector<TString> Res;
        THashMap<TString, TString> Tables;

        TRunSingleProgram(const TString& src, IOutputStream& err)
            : Src(src)
            , Err(err)
        {
        }

        bool Run(
            const NKikimr::NMiniKQL::IFunctionRegistry* funcReg
        ) {
            auto yqlNativeServices = NFile::TYtFileServices::Make(funcReg, Tables, {}, TmpDir);
            auto ytGateway = CreateYtFileGateway(yqlNativeServices);

            TVector<TDataProviderInitializer> dataProvidersInit;
            dataProvidersInit.push_back(GetYtNativeDataProviderInitializer(ytGateway));
            TProgramFactory factory(true, funcReg, 0ULL, dataProvidersInit, "ut");

            TProgramPtr program = factory.Create("-stdin-", Src);
            program->ConfigureYsonResultFormat(NYson::EYsonFormat::Text);
            if (!Parameters.empty()) {
                program->SetParametersYson(Parameters);
            }

            if (!program->ParseYql() || !program->Compile(GetUsername())) {
                program->PrintErrorsTo(Err);
                return false;
            }

            TProgram::TStatus status = program->Run(GetUsername());
            if (status == TProgram::TStatus::Error) {
                program->PrintErrorsTo(Err);
            }
            Res = program->Results();
            return status == TProgram::TStatus::Ok;
        }

        void AddResults(TVector<TString>& res) const {
            res.insert(res.end(), Res.begin(), Res.end());
        }

        bool Finished() const {
            return true;
        }
    };

    template <typename TDriver>
    TVector<TString> Run(TDriver& driver) {
        auto functionRegistry = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry());

        TVector<TString> res;
        do {
            const bool runRes = driver.Run(functionRegistry.Get());
            UNIT_ASSERT(runRes);

            driver.AddResults(res);
        } while (!driver.Finished());
        return res;
    }

    TVector<TString> RunProgram(const TString& programSrc, const THashMap<TString, TString>& tables, const TString& tmpDir = TString(), const TString& params = TString()) {
        TRunSingleProgram driver(programSrc, Cerr);
        driver.Tables = tables;
        driver.TmpDir = tmpDir;
        driver.Parameters = params;
        return Run(driver);
    }

    void ExecuteAndCheckRanges(const TString& toExecute, const TString& expectedExecuteResult) {
        auto s = TStringBuilder() <<
                 "(\n"
                 "(let res_sink (DataSink 'result))\n"
                 "(let data (block '" << toExecute << "))\n"
                 "(let world (Write! world res_sink (Key) data '()))\n"
                 "(let world (Commit! world res_sink))\n"
                 "(return world)\n"
                 ")\n";
        //Cerr << "PROG: " << s << "\n";
        auto res = RunProgram(s, THashMap<TString, TString>());
        UNIT_ASSERT_VALUES_EQUAL(res.size(), 1);
        //Cerr << "RES: " << res[0] << "\n";
        UNIT_ASSERT_NO_DIFF(expectedExecuteResult, res[0]);
    }

    void ExtractAndExecuteRanges(const TString& prog, const TString& expectedExecuteResult, TPredicateExtractorSettings settings = {}, TVector<TString> indexKeys = { "x", "y" }) {
        TExprContext exprCtx;
        TTypeAnnotationContextPtr typesCtx;

        TExprNode::TPtr exprRoot = ParseAndOptimize(prog, exprCtx, typesCtx);

        TExprNode::TPtr filterLambda = LocateFilterLambda(exprRoot);

        THashSet<TString> usedColumns;
        using NDetail::TPredicateRangeExtractor;

        settings.HaveNextValueCallable = true;
        auto extractor = MakePredicateRangeExtractor(settings);

        UNIT_ASSERT(extractor->Prepare(filterLambda, *filterLambda->Head().Head().GetTypeAnn(), usedColumns, exprCtx, *typesCtx));

        auto buildResult = extractor->BuildComputeNode(indexKeys, exprCtx, *typesCtx);
        UNIT_ASSERT(buildResult.ComputeNode);

        ExecuteAndCheckRanges(DumpNode(*buildResult.ComputeNode, exprCtx), expectedExecuteResult);
    }

    TString GetOptionalSrc() {
        return
            "use plato;\n"
            "\n"
            "$src = [\n"
            "    <|x:1/0, y:2/0|>,\n"
            "    <|x:1/0, y:1|>,\n"
            "    <|x:1, y:1/0|>,\n"
            "];\n"
            "\n";
    }

    TString GetNonOptionsSrc() {
        return
            "use plato;\n"
            "\n"
            "$src = [\n"
            "    <|x:1, y:2|>,\n"
            "    <|x:3, y:4|>,\n"
            "];\n"
            "\n";
    }

    TString GetNonOptionsSrc4() {
        return
            "use plato;\n"
            "\n"
            "$src = [\n"
            "    <|x:1, y:2, z:3, t:4|>,\n"
            "    <|x:3, y:4, z:5, t:6|>,\n"
            "];\n"
            "\n";
    }

    Y_UNIT_TEST(PropagateNot) {
        TString prog = GetOptionalSrc() +
            "insert into Output with truncate\n"
            "select * from as_table($src) where not (3 > x or not(y > 3));";

        TString expectedRanges =
            "(\n"
            "(let $1 (OptionalType (DataType 'Int32)))\n"
            "(let $2 (StructType '('\"x\" $1) '('\"y\" $1)))\n"
            "(let $3 (Int32 '\"3\"))\n"
            "(return (RangeAnd (Range $2 (lambda '($4) (Coalesce (>= (Member $4 '\"x\") $3) (Bool 'false)))) (Range $2 (lambda '($5) (Coalesce (> (Member $5 '\"y\") $3) (Bool 'false))))))\n"
            ")\n";

        TExprContext exprCtx;
        TTypeAnnotationContextPtr typesCtx;
        TExprNode::TPtr exprRoot = ParseAndOptimize(prog, exprCtx, typesCtx);
        CheckRanges(exprRoot, { "x", "y" }, exprCtx, *typesCtx, expectedRanges, { "x", "y" });
    }

    Y_UNIT_TEST(RangeRestInOr) {
        TString prog = GetNonOptionsSrc() +
            "insert into Output with truncate\n"
            "select * from as_table($src) where (3 > x or (x + y) > 3);";

        TString expectedRanges =
            "(\n"
            "(let $1 (DataType 'Int32))\n"
            "(return (RangeRest (StructType '('\"x\" $1) '('\"y\" $1)) (lambda '($2) (block '(\n"
            "  (let $3 (Int32 '\"3\"))\n"
            "  (let $4 (Member $2 '\"x\"))\n"
            "  (return (Or (> $3 $4) (> (+ $4 (Member $2 '\"y\")) $3)))\n"
            ")))))\n"
            ")\n";

        TExprContext exprCtx;
        TTypeAnnotationContextPtr typesCtx;
        TExprNode::TPtr exprRoot = ParseAndOptimize(prog, exprCtx, typesCtx);
        CheckRanges(exprRoot, { "x", "y" }, exprCtx, *typesCtx, expectedRanges, {});
    }

    Y_UNIT_TEST(RangeRestDueToDisjointColumnSet) {
        TString prog = GetNonOptionsSrc4() +
            "insert into Output with truncate\n"
            "select * from as_table($src) where (x = 1 and y = 2) or (z = 3 and t = 4);";

        TString expectedRanges =
            "(\n"
            "(let $1 (DataType 'Int32))\n"
            "(let $2 (StructType '('\"t\" $1) '('\"x\" $1) '('\"y\" $1) '('\"z\" $1)))\n"
            "(return (RangeRest $2 (lambda '($3) (Or (And (== (Member $3 '\"x\") (Int32 '1)) (== (Member $3 '\"y\") (Int32 '\"2\"))) (And (== (Member $3 '\"z\") (Int32 '\"3\")) (== (Member $3 '\"t\") (Int32 '\"4\")))))))\n"
            ")\n";

        TExprContext exprCtx;
        TTypeAnnotationContextPtr typesCtx;
        TExprNode::TPtr exprRoot = ParseAndOptimize(prog, exprCtx, typesCtx);
        CheckRanges(exprRoot, { "x", "y" }, exprCtx, *typesCtx, expectedRanges, {});
    }

    Y_UNIT_TEST(Exists) {
        TString prog = GetOptionalSrc() +
            "insert into Output with truncate\n"
            "select * from as_table($src) where x is null;";

        TString expectedRanges =
            "(\n"
            "(let $1 (OptionalType (DataType 'Int32)))\n"
            "(return (Range (StructType '('\"x\" $1) '('\"y\" $1)) (lambda '($2) (Not (Exists (Member $2 '\"x\"))))))\n"
            ")\n";

        TExprContext exprCtx;
        TTypeAnnotationContextPtr typesCtx;
        TExprNode::TPtr exprRoot = ParseAndOptimize(prog, exprCtx, typesCtx);
        CheckRanges(exprRoot, { "x", "y" }, exprCtx, *typesCtx, expectedRanges, { "x" });
    }

#if 0
    Y_UNIT_TEST(SqlNotIn) {
        TString prog = GetOptionalSrc() +
            "insert into Output with truncate\n"
            "select * from as_table($src) where y not in (1, 2, 3);";

        TString expectedRanges =
            "(\n"
            "(let $1 (OptionalType (DataType 'Int32)))\n"
            "(return (Range (StructType '('\"x\" $1) '('\"y\" $1)) (lambda '($2) (block '(\n"
            "  (let $3 '((Int32 '\"1\") (Int32 '\"2\") (Int32 '\"3\")))\n"
            "  (let $4 (SqlIn $3 (Member $2 '\"y\") '()))\n"
            "  (return (Not (Coalesce $4 (Bool 'true))))\n"
            ")))))\n"
            ")\n";

        TExprContext exprCtx;
        TTypeAnnotationContextPtr typesCtx;
        TExprNode::TPtr exprRoot = ParseAndOptimize(prog, exprCtx, typesCtx);
        CheckRanges(exprRoot, { "x", "y" }, exprCtx, *typesCtx, expectedRanges, { "y" });
    }
#endif

    Y_UNIT_TEST(SqlIn) {
        TString prog = GetOptionalSrc() +
            "insert into Output with truncate\n"
            "select * from as_table($src) where x in (4294967295u, 2u,  1u) AND y != 100;";

        TString expectedRanges =
            "(\n"
            "(let $1 (OptionalType (DataType 'Int32)))\n"
            "(let $2 (StructType '('\"x\" $1) '('\"y\" $1)))\n"
            "(let $3 (Bool 'false))\n"
            "(return (RangeAnd (Range $2 (lambda '($4) (block '(\n"
            "  (let $5 '((Uint32 '\"4294967295\") (Uint32 '\"2\") (Uint32 '1)))\n"
            "  (let $6 (SqlIn $5 (Member $4 '\"x\") '()))\n"
            "  (return (Coalesce $6 $3))\n"
            ")))) (Range $2 (lambda '($7) (Coalesce (!= (Member $7 '\"y\") (Int32 '\"100\")) $3)))))\n"
            ")\n";

        TString expectedComputeRanges =
            "(\n"
            "(let $1 (OptionalType (DataType 'Int32)))\n"
            "(let $2 (IfPresent (Map (Just '((Uint32 '\"4294967295\") (Uint32 '\"2\") (Uint32 '1))) (lambda '($4) (AsList (Nth $4 '0) (Nth $4 '1) (Nth $4 '2)))) (lambda '($5) (block '(\n"
            "  (let $6 (Collect (Take (FlatMap $5 (lambda '($8) (block '(\n"
            "    (let $9 (RangeFor '== $8 $1))\n"
            "    (return (RangeMultiply (Uint64 '10000) $9))\n"
            "  )))) (Uint64 '10001))))\n"
            "  (let $7 '((Nothing (OptionalType $1)) (Int32 '0)))\n"
            "  (return (IfStrict (> (Length $6) (Uint64 '10000)) (AsRange '($7 $7)) (RangeUnion $6)))\n"
            "))) (RangeEmpty $1)))\n"
            "(let $3 (RangeFor '!= (Int32 '\"100\") $1))\n"
            "(return (RangeFinalize (RangeMultiply (Uint64 '10000) (RangeUnion (RangeIntersect (RangeMultiply (Uint64 '10000) $2 $3))))))\n"
            ")\n";

        TExprContext exprCtx;
        TTypeAnnotationContextPtr typesCtx;
        TExprNode::TPtr exprRoot = ParseAndOptimize(prog, exprCtx, typesCtx);
        CheckRanges(exprRoot, { "x", "y" }, exprCtx, *typesCtx, expectedRanges, { "x", "y" }, expectedComputeRanges);

        TString expectedExecuteResult = R"__({"Write"=[{"Data"=[[[[["1"]];[#];"0"];[[["1"]];[["100"]];"0"]];[[[["1"]];[["100"]];"0"];[[["1"]];#;"1"]];[[[["2"]];[#];"0"];[[["2"]];[["100"]];"0"]];[[[["2"]];[["100"]];"0"];[[["2"]];#;"1"]]]}]})__";
        ExecuteAndCheckRanges(expectedComputeRanges, expectedExecuteResult);
    }

    Y_UNIT_TEST(AndWithOr) {
        TString prog = GetOptionalSrc() +
            "insert into Output with truncate\n"
            "select * from as_table($src) where x > 2 or (x < 1 and y > 0);";

        TString expectedRanges =
            "(\n"
            "(let $1 (OptionalType (DataType 'Int32)))\n"
            "(let $2 (StructType '('\"x\" $1) '('\"y\" $1)))\n"
            "(let $3 (Bool 'false))\n"
            "(return (RangeOr (Range $2 (lambda '($4) (Coalesce (> (Member $4 '\"x\") (Int32 '\"2\")) $3))) (RangeAnd (Range $2 (lambda '($5) (Coalesce (< (Member $5 '\"x\") (Int32 '1)) $3))) (Range $2 (lambda '($6) (Coalesce (> (Member $6 '\"y\") "
            "(Int32 '0)) $3))))))\n"
            ")\n";

        TExprContext exprCtx;
        TTypeAnnotationContextPtr typesCtx;
        TExprNode::TPtr exprRoot = ParseAndOptimize(prog, exprCtx, typesCtx);
        CheckRanges(exprRoot, { "x", "y" }, exprCtx, *typesCtx, expectedRanges, { "y", "x" });
    }

    Y_UNIT_TEST(OrWithConst) {
        TString prog = GetOptionalSrc() +
            "insert into Output with truncate\n"
            "select * from as_table($src) where 1 > 2 or x > 2;";

        TString expectedRanges =
            "(\n"
            "(let $1 (Int32 '\"2\"))\n"
            "(let $2 (OptionalType (DataType 'Int32)))\n"
            "(return (RangeOr (RangeConst (> (Int32 '1) $1)) (Range (StructType '('\"x\" $2) '('\"y\" $2)) (lambda '($3) (Coalesce (> (Member $3 '\"x\") $1) (Bool 'false))))))\n"
            ")\n";

        TExprContext exprCtx;
        TTypeAnnotationContextPtr typesCtx;
        TExprNode::TPtr exprRoot = ParseAndOptimize(prog, exprCtx, typesCtx);
        CheckRanges(exprRoot, { "x" }, exprCtx, *typesCtx, expectedRanges, { "x" });
    }

    Y_UNIT_TEST(AndWithAllConst) {
        TString prog = GetOptionalSrc() +
            "insert into Output with truncate\n"
            "select * from as_table($src) where 3 > 0 and 1 > 2;";

        TString expectedRanges =
            "(\n"
            "(return (RangeConst (And (> (Int32 '\"3\") (Int32 '0)) (> (Int32 '1) (Int32 '\"2\")))))\n"
            ")\n";

        TExprContext exprCtx;
        TTypeAnnotationContextPtr typesCtx;
        TExprNode::TPtr exprRoot = ParseAndOptimize(prog, exprCtx, typesCtx);
        CheckRanges(exprRoot, { "x", "y" }, exprCtx, *typesCtx, expectedRanges, { });
    }

    Y_UNIT_TEST(OrWithDifferentColumns) {
        TString prog = GetOptionalSrc() +
            "insert into Output with truncate\n"
            "select * from as_table($src) where x > 0 or 1 < y;";

        TString expectedRanges =
            "(\n"
            "(let $1 (OptionalType (DataType 'Int32)))\n"
            "(return (RangeRest (StructType '('\"x\" $1) '('\"y\" $1)) (lambda '($2) (block '(\n"
            "  (let $3 (Bool 'false))\n"
            "  (return (Or (Coalesce (> (Member $2 '\"x\") (Int32 '0)) $3) (Coalesce (< (Int32 '1) (Member $2 '\"y\")) $3)))\n"
            ")))))\n"
            ")\n";

        TExprContext exprCtx;
        TTypeAnnotationContextPtr typesCtx;
        TExprNode::TPtr exprRoot = ParseAndOptimize(prog, exprCtx, typesCtx);
        CheckRanges(exprRoot, { "x", "y" }, exprCtx, *typesCtx, expectedRanges, { });
    }

    Y_UNIT_TEST(TupleEquals) {
        TString prog = GetOptionalSrc() +
            "insert into Output with truncate\n"
            "select * from as_table($src) where x < y and (x, y) == (1, 2);";

        TString expectedRanges =
            "(\n"
            "(let $1 (OptionalType (DataType 'Int32)))\n"
            "(let $2 (StructType '('\"x\" $1) '('\"y\" $1)))\n"
            "(let $3 (Bool 'false))\n"
            "(return (RangeAnd (RangeRest $2 (lambda '($4) (Coalesce (< (Member $4 '\"x\") (Member $4 '\"y\")) $3))) (Range $2 (lambda '($5) (Coalesce (== (Member $5 '\"x\") (Int32 '1)) $3))) (Range $2 (lambda '($6) (Coalesce (== (Member $6 '\"y\") (Int32 '\"2\")) $3)))))\n"
            ")\n";

        TExprContext exprCtx;
        TTypeAnnotationContextPtr typesCtx;
        TExprNode::TPtr exprRoot = ParseAndOptimize(prog, exprCtx, typesCtx);
        CheckRanges(exprRoot, { "x", "y" }, exprCtx, *typesCtx, expectedRanges, { "x", "y" });
    }

    Y_UNIT_TEST(TupleLessOrEquals) {
        TString prog = GetNonOptionsSrc() +
            "insert into Output with truncate\n"
            "select * from as_table($src) where (x, y) <= (1, 2);";

        TString expectedRanges =
            "(\n"
            "(let $1 (DataType 'Int32))\n"
            "(let $2 (StructType '('\"x\" $1) '('\"y\" $1)))\n"
            "(let $3 (Int32 '1))\n"
            "(let $4 (Int32 '\"2\"))\n"
            "(return (RangeOr (Range $2 (lambda '($5) (< (Member $5 '\"x\") $3))) (RangeAnd (Range $2 (lambda '($6) (== (Member $6 '\"x\") $3))) (Range $2 (lambda '($7) (< (Member $7 '\"y\") $4)))) (RangeAnd (Range $2 (lambda '($8) (== (Member $8 '\"x\") $3))) (Range $2 (lambda '($9) (== (Member $9 '\"y\") $4))))))\n"
            ")\n";

        TExprContext exprCtx;
        TTypeAnnotationContextPtr typesCtx;
        TExprNode::TPtr exprRoot = ParseAndOptimize(prog, exprCtx, typesCtx);
        CheckRanges(exprRoot, { "x", "y" }, exprCtx, *typesCtx, expectedRanges, { "x", "y" });
    }

    Y_UNIT_TEST(BasicRange) {
        TString prog = GetOptionalSrc() +
            "insert into Output with truncate\n"
            "select * from as_table($src) where x < 1 + 1;";

        TString expectedRanges =
            "(\n"
            "(let $1 (OptionalType (DataType 'Int32)))\n"
            "(return (Range (StructType '('\"x\" $1) '('\"y\" $1)) (lambda '($2) (block '(\n"
            "  (let $3 (Int32 '1))\n"
            "  (return (Coalesce (< (Member $2 '\"x\") (+ $3 $3)) (Bool 'false)))\n"
            ")))))\n"
            ")\n";

        TString expectedComputeRanges =
            "(\n"
            "(let $1 (Int32 '1))\n"
            "(return (RangeFinalize (RangeMultiply (Uint64 '10000) (RangeUnion (RangeFor '< (+ $1 $1) (OptionalType (DataType 'Int32)))))))\n"
            ")\n";

        TExprContext exprCtx;
        TTypeAnnotationContextPtr typesCtx;
        TExprNode::TPtr exprRoot = ParseAndOptimize(prog, exprCtx, typesCtx);
        CheckRanges(exprRoot, { "x" }, exprCtx, *typesCtx, expectedRanges, { "x" }, expectedComputeRanges);

        TString expectedExecuteResult = R"__({"Write"=[{"Data"=[[[[#];"0"];[[["2"]];"0"]]]}]})__";
        ExecuteAndCheckRanges(expectedComputeRanges, expectedExecuteResult);
    }

    Y_UNIT_TEST(BasicRange2) {
        TString prog = GetOptionalSrc() +
            "insert into Output with truncate\n"
            "select * from as_table($src) where x < 1 + 1 and x >= 0 or x < -10";

        TString expectedRanges =
            "(\n"
            "(let $1 (OptionalType (DataType 'Int32)))\n"
            "(let $2 (StructType '('\"x\" $1) '('\"y\" $1)))\n"
            "(let $3 (Bool 'false))\n"
            "(return (RangeOr (RangeAnd (Range $2 (lambda '($4) (block '(\n"
            "  (let $5 (Int32 '1))\n"
            "  (return (Coalesce (< (Member $4 '\"x\") (+ $5 $5)) $3))\n"
            ")))) (Range $2 (lambda '($6) (Coalesce (>= (Member $6 '\"x\") (Int32 '0)) $3)))) (Range $2 (lambda '($7) (Coalesce (< (Member $7 '\"x\") (Int32 '\"-10\")) $3)))))\n"
            ")\n";

        TString expectedComputeRanges =
            "(\n"
            "(let $1 (Int32 '1))\n"
            "(let $2 (OptionalType (DataType 'Int32)))\n"
            "(let $3 (RangeFor '< (+ $1 $1) $2))\n"
            "(let $4 (RangeFor '>= (Int32 '0) $2))\n"
            "(let $5 (RangeFor '< (Int32 '\"-10\") $2))\n"
            "(let $6 '((Nothing (OptionalType $2)) (Int32 '0)))\n"
            "(return (RangeFinalize (RangeMultiply (Uint64 '10000) (RangeUnion (RangeMultiply (Uint64 '10000) (RangeUnion (RangeIntersect $3 $4) $5) (AsRange '($6 $6)))))))\n"
            ")\n";

        TExprContext exprCtx;
        TTypeAnnotationContextPtr typesCtx;
        TExprNode::TPtr exprRoot = ParseAndOptimize(prog, exprCtx, typesCtx);
        CheckRanges(exprRoot, { "x", "y" }, exprCtx, *typesCtx, expectedRanges, { "x" }, expectedComputeRanges);

        TString expectedExecuteResult = R"__({"Write"=[{"Data"=[[[[#];#;"0"];[[["-10"]];#;"0"]];[[[["0"]];#;"1"];[[["2"]];#;"0"]]]}]})__";
        ExecuteAndCheckRanges(expectedComputeRanges, expectedExecuteResult);
    }

    Y_UNIT_TEST(BasicIntRounding) {
        TString prog = GetOptionalSrc() +
            "insert into Output with truncate\n"
            "select * from as_table($src) where x < 4294967295u;";

        TString expectedRanges =
            "(\n"
            "(let $1 (OptionalType (DataType 'Int32)))\n"
            "(return (Range (StructType '('\"x\" $1) '('\"y\" $1)) (lambda '($2) (Coalesce (< (Member $2 '\"x\") (Uint32 '\"4294967295\")) (Bool 'false)))))\n"
            ")\n";

        TString expectedComputeRanges =
            "(\n"
            "(return (RangeFinalize (RangeMultiply (Uint64 '10000) (RangeUnion (RangeFor '< (Uint32 '\"4294967295\") (OptionalType (DataType 'Int32)))))))\n"
            ")\n";

        TExprContext exprCtx;
        TTypeAnnotationContextPtr typesCtx;
        TExprNode::TPtr exprRoot = ParseAndOptimize(prog, exprCtx, typesCtx);
        CheckRanges(exprRoot, { "x" }, exprCtx, *typesCtx, expectedRanges, { "x" }, expectedComputeRanges);

        TString expectedExecuteResult = R"__({"Write"=[{"Data"=[[[[#];"0"];[[["2147483647"]];"1"]]]}]})__";
        ExecuteAndCheckRanges(expectedComputeRanges, expectedExecuteResult);
    }

    Y_UNIT_TEST(RangeWithFalseConst) {
        TString prog = GetOptionalSrc() +
            "insert into Output with truncate\n"
            "select * from as_table($src) where x < 1 and 1+1 > 2;";

        TString expectedExecuteResult = R"__({"Write"=[{"Data"=[]}]})__";
        ExtractAndExecuteRanges(prog, expectedExecuteResult);
    }

    Y_UNIT_TEST(RangeWithTrueConst) {
        TString prog = GetOptionalSrc() +
            "insert into Output with truncate\n"
            "select * from as_table($src) where x < 1 and 1+1 >= 2;";

        TString expectedExecuteResult = R"__({"Write"=[{"Data"=[[[[#];#;"0"];[[["1"]];#;"0"]]]}]})__";
        ExtractAndExecuteRanges(prog, expectedExecuteResult);
    }

    Y_UNIT_TEST(RangeSinglePoint) {
        TString prog = GetOptionalSrc() +
            "insert into Output with truncate\n"
            "select * from as_table($src) where x == 10;";

        TString expectedExecuteResult = R"__({"Write"=[{"Data"=[[[[["10"]];#;"1"];[[["10"]];#;"1"]]]}]})__";
        ExtractAndExecuteRanges(prog, expectedExecuteResult);
    }

    Y_UNIT_TEST(RangeWithoutSinglePoint) {
        TString prog = GetOptionalSrc() +
            "insert into Output with truncate\n"
            "select * from as_table($src) where x != 10;";

        TString expectedExecuteResult = R"__({"Write"=[{"Data"=[[[[#];#;"0"];[[["10"]];#;"0"]];[[[["10"]];#;"0"];[#;#;"0"]]]}]})__";
        ExtractAndExecuteRanges(prog, expectedExecuteResult);
    }

    Y_UNIT_TEST(RangeUnionOverlapping) {
        // (1, 3) union (2, 4]
        TString prog = GetOptionalSrc() +
            "insert into Output with truncate\n"
            "select * from as_table($src) where (x > 1 and x < 3) or (x <= 4 and x > 2);";

        TString expectedExecuteResult = R"__({"Write"=[{"Data"=[[[[["1"]];#;"0"];[[["4"]];#;"1"]]]}]})__";
        ExtractAndExecuteRanges(prog, expectedExecuteResult);
    }

    Y_UNIT_TEST(RangeUnionAdjacent) {
        // (1, 3) union [3, 4)
        TString prog = GetOptionalSrc() +
            "insert into Output with truncate\n"
            "select * from as_table($src) where (x > 1 and x < 3) or (x < 4 and x >= 3);";

        TString expectedExecuteResult = R"__({"Write"=[{"Data"=[[[[["1"]];#;"0"];[[["4"]];#;"0"]]]}]})__";
        ExtractAndExecuteRanges(prog, expectedExecuteResult);
    }

    Y_UNIT_TEST(RangeUnionDisjoint) {
        // (-inf, 2) union [3, 4)
        TString prog = GetOptionalSrc() +
            "insert into Output with truncate\n"
            "select * from as_table($src) where x < 2 or (x < 4 and x >= 3);";

        TString expectedExecuteResult = R"__({"Write"=[{"Data"=[[[[#];#;"0"];[[["2"]];#;"0"]];[[[["3"]];#;"1"];[[["3"]];#;"1"]]]}]})__";
        ExtractAndExecuteRanges(prog, expectedExecuteResult);
    }

    Y_UNIT_TEST(RangeIntersect3) {
        // ((-inf, 3) or [5, +inf)) intersect [2, 6)
        TString prog = GetOptionalSrc() +
            "insert into Output with truncate\n"
            "select * from as_table($src) where (x < 3 or x >= 5) and (x >= 2 and x < 6);";

        TString expectedExecuteResult = R"__({"Write"=[{"Data"=[[[[["2"]];#;"1"];[[["2"]];#;"1"]];[[[["5"]];#;"1"];[[["5"]];#;"1"]]]}]})__";
        ExtractAndExecuteRanges(prog, expectedExecuteResult);
    }

    Y_UNIT_TEST(TupleLess) {
        TString prog = GetOptionalSrc() +
            "insert into Output with truncate\n"
            "select * from as_table($src) where (x, y) < (5, 1 + 3);";

        TString expectedExecuteResult = R"__({"Write"=[{"Data"=[[[[#];#;"0"];[[["5"]];#;"0"]];[[[["5"]];[#];"0"];[[["5"]];[["4"]];"0"]]]}]})__";
        ExtractAndExecuteRanges(prog, expectedExecuteResult);
    }

    Y_UNIT_TEST(TupleLessOrEquals2) {
        TString prog = GetOptionalSrc() +
            "insert into Output with truncate\n"
            "select * from as_table($src) where (x, y) <= (5, 1 + 3);";

        TString expectedExecuteResult = R"__({"Write"=[{"Data"=[[[[#];#;"0"];[[["5"]];#;"0"]];[[[["5"]];[#];"0"];[[["5"]];[["5"]];"0"]]]}]})__";
        ExtractAndExecuteRanges(prog, expectedExecuteResult);
    }

    Y_UNIT_TEST(DateRounding) {
        TString prefixDt =
            "use plato;\n"
            "$src = [<|x:CurrentUtcDatetime(), y:1|>, <|x:CurrentUtcDatetime(), y:2|>];\n"
            "insert into Output with truncate\n";

        ExtractAndExecuteRanges(prefixDt + "select * from as_table($src) where x > Timestamp('2105-12-31T23:59:59.123456Z');",
            R"__({"Write"=[{"Data"=[]}]})__");
        ExtractAndExecuteRanges(prefixDt + "select * from as_table($src) where x > Timestamp('2105-12-31T23:59:58.123456Z');",
            R"__({"Write"=[{"Data"=[[[["4291747199"];#;"1"];[#;#;"0"]]]}]})__");
        ExtractAndExecuteRanges(prefixDt + "select * from as_table($src) where x < Timestamp('2105-12-31T23:59:59.123456Z');",
            R"__({"Write"=[{"Data"=[[[#;#;"0"];[["4291747199"];#;"1"]]]}]})__");

        TString prefixDate =
            "use plato;\n"
            "$src = [<|x:Just(CurrentUtcDate()), y:1|>, <|x:CurrentUtcDate(), y:2|>];\n"
            "insert into Output with truncate\n";

        ExtractAndExecuteRanges(prefixDate + "select * from as_table($src) where x > Datetime('2105-12-31T01:00:00Z');",
                            R"__({"Write"=[{"Data"=[]}]})__");
        ExtractAndExecuteRanges(prefixDate + "select * from as_table($src) where x > Timestamp('2105-12-30T23:59:59Z');",
                            R"__({"Write"=[{"Data"=[[[[["49672"]];#;"1"];[#;#;"0"]]]}]})__");
        ExtractAndExecuteRanges(prefixDate + "select * from as_table($src) where x < Timestamp('2105-12-31T23:59:59Z');",
                            R"__({"Write"=[{"Data"=[[[[#];#;"0"];[[["49672"]];#;"1"]]]}]})__");
    }

    Y_UNIT_TEST(RangeRestInAnd) {
        TString prog =
            "use plato;\n"
            "$src = [<|x:1, y:2, z:3|>, <|x:1/0, y:1/0, z:3|>];\n"
            "insert into Output with truncate\n"
            "select * from as_table($src) where x < 10 and z in (1, 2);";

        TString expectedExecuteResult = R"__({"Write"=[{"Data"=[[[[#];#;"0"];[[["10"]];#;"0"]]]}]})__";
        ExtractAndExecuteRanges(prog, expectedExecuteResult);
    }

    Y_UNIT_TEST(RangeRestInAnd2) {
        TString prog =
            "use plato;\n"
            "$src = [<|x:1, y:2, z:3|>, <|x:1/0, y:1/0, z:3|>];\n"
            "insert into Output with truncate\n"
            "select * from as_table($src) where x == 10 and y > 123 and z in (1, 2);";

        TString expectedExecuteResult = R"__({"Write"=[{"Data"=[[[[["10"]];[["123"]];"0"];[[["10"]];#;"1"]]]}]})__";
        ExtractAndExecuteRanges(prog, expectedExecuteResult);
    }

    Y_UNIT_TEST(SecondIndexAndTuple) {
        TString prog = GetOptionalSrc() +
            "insert into Output with truncate\n"
            "select * from as_table($src) where y > 2 and (x, y) <= (5, 1 + 3);";

        TString expectedExecuteResult = R"__({"Write"=[{"Data"=[[[[#];#;"0"];[[["5"]];#;"0"]];[[[["5"]];[#];"0"];[[["5"]];[["5"]];"0"]]]}]})__";
        ExtractAndExecuteRanges(prog, expectedExecuteResult);
    }

    Y_UNIT_TEST(IsNotNull) {
        TString prog = GetOptionalSrc() +
            "insert into Output with truncate\n"
            "select * from as_table($src) where x is not null;";

        TString expectedExecuteResult = R"__({"Write"=[{"Data"=[[[[#];#;"0"];[#;#;"0"]]]}]})__";
        ExtractAndExecuteRanges(prog, expectedExecuteResult);
    }

    Y_UNIT_TEST(IsNull) {
        TString prog = GetOptionalSrc() +
            "insert into Output with truncate\n"
            "select * from as_table($src) where x is null;";

        TString expectedExecuteResult = R"__({"Write"=[{"Data"=[[[[#];#;"1"];[[#];#;"1"]]]}]})__";
        ExtractAndExecuteRanges(prog, expectedExecuteResult);
    }

    Y_UNIT_TEST(NaNComparison) {
        TString prog =
            "use plato;\n"
            "$src = [<|x:Double('nan'), y:1|>, <|x:Double('inf'), y:2|>];\n"
            "insert into Output with truncate\n"
            "select * from as_table($src) where x > Double('nan');";

        TString expectedExecuteResult = R"__({"Write"=[{"Data"=[]}]})__";
        ExtractAndExecuteRanges(prog, expectedExecuteResult);
    }

    Y_UNIT_TEST(NaNEquals) {
        TString prog =
            "use plato;\n"
            "$src = [<|x:Decimal('nan', 15, 10), y:1|>, <|x:Decimal('inf', 15, 10), y:2|>];\n"
            "insert into Output with truncate\n"
            "select * from as_table($src) where x = Decimal('nan', 15, 10);";

        TString expectedExecuteResult = R"__({"Write"=[{"Data"=[]}]})__";
        ExtractAndExecuteRanges(prog, expectedExecuteResult);
    }

    Y_UNIT_TEST(NaNNotEquals) {
        TString prog =
            "use plato;\n"
            "$src = [<|x:Float('nan'), y:1|>, <|x:Float('inf'), y:2|>];\n"
            "insert into Output with truncate\n"
            "select * from as_table($src) where x != 0.0f/0.0f;";

        TString expectedExecuteResult = R"__({"Write"=[{"Data"=[[[#;#;"0"];[#;#;"0"]]]}]})__";
        ExtractAndExecuteRanges(prog, expectedExecuteResult);
    }


    Y_UNIT_TEST(PointPrefixLen) {
        TString prog =
            "use plato;\n"
            "$src = [<|x:1, y:1, z:1|>, <|x:2, y:2, z:2|>];\n"
            "insert into Output with truncate\n"
            "select * from as_table($src) where x = 1 and y in (1, 2, 3) and z > 0;";

        TExprContext exprCtx;
        TTypeAnnotationContextPtr typesCtx;
        TExprNode::TPtr exprRoot = ParseAndOptimize(prog, exprCtx, typesCtx);
        TExprNode::TPtr filterLambda = LocateFilterLambda(exprRoot);

        THashSet<TString> usedColumns;
        using NDetail::TPredicateRangeExtractor;

        TPredicateExtractorSettings settings;
        settings.HaveNextValueCallable = true;
        auto extractor = MakePredicateRangeExtractor(settings);

        UNIT_ASSERT(extractor->Prepare(filterLambda, *filterLambda->Head().Head().GetTypeAnn(), usedColumns, exprCtx, *typesCtx));

        auto buildResult = extractor->BuildComputeNode({ "x", "y", "z"}, exprCtx, *typesCtx);
        UNIT_ASSERT(buildResult.ComputeNode);
        UNIT_ASSERT(buildResult.UsedPrefixLen == 3);
        UNIT_ASSERT(buildResult.PointPrefixLen == 2);
    }

    Y_UNIT_TEST(UnlimitedRangesResidual) {
        TString prog =
            "use plato;\n"
            "declare $param as List<Int32>;\n"
            "$src = [<|x:1, y:1, z:1|>, <|x:2, y:2, z:2|>];\n"
            "insert into Output with truncate\n"
            "select * from as_table($src) where x = 1 and y in $param and z = 0;";

        TExprContext exprCtx;
        TTypeAnnotationContextPtr typesCtx;
        TExprNode::TPtr exprRoot = ParseAndOptimize(prog, exprCtx, typesCtx);
        TExprNode::TPtr filterLambda = LocateFilterLambda(exprRoot);

        THashSet<TString> usedColumns;
        using NDetail::TPredicateRangeExtractor;

        TPredicateExtractorSettings settings;
        settings.MaxRanges = Nothing();
        auto extractor = MakePredicateRangeExtractor(settings);

        UNIT_ASSERT(extractor->Prepare(filterLambda, *filterLambda->Head().Head().GetTypeAnn(), usedColumns, exprCtx, *typesCtx));

        auto buildResult = extractor->BuildComputeNode({ "x", "y", "z"}, exprCtx, *typesCtx);

        auto canonicalLambda =
            "(\n"
            "(return (lambda '($1) (OptionalIf (Bool 'true) $1)))\n"
            ")\n";
        auto lambda = DumpNode(*buildResult.PrunedLambda, exprCtx);
        UNIT_ASSERT_EQUAL(lambda, canonicalLambda);
        //Cerr << DumpNode(*buildResult.ComputeNode, exprCtx);
    }

    Y_UNIT_TEST(BoolPredicate) {
        TString prog =
            "use plato;\n"
            "declare $param as List<Int32>;\n"
            "$src = [<|x:true, y:1|>, <|x:false, y:2|>];\n"
            "insert into Output with truncate\n"
            "select * from as_table($src) where not x;";

        TExprContext exprCtx;
        TTypeAnnotationContextPtr typesCtx;
        TExprNode::TPtr exprRoot = ParseAndOptimize(prog, exprCtx, typesCtx);
        TExprNode::TPtr filterLambda = LocateFilterLambda(exprRoot);

        THashSet<TString> usedColumns;
        using NDetail::TPredicateRangeExtractor;

        TPredicateExtractorSettings settings;
        auto extractor = MakePredicateRangeExtractor(settings);

        UNIT_ASSERT(extractor->Prepare(filterLambda, *filterLambda->Head().Head().GetTypeAnn(), usedColumns, exprCtx, *typesCtx));

        auto buildResult = extractor->BuildComputeNode({ "x" }, exprCtx, *typesCtx);

        UNIT_ASSERT(buildResult.ComputeNode);

        auto canonicalRanges =
            "(\n"
            "(return (RangeFinalize (RangeMultiply (Uint64 '10000) (RangeUnion (RangeFor '=== (Bool 'false) (DataType 'Bool))))))\n"
            ")\n";
        auto ranges = DumpNode(*buildResult.ComputeNode, exprCtx);
        UNIT_ASSERT_EQUAL(ranges, canonicalRanges);

        auto canonicalLambda =
            "(\n"
            "(return (lambda '($1) (OptionalIf (Bool 'true) $1)))\n"
            ")\n";
        auto lambda = DumpNode(*buildResult.PrunedLambda, exprCtx);
        UNIT_ASSERT_EQUAL(lambda, canonicalLambda);
    }

    Y_UNIT_TEST(BoolPredicateLiteralRange) {
        TString prog =
            "use plato;\n"
            "declare $param as List<Int32>;\n"
            "$src = [<|x:true, y:1|>, <|x:false, y:2|>];\n"
            "insert into Output with truncate\n"
            "select * from as_table($src) where not x and y = 1;";

        TExprContext exprCtx;
        TTypeAnnotationContextPtr typesCtx;
        TExprNode::TPtr exprRoot = ParseAndOptimize(prog, exprCtx, typesCtx);
        TExprNode::TPtr filterLambda = LocateFilterLambda(exprRoot);

        THashSet<TString> usedColumns;
        using NDetail::TPredicateRangeExtractor;

        TPredicateExtractorSettings settings;
        settings.BuildLiteralRange = true;
        auto extractor = MakePredicateRangeExtractor(settings);

        UNIT_ASSERT(extractor->Prepare(filterLambda, *filterLambda->Head().Head().GetTypeAnn(), usedColumns, exprCtx, *typesCtx));

        auto buildResult = extractor->BuildComputeNode({ "x", "y"}, exprCtx, *typesCtx);

        UNIT_ASSERT(buildResult.ComputeNode);
        UNIT_ASSERT(buildResult.LiteralRange);

        auto canonicalRanges =
            "(\n"
            "(let $1 (RangeFor '== (Bool 'false) (DataType 'Bool)))\n"
            "(let $2 (RangeFor '=== (Int32 '1) (DataType 'Int32)))\n"
            "(return (RangeFinalize (RangeMultiply (Uint64 '10000) (RangeUnion (RangeIntersect (RangeMultiply (Uint64 '10000) $1 $2))))))\n"
            ")\n";
        auto ranges = DumpNode(*buildResult.ComputeNode, exprCtx);
        UNIT_ASSERT_EQUAL(ranges, canonicalRanges);

        auto canonicalLambda =
            "(\n"
            "(return (lambda '($1) (OptionalIf (Bool 'true) $1)))\n"
            ")\n";
        auto lambda = DumpNode(*buildResult.PrunedLambda, exprCtx);

        UNIT_ASSERT_EQUAL(lambda, canonicalLambda);
    }

    Y_UNIT_TEST(LookupAnd) {
        TString prog = GetNonOptionsSrc4() +
            "insert into Output with truncate\n"
            "select * from as_table($src) where (x, y) > (1, 2) and x in [0, 1, 2, 3];";

        TExprContext exprCtx;
        TTypeAnnotationContextPtr typesCtx;
        TExprNode::TPtr exprRoot = ParseAndOptimize(prog, exprCtx, typesCtx);
        TExprNode::TPtr filterLambda = LocateFilterLambda(exprRoot);

        THashSet<TString> usedColumns;
        using NDetail::TPredicateRangeExtractor;

        auto extractor = MakePredicateRangeExtractor({});
        UNIT_ASSERT(extractor->Prepare(filterLambda, *filterLambda->Head().Head().GetTypeAnn(), usedColumns, exprCtx, *typesCtx));

        auto buildResult = extractor->BuildComputeNode({ "x", "y" }, exprCtx, *typesCtx);
        UNIT_ASSERT(buildResult.ComputeNode);

        auto canonicalRanges =
            "(\n"
            "(let $1 (Int32 '1))\n"
            "(let $2 (Int32 '\"2\"))\n"
            "(let $3 (DataType 'Int32))\n"
            "(let $4 (OptionalType $3))\n"
            "(let $5 (IfPresent (Map (Just (AsList (Int32 '0) $1 $2 (Int32 '\"3\"))) (lambda '($13) $13)) (lambda '($14) (block '(\n"
            "  (let $15 (Collect (Take (FlatMap $14 (lambda '($17) (block '(\n"
            "    (let $18 (RangeFor '== $17 $3))\n"
            "    (return (RangeMultiply (Uint64 '10000) $18))\n"
            "  )))) (Uint64 '10001))))\n"
            "  (let $16 '((Nothing $4) (Int32 '0)))\n"
            "  (return (IfStrict (> (Length $15) (Uint64 '10000)) (AsRange '($16 $16)) (RangeUnion $15)))\n"
            "))) (RangeEmpty $3)))\n"
            "(let $6 '((Nothing $4) (Int32 '0)))\n"
            "(let $7 (RangeMultiply (Uint64 '10000) $5 (AsRange '($6 $6))))\n"
            "(let $8 (RangeFor '> $1 $3))\n"
            "(let $9 '((Nothing $4) (Int32 '0)))\n"
            "(let $10 (RangeMultiply (Uint64 '10000) $8 (AsRange '($9 $9))))\n"
            "(let $11 (RangeFor '== $1 $3))\n"
            "(let $12 (RangeFor '> $2 $3))\n"
            "(return (RangeFinalize (RangeMultiply (Uint64 '10000) (RangeUnion (RangeIntersect $7 (RangeUnion $10 (RangeIntersect (RangeMultiply (Uint64 '10000) $11 $12))))))))\n"
            ")\n"
            ;
        auto ranges = DumpNode(*buildResult.ComputeNode, exprCtx);
        Cerr << ranges;

        auto canonicalLambda =
            "(\n"
            "(return (lambda '($1) (OptionalIf (Bool 'true) $1)))\n"
            ")\n";
        auto lambda = DumpNode(*buildResult.PrunedLambda, exprCtx);
        Cerr << lambda;

        UNIT_ASSERT_EQUAL(ranges, canonicalRanges);
        UNIT_ASSERT_EQUAL(lambda, canonicalLambda);

        TString expectedExecuteResult = R"__({"Write"=[{"Data"=[[[["1"];["2"];"0"];[["1"];#;"1"]];[[["2"];#;"1"];[["2"];#;"1"]];[[["3"];#;"1"];[["3"];#;"1"]]]}]})__";
        ExecuteAndCheckRanges(ranges, expectedExecuteResult);
    }

    Y_UNIT_TEST(MixedAnd) {
        TString prog = GetNonOptionsSrc4() +
            "insert into Output with truncate\n"
            "select * from as_table($src) where (x in [1, 2]) and (x, y) in [(2, 3), (3, 4)] and (z, t) < (4, 5) and (y, z, t) > (0,0,0);";

        TExprContext exprCtx;
        TTypeAnnotationContextPtr typesCtx;
        TExprNode::TPtr exprRoot = ParseAndOptimize(prog, exprCtx, typesCtx);
        TExprNode::TPtr filterLambda = LocateFilterLambda(exprRoot);

        THashSet<TString> usedColumns;
        using NDetail::TPredicateRangeExtractor;

        auto extractor = MakePredicateRangeExtractor({});
        UNIT_ASSERT(extractor->Prepare(filterLambda, *filterLambda->Head().Head().GetTypeAnn(), usedColumns, exprCtx, *typesCtx));

        auto buildResult = extractor->BuildComputeNode({ "x", "y", "z", "t" }, exprCtx, *typesCtx);
        UNIT_ASSERT(buildResult.ComputeNode);

        auto ranges = DumpNode(*buildResult.ComputeNode, exprCtx);
        Cerr << ranges;

        auto canonicalLambda =
            "(\n"
            "(return (lambda '($1) (OptionalIf (Bool 'true) $1)))\n"
            ")\n";
        auto lambda = DumpNode(*buildResult.PrunedLambda, exprCtx);
        Cerr << lambda;

        TString expectedExecuteResult = R"__({"Write"=[{"Data"=[[[["2"];["3"];#;#;"1"];[["2"];["3"];["4"];["5"];"0"]]]}]})__";
        ExecuteAndCheckRanges(ranges, expectedExecuteResult);

        UNIT_ASSERT_EQUAL(lambda, canonicalLambda);
    }

    Y_UNIT_TEST(OverflowRanges) {
        TString prog1 = GetOptionalSrc() +
            "insert into Output with truncate\n"
            "select * from as_table($src) where x = 1 and y in [1, 2, 3];";

        TString prog2 = GetOptionalSrc() +
            "insert into Output with truncate\n"
            "select * from as_table($src) where x = 1 and y in [1, 2, 3] and (x, y) < (4, 4);";

        TString prog3 = GetOptionalSrc() +
            "insert into Output with truncate\n"
            "select * from as_table($src) where x = 1;";

        TString expectedExecuteResult = R"__({"Write"=[{"Data"=[[[[["1"]];#;"1"];[[["1"]];#;"1"]]]}]})__";
        ExtractAndExecuteRanges(prog1, expectedExecuteResult, TPredicateExtractorSettings{.MaxRanges = 2});
        ExtractAndExecuteRanges(prog2, expectedExecuteResult, TPredicateExtractorSettings{.MaxRanges = 2});
        ExtractAndExecuteRanges(prog3, expectedExecuteResult, TPredicateExtractorSettings{.MaxRanges = 2});
    }

    Y_UNIT_TEST(OverflowRanges2) {
        TString prog = GetNonOptionsSrc4() +
            "insert into Output with truncate\n"
            "select * from as_table($src) where x = 1 and ((y = 2 and z in [1, 2, 3]) or ((y, z) = (2, 2))) and t = 3;";

        TString expectedExecuteResult = R"__({"Write"=[{"Data"=[[[["1"];["2"];#;#;"1"];[["1"];["2"];#;#;"1"]]]}]})__";
        ExtractAndExecuteRanges(prog, expectedExecuteResult, TPredicateExtractorSettings{.MaxRanges = 2}, { "x", "y", "z", "t" });
    }
}

} // namespace NYql
