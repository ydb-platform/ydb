#include <ydb/library/yql/providers/yt/provider/yql_yt_provider.h>
#include <ydb/library/yql/providers/yt/gateway/file/yql_yt_file.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/ast/yql_ast_annotation.h>
#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/type_ann/type_ann_core.h>
#include <ydb/library/yql/core/type_ann/type_ann_expr.h>
#include <ydb/library/yql/core/ut_common/yql_ut_common.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/services/yql_transform_pipeline.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/string/cast.h>

namespace NYql {

Y_UNIT_TEST_SUITE(TYqlExprConstraints) {

    static TExprNode::TPtr ParseAndAnnotate(const TStringBuf program, TExprContext& exprCtx) {
        TAstParseResult astRes = ParseAst(program);
        UNIT_ASSERT(astRes.IsOk());
        TExprNode::TPtr exprRoot;
        UNIT_ASSERT(CompileExpr(*astRes.Root, exprRoot, exprCtx, nullptr, nullptr));

        auto functionRegistry = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry());
        TTestTablesMapping testTables;
        auto yqlNativeServices = NFile::TYtFileServices::Make(functionRegistry.Get(), testTables);
        auto ytGateway = CreateYtFileGateway(yqlNativeServices);
        auto typeAnnotationContext = MakeIntrusive<TTypeAnnotationContext>();
        typeAnnotationContext->RandomProvider = CreateDeterministicRandomProvider(1);
        auto ytState = MakeIntrusive<TYtState>();
        ytState->Gateway = ytGateway;
        ytState->Types = typeAnnotationContext.Get();

        InitializeYtGateway(ytGateway, ytState);
        typeAnnotationContext->AddDataSink(YtProviderName, CreateYtDataSink(ytState));
        typeAnnotationContext->AddDataSource(YtProviderName, CreateYtDataSource(ytState));

        auto transformer = TTransformationPipeline(typeAnnotationContext)
            .AddServiceTransformers()
            .AddPreTypeAnnotation()
            .AddExpressionEvaluation(*functionRegistry)
            .AddIOAnnotation()
            .AddTypeAnnotation()
            .AddPostTypeAnnotation()
            .Build();

        const auto status = SyncTransform(*transformer, exprRoot, exprCtx);
        if (status == IGraphTransformer::TStatus::Error)
            Cerr << exprCtx.IssueManager.GetIssues().ToString() << Endl;
        UNIT_ASSERT(status == IGraphTransformer::TStatus::Ok);
        return exprRoot;
    }

    template <class TConstraint>
    static void CheckConstraint(const TExprNode::TPtr& exprRoot, const TStringBuf nodeName, const TStringBuf constrStr) {
        TExprNode* nodeToCheck = nullptr;
        VisitExpr(exprRoot, [nodeName, &nodeToCheck] (const TExprNode::TPtr& node) {
            if (node->IsCallable(nodeName)) {
                nodeToCheck = node.Get();
            }
            return !nodeToCheck;
        });
        UNIT_ASSERT(nodeToCheck);
        UNIT_ASSERT(nodeToCheck->GetState() == TExprNode::EState::ConstrComplete);
        const auto constr = nodeToCheck->GetConstraint<TConstraint>();
        if (constrStr.empty()) {
            UNIT_ASSERT(!constr);
        } else {
            UNIT_ASSERT(constr);
            UNIT_ASSERT(constr->IsApplicableToType(*nodeToCheck->GetTypeAnn()));
            UNIT_ASSERT_VALUES_EQUAL(ToString(*constr), constrStr);
        }
    }

    Y_UNIT_TEST(Sort) {
        const auto s = R"((
            (let mr_sink (DataSink 'yt (quote plato)))
            (let list (AsList
                (AsStruct '('key (String '4)) '('subkey (String 'c)) '('value (String 'v)))
                (AsStruct '('key (String '1)) '('subkey (String 'd)) '('value (String 'v)))
                (AsStruct '('key (String '3)) '('subkey (String 'b)) '('value (String 'v)))
            ))
            (let sorted (Sort list (Bool 'True) (lambda '(item) (Member item 'key))))
            (let world (Write! world mr_sink (Key '('table (String 'Output))) sorted '('('mode 'renew))))
            (let world (Commit! world mr_sink))
            (return world)
        ))";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TSortedConstraintNode>(exprRoot, "Sort", "Sorted(key[asc])");
    }

    Y_UNIT_TEST(SortByStablePickle) {
        const auto s = R"((
            (let mr_sink (DataSink 'yt (quote plato)))
            (let list (AsList
                (AsStruct '('key (String '4)) '('subkey (String 'c)) '('value (String 'v)))
                (AsStruct '('key (String '1)) '('subkey (String 'd)) '('value (String 'v)))
                (AsStruct '('key (String '3)) '('subkey (String 'b)) '('value (String 'v)))
            ))
            (let sorted (Sort list '((Bool 'False) (Bool 'True)) (lambda '(item) '((Member item 'key) (StablePickle (Member item 'subkey))))))
            (let world (Write! world mr_sink (Key '('table (String 'Output))) sorted '('('mode 'renew))))
            (let world (Commit! world mr_sink))
            (return world)
        ))";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TSortedConstraintNode>(exprRoot, "Sort", "");
    }

    Y_UNIT_TEST(SortByTranspentIfPresent) {
        const auto s = R"((
            (let mr_sink (DataSink 'yt (quote plato)))
            (let list (AsList
                (Just (AsStruct '('key (String '4)) '('subkey (String 'c)) '('value (String 'v))))
                (Just (AsStruct '('key (String '1)) '('subkey (String 'd)) '('value (String 'v))))
                (Just (AsStruct '('key (String '3)) '('subkey (String 'b)) '('value (String 'v))))
            ))
            (let sorted (Sort list (Bool 'True) (lambda '(row) (FlatMap row (lambda '(item) (Just (Member item 'key)))))))
            (let map (OrderedFlatMap sorted (lambda '(item) item)))
            (let world (Write! world mr_sink (Key '('table (String 'Output))) map '('('mode 'renew))))
            (let world (Commit! world mr_sink))
            (return world)
        ))";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TSortedConstraintNode>(exprRoot, "OrderedFlatMap", "Sorted(key[asc])");
    }

    Y_UNIT_TEST(SortByDuplicateColumn) {
        const auto s = R"((
            (let mr_sink (DataSink 'yt (quote plato)))
            (let list (AsList
                (AsStruct '('key (String '4)) '('subkey (String 'c)) '('value (String 'v)))
                (AsStruct '('key (String '1)) '('subkey (String 'd)) '('value (String 'v)))
                (AsStruct '('key (String '3)) '('subkey (String 'b)) '('value (String 'v)))
            ))
            (let sorted (Sort list '((Bool 'True) (Bool 'False) (Bool 'True)) (lambda '(item) '((Member item 'key) (Member item 'key) (Member item 'subkey)))))
            (let world (Write! world mr_sink (Key '('table (String 'Output))) sorted '('('mode 'renew))))
            (let world (Commit! world mr_sink))
            (return world)
        ))";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TSortedConstraintNode>(exprRoot, "Sort", "Sorted(key[asc];subkey[asc])");
    }

    Y_UNIT_TEST(SortByFullRow) {
        const auto s = R"((
            (let mr_sink (DataSink 'yt (quote plato)))
            (let list (AsList (Utf8 's) (Utf8 'o) (Utf8 'r) (Utf8 't)))
            (let sorted (Sort list (Bool 'True) (lambda '(item) item)))
            (let map (OrderedMap sorted (lambda '(item) (AsStruct '('key item)))))
            (let world (Write! world mr_sink (Key '('table (String 'Output))) map '('('mode 'renew))))
            (let world (Commit! world mr_sink))
            (return world)
        ))";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TSortedConstraintNode>(exprRoot, "Sort", "Sorted(/[asc])");
        CheckConstraint<TSortedConstraintNode>(exprRoot, "OrderedMap", "Sorted(key[asc])");
    }

    Y_UNIT_TEST(SortByTuple) {
        const auto s = R"((
            (let mr_sink (DataSink 'yt (quote plato)))
            (let list (AsList
                (AsStruct '('key (String '4)) '('subkey (String 'c)) '('value (String 'v)))
                (AsStruct '('key (String '1)) '('subkey (String 'd)) '('value (String 'v)))
                (AsStruct '('key (String '3)) '('subkey (String 'b)) '('value (String 'v)))
            ))
            (let sorted (Sort list (Bool 'False) (lambda '(item) '((Member item 'key) (Member item 'subkey)))))
            (let world (Write! world mr_sink (Key '('table (String 'Output))) sorted '('('mode 'renew))))
            (let world (Commit! world mr_sink))
            (return world)
        ))";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TSortedConstraintNode>(exprRoot, "Sort", "Sorted(key[desc];subkey[desc])");
    }

    Y_UNIT_TEST(SortByTupleElements) {
        const auto s = R"((
            (let mr_sink (DataSink 'yt (quote plato)))
            (let list (AsList
                (AsStruct '('key (String '4)) '('subkey (String 'c)) '('value '((String 'x) (String 'a) (String 'u))))
                (AsStruct '('key (String '1)) '('subkey (String 'd)) '('value '((String 'y) (String 'b) (String 'v))))
                (AsStruct '('key (String '3)) '('subkey (String 'b)) '('value '((String 'z) (String 'c) (String 'w))))
            ))
            (let sorted (Sort list '((Not (Bool 'False)) (Bool 'False) (Not (Bool 'True))) (lambda '(item) '((Nth (Member item 'value) '2) (Member item 'key) (Nth (Member item 'value) '0)))))
            (let map (OrderedMap sorted (lambda '(item) (AsStruct '('tuple '((Nth (Member item 'value) '1) (Nth (Member item 'value) '2) (Nth (Member item 'value) '0) (Member item 'subkey) (Member item 'key)))))))
            (let world (Write! world mr_sink (Key '('table (String 'Output))) map '('('mode 'renew))))
            (let world (Commit! world mr_sink))
            (return world)
        ))";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TSortedConstraintNode>(exprRoot, "Sort", "Sorted(value/2[asc];key[desc];value/0[desc])");
        CheckConstraint<TSortedConstraintNode>(exprRoot, "OrderedMap", "Sorted(tuple/1[asc];tuple/4[desc];tuple/2[desc])");
    }

    Y_UNIT_TEST(SortByAllTupleElementsInRightOrder) {
        const auto s = R"((
            (let mr_sink (DataSink 'yt (quote plato)))
            (let list (AsList
                (AsStruct '('key (String '4)) '('subkey (String 'c)) '('value '((String 'x) (String 'a) (String 'u))))
                (AsStruct '('key (String '1)) '('subkey (String 'd)) '('value '((String 'y) (String 'b) (String 'v))))
                (AsStruct '('key (String '3)) '('subkey (String 'b)) '('value '((String 'z) (String 'c) (String 'w))))
            ))
            (let sorted (Sort list (Bool 'False) (lambda '(item) '((Nth (Member item 'value) '0) (Nth (Member item 'value) '1) (Nth (Member item 'value) '2)))))
            (let map (OrderedMap sorted (lambda '(item) (AsStruct '('tuple (Member item 'value))))))
            (let world (Write! world mr_sink (Key '('table (String 'Output))) map '('('mode 'renew))))
            (let world (Commit! world mr_sink))
            (return world)
        ))";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TSortedConstraintNode>(exprRoot, "Sort", "Sorted(value[desc])");
        CheckConstraint<TSortedConstraintNode>(exprRoot, "OrderedMap", "Sorted(tuple[desc])");
    }

    Y_UNIT_TEST(SortByAllTupleElementsInWrongOrder) {
        const auto s = R"((
            (let mr_sink (DataSink 'yt (quote plato)))
            (let list (AsList
                (AsStruct '('key (String '4)) '('subkey (String 'c)) '('value '((String 'x) (String 'a) (String 'u))))
                (AsStruct '('key (String '1)) '('subkey (String 'd)) '('value '((String 'y) (String 'b) (String 'v))))
                (AsStruct '('key (String '3)) '('subkey (String 'b)) '('value '((String 'z) (String 'c) (String 'w))))
            ))
            (let sorted (Sort list (Bool 'True) (lambda '(item) '((Nth (Member item 'value) '1) (Nth (Member item 'value) '0) (Nth (Member item 'value) '2)))))
            (let map (OrderedMap sorted (lambda '(item) (AsStruct '('tuple (Member item 'value))))))
            (let world (Write! world mr_sink (Key '('table (String 'Output))) map '('('mode 'renew))))
            (let world (Commit! world mr_sink))
            (return world)
        ))";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TSortedConstraintNode>(exprRoot, "Sort", "Sorted(value/1[asc];value/0[asc];value/2[asc])");
        CheckConstraint<TSortedConstraintNode>(exprRoot, "OrderedMap", "Sorted(tuple/1[asc];tuple/0[asc];tuple/2[asc])");
    }

    Y_UNIT_TEST(SortByTupleWithSingleElementAndCopyOfElement) {
        const auto s = R"((
            (let mr_sink (DataSink 'yt (quote plato)))
            (let list (AsList
                (AsStruct '('key (String '4)) '('subkey (String 'c)) '('value '((String 'x))))
                (AsStruct '('key (String '1)) '('subkey (String 'd)) '('value '((String 'y))))
                (AsStruct '('key (String '3)) '('subkey (String 'b)) '('value '((String 'z))))
            ))
            (let sorted (Sort list (Bool 'False) (lambda '(item) '((Nth (Member item 'value) '0)))))
            (let map (OrderedMap sorted (lambda '(item) (AsStruct '('tuple (Member item 'value)) '('element (Nth (Member item 'value) '0))))))
            (let world (Write! world mr_sink (Key '('table (String 'Output))) map '('('mode 'renew))))
            (let world (Commit! world mr_sink))
            (return world)
        ))";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TSortedConstraintNode>(exprRoot, "OrderedMap", "Sorted(element,tuple[desc])");
    }

    Y_UNIT_TEST(SortByFullTupleOnTop) {
        const auto s = R"((
            (let mr_sink (DataSink 'yt (quote plato)))
            (let list (AsList
                '((String 'x) (String 'a) (String 'u))
                '((String 'y) (String 'b) (String 'v))
                '((String 'z) (String 'c) (String 'w))
            ))
            (let sorted (Sort list (Bool 'False) (lambda '(item) item)))
            (let map (OrderedMap sorted (lambda '(item) (AsStruct '('one (Nth item '0)) '('two (Nth item '1)) '('three (Nth item '2))))))
            (let world (Write! world mr_sink (Key '('table (String 'Output))) map '('('mode 'renew))))
            (let world (Commit! world mr_sink))
            (return world)
        ))";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TSortedConstraintNode>(exprRoot, "Sort", "Sorted(0[desc];1[desc];2[desc])");
        CheckConstraint<TSortedConstraintNode>(exprRoot, "OrderedMap", "Sorted(one[desc];two[desc];three[desc])");
    }

    Y_UNIT_TEST(SortByColumnAndExpr) {
        const auto s = R"((
            (let mr_sink (DataSink 'yt (quote plato)))
            (let list (AsList
                (AsStruct '('key (String '4)) '('subkey (String 'c)) '('value (String 'v)))
                (AsStruct '('key (String '1)) '('subkey (String 'd)) '('value (String 'v)))
                (AsStruct '('key (String '3)) '('subkey (String 'b)) '('value (String 'v)))
            ))
            (let sorted (Sort list '((Bool 'True) (Bool 'False) (Bool 'True)) (lambda '(item) '((Member item 'key) (SafeCast (Member item 'value) (DataType 'Utf8)) (Member item 'subkey)))))
            (let world (Write! world mr_sink (Key '('table (String 'Output))) sorted '('('mode 'renew))))
            (let world (Commit! world mr_sink))
            (return world)
        ))";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TSortedConstraintNode>(exprRoot, "Sort", "");
    }

    Y_UNIT_TEST(SortDesc) {
        const auto s = R"((
            (let mr_sink (DataSink 'yt (quote plato)))
            (let list (AsList
                (AsStruct '('key (String '4)) '('subkey (String 'c)) '('value (String 'v)))
                (AsStruct '('key (String '1)) '('subkey (String 'd)) '('value (String 'v)))
                (AsStruct '('key (String '3)) '('subkey (String 'b)) '('value (String 'v)))
            ))
            (let sorted (Sort list '((Bool 'True) (Bool 'False)) (lambda '(item) '((Member item 'key) (Member item 'subkey)))))
            (let world (Write! world mr_sink (Key '('table (String 'Output))) sorted '('('mode 'renew))))
            (let world (Commit! world mr_sink))
            (return world)
        ))";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TSortedConstraintNode>(exprRoot, "Sort", "Sorted(key[asc];subkey[desc])");
    }

    Y_UNIT_TEST(SortedOverWideMap) {
        const auto s = R"((
            (let mr_sink (DataSink 'yt (quote plato)))
            (let list (AsList
                (AsStruct '('key (String '4)) '('subkey (String 'c)) '('value (String 'v)))
                (AsStruct '('key (String '1)) '('subkey (String 'd)) '('value (String 'v)))
                (AsStruct '('key (String '3)) '('subkey (String 'b)) '('value (String 'v)))
            ))
            (let sorted (ToFlow (Sort list '((Bool 'True) (Bool 'False)) (lambda '(item) '((Member item 'key) (Member item 'subkey))))))
            (let expand (ExpandMap sorted (lambda '(item) (Member item 'key) (Member item 'subkey) (Member item 'value))))
            (let wide (WideMap expand (lambda '(a b c) c b a)))
            (let narrow (NarrowMap wide (lambda '(x y z) (AsStruct '('x x) '('y y) '('z z)))))
            (let world (Write! world mr_sink (Key '('table (String 'Output))) (Collect narrow) '('('mode 'renew))))
            (let world (Commit! world mr_sink))
            (return world)
        ))";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TSortedConstraintNode>(exprRoot, "Collect", "Sorted(z[asc];y[desc])");
    }

    Y_UNIT_TEST(SortedOverWideTopSort) {
        const auto s = R"((
            (let mr_sink (DataSink 'yt (quote plato)))
            (let list (AsList
                (AsStruct '('key (String '4)) '('subkey (String 'c)) '('value (String 'v)))
                (AsStruct '('key (String '1)) '('subkey (String 'd)) '('value (String 'v)))
                (AsStruct '('key (String '3)) '('subkey (String 'b)) '('value (String 'v)))
            ))
            (let flow (ToFlow list))
            (let expand (ExpandMap flow (lambda '(item) (Member item 'key) (Member item 'subkey) (Member item 'value))))
            (let wide (WideTopSort expand (Uint64 '2) '('('2 (Bool 'False)) '('0 (Bool 'True)))))
            (let narrow (NarrowMap wide (lambda '(x y z) (AsStruct '('x x) '('y y) '('z z)))))
            (let world (Write! world mr_sink (Key '('table (String 'Output))) (Collect narrow) '('('mode 'renew))))
            (let world (Commit! world mr_sink))
            (return world)
        ))";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TSortedConstraintNode>(exprRoot, "Collect", "Sorted(z[desc];x[asc])");
    }

    Y_UNIT_TEST(SortedOverOrderedMultiMap) {
        const auto s = R"((
            (let mr_sink (DataSink 'yt (quote plato)))
            (let list (AsList
                (AsStruct '('key (String '4)) '('subkey (String 'c)) '('value (String 'u)))
                (AsStruct '('key (String '1)) '('subkey (String 'd)) '('value (String 'v)))
                (AsStruct '('key (String '3)) '('subkey (String 'b)) '('value (String 'w)))
            ))
            (let sorted (Sort list (Bool 'True) (lambda '(item) '((Member item 'key) (Member item 'value)))))
            (let map (OrderedMultiMap sorted (lambda '(row)
                (AsStruct '('x (Member row 'key)) '('y (Member row 'subkey)) '('z (Member row 'value)))
                (AsStruct '('x (Member row 'key)) '('y (String 'subkey_stub)) '('z (Member row 'value)))
            )))
            (let world (Write! world mr_sink (Key '('table (String 'Output))) map '('('mode 'renew))))
            (let world (Commit! world mr_sink))
            (return world)
        ))";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TSortedConstraintNode>(exprRoot, "OrderedMultiMap", "Sorted(x[asc];z[asc])");
    }

    Y_UNIT_TEST(SortedOverNestedFlatMap) {
        const auto s = R"((
            (let mr_sink (DataSink 'yt (quote plato)))
            (let list (AsList
                (AsStruct '('key (String '4)) '('subkey (String 'c)) '('value (String 'u)))
                (AsStruct '('key (String '1)) '('subkey (String 'd)) '('value (String 'v)))
                (AsStruct '('key (String '3)) '('subkey (String 'b)) '('value (String 'w)))
            ))
            (let sorted (Sort list (Bool 'True) (lambda '(item) '((Member item 'key) (Member item 'value)))))
            (let map (OrderedFlatMap sorted (lambda '(row) (FlatMap (ListFromRange (Uint8 '0) (Uint8 '5) (Uint8 '1)) (lambda '(index)
                (OptionalIf (AggrNotEquals index (Uint8 '3)) (AsStruct '('x (Member row 'key)) '('y (Concat (ToString index) (Member row 'subkey))) '('z (Member row 'value))))
            )))))
            (let world (Write! world mr_sink (Key '('table (String 'Output))) (LazyList map) '('('mode 'renew))))
            (let world (Commit! world mr_sink))
            (return world)
        ))";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TSortedConstraintNode>(exprRoot, "LazyList", "Sorted(x[asc];z[asc])");
    }

    Y_UNIT_TEST(SortedOverNestedOrderedFlatMapWithSort) {
        const auto s = R"((
            (let mr_sink (DataSink 'yt (quote plato)))
            (let list (AsList
                (AsStruct '('key (String '4)) '('subkey (String 'c)) '('value (String 'u)))
                (AsStruct '('key (String '1)) '('subkey (String 'd)) '('value (String 'v)))
                (AsStruct '('key (String '3)) '('subkey (String 'b)) '('value (String 'w)))
            ))
            (let sorted (Sort list (Bool 'True) (lambda '(item) '((Member item 'key) (Member item 'value)))))
            (let map (OrderedFlatMap sorted (lambda '(row) (OrderedFlatMap (Sort (ListFromRange (Uint8 '0) (Uint8 '5) (Uint8 '1)) (Bool 'True) (lambda '(item) item)) (lambda '(index)
                (OptionalIf (AggrNotEquals index (Uint8 '3)) (AsStruct '('x (Member row 'key)) '('y (Concat (ToString index) (Member row 'subkey))) '('z (Member row 'value))))
            )))))
            (let world (Write! world mr_sink (Key '('table (String 'Output))) (LazyList map) '('('mode 'renew))))
            (let world (Commit! world mr_sink))
            (return world)
        ))";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TSortedConstraintNode>(exprRoot, "LazyList", "Sorted(x[asc];z[asc])");
    }

    Y_UNIT_TEST(TopSort) {
        const auto s = R"((
            (let mr_sink (DataSink 'yt (quote plato)))
            (let list (AsList
                (AsStruct '('key (String '4)) '('subkey (String 'c)) '('value (String 'v)))
                (AsStruct '('key (String '1)) '('subkey (String 'd)) '('value (String 'v)))
                (AsStruct '('key (String '3)) '('subkey (String 'b)) '('value (String 'v)))
            ))
            (let sorted (TopSort list (Uint64 '2) '((Bool 'True) (Bool 'True)) (lambda '(item) '((Member item 'key) (Member item 'subkey)))))
            (let world (Write! world mr_sink (Key '('table (String 'Output))) sorted '('('mode 'renew))))
            (let world (Commit! world mr_sink))
            (return world)
        ))";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TSortedConstraintNode>(exprRoot, "TopSort", "Sorted(key[asc];subkey[asc])");
    }

    Y_UNIT_TEST(MergeWithFirstEmpty) {
        const auto s = R"((
            (let mr_sink (DataSink 'yt (quote plato)))
            (let list (AsList
                (AsStruct '('key (String '4)) '('subkey (String 'c)) '('value (String 'v)))
                (AsStruct '('key (String '1)) '('subkey (String 'd)) '('value (String 'v)))
                (AsStruct '('key (String '3)) '('subkey (String 'b)) '('value (String 'v)))
            ))
            (let sorted (Sort list (Bool 'True) (lambda '(item) (Member item 'key))))
            (let empty (List (ListType (StructType '('key (DataType 'String)) '('subkey (DataType 'String)) '('value (DataType 'String))))))
            (let merged (Merge empty sorted))
            (let world (Write! world mr_sink (Key '('table (String 'Output))) merged '('('mode 'renew))))
            (let world (Commit! world mr_sink))
            (return world)
        ))";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TSortedConstraintNode>(exprRoot, "Merge", "Sorted(key[asc])");
    }

    Y_UNIT_TEST(MergeWithCloneColumns) {
        const auto s = R"((
            (let mr_sink (DataSink 'yt (quote plato)))
            (let list (AsList
                (AsStruct '('key (String '4)) '('subkey (String 'c)) '('value (String 'v)))
                (AsStruct '('key (String '1)) '('subkey (String 'd)) '('value (String 'v)))
                (AsStruct '('key (String '3)) '('subkey (String 'b)) '('value (String 'v)))
            ))
            (let sorted (Sort list (Bool 'False) (lambda '(item) (Member item 'key))))
            (let clone (OrderedMap sorted (lambda '(row) (AsStruct '('key (Member row 'key)) '('subkey (Member row 'key)) '('value (Member row 'key))))))
            (let merged (Merge clone sorted))
            (let world (Write! world mr_sink (Key '('table (String 'Output))) merged '('('mode 'renew))))
            (let world (Commit! world mr_sink))
            (return world)
        ))";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TSortedConstraintNode>(exprRoot, "Merge", "Sorted(key[desc])");
    }

    Y_UNIT_TEST(UnionMergeWithDiffTypes) {
        const auto s = R"((
            (let mr_sink (DataSink 'yt (quote plato)))
            (let list1 (AsList
                (AsStruct '('key (String '4)) '('subkey (String 'c)) '('value (String 'v)))
                (AsStruct '('key (String '1)) '('subkey (String 'd)) '('value (String 'v)))
                (AsStruct '('key (String '3)) '('subkey (String 'b)) '('value (String 'v)))
            ))
            (let list2 (AsList
                (AsStruct '('key (String '4)) '('subkey (Utf8 'c)) '('value (String 'v)))
                (AsStruct '('key (String '1)) '('subkey (Utf8 'd)) '('value (String 'v)))
                (AsStruct '('key (String '3)) '('subkey (Utf8 'b)) '('value (String 'v)))
            ))
            (let sorted1 (Sort list1 (Bool 'False) (lambda '(item) '((Member item 'key) (Member item 'subkey) (Member item 'value)))))
            (let sorted2 (Sort list2 (Bool 'False) (lambda '(item) '((Member item 'key) (Member item 'subkey) (Member item 'value)))))
            (let merged (UnionMerge sorted1 sorted2))
            (let world (Write! world mr_sink (Key '('table (String 'Output))) merged '('('mode 'renew))))
            (let world (Commit! world mr_sink))
            (return world)
        ))";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TSortedConstraintNode>(exprRoot, "UnionMerge", "Sorted(key[desc])");
    }

    Y_UNIT_TEST(ExtractMembersKey) {
        const auto s = R"((
            (let mr_sink (DataSink 'yt (quote plato)))
            (let list (AsList
                (AsStruct '('key (String '4)) '('subkey (String 'c)) '('value (String 'v)))
                (AsStruct '('key (String '1)) '('subkey (String 'd)) '('value (String 'w)))
                (AsStruct '('key (String '3)) '('subkey (String 'b)) '('value (String 'u)))
            ))
            (let sorted (Sort list '((Bool 'True) (Bool 'True)) (lambda '(item) '((Member item 'key) (Member item 'subkey)))))
            (let sorted (ExtractMembers sorted '('key)))
            (let world (Write! world mr_sink (Key '('table (String 'Output))) sorted '('('mode 'renew))))
            (let world (Commit! world mr_sink))
            (return world)
        ))";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TSortedConstraintNode>(exprRoot, "ExtractMembers", "Sorted(key[asc])");
    }

    Y_UNIT_TEST(ExtractMembersNonKey) {
        const auto s = R"((
            (let mr_sink (DataSink 'yt (quote plato)))
            (let list (AsList
                (AsStruct '('key (String '4)) '('subkey (String 'c)) '('value (String 'v)))
                (AsStruct '('key (String '1)) '('subkey (String 'd)) '('value (String 'w)))
                (AsStruct '('key (String '3)) '('subkey (String 'b)) '('value (String 'u)))
            ))
            (let sorted (Sort list '((Bool 'True) (Bool 'True)) (lambda '(item) '((Member item 'key) (Member item 'subkey)))))
            (let sorted (ExtractMembers sorted '('value)))
            (let world (Write! world mr_sink (Key '('table (String 'Output))) sorted '('('mode 'renew))))
            (let world (Commit! world mr_sink))
            (return world)
        ))";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TSortedConstraintNode>(exprRoot, "ExtractMembers", "");
    }

    Y_UNIT_TEST(OrderedLMap) {
        const auto s = R"((
            (let mr_sink (DataSink 'yt (quote plato)))
            (let list (AsList
                (AsStruct '('key (String '4)) '('subkey (String 'c)) '('value (String 'v)))
                (AsStruct '('key (String '1)) '('subkey (String 'd)) '('value (String 'w)))
                (AsStruct '('key (String '3)) '('subkey (String 'b)) '('value (String 'u)))
            ))
            (let sorted (Sort list '((Bool 'False) (Bool 'False)) (lambda '(item) '((Member item 'key) (Member item 'subkey)))))
            (let sorted (OrderedLMap sorted (lambda '(stream) (OrderedFlatMap stream (lambda '(item) (Just item))))))
            (let world (Write! world mr_sink (Key '('table (String 'Output))) sorted '('('mode 'renew))))
            (let world (Commit! world mr_sink))
            (return world)
        ))";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TSortedConstraintNode>(exprRoot, "OrderedLMap", "Sorted(key[desc];subkey[desc])");
        CheckConstraint<TSortedConstraintNode>(exprRoot, "OrderedFlatMap", "Sorted(key[desc];subkey[desc])");
    }

    Y_UNIT_TEST(OrderedFlatMap) {
        const auto s = R"((
            (let mr_sink (DataSink 'yt (quote plato)))
            (let list (AsList
                (AsStruct '('key (String '4)) '('subkey (String 'c)) '('value (String 'v)))
                (AsStruct '('key (String '1)) '('subkey (String 'd)) '('value (String 'w)))
                (AsStruct '('key (String '3)) '('subkey (String 'b)) '('value (String 'u)))
            ))
            (let list (AssumeUnique list '('key) '('subkey 'value)))
            (let list (AssumeDistinct list '('key 'subkey) '('value)))
            (let sorted (Sort list '((Bool 'True) (Bool 'True)) (lambda '(item) '((Member item 'key) (Member item 'subkey)))))
            (let sorted (OrderedFlatMap sorted (lambda '(item) (OptionalIf (== (Member item 'key) (String '1)) (AsStruct '('k (Member item 'key)) '('v (Member item 'value)))))))
            (let world (Write! world mr_sink (Key '('table (String 'Output))) sorted '('('mode 'renew))))
            (let world (Commit! world mr_sink))
            (return world)
        ))";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TSortedConstraintNode>(exprRoot, "OrderedFlatMap", "Sorted(k[asc])");
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "OrderedFlatMap", "Unique((k))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "OrderedFlatMap", "Distinct((v))");
    }

    Y_UNIT_TEST(OrderedFlatMapWithEmptyFilter) {
        const auto s = R"((
            (let mr_sink (DataSink 'yt (quote plato)))
            (let list (AsList
                (AsStruct '('key (String '4)) '('subkey (String 'c)) '('value (String 'v)))
                (AsStruct '('key (String '1)) '('subkey (String 'd)) '('value (String 'w)))
                (AsStruct '('key (String '3)) '('subkey (String 'b)) '('value (String 'u)))
            ))
            (let list (AssumeUnique list '('key) '('subkey 'value)))
            (let list (AssumeDistinct list '('key 'subkey) '('value)))
            (let sorted (Sort list '((Bool 'True) (Bool 'True)) (lambda '(item) '((Member item 'key) (Member item 'subkey)))))
            (let sorted (OrderedFlatMap sorted (lambda '(item) (OptionalIf (Bool 'false) (AsStruct '('k (Member item 'key)) '('v (Member item 'value)))))))
            (let world (Write! world mr_sink (Key '('table (String 'Output))) sorted '('('mode 'renew))))
            (let world (Commit! world mr_sink))
            (return world)
        ))";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TSortedConstraintNode>(exprRoot, "OrderedFlatMap", "Sorted(k[asc])");
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "OrderedFlatMap", "Unique((k))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "OrderedFlatMap", "Distinct((v))");
    }

    Y_UNIT_TEST(OrderedFlatMapNonKey) {
        const auto s = R"((
            (let mr_sink (DataSink 'yt (quote plato)))
            (let list (AsList
                (AsStruct '('key (String '4)) '('subkey (String 'c)) '('value (String 'v)))
                (AsStruct '('key (String '1)) '('subkey (String 'd)) '('value (String 'w)))
                (AsStruct '('key (String '3)) '('subkey (String 'b)) '('value (String 'u)))
            ))
            (let sorted (Sort list '((Bool 'True) (Bool 'True)) (lambda '(item) '((Member item 'key) (Member item 'subkey)))))
            (let sorted (OrderedFlatMap sorted (lambda '(item) (OptionalIf (== (Member item 'key) (String '1)) (AsStruct '('key (Member item 'value)))))))
            (let world (Write! world mr_sink (Key '('table (String 'Output))) sorted '('('mode 'renew))))
            (let world (Commit! world mr_sink))
            (return world)
        ))";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TSortedConstraintNode>(exprRoot, "OrderedFlatMap", "");
    }

    Y_UNIT_TEST(OrderedFilter) {
        const auto s = R"((
            (let mr_sink (DataSink 'yt (quote plato)))
            (let list (AsList
                (AsStruct '('key (String '4)) '('subkey (String 'c)) '('value (String 'v)))
                (AsStruct '('key (String '1)) '('subkey (String 'd)) '('value (String 'w)))
                (AsStruct '('key (String '3)) '('subkey (String 'b)) '('value (String 'u)))
            ))
            (let list (AssumeUnique list '('key) '('subkey 'value)))
            (let list (AssumeDistinct list '('key 'subkey) '('value)))
            (let sorted (Sort list '((Bool 'True) (Bool 'True)) (lambda '(item) '((Member item 'key) (Member item 'subkey)))))
            (let sorted (OrderedFilter sorted (lambda '(item) (> (Member item 'key) (String '0)))))
            (let world (Write! world mr_sink (Key '('table (String 'Output))) sorted '('('mode 'renew))))
            (let world (Commit! world mr_sink))
            (return world)
        ))";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TSortedConstraintNode>(exprRoot, "OrderedFilter", "Sorted(key[asc];subkey[asc])");
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "OrderedFilter", "Unique((key)(subkey,value))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "OrderedFilter", "Distinct((key,subkey)(value))");
    }

    Y_UNIT_TEST(OrderedMapNullifyOneColumn) {
        const auto s = R"((
            (let mr_sink (DataSink 'yt (quote plato)))
            (let list (AsList
                (AsStruct '('key (String '4)) '('subkey (String 'c)) '('value (String 'v)))
                (AsStruct '('key (String '1)) '('subkey (String 'd)) '('value (String 'w)))
                (AsStruct '('key (String '3)) '('subkey (String 'b)) '('value (String 'u)))
            ))
            (let list (AssumeUnique list '('key) '('subkey 'value)))
            (let list (AssumeDistinct list '('key 'subkey) '('value)))
            (let sorted (Sort list (Bool 'False) (lambda '(row) '((Member row 'key) (Member row 'subkey) (Member row 'value)))))
            (let map (OrderedMap sorted (lambda '(row) (AsStruct '('k (Member row 'key)) '('s (OptionalIf (AggrNotEquals (Member row 'key) (Member row 'value)) (Member row 'subkey))) '('v (Member row 'value))))))
            (let world (Write! world mr_sink (Key '('table (String 'Output))) map '('('mode 'renew))))
            (let world (Commit! world mr_sink))
            (return world)
        ))";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TSortedConstraintNode>(exprRoot, "OrderedMap", "Sorted(k[desc])");
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "OrderedMap", "Unique((k)(s,v))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "OrderedMap", "Distinct((v))");
    }

    Y_UNIT_TEST(NestedFlatMapByOptional) {
        const auto s = R"((
            (let mr_sink (DataSink 'yt (quote plato)))
            (let list (AsList
                (AsStruct '('key (String '4)) '('subkey (String 'c)) '('value (String 'v)))
                (AsStruct '('key (String '1)) '('subkey (String 'd)) '('value (String 'w)))
                (AsStruct '('key (String '3)) '('subkey (String 'b)) '('value (String 'u)))
            ))
            (let list (AssumeUnique list '('key 'subkey) '('value)))
            (let list (AssumeDistinct list '('key) '('subkey 'value)))
            (let sorted (Sort list '((Bool 'False) (Bool 'True)) (lambda '(item) '((Member item 'value) (Member item 'subkey)))))
            (let mapped (OrderedFlatMap sorted (lambda '(item) (FlatMap (StrictCast (Member item 'key) (OptionalType (DataType 'Uint8)))
                (lambda '(key) (Just (AsStruct '('k key) '('s (Member item 'subkey)) '('v (Member item 'value)))))
            ))))
            (let world (Write! world mr_sink (Key '('table (String 'Output))) mapped '('('mode 'renew))))
            (let world (Commit! world mr_sink))
            (return world)
        ))";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TSortedConstraintNode>(exprRoot, "OrderedFlatMap", "Sorted(v[desc];s[asc])");
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "OrderedFlatMap", "Unique((k,s)(v))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "OrderedFlatMap", "Distinct((s,v))");
    }

    Y_UNIT_TEST(FlattenMembers) {
        const auto s = R"((
            (let mr_sink (DataSink 'yt (quote plato)))
            (let list (AsList
                (AsStruct '('key (String '4)) '('subkey (String 'c)) '('value (String 'v)))
                (AsStruct '('key (String '1)) '('subkey (String 'd)) '('value (String 'w)))
                (AsStruct '('key (String '3)) '('subkey (String 'b)) '('value (String 'u)))
            ))
            (let list (AssumeUnique list '('key) '('subkey 'value)))
            (let list (AssumeDistinct list '('key 'subkey) '('value)))
            (let sorted (Sort list '((Bool 'True) (Bool 'True)) (lambda '(item) '((Member item 'key) (Member item 'subkey)))))
            (let mapped (OrderedMap sorted (lambda '(item) '((AsStruct '('key (Member item 'key)) '('subkey (Member item 'subkey))) (AsStruct '('subkey (Member item 'subkey)) '('value (Member item 'value)))))))
            (let flatten (OrderedMap mapped (lambda '(pair) (FlattenMembers '('one (Nth pair '0)) '('two (Nth pair '1))))))
            (let world (Write! world mr_sink (Key '('table (String 'Output))) (LazyList flatten) '('('mode 'renew))))
            (let world (Commit! world mr_sink))
            (return world)
        ))";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TSortedConstraintNode>(exprRoot, "LazyList", "Sorted(onekey[asc];onesubkey,twosubkey[asc])");
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "LazyList", "Unique((onekey)({onesubkey,twosubkey},twovalue))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "LazyList", "Distinct((onekey,{onesubkey,twosubkey})(twovalue))");
    }

    Y_UNIT_TEST(Visit) {
        const auto s = R"((
(let mr_sink (DataSink 'yt (quote plato)))

(let list (AsList
    (AsStruct '('key (String 'aaa)))
    (AsStruct '('key (String 'bbb)))
    (AsStruct '('key (String 'ccc)))
))

(let structType (StructType '('key (DataType 'String)) '('value (DataType 'String))))
(let tupleType (TupleType structType structType structType))
(let vt (VariantType tupleType))
(let vlist (Extend
    (Map list (lambda '(x) (Variant (AsStruct '('key (Member x 'key)) '('value (String '0))) '0 vt)))
    (Map list (lambda '(x) (Variant (AsStruct '('key (Member x 'key)) '('value (String '1))) '1 vt)))
    (Map list (lambda '(x) (Variant (AsStruct '('key (Member x 'key)) '('value (String '2))) '2 vt)))
))

(let vlist (FlatMap vlist (lambda '(item) (Visit item
    '0 (lambda '(v) (Just (Variant (AsStruct '('key (Member v 'key)) '('value (Member v 'value))) '1 vt)))
    (Nothing (OptionalType vt))
))))

(let res (Map vlist (lambda '(item) (VariantItem item))))

(let world (Write! world mr_sink (Key '('table (String 'Output))) res '('('mode 'renew))))
(let world (Commit! world mr_sink))
(return world)
))";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TMultiConstraintNode>(exprRoot, "Extend", "Multi(0:{},1:{},2:{})");
        CheckConstraint<TMultiConstraintNode>(exprRoot, "Visit", "Multi(1:{})");
        CheckConstraint<TVarIndexConstraintNode>(exprRoot, "Visit", "VarIndex(1:0)");
        CheckConstraint<TMultiConstraintNode>(exprRoot, "FlatMap", "Multi(1:{})");
        CheckConstraint<TMultiConstraintNode>(exprRoot, "VariantItem", "");
        CheckConstraint<TMultiConstraintNode>(exprRoot, "Map", "");
    }

    Y_UNIT_TEST(SwitchAsFilter) {
        const auto s = R"((
(let mr_sink (DataSink 'yt (quote plato)))

(let list (AsList
    (AsStruct '('key (String 'aaa)))
    (AsStruct '('key (String 'bbb)))
    (AsStruct '('key (String 'ccc)))
))

(let structType (StructType '('key (DataType 'String)) '('value (DataType 'String))))
(let tupleType (TupleType structType structType structType))
(let vt (VariantType tupleType))
(let vlist (Extend
    (Map list (lambda '(x) (Variant (AsStruct '('key (Member x 'key)) '('value (String '0))) '0 vt)))
    (Map list (lambda '(x) (Variant (AsStruct '('key (Member x 'key)) '('value (String '1))) '1 vt)))
    (Map list (lambda '(x) (Variant (AsStruct '('key (Member x 'key)) '('value (String '2))) '2 vt)))
))

(let res (Switch (Iterator vlist) '0 '('0) (lambda '(item) item)))

(let world (Write! world mr_sink (Key '('table (String 'Output))) res '('('mode 'renew))))
(let world (Commit! world mr_sink))
(return world)
))";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TMultiConstraintNode>(exprRoot, "Extend", "Multi(0:{},1:{},2:{})");
        CheckConstraint<TMultiConstraintNode>(exprRoot, "Switch", "");
    }

    Y_UNIT_TEST(EmptyForMissingMultiOut) {
        const auto s = R"((
(let mr_sink (DataSink 'yt (quote plato)))

(let list (AsList
    (AsStruct '('key (String 'aaa)))
    (AsStruct '('key (String 'bbb)))
    (AsStruct '('key (String 'ccc)))
))

(let structType (StructType '('key (DataType 'String)) '('value (DataType 'String))))
(let tupleType3 (TupleType structType structType structType))
(let vt3 (VariantType tupleType3))
(let tupleType2 (TupleType structType structType))
(let vt2 (VariantType tupleType2))
(let vlist (Extend
    (Map list (lambda '(x) (Variant (AsStruct '('key (Member x 'key)) '('value (String '0))) '0 vt3)))
    (Map list (lambda '(x) (Variant (AsStruct '('key (Member x 'key)) '('value (String '1))) '1 vt3)))
))

(let vlist (FlatMap vlist (lambda '(item) (Visit item
    '0 (lambda '(v) (Just (Variant v '0 vt2)))
    '1 (lambda '(v) (Just (Variant v '0 vt2)))
    '2 (lambda '(v) (Just (Variant v '1 vt2)))
))))

(let res (Map vlist (lambda '(item) (VariantItem item))))

(let world (Write! world mr_sink (Key '('table (String 'Output))) res '('('mode 'renew))))
(let world (Commit! world mr_sink))
(return world)
))";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TMultiConstraintNode>(exprRoot, "Extend", "Multi(0:{},1:{})");
        CheckConstraint<TMultiConstraintNode>(exprRoot, "FlatMap", "Multi(0:{})");
    }

    Y_UNIT_TEST(SwitchWithReplicate) {
        const auto s = R"((
(let mr_sink (DataSink 'yt (quote plato)))

(let list (AsList
    (AsStruct '('key (String 'aaa)))
    (AsStruct '('key (String 'bbb)))
    (AsStruct '('key (String 'ccc)))
))

(let vlist (Switch (Iterator list) '0 '('0) (lambda '(item) item) '('0) (lambda '(item) item)))

(let res (Map vlist (lambda '(item) (VariantItem item))))

(let world (Write! world mr_sink (Key '('table (String 'Output))) res '('('mode 'renew))))
(let world (Commit! world mr_sink))
(return world)
))";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TMultiConstraintNode>(exprRoot, "Switch", "Multi(0:{},1:{})");
        CheckConstraint<TMultiConstraintNode>(exprRoot, "Map", "");
    }

    Y_UNIT_TEST(SwitchWithMultiOut) {
        const auto s = R"((
(let mr_sink (DataSink 'yt (quote plato)))

(let list (AsList
    (AsStruct '('key (String 'aaa)))
    (AsStruct '('key (String 'bbb)))
    (AsStruct '('key (String 'ccc)))
))

(let structType (StructType '('key (DataType 'String)) '('value (DataType 'String))))
(let tupleType (TupleType structType structType structType))
(let vt (VariantType tupleType))
(let vlist (Extend
    (Map list (lambda '(x) (Variant (AsStruct '('key (Member x 'key)) '('value (String '0))) '0 vt)))
    (Map list (lambda '(x) (Variant (AsStruct '('key (Member x 'key)) '('value (String '1))) '1 vt)))
    (Map list (lambda '(x) (Variant (AsStruct '('key (Member x 'key)) '('value (String '2))) '2 vt)))
))

(let vlist (Switch (Iterator vlist) '0 '('0) (lambda '(s) (Map s (lambda '(item) (Variant item '1 vt)))) '('1) (lambda '(s) s)))

(let res (Map vlist (lambda '(item) (VariantItem item))))

(let world (Write! world mr_sink (Key '('table (String 'Output))) res '('('mode 'renew))))
(let world (Commit! world mr_sink))
(return world)
))";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TMultiConstraintNode>(exprRoot, "Extend", "Multi(0:{},1:{},2:{})");
        CheckConstraint<TMultiConstraintNode>(exprRoot, "Switch", "Multi(1:{},3:{})");
    }

    Y_UNIT_TEST(MultiOutLMapWithEmptyInput) {
        auto s = R"((
(let mr_sink (DataSink 'yt (quote plato)))

(let structType (StructType '('key (DataType 'String)) '('value (DataType 'String))))
(let tupleType (TupleType structType structType structType))
(let vt (VariantType tupleType))

(let vlist (LMap (List (ListType structType)) (lambda '(stream)
    (FlatMap stream (lambda '(item)
        (Extend (AsList (Variant item '0 vt)) (AsList (Variant item '1 vt)))
    ))
)))

(let vlist (Switch (Iterator vlist) '0 '('0) (lambda '(s) s) '('1) (lambda '(s) s)))

(let res (Map vlist (lambda '(item) (VariantItem item))))

(let world (Write! world mr_sink (Key '('table (String 'Output))) res '('('mode 'renew))))
(let world (Commit! world mr_sink))
(return world)
))";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TEmptyConstraintNode>(exprRoot, "LMap", "Empty");
        CheckConstraint<TMultiConstraintNode>(exprRoot, "LMap", "");
        CheckConstraint<TEmptyConstraintNode>(exprRoot, "Switch", "Empty");
        CheckConstraint<TMultiConstraintNode>(exprRoot, "Switch", "");
    }

    Y_UNIT_TEST(Unique) {
        const auto s = R"((
    (let mr_sink (DataSink 'yt (quote plato)))
    (let list (AsList
        (AsStruct '('key (String '4)) '('subkey (String 'c)) '('value (String 'x)))
        (AsStruct '('key (String '1)) '('subkey (String 'b)) '('value (String 'y)))
        (AsStruct '('key (String '4)) '('subkey (String 'b)) '('value (String 'z)))
    ))
    (let list (AssumeUnique list '('key 'subkey) '('value)))
    (let list (FlatMap list (lambda '(item) (block '(
        (let res (Map (Just item) (lambda '(m)
            (AsStruct
                '('key1 (Member m 'key))
                '('key2 (Member m 'key))
                '('subkey (Member m 'subkey))
                '('value (Member m 'value))
            )
        )))
        (let res (Map res (lambda '(m)
            (AsStruct
                '('p.key1 (Member m 'key1))
                '('p.key2 (Member m 'key2))
                '('p.subkey (Member m 'subkey))
                '('value (Member m 'value))
            )
        )))
        (return res)
    )))))
    (let world (Write! world mr_sink (Key '('table (String 'Output))) (Take list (Uint64 '2)) '('('mode 'renew))))
    (let world (Commit! world mr_sink))
    (return world)
))";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "Take", "Unique(({p.key1,p.key2},p.subkey)(value))");
    }

    Y_UNIT_TEST(UniqueOverTuple) {
        const auto s = R"((
    (let mr_sink (DataSink 'yt (quote plato)))
    (let list (AsList
        (AsStruct '('key (String '4)) '('subkey (String 'c)) '('value (String 'm)))
        (AsStruct '('key (String '1)) '('subkey (String 'b)) '('value (String 'm)))
        (AsStruct '('key (String '3)) '('subkey (String 'b)) '('value (String 'n)))
    ))
    (let list (AssumeUnique list '('key) '('subkey 'value)))
    (let list (FlatMap list (lambda '(item) (block '(
        (let res (Map (Just item) (lambda '(m)
            '(
                (Member m 'key)
                (Member m 'key)
                (Member m 'subkey)
                (Member m 'value)
            )
        )))
        (let res (Map res (lambda '(m)
            (AsStruct
                '('p.key1 (Nth m '0))
                '('p.key2 (Nth m '1))
                '('p.subkey (Nth m '2))
                '('value (Nth m '3))
            )
        )))
        (return (Just (DivePrefixMembers (Unwrap res) '('p.))))
    )))))
    (let world (Write! world mr_sink (Key '('table (String 'Output))) list '('('mode 'renew))))
    (let world (Commit! world mr_sink))
    (return world)
))";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TPartOfUniqueConstraintNode>(exprRoot, "Unwrap", "PartOfUnique(p.key1:key,p.key2:key,p.subkey:subkey,value:value)");
        CheckConstraint<TPartOfUniqueConstraintNode>(exprRoot, "DivePrefixMembers", "PartOfUnique(key1:key,key2:key,subkey:subkey)");
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "FlatMap", "Unique(({key1,key2}))");
    }

    Y_UNIT_TEST(UniqueOverWideFlow) {
        const auto s = R"((
    (let mr_sink (DataSink 'yt (quote plato)))
    (let list (AsList
        (AsStruct '('key (String '4)) '('subkey (String 'c)) '('value (String 'x)))
        (AsStruct '('key (String '1)) '('subkey (String 'b)) '('value (String 'y)))
        (AsStruct '('key (String '4)) '('subkey (String 'b)) '('value (String 'z)))
    ))
    (let list (AssumeUnique list '('key 'subkey) '('value)))
    (let list (FlatMap list (lambda '(item) (block '(
        (let res (ExpandMap (ToFlow (Just item)) (lambda '(m)
            (Member m 'key)
            (Member m 'key)
            (Member m 'subkey)
            (Member m 'value)
        )))
        (let res (WideMap res (lambda '(m0 m1 m2 m3) m0 m2 m3 m1)))
        (let res (NarrowMap res (lambda '(m0 m1 m2 m3)
            (AsStruct
                '('p.key1 m0)
                '('p.key2 m3)
                '('p.subkey m1)
                '('value m2)
            )
        )))
        (return (Map res (lambda '(row) (DivePrefixMembers row '('p.)))))
    )))))
    (let world (Write! world mr_sink (Key '('table (String 'Output))) list '('('mode 'renew))))
    (let world (Commit! world mr_sink))
    (return world)
))";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TPartOfUniqueConstraintNode>(exprRoot, "NarrowMap", "PartOfUnique(p.key1:key,p.key2:key,p.subkey:subkey,value:value)");
        CheckConstraint<TPartOfUniqueConstraintNode>(exprRoot, "Map", "PartOfUnique(key1:key,key2:key,subkey:subkey)");
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "FlatMap", "Unique(({key1,key2},subkey))");
    }

    Y_UNIT_TEST(UniqueExOverWideFlow) {
        const auto s = R"((
    (let mr_sink (DataSink 'yt (quote plato)))
    (let list (AsList
        (AsStruct '('key (String '4)) '('subkey (String 'c)) '('value (String 'v)))
        (AsStruct '('key (String '1)) '('subkey (String 'd)) '('value (String 'v)))
        (AsStruct '('key (String '3)) '('subkey (String 'b)) '('value (String 'v)))
    ))
    (let list (AssumeUnique list))
    (let list (FlatMap list (lambda '(item) (block '(
        (let res (ExpandMap (ToFlow (Just item)) (lambda '(m)
            (Member m 'key)
            (Member m 'key)
            (Member m 'subkey)
            (Member m 'value)
        )))
        (let res (WideMap res (lambda '(m0 m1 m2 m3) (AsStruct '('xxx m0) '('yyy m2)) '(m1 m3))))
        (let res (WideMap res (lambda '(s0 t1) '((Member s0 'xxx) (Nth t1 '1)) (ReplaceMember s0 'xxx (Nth t1 '0)))))
        (let res (NarrowMap res (lambda '(t0 s1) (AddMember (AddMember s1 'one (Nth t0 '1)) 'two (Nth t0 '0)))))
        (return (Collect res))
    )))))
    (let world (Write! world mr_sink (Key '('table (String 'Output))) (LazyList list) '('('mode 'renew))))
    (let world (Commit! world mr_sink))
    (return world)
))";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "LazyList", "Unique((one,{two,xxx},yyy))");
    }

    Y_UNIT_TEST(Distinct) {
        const auto s = R"((
    (let mr_sink (DataSink 'yt (quote plato)))
    (let list (AsList
        (AsStruct '('key (String '4)) '('subkey (String 'c)) '('value (String 'x)))
        (AsStruct '('key (String '1)) '('subkey (String 'b)) '('value (String 'y)))
        (AsStruct '('key (String '4)) '('subkey (String 'b)) '('value (String 'z)))
    ))
    (let list (AssumeDistinct list '('key 'subkey) '('value)))
    (let list (FlatMap list (lambda '(item) (block '(
        (let res (Map (Just item) (lambda '(m)
            (AsStruct
                '('key1 (Member m 'key))
                '('key2 (Member m 'key))
                '('subkey (Member m 'subkey))
                '('value (Member m 'value))
            )
        )))
        (let res (Map res (lambda '(m)
            (AsStruct
                '('p.key1 (Member m 'key1))
                '('p.key2 (Member m 'key2))
                '('p.subkey (Member m 'subkey))
                '('value (Member m 'value))
            )
        )))
        (return res)
    )))))
    (let world (Write! world mr_sink (Key '('table (String 'Output))) (Take list (Uint64 '2)) '('('mode 'renew))))
    (let world (Commit! world mr_sink))
    (return world)
))";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "Take", "Distinct(({p.key1,p.key2},p.subkey)(value))");
    }

    Y_UNIT_TEST(DistinctOverTuple) {
        const auto s = R"((
    (let mr_sink (DataSink 'yt (quote plato)))
    (let list (AsList
        (AsStruct '('key (String '4)) '('subkey (String 'c)) '('value (String 'm)))
        (AsStruct '('key (String '1)) '('subkey (String 'b)) '('value (String 'm)))
        (AsStruct '('key (String '3)) '('subkey (String 'b)) '('value (String 'n)))
    ))
    (let list (AssumeDistinct list '('key) '('subkey 'value)))
    (let list (FlatMap list (lambda '(item) (block '(
        (let res (Map (Just item) (lambda '(m)
            '(
                (Member m 'key)
                (Member m 'key)
                (Member m 'subkey)
                (Member m 'value)
            )
        )))
        (let res (Map res (lambda '(m)
            (AsStruct
                '('p.key1 (Nth m '0))
                '('p.key2 (Nth m '1))
                '('p.subkey (Nth m '2))
                '('value (Nth m '3))
            )
        )))
        (return (Just (DivePrefixMembers (Unwrap res) '('p.))))
    )))))
    (let world (Write! world mr_sink (Key '('table (String 'Output))) list '('('mode 'renew))))
    (let world (Commit! world mr_sink))
    (return world)
))";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TPartOfDistinctConstraintNode>(exprRoot, "Unwrap", "PartOfDistinct(p.key1:key,p.key2:key,p.subkey:subkey,value:value)");
        CheckConstraint<TPartOfDistinctConstraintNode>(exprRoot, "DivePrefixMembers", "PartOfDistinct(key1:key,key2:key,subkey:subkey)");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "FlatMap", "Distinct(({key1,key2}))");
    }

    Y_UNIT_TEST(DistinctOverWideFlow) {
        const auto s = R"((
    (let mr_sink (DataSink 'yt (quote plato)))
    (let list (AsList
        (AsStruct '('key (String '4)) '('subkey (String 'c)) '('value (String 'x)))
        (AsStruct '('key (String '1)) '('subkey (String 'b)) '('value (String 'y)))
        (AsStruct '('key (String '4)) '('subkey (String 'b)) '('value (String 'z)))
    ))
    (let list (AssumeDistinct list '('key 'subkey) '('value)))
    (let list (FlatMap list (lambda '(item) (block '(
        (let res (ExpandMap (ToFlow (Just item)) (lambda '(m)
            (Member m 'key)
            (Member m 'key)
            (Member m 'subkey)
            (Member m 'value)
        )))
        (let res (WideMap res (lambda '(m0 m1 m2 m3) m0 m2 m3 m1)))
        (let res (NarrowMap res (lambda '(m0 m1 m2 m3)
            (AsStruct
                '('p.key1 m0)
                '('p.key2 m3)
                '('p.subkey m1)
                '('value m2)
            )
        )))
        (return (Map res (lambda '(row) (DivePrefixMembers row '('p.)))))
    )))))
    (let world (Write! world mr_sink (Key '('table (String 'Output))) list '('('mode 'renew))))
    (let world (Commit! world mr_sink))
    (return world)
))";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TPartOfDistinctConstraintNode>(exprRoot, "NarrowMap", "PartOfDistinct(p.key1:key,p.key2:key,p.subkey:subkey,value:value)");
        CheckConstraint<TPartOfDistinctConstraintNode>(exprRoot, "Map", "PartOfDistinct(key1:key,key2:key,subkey:subkey)");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "FlatMap", "Distinct(({key1,key2},subkey))");
    }

    Y_UNIT_TEST(DistinctExOverWideFlow) {
        const auto s = R"((
    (let mr_sink (DataSink 'yt (quote plato)))
    (let list (AsList
        (AsStruct '('key (String '4)) '('subkey (String 'c)) '('value (String 'v)))
        (AsStruct '('key (String '1)) '('subkey (String 'd)) '('value (String 'v)))
        (AsStruct '('key (String '3)) '('subkey (String 'b)) '('value (String 'v)))
    ))
    (let list (AssumeDistinct list))
    (let list (FlatMap list (lambda '(item) (block '(
        (let res (ExpandMap (ToFlow (Just item)) (lambda '(m)
            (Member m 'key)
            (Member m 'key)
            (Member m 'subkey)
            (Member m 'value)
        )))
        (let res (WideMap res (lambda '(m0 m1 m2 m3) (AsStruct '('xxx m0) '('yyy m2)) '(m1 m3))))
        (let res (WideMap res (lambda '(s0 t1) '((Member s0 'xxx) (Nth t1 '1)) (ReplaceMember s0 'xxx (Nth t1 '0)))))
        (let res (NarrowMap res (lambda '(t0 s1) (AddMember (AddMember s1 'one (Nth t0 '1)) 'two (Nth t0 '0)))))
        (return (Collect res))
    )))))
    (let world (Write! world mr_sink (Key '('table (String 'Output))) (LazyList list) '('('mode 'renew))))
    (let world (Commit! world mr_sink))
    (return world)
))";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "LazyList", "Distinct((one,{two,xxx},yyy))");
    }

    Y_UNIT_TEST(PartitionsByKeysWithCondense1) {
        const auto s = R"(
(
    (let mr_sink (DataSink 'yt (quote plato)))
    (let list (AsList
        (AsStruct '('key (Just (String '4))) '('subkey (Just (String 'c))) '('value (Just (String 'x))))
        (AsStruct '('key (Just (String '1))) '('subkey (Just (String 'b))) '('value (Just (String 'y))))
        (AsStruct '('key (Just (String '4))) '('subkey (Just (String 'b))) '('value (Just (String 'z))))
    ))
    (let extractor (lambda '(item) '((Member item 'key) (Member item 'subkey))))
    (let aggr (PartitionsByKeys list extractor (Void) (Void)
        (lambda '(stream) (Condense1 stream (lambda '(row) row)
            (lambda '(row state) (IsKeySwitch row state extractor extractor))
            (lambda '(row state) (AsStruct '('key (Member row 'key)) '('subkey (Member row 'subkey)) '('value (Coalesce (Member row 'value) (Member state 'value)))))
        ))
    ))
    (let world (Write! world mr_sink (Key '('table (String 'Output))) (Skip aggr (Uint64 '1)) '('('mode 'renew))))
    (let world (Commit! world mr_sink))
    (return world)
)
    )";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "Skip", "Unique((key,subkey))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "Skip", "Distinct((key,subkey))");
    }

    Y_UNIT_TEST(PartitionsByKeysWithCondense1AndStablePickle) {
        const auto s = R"(
(
    (let mr_sink (DataSink 'yt (quote plato)))
    (let list (AsList
        (AsStruct '('key (Just (String '4))) '('subkey (Just (String 'c))) '('value (Just (String 'x))))
        (AsStruct '('key (Just (String '1))) '('subkey (Just (String 'b))) '('value (Just (String 'y))))
        (AsStruct '('key (Just (String '4))) '('subkey (Just (String 'b))) '('value (Just (String 'z))))
    ))
    (let extractor (lambda '(item) '((Member item 'key) (StablePickle(Member item 'subkey)))))
    (let aggr (PartitionsByKeys list extractor (Void) (Void)
        (lambda '(stream) (Condense1 stream (lambda '(row) row)
            (lambda '(row state) (IsKeySwitch row state extractor extractor))
            (lambda '(row state) (AsStruct '('key (Member row 'key)) '('subkey (Member row 'subkey)) '('value (Coalesce (Member row 'value) (Member state 'value)))))
        ))
    ))
    (let world (Write! world mr_sink (Key '('table (String 'Output))) (Skip aggr (Uint64 '1)) '('('mode 'renew))))
    (let world (Commit! world mr_sink))
    (return world)
)
    )";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "Skip", "Unique((key,subkey))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "Skip", "Distinct((key,subkey))");
    }

    Y_UNIT_TEST(PartitionsByKeysWithCondense1WithSingleItemTupleKey) {
        const auto s = R"(
(
    (let mr_sink (DataSink 'yt (quote plato)))
    (let list (AsList
        (AsStruct '('key '((Just (String '4)))) '('subkey (Just (String 'c))) '('value (Just (String 'x))))
        (AsStruct '('key '((Just (String '1)))) '('subkey (Just (String 'b))) '('value (Just (String 'y))))
        (AsStruct '('key '((Just (String '4)))) '('subkey (Just (String 'b))) '('value (Just (String 'z))))
    ))
    (let deep (lambda '(row) (Nth (Member row 'key) '0)))
    (let aggr (PartitionsByKeys list (lambda '(item) (Member item 'key)) (Void) (Void)
        (lambda '(stream) (Map (Condense1 stream deep
            (lambda '(row state) (IsKeySwitch row state deep (lambda '(item) item)))
            (lambda '(row state) state)
        )
            (lambda '(item) (AsStruct '('key '(item))))
        ))
    ))
    (let world (Write! world mr_sink (Key '('table (String 'Output))) (Skip aggr (Uint64 '1)) '('('mode 'renew))))
    (let world (Commit! world mr_sink))
    (return world)
)
    )";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "Skip", "Unique((key))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "Skip", "Distinct((key))");
    }

    Y_UNIT_TEST(PartitionsByKeysWithCondense1WithPairItemsTupleKeyAndDuplicateOut) {
        const auto s = R"(
(
    (let mr_sink (DataSink 'yt (quote plato)))
    (let list (AsList
        (AsStruct '('key '((Just (String '4)) (Just (String '%)))) '('subkey (Just (String 'c))) '('value (Just (String 'x))))
        (AsStruct '('key '((Just (String '1)) (Just (String '€)))) '('subkey (Just (String 'b))) '('value (Just (String 'y))))
        (AsStruct '('key '((Just (String '4)) (Just (String '$)))) '('subkey (Just (String 'b))) '('value (Just (String 'z))))
    ))
    (let extractor (lambda '(row) '((Nth (Member row 'key) '0) (Nth (Member row 'key) '1))))
    (let aggr (PartitionsByKeys list (lambda '(item) (Member item 'key)) (Void) (Void)
        (lambda '(stream) (Map (Condense1 stream extractor
            (lambda '(row state) (IsKeySwitch row state extractor (lambda '(item) item)))
            (lambda '(row state) state)
        )
            (lambda '(item) (AsStruct '('one '(item)) '('two '(item))))
        ))
    ))
    (let world (Write! world mr_sink (Key '('table (String 'Output))) (Skip aggr (Uint64 '1)) '('('mode 'renew))))
    (let world (Commit! world mr_sink))
    (return world)
)
    )";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "Skip", "Unique(({one,two}))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "Skip", "Distinct(({one,two}))");
    }

    Y_UNIT_TEST(ShuffleByKeysInputUnique) {
        const auto s = R"(
(
    (let mr_sink (DataSink 'yt (quote plato)))
    (let list (AsList
        (AsStruct '('key (Just (String '4))) '('subkey (Just (String 'c))) '('value (Just (String 'x))))
        (AsStruct '('key (Just (String '1))) '('subkey (Just (String 'b))) '('value (Just (String 'y))))
        (AsStruct '('key (Just (String '4))) '('subkey (Just (String 'b))) '('value (Just (String 'z))))
    ))
    (let list (AssumeUnique list '('key) '('subkey)))
    (let list (AssumeDistinct list '('key 'subkey)))
    (let aggr (ShuffleByKeys list
        (lambda '(item) '((Member item 'key) (Member item 'subkey)))
        (lambda '(stream) (Take stream (Uint64 '100)))
    ))
    (let world (Write! world mr_sink (Key '('table (String 'Output))) (Skip aggr (Uint64 '1)) '('('mode 'renew))))
    (let world (Commit! world mr_sink))
    (return world)
)
    )";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "Skip", "Unique((key)(subkey))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "Skip", "Distinct((key,subkey))");
    }

    Y_UNIT_TEST(ShuffleByKeysHandlerUnique) {
        const auto s = R"(
(
    (let mr_sink (DataSink 'yt (quote plato)))
    (let list (AsList
        (AsStruct '('key (Just (String '4))) '('subkey (Just (String 'c))) '('value (Just (String 'x))))
        (AsStruct '('key (Just (String '1))) '('subkey (Just (String 'b))) '('value (Just (String 'y))))
        (AsStruct '('key (Just (String '4))) '('subkey (Just (String 'b))) '('value (Just (String 'z))))
    ))
    (let list (AssumeUnique list '('key) '('subkey)))
    (let list (AssumeDistinct list '('key 'subkey)))
    (let aggr (ShuffleByKeys list
        (lambda '(item) '((Member item 'key) (Member item 'subkey)))
        (lambda '(stream) (AssumeDistinct (AssumeUnique stream '('key) '('subkey)) '('key 'subkey)))
    ))
    (let world (Write! world mr_sink (Key '('table (String 'Output))) (Skip aggr (Uint64 '1)) '('('mode 'renew))))
    (let world (Commit! world mr_sink))
    (return world)
)
    )";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "Skip", "Unique((key)(subkey))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "Skip", "Distinct((key,subkey))");
    }

    Y_UNIT_TEST(Reverse) {
        const auto s = R"(
(
    (let mr_sink (DataSink 'yt (quote plato)))
    (let list (AsList
        (AsStruct '('key (Just (String '4))) '('subkey (Just (String 'c))) '('value (Just (String 'x))))
        (AsStruct '('key (Just (String '1))) '('subkey (Just (String 'b))) '('value (Just (String 'y))))
        (AsStruct '('key (Just (String '4))) '('subkey (Just (String 'b))) '('value (Just (String 'z))))
    ))
    (let list (AssumeUnique list '('key) '('subkey)))
    (let list (AssumeDistinct list '('key 'subkey)))
    (let sorted (Sort list '((Bool 'False) (Bool 'True)) (lambda '(item) '((Member item 'key) (Member item 'subkey)))))
    (let reverse (Reverse sorted))
    (let world (Write! world mr_sink (Key '('table (String 'Output))) reverse '('('mode 'renew))))
    (let world (Commit! world mr_sink))
    (return world)
)
    )";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "Reverse", "Unique((key)(subkey))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "Reverse", "Distinct((key,subkey))");
        CheckConstraint<TSortedConstraintNode>(exprRoot, "Reverse", "Sorted(key[asc];subkey[desc])");
    }

    Y_UNIT_TEST(DictItems) {
        const auto s = R"(
(
    (let mr_sink (DataSink 'yt (quote plato)))
    (let list (AsList
        (AsStruct '('key (Just (String '4))) '('subkey (Just (String 'c))) '('value (Just (String 'x))))
        (AsStruct '('key (Just (String '1))) '('subkey (Just (String 'b))) '('value (Just (String 'y))))
        (AsStruct '('key (Just (String '4))) '('subkey (Just (String 'b))) '('value (Just (String 'z))))
    ))
    (let list (AssumeUnique list '('key) '('subkey)))
    (let list (AssumeDistinct list '('key 'subkey)))
    (let dict (ToDict list (lambda '(item) (Member item 'value)) (lambda '(item) (AsStruct '('k (Member item 'key)) '('s (Member item 'subkey)))) '('One 'Hashed)))
    (let items (Map (DictItems dict) (lambda '(item) (AsStruct '('v (Nth item '0)) '('k (Member (Nth item '1) 'k)) '('s (Member (Nth item '1) 's))))))
    (let world (Write! world mr_sink (Key '('table (String 'Output))) items '('('mode 'renew))))
    (let world (Commit! world mr_sink))
    (return world)
)
    )";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "Map", "Unique((k)(s)(v))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "Map", "Distinct((k,s)(v))");
    }

    Y_UNIT_TEST(DictKeys) {
        const auto s = R"(
(
    (let mr_sink (DataSink 'yt (quote plato)))
    (let list (AsList
        (AsStruct '('key (Just (String '4))) '('subkey (Just (String 'c))) '('value (Just (String 'x))))
        (AsStruct '('key (Just (String '1))) '('subkey (Just (String 'b))) '('value (Just (String 'y))))
        (AsStruct '('key (Just (String '4))) '('subkey (Just (String 'b))) '('value (Just (String 'z))))
    ))
    (let dict (ToDict list (lambda '(item) (Member item 'value)) (lambda '(item) (AsStruct '('k (Member item 'key)) '('s (Member item 'subkey)))) '('One 'Hashed)))
    (let items (Map (DictKeys dict) (lambda '(row) (AsStruct '('v row)))))
    (let world (Write! world mr_sink (Key '('table (String 'Output))) items '('('mode 'renew))))
    (let world (Commit! world mr_sink))
    (return world)
)
    )";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "Map", "Unique((v))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "Map", "Distinct((v))");
    }

    Y_UNIT_TEST(DictPayloads) {
        const auto s = R"(
(
    (let mr_sink (DataSink 'yt (quote plato)))
    (let list (AsList
        (AsStruct '('key (Just (String '4))) '('subkey (Just (String 'c))) '('value (Just (String 'x))))
        (AsStruct '('key (Just (String '1))) '('subkey (Just (String 'b))) '('value (Just (String 'y))))
        (AsStruct '('key (Just (String '4))) '('subkey (Just (String 'b))) '('value (Just (String 'z))))
    ))
    (let list (AssumeUnique list '('key) '('subkey)))
    (let list (AssumeDistinct list '('key 'subkey)))
    (let dict (ToDict list (lambda '(item) (Member item 'value)) (lambda '(item) (AsStruct '('k (Member item 'key)) '('s (Member item 'subkey)))) '('One 'Hashed)))
    (let items (DictPayloads dict))
    (let world (Write! world mr_sink (Key '('table (String 'Output))) items '('('mode 'renew))))
    (let world (Commit! world mr_sink))
    (return world)
)
    )";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "DictPayloads", "Unique((k)(s))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "DictPayloads", "Distinct((k,s))");
    }

    Y_UNIT_TEST(GraceJoinInner) {
        const auto s = R"(
(
    (let list1 (AsList
    (AsStruct '('key1 (Int32 '1)) '('subkey1 (Uint8 '0)) '('value1 (String 'A)))
    (AsStruct '('key1 (Int32 '7)) '('subkey1 (Uint8 '0)) '('value1 (String 'B)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '0)) '('value1 (String 'C)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '1)) '('value1 (String 'D)))
    ))

    (let list1 (AssumeUnique list1 '('key1 'subkey1) '('value1)))
    (let list1 (AssumeDistinct list1 '('key1 'subkey1) '('value1)))

    (let list2 (AsList
    (AsStruct '('key2 (Int32 '9)) '('subkey2 (Uint8 '0)) '('value2 (String 'Z)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '0)) '('value2 (String 'Y)))
    (AsStruct '('key2 (Int32 '3)) '('subkey2 (Uint8 '1)) '('value2 (String 'X)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '1)) '('value2 (String 'W)))
    (AsStruct '('key2 (Int32 '8)) '('subkey2 (Uint8 '1)) '('value2 (String 'V)))
    ))

    (let list2 (AssumeUnique list2 '('key2 'subkey2) '('value2)))
    (let list2 (AssumeDistinct list2 '('key2 'subkey2) '('value2)))

    (let flow1 (ExpandMap (ToFlow list1) (lambda '(item) (Member item 'key1) (Member item 'subkey1) (Member item 'value1))))
    (let flow2 (ExpandMap (ToFlow list2) (lambda '(item) (Member item 'key2) (Member item 'subkey2) (Member item 'value2))))

    (let join (GraceJoinCore flow1 flow2 'Inner '('0 '1) '('0 '1) '('0 '0 '2 '1) '('0 '2 '1 '3 '2 '4) '() '() '()))
    (let list (Collect (NarrowMap join (lambda '(lk lv rk rs rv) (AsStruct '('lk lk) '('lv lv) '('rk rk) '('rs rs) '('rv rv))))))

    (let res_sink (DataSink 'yt (quote plato)))
    (let world (Write! world res_sink (Key '('table (String 'Output))) list '('('mode 'renew))))

    (let world (Commit! world res_sink))
    (return world)
)
        )";
        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "Collect", "Unique((lv)(rk,rs)(rv))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "Collect", "Distinct((lv)(rk,rs)(rv))");
    }

    Y_UNIT_TEST(GraceJoinLeft) {
        const auto s = R"(
(
    (let list1 (AsList
    (AsStruct '('key1 (Int32 '1)) '('subkey1 (Uint8 '0)) '('value1 (String 'A)))
    (AsStruct '('key1 (Int32 '7)) '('subkey1 (Uint8 '0)) '('value1 (String 'B)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '0)) '('value1 (String 'C)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '1)) '('value1 (String 'D)))
    ))

    (let list1 (AssumeUnique list1 '('key1 'subkey1) '('value1)))
    (let list1 (AssumeDistinct list1 '('key1 'subkey1) '('value1)))

    (let list2 (AsList
    (AsStruct '('key2 (Int32 '9)) '('subkey2 (Uint8 '0)) '('value2 (String 'Z)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '0)) '('value2 (String 'Y)))
    (AsStruct '('key2 (Int32 '3)) '('subkey2 (Uint8 '1)) '('value2 (String 'X)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '1)) '('value2 (String 'W)))
    (AsStruct '('key2 (Int32 '8)) '('subkey2 (Uint8 '1)) '('value2 (String 'V)))
    ))

    (let list2 (AssumeUnique list2 '('key2 'subkey2) '('value2)))
    (let list2 (AssumeDistinct list2 '('key2 'subkey2) '('value2)))

    (let flow1 (ExpandMap (ToFlow list1) (lambda '(item) (Member item 'key1) (Member item 'subkey1) (Member item 'value1))))
    (let flow2 (ExpandMap (ToFlow list2) (lambda '(item) (Member item 'key2) (Member item 'subkey2) (Member item 'value2))))

    (let join (GraceJoinCore flow1 flow2 'Left '('0 '1) '('0 '1) '('0 '0 '2 '1) '('0 '2 '1 '3 '2 '4) '() '() '()))
    (let list (Collect (NarrowMap join (lambda '(lk lv rk rs rv) (AsStruct '('lk lk) '('lv lv) '('rk rk) '('rs rs) '('rv rv))))))

    (let res_sink (DataSink 'yt (quote plato)))
    (let world (Write! world res_sink (Key '('table (String 'Output))) list '('('mode 'renew))))

    (let world (Commit! world res_sink))
    (return world)
)
        )";
        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "Collect", "Unique((lv)(rk,rs)(rv))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "Collect", "Distinct((lv))");
    }

    Y_UNIT_TEST(GraceJoinFull) {
        const auto s = R"(
(
    (let list1 (AsList
    (AsStruct '('key1 (Int32 '1)) '('subkey1 (Uint8 '0)) '('value1 (String 'A)))
    (AsStruct '('key1 (Int32 '7)) '('subkey1 (Uint8 '0)) '('value1 (String 'B)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '0)) '('value1 (String 'C)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '1)) '('value1 (String 'D)))
    ))

    (let list1 (AssumeUnique list1 '('key1 'subkey1) '('value1)))
    (let list1 (AssumeDistinct list1 '('key1 'subkey1) '('value1)))

    (let list2 (AsList
    (AsStruct '('key2 (Int32 '9)) '('subkey2 (Uint8 '0)) '('value2 (String 'Z)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '0)) '('value2 (String 'Y)))
    (AsStruct '('key2 (Int32 '3)) '('subkey2 (Uint8 '1)) '('value2 (String 'X)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '1)) '('value2 (String 'W)))
    (AsStruct '('key2 (Int32 '8)) '('subkey2 (Uint8 '1)) '('value2 (String 'V)))
    ))

    (let list2 (AssumeUnique list2 '('key2 'subkey2) '('value2)))
    (let list2 (AssumeDistinct list2 '('key2 'subkey2) '('value2)))

    (let flow1 (ExpandMap (ToFlow list1) (lambda '(item) (Member item 'key1) (Member item 'subkey1) (Member item 'value1))))
    (let flow2 (ExpandMap (ToFlow list2) (lambda '(item) (Member item 'key2) (Member item 'subkey2) (Member item 'value2))))

    (let join (GraceJoinCore flow1 flow2 'Full '('0 '1) '('0 '1) '('0 '0 '2 '1) '('0 '2 '1 '3 '2 '4) '() '() '()))
    (let list (Collect (NarrowMap join (lambda '(lk lv rk rs rv) (AsStruct '('lk lk) '('lv lv) '('rk rk) '('rs rs) '('rv rv))))))

    (let res_sink (DataSink 'yt (quote plato)))
    (let world (Write! world res_sink (Key '('table (String 'Output))) list '('('mode 'renew))))

    (let world (Commit! world res_sink))
    (return world)
)
        )";
        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "Collect", "Unique((lv)(rk,rs)(rv))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "Collect", "");
    }

    Y_UNIT_TEST(GraceJoinRight) {
        const auto s = R"(
(
    (let list1 (AsList
    (AsStruct '('key1 (Int32 '1)) '('subkey1 (Uint8 '0)) '('value1 (String 'A)))
    (AsStruct '('key1 (Int32 '7)) '('subkey1 (Uint8 '0)) '('value1 (String 'B)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '0)) '('value1 (String 'C)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '1)) '('value1 (String 'D)))
    ))

    (let list1 (AssumeUnique list1 '('key1 'subkey1) '('value1)))
    (let list1 (AssumeDistinct list1 '('key1 'subkey1) '('value1)))

    (let list2 (AsList
    (AsStruct '('key2 (Int32 '9)) '('subkey2 (Uint8 '0)) '('value2 (String 'Z)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '0)) '('value2 (String 'Y)))
    (AsStruct '('key2 (Int32 '3)) '('subkey2 (Uint8 '1)) '('value2 (String 'X)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '1)) '('value2 (String 'W)))
    (AsStruct '('key2 (Int32 '8)) '('subkey2 (Uint8 '1)) '('value2 (String 'V)))
    ))

    (let list2 (AssumeUnique list2 '('key2 'subkey2) '('value2)))
    (let list2 (AssumeDistinct list2 '('key2 'subkey2) '('value2)))

    (let flow1 (ExpandMap (ToFlow list1) (lambda '(item) (Member item 'key1) (Member item 'subkey1) (Member item 'value1))))
    (let flow2 (ExpandMap (ToFlow list2) (lambda '(item) (Member item 'key2) (Member item 'subkey2) (Member item 'value2))))

    (let join (GraceJoinCore flow1 flow2 'Right '('0 '1) '('0 '1) '('0 '0 '2 '1) '('0 '2 '1 '3 '2 '4) '() '() '()))
    (let list (Collect (NarrowMap join (lambda '(lk lv rk rs rv) (AsStruct '('lk lk) '('lv lv) '('rk rk) '('rs rs) '('rv rv))))))

    (let res_sink (DataSink 'yt (quote plato)))
    (let world (Write! world res_sink (Key '('table (String 'Output))) list '('('mode 'renew))))

    (let world (Commit! world res_sink))
    (return world)
)
        )";
        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "Collect", "Unique((lv)(rk,rs)(rv))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "Collect", "Distinct((rk,rs)(rv))");
    }

    Y_UNIT_TEST(GraceJoinExclusion) {
        const auto s = R"(
(
    (let list1 (AsList
    (AsStruct '('key1 (Int32 '1)) '('subkey1 (Uint8 '0)) '('value1 (String 'A)))
    (AsStruct '('key1 (Int32 '7)) '('subkey1 (Uint8 '0)) '('value1 (String 'B)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '0)) '('value1 (String 'C)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '1)) '('value1 (String 'D)))
    ))

    (let list1 (AssumeUnique list1 '('key1 'subkey1) '('value1)))
    (let list1 (AssumeDistinct list1 '('key1 'subkey1) '('value1)))

    (let list2 (AsList
    (AsStruct '('key2 (Int32 '9)) '('subkey2 (Uint8 '0)) '('value2 (String 'Z)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '0)) '('value2 (String 'Y)))
    (AsStruct '('key2 (Int32 '3)) '('subkey2 (Uint8 '1)) '('value2 (String 'X)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '1)) '('value2 (String 'W)))
    (AsStruct '('key2 (Int32 '8)) '('subkey2 (Uint8 '1)) '('value2 (String 'V)))
    ))

    (let list2 (AssumeUnique list2 '('key2 'subkey2) '('value2)))
    (let list2 (AssumeDistinct list2 '('key2 'subkey2) '('value2)))

    (let flow1 (ExpandMap (ToFlow list1) (lambda '(item) (Member item 'key1) (Member item 'subkey1) (Member item 'value1))))
    (let flow2 (ExpandMap (ToFlow list2) (lambda '(item) (Member item 'key2) (Member item 'subkey2) (Member item 'value2))))

    (let join (GraceJoinCore flow1 flow2 'Exclusion '('0 '1) '('0 '1) '('0 '0 '2 '1) '('0 '2 '1 '3 '2 '4) '() '() '()))
    (let list (Collect (NarrowMap join (lambda '(lk lv rk rs rv) (AsStruct '('lk lk) '('lv lv) '('rk rk) '('rs rs) '('rv rv))))))

    (let res_sink (DataSink 'yt (quote plato)))
    (let world (Write! world res_sink (Key '('table (String 'Output))) list '('('mode 'renew))))

    (let world (Commit! world res_sink))
    (return world)
)
        )";
        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "Collect", "Unique((lv)(rk,rs)(rv))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "Collect", "");
    }

    Y_UNIT_TEST(GraceJoinLeftSemi) {
        const auto s = R"(
(
    (let list1 (AsList
    (AsStruct '('key1 (Int32 '1)) '('subkey1 (Uint8 '0)) '('value1 (String 'A)))
    (AsStruct '('key1 (Int32 '7)) '('subkey1 (Uint8 '0)) '('value1 (String 'B)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '0)) '('value1 (String 'C)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '1)) '('value1 (String 'D)))
    ))

    (let list1 (AssumeUnique list1 '('key1 'subkey1) '('value1)))
    (let list1 (AssumeDistinct list1 '('key1 'subkey1) '('value1)))

    (let list2 (AsList
    (AsStruct '('key2 (Int32 '9)) '('subkey2 (Uint8 '0)) '('value2 (String 'Z)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '0)) '('value2 (String 'Y)))
    (AsStruct '('key2 (Int32 '3)) '('subkey2 (Uint8 '1)) '('value2 (String 'X)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '1)) '('value2 (String 'W)))
    (AsStruct '('key2 (Int32 '8)) '('subkey2 (Uint8 '1)) '('value2 (String 'V)))
    ))

    (let list2 (AssumeUnique list2 '('key2 'subkey2) '('value2)))
    (let list2 (AssumeDistinct list2 '('key2 'subkey2) '('value2)))

    (let flow1 (ExpandMap (ToFlow list1) (lambda '(item) (Member item 'key1) (Member item 'subkey1) (Member item 'value1))))
    (let flow2 (ExpandMap (ToFlow list2) (lambda '(item) (Member item 'key2) (Member item 'subkey2) (Member item 'value2))))

    (let join (GraceJoinCore flow1 flow2 'LeftSemi '('0 '1) '('0 '1) '('0 '2 '1 '1 '2 '0) '() '() '() '()))
    (let list (Collect (NarrowMap join (lambda '(lv ls lk) (AsStruct '('lk lk) '('lv lv) '('ls ls))))))

    (let res_sink (DataSink 'yt (quote plato)))
    (let world (Write! world res_sink (Key '('table (String 'Output))) list '('('mode 'renew))))

    (let world (Commit! world res_sink))
    (return world)
)
        )";
        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "Collect", "Unique((lk,ls)(lv))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "Collect", "Distinct((lk,ls)(lv))");
    }

    Y_UNIT_TEST(GraceJoinLeftOnly) {
        const auto s = R"(
(
    (let list1 (AsList
    (AsStruct '('key1 (Int32 '1)) '('subkey1 (Uint8 '0)) '('value1 (String 'A)))
    (AsStruct '('key1 (Int32 '7)) '('subkey1 (Uint8 '0)) '('value1 (String 'B)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '0)) '('value1 (String 'C)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '1)) '('value1 (String 'D)))
    ))

    (let list1 (AssumeUnique list1 '('key1 'subkey1) '('value1)))
    (let list1 (AssumeDistinct list1 '('key1 'subkey1) '('value1)))

    (let list2 (AsList
    (AsStruct '('key2 (Int32 '9)) '('subkey2 (Uint8 '0)) '('value2 (String 'Z)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '0)) '('value2 (String 'Y)))
    (AsStruct '('key2 (Int32 '3)) '('subkey2 (Uint8 '1)) '('value2 (String 'X)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '1)) '('value2 (String 'W)))
    (AsStruct '('key2 (Int32 '8)) '('subkey2 (Uint8 '1)) '('value2 (String 'V)))
    ))

    (let list2 (AssumeUnique list2 '('key2 'subkey2) '('value2)))
    (let list2 (AssumeDistinct list2 '('key2 'subkey2) '('value2)))

    (let flow1 (ExpandMap (ToFlow list1) (lambda '(item) (Member item 'key1) (Member item 'subkey1) (Member item 'value1))))
    (let flow2 (ExpandMap (ToFlow list2) (lambda '(item) (Member item 'key2) (Member item 'subkey2) (Member item 'value2))))

    (let join (GraceJoinCore flow1 flow2 'LeftOnly '('0 '1) '('0 '1) '('0 '1 '2 '0) '() '() '() '()))
    (let list (Collect (NarrowMap join (lambda '(lv lk) (AsStruct '('lk lk) '('lv lv))))))

    (let res_sink (DataSink 'yt (quote plato)))
    (let world (Write! world res_sink (Key '('table (String 'Output))) list '('('mode 'renew))))

    (let world (Commit! world res_sink))
    (return world)
)
        )";
        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "Collect", "Unique((lv))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "Collect", "Distinct((lv))");
    }


    Y_UNIT_TEST(GraceJoinRightOnly) {
        const auto s = R"(
(
    (let list1 (AsList
    (AsStruct '('key1 (Int32 '1)) '('subkey1 (Uint8 '0)) '('value1 (String 'A)))
    (AsStruct '('key1 (Int32 '7)) '('subkey1 (Uint8 '0)) '('value1 (String 'B)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '0)) '('value1 (String 'C)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '1)) '('value1 (String 'D)))
    ))

    (let list1 (AssumeUnique list1 '('key1 'subkey1) '('value1)))
    (let list1 (AssumeDistinct list1 '('key1 'subkey1) '('value1)))

    (let list2 (AsList
    (AsStruct '('key2 (Int32 '9)) '('subkey2 (Uint8 '0)) '('value2 (String 'Z)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '0)) '('value2 (String 'Y)))
    (AsStruct '('key2 (Int32 '3)) '('subkey2 (Uint8 '1)) '('value2 (String 'X)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '1)) '('value2 (String 'W)))
    (AsStruct '('key2 (Int32 '8)) '('subkey2 (Uint8 '1)) '('value2 (String 'V)))
    ))

    (let list2 (AssumeUnique list2 '('key2 'subkey2) '('value2)))
    (let list2 (AssumeDistinct list2 '('key2 'subkey2) '('value2)))

    (let flow1 (ExpandMap (ToFlow list1) (lambda '(item) (Member item 'key1) (Member item 'subkey1) (Member item 'value1))))
    (let flow2 (ExpandMap (ToFlow list2) (lambda '(item) (Member item 'key2) (Member item 'subkey2) (Member item 'value2))))

    (let join (GraceJoinCore flow1 flow2 'RightOnly '('0 '1) '('0 '1) '() '('0 '2 '1 '1 '2 '0) '() '() '()))
    (let list (Collect (NarrowMap join (lambda '(rv rs rk) (AsStruct '('rk rk) '('rv rv) '('rs rs))))))

    (let res_sink (DataSink 'yt (quote plato)))
    (let world (Write! world res_sink (Key '('table (String 'Output))) list '('('mode 'renew))))

    (let world (Commit! world res_sink))
    (return world)
)
        )";
        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "Collect", "Unique((rk,rs)(rv))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "Collect", "Distinct((rk,rs)(rv))");
    }

    Y_UNIT_TEST(GraceJoinRightSemi) {
        const auto s = R"(
(
    (let list1 (AsList
    (AsStruct '('key1 (Int32 '1)) '('subkey1 (Uint8 '0)) '('value1 (String 'A)))
    (AsStruct '('key1 (Int32 '7)) '('subkey1 (Uint8 '0)) '('value1 (String 'B)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '0)) '('value1 (String 'C)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '1)) '('value1 (String 'D)))
    ))

    (let list1 (AssumeUnique list1 '('key1 'subkey1) '('value1)))
    (let list1 (AssumeDistinct list1 '('key1 'subkey1) '('value1)))

    (let list2 (AsList
    (AsStruct '('key2 (Int32 '9)) '('subkey2 (Uint8 '0)) '('value2 (String 'Z)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '0)) '('value2 (String 'Y)))
    (AsStruct '('key2 (Int32 '3)) '('subkey2 (Uint8 '1)) '('value2 (String 'X)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '1)) '('value2 (String 'W)))
    (AsStruct '('key2 (Int32 '8)) '('subkey2 (Uint8 '1)) '('value2 (String 'V)))
    ))

    (let list2 (AssumeUnique list2 '('key2 'subkey2) '('value2)))
    (let list2 (AssumeDistinct list2 '('key2 'subkey2) '('value2)))

    (let flow1 (ExpandMap (ToFlow list1) (lambda '(item) (Member item 'key1) (Member item 'subkey1) (Member item 'value1))))
    (let flow2 (ExpandMap (ToFlow list2) (lambda '(item) (Member item 'key2) (Member item 'subkey2) (Member item 'value2))))

    (let join (GraceJoinCore flow1 flow2 'RightSemi '('0 '1) '('0 '1) '() '('0 '1 '2 '0) '() '() '()))
    (let list (Collect (NarrowMap join (lambda '(rv rk) (AsStruct '('rk rk) '('rv rv))))))

    (let res_sink (DataSink 'yt (quote plato)))
    (let world (Write! world res_sink (Key '('table (String 'Output))) list '('('mode 'renew))))

    (let world (Commit! world res_sink))
    (return world)
)
        )";
        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "Collect", "Unique((rv))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "Collect", "Distinct((rv))");
    }

    Y_UNIT_TEST(GraceJoinInnerBothAny) {
        const auto s = R"(
(
    (let list1 (AsList
    (AsStruct '('key1 (Int32 '1)) '('subkey1 (Uint8 '1)) '('value1 (String 'A)))
    (AsStruct '('key1 (Int32 '7)) '('subkey1 (Uint8 '2)) '('value1 (String 'B)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '3)) '('value1 (String 'C)))
    (AsStruct '('key1 (Int32 '8)) '('subkey1 (Uint8 '4)) '('value1 (String 'D)))
    ))

    (let list1 (AssumeUnique list1 '('key1) '('value1)))
    (let list1 (AssumeDistinct list1 '('subkey1) '('value1)))

    (let list2 (AsList
    (AsStruct '('key2 (Int32 '0)) '('subkey2 (Uint8 '2)) '('value2 (String 'Z)))
    (AsStruct '('key2 (Int32 '0)) '('subkey2 (Uint8 '2)) '('value2 (String 'Y)))
    (AsStruct '('key2 (Int32 '0)) '('subkey2 (Uint8 '3)) '('value2 (String 'X)))
    (AsStruct '('key2 (Int32 '3)) '('subkey2 (Uint8 '3)) '('value2 (String 'W)))
    (AsStruct '('key2 (Int32 '3)) '('subkey2 (Uint8 '3)) '('value2 (String 'V)))
    ))

    (let list2 (AssumeUnique list2 '('key2 'value2)))
    (let list2 (AssumeDistinct list2 '('subkey2 'value2)))

    (let flow1 (ExpandMap (ToFlow list1) (lambda '(item) (Member item 'key1) (Member item 'subkey1) (Member item 'value1))))
    (let flow2 (ExpandMap (ToFlow list2) (lambda '(item) (Member item 'key2) (Member item 'subkey2) (Member item 'value2))))

    (let join (GraceJoinCore flow1 flow2 'Inner '('0 '1) '('0 '1) '('0 '0 '1 '1 '2 '2) '('0 '3 '1 '4 '2 '5) '() '() '('LeftAny 'RightAny)))
    (let list (Collect (NarrowMap join (lambda '(lk ls lv rk rs rv) (AsStruct '('lk lk) '('ls ls) '('lv lv) '('rk rk) '('rs rs) '('rv rv))))))

    (let res_sink (DataSink 'yt (quote plato)))
    (let world (Write! world res_sink (Key '('table (String 'Output))) list '('('mode 'renew))))

    (let world (Commit! world res_sink))
    (return world)
)
        )";

        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "Collect", "Unique((lk)(lv)(rk,rv))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "Collect", "Distinct((ls)(lv)(rs,rv))");
    }

    Y_UNIT_TEST(MapJoinInnerOne) {
        const auto s = R"(
(
    (let list1 (AsList
    (AsStruct '('key1 (Int32 '1)) '('subkey1 (Uint8 '0)) '('value1 (String 'A)))
    (AsStruct '('key1 (Int32 '7)) '('subkey1 (Uint8 '0)) '('value1 (String 'B)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '0)) '('value1 (String 'C)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '1)) '('value1 (String 'D)))
    ))

    (let list1 (AssumeUnique list1 '('key1 'subkey1) '('value1)))
    (let list1 (AssumeDistinct list1 '('key1 'subkey1) '('value1)))

    (let list2 (AsList
    (AsStruct '('key2 (Int32 '9)) '('subkey2 (Uint8 '0)) '('value2 (String 'Z)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '0)) '('value2 (String 'Y)))
    (AsStruct '('key2 (Int32 '3)) '('subkey2 (Uint8 '1)) '('value2 (String 'X)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '1)) '('value2 (String 'W)))
    (AsStruct '('key2 (Int32 '8)) '('subkey2 (Uint8 '1)) '('value2 (String 'V)))
    ))

    (let list2 (AssumeUnique list2 '('key2 'subkey2) '('value2)))
    (let list2 (AssumeDistinct list2 '('key2 'subkey2) '('value2)))

    (let dict (ToDict list2 (lambda '(item) '((Member item 'key2) (Member item 'subkey2))) (lambda '(item) '((Member item 'subkey2) (Member item 'value2))) '('One 'Hashed)))

    (let join (MapJoinCore (ToFlow list1) dict 'Inner '('key1 'subkey1) '('key2 'subkey2) '('key1 'key 'subkey1 'subkey 'value1 'value) '('0 's '1 'v) '() '()))
    (let list (Collect join))

    (let res_sink (DataSink 'yt (quote plato)))
    (let world (Write! world res_sink (Key '('table (String 'Output))) list '('('mode 'renew))))

    (let world (Commit! world res_sink))
    (return world)
)
        )";
        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "Collect", "Unique((key,subkey)(v)(value))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "Collect", "Distinct((key,subkey)(v)(value))");
    }

    Y_UNIT_TEST(MapJoinInnerMany) {
        const auto s = R"(
(
    (let list1 (AsList
    (AsStruct '('key1 (Int32 '1)) '('subkey1 (Uint8 '0)) '('value1 (String 'A)))
    (AsStruct '('key1 (Int32 '7)) '('subkey1 (Uint8 '0)) '('value1 (String 'B)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '0)) '('value1 (String 'C)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '1)) '('value1 (String 'D)))
    ))

    (let list1 (AssumeUnique list1 '('key1 'subkey1) '('value1)))
    (let list1 (AssumeDistinct list1 '('key1 'subkey1) '('value1)))

    (let list2 (AsList
    (AsStruct '('key2 (Int32 '9)) '('subkey2 (Uint8 '0)) '('value2 (String 'Z)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '0)) '('value2 (String 'Y)))
    (AsStruct '('key2 (Int32 '3)) '('subkey2 (Uint8 '1)) '('value2 (String 'X)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '1)) '('value2 (String 'W)))
    (AsStruct '('key2 (Int32 '8)) '('subkey2 (Uint8 '1)) '('value2 (String 'V)))
    ))

    (let list2 (AssumeUnique list2 '('key2 'subkey2) '('value2)))
    (let list2 (AssumeDistinct list2 '('key2 'subkey2) '('value2)))

    (let dict (ToDict list2 (lambda '(item) '((Member item 'key2) (Member item 'subkey2))) (lambda '(item) '((Member item 'subkey2) (Member item 'value2))) '('Many 'Hashed)))

    (let join (MapJoinCore (ToFlow list1) dict 'Inner '('key1 'subkey1) '('key2 'subkey2) '('key1 'key 'subkey1 'subkey 'value1 'value) '('0 's '1 'v) '() '()))
    (let list (Collect join))

    (let res_sink (DataSink 'yt (quote plato)))
    (let world (Write! world res_sink (Key '('table (String 'Output))) list '('('mode 'renew))))

    (let world (Commit! world res_sink))
    (return world)
)
        )";
        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "Collect", "");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "Collect", "");
    }

    Y_UNIT_TEST(MapJoinLeftOne) {
        const auto s = R"(
(
    (let list1 (AsList
    (AsStruct '('key1 (Int32 '1)) '('subkey1 (Uint8 '0)) '('value1 (String 'A)))
    (AsStruct '('key1 (Int32 '7)) '('subkey1 (Uint8 '0)) '('value1 (String 'B)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '0)) '('value1 (String 'C)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '1)) '('value1 (String 'D)))
    ))

    (let list1 (AssumeUnique list1 '('key1 'subkey1) '('value1)))
    (let list1 (AssumeDistinct list1 '('key1 'subkey1) '('value1)))

    (let list2 (AsList
    (AsStruct '('key2 (Int32 '9)) '('subkey2 (Uint8 '0)) '('value2 (String 'Z)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '0)) '('value2 (String 'Y)))
    (AsStruct '('key2 (Int32 '3)) '('subkey2 (Uint8 '1)) '('value2 (String 'X)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '1)) '('value2 (String 'W)))
    (AsStruct '('key2 (Int32 '8)) '('subkey2 (Uint8 '1)) '('value2 (String 'V)))
    ))

    (let list2 (AssumeUnique list2 '('key2 'subkey2) '('value2)))
    (let list2 (AssumeDistinct list2 '('key2 'subkey2) '('value2)))

    (let dict (ToDict list2 (lambda '(item) '((Member item 'key2) (Member item 'subkey2))) (lambda '(item) '((Member item 'subkey2) (Member item 'value2))) '('One 'Hashed)))

    (let join (MapJoinCore (ToFlow list1) dict 'Left '('key1 'subkey1) '('key2 'subkey2) '('key1 'key 'subkey1 'subkey 'value1 'value) '('0 's '1 'v) '() '()))
    (let list (Collect join))

    (let res_sink (DataSink 'yt (quote plato)))
    (let world (Write! world res_sink (Key '('table (String 'Output))) list '('('mode 'renew))))

    (let world (Commit! world res_sink))
    (return world)
)
        )";
        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "Collect", "Unique((key,subkey)(v)(value))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "Collect", "Distinct((key,subkey)(value))");
    }

    Y_UNIT_TEST(MapJoinLeftMany) {
        const auto s = R"(
(
    (let list1 (AsList
    (AsStruct '('key1 (Int32 '1)) '('subkey1 (Uint8 '0)) '('value1 (String 'A)))
    (AsStruct '('key1 (Int32 '7)) '('subkey1 (Uint8 '0)) '('value1 (String 'B)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '0)) '('value1 (String 'C)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '1)) '('value1 (String 'D)))
    ))

    (let list1 (AssumeUnique list1 '('key1 'subkey1) '('value1)))
    (let list1 (AssumeDistinct list1 '('key1 'subkey1) '('value1)))

    (let list2 (AsList
    (AsStruct '('key2 (Int32 '9)) '('subkey2 (Uint8 '0)) '('value2 (String 'Z)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '0)) '('value2 (String 'Y)))
    (AsStruct '('key2 (Int32 '3)) '('subkey2 (Uint8 '1)) '('value2 (String 'X)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '1)) '('value2 (String 'W)))
    (AsStruct '('key2 (Int32 '8)) '('subkey2 (Uint8 '1)) '('value2 (String 'V)))
    ))

    (let list2 (AssumeUnique list2 '('key2 'subkey2) '('value2)))
    (let list2 (AssumeDistinct list2 '('key2 'subkey2) '('value2)))

    (let dict (ToDict list2 (lambda '(item) '((Member item 'key2) (Member item 'subkey2))) (lambda '(item) '((Member item 'subkey2) (Member item 'value2))) '('Many 'Hashed)))

    (let join (MapJoinCore (ToFlow list1) dict 'Left '('key1 'subkey1) '('key2 'subkey2) '('key1 'key 'subkey1 'subkey 'value1 'value) '('0 's '1 'v) '() '()))
    (let list (Collect join))

    (let res_sink (DataSink 'yt (quote plato)))
    (let world (Write! world res_sink (Key '('table (String 'Output))) list '('('mode 'renew))))

    (let world (Commit! world res_sink))
    (return world)
)
        )";
        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "Collect", "");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "Collect", "");
    }

    Y_UNIT_TEST(MapJoinLeftSemi) {
        const auto s = R"(
(
    (let list1 (AsList
    (AsStruct '('key1 (Int32 '1)) '('subkey1 (Uint8 '0)) '('value1 (String 'A)))
    (AsStruct '('key1 (Int32 '7)) '('subkey1 (Uint8 '0)) '('value1 (String 'B)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '0)) '('value1 (String 'C)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '1)) '('value1 (String 'D)))
    ))

    (let list1 (AssumeUnique list1 '('key1 'subkey1) '('value1)))
    (let list1 (AssumeDistinct list1 '('key1 'subkey1) '('value1)))

    (let list2 (AsList
    (AsStruct '('key2 (Int32 '9)) '('subkey2 (Uint8 '0)) '('value2 (String 'Z)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '0)) '('value2 (String 'Y)))
    (AsStruct '('key2 (Int32 '3)) '('subkey2 (Uint8 '1)) '('value2 (String 'X)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '1)) '('value2 (String 'W)))
    (AsStruct '('key2 (Int32 '8)) '('subkey2 (Uint8 '1)) '('value2 (String 'V)))
    ))

    (let list2 (AssumeUnique list2 '('key2 'subkey2) '('value2)))
    (let list2 (AssumeDistinct list2 '('key2 'subkey2) '('value2)))

    (let dict (ToDict list2 (lambda '(item) '((Member item 'key2) (Member item 'subkey2))) (lambda '(item) '()) '('One 'Hashed)))

    (let join (MapJoinCore (ToFlow list1) dict 'LeftSemi '('key1 'subkey1) '('key2 'subkey2) '('key1 'key 'subkey1 'subkey 'value1 'value) '() '() '()))
    (let list (Collect join))

    (let res_sink (DataSink 'yt (quote plato)))
    (let world (Write! world res_sink (Key '('table (String 'Output))) list '('('mode 'renew))))

    (let world (Commit! world res_sink))
    (return world)
)
        )";
        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "Collect", "Unique((key,subkey)(value))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "Collect", "Distinct((key,subkey)(value))");
    }

    Y_UNIT_TEST(MapJoinLeftOnly) {
        const auto s = R"(
(
    (let list1 (AsList
    (AsStruct '('key1 (Int32 '1)) '('subkey1 (Uint8 '0)) '('value1 (String 'A)))
    (AsStruct '('key1 (Int32 '7)) '('subkey1 (Uint8 '0)) '('value1 (String 'B)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '0)) '('value1 (String 'C)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '1)) '('value1 (String 'D)))
    ))

    (let list1 (AssumeUnique list1 '('key1 'subkey1) '('value1)))
    (let list1 (AssumeDistinct list1 '('key1 'subkey1) '('value1)))

    (let list2 (AsList
    (AsStruct '('key2 (Int32 '9)) '('subkey2 (Uint8 '0)) '('value2 (String 'Z)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '0)) '('value2 (String 'Y)))
    (AsStruct '('key2 (Int32 '3)) '('subkey2 (Uint8 '1)) '('value2 (String 'X)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '1)) '('value2 (String 'W)))
    (AsStruct '('key2 (Int32 '8)) '('subkey2 (Uint8 '1)) '('value2 (String 'V)))
    ))

    (let list2 (AssumeUnique list2 '('key2 'subkey2) '('value2)))
    (let list2 (AssumeDistinct list2 '('key2 'subkey2) '('value2)))

    (let dict (ToDict list2 (lambda '(item) '((Member item 'key2) (Member item 'subkey2))) (lambda '(item) '()) '('One 'Hashed)))

    (let join (MapJoinCore (ToFlow list1) dict 'LeftOnly '('key1 'subkey1) '('key2 'subkey2) '('key1 'key 'value1 'value) '() '() '()))
    (let list (Collect join))

    (let res_sink (DataSink 'yt (quote plato)))
    (let world (Write! world res_sink (Key '('table (String 'Output))) list '('('mode 'renew))))

    (let world (Commit! world res_sink))
    (return world)
)
        )";
        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "Collect", "Unique((value))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "Collect", "Distinct((value))");
    }

    Y_UNIT_TEST(EquiJoinWithRenames) {
        const auto s = R"(
(
    (let list1 (AsList
    (AsStruct '('key1 (Int32 '1)) '('subkey1 (Uint8 '0)) '('value1 (String 'A)))
    (AsStruct '('key1 (Int32 '7)) '('subkey1 (Uint8 '0)) '('value1 (String 'B)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '0)) '('value1 (String 'C)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '1)) '('value1 (String 'D)))
    ))

    (let list1 (AssumeUnique list1 '('key1 'subkey1) '('value1)))
    (let list1 (AssumeDistinct list1 '('key1 'subkey1) '('value1)))

    (let list2 (AsList
    (AsStruct '('key2 (Int32 '9)) '('subkey2 (Uint8 '0)) '('value2 (String 'Z)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '0)) '('value2 (String 'Y)))
    (AsStruct '('key2 (Int32 '3)) '('subkey2 (Uint8 '1)) '('value2 (String 'X)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '1)) '('value2 (String 'W)))
    (AsStruct '('key2 (Int32 '8)) '('subkey2 (Uint8 '1)) '('value2 (String 'V)))
    ))

    (let list2 (AssumeUnique list2 '('key2 'subkey2) '('value2)))
    (let list2 (AssumeDistinct list2 '('key2 'subkey2) '('value2)))

    (let join (EquiJoin '(list1 'a) '(list2 'b) '('Inner 'a 'b '('a 'key1 'a 'subkey1) '('b 'key2 'b 'subkey2) '()) '(
        '('rename 'a.key1 'key_1)
        '('rename 'a.key1 'key_2)
        '('rename 'a.subkey1 'subkey_1)
        '('rename 'a.subkey1 'subkey_2)
        '('rename 'a.value1 '"")
        '('rename 'b.key2 '"")
        '('rename 'b.value2 'value_1)
        '('rename 'b.value2 'value_2)
    )))
    (let lazy (LazyList join))

    (let res_sink (DataSink 'yt (quote plato)))
    (let world (Write! world res_sink (Key '('table (String 'Output))) lazy '('('mode 'renew))))

    (let world (Commit! world res_sink))
    (return world)
)
        )";
        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "LazyList", "Unique(({key_1,key_2},{subkey_1,subkey_2})({value_1,value_2}))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "LazyList", "Distinct(({key_1,key_2},{subkey_1,subkey_2})({value_1,value_2}))");
    }

    Y_UNIT_TEST(EquiJoinWithPartialRenames) {
        const auto s = R"(
(
    (let list1 (AsList
    (AsStruct '('key1 (Int32 '1)) '('subkey1 (Uint8 '0)) '('value1 (String 'A)))
    (AsStruct '('key1 (Int32 '7)) '('subkey1 (Uint8 '0)) '('value1 (String 'B)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '0)) '('value1 (String 'C)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '1)) '('value1 (String 'D)))
    ))

    (let list1 (AssumeUnique list1 '('key1 'subkey1) '('value1)))
    (let list1 (AssumeDistinct list1 '('key1 'subkey1) '('value1)))

    (let list2 (AsList
    (AsStruct '('key2 (Int32 '9)) '('subkey2 (Uint8 '0)) '('value2 (String 'Z)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '0)) '('value2 (String 'Y)))
    (AsStruct '('key2 (Int32 '3)) '('subkey2 (Uint8 '1)) '('value2 (String 'X)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '1)) '('value2 (String 'W)))
    (AsStruct '('key2 (Int32 '8)) '('subkey2 (Uint8 '1)) '('value2 (String 'V)))
    ))

    (let list2 (AssumeUnique list2 '('key2 'subkey2) '('value2)))
    (let list2 (AssumeDistinct list2 '('key2 'subkey2) '('value2)))

    (let join (EquiJoin '(list1 'a) '(list2 'b) '('Left 'a 'b '('a 'key1 'a 'subkey1) '('b 'key2 'b 'subkey2) '()) '(
        '('rename 'a.key1 'key_1)
        '('rename 'a.key1 'key_2)
        '('rename 'a.value1 '"")
        '('rename 'b.key2 '"")
        '('rename 'b.value2 'value)
    )))
    (let lazy (LazyList join))

    (let res_sink (DataSink 'yt (quote plato)))
    (let world (Write! world res_sink (Key '('table (String 'Output))) lazy '('('mode 'renew))))

    (let world (Commit! world res_sink))
    (return world)
)
        )";
        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "LazyList", "Unique((a.subkey1,{key_1,key_2})(value))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "LazyList", "Distinct((a.subkey1,{key_1,key_2}))");
    }

    Y_UNIT_TEST(EquiJoinInnerInner) {
        const auto s = R"(
(
    (let list1 (AsList
    (AsStruct '('key1 (Int32 '1)) '('subkey1 (Uint8 '0)) '('value1 (String 'A)))
    (AsStruct '('key1 (Int32 '7)) '('subkey1 (Uint8 '0)) '('value1 (String 'B)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '0)) '('value1 (String 'C)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '1)) '('value1 (String 'D)))
    ))

    (let list1 (AssumeUnique list1 '('key1 'subkey1) '('value1)))
    (let list1 (AssumeDistinct list1 '('key1 'subkey1) '('value1)))

    (let list2 (AsList
    (AsStruct '('key2 (Int32 '9)) '('subkey2 (Uint8 '0)) '('value2 (String 'Z)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '0)) '('value2 (String 'Y)))
    (AsStruct '('key2 (Int32 '3)) '('subkey2 (Uint8 '1)) '('value2 (String 'X)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '1)) '('value2 (String 'W)))
    (AsStruct '('key2 (Int32 '8)) '('subkey2 (Uint8 '1)) '('value2 (String 'V)))
    ))

    (let list2 (AssumeUnique list2 '('key2 'subkey2) '('value2)))
    (let list2 (AssumeDistinct list2 '('key2 'subkey2) '('value2)))

    (let list3 (AsList
    (AsStruct '('key3 (Int32 '1)) '('subkey3 (Uint8 '0)) '('value3 (String 'G)))
    (AsStruct '('key3 (Int32 '4)) '('subkey3 (Uint8 '1)) '('value3 (String 'H)))
    (AsStruct '('key3 (Int32 '2)) '('subkey3 (Uint8 '0)) '('value3 (String 'I)))
    (AsStruct '('key3 (Int32 '3)) '('subkey3 (Uint8 '1)) '('value3 (String 'J)))
    (AsStruct '('key3 (Int32 '3)) '('subkey3 (Uint8 '0)) '('value3 (String 'K)))
    ))

    (let list3 (AssumeUnique list3 '('key3 'subkey3) '('value3)))
    (let list3 (AssumeDistinct list3 '('key3 'subkey3) '('value3)))

    (let join (EquiJoin '(list1 'a) '(list2 'b) '(list3 'c) '('Inner '('Inner 'a 'b '('a 'key1 'a 'subkey1) '('b 'key2 'b 'subkey2) '()) 'c '('b 'key2 'b 'subkey2) '('c 'key3 'c 'subkey3) '()) '()))
    (let lazy (LazyList join))

    (let res_sink (DataSink 'yt (quote plato)))
    (let world (Write! world res_sink (Key '('table (String 'Output))) lazy '('('mode 'renew))))

    (let world (Commit! world res_sink))
    (return world)
)
        )";
        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "LazyList", "Unique((a.key1,a.subkey1)(a.value1)(b.key2,b.subkey2)(b.value2)(c.key3,c.subkey3)(c.value3))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "LazyList", "Distinct((a.key1,a.subkey1)(a.value1)(b.key2,b.subkey2)(b.value2)(c.key3,c.subkey3)(c.value3))");
    }

    Y_UNIT_TEST(EquiJoinInnerLeft) {
        const auto s = R"(
(
    (let list1 (AsList
    (AsStruct '('key1 (Int32 '1)) '('subkey1 (Uint8 '0)) '('value1 (String 'A)))
    (AsStruct '('key1 (Int32 '7)) '('subkey1 (Uint8 '0)) '('value1 (String 'B)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '0)) '('value1 (String 'C)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '1)) '('value1 (String 'D)))
    ))

    (let list1 (AssumeUnique list1 '('key1 'subkey1) '('value1)))
    (let list1 (AssumeDistinct list1 '('key1 'subkey1) '('value1)))

    (let list2 (AsList
    (AsStruct '('key2 (Int32 '9)) '('subkey2 (Uint8 '0)) '('value2 (String 'Z)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '0)) '('value2 (String 'Y)))
    (AsStruct '('key2 (Int32 '3)) '('subkey2 (Uint8 '1)) '('value2 (String 'X)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '1)) '('value2 (String 'W)))
    (AsStruct '('key2 (Int32 '8)) '('subkey2 (Uint8 '1)) '('value2 (String 'V)))
    ))

    (let list2 (AssumeUnique list2 '('key2 'subkey2) '('value2)))
    (let list2 (AssumeDistinct list2 '('key2 'subkey2) '('value2)))

    (let list3 (AsList
    (AsStruct '('key3 (Int32 '1)) '('subkey3 (Uint8 '0)) '('value3 (String 'G)))
    (AsStruct '('key3 (Int32 '4)) '('subkey3 (Uint8 '1)) '('value3 (String 'H)))
    (AsStruct '('key3 (Int32 '2)) '('subkey3 (Uint8 '0)) '('value3 (String 'I)))
    (AsStruct '('key3 (Int32 '3)) '('subkey3 (Uint8 '1)) '('value3 (String 'J)))
    (AsStruct '('key3 (Int32 '3)) '('subkey3 (Uint8 '0)) '('value3 (String 'K)))
    ))

    (let list3 (AssumeUnique list3 '('key3 'subkey3) '('value3)))
    (let list3 (AssumeDistinct list3 '('key3 'subkey3) '('value3)))

    (let join (EquiJoin '(list1 'a) '(list2 'b) '(list3 'c) '('Inner '('Left 'a 'b '('a 'key1 'a 'subkey1) '('b 'key2 'b 'subkey2) '()) 'c '('b 'key2 'b 'subkey2) '('c 'key3 'c 'subkey3) '()) '()))
    (let lazy (LazyList join))

    (let res_sink (DataSink 'yt (quote plato)))
    (let world (Write! world res_sink (Key '('table (String 'Output))) lazy '('('mode 'renew))))

    (let world (Commit! world res_sink))
    (return world)
)
        )";
        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "LazyList", "Unique((a.key1,a.subkey1)(a.value1)(b.key2,b.subkey2)(b.value2)(c.key3,c.subkey3)(c.value3))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "LazyList", "Distinct((a.key1,a.subkey1)(a.value1)(c.key3,c.subkey3)(c.value3))");
    }

    Y_UNIT_TEST(EquiJoinInnerRight) {
        const auto s = R"(
(
    (let list1 (AsList
    (AsStruct '('key1 (Int32 '1)) '('subkey1 (Uint8 '0)) '('value1 (String 'A)))
    (AsStruct '('key1 (Int32 '7)) '('subkey1 (Uint8 '0)) '('value1 (String 'B)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '0)) '('value1 (String 'C)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '1)) '('value1 (String 'D)))
    ))

    (let list1 (AssumeUnique list1 '('key1 'subkey1) '('value1)))
    (let list1 (AssumeDistinct list1 '('key1 'subkey1) '('value1)))

    (let list2 (AsList
    (AsStruct '('key2 (Int32 '9)) '('subkey2 (Uint8 '0)) '('value2 (String 'Z)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '0)) '('value2 (String 'Y)))
    (AsStruct '('key2 (Int32 '3)) '('subkey2 (Uint8 '1)) '('value2 (String 'X)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '1)) '('value2 (String 'W)))
    (AsStruct '('key2 (Int32 '8)) '('subkey2 (Uint8 '1)) '('value2 (String 'V)))
    ))

    (let list2 (AssumeUnique list2 '('key2 'subkey2) '('value2)))
    (let list2 (AssumeDistinct list2 '('key2 'subkey2) '('value2)))

    (let list3 (AsList
    (AsStruct '('key3 (Int32 '1)) '('subkey3 (Uint8 '0)) '('value3 (String 'G)))
    (AsStruct '('key3 (Int32 '4)) '('subkey3 (Uint8 '1)) '('value3 (String 'H)))
    (AsStruct '('key3 (Int32 '2)) '('subkey3 (Uint8 '0)) '('value3 (String 'I)))
    (AsStruct '('key3 (Int32 '3)) '('subkey3 (Uint8 '1)) '('value3 (String 'J)))
    (AsStruct '('key3 (Int32 '3)) '('subkey3 (Uint8 '0)) '('value3 (String 'K)))
    ))

    (let list3 (AssumeUnique list3 '('key3 'subkey3) '('value3)))
    (let list3 (AssumeDistinct list3 '('key3 'subkey3) '('value3)))

    (let join (EquiJoin '(list1 'a) '(list2 'b) '(list3 'c) '('Inner '('Right 'a 'b '('a 'key1 'a 'subkey1) '('b 'key2 'b 'subkey2) '()) 'c '('b 'key2 'b 'subkey2) '('c 'key3 'c 'subkey3) '()) '()))
    (let lazy (LazyList join))

    (let res_sink (DataSink 'yt (quote plato)))
    (let world (Write! world res_sink (Key '('table (String 'Output))) lazy '('('mode 'renew))))

    (let world (Commit! world res_sink))
    (return world)
)
        )";
        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "LazyList", "Unique((a.key1,a.subkey1)(a.value1)(b.key2,b.subkey2)(b.value2)(c.key3,c.subkey3)(c.value3))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "LazyList", "Distinct((b.key2,b.subkey2)(b.value2)(c.key3,c.subkey3)(c.value3))");
    }

    Y_UNIT_TEST(EquiJoinInnerFull) {
        const auto s = R"(
(
    (let list1 (AsList
    (AsStruct '('key1 (Int32 '1)) '('subkey1 (Uint8 '0)) '('value1 (String 'A)))
    (AsStruct '('key1 (Int32 '7)) '('subkey1 (Uint8 '0)) '('value1 (String 'B)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '0)) '('value1 (String 'C)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '1)) '('value1 (String 'D)))
    ))

    (let list1 (AssumeUnique list1 '('key1 'subkey1) '('value1)))
    (let list1 (AssumeDistinct list1 '('key1 'subkey1) '('value1)))

    (let list2 (AsList
    (AsStruct '('key2 (Int32 '9)) '('subkey2 (Uint8 '0)) '('value2 (String 'Z)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '0)) '('value2 (String 'Y)))
    (AsStruct '('key2 (Int32 '3)) '('subkey2 (Uint8 '1)) '('value2 (String 'X)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '1)) '('value2 (String 'W)))
    (AsStruct '('key2 (Int32 '8)) '('subkey2 (Uint8 '1)) '('value2 (String 'V)))
    ))

    (let list2 (AssumeUnique list2 '('key2 'subkey2) '('value2)))
    (let list2 (AssumeDistinct list2 '('key2 'subkey2) '('value2)))

    (let list3 (AsList
    (AsStruct '('key3 (Int32 '1)) '('subkey3 (Uint8 '0)) '('value3 (String 'G)))
    (AsStruct '('key3 (Int32 '4)) '('subkey3 (Uint8 '1)) '('value3 (String 'H)))
    (AsStruct '('key3 (Int32 '2)) '('subkey3 (Uint8 '0)) '('value3 (String 'I)))
    (AsStruct '('key3 (Int32 '3)) '('subkey3 (Uint8 '1)) '('value3 (String 'J)))
    (AsStruct '('key3 (Int32 '3)) '('subkey3 (Uint8 '0)) '('value3 (String 'K)))
    ))

    (let list3 (AssumeUnique list3 '('key3 'subkey3) '('value3)))
    (let list3 (AssumeDistinct list3 '('key3 'subkey3) '('value3)))

    (let join (EquiJoin '(list1 'a) '(list2 'b) '(list3 'c) '('Inner '('Full 'a 'b '('a 'key1 'a 'subkey1) '('b 'key2 'b 'subkey2) '()) 'c '('b 'key2 'b 'subkey2) '('c 'key3 'c 'subkey3) '()) '()))
    (let lazy (LazyList join))

    (let res_sink (DataSink 'yt (quote plato)))
    (let world (Write! world res_sink (Key '('table (String 'Output))) lazy '('('mode 'renew))))

    (let world (Commit! world res_sink))
    (return world)
)
        )";
        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "LazyList", "Unique((a.key1,a.subkey1)(a.value1)(b.key2,b.subkey2)(b.value2)(c.key3,c.subkey3)(c.value3))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "LazyList", "Distinct((c.key3,c.subkey3)(c.value3))");
    }

    Y_UNIT_TEST(EquiJoinInnerExclusion) {
        const auto s = R"(
(
    (let list1 (AsList
    (AsStruct '('key1 (Int32 '1)) '('subkey1 (Uint8 '0)) '('value1 (String 'A)))
    (AsStruct '('key1 (Int32 '7)) '('subkey1 (Uint8 '0)) '('value1 (String 'B)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '0)) '('value1 (String 'C)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '1)) '('value1 (String 'D)))
    ))

    (let list1 (AssumeUnique list1 '('key1 'subkey1) '('value1)))
    (let list1 (AssumeDistinct list1 '('key1 'subkey1) '('value1)))

    (let list2 (AsList
    (AsStruct '('key2 (Int32 '9)) '('subkey2 (Uint8 '0)) '('value2 (String 'Z)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '0)) '('value2 (String 'Y)))
    (AsStruct '('key2 (Int32 '3)) '('subkey2 (Uint8 '1)) '('value2 (String 'X)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '1)) '('value2 (String 'W)))
    (AsStruct '('key2 (Int32 '8)) '('subkey2 (Uint8 '1)) '('value2 (String 'V)))
    ))

    (let list2 (AssumeUnique list2 '('key2 'subkey2) '('value2)))
    (let list2 (AssumeDistinct list2 '('key2 'subkey2) '('value2)))

    (let list3 (AsList
    (AsStruct '('key3 (Int32 '1)) '('subkey3 (Uint8 '0)) '('value3 (String 'G)))
    (AsStruct '('key3 (Int32 '4)) '('subkey3 (Uint8 '1)) '('value3 (String 'H)))
    (AsStruct '('key3 (Int32 '2)) '('subkey3 (Uint8 '0)) '('value3 (String 'I)))
    (AsStruct '('key3 (Int32 '3)) '('subkey3 (Uint8 '1)) '('value3 (String 'J)))
    (AsStruct '('key3 (Int32 '3)) '('subkey3 (Uint8 '0)) '('value3 (String 'K)))
    ))

    (let list3 (AssumeUnique list3 '('key3 'subkey3) '('value3)))
    (let list3 (AssumeDistinct list3 '('key3 'subkey3) '('value3)))

    (let join (EquiJoin '(list1 'a) '(list2 'b) '(list3 'c) '('Inner '('Exclusion 'a 'b '('a 'key1 'a 'subkey1) '('b 'key2 'b 'subkey2) '()) 'c '('b 'key2 'b 'subkey2) '('c 'key3 'c 'subkey3) '()) '()))
    (let lazy (LazyList join))

    (let res_sink (DataSink 'yt (quote plato)))
    (let world (Write! world res_sink (Key '('table (String 'Output))) lazy '('('mode 'renew))))

    (let world (Commit! world res_sink))
    (return world)
)
        )";
        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "LazyList", "Unique((a.key1,a.subkey1)(a.value1)(b.key2,b.subkey2)(b.value2)(c.key3,c.subkey3)(c.value3))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "LazyList", "Distinct((c.key3,c.subkey3)(c.value3))");
    }

    Y_UNIT_TEST(EquiJoinInnerLeftOnly) {
        const auto s = R"(
(
    (let list1 (AsList
    (AsStruct '('key1 (Int32 '1)) '('subkey1 (Uint8 '0)) '('value1 (String 'A)))
    (AsStruct '('key1 (Int32 '7)) '('subkey1 (Uint8 '0)) '('value1 (String 'B)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '0)) '('value1 (String 'C)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '1)) '('value1 (String 'D)))
    ))

    (let list1 (AssumeUnique list1 '('key1 'subkey1) '('value1)))
    (let list1 (AssumeDistinct list1 '('key1 'subkey1) '('value1)))

    (let list2 (AsList
    (AsStruct '('key2 (Int32 '9)) '('subkey2 (Uint8 '0)) '('value2 (String 'Z)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '0)) '('value2 (String 'Y)))
    (AsStruct '('key2 (Int32 '3)) '('subkey2 (Uint8 '1)) '('value2 (String 'X)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '1)) '('value2 (String 'W)))
    (AsStruct '('key2 (Int32 '8)) '('subkey2 (Uint8 '1)) '('value2 (String 'V)))
    ))

    (let list2 (AssumeUnique list2 '('key2 'subkey2) '('value2)))
    (let list2 (AssumeDistinct list2 '('key2 'subkey2) '('value2)))

    (let list3 (AsList
    (AsStruct '('key3 (Int32 '1)) '('subkey3 (Uint8 '0)) '('value3 (String 'G)))
    (AsStruct '('key3 (Int32 '4)) '('subkey3 (Uint8 '1)) '('value3 (String 'H)))
    (AsStruct '('key3 (Int32 '2)) '('subkey3 (Uint8 '0)) '('value3 (String 'I)))
    (AsStruct '('key3 (Int32 '3)) '('subkey3 (Uint8 '1)) '('value3 (String 'J)))
    (AsStruct '('key3 (Int32 '3)) '('subkey3 (Uint8 '0)) '('value3 (String 'K)))
    ))

    (let list3 (AssumeUnique list3 '('key3 'subkey3) '('value3)))
    (let list3 (AssumeDistinct list3 '('key3 'subkey3) '('value3)))

    (let join (EquiJoin '(list1 'a) '(list2 'b) '(list3 'c) '('Inner '('LeftOnly 'a 'b '('a 'key1 'a 'subkey1) '('b 'key2 'b 'subkey2) '()) 'c '('a 'key1 'a 'subkey1) '('c 'key3 'c 'subkey3) '()) '()))
    (let lazy (LazyList join))

    (let res_sink (DataSink 'yt (quote plato)))
    (let world (Write! world res_sink (Key '('table (String 'Output))) lazy '('('mode 'renew))))

    (let world (Commit! world res_sink))
    (return world)
)
        )";
        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "LazyList", "Unique((a.key1,a.subkey1)(a.value1)(c.key3,c.subkey3)(c.value3))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "LazyList", "Distinct((a.key1,a.subkey1)(a.value1)(c.key3,c.subkey3)(c.value3))");
    }

    Y_UNIT_TEST(EquiJoinInnerLeftSemi) {
        const auto s = R"(
(
    (let list1 (AsList
    (AsStruct '('key1 (Int32 '1)) '('subkey1 (Uint8 '0)) '('value1 (String 'A)))
    (AsStruct '('key1 (Int32 '7)) '('subkey1 (Uint8 '0)) '('value1 (String 'B)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '0)) '('value1 (String 'C)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '1)) '('value1 (String 'D)))
    ))

    (let list1 (AssumeUnique list1 '('key1 'subkey1) '('value1)))
    (let list1 (AssumeDistinct list1 '('key1 'subkey1) '('value1)))

    (let list2 (AsList
    (AsStruct '('key2 (Int32 '9)) '('subkey2 (Uint8 '0)) '('value2 (String 'Z)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '0)) '('value2 (String 'Y)))
    (AsStruct '('key2 (Int32 '3)) '('subkey2 (Uint8 '1)) '('value2 (String 'X)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '1)) '('value2 (String 'W)))
    (AsStruct '('key2 (Int32 '8)) '('subkey2 (Uint8 '1)) '('value2 (String 'V)))
    ))

    (let list2 (AssumeUnique list2 '('key2 'subkey2) '('value2)))
    (let list2 (AssumeDistinct list2 '('key2 'subkey2) '('value2)))

    (let list3 (AsList
    (AsStruct '('key3 (Int32 '1)) '('subkey3 (Uint8 '0)) '('value3 (String 'G)))
    (AsStruct '('key3 (Int32 '4)) '('subkey3 (Uint8 '1)) '('value3 (String 'H)))
    (AsStruct '('key3 (Int32 '2)) '('subkey3 (Uint8 '0)) '('value3 (String 'I)))
    (AsStruct '('key3 (Int32 '3)) '('subkey3 (Uint8 '1)) '('value3 (String 'J)))
    (AsStruct '('key3 (Int32 '3)) '('subkey3 (Uint8 '0)) '('value3 (String 'K)))
    ))

    (let list3 (AssumeUnique list3 '('key3 'subkey3) '('value3)))
    (let list3 (AssumeDistinct list3 '('key3 'subkey3) '('value3)))

    (let join (EquiJoin '(list1 'a) '(list2 'b) '(list3 'c) '('Inner '('LeftSemi 'a 'b '('a 'key1 'a 'subkey1) '('b 'key2 'b 'subkey2) '()) 'c '('a 'key1 'a 'subkey1) '('c 'key3 'c 'subkey3) '()) '()))
    (let lazy (LazyList join))

    (let res_sink (DataSink 'yt (quote plato)))
    (let world (Write! world res_sink (Key '('table (String 'Output))) lazy '('('mode 'renew))))

    (let world (Commit! world res_sink))
    (return world)
)
        )";
        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "LazyList", "Unique((a.key1,a.subkey1)(a.value1)(c.key3,c.subkey3)(c.value3))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "LazyList", "Distinct((a.key1,a.subkey1)(a.value1)(c.key3,c.subkey3)(c.value3))");
    }

    Y_UNIT_TEST(EquiJoinInnerRightOnly) {
        const auto s = R"(
(
    (let list1 (AsList
    (AsStruct '('key1 (Int32 '1)) '('subkey1 (Uint8 '0)) '('value1 (String 'A)))
    (AsStruct '('key1 (Int32 '7)) '('subkey1 (Uint8 '0)) '('value1 (String 'B)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '0)) '('value1 (String 'C)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '1)) '('value1 (String 'D)))
    ))

    (let list1 (AssumeUnique list1 '('key1 'subkey1) '('value1)))
    (let list1 (AssumeDistinct list1 '('key1 'subkey1) '('value1)))

    (let list2 (AsList
    (AsStruct '('key2 (Int32 '9)) '('subkey2 (Uint8 '0)) '('value2 (String 'Z)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '0)) '('value2 (String 'Y)))
    (AsStruct '('key2 (Int32 '3)) '('subkey2 (Uint8 '1)) '('value2 (String 'X)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '1)) '('value2 (String 'W)))
    (AsStruct '('key2 (Int32 '8)) '('subkey2 (Uint8 '1)) '('value2 (String 'V)))
    ))

    (let list2 (AssumeUnique list2 '('key2 'subkey2) '('value2)))
    (let list2 (AssumeDistinct list2 '('key2 'subkey2) '('value2)))

    (let list3 (AsList
    (AsStruct '('key3 (Int32 '1)) '('subkey3 (Uint8 '0)) '('value3 (String 'G)))
    (AsStruct '('key3 (Int32 '4)) '('subkey3 (Uint8 '1)) '('value3 (String 'H)))
    (AsStruct '('key3 (Int32 '2)) '('subkey3 (Uint8 '0)) '('value3 (String 'I)))
    (AsStruct '('key3 (Int32 '3)) '('subkey3 (Uint8 '1)) '('value3 (String 'J)))
    (AsStruct '('key3 (Int32 '3)) '('subkey3 (Uint8 '0)) '('value3 (String 'K)))
    ))

    (let list3 (AssumeUnique list3 '('key3 'subkey3) '('value3)))
    (let list3 (AssumeDistinct list3 '('key3 'subkey3) '('value3)))

    (let join (EquiJoin '(list1 'a) '(list2 'b) '(list3 'c) '('Inner '('RightOnly 'a 'b '('a 'key1 'a 'subkey1) '('b 'key2 'b 'subkey2) '()) 'c '('b 'key2 'b 'subkey2) '('c 'key3 'c 'subkey3) '()) '()))
    (let lazy (LazyList join))

    (let res_sink (DataSink 'yt (quote plato)))
    (let world (Write! world res_sink (Key '('table (String 'Output))) lazy '('('mode 'renew))))

    (let world (Commit! world res_sink))
    (return world)
)
        )";
        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "LazyList", "Unique((b.key2,b.subkey2)(b.value2)(c.key3,c.subkey3)(c.value3))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "LazyList", "Distinct((b.key2,b.subkey2)(b.value2)(c.key3,c.subkey3)(c.value3))");
    }

    Y_UNIT_TEST(EquiJoinInnerRightSemi) {
        const auto s = R"(
(
    (let list1 (AsList
    (AsStruct '('key1 (Int32 '1)) '('subkey1 (Uint8 '0)) '('value1 (String 'A)))
    (AsStruct '('key1 (Int32 '7)) '('subkey1 (Uint8 '0)) '('value1 (String 'B)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '0)) '('value1 (String 'C)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '1)) '('value1 (String 'D)))
    ))

    (let list1 (AssumeUnique list1 '('key1 'subkey1) '('value1)))
    (let list1 (AssumeDistinct list1 '('key1 'subkey1) '('value1)))

    (let list2 (AsList
    (AsStruct '('key2 (Int32 '9)) '('subkey2 (Uint8 '0)) '('value2 (String 'Z)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '0)) '('value2 (String 'Y)))
    (AsStruct '('key2 (Int32 '3)) '('subkey2 (Uint8 '1)) '('value2 (String 'X)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '1)) '('value2 (String 'W)))
    (AsStruct '('key2 (Int32 '8)) '('subkey2 (Uint8 '1)) '('value2 (String 'V)))
    ))

    (let list2 (AssumeUnique list2 '('key2 'subkey2) '('value2)))
    (let list2 (AssumeDistinct list2 '('key2 'subkey2) '('value2)))

    (let list3 (AsList
    (AsStruct '('key3 (Int32 '1)) '('subkey3 (Uint8 '0)) '('value3 (String 'G)))
    (AsStruct '('key3 (Int32 '4)) '('subkey3 (Uint8 '1)) '('value3 (String 'H)))
    (AsStruct '('key3 (Int32 '2)) '('subkey3 (Uint8 '0)) '('value3 (String 'I)))
    (AsStruct '('key3 (Int32 '3)) '('subkey3 (Uint8 '1)) '('value3 (String 'J)))
    (AsStruct '('key3 (Int32 '3)) '('subkey3 (Uint8 '0)) '('value3 (String 'K)))
    ))

    (let list3 (AssumeUnique list3 '('key3 'subkey3) '('value3)))
    (let list3 (AssumeDistinct list3 '('key3 'subkey3) '('value3)))

    (let join (EquiJoin '(list1 'a) '(list2 'b) '(list3 'c) '('Inner '('RightSemi 'a 'b '('a 'key1 'a 'subkey1) '('b 'key2 'b 'subkey2) '()) 'c '('b 'key2 'b 'subkey2) '('c 'key3 'c 'subkey3) '()) '()))
    (let lazy (LazyList join))

    (let res_sink (DataSink 'yt (quote plato)))
    (let world (Write! world res_sink (Key '('table (String 'Output))) lazy '('('mode 'renew))))

    (let world (Commit! world res_sink))
    (return world)
)
        )";
        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "LazyList", "Unique((b.key2,b.subkey2)(b.value2)(c.key3,c.subkey3)(c.value3))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "LazyList", "Distinct((b.key2,b.subkey2)(b.value2)(c.key3,c.subkey3)(c.value3))");
    }

    Y_UNIT_TEST(EquiJoinLeftInner) {
        const auto s = R"(
(
    (let list1 (AsList
    (AsStruct '('key1 (Int32 '1)) '('subkey1 (Uint8 '0)) '('value1 (String 'A)))
    (AsStruct '('key1 (Int32 '7)) '('subkey1 (Uint8 '0)) '('value1 (String 'B)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '0)) '('value1 (String 'C)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '1)) '('value1 (String 'D)))
    ))

    (let list1 (AssumeUnique list1 '('key1 'subkey1) '('value1)))
    (let list1 (AssumeDistinct list1 '('key1 'subkey1) '('value1)))

    (let list2 (AsList
    (AsStruct '('key2 (Int32 '9)) '('subkey2 (Uint8 '0)) '('value2 (String 'Z)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '0)) '('value2 (String 'Y)))
    (AsStruct '('key2 (Int32 '3)) '('subkey2 (Uint8 '1)) '('value2 (String 'X)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '1)) '('value2 (String 'W)))
    (AsStruct '('key2 (Int32 '8)) '('subkey2 (Uint8 '1)) '('value2 (String 'V)))
    ))

    (let list2 (AssumeUnique list2 '('key2 'subkey2) '('value2)))
    (let list2 (AssumeDistinct list2 '('key2 'subkey2) '('value2)))

    (let list3 (AsList
    (AsStruct '('key3 (Int32 '1)) '('subkey3 (Uint8 '0)) '('value3 (String 'G)))
    (AsStruct '('key3 (Int32 '4)) '('subkey3 (Uint8 '1)) '('value3 (String 'H)))
    (AsStruct '('key3 (Int32 '2)) '('subkey3 (Uint8 '0)) '('value3 (String 'I)))
    (AsStruct '('key3 (Int32 '3)) '('subkey3 (Uint8 '1)) '('value3 (String 'J)))
    (AsStruct '('key3 (Int32 '3)) '('subkey3 (Uint8 '0)) '('value3 (String 'K)))
    ))

    (let list3 (AssumeUnique list3 '('key3 'subkey3) '('value3)))
    (let list3 (AssumeDistinct list3 '('key3 'subkey3) '('value3)))

    (let join (EquiJoin '(list1 'a) '(list2 'b) '(list3 'c) '('Left '('Inner 'a 'b '('a 'key1 'a 'subkey1) '('b 'key2 'b 'subkey2) '()) 'c '('b 'key2 'b 'subkey2) '('c 'key3 'c 'subkey3) '()) '()))
    (let lazy (LazyList join))

    (let res_sink (DataSink 'yt (quote plato)))
    (let world (Write! world res_sink (Key '('table (String 'Output))) lazy '('('mode 'renew))))

    (let world (Commit! world res_sink))
    (return world)
)
        )";
        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "LazyList", "Distinct((a.key1,a.subkey1)(a.value1)(b.key2,b.subkey2)(b.value2))");
    }

    Y_UNIT_TEST(EquiJoinLeftLeft) {
        const auto s = R"(
(
    (let list1 (AsList
    (AsStruct '('key1 (Int32 '1)) '('subkey1 (Uint8 '0)) '('value1 (String 'A)))
    (AsStruct '('key1 (Int32 '7)) '('subkey1 (Uint8 '0)) '('value1 (String 'B)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '0)) '('value1 (String 'C)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '1)) '('value1 (String 'D)))
    ))

    (let list1 (AssumeUnique list1 '('key1 'subkey1) '('value1)))
    (let list1 (AssumeDistinct list1 '('key1 'subkey1) '('value1)))

    (let list2 (AsList
    (AsStruct '('key2 (Int32 '9)) '('subkey2 (Uint8 '0)) '('value2 (String 'Z)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '0)) '('value2 (String 'Y)))
    (AsStruct '('key2 (Int32 '3)) '('subkey2 (Uint8 '1)) '('value2 (String 'X)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '1)) '('value2 (String 'W)))
    (AsStruct '('key2 (Int32 '8)) '('subkey2 (Uint8 '1)) '('value2 (String 'V)))
    ))

    (let list2 (AssumeUnique list2 '('key2 'subkey2) '('value2)))
    (let list2 (AssumeDistinct list2 '('key2 'subkey2) '('value2)))

    (let list3 (AsList
    (AsStruct '('key3 (Int32 '1)) '('subkey3 (Uint8 '0)) '('value3 (String 'G)))
    (AsStruct '('key3 (Int32 '4)) '('subkey3 (Uint8 '1)) '('value3 (String 'H)))
    (AsStruct '('key3 (Int32 '2)) '('subkey3 (Uint8 '0)) '('value3 (String 'I)))
    (AsStruct '('key3 (Int32 '3)) '('subkey3 (Uint8 '1)) '('value3 (String 'J)))
    (AsStruct '('key3 (Int32 '3)) '('subkey3 (Uint8 '0)) '('value3 (String 'K)))
    ))

    (let list3 (AssumeUnique list3 '('key3 'subkey3) '('value3)))
    (let list3 (AssumeDistinct list3 '('key3 'subkey3) '('value3)))

    (let join (EquiJoin '(list1 'a) '(list2 'b) '(list3 'c) '('Left '('Left 'a 'b '('a 'key1 'a 'subkey1) '('b 'key2 'b 'subkey2) '()) 'c '('b 'key2 'b 'subkey2) '('c 'key3 'c 'subkey3) '()) '()))
    (let lazy (LazyList join))

    (let res_sink (DataSink 'yt (quote plato)))
    (let world (Write! world res_sink (Key '('table (String 'Output))) lazy '('('mode 'renew))))

    (let world (Commit! world res_sink))
    (return world)
)
        )";
        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "LazyList", "Unique((a.key1,a.subkey1)(a.value1)(b.key2,b.subkey2)(b.value2)(c.key3,c.subkey3)(c.value3))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "LazyList", "Distinct((a.key1,a.subkey1)(a.value1))");
    }

    Y_UNIT_TEST(EquiJoinLeftRight) {
        const auto s = R"(
(
    (let list1 (AsList
    (AsStruct '('key1 (Int32 '1)) '('subkey1 (Uint8 '0)) '('value1 (String 'A)))
    (AsStruct '('key1 (Int32 '7)) '('subkey1 (Uint8 '0)) '('value1 (String 'B)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '0)) '('value1 (String 'C)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '1)) '('value1 (String 'D)))
    ))

    (let list1 (AssumeUnique list1 '('key1 'subkey1) '('value1)))
    (let list1 (AssumeDistinct list1 '('key1 'subkey1) '('value1)))

    (let list2 (AsList
    (AsStruct '('key2 (Int32 '9)) '('subkey2 (Uint8 '0)) '('value2 (String 'Z)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '0)) '('value2 (String 'Y)))
    (AsStruct '('key2 (Int32 '3)) '('subkey2 (Uint8 '1)) '('value2 (String 'X)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '1)) '('value2 (String 'W)))
    (AsStruct '('key2 (Int32 '8)) '('subkey2 (Uint8 '1)) '('value2 (String 'V)))
    ))

    (let list2 (AssumeUnique list2 '('key2 'subkey2) '('value2)))
    (let list2 (AssumeDistinct list2 '('key2 'subkey2) '('value2)))

    (let list3 (AsList
    (AsStruct '('key3 (Int32 '1)) '('subkey3 (Uint8 '0)) '('value3 (String 'G)))
    (AsStruct '('key3 (Int32 '4)) '('subkey3 (Uint8 '1)) '('value3 (String 'H)))
    (AsStruct '('key3 (Int32 '2)) '('subkey3 (Uint8 '0)) '('value3 (String 'I)))
    (AsStruct '('key3 (Int32 '3)) '('subkey3 (Uint8 '1)) '('value3 (String 'J)))
    (AsStruct '('key3 (Int32 '3)) '('subkey3 (Uint8 '0)) '('value3 (String 'K)))
    ))

    (let list3 (AssumeUnique list3 '('key3 'subkey3) '('value3)))
    (let list3 (AssumeDistinct list3 '('key3 'subkey3) '('value3)))

    (let join (EquiJoin '(list1 'a) '(list2 'b) '(list3 'c) '('Left '('Right 'a 'b '('a 'key1 'a 'subkey1) '('b 'key2 'b 'subkey2) '()) 'c '('b 'key2 'b 'subkey2) '('c 'key3 'c 'subkey3) '()) '()))
    (let lazy (LazyList join))

    (let res_sink (DataSink 'yt (quote plato)))
    (let world (Write! world res_sink (Key '('table (String 'Output))) lazy '('('mode 'renew))))

    (let world (Commit! world res_sink))
    (return world)
)
        )";
        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "LazyList", "Unique((a.key1,a.subkey1)(a.value1)(b.key2,b.subkey2)(b.value2)(c.key3,c.subkey3)(c.value3))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "LazyList", "Distinct((b.key2,b.subkey2)(b.value2))");
    }

    Y_UNIT_TEST(EquiJoinLeftFull) {
        const auto s = R"(
(
    (let list1 (AsList
    (AsStruct '('key1 (Int32 '1)) '('subkey1 (Uint8 '0)) '('value1 (String 'A)))
    (AsStruct '('key1 (Int32 '7)) '('subkey1 (Uint8 '0)) '('value1 (String 'B)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '0)) '('value1 (String 'C)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '1)) '('value1 (String 'D)))
    ))

    (let list1 (AssumeUnique list1 '('key1 'subkey1) '('value1)))
    (let list1 (AssumeDistinct list1 '('key1 'subkey1) '('value1)))

    (let list2 (AsList
    (AsStruct '('key2 (Int32 '9)) '('subkey2 (Uint8 '0)) '('value2 (String 'Z)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '0)) '('value2 (String 'Y)))
    (AsStruct '('key2 (Int32 '3)) '('subkey2 (Uint8 '1)) '('value2 (String 'X)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '1)) '('value2 (String 'W)))
    (AsStruct '('key2 (Int32 '8)) '('subkey2 (Uint8 '1)) '('value2 (String 'V)))
    ))

    (let list2 (AssumeUnique list2 '('key2 'subkey2) '('value2)))
    (let list2 (AssumeDistinct list2 '('key2 'subkey2) '('value2)))

    (let list3 (AsList
    (AsStruct '('key3 (Int32 '1)) '('subkey3 (Uint8 '0)) '('value3 (String 'G)))
    (AsStruct '('key3 (Int32 '4)) '('subkey3 (Uint8 '1)) '('value3 (String 'H)))
    (AsStruct '('key3 (Int32 '2)) '('subkey3 (Uint8 '0)) '('value3 (String 'I)))
    (AsStruct '('key3 (Int32 '3)) '('subkey3 (Uint8 '1)) '('value3 (String 'J)))
    (AsStruct '('key3 (Int32 '3)) '('subkey3 (Uint8 '0)) '('value3 (String 'K)))
    ))

    (let list3 (AssumeUnique list3 '('key3 'subkey3) '('value3)))
    (let list3 (AssumeDistinct list3 '('key3 'subkey3) '('value3)))

    (let join (EquiJoin '(list1 'a) '(list2 'b) '(list3 'c) '('Left '('Full 'a 'b '('a 'key1 'a 'subkey1) '('b 'key2 'b 'subkey2) '()) 'c '('b 'key2 'b 'subkey2) '('c 'key3 'c 'subkey3) '()) '()))
    (let lazy (LazyList join))

    (let res_sink (DataSink 'yt (quote plato)))
    (let world (Write! world res_sink (Key '('table (String 'Output))) lazy '('('mode 'renew))))

    (let world (Commit! world res_sink))
    (return world)
)
        )";
        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "LazyList", "Unique((a.key1,a.subkey1)(a.value1)(b.key2,b.subkey2)(b.value2)(c.key3,c.subkey3)(c.value3))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "LazyList", "");
    }

    Y_UNIT_TEST(EquiJoinLeftExclusion) {
        const auto s = R"(
(
    (let list1 (AsList
    (AsStruct '('key1 (Int32 '1)) '('subkey1 (Uint8 '0)) '('value1 (String 'A)))
    (AsStruct '('key1 (Int32 '7)) '('subkey1 (Uint8 '0)) '('value1 (String 'B)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '0)) '('value1 (String 'C)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '1)) '('value1 (String 'D)))
    ))

    (let list1 (AssumeUnique list1 '('key1 'subkey1) '('value1)))
    (let list1 (AssumeDistinct list1 '('key1 'subkey1) '('value1)))

    (let list2 (AsList
    (AsStruct '('key2 (Int32 '9)) '('subkey2 (Uint8 '0)) '('value2 (String 'Z)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '0)) '('value2 (String 'Y)))
    (AsStruct '('key2 (Int32 '3)) '('subkey2 (Uint8 '1)) '('value2 (String 'X)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '1)) '('value2 (String 'W)))
    (AsStruct '('key2 (Int32 '8)) '('subkey2 (Uint8 '1)) '('value2 (String 'V)))
    ))

    (let list2 (AssumeUnique list2 '('key2 'subkey2) '('value2)))
    (let list2 (AssumeDistinct list2 '('key2 'subkey2) '('value2)))

    (let list3 (AsList
    (AsStruct '('key3 (Int32 '1)) '('subkey3 (Uint8 '0)) '('value3 (String 'G)))
    (AsStruct '('key3 (Int32 '4)) '('subkey3 (Uint8 '1)) '('value3 (String 'H)))
    (AsStruct '('key3 (Int32 '2)) '('subkey3 (Uint8 '0)) '('value3 (String 'I)))
    (AsStruct '('key3 (Int32 '3)) '('subkey3 (Uint8 '1)) '('value3 (String 'J)))
    (AsStruct '('key3 (Int32 '3)) '('subkey3 (Uint8 '0)) '('value3 (String 'K)))
    ))

    (let list3 (AssumeUnique list3 '('key3 'subkey3) '('value3)))
    (let list3 (AssumeDistinct list3 '('key3 'subkey3) '('value3)))

    (let join (EquiJoin '(list1 'a) '(list2 'b) '(list3 'c) '('Left '('Exclusion 'a 'b '('a 'key1 'a 'subkey1) '('b 'key2 'b 'subkey2) '()) 'c '('b 'key2 'b 'subkey2) '('c 'key3 'c 'subkey3) '()) '()))
    (let lazy (LazyList join))

    (let res_sink (DataSink 'yt (quote plato)))
    (let world (Write! world res_sink (Key '('table (String 'Output))) lazy '('('mode 'renew))))

    (let world (Commit! world res_sink))
    (return world)
)
        )";
        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "LazyList", "Unique((a.key1,a.subkey1)(a.value1)(b.key2,b.subkey2)(b.value2)(c.key3,c.subkey3)(c.value3))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "LazyList", "");
    }

    Y_UNIT_TEST(EquiJoinLeftLeftOnly) {
        const auto s = R"(
(
    (let list1 (AsList
    (AsStruct '('key1 (Int32 '1)) '('subkey1 (Uint8 '0)) '('value1 (String 'A)))
    (AsStruct '('key1 (Int32 '7)) '('subkey1 (Uint8 '0)) '('value1 (String 'B)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '0)) '('value1 (String 'C)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '1)) '('value1 (String 'D)))
    ))

    (let list1 (AssumeUnique list1 '('key1 'subkey1) '('value1)))
    (let list1 (AssumeDistinct list1 '('key1 'subkey1) '('value1)))

    (let list2 (AsList
    (AsStruct '('key2 (Int32 '9)) '('subkey2 (Uint8 '0)) '('value2 (String 'Z)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '0)) '('value2 (String 'Y)))
    (AsStruct '('key2 (Int32 '3)) '('subkey2 (Uint8 '1)) '('value2 (String 'X)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '1)) '('value2 (String 'W)))
    (AsStruct '('key2 (Int32 '8)) '('subkey2 (Uint8 '1)) '('value2 (String 'V)))
    ))

    (let list2 (AssumeUnique list2 '('key2 'subkey2) '('value2)))
    (let list2 (AssumeDistinct list2 '('key2 'subkey2) '('value2)))

    (let list3 (AsList
    (AsStruct '('key3 (Int32 '1)) '('subkey3 (Uint8 '0)) '('value3 (String 'G)))
    (AsStruct '('key3 (Int32 '4)) '('subkey3 (Uint8 '1)) '('value3 (String 'H)))
    (AsStruct '('key3 (Int32 '2)) '('subkey3 (Uint8 '0)) '('value3 (String 'I)))
    (AsStruct '('key3 (Int32 '3)) '('subkey3 (Uint8 '1)) '('value3 (String 'J)))
    (AsStruct '('key3 (Int32 '3)) '('subkey3 (Uint8 '0)) '('value3 (String 'K)))
    ))

    (let list3 (AssumeUnique list3 '('key3 'subkey3) '('value3)))
    (let list3 (AssumeDistinct list3 '('key3 'subkey3) '('value3)))

    (let join (EquiJoin '(list1 'a) '(list2 'b) '(list3 'c) '('Left '('LeftOnly 'a 'b '('a 'key1 'a 'subkey1) '('b 'key2 'b 'subkey2) '()) 'c '('a 'key1 'a 'subkey1) '('c 'key3 'c 'subkey3) '()) '()))
    (let lazy (LazyList join))

    (let res_sink (DataSink 'yt (quote plato)))
    (let world (Write! world res_sink (Key '('table (String 'Output))) lazy '('('mode 'renew))))

    (let world (Commit! world res_sink))
    (return world)
)
        )";
        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "LazyList", "Unique((a.key1,a.subkey1)(a.value1)(c.key3,c.subkey3)(c.value3))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "LazyList", "Distinct((a.key1,a.subkey1)(a.value1))");
    }

    Y_UNIT_TEST(EquiJoinLeftLeftSemi) {
        const auto s = R"(
(
    (let list1 (AsList
    (AsStruct '('key1 (Int32 '1)) '('subkey1 (Uint8 '0)) '('value1 (String 'A)))
    (AsStruct '('key1 (Int32 '7)) '('subkey1 (Uint8 '0)) '('value1 (String 'B)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '0)) '('value1 (String 'C)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '1)) '('value1 (String 'D)))
    ))

    (let list1 (AssumeUnique list1 '('key1 'subkey1) '('value1)))
    (let list1 (AssumeDistinct list1 '('key1 'subkey1) '('value1)))

    (let list2 (AsList
    (AsStruct '('key2 (Int32 '9)) '('subkey2 (Uint8 '0)) '('value2 (String 'Z)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '0)) '('value2 (String 'Y)))
    (AsStruct '('key2 (Int32 '3)) '('subkey2 (Uint8 '1)) '('value2 (String 'X)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '1)) '('value2 (String 'W)))
    (AsStruct '('key2 (Int32 '8)) '('subkey2 (Uint8 '1)) '('value2 (String 'V)))
    ))

    (let list2 (AssumeUnique list2 '('key2 'subkey2) '('value2)))
    (let list2 (AssumeDistinct list2 '('key2 'subkey2) '('value2)))

    (let list3 (AsList
    (AsStruct '('key3 (Int32 '1)) '('subkey3 (Uint8 '0)) '('value3 (String 'G)))
    (AsStruct '('key3 (Int32 '4)) '('subkey3 (Uint8 '1)) '('value3 (String 'H)))
    (AsStruct '('key3 (Int32 '2)) '('subkey3 (Uint8 '0)) '('value3 (String 'I)))
    (AsStruct '('key3 (Int32 '3)) '('subkey3 (Uint8 '1)) '('value3 (String 'J)))
    (AsStruct '('key3 (Int32 '3)) '('subkey3 (Uint8 '0)) '('value3 (String 'K)))
    ))

    (let list3 (AssumeUnique list3 '('key3 'subkey3) '('value3)))
    (let list3 (AssumeDistinct list3 '('key3 'subkey3) '('value3)))

    (let join (EquiJoin '(list1 'a) '(list2 'b) '(list3 'c) '('Left '('LeftSemi 'a 'b '('a 'key1 'a 'subkey1) '('b 'key2 'b 'subkey2) '()) 'c '('a 'key1 'a 'subkey1) '('c 'key3 'c 'subkey3) '()) '()))
    (let lazy (LazyList join))

    (let res_sink (DataSink 'yt (quote plato)))
    (let world (Write! world res_sink (Key '('table (String 'Output))) lazy '('('mode 'renew))))

    (let world (Commit! world res_sink))
    (return world)
)
        )";
        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "LazyList", "Unique((a.key1,a.subkey1)(a.value1)(c.key3,c.subkey3)(c.value3))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "LazyList", "Distinct((a.key1,a.subkey1)(a.value1))");
    }

    Y_UNIT_TEST(EquiJoinLeftRightOnly) {
        const auto s = R"(
(
    (let list1 (AsList
    (AsStruct '('key1 (Int32 '1)) '('subkey1 (Uint8 '0)) '('value1 (String 'A)))
    (AsStruct '('key1 (Int32 '7)) '('subkey1 (Uint8 '0)) '('value1 (String 'B)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '0)) '('value1 (String 'C)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '1)) '('value1 (String 'D)))
    ))

    (let list1 (AssumeUnique list1 '('key1 'subkey1) '('value1)))
    (let list1 (AssumeDistinct list1 '('key1 'subkey1) '('value1)))

    (let list2 (AsList
    (AsStruct '('key2 (Int32 '9)) '('subkey2 (Uint8 '0)) '('value2 (String 'Z)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '0)) '('value2 (String 'Y)))
    (AsStruct '('key2 (Int32 '3)) '('subkey2 (Uint8 '1)) '('value2 (String 'X)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '1)) '('value2 (String 'W)))
    (AsStruct '('key2 (Int32 '8)) '('subkey2 (Uint8 '1)) '('value2 (String 'V)))
    ))

    (let list2 (AssumeUnique list2 '('key2 'subkey2) '('value2)))
    (let list2 (AssumeDistinct list2 '('key2 'subkey2) '('value2)))

    (let list3 (AsList
    (AsStruct '('key3 (Int32 '1)) '('subkey3 (Uint8 '0)) '('value3 (String 'G)))
    (AsStruct '('key3 (Int32 '4)) '('subkey3 (Uint8 '1)) '('value3 (String 'H)))
    (AsStruct '('key3 (Int32 '2)) '('subkey3 (Uint8 '0)) '('value3 (String 'I)))
    (AsStruct '('key3 (Int32 '3)) '('subkey3 (Uint8 '1)) '('value3 (String 'J)))
    (AsStruct '('key3 (Int32 '3)) '('subkey3 (Uint8 '0)) '('value3 (String 'K)))
    ))

    (let list3 (AssumeUnique list3 '('key3 'subkey3) '('value3)))
    (let list3 (AssumeDistinct list3 '('key3 'subkey3) '('value3)))

    (let join (EquiJoin '(list1 'a) '(list2 'b) '(list3 'c) '('Left '('RightOnly 'a 'b '('a 'key1 'a 'subkey1) '('b 'key2 'b 'subkey2) '()) 'c '('b 'key2 'b 'subkey2) '('c 'key3 'c 'subkey3) '()) '()))
    (let lazy (LazyList join))

    (let res_sink (DataSink 'yt (quote plato)))
    (let world (Write! world res_sink (Key '('table (String 'Output))) lazy '('('mode 'renew))))

    (let world (Commit! world res_sink))
    (return world)
)
        )";
        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "LazyList", "Unique((b.key2,b.subkey2)(b.value2)(c.key3,c.subkey3)(c.value3))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "LazyList", "Distinct((b.key2,b.subkey2)(b.value2))");
    }

    Y_UNIT_TEST(EquiJoinLeftRightSemi) {
        const auto s = R"(
(
    (let list1 (AsList
    (AsStruct '('key1 (Int32 '1)) '('subkey1 (Uint8 '0)) '('value1 (String 'A)))
    (AsStruct '('key1 (Int32 '7)) '('subkey1 (Uint8 '0)) '('value1 (String 'B)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '0)) '('value1 (String 'C)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '1)) '('value1 (String 'D)))
    ))

    (let list1 (AssumeUnique list1 '('key1 'subkey1) '('value1)))
    (let list1 (AssumeDistinct list1 '('key1 'subkey1) '('value1)))

    (let list2 (AsList
    (AsStruct '('key2 (Int32 '9)) '('subkey2 (Uint8 '0)) '('value2 (String 'Z)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '0)) '('value2 (String 'Y)))
    (AsStruct '('key2 (Int32 '3)) '('subkey2 (Uint8 '1)) '('value2 (String 'X)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '1)) '('value2 (String 'W)))
    (AsStruct '('key2 (Int32 '8)) '('subkey2 (Uint8 '1)) '('value2 (String 'V)))
    ))

    (let list2 (AssumeUnique list2 '('key2 'subkey2) '('value2)))
    (let list2 (AssumeDistinct list2 '('key2 'subkey2) '('value2)))

    (let list3 (AsList
    (AsStruct '('key3 (Int32 '1)) '('subkey3 (Uint8 '0)) '('value3 (String 'G)))
    (AsStruct '('key3 (Int32 '4)) '('subkey3 (Uint8 '1)) '('value3 (String 'H)))
    (AsStruct '('key3 (Int32 '2)) '('subkey3 (Uint8 '0)) '('value3 (String 'I)))
    (AsStruct '('key3 (Int32 '3)) '('subkey3 (Uint8 '1)) '('value3 (String 'J)))
    (AsStruct '('key3 (Int32 '3)) '('subkey3 (Uint8 '0)) '('value3 (String 'K)))
    ))

    (let list3 (AssumeUnique list3 '('key3 'subkey3) '('value3)))
    (let list3 (AssumeDistinct list3 '('key3 'subkey3) '('value3)))

    (let join (EquiJoin '(list1 'a) '(list2 'b) '(list3 'c) '('Left '('RightSemi 'a 'b '('a 'key1 'a 'subkey1) '('b 'key2 'b 'subkey2) '()) 'c '('b 'key2 'b 'subkey2) '('c 'key3 'c 'subkey3) '()) '()))
    (let lazy (LazyList join))

    (let res_sink (DataSink 'yt (quote plato)))
    (let world (Write! world res_sink (Key '('table (String 'Output))) lazy '('('mode 'renew))))

    (let world (Commit! world res_sink))
    (return world)
)
        )";
        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "LazyList", "Unique((b.key2,b.subkey2)(b.value2)(c.key3,c.subkey3)(c.value3))");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "LazyList", "Distinct((b.key2,b.subkey2)(b.value2))");
    }

    Y_UNIT_TEST(EquiJoinFlatten) {
        const auto s = R"(
(
    (let list1 (AsList
    (AsStruct '('key1 (Int32 '1)) '('subkey1 (Uint8 '0)) '('value1 (String 'A)))
    (AsStruct '('key1 (Int32 '7)) '('subkey1 (Uint8 '0)) '('value1 (String 'B)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '0)) '('value1 (String 'C)))
    (AsStruct '('key1 (Int32 '4)) '('subkey1 (Uint8 '1)) '('value1 (String 'D)))
    ))

    (let list1 (AssumeUnique list1 '('key1 'subkey1) '('value1)))
    (let list1 (AssumeDistinct list1 '('key1 'subkey1) '('value1)))

    (let list2 (AsList
    (AsStruct '('key2 (Int32 '9)) '('subkey2 (Uint8 '0)) '('value2 (String 'Z)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '0)) '('value2 (String 'Y)))
    (AsStruct '('key2 (Int32 '3)) '('subkey2 (Uint8 '1)) '('value2 (String 'X)))
    (AsStruct '('key2 (Int32 '4)) '('subkey2 (Uint8 '1)) '('value2 (String 'W)))
    (AsStruct '('key2 (Int32 '8)) '('subkey2 (Uint8 '1)) '('value2 (String 'V)))
    ))

    (let list2 (AssumeUnique list2 '('key2 'subkey2) '('value2)))
    (let list2 (AssumeDistinct list2 '('key2 'subkey2) '('value2)))

    (let list3 (AsList
    (AsStruct '('key3 (Int32 '1)) '('subkey3 (Uint8 '0)) '('value3 (String 'G)))
    (AsStruct '('key3 (Int32 '4)) '('subkey3 (Uint8 '1)) '('value3 (String 'H)))
    (AsStruct '('key3 (Int32 '2)) '('subkey3 (Uint8 '0)) '('value3 (String 'I)))
    (AsStruct '('key3 (Int32 '3)) '('subkey3 (Uint8 '1)) '('value3 (String 'J)))
    (AsStruct '('key3 (Int32 '3)) '('subkey3 (Uint8 '0)) '('value3 (String 'K)))
    ))

    (let list3 (AssumeUnique list3 '('key3 'subkey3) '('value3)))
    (let list3 (AssumeDistinct list3 '('key3 'subkey3) '('value3)))

    (let join (EquiJoin '(list1 'a) '(list2 'b) '(list3 'c) '('Inner '('Inner 'a 'b '('a 'key1 'a 'subkey1) '('b 'key2 'b 'subkey2) '()) 'c '('b 'key2 'b 'subkey2) '('c 'key3 'c 'subkey3) '()) '('('flatten))))
    (let lazy (LazyList join))

    (let res_sink (DataSink 'yt (quote plato)))
    (let world (Write! world res_sink (Key '('table (String 'Output))) lazy '('('mode 'renew))))

    (let world (Commit! world res_sink))
    (return world)
)
        )";
        TExprContext exprCtx;
        const auto exprRoot = ParseAndAnnotate(s, exprCtx);
        CheckConstraint<TUniqueConstraintNode>(exprRoot, "LazyList", "");
        CheckConstraint<TDistinctConstraintNode>(exprRoot, "LazyList", "");
    }
}

} // namespace NYql
