#include <ydb/library/yql/dq/opt/dq_opt_phy.h>

#include <yql/essentials/core/type_ann/type_ann_expr.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/core/yql_opt_window.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql::NDq {
namespace {

struct TFixture {
    TExprContext Ctx;
    TTypeAnnotationContext Types;

    TExprNode::TPtr Parse(TStringBuf expression) {
        const auto ast = ParseAst(TStringBuilder() << R"((
            (let data (AsList (AsStruct
                '('key (Uint32 '1))
                '('value (Int32 '2))
                '('unused_fat_col (String 'payload)))))
            (let rowType (ListItemType (TypeOf data)))
            (let sumTraits (WindowTraits (StructType '('value (DataType 'Int32)))
                (lambda '(row) (Member row 'value))
                (lambda '(row state) (+ state (Member row 'value)))
                (lambda '(row state) (Void))
                (lambda '(state) state)
                (Int32 '0)))
            (let countTraits (WindowTraits (StructType)
                (lambda '(row) (Uint64 '1))
                (lambda '(row state) (+ state (Uint64 '1)))
                (lambda '(row state) (Void))
                (lambda '(state) state)
                (Uint64 '0)))
            (let wholeRowTraits (WindowTraits rowType
                (lambda '(row) (AsList row))
                (lambda '(row state) (Append state row))
                (lambda '(row state) (Void))
                (lambda '(state) state)
                (List (ListType rowType))))
            (let sortTraits (SortTraits (ListType rowType) (Bool 'true) (lambda '(row) (Member row 'key))))
            (let fullFrame '('('begin (Void)) '('end (Void)) '('sortSpec (Void))))
            (let orderedFullFrame '('('begin (Void)) '('end (Void)) '('sortSpec sortTraits)))
            (let compactFrame '('('begin (Void)) '('end (Void)) '('sortSpec (Void)) '('compact (Void))))
            (let sumInit (lambda '(row) (Member row 'value)))
            (let noSwitch (lambda '(row state) (Bool 'false)))
            (let sumUpdate (lambda '(row state) (+ state (Member row 'value))))
            (let keySelector (lambda '(row) (Member row 'key)))
            (return )" << expression << "))");
        UNIT_ASSERT_C(ast.IsOk(), ast.Issues.ToString());
        TExprNode::TPtr result;
        UNIT_ASSERT_C(CompileExpr(*ast.Root, result, Ctx, nullptr, nullptr), Ctx.IssueManager.GetIssues().ToString());
        Annotate(result);
        return result;
    }

    void Annotate(TExprNode::TPtr& node) {
        Ctx.RepeatTransformCounter = 0;
        UNIT_ASSERT_C(InstantAnnotateTypes(node, Ctx, /*wholeProgram=*/false, Types), Ctx.IssueManager.GetIssues().ToString());
    }

    // ExpandCalcOverWindow, then CastStruct expansion as done by common_opt before the DQ physical optimizer.
    TExprNode::TPtr ExpandWindow(const TExprNode::TPtr& window) {
        auto result = ExpandCalcOverWindow(window, Ctx, Types);
        Annotate(result);
        for (;;) {
            TOptimizeExprSettings settings(nullptr);
            const auto status = OptimizeExpr(result, result, [](const TExprNode::TPtr& node, TExprContext& ctx) {
                return node->IsCallable("CastStruct") ? ExpandCastStruct(node, ctx) : node;
            }, Ctx, settings);
            UNIT_ASSERT(status.Level != IGraphTransformer::TStatus::Error);
            Annotate(result);
            if (status.Level == IGraphTransformer::TStatus::Ok) {
                break;
            }
        }
        UNIT_ASSERT(IsSameAnnotation(*window->GetTypeAnn(), *result->GetTypeAnn()));
        return result;
    }

    // Applies the rule to the first PartitionsByKeys. Returns nullptr if the rule did not change it.
    TExprNode::TPtr Narrow(const TExprNode::TPtr& root) {
        const auto partition = FindPartition(root);
        const auto narrowed = DqNarrowPartitionsByKeysInput(NNodes::TExprBase(partition), Ctx).Ptr();
        if (narrowed == partition) {
            return nullptr;
        }
        UNIT_ASSERT(narrowed->Head().IsCallable("ExtractMembers"));
        auto result = Ctx.ReplaceNode(TExprNode::TPtr(root), *partition, narrowed);
        CheckArguments(*result);
        Annotate(result);
        UNIT_ASSERT_C(IsSameAnnotation(*root->GetTypeAnn(), *result->GetTypeAnn()), *result->GetTypeAnn());
        // The rewritten node is stable: applying the rule again changes nothing.
        const auto again = FindPartition(result);
        UNIT_ASSERT_EQUAL(DqNarrowPartitionsByKeysInput(NNodes::TExprBase(again), Ctx).Raw(), again.Get());
        return result;
    }

    static TExprNode::TPtr FindPartition(const TExprNode::TPtr& root) {
        const auto partition = FindNode(root, [](const TExprNode::TPtr& node) {
            return node->IsCallable("PartitionsByKeys");
        });
        UNIT_ASSERT(partition);
        return partition;
    }

    static void CheckInputFields(const TExprNode::TPtr& root, std::initializer_list<TStringBuf> expected) {
        const auto* rowType = GetSeqItemType(FindPartition(root)->Head().GetTypeAnn())->Cast<TStructExprType>();
        UNIT_ASSERT_VALUES_EQUAL_C(rowType->GetSize(), expected.size(), *rowType);
        for (auto name : expected) {
            UNIT_ASSERT_C(rowType->FindItem(name), name);
        }
    }

    void CheckNotNarrowed(TStringBuf expression) {
        UNIT_ASSERT(!Narrow(Parse(expression)));
    }
};

TString PartitionsByKeys(TStringBuf keySelector, TStringBuf handler) {
    return TStringBuilder() << "(PartitionsByKeys data " << keySelector << " (Void) (Void) " << handler << ")";
}

} // namespace

Y_UNIT_TEST_SUITE(DqNarrowPartitionsByKeysInput) {
    Y_UNIT_TEST(WindowFullFrameDropsUnusedColumn) {
        TFixture fixture;
        const auto expanded = fixture.ExpandWindow(fixture.Parse(
            "(CalcOverWindow data '('key) (Void) '((WinOnRows fullFrame '('total sumTraits))))"));
        const auto narrowed = fixture.Narrow(expanded);
        UNIT_ASSERT(narrowed);
        TFixture::CheckInputFields(narrowed, {"key", "value"});
    }

    Y_UNIT_TEST(WindowOrderedFullFrameKeepsSortKey) {
        TFixture fixture;
        const auto expanded = fixture.ExpandWindow(fixture.Parse(
            "(CalcOverWindow data '() sortTraits '((WinOnRows orderedFullFrame '('total sumTraits))))"));
        const auto narrowed = fixture.Narrow(expanded);
        UNIT_ASSERT(narrowed);
        TFixture::CheckInputFields(narrowed, {"key", "value"});
    }

    Y_UNIT_TEST(WindowCountWithoutKeysKeepsOneColumn) {
        TFixture fixture;
        const auto expanded = fixture.ExpandWindow(fixture.Parse(
            "(CalcOverWindow data '() (Void) '((WinOnRows fullFrame '('rows countTraits))))"));
        const auto narrowed = fixture.Narrow(expanded);
        UNIT_ASSERT(narrowed);
        // No field is read; the first fixed-size column is kept instead of an empty struct.
        TFixture::CheckInputFields(narrowed, {"key"});
    }

    Y_UNIT_TEST(WindowWholeRowAggregateIsNotNarrowed) {
        TFixture fixture;
        const auto expanded = fixture.ExpandWindow(fixture.Parse(
            "(CalcOverWindow data '('key) (Void) '((WinOnRows fullFrame '('rows wholeRowTraits))))"));
        UNIT_ASSERT(!fixture.Narrow(expanded));
    }

    Y_UNIT_TEST(WindowCompactFrameIsNotNarrowed) {
        TFixture fixture;
        const auto expanded = fixture.ExpandWindow(fixture.Parse(
            "(CalcOverWindow data '('key) (Void) '((WinOnRows compactFrame '('total sumTraits))))"));
        UNIT_ASSERT(!fixture.Narrow(expanded));
    }

    Y_UNIT_TEST(PlainCondense1DropsUnusedColumn) {
        TFixture fixture;
        const auto narrowed = fixture.Narrow(fixture.Parse(PartitionsByKeys("keySelector",
            "(lambda '(stream) (Condense1 stream sumInit noSwitch sumUpdate))")));
        UNIT_ASSERT(narrowed);
        TFixture::CheckInputFields(narrowed, {"key", "value"});
    }

    Y_UNIT_TEST(IsKeySwitchReadsOnlyRowKey) {
        TFixture fixture;
        const auto narrowed = fixture.Narrow(fixture.Parse(PartitionsByKeys("keySelector", R"(
            (lambda '(stream) (Condense1 stream
                (lambda '(row) (AsStruct '('key (Member row 'key)) '('sum (Member row 'value))))
                (lambda '(row state) (IsKeySwitch row state
                    (lambda '(item) (Member item 'key))
                    (lambda '(item) (Member item 'key))))
                (lambda '(row state) (AsStruct '('key (Member state 'key)) '('sum (+ (Member state 'sum) (Member row 'value)))))))
        )")));
        UNIT_ASSERT(narrowed);
        TFixture::CheckInputFields(narrowed, {"key", "value"});
    }

    // #59253: AGGREGATE_LIST(TableRow()) takes the whole row, only key looks like a Member read.
    Y_UNIT_TEST(WholeRowInInitIsNotNarrowed) {
        TFixture fixture;
        fixture.CheckNotNarrowed(PartitionsByKeys("(lambda '(row) (Uint32 '0))", R"(
            (lambda '(stream) (Condense1 stream
                (lambda '(row) (AsStruct '('key (Member row 'key)) '('rows (AsList row))))
                noSwitch
                (lambda '(row state) (AsStruct '('key (Member state 'key)) '('rows (Append (Member state 'rows) row))))))
        )"));
    }

    Y_UNIT_TEST(WholeRowInUpdateIsNotNarrowed) {
        TFixture fixture;
        fixture.CheckNotNarrowed(PartitionsByKeys("keySelector", R"(
            (lambda '(stream) (Condense1 stream
                (lambda '(row) row)
                noSwitch
                (lambda '(row state) row)))
        )"));
    }

    Y_UNIT_TEST(WholeRowInSwitchIsNotNarrowed) {
        TFixture fixture;
        fixture.CheckNotNarrowed(PartitionsByKeys("keySelector", R"(
            (lambda '(stream) (Condense1 stream sumInit
                (lambda '(row state) (Exists (Just row)))
                sumUpdate))
        )"));
    }

    Y_UNIT_TEST(ChopperIsNotNarrowed) {
        TFixture fixture;
        fixture.CheckNotNarrowed(PartitionsByKeys("keySelector", R"(
            (lambda '(stream) (Chopper (ToStream stream) keySelector (lambda '(key row) (Bool 'false))
                (lambda '(key group) (Condense1 group sumInit noSwitch sumUpdate))))
        )"));
    }

    Y_UNIT_TEST(StreamNotDirectlyFoldedIsNotNarrowed) {
        TFixture fixture;
        fixture.CheckNotNarrowed(PartitionsByKeys("keySelector",
            "(lambda '(stream) (Condense1 (OrderedMap stream (lambda '(row) row)) sumInit noSwitch sumUpdate))"));
    }

    Y_UNIT_TEST(StreamPassThroughIsNotNarrowed) {
        TFixture fixture;
        fixture.CheckNotNarrowed(PartitionsByKeys("keySelector", "(lambda '(stream) stream)"));
    }

    Y_UNIT_TEST(SecondStreamUseIsNotNarrowed) {
        TFixture fixture;
        fixture.CheckNotNarrowed(PartitionsByKeys("keySelector", R"(
            (lambda '(stream) (Condense1 stream
                (lambda '(row) (Take stream (Uint64 '1)))
                noSwitch
                (lambda '(row state) state)))
        )"));
    }

    Y_UNIT_TEST(AllFieldsUsedIsNotNarrowed) {
        TFixture fixture;
        fixture.CheckNotNarrowed(PartitionsByKeys("keySelector", R"(
            (lambda '(stream) (Condense1 stream
                (lambda '(row) (Size (Member row 'unused_fat_col)))
                noSwitch
                (lambda '(row state) (+ state (Size (Member row 'unused_fat_col))))))
        )"));
    }

    Y_UNIT_TEST(AlreadyNarrowInputIsNotRewrittenAgain) {
        TFixture fixture;
        fixture.CheckNotNarrowed(R"((PartitionsByKeys (ExtractMembers data '('key 'value)) keySelector (Void) (Void)
            (lambda '(stream) (Condense1 stream sumInit noSwitch sumUpdate))))");
    }
}

} // namespace NYql::NDq
