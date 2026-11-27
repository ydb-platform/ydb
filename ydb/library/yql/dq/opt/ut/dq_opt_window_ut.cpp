#include <ydb/library/yql/dq/opt/dq_opt_log.h>

#include <yql/essentials/core/type_ann/type_ann_expr.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_user_data_storage.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/providers/common/udf_resolve/yql_simple_udf_resolver.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql::NDq {
namespace {

struct TWindowFixture {
    TExprContext Ctx;
    NKikimr::NMiniKQL::IFunctionRegistry::TPtr FunctionRegistry;
    TTypeAnnotationContext Types;

    TWindowFixture() {
        auto registry = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry())->Clone();
        NKikimr::NMiniKQL::FillStaticModules(*registry);
        FunctionRegistry = std::move(registry);
        Types.UdfResolver = NCommon::CreateSimpleUdfResolver(FunctionRegistry.Get());
        Types.UserDataStorage = MakeIntrusive<TUserDataStorage>(nullptr, TUserDataTable(), nullptr, nullptr);
    }

    TExprNode::TPtr Parse(TStringBuf expression) {
        const auto ast = ParseAst(TStringBuilder() << R"((
            (let data (AsList (AsStruct
                '('key (Uint32 '1))
                '('value (Int32 '2))
                '('unused_fat_col (String 'payload)))))
            (let rowType (ListItemType (TypeOf data)))
            (let sumTraits (WindowTraits (StructType '('value (DataType 'Int32)))
                (lambda '(row) (Member row 'value))
                (lambda '(row state) (AggrAdd state (Member row 'value)))
                (lambda '(row state) (Void))
                (lambda '(state) state)
                (Int32 '0)))
            (let distinctTraits (WindowTraits (DataType 'Int32)
                (lambda '(value) value)
                (lambda '(value state) (AggrAdd state value))
                (lambda '(value state) (Void))
                (lambda '(state) state)
                (Int32 '0)))
            (let wholeRowTraits (WindowTraits rowType
                (lambda '(row) row)
                (lambda '(row state) row)
                (lambda '(row state) (Void))
                (lambda '(state) state)
                (Null)))
            (let sortTraits (SortTraits (ListType rowType) (Bool 'true) (lambda '(row) (Member row 'key))))
            (let wholeRowSortTraits (SortTraits (ListType rowType) (Bool 'true) (lambda '(row) (StablePickle row))))
            (let fullFrame '('('begin (Void)) '('end (Void)) '('sortSpec (Void))))
            (let orderedFullFrame '('('begin (Void)) '('end (Void)) '('sortSpec sortTraits)))
            (let wholeRowSortFrame '('('begin (Void)) '('end (Void)) '('sortSpec wholeRowSortTraits)))
            (let compactFrame '('('begin (Void)) '('end (Void)) '('sortSpec (Void)) '('compact (Void))))
            (let runningFrame '('('begin (Void)) '('end (Int32 '0)) '('sortSpec sortTraits)))
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

    TExprNode::TPtr Expand(const TExprNode::TPtr& window, bool narrow = true) {
        auto result = DqExpandWindowFunctions(NNodes::TExprBase(window), Ctx, Types, /*enforceCompact=*/false, narrow).Ptr();
        CheckArguments(*result);
        Annotate(result);
        UNIT_ASSERT_C(IsSameAnnotation(*window->GetTypeAnn(), *result->GetTypeAnn()), Ctx.IssueManager.GetIssues().ToString());
        return result;
    }
};

TExprNode::TPtr FindPartition(const TExprNode::TPtr& root) {
    const auto partition = FindNode(root, [](const TExprNode::TPtr& node) {
        return node->IsCallable("PartitionsByKeys");
    });
    UNIT_ASSERT(partition);
    return partition;
}

void CheckInputFields(const TExprNode::TPtr& partition, std::initializer_list<TStringBuf> expected) {
    const auto* rowType = GetSeqItemType(partition->Head().GetTypeAnn())->Cast<TStructExprType>();
    UNIT_ASSERT_VALUES_EQUAL(rowType->GetSize(), expected.size());
    for (auto name : expected) {
        UNIT_ASSERT_C(rowType->FindItem(name), name);
    }
}

} // namespace

Y_UNIT_TEST_SUITE(DqNarrowWindowAggregateInput) {
    Y_UNIT_TEST(FullFrameProjectsOnlyAggregateInput) {
        TWindowFixture fixture;
        const auto window = fixture.Parse("(CalcOverWindow data '('key) (Void) '((WinOnRows fullFrame '('total sumTraits))))");
        const auto expanded = fixture.Expand(window);
        const auto partition = FindPartition(expanded);
        CheckInputFields(partition, {"key", "value"});
        UNIT_ASSERT(partition->Head().IsCallable("ExtractMembers"));
        bool keepsOriginalInput = false;
        VisitExpr(expanded, [&](const TExprNode::TPtr& node) {
            keepsOriginalInput |= node.Get() == window->Child(0);
            return !keepsOriginalInput;
        });
        UNIT_ASSERT(keepsOriginalInput);
    }

    Y_UNIT_TEST(FlagOffKeepsAggregateInput) {
        TWindowFixture fixture;
        const auto window = fixture.Parse("(CalcOverWindow data '('key) (Void) '((WinOnRows fullFrame '('total sumTraits))))");
        CheckInputFields(FindPartition(fixture.Expand(window, false)), {"key", "value", "unused_fat_col"});
    }

    Y_UNIT_TEST(OrderedFrameKeepsSortColumn) {
        TWindowFixture fixture;
        const auto window = fixture.Parse("(CalcOverWindow data '() sortTraits '((WinOnRows orderedFullFrame '('total sumTraits))))");
        CheckInputFields(FindPartition(fixture.Expand(window)), {"key", "value"});
    }

    Y_UNIT_TEST(DistinctScalarColumnIsKept) {
        TWindowFixture fixture;
        const auto window = fixture.Parse("(CalcOverWindow data '('key) (Void) '((WinOnRows fullFrame '('total distinctTraits 'value))))");
        CheckInputFields(FindPartition(fixture.Expand(window)), {"key", "value"});
    }

    Y_UNIT_TEST(DistinctStructColumnIsKept) {
        TWindowFixture fixture;
        const auto window = fixture.Parse(R"((CalcOverWindow
            (Map data (lambda '(row) (AddMember row 'struct_value (AsStruct '('nested (Member row 'value))))))
            '('key) (Void) '((WinOnRows fullFrame '('total
                (WindowTraits (StructType '('nested (DataType 'Int32)))
                    (lambda '(value) (Member value 'nested))
                    (lambda '(value state) (AggrAdd state (Member value 'nested)))
                    (lambda '(value state) (Void))
                    (lambda '(state) state)
                    (Int32 '0))
                'struct_value)))))");
        CheckInputFields(FindPartition(fixture.Expand(window)), {"key", "struct_value"});
    }

    Y_UNIT_TEST(NullablePartitionKeyKeepsSyntheticJoinKey) {
        TWindowFixture fixture;
        const auto window = fixture.Parse(R"((CalcOverWindow
            (Map data (lambda '(row) (ReplaceMember row 'key (Just (Member row 'key)))))
            '('key) (Void) '((WinOnRows fullFrame '('total sumTraits)))))");
        CheckInputFields(FindPartition(fixture.Expand(window)), {"_yql_CalcOverWindowJoinKey0", "value"});
    }

    Y_UNIT_TEST(WholeRowSortKeepsAllFields) {
        TWindowFixture fixture;
        const auto window = fixture.Parse(R"((CalcOverWindow data '()
            wholeRowSortTraits
            '((WinOnRows wholeRowSortFrame '('total sumTraits)))))");
        CheckInputFields(FindPartition(fixture.Expand(window)), {"key", "value", "unused_fat_col"});
    }

    Y_UNIT_TEST(WholeRowAggregateKeepsAllFields) {
        TWindowFixture fixture;
        const auto window = fixture.Parse("(CalcOverWindow data '('key) (Void) '((WinOnRows fullFrame '('total wholeRowTraits))))");
        CheckInputFields(FindPartition(fixture.Expand(window)), {"key", "value", "unused_fat_col"});
    }

    Y_UNIT_TEST(ExistingPartitionIsNotRewritten) {
        TWindowFixture fixture;
        const auto window = fixture.Parse(R"((CalcOverWindow
            (PartitionsByKeys data (lambda '(row) (Member row 'key)) (Void) (Void)
                (lambda '(stream) (Condense1 stream
                    (lambda '(row) row)
                    (lambda '(row state) (Bool 'false))
                    (lambda '(row state) row))))
            '('key) (Void) '((WinOnRows fullFrame '('total sumTraits)))))");
        const auto originalPartition = window->HeadPtr();
        const auto expanded = fixture.Expand(window);
        bool foundOriginal = false;
        VisitExpr(expanded, [&](const TExprNode::TPtr& node) {
            foundOriginal |= node == originalPartition;
            return true;
        });
        UNIT_ASSERT(foundOriginal);
        CheckInputFields(originalPartition, {"key", "value", "unused_fat_col"});
    }

    Y_UNIT_TEST(CompactFrameKeepsAllFields) {
        TWindowFixture fixture;
        const auto window = fixture.Parse("(CalcOverWindow data '('key) (Void) '((WinOnRows compactFrame '('total sumTraits))))");
        CheckInputFields(FindPartition(fixture.Expand(window)), {"key", "value", "unused_fat_col"});
    }

    Y_UNIT_TEST(RunningFrameKeepsAllFields) {
        TWindowFixture fixture;
        const auto window = fixture.Parse("(CalcOverWindow data '('key) sortTraits '((WinOnRows runningFrame '('total sumTraits))))");
        CheckInputFields(FindPartition(fixture.Expand(window)), {"key", "value", "unused_fat_col"});
    }

    Y_UNIT_TEST(SessionWindowKeepsAllFields) {
        TWindowFixture fixture;
        const auto window = fixture.Parse(R"((CalcOverSessionWindow data '('key) sortTraits
            '((WinOnRows orderedFullFrame '('total sumTraits)))
            (SessionWindowTraits (ListType rowType) sortTraits
                (lambda '(row) (Member row 'key))
                (lambda '(prev next) (Bool 'true)))
            '()))");
        const auto expanded = fixture.Expand(window);
        bool foundPartition = false;
        VisitExpr(expanded, [&](const TExprNode::TPtr& node) {
            if (node->IsCallable("PartitionsByKeys")) {
                foundPartition = true;
                const auto* rowType = GetSeqItemType(node->Head().GetTypeAnn())->Cast<TStructExprType>();
                UNIT_ASSERT(rowType->FindItem("unused_fat_col"));
            }
            return true;
        });
        UNIT_ASSERT(foundPartition);
    }
}

} // namespace NYql::NDq
