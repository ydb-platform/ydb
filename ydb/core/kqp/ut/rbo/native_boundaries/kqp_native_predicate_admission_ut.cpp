#include "../kqp_rbo_test_helpers.h"
#include <ydb/core/kqp/opt/rbo/frontend_lowering.h>
#include <ydb/core/kqp/opt/rbo/frontend/predicate.h>
#include <yql/essentials/core/extract_predicate/extract_predicate.h>
#include <yql/essentials/core/services/yql_transform_pipeline.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <util/string/cast.h>

namespace NKikimr::NKqp {
namespace {
using namespace NFrontend;
using K = ETypeKind;
using O = EOpcode;

struct TPredicateRangeFixture : NTests::TIdTestContext {
    explicit TPredicateRangeFixture(TType keyType) : KeyType(keyType) {
        using namespace NYql;
        auto table = MakeIntrusive<TKikimrTableMetadata>();
        table->Name = "/Root/table"; table->DoesExist = true;
        table->Kind = EKikimrTableKind::Datashard; table->KeyColumnNames = {"id"};
        table->Columns["id"].Type = TString(keyType.Required().ToString());
        table->Columns["id"].NotNull = !keyType.OptionalDepth;
        table->Columns["payload"].Type = "Int64"; table->Columns["payload"].NotNull = true;
        Tables->GetOrAddTable("ut", "/Root", table->Name).Metadata = table;
        Source = Read({Key, Payload});
        Source->TableRef.NativeCatalog.emplace(TCatalogTable{std::string(table->Name), {{"id", keyType}, {"payload", {K::Int64}}}});
        SetType(*Source);
        Source->NativeSchema->Row[Key] = keyType;
        Source->NativeSchema->Row[Payload] = {K::Int64};
        RowType = ExprCtx.MakeType<TStructExprType>(TVector<const TItemExprType*>{
            ExprCtx.MakeType<TItemExprType>("id", ExportNativeType(keyType, ExprCtx)),
            ExprCtx.MakeType<TItemExprType>("payload", ExportNativeType({K::Int64}, ExprCtx))});
        Row = ExprCtx.NewArgument(Pos, "row"); Row->SetTypeAnn(RowType);
        Row->AddConstraint(ExprCtx.MakeConstraint<TEmptyConstraintNode>());
        KeyNode = ExprCtx.NewCallable(Pos, "Member", {Row, ExprCtx.NewAtom(Pos, "id")});
        PayloadNode = ExprCtx.NewCallable(Pos, "Member", {Row, ExprCtx.NewAtom(Pos, "payload")});
    }

    NYql::TExprNode::TPtr Bool(bool value) {
        return ExprCtx.NewCallable(Pos, "Bool", {ExprCtx.NewAtom(Pos, value ? "true" : "false")});
    }
    NYql::TExprNode::TPtr Integer(std::int64_t value) {
        return ExprCtx.NewCallable(Pos, "Int64", {ExprCtx.NewAtom(Pos, ToString(value))});
    }
    void Check(TExprId native, NYql::TExprNode::TPtr legacy, TStringBuf label, TString& mismatches) {
        using namespace NYql;
        auto& arena = *Props.NativeExpressions;
        native = arena.FoldConstants(std::span(&native, 1)).front();
        native = PrepareFilterPredicate(arena, native);
        // Match the frozen pre-PR new-RBO route: type the scalar and then
        // apply push_ranges' explicit Exists-over-required-column adapter.
        // Common SQL optimization does not run on this route before RBO.
        auto types = MakeIntrusive<TTypeAnnotationContext>();
        auto parameter = ExprCtx.NewCallable(Pos, "Parameter", {
            ExprCtx.NewAtom(Pos, "__range_row"), ExpandType(Pos, *RowType, ExprCtx)});
        auto normalized = ExprCtx.NewCallable(Pos, "Coalesce", {
            ExprCtx.ReplaceNode(std::move(legacy), *Row, parameter), Bool(false)});
        auto common = TTransformationPipeline(types).AddTypeAnnotationTransformer()
            .AddPostTypeAnnotation(/*forSubGraph=*/true).Build(false);
        UNIT_ASSERT_VALUES_EQUAL_C(InstantTransform(*common, normalized, ExprCtx),
            IGraphTransformer::TStatus::Ok, label << ": " << ExprCtx.IssueManager.GetIssues().ToString());
        UNIT_ASSERT_C(OptimizeExpr(normalized, normalized,
            [&](const NYql::TExprNode::TPtr& node, TExprContext& ctx) {
                return node->IsCallable("Exists") ? OptimizeExists(node, ctx, *types) : node;
            }, ExprCtx, TOptimizeExprSettings(types.Get())) != IGraphTransformer::TStatus::Error, label);
        UNIT_ASSERT_C(InstantAnnotateTypes(normalized, ExprCtx, false, *types), label);
        UNIT_ASSERT_C(OptimizeExpr(normalized, normalized,
            [&](const NYql::TExprNode::TPtr& node, TExprContext&) {
                return node->IsCallable("Parameter") && node->Head().Content() == "__range_row" ? Row : node;
            }, ExprCtx, TOptimizeExprSettings(types.Get())) != IGraphTransformer::TStatus::Error, label);
        auto lambda = ExprCtx.NewLambda(Pos, ExprCtx.NewArguments(Pos, {Row}), std::move(normalized));
        TPredicateExtractorSettings settings;
        settings.HaveNextValueCallable = true; settings.BuildLiteralRange = true;
        auto extractor = MakePredicateRangeExtractor(settings);
        THashSet<TString> possible;
        UNIT_ASSERT_C(extractor->Prepare(lambda, *RowType, possible, ExprCtx, *types), label);
        const auto expected = extractor->BuildComputeNode({"id"}, ExprCtx, *types);
        const auto& remaining = expected.PrunedLambda->Tail();
        const bool expectedResidual = !(remaining.IsCallable("Bool") && remaining.Head().Content() == "true");
        auto filter = MakeIntrusive<TOpFilter>(Source, Pos, TExpression::FromNative(Props.NativeExpressions, native, &Props));
        const auto nodes = ExprCtx.NodeAllocationCounter, annotations = ExprCtx.TypeNodes.size();
        const auto result = TPushRangesRule(NYql::EStorageType::RowStorage).SimpleMatchAndApply(filter, RboCtx, Props);
        UNIT_ASSERT_VALUES_EQUAL(ExprCtx.NodeAllocationCounter, nodes);
        UNIT_ASSERT_VALUES_EQUAL(ExprCtx.TypeNodes.size(), annotations);
        const bool residual = result->Kind == EOperator::Filter;
        const auto source = residual ? result->GetChildren().front() : result;
        const auto& actual = CastOperator<TOpRead>(*source).RangeInfo;
        const auto used = actual ? actual->UsedPrefixLen : 0;
        const auto points = actual ? actual->PointPrefixLen : 0;
        if (used != expected.UsedPrefixLen || points != expected.PointPrefixLen || residual != expectedResidual) {
            mismatches += TStringBuilder() << label << " used=" << used << '/' << expected.UsedPrefixLen
                << " points=" << points << '/' << expected.PointPrefixLen << " residual=" << residual << '/' << expectedResidual << '\n';
        }
        LastRead = CastOperator<TOpRead>(source);
    }

    TType KeyType;
    TInfoUnitId Key = Id("id"), Payload = Id("payload");
    TIntrusivePtr<TOpRead> Source, LastRead;
    const NYql::TStructExprType* RowType = nullptr;
    NYql::TExprNode::TPtr Row, KeyNode, PayloadNode;
};
}

Y_UNIT_TEST_SUITE(KqpNativePredicateAdmission) {
    Y_UNIT_TEST(NullTestsMatchLegacyPrefixAndResidualContracts) {
        TString mismatches;
        for (const auto kind : {K::Int64, K::Double, K::String}) for (const bool nullable : {false, true}) {
            for (const bool exists : {false, true}) {
                TPredicateRangeFixture f({kind, nullable});
                auto& arena = *f.Props.NativeExpressions;
                const auto key = arena.Column(f.Key, f.KeyType);
                auto native = arena.Unary(exists ? O::IsNotNull : O::IsNull, key);
                auto legacy = f.ExprCtx.NewCallable(f.Pos, "Exists", {f.KeyNode});
                if (!exists) legacy = f.ExprCtx.NewCallable(f.Pos, "Not", {legacy});
                const TString label = TStringBuilder() << f.KeyType.ToString() << " exists=" << exists;
                f.Check(native, legacy, label, mismatches);
                if (kind == K::Double && nullable && exists && f.LastRead->RangeInfo) {
                    const auto& range = f.LastRead->RangeInfo->NativeRanges->front();
                    UNIT_ASSERT(range.Upper.Values.empty()); // NaN is not NULL.
                    UNIT_ASSERT_VALUES_EQUAL(range.Lower.Values.size(), 1);
                    UNIT_ASSERT(!range.Lower.Inclusive);
                }
            }
        }
        UNIT_ASSERT_C(mismatches.empty(), mismatches);
    }

    Y_UNIT_TEST(OrderedComparisonsMatchLegacyPrefixAndResidualContracts) {
        TString mismatches;
        for (const bool nullable : {false, true}) for (const bool negated : {false, true})
        for (const bool reversed : {false, true})
        for (const auto& [op, name] : {std::pair{O::Equal, "=="}, {O::NotEqual, "!="},
                {O::Less, "<"}, {O::LessEqual, "<="}, {O::Greater, ">"}, {O::GreaterEqual, ">="}}) {
            TPredicateRangeFixture f({K::Int64, nullable});
            auto& arena = *f.Props.NativeExpressions;
            const auto key = arena.Column(f.Key, f.KeyType);
            const auto bound = arena.Literal({K::Int64}, std::int64_t{2});
            auto native = arena.Binary(op, reversed ? bound : key, reversed ? key : bound);
            auto legacy = f.ExprCtx.NewCallable(f.Pos, name, reversed
                ? NYql::TExprNode::TListType{f.Integer(2), f.KeyNode}
                : NYql::TExprNode::TListType{f.KeyNode, f.Integer(2)});
            if (negated) {
                native = arena.Unary(O::Not, native);
                legacy = f.ExprCtx.NewCallable(f.Pos, "Not", {legacy});
            }
            const TString label = TStringBuilder() << name << " nullable=" << nullable
                << " negated=" << negated << " reversed=" << reversed;
            f.Check(native, legacy, label, mismatches);
        }
        UNIT_ASSERT_C(mismatches.empty(), mismatches);
    }

    Y_UNIT_TEST(ClosedConditionsMatchLegacyPrefixAndResidualContracts) {
        TString mismatches;
        for (const bool nullable : {false, true}) for (unsigned variant = 0; variant < 4; ++variant) {
            TPredicateRangeFixture f({K::Int64, nullable});
            auto& arena = *f.Props.NativeExpressions;
            auto native = arena.Parameter("flag", {K::Bool});
            auto legacy = f.ExprCtx.NewCallable(f.Pos, "Parameter", {f.ExprCtx.NewAtom(f.Pos, "flag"),
                NYql::ExpandType(f.Pos, *ExportNativeType({K::Bool}, f.ExprCtx), f.ExprCtx)});
            if (variant == 1) {
                native = arena.Binary(O::Or, native, arena.Binary(O::GreaterEqual,
                    arena.Column(f.Key, f.KeyType), arena.Literal({K::Int64}, std::int64_t{2})));
                legacy = f.ExprCtx.NewCallable(f.Pos, "Or", {legacy,
                    f.ExprCtx.NewCallable(f.Pos, ">=", {f.KeyNode, f.Integer(2)})});
            } else if (variant == 2) {
                native = arena.Binary(O::And, arena.Binary(O::Equal,
                    arena.Column(f.Payload, {K::Int64}), arena.Literal({K::Int64}, std::int64_t{7})), native);
                legacy = f.ExprCtx.NewCallable(f.Pos, "And", {
                    f.ExprCtx.NewCallable(f.Pos, "==", {f.PayloadNode, f.Integer(7)}), legacy});
            } else if (variant == 3) {
                native = arena.Ensure(arena.Literal({K::Bool}, true), arena.Literal({K::Bool}, false), "checked closed");
                legacy = f.ExprCtx.NewCallable(f.Pos, "Ensure", {f.Bool(true), f.Bool(false),
                    f.ExprCtx.NewCallable(f.Pos, "String", {f.ExprCtx.NewAtom(f.Pos, "checked closed")})});
            }
            const TString label = TStringBuilder() << "nullable=" << nullable << " variant=" << variant;
            f.Check(native, legacy, label, mismatches);
        }
        UNIT_ASSERT_C(mismatches.empty(), mismatches);
    }

    Y_UNIT_TEST(ResidualInequalityDoesNotDuplicateKeyRanges) {
        TString mismatches;
        for (const bool nullable : {false, true}) {
            TPredicateRangeFixture f({K::Int64, nullable});
            auto& arena = *f.Props.NativeExpressions;
            const auto native = arena.Binary(O::And,
                arena.Binary(O::Equal, arena.Column(f.Key, f.KeyType),
                    arena.Literal({K::Int64}, std::int64_t{2})),
                arena.Binary(O::NotEqual, arena.Column(f.Payload, {K::Int64}),
                    arena.Literal({K::Int64}, std::int64_t{7})));
            const auto legacy = f.ExprCtx.NewCallable(f.Pos, "And", {
                f.ExprCtx.NewCallable(f.Pos, "==", {f.KeyNode, f.Integer(2)}),
                f.ExprCtx.NewCallable(f.Pos, "!=", {f.PayloadNode, f.Integer(7)})});
            f.Check(native, legacy, "key point with residual inequality", mismatches);
            UNIT_ASSERT(f.LastRead->RangeInfo && f.LastRead->RangeInfo->NativeRanges);
            UNIT_ASSERT_VALUES_EQUAL(f.LastRead->RangeInfo->NativeRanges->size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(*f.LastRead->RangeInfo->ExpectedMaxRanges, 1);
        }
        UNIT_ASSERT_C(mismatches.empty(), mismatches);
    }

    Y_UNIT_TEST(LegacyMembershipMatchesPrefixesAndResidualContracts) {
        using namespace NYql;
        TString mismatches;
        for (const bool nullable : {false, true}) for (const bool residual : {false, true})
        for (const bool negated : {false, true}) for (unsigned variant = 0; variant < 5; ++variant) {
            // The frozen pre-PR compiler throws for empty tuples and for
            // NOT IN over heterogeneous tuple types. Check those separately.
            if (variant == 3 || (negated && variant == 1)) continue;
            TPredicateRangeFixture f({K::Int64, nullable});
            auto& arena = *f.Props.NativeExpressions;
            std::vector<TExprId> items;
            NYql::TExprNode::TListType oldItems;
            if (variant < 2) {
                items.push_back(arena.Literal({K::Int64}, std::int64_t{1}));
                oldItems.push_back(f.Integer(1));
            }
            if (variant == 0) {
                items.push_back(arena.Literal({K::Int64}, std::int64_t{2}));
                oldItems.push_back(f.Integer(2));
            } else if (variant == 1 || variant == 2) {
                items.push_back(arena.Literal({K::Int64, true}, {}));
                oldItems.push_back(f.ExprCtx.NewCallable(f.Pos, "Nothing", {
                    ExpandType(f.Pos, *ExportNativeType({K::Int64, true}, f.ExprCtx), f.ExprCtx)}));
            } else if (variant == 4) {
                items.push_back(arena.Parameter("bound", {K::Int64, true}));
                oldItems.push_back(f.ExprCtx.NewCallable(f.Pos, "Parameter", {f.ExprCtx.NewAtom(f.Pos, "bound"),
                    ExpandType(f.Pos, *ExportNativeType({K::Int64, true}, f.ExprCtx), f.ExprCtx)}));
            }
            const auto key = arena.Column(f.Key, f.KeyType);
            auto native = arena.InCollection(key, arena.Collection(items, false), false);
            auto legacy = f.ExprCtx.NewCallable(f.Pos, "SqlIn", {
                f.ExprCtx.NewList(f.Pos, std::move(oldItems)), f.KeyNode, f.ExprCtx.NewList(f.Pos, {})});
            if (negated) {
                native = arena.Unary(O::Not, native);
                legacy = f.ExprCtx.NewCallable(f.Pos, "Not", {legacy});
            }
            if (residual) {
                native = arena.Binary(O::And, native, arena.Binary(O::Equal,
                    arena.Column(f.Payload, {K::Int64}), arena.Literal({K::Int64}, std::int64_t{7})));
                legacy = f.ExprCtx.NewCallable(f.Pos, "And", {legacy,
                    f.ExprCtx.NewCallable(f.Pos, "==", {f.PayloadNode, f.Integer(7)})});
            }
            const TString label = TStringBuilder() << "nullable=" << nullable << " residual=" << residual
                << " not=" << negated << " variant=" << variant;
            // Scalar folding turns NULL-only membership into a constant
            // selection (or removes it from a required-key conjunction).
            // Compare a constant filter with the old constant-filter adapter;
            // populated compiler tests separately compare the original SQL.
            if (variant == 2 && (!negated || (!nullable && !residual))) {
                f.Check(native, f.Bool(negated), label, mismatches);
            } else {
                f.Check(native, legacy, label, mismatches);
            }
        }
        UNIT_ASSERT_C(mismatches.empty(), mismatches);
    }

    Y_UNIT_TEST(MembershipDoesNotInheritLegacyExtractorCrashes) {
        using namespace NYql;
        for (const bool nullable : {false, true}) for (const bool negated : {false, true})
        for (const bool heterogeneous : {false, true}) {
            if (heterogeneous && !negated) continue;
            TPredicateRangeFixture f({K::Int64, nullable});
            auto& arena = *f.Props.NativeExpressions;
            const auto key = arena.Column(f.Key, f.KeyType);
            std::vector<TExprId> items;
            NYql::TExprNode::TListType oldItems;
            if (heterogeneous) {
                items = {arena.Literal({K::Int64}, std::int64_t{1}), arena.Literal({K::Int64, true}, {})};
                oldItems = {f.Integer(1), f.ExprCtx.NewCallable(f.Pos, "Nothing", {
                    ExpandType(f.Pos, *ExportNativeType({K::Int64, true}, f.ExprCtx), f.ExprCtx)})};
            }
            auto native = arena.InCollection(key, arena.Collection(items, false), false);
            auto legacy = f.ExprCtx.NewCallable(f.Pos, "SqlIn", {
                f.ExprCtx.NewList(f.Pos, std::move(oldItems)), f.KeyNode, f.ExprCtx.NewList(f.Pos, {})});
            if (negated) {
                native = arena.Unary(O::Not, native);
                legacy = f.ExprCtx.NewCallable(f.Pos, "Not", {legacy});
            }
            TString mismatches;
            const auto error = negated ? "GetSqlInCollectionItemType" : "index out of range";
            UNIT_ASSERT_EXCEPTION_CONTAINS(f.Check(native, legacy, "unsupported legacy tuple", mismatches),
                yexception, error);
            const auto folded = arena.FoldConstants(std::span(&native, 1)).front();
            const auto prepared = PrepareFilterPredicate(arena, folded);
            for (const bool missing : {false, true}) for (const std::int64_t input : {1, 2}) {
                if (missing && !nullable) continue;
                TEvaluationContext context;
                context.Column = [&](auto) { return TValue{f.KeyType,
                    missing ? TScalar{} : TScalar{input}}; };
                for (const auto expression : {native, folded, prepared}) {
                    const auto value = Evaluate(arena, expression, context);
                    const bool selected = std::holds_alternative<bool>(value.Data) && std::get<bool>(value.Data);
                    UNIT_ASSERT_VALUES_EQUAL(selected, !missing && ((heterogeneous && input == 1) != negated));
                }
            }
        }
    }
}
} // namespace NKikimr::NKqp
