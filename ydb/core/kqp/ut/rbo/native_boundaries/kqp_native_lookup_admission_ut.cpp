#include "../kqp_rbo_test_helpers.h"
#include <ydb/core/kqp/opt/rbo/kqp_rbo_lookup_join.h>
#include <ydb/core/kqp/opt/rbo/frontend_lowering.h>
#include <yql/essentials/core/extract_predicate/extract_predicate.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_expr_optimize.h>

namespace NKikimr::NKqp {
namespace {

struct TLookupFixture : NTests::TIdTestContext {
    TLookupFixture(bool covering = true) {
        Table = MakeIntrusive<NYql::TKikimrTableMetadata>();
        Table->Name = "/Root/table";
        Table->DoesExist = true;
        Table->Kind = NYql::EKikimrTableKind::Datashard;
        Table->KeyColumnNames = {"id"};
        for (const auto* name : {"id", "g", "payload"}) {
            auto& column = Table->Columns[name];
            column.Type = "Int32";
            column.TypeInfo = NScheme::TTypeInfo(NScheme::NTypeIds::Int32);
            column.NotNull = true;
        }
        Index = MakeIntrusive<NYql::TKikimrTableMetadata>();
        Index->Name = "/Root/table/by_g/indexImplTable";
        Index->DoesExist = true;
        Index->Kind = NYql::EKikimrTableKind::Datashard;
        Index->KeyColumnNames = {"g", "id"};
        Index->Columns = Table->Columns;
        if (!covering) Index->Columns.erase("payload");
        Table->Indexes.emplace_back("by_g", TVector<TString>{"g", "id"},
            covering ? TVector<TString>{"payload"} : TVector<TString>{},
            NYql::TIndexDescription::EType::GlobalSync, NYql::TIndexDescription::EIndexState::Ready,
            1, 1, 1, std::monostate{});
        Table->ImplTables.push_back(Index);
        Tables->GetOrAddTable("ut", "/Root", Table->Name).Metadata = Table;
        Tables->GetOrAddTable("ut", "/Root", Index->Name).Metadata = Index;
        Source = Read({Key, Group, Payload});
        SetType(*Source);
    }

    TInfoUnitId Key = Id("id"), Group = Id("g"), Payload = Id("payload");
    TIntrusivePtr<NYql::TKikimrTableMetadata> Table, Index;
    TIntrusivePtr<TOpRead> Source;
};

} // namespace

Y_UNIT_TEST_SUITE(KqpNativeLookupAdmission) {
    Y_UNIT_TEST(LookupKeyComparisonRemovesOneOptionalLayer) {
        using namespace NFrontend;
        using namespace NYql;
        for (std::uint8_t leftDepth = 0; leftDepth < 3; ++leftDepth)
        for (std::uint8_t rightDepth = 0; rightDepth < 2; ++rightDepth) {
            TLookupFixture f;
            f.QueryCtx->Type = EKikimrQueryType::Query;
            f.Config->SetEnableKqpDataQueryStreamIdxLookupJoin(true);
            const TType leftType{ETypeKind::Int32, leftDepth}, rightType{ETypeKind::Int32, rightDepth};
            const auto leftKey = f.Id("probe");
            auto left = f.Read({leftKey});
            f.SetType(*left);
            left->NativeSchema->Row[leftKey] = leftType;
            f.Source->NativeSchema->Row[f.Key] = rightType;
            f.Table->Columns["id"].NotNull = !rightDepth;
            TJoinIUs keys;
            keys.Add(leftKey, f.Key);
            auto join = MakeIntrusive<TOpJoin>(left, f.Source, f.Pos, "Inner", std::move(keys));
            join->Props.JoinAlgo = EJoinAlgoType::LookupJoin;

            // Independent pre-PR key check: StripOptional removed exactly
            // one wrapper, so Optional<Optional<Int32>> cannot probe Int32.
            const auto stripOptional = [](const TTypeAnnotationNode* type) {
                return type->GetKind() == ETypeAnnotationKind::Optional
                    ? type->Cast<TOptionalExprType>()->GetItemType() : type;
            };
            const bool expected = stripOptional(ExportNativeType(leftType, f.ExprCtx)) ==
                stripOptional(ExportNativeType(rightType, f.ExprCtx));
            const auto nodes = f.ExprCtx.NodeAllocationCounter, types = f.ExprCtx.TypeNodes.size();
            const auto result = TRewriteJoinToIndexLookupJoinRule().SimpleMatchAndApply(join, f.RboCtx, f.Props);
            UNIT_ASSERT_VALUES_EQUAL_C(result->Kind == EOperator::IndexLookupJoin, expected,
                "left=" << leftType.ToString() << " right=" << rightType.ToString());
            if (!expected) UNIT_ASSERT(join->Props.JoinAlgo == EJoinAlgoType::MapJoin);
            UNIT_ASSERT_VALUES_EQUAL(f.ExprCtx.NodeAllocationCounter, nodes);
            UNIT_ASSERT_VALUES_EQUAL(f.ExprCtx.TypeNodes.size(), types);
        }
    }

    Y_UNIT_TEST(CoveringIndexShortcutPreservesRangeInfoExclusion) {
        // d11af71c6d: only reads without RangeInfo use this shortcut. A
        // full native range and restored point-prefix metadata still count.
        for (const bool automatic : {false, true}) for (const bool prefixes : {false, true})
        for (const bool inner : {false, true}) for (int rangeKind : {0, 1, 2}) {
            TLookupFixture f;
            f.Config->EnableAutoIndexSelectionForIndexLookupJoin = automatic;
            f.Config->EnableLookupJoinPointPrefixes = prefixes;
            if (rangeKind) {
                auto& info = f.Source->RangeInfo.emplace();
                if (rangeKind == 1) {
                    info.KeyColumns = {"id"};
                    info.NativeRanges.emplace().emplace_back();
                }
                f.Source->OriginalPredicate = TExpression::FromNative(f.Props.NativeExpressions,
                    f.Props.NativeExpressions->Literal({NFrontend::ETypeKind::Bool}, true), &f.Props);
            }
            const auto nodes = f.ExprCtx.NodeAllocationCounter, types = f.ExprCtx.TypeNodes.size();
            const auto target = ChooseLookupJoinTarget(*f.Source, {"id", "g", "payload"}, {"g"}, inner, f.KqpCtx);
            UNIT_ASSERT_VALUES_EQUAL_C(target.has_value(), automatic && rangeKind == 0,
                "automatic=" << automatic << " prefixes=" << prefixes << " inner=" << inner << " rangeKind=" << rangeKind);
            if (target) {
                UNIT_ASSERT(target->Metadata == f.Index);
                UNIT_ASSERT(!target->PointPrefix && !target->MainTable);
            }
            UNIT_ASSERT_VALUES_EQUAL(f.ExprCtx.NodeAllocationCounter, nodes);
            UNIT_ASSERT_VALUES_EQUAL(f.ExprCtx.TypeNodes.size(), types);
        }
    }

    Y_UNIT_TEST(RestoredSourceKeepsRangeInfoWithoutPointPrefixes) {
        TLookupFixture f;
        auto input = f.Read({f.Key, f.Group});
        input->TableRef.Path = f.Index->Name;
        TOpTableLookup::TLookupKeys keys;
        keys.Append(f.Key, "id");
        auto lookup = MakeIntrusive<TOpTableLookup>(input, f.Pos, f.Source->TableRef,
            f.Source->GetColumns(), std::move(keys));
        f.SetType(*lookup);
        const auto predicate = TExpression::FromNative(f.Props.NativeExpressions,
            f.Props.NativeExpressions->Binary(NFrontend::EOpcode::Greater,
                f.Props.NativeExpressions->Column(f.Group, {NFrontend::ETypeKind::Int32}),
                f.Props.NativeExpressions->Literal({NFrontend::ETypeKind::Int32}, std::int64_t{0})), &f.Props);
        lookup->SourceRead = TOpTableLookup::TSourceRead{predicate, {}};
        const auto nodes = f.ExprCtx.NodeAllocationCounter, types = f.ExprCtx.TypeNodes.size();
        const auto restored = MatchLookupJoinRightSide(lookup);
        UNIT_ASSERT(restored && !restored->Filter);
        UNIT_ASSERT(restored->Read->RangeInfo);
        UNIT_ASSERT(restored->Read->RangeInfo->PointPrefixes.empty());
        UNIT_ASSERT(!restored->Read->RangeInfo->NativeRanges);
        UNIT_ASSERT(restored->Read->OriginalPredicate);
        UNIT_ASSERT_VALUES_EQUAL(restored->Read->OriginalPredicate->GetNativeRoot(), predicate.GetNativeRoot());
        UNIT_ASSERT(restored->Read->GetColumns() == f.Source->GetColumns());
        UNIT_ASSERT_VALUES_EQUAL(restored->Read->TableRef.Path, f.Table->Name);
        UNIT_ASSERT_VALUES_EQUAL(f.ExprCtx.NodeAllocationCounter, nodes);
        UNIT_ASSERT_VALUES_EQUAL(f.ExprCtx.TypeNodes.size(), types);
    }

    Y_UNIT_TEST(NonCoveringIndexSavesSourceOnlyWhenPointPrefixPolicyAllows) {
        using namespace NFrontend;
        for (const bool prefixes : {false, true}) {
            TLookupFixture f(false);
            f.Config->OptDisableAutoIndexSelection = false;
            f.Config->EnableLookupJoinPointPrefixes = prefixes;
            const auto arena = f.Props.NativeExpressions;
            const auto predicate = arena->Binary(EOpcode::Equal, arena->Column(f.Group, {ETypeKind::Int32}),
                arena->Literal({ETypeKind::Int32}, std::int64_t{2}));
            auto filter = MakeIntrusive<TOpFilter>(f.Source, f.Pos, TExpression::FromNative(arena, predicate, &f.Props));
            const auto nodes = f.ExprCtx.NodeAllocationCounter, types = f.ExprCtx.TypeNodes.size();
            const auto result = TPushRangesRule(NYql::EStorageType::RowStorage).SimpleMatchAndApply(filter, f.RboCtx, f.Props);
            UNIT_ASSERT(result->Kind == EOperator::TableLookup);
            const auto& lookup = CastOperator<TOpTableLookup>(*result);
            UNIT_ASSERT_VALUES_EQUAL(lookup.SourceRead.has_value(), prefixes);
            if (prefixes) {
                UNIT_ASSERT_VALUES_EQUAL(lookup.SourceRead->Predicate.GetNativeRoot(), predicate);
                UNIT_ASSERT_VALUES_EQUAL(lookup.SourceRead->PointPrefixes.size(), 1);
                UNIT_ASSERT(lookup.SourceRead->PointPrefixes.front().NativePoints);
            }
            UNIT_ASSERT_VALUES_EQUAL(f.ExprCtx.NodeAllocationCounter, nodes);
            UNIT_ASSERT_VALUES_EQUAL(f.ExprCtx.TypeNodes.size(), types);
        }
    }
    Y_UNIT_TEST(ClosedBooleanRangesMatchLegacyExtraction) {
        using namespace NFrontend;
        using namespace NYql;
        // The legacy extractor uses a conditional full range for a predicate
        // independent of the row, including NULL and runtime Boolean values.
        for (const bool nullable : {false, true}) for (int expression : {0, 1, 2, 3}) {
            TLookupFixture f;
            const TType type{ETypeKind::Bool, nullable};
            const auto arena = f.Props.NativeExpressions;
            auto root = expression < 2 ? arena->Literal(type, expression != 0) : arena->Parameter("enabled", type);
            if (expression == 3) root = arena->Unary(EOpcode::Not, root);
            auto& ctx = f.ExprCtx;
            const auto* rowType = ctx.MakeType<TStructExprType>(TVector<const TItemExprType*>{
                ctx.MakeType<TItemExprType>("id", ctx.MakeType<TDataExprType>(EDataSlot::Int32))});
            auto row = ctx.NewArgument(f.Pos, "row");
            row->SetTypeAnn(rowType);
            row->AddConstraint(ctx.MakeConstraint<TEmptyConstraintNode>());
            const auto boolean = [&](bool value) {
                return ctx.NewCallable(f.Pos, "Bool", {ctx.NewAtom(f.Pos, value ? "true" : "false")});
            };
            NYql::TExprNode::TPtr expected = expression < 2 ? boolean(expression != 0)
                : ctx.NewCallable(f.Pos, "Parameter", {ctx.NewAtom(f.Pos, "enabled"),
                    ExpandType(f.Pos, *ExportNativeType(type, ctx), ctx)});
            if (nullable && expression < 2) expected = ctx.NewCallable(f.Pos, "Just", {expected});
            if (expression == 3) expected = ctx.NewCallable(f.Pos, "Not", {expected});
            if (nullable) expected = ctx.NewCallable(f.Pos, "Coalesce", {expected, boolean(false)});
            auto lambda = ctx.NewLambda(f.Pos, ctx.NewArguments(f.Pos, {row}), std::move(expected));
            TPredicateExtractorSettings settings;
            settings.HaveNextValueCallable = true;
            settings.BuildLiteralRange = true;
            auto typesContext = MakeIntrusive<TTypeAnnotationContext>();
            auto extractor = MakePredicateRangeExtractor(settings);
            THashSet<TString> possibleKeys;
            UNIT_ASSERT(extractor->Prepare(lambda, *rowType, possibleKeys, ctx, *typesContext));
            const auto reference = extractor->BuildComputeNode({"id"}, ctx, *typesContext);
            UNIT_ASSERT(reference.ComputeNode);
            UNIT_ASSERT_VALUES_EQUAL(reference.UsedPrefixLen, 1);
            UNIT_ASSERT_VALUES_EQUAL(reference.PointPrefixLen, 0);

            auto filter = MakeIntrusive<TOpFilter>(f.Source, f.Pos, TExpression::FromNative(arena, root, &f.Props));
            const auto nodes = ctx.NodeAllocationCounter, types = ctx.TypeNodes.size();
            const auto result = TPushRangesRule(NYql::EStorageType::RowStorage).SimpleMatchAndApply(filter, f.RboCtx, f.Props);
            const auto source = result->Kind == EOperator::Filter ? result->GetChildren().front() : result;
            const auto& actual = CastOperator<TOpRead>(*source).RangeInfo;
            UNIT_ASSERT_C(actual && actual->NativeRanges, "nullable=" << nullable << " expression=" << expression);
            UNIT_ASSERT_VALUES_EQUAL(actual->UsedPrefixLen, reference.UsedPrefixLen);
            UNIT_ASSERT_VALUES_EQUAL(actual->PointPrefixLen, reference.PointPrefixLen);
            UNIT_ASSERT_VALUES_EQUAL(actual->NativeRanges->size(), 1);
            const auto& range = actual->NativeRanges->front();
            UNIT_ASSERT(range.Lower.Values.empty() && range.Upper.Values.empty());
            for (const TScalar& value : std::vector<TScalar>{false, true, std::monostate{}}) {
                if (!nullable && std::holds_alternative<std::monostate>(value)) continue;
                TEvaluationContext evaluation;
                evaluation.Parameter = [&](std::string_view) { return TValue{type, value}; };
                const auto original = Evaluate(*arena, root, evaluation);
                const bool includes = std::holds_alternative<bool>(original.Data) && std::get<bool>(original.Data);
                UNIT_ASSERT_VALUES_EQUAL(range.Condition
                    ? std::get<bool>(Evaluate(*arena, range.Condition->GetNativeRoot(), evaluation).Data) : true, includes);
            }
            UNIT_ASSERT_VALUES_EQUAL(ctx.NodeAllocationCounter, nodes);
            UNIT_ASSERT_VALUES_EQUAL(ctx.TypeNodes.size(), types);
        }
    }

}
} // namespace NKikimr::NKqp
