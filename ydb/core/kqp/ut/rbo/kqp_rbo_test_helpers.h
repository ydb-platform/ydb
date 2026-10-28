#pragma once

#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>
#include <ydb/core/kqp/common/kqp_user_request_context.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>
#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/time_provider/time_provider.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp::NTests {

struct TIdTestContext {
    TIdTestContext()
        : FuncRegistry(NMiniKQL::CreateFunctionRegistry(NMiniKQL::CreateBuiltinRegistry()))
        , Config(MakeIntrusive<TKikimrConfiguration>())
        , QueryCtx(MakeIntrusive<TKikimrQueryContext>(FuncRegistry.Get(), CreateDefaultTimeProvider(), CreateDefaultRandomProvider()))
        , Tables(MakeIntrusive<TKikimrTablesData>())
        , UserRequestContext(MakeIntrusive<TUserRequestContext>())
        , KqpCtx("ut", Config, QueryCtx, Tables, UserRequestContext)
        , RboCtx(KqpCtx, ExprCtx, TypeCtx, TypeAnnTransformer, *FuncRegistry)
    {}

    TInfoUnitId Id(TString name = "column") {
        return Props.InfoUnitRegistry.Add(TInfoUnit(name));
    }

    TIntrusivePtr<TOpRead> Read(TUnorderedIUs columns) {
        const auto table = NNodes::Build<NNodes::TKqpTable>(ExprCtx, Pos)
            .Path().Build("/Root/table").PathId().Build("1:1")
            .SysView().Build("").Version().Build("1").Done().Ptr();
        return MakeIntrusive<TOpRead>("", std::move(columns), NYql::EStorageType::RowStorage,
            table, nullptr, nullptr, std::nullopt, std::nullopt, ESortDir::None, TPhysicalOpProps{}, Pos);
    }

    TIntrusivePtr<TOpUnionAll> Union(TIntrusivePtr<IOperator> left, TIntrusivePtr<IOperator> right,
        std::initializer_list<std::tuple<TInfoUnitId, TInfoUnitId, TInfoUnitId>> bindings, bool ordered = false) {
        TUnionAllIUs columns(TUnionInputPolicy{2});
        for (const auto& [output, a, b] : bindings) {
            columns.Add(output, TUnionInputRow{{a, b}});
        }
        return MakeIntrusive<TOpUnionAll>(std::move(left), std::move(right), Pos, std::move(columns), ordered);
    }

    void SetType(IOperator& op, const TUnorderedIUs& nullable = {}) {
        TVector<const TItemExprType*> fields;
        for (auto id : op.GetOutputIUs()) {
            const TTypeAnnotationNode* type = ExprCtx.MakeType<TDataExprType>(EDataSlot::Int32);
            if (nullable.Contains(id)) {
                type = ExprCtx.MakeType<TOptionalExprType>(type);
            }
            fields.push_back(ExprCtx.MakeType<TItemExprType>(ExprCtx.GetIndexAsString(id), type));
        }
        op.Type = ExprCtx.MakeType<TListExprType>(ExprCtx.MakeType<TStructExprType>(fields));
    }

    TExpression Column(TInfoUnitId id) { return MakeColumnAccess(id, Pos, &ExprCtx, &Props); }
    TExpression Constant() { return MakeConstant("Uint64", "1", Pos, &ExprCtx); }

    TIntrusivePtr<TOpMap> Copies(TIntrusivePtr<IOperator> input,
        std::initializer_list<std::pair<TInfoUnitId, TInfoUnitId>> copies) {
        TMapIUs definitions;
        for (const auto& [output, source] : copies) {
            definitions.Add(output, Column(source));
        }
        return MakeIntrusive<TOpMap>(std::move(input), Pos, std::move(definitions));
    }

    TIntrusivePtr<TOpRoot> Root(TIntrusivePtr<IOperator> input, TOrderedIUs<TString> columns) {
        auto root = MakeIntrusive<TOpRoot>(std::move(input), Pos, std::move(columns));
        root->PlanProps = std::move(Props);
        for (const auto& item : *root) {
            item.Current->BindExpressionPlanProps(&root->PlanProps);
        }
        root->RecomputeOutputIUsSubtree();
        root->ComputeParents();
        return root;
    }

    const TPositionHandle Pos;
    TExprContext ExprCtx;
    TTypeAnnotationContext TypeCtx;
    TNullTransformer TypeAnnTransformer;
    TIntrusivePtr<NMiniKQL::IFunctionRegistry> FuncRegistry;
    TIntrusivePtr<TKikimrConfiguration> Config;
    TIntrusivePtr<TKikimrQueryContext> QueryCtx;
    TIntrusivePtr<TKikimrTablesData> Tables;
    TIntrusivePtr<TUserRequestContext> UserRequestContext;
    NOpt::TKqpOptimizeContext KqpCtx;
    TRBOContext RboCtx;
    TPlanProps Props;
};

// Every ID has one definition, operators use only their children's outputs,
// and Join inputs stay disjoint.
inline void AssertIdInvariants(IOperator& root, TPlanProps& props) {
    TUnorderedIUs definitions;
    for (const auto& item : IterateSubtree(&root)) {
        auto& op = *item.Current;
        TUnorderedIUs own, available;
        for (auto* child : op.GetChildren()) {
            available.UnionWith(child->GetOutputIUs());
        }
        UNIT_ASSERT(op.GetUsedIUs(props).IsSubsetOf(available));
        switch (op.Kind) {
            case EOperator::Source:
                own = CastOperator<TOpRead>(op).GetColumns();
                break;
            case EOperator::Map:
                own = CastOperator<TOpMap>(op).GetMapElements().Keys();
                break;
            case EOperator::AddDependencies:
                own = CastOperator<TOpAddDependencies>(op).GetDependencies().Keys();
                break;
            case EOperator::Aggregate:
                own = CastOperator<TOpAggregate>(op).GetAggregationTraits().Keys();
                break;
            case EOperator::UnionAll: {
                auto& merge = CastOperator<TOpUnionAll>(op);
                own = merge.GetColumns().Keys();
                for (const auto& [output, row] : merge.GetColumns().Items()) {
                    for (size_t i = 0; i < row.Inputs.size(); ++i) {
                        UNIT_ASSERT(merge.GetInput(i)->GetOutputIUs().Contains(row.Inputs[i]));
                    }
                }
                break;
            }
            case EOperator::Replicate:
                if (!CastOperator<TOpReplicate>(op).IsPrimary()) {
                    own = op.GetOutputIUs();
                }
                break;
            case EOperator::Join: {
                auto& join = CastOperator<TOpJoin>(op);
                UNIT_ASSERT(!join.GetLeftInput()->GetOutputIUs().HasAny(join.GetRightInput()->GetOutputIUs()));
                UNIT_ASSERT(join.JoinKeys.Left().IsSubsetOf(join.GetLeftInput()->GetOutputIUs()));
                UNIT_ASSERT(join.JoinKeys.Right().IsSubsetOf(join.GetRightInput()->GetOutputIUs()));
                break;
            }
            default:
                break;
        }
        UNIT_ASSERT(!definitions.HasAny(own));
        definitions.UnionWith(own);
    }
}

} // namespace NKikimr::NKqp::NTests
