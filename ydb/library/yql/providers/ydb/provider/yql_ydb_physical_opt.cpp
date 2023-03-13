#include "yql_ydb_provider_impl.h"

#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/providers/ydb/expr_nodes/yql_ydb_expr_nodes.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>
#include <ydb/library/yql/providers/common/transform/yql_optimize.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/utils/log/log.h>


namespace NYql {

using namespace NNodes;

namespace {

class TYdbPhysicalOptProposalTransformer : public TOptimizeTransformerBase {
public:
    TYdbPhysicalOptProposalTransformer(TYdbState::TPtr state)
        : TOptimizeTransformerBase(state->Types, NLog::EComponent::ProviderYdb, {})
        , State_(state)
    {
#define HNDL(name) "PhysicalOptimizer-"#name, Hndl(&TYdbPhysicalOptProposalTransformer::name)
        AddHandler(0, &TCoLeft::Match,      HNDL(TrimReadWorld));
        AddHandler(0, &TCoTake::Match,      HNDL(Take));
        AddHandler(0, &TCoNarrowMap::Match, HNDL(ReadZeroColumns));
        AddHandler(0, &TDqStage::Match,     HNDL(SourceZeroColumns));
#undef HNDL
    }

    TMaybeNode<TExprBase> TrimReadWorld(TExprBase node, TExprContext& ctx) const {
        Y_UNUSED(ctx);

        const auto& maybeRead = node.Cast<TCoLeft>().Input().Maybe<TYdbReadTable>();
        if (!maybeRead) {
            return node;
        }

        return TExprBase(maybeRead.Cast().World().Ptr());
    }

    TMaybeNode<TExprBase> Take(TExprBase node, TExprContext& ctx) const {
        const auto& take = node.Cast<TCoTake>();
        const auto& wrap = take.Input().Maybe<TDqReadWrap>();
        if (!wrap) {
            return node;
        }

        const auto& dqrw = wrap.Cast();
        const auto& read = dqrw.Input().Maybe<TYdbReadTable>();
        if (!read) {
            return node;
        }

        const auto& cast = read.Cast();
        if (cast.LimitHint()) {
            return node;
        }

        return Build<TCoTake>(ctx, take.Pos())
            .Input<TDqReadWrap>()
                .InitFrom(dqrw)
                .Input<TYdbReadTable>()
                    .InitFrom(cast)
                    .LimitHint(take.Count())
                .Build()
            .Build()
            .Count(take.Count())
            .Done();
    }

    TMaybeNode<TExprBase> ReadZeroColumns(TExprBase node, TExprContext& ctx) const {
        const auto& narrow = node.Maybe<TCoNarrowMap>();
        if (const auto& wide = narrow.Cast().Input().Maybe<TDqReadWideWrap>()) {
            if (const auto& maybe = wide.Cast().Input().Maybe<TYdbReadTable>()) {
                if (!wide.Cast().Ref().GetTypeAnn()->Cast<TFlowExprType>()->GetItemType()->Cast<TMultiExprType>()->GetSize()) {
                    const auto& read = maybe.Cast();
                    const auto structType = State_->Tables[std::make_pair(read.DataSource().Cluster().StringValue(), read.Table().StringValue())].ItemType;
                    YQL_ENSURE(structType->GetSize());
                    auto columns = ctx.NewList(read.Pos(), {ctx.NewAtom(read.Pos(), GetLightColumn(*structType)->GetName())});
                    return Build<TCoNarrowMap>(ctx, narrow.Cast().Pos())
                        .Input<TDqReadWideWrap>()
                            .InitFrom(wide.Cast())
                            .Input<TYdbReadTable>()
                                .InitFrom(read)
                                .Columns(std::move(columns))
                            .Build()
                        .Build()
                        .Lambda()
                            .Args({"stub"})
                            .Body<TCoAsStruct>().Build()
                        .Build()
                    .Done();
                }
            }
        }

        return node;
    }

    TMaybeNode<TExprBase> SourceZeroColumns(TExprBase node, TExprContext& ctx) const {
        if (const auto& stage = node.Cast<TDqStage>(); stage.Inputs().Size() == 1U) {
            if (const auto& maySource = stage.Inputs().Item(0).Maybe<TDqSource>()) {
                if (const auto& source = maySource.Cast(); const auto& settings = source.Settings().Maybe<TYdbSourceSettings>()) {
                    if (const auto& cast = settings.Cast(); cast.Columns().Empty()) {
                        const auto& prog = stage.Program();
                        const auto narrow = FindNode(prog.Ptr(), [arg = prog.Args().Arg(0).Raw()] (const TExprNode::TPtr& node) {
                            return node->IsCallable(TCoNarrowMap::CallableName()) && node->Head().IsCallable(TDqSourceWideWrap::CallableName()) && arg == &node->Head().Head();
                        });

                        const auto lightField = GetLightColumn(*State_->Tables[std::make_pair(source.DataSource().Cast<TYdbDataSource>().Cluster().StringValue(), cast.Table().StringValue())].ItemType);
                        auto newNarrow = Build<TCoNarrowMap>(ctx, narrow->Pos())
                            .Input<TDqSourceWideWrap>()
                                .Input(prog.Args().Arg(0))
                                .DataSource(source.DataSource().Cast<TCoDataSource>())
                                .RowType(ExpandType(narrow->Pos(), *ctx.MakeType<TStructExprType>(TVector<const TItemExprType*>{lightField}), ctx))
                                .Build()
                            .Lambda()
                                .Args({"stub"})
                                .Body<TCoAsStruct>().Build()
                                .Build()
                            .Done()
                        .Ptr();

                        return Build<TDqStage>(ctx, stage.Pos())
                            .Inputs()
                                .Add<TDqSource>()
                                    .DataSource(source.DataSource())
                                    .Settings<TYdbSourceSettings>()
                                        .InitFrom(cast)
                                        .Columns().Add().Build(lightField->GetName()).Build()
                                        .Build()
                                    .Build()
                                .Build()
                            .Program(ctx.DeepCopyLambda(prog.Ref(), ctx.ReplaceNode(prog.Body().Ptr(), *narrow, std::move(newNarrow))))
                            .Settings(stage.Settings())
                        .Done();

                    }
                }
            }
        }

        return node;
    }
private:
    const TYdbState::TPtr State_;
};

}

THolder<IGraphTransformer> CreateYdbPhysicalOptProposalTransformer(TYdbState::TPtr state) {
    return MakeHolder<TYdbPhysicalOptProposalTransformer>(state);
}

} // namespace NYql
