#include "yql_yt_provider_impl.h"
#include "yql_yt_helpers.h"
#include "yql_yt_join_impl.h"

#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <ydb/library/yql/providers/common/transform/yql_visit.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/core/yql_expr_constraint.h>
#include <ydb/library/yql/ast/yql_constraint.h>

#include <util/generic/xrange.h>

namespace NYql {

using namespace NNodes;

namespace {

class TYtDataSinkConstraintTransformer : public TVisitorTransformerBase {
public:
    TYtDataSinkConstraintTransformer(TYtState::TPtr state, bool subGraph)
        : TVisitorTransformerBase(false)
        , State_(state)
        , SubGraph(subGraph)
    {
        AddHandler({TYtOutTable::CallableName()}, Hndl(&TYtDataSinkConstraintTransformer::HandleOutTable));
        AddHandler({TYtOutput::CallableName()}, Hndl(&TYtDataSinkConstraintTransformer::HandleOutput));
        AddHandler({TYtSort::CallableName()}, Hndl(&TYtDataSinkConstraintTransformer::HandleTransientOp));
        AddHandler({TYtCopy::CallableName()}, Hndl(&TYtDataSinkConstraintTransformer::HandleTransientOp));
        AddHandler({TYtMerge::CallableName()}, Hndl(&TYtDataSinkConstraintTransformer::HandleTransientOp));
        AddHandler({TYtMap::CallableName()}, Hndl(&TYtDataSinkConstraintTransformer::HandleUserJobOp<TYtMap::idx_Mapper, TYtMap::idx_Mapper>));
        AddHandler({TYtReduce::CallableName()}, Hndl(&TYtDataSinkConstraintTransformer::HandleUserJobOp<TYtReduce::idx_Reducer, TYtReduce::idx_Reducer>));
        AddHandler({TYtMapReduce::CallableName()}, Hndl(&TYtDataSinkConstraintTransformer::HandleUserJobOp<TYtMapReduce::idx_Mapper, TYtMapReduce::idx_Reducer>));
        AddHandler({TYtWriteTable::CallableName()}, Hndl(&TYtDataSinkConstraintTransformer::HandleWriteTable));
        AddHandler({TYtFill::CallableName()}, Hndl(&TYtDataSinkConstraintTransformer::HandleFill));
        AddHandler({TYtTouch::CallableName()}, Hndl(&TYtDataSinkConstraintTransformer::HandleTouch));
        AddHandler({TYtDropTable::CallableName()}, Hndl(&TYtDataSinkConstraintTransformer::HandleDefault));
        AddHandler({TCoCommit::CallableName()}, Hndl(&TYtDataSinkConstraintTransformer::HandleCommit));
        AddHandler({TYtPublish::CallableName()}, Hndl(&TYtDataSinkConstraintTransformer::HandlePublish));
        AddHandler({TYtEquiJoin::CallableName()}, Hndl(&TYtDataSinkConstraintTransformer::HandleEquiJoin));
        AddHandler({TYtStatOutTable::CallableName()}, Hndl(&TYtDataSinkConstraintTransformer::HandleDefault));
        AddHandler({TYtStatOut::CallableName()}, Hndl(&TYtDataSinkConstraintTransformer::HandleDefault));
        AddHandler({TYtDqProcessWrite ::CallableName()}, Hndl(&TYtDataSinkConstraintTransformer::HandleDqProcessWrite));
        AddHandler({TYtTryFirst ::CallableName()}, Hndl(&TYtDataSinkConstraintTransformer::HandleTryFirst));
    }
private:
    static void CopyExcept(TExprNode* dst, const TExprNode& from, const TStringBuf& except) {
        for (const auto c: from.GetAllConstraints()) {
            if (c->GetName() != except) {
                dst->AddConstraint(c);
            }
        }
    }

    TStatus HandleOutTable(TExprBase input, TExprContext& ctx) {
        const auto table = input.Cast<TYtOutTable>();
        TConstraintSet set;
        if (!table.RowSpec().Maybe<TCoVoid>()) {
            TYqlRowSpecInfo rowSpec(table.RowSpec(), false);
            set = rowSpec.GetAllConstraints(ctx);

            if (!set.GetConstraint<TSortedConstraintNode>()) {
                if (const auto sorted = rowSpec.MakeSortConstraint(ctx))
                    set.AddConstraint(sorted);
            }
        }

        if (!(set.GetConstraint<TDistinctConstraintNode>() || set.GetConstraint<TUniqueConstraintNode>())) {
            if (const auto& uniqueBy = NYql::GetSetting(table.Settings().Ref(), EYtSettingType::UniqueBy)) {
                std::vector<std::string_view> columns;
                columns.reserve(uniqueBy->Tail().ChildrenSize());
                uniqueBy->Tail().ForEachChild([&columns](const TExprNode& column) { columns.emplace_back(column.Content()); });
                set.AddConstraint(ctx.MakeConstraint<TUniqueConstraintNode>(columns));
                set.AddConstraint(ctx.MakeConstraint<TDistinctConstraintNode>(columns));
            }
        }

        if (!table.Stat().Maybe<TCoVoid>()) {
            if (TYtTableStatInfo(table.Stat()).IsEmpty()) {
                set.AddConstraint(ctx.MakeConstraint<TEmptyConstraintNode>());
            }
        }
        input.Ptr()->SetConstraints(set);
        return TStatus::Ok;
    }

    TStatus HandleOutput(TExprBase input, TExprContext& ctx) {
        auto out = input.Cast<TYtOutput>();
        auto op = GetOutputOp(out);
        const bool skipSort = IsUnorderedOutput(out);
        if (op.Output().Size() > 1) {
            if (auto multi = op.Ref().GetConstraint<TMultiConstraintNode>()) {
                if (auto constraints = multi->GetItem(FromString<ui32>(out.OutIndex().Value()))) {
                    for (auto c: constraints->GetAllConstraints()) {
                        if (!skipSort || c->GetName() != TSortedConstraintNode::Name()) {
                            input.Ptr()->AddConstraint(c);
                        }
                    }
                } else {
                    input.Ptr()->AddConstraint(ctx.MakeConstraint<TEmptyConstraintNode>());
                }
            }
            if (auto empty = op.Ref().GetConstraint<TEmptyConstraintNode>()) {
                input.Ptr()->AddConstraint(empty);
            }
        } else {
            for (auto c: op.Ref().GetAllConstraints()) {
                if (!skipSort || c->GetName() != TSortedConstraintNode::Name()) {
                    input.Ptr()->AddConstraint(c);
                }
            }
        }

        return TStatus::Ok;
    }

    template <size_t InLambdaNdx, size_t OutLambdaNdx>
    TStatus HandleUserJobOp(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        auto op = TYtWithUserJobsOpBase(input);
        TExprNode::TPtr lambda = input->ChildPtr(InLambdaNdx);
        size_t lambdaNdx = InLambdaNdx;

        bool singleLambda = InLambdaNdx == OutLambdaNdx;
        if (!singleLambda && TCoVoid::Match(lambda.Get())) {
            singleLambda = true;
            lambda = input->ChildPtr(OutLambdaNdx);
            lambdaNdx = OutLambdaNdx;
        }

        const auto filter = NYql::HasSetting(op.Settings().Ref(), EYtSettingType::Ordered) ?
            [](const std::string_view& name) { return TEmptyConstraintNode::Name() == name || TUniqueConstraintNode::Name() == name || TDistinctConstraintNode::Name() == name || TSortedConstraintNode::Name() == name; }:
            [](const std::string_view& name) { return TEmptyConstraintNode::Name() == name || TUniqueConstraintNode::Name() == name || TDistinctConstraintNode::Name() == name; };
        TConstraintNode::TListType argConstraints;
        if (op.Input().Size() > 1) {
            TMultiConstraintNode::TMapType multiItems;
            bool allEmpty = true;
            for (ui32 index = 0; index < op.Input().Size(); ++index) {
                auto section = op.Input().Item(index);
                if (!section.Ref().GetConstraint<TEmptyConstraintNode>()) {
                    multiItems.push_back(std::make_pair(index, section.Ref().GetConstraintSet()));
                    multiItems.back().second.FilterConstraints(filter);
                    allEmpty = false;
                }
            }
            if (!multiItems.empty()) {
                argConstraints.push_back(ctx.MakeConstraint<TMultiConstraintNode>(std::move(multiItems)));
            } else if (allEmpty) {
                argConstraints.push_back(ctx.MakeConstraint<TEmptyConstraintNode>());
            }
        } else {
            auto set = op.Input().Item(0).Ref().GetConstraintSet();
            set.FilterConstraints(filter);
            argConstraints = set.GetAllConstraints();
            if (singleLambda) {
                if (const auto& reduceBy = NYql::GetSettingAsColumnList(op.Settings().Ref(), EYtSettingType::ReduceBy); !reduceBy.empty()) {
                    TPartOfConstraintBase::TSetOfSetsType sets;
                    sets.reserve(reduceBy.size());
                    std::transform(reduceBy.cbegin(), reduceBy.cend(), std::back_inserter(sets), [&ctx](const TString& column) { return TPartOfConstraintBase::TSetType{TPartOfConstraintBase::TPathType(1U, ctx.AppendString(column))}; });
                    argConstraints.push_back(ctx.MakeConstraint<TChoppedConstraintNode>(std::move(sets)));
                }
            }
        }

        auto status = UpdateLambdaConstraints(lambda, ctx, {argConstraints});
        if (lambda != input->ChildPtr(lambdaNdx)) {
            output = ctx.ChangeChild(*input, lambdaNdx, std::move(lambda));
            status = status.Combine(TStatus::Repeat);
            return status;
        }
        if (status != TStatus::Ok) {
            return status;
        }

        if (!singleLambda) {
            TConstraintNode::TListType argConstraints;
            if (auto empty = lambda->GetConstraint<TEmptyConstraintNode>()) {
                argConstraints.push_back(empty);
            }

            if (const auto& reduceBy = NYql::GetSettingAsColumnList(op.Settings().Ref(), EYtSettingType::ReduceBy); !reduceBy.empty()) {
                TPartOfConstraintBase::TSetOfSetsType sets;
                sets.reserve(reduceBy.size());
                std::transform(reduceBy.cbegin(), reduceBy.cend(), std::back_inserter(sets), [&ctx](const TString& column) { return TPartOfConstraintBase::TSetType{TPartOfConstraintBase::TPathType(1U, ctx.AppendString(column))}; });
                argConstraints.push_back(ctx.MakeConstraint<TChoppedConstraintNode>(std::move(sets)));
            }

            TExprNode::TPtr outLambda = input->ChildPtr(OutLambdaNdx);
            auto status = UpdateLambdaConstraints(outLambda, ctx, {argConstraints});
            if (outLambda != input->ChildPtr(OutLambdaNdx)) {
                output = ctx.ChangeChild(*input, OutLambdaNdx, std::move(outLambda));
                status = status.Combine(TStatus::Repeat);
            }
            if (status != TStatus::Ok) {
                return status;
            }
        }

        SetResultConstraint(input, *input->Child(OutLambdaNdx), op.Output(), ctx);
        if (op.Input().Size() == 1) {
            if (auto empty = op.Input().Item(0).Ref().GetConstraint<TEmptyConstraintNode>()) {
                input->AddConstraint(empty);
            }
        }

        return TStatus::Ok;
    }

    TStatus HandleFill(TExprBase input, TExprContext& ctx) {
        auto fill = input.Cast<TYtFill>();
        auto status = UpdateLambdaConstraints(fill.Content().Ref());
        if (status != TStatus::Ok) {
            return status;
        }

        SetResultConstraint(input.Ptr(), fill.Content().Ref(), fill.Output(), ctx);
        return TStatus::Ok;
    }

    static TConstraintNode::TListType GetConstraintsForInputArgument(const TConstraintSet& set, TExprContext& ctx) {
        TConstraintNode::TListType argsConstraints;
        if (auto mapping = TPartOfUniqueConstraintNode::GetCommonMapping(set.GetConstraint<TUniqueConstraintNode>()); !mapping.empty()) {
            argsConstraints.emplace_back(ctx.MakeConstraint<TPartOfUniqueConstraintNode>(std::move(mapping)));
        }
        if (auto mapping = TPartOfDistinctConstraintNode::GetCommonMapping(set.GetConstraint<TDistinctConstraintNode>()); !mapping.empty()) {
            argsConstraints.emplace_back(ctx.MakeConstraint<TPartOfDistinctConstraintNode>(std::move(mapping)));
        }
        return argsConstraints;
    }

    TStatus HandleEquiJoin(TExprBase input, TExprContext& ctx) {
        const auto equiJoin = input.Cast<TYtEquiJoin>();
        TStatus status = TStatus::Ok;
        for (const auto i : xrange(equiJoin.Input().Size())) {
            const auto premapIndex = i + 7U;
            if (equiJoin.Ref().Child(premapIndex)->IsLambda()) {
                status = status.Combine(UpdateLambdaConstraints(equiJoin.Ptr()->ChildRef(premapIndex), ctx, {GetConstraintsForInputArgument(equiJoin.Input().Item(i).Ref().GetConstraintSet(), ctx)}));
                if (status == TStatus::Error)
                    return status;
            }
        }
        if (status != TStatus::Ok)
            return status;

        input.Ptr()->SetConstraints(ImportYtEquiJoin(equiJoin, ctx)->Constraints);
        return TStatus::Ok;
    }

    TStatus HandleTransientOp(TExprBase input, TExprContext& ctx) {
        auto status = HandleDefault(input, ctx);
        if (status != TStatus::Ok) {
            return status;
        }
        auto op = input.Cast<TYtTransientOpBase>();
        YQL_ENSURE(op.Input().Size() == 1);
        YQL_ENSURE(op.Output().Size() == 1);

        input.Ptr()->CopyConstraints(op.Output().Item(0).Ref());
        if (auto empty = op.Input().Item(0).Ref().GetConstraint<TEmptyConstraintNode>()) {
            input.Ptr()->AddConstraint(empty);
        }

        return TStatus::Ok;
    }

    TStatus HandleTouch(TExprBase input, TExprContext& ctx) {
        auto status = HandleDefault(input, ctx);
        if (status != TStatus::Ok) {
            return status;
        }
        input.Ptr()->CopyConstraints(input.Cast<TYtTouch>().Output().Item(0).Ref());
        input.Ptr()->AddConstraint(ctx.MakeConstraint<TEmptyConstraintNode>());
        return TStatus::Ok;
    }

    TStatus HandleDefault(TExprBase input, TExprContext& /*ctx*/) {
        return UpdateAllChildLambdasConstraints(input.Ref());
    }

    TStatus HandleWriteTable(TExprBase input, TExprContext& ctx) {
        if (SubGraph) {
            return TStatus::Ok;
        }

        auto writeTable = input.Cast<TYtWriteTable>();

        const bool initialWrite = NYql::HasSetting(writeTable.Settings().Ref(), EYtSettingType::Initial);
        const auto outTableInfo = TYtTableInfo(writeTable.Table());

        if (auto commitEpoch = outTableInfo.CommitEpoch.GetOrElse(0)) {
            const auto cluster = TString{writeTable.DataSink().Cluster().Value()};
            TYtTableDescription& nextDescription = State_->TablesData->GetModifTable(cluster, outTableInfo.Name, commitEpoch);

            if (initialWrite) {
                nextDescription.ConstraintsReady = false;
                nextDescription.Constraints.Clear();
                if (nextDescription.IsReplaced) {
                    nextDescription.Constraints = writeTable.Content().Ref().GetConstraintSet();
                } else {
                    const TYtTableDescription& description = State_->TablesData->GetTable(cluster, outTableInfo.Name, outTableInfo.Epoch);
                    YQL_ENSURE(description.ConstraintsReady);
                    nextDescription.Constraints = description.Constraints;
                }
            }

            if (!initialWrite || !nextDescription.IsReplaced) {
                if (const auto tableSort = nextDescription.Constraints.RemoveConstraint<TSortedConstraintNode>()) {
                    if (const auto contentSort = writeTable.Content().Ref().GetConstraint<TSortedConstraintNode>()) {
                        if (const auto commonSort = tableSort->MakeCommon(contentSort, ctx)) {
                            nextDescription.Constraints.AddConstraint(commonSort);
                        }
                    }
                }
                if (!writeTable.Content().Ref().GetConstraint<TEmptyConstraintNode>()) {
                    nextDescription.Constraints.RemoveConstraint<TEmptyConstraintNode>();
                }
                nextDescription.Constraints.RemoveConstraint<TUniqueConstraintNode>();
                nextDescription.Constraints.RemoveConstraint<TDistinctConstraintNode>();
            }
        }

        return TStatus::Ok;
    }

    TStatus HandlePublish(TExprBase input, TExprContext& ctx) {
        if (SubGraph) {
            return TStatus::Ok;
        }

        auto publish = input.Cast<TYtPublish>();

        const bool initialWrite = NYql::HasSetting(publish.Settings().Ref(), EYtSettingType::Initial);
        const auto outTableInfo = TYtTableInfo(publish.Publish());

        if (auto commitEpoch = outTableInfo.CommitEpoch.GetOrElse(0)) {

            const auto cluster = TString{publish.DataSink().Cluster().Value()};
            const auto tableName = TString{TYtTableInfo::GetTableLabel(publish.Publish())};
            TYtTableDescription& nextDescription = State_->TablesData->GetModifTable(cluster, tableName, commitEpoch);

            if (initialWrite) {
                nextDescription.ConstraintsReady = false;
                nextDescription.Constraints.Clear();
                if (nextDescription.IsReplaced) {
                    nextDescription.Constraints = publish.Input().Item(0).Ref().GetConstraintSet();
                } else {
                    const TYtTableDescription& description = State_->TablesData->GetTable(cluster, tableName, outTableInfo.Epoch);
                    YQL_ENSURE(description.ConstraintsReady);
                    nextDescription.Constraints = description.Constraints;
                }
            }

            const size_t from = nextDescription.IsReplaced ? 1 : 0;
            auto tableSort = nextDescription.Constraints.RemoveConstraint<TSortedConstraintNode>();
            for (size_t i = from; i < publish.Input().Size() && tableSort; ++i) {
                tableSort = tableSort->MakeCommon(publish.Input().Item(i).Ref().GetConstraint<TSortedConstraintNode>(), ctx);
            }
            if (tableSort) {
                nextDescription.Constraints.AddConstraint(tableSort);
            }
            if (AnyOf(publish.Input(), [](const TYtOutput& out) { return !out.Ref().GetConstraint<TEmptyConstraintNode>(); })) {
                nextDescription.Constraints.RemoveConstraint<TEmptyConstraintNode>();
            }
            if (publish.Input().Size() > (nextDescription.IsReplaced ? 1 : 0)) {
                nextDescription.Constraints.RemoveConstraint<TUniqueConstraintNode>();
                nextDescription.Constraints.RemoveConstraint<TDistinctConstraintNode>();
            }
        }

        return TStatus::Ok;
    }

    TStatus HandleCommit(TExprBase input, TExprContext& ctx) {
        if (SubGraph) {
            return TStatus::Ok;
        }

        const auto commit = input.Cast<TCoCommit>();
        const auto settings = NCommon::ParseCommitSettings(commit, ctx);

        if (settings.Epoch) {
            const ui32 epoch = FromString(settings.Epoch.Cast().Value());
            for (const auto& clusterAndTable : State_->TablesData->GetAllEpochTables(epoch)) {
                State_->TablesData->GetModifTable(clusterAndTable.first, clusterAndTable.second, epoch).SetConstraintsReady();
            }
        }
        return TStatus::Ok;
    }

private:
    void SetResultConstraint(const TExprNode::TPtr& input, const TExprNode& source, const TYtOutSection& outputs, TExprContext& ctx) {
        if (outputs.Size() == 1) {
            auto out = outputs.Item(0);
            input->CopyConstraints(out.Ref());
            if (auto empty = source.GetConstraint<TEmptyConstraintNode>()) {
                input->AddConstraint(empty);
                if (!out.Stat().Maybe<TCoVoid>() && State_->Types->IsConstraintCheckEnabled<TEmptyConstraintNode>()) {
                    YQL_ENSURE(out.Ref().GetConstraint<TEmptyConstraintNode>(), "Invalid Empty constraint");
                }
            }
        } else {
            TMultiConstraintNode::TMapType multiItems;
            auto multi = source.GetConstraint<TMultiConstraintNode>();
            auto empty = source.GetConstraint<TEmptyConstraintNode>();
            if (multi) {
                multiItems = multi->GetItems();
            }
            for (ui32 i = 0; i < outputs.Size(); ++i) {
                auto out = outputs.Item(i);
                bool addEmpty = false;
                if ((multi && !multiItems.has(i)) || empty) {
                    if (!out.Stat().Maybe<TCoVoid>() && State_->Types->IsConstraintCheckEnabled<TEmptyConstraintNode>()) {
                        YQL_ENSURE(out.Ref().GetConstraint<TEmptyConstraintNode>(), "Invalid Empty constraint");
                    }
                    addEmpty = true;
                }
                multiItems[i] = out.Ref().GetConstraintSet();
                if (addEmpty) {
                    multiItems[i].AddConstraint(ctx.MakeConstraint<TEmptyConstraintNode>());
                }
            }
            if (!multiItems.empty()) {
                input->AddConstraint(ctx.MakeConstraint<TMultiConstraintNode>(std::move(multiItems)));
            }
            if (empty) {
                input->AddConstraint(empty);
            }
        }
    }

    TStatus HandleDqProcessWrite(TExprBase input, TExprContext& ctx) {
        if (const auto status = HandleDefault(input, ctx); status != TStatus::Ok) {
            return status;
        }
        bool complete = input.Ref().GetState() >= TExprNode::EState::ExecutionComplete;
        if (!complete && input.Ref().HasResult()) {
            const auto& result = input.Ref().GetResult();
            complete = result.IsWorld();
        }
        if (complete)
            input.Ptr()->CopyConstraints(input.Cast<TYtDqProcessWrite>().Output().Item(0).Ref());
        else
            CopyExcept(input.Ptr().Get(), input.Cast<TYtDqProcessWrite>().Output().Item(0).Ref(), TEmptyConstraintNode::Name());
        return TStatus::Ok;
    }

    TStatus HandleTryFirst(TExprBase input, TExprContext&) {
        input.Ptr()->CopyConstraints(input.Ref().Tail());
        return TStatus::Ok;
    }
private:
    const TYtState::TPtr State_;
    const bool SubGraph;
};

}

THolder<IGraphTransformer> CreateYtDataSinkConstraintTransformer(TYtState::TPtr state, bool subGraph) {
    return THolder(new TYtDataSinkConstraintTransformer(state, subGraph));
}

}
