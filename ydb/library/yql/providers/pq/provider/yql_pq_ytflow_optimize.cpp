#include "yql_pq_ytflow_optimize.h"

#include <yt/yql/providers/ytflow/expr_nodes/yql_ytflow_expr_nodes.h>

namespace NYql {

using namespace NNodes;

class TPqYtflowOptimization : public TEmptyYtflowOptimization {
public:
    TPqYtflowOptimization(const TPqState::TPtr& state)
        : State_(state.Get())
    {
    }

    TExprNode::TPtr ApplyExtractMembers(
        const TExprNode::TPtr& read, const TExprNode::TPtr& members, TExprContext& ctx
    ) override {
        auto maybeReadTopic = TMaybeNode<TPqReadTopic>(read);
        if (!maybeReadTopic) {
            return read;
        }

        auto readTopic = maybeReadTopic.Cast();
        auto topic = readTopic.Topic();
        auto rowSpecType = topic.RowSpec().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        auto membersAtomList = TCoAtomList(members);
        THashSet<TString> membersSet(membersAtomList.begin(), membersAtomList.end());

        TVector<TCoAtom> columns;
        TVector<const TItemExprType*> extractedMemberTypes;
        for (auto* memberType : rowSpecType->Cast<TStructExprType>()->GetItems()) {
            if (membersSet.contains(memberType->GetName())) {
                columns.push_back(Build<TCoAtom>(ctx, read->Pos())
                    .Value(memberType->GetName())
                    .Done());
                extractedMemberTypes.push_back(memberType);
            }
        }

        TVector<TCoNameValueTuple> extractedMetadata;
        for (auto metadata : topic.Metadata()) {
            auto metadataAtom = metadata.Value().Maybe<TCoAtom>().Cast();
            if (membersSet.contains(metadataAtom.StringValue())) {
                columns.push_back(metadataAtom);
                extractedMetadata.push_back(metadata);
            }
        }

        return Build<TPqReadTopic>(ctx, read->Pos())
            .InitFrom(maybeReadTopic.Cast())
            .Topic<TPqTopic>()
                .InitFrom(topic)
                .RowSpec(ExpandType(TPositionHandle{}, *ctx.MakeType<TStructExprType>(extractedMemberTypes), ctx))
                .Metadata().Add(extractedMetadata).Build()
                .Build()
            .Columns<TCoAtomList>()
                .Add(columns)
                .Build()
            .Done().Ptr();
    }

    TExprNode::TPtr ApplyUnordered(const TExprNode::TPtr& read, TExprContext& ctx) override {
        auto maybeReadTopic = TMaybeNode<TPqReadTopic>(read);
        if (!maybeReadTopic) {
            return read;
        }

        return Build<TCoUnordered>(ctx, read->Pos())
            .Input<TPqReadTopic>()
                .InitFrom(maybeReadTopic.Cast())
                .Build()
            .Done().Ptr();
    }

    TExprNode::TPtr TrimWriteContent(const TExprNode::TPtr& write, TExprContext& ctx) override {
        auto maybeWriteTopic = TMaybeNode<TPqWriteTopic>(write);
        if (!maybeWriteTopic) {
            return write;
        }

        auto listType = maybeWriteTopic.Cast().Input().Ref().GetTypeAnn();
        auto* itemType = listType->Cast<TListExprType>()->GetItemType();

        return Build<TPqWriteTopic>(ctx, write->Pos())
            .InitFrom(maybeWriteTopic.Cast())
            .Input<TYtflowReadStub>()
                .World(ctx.NewWorld(TPositionHandle{}))
                .ItemType(ExpandType(TPositionHandle{}, *itemType, ctx))
                .Build()
            .Done().Ptr();
    }

private:
    TPqState* State_;
};

THolder<IYtflowOptimization> CreatePqYtflowOptimization(const TPqState::TPtr& state) {
    YQL_ENSURE(state);
    return MakeHolder<TPqYtflowOptimization>(state);
}

} // namespace NYql
