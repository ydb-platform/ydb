#include "yql_yt_ytflow_optimize.h"
#include "yql_yt_helpers.h"

#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/utils/log/log.h>

#include <yt/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <yt/yql/providers/yt/lib/row_spec/yql_row_spec.h>
#include <yt/yql/providers/ytflow/expr_nodes/yql_ytflow_expr_nodes.h>


namespace NYql {

using namespace NNodes;


class TYtYtflowOptimization: public IYtflowOptimization {
public:
    TYtYtflowOptimization(TYtState* state)
        : State_(state)
    {
        Y_UNUSED(State_);
    }

public:
    TExprNode::TPtr ApplyExtractMembers(
        const TExprNode::TPtr& read, const TExprNode::TPtr& members, TExprContext& ctx
    ) override {
        auto maybeReadTable = TMaybeNode<TYtReadTable>(read);
        if (!maybeReadTable) {
            return read;
        }

        TVector<TYtSection> sections;
        for (auto section: maybeReadTable.Cast().Input()) {
            sections.push_back(UpdateInputFields(section, TExprBase(members), ctx));
        }

        YQL_CLOG(DEBUG, ProviderYt) << __FUNCTION__;

        return Build<TYtReadTable>(ctx, read->Pos())
            .InitFrom(maybeReadTable.Cast())
            .Input()
                .Add(std::move(sections))
                .Build()
            .Done().Ptr();
    }

    TExprNode::TPtr ApplyUnordered(const TExprNode::TPtr& read, TExprContext& ctx) override {
        auto maybeReadTable = TMaybeNode<TYtReadTable>(read);
        if (!maybeReadTable) {
            return read;
        }

        auto input = maybeReadTable.Cast().Input();

        TExprNode::TListType sections(input.Size());
        for (size_t index = 0; index < sections.size(); ++index) {
            sections[index] = MakeUnorderedSection<true>(input.Item(index), ctx).Ptr();
        }

        YQL_CLOG(DEBUG, ProviderYt) << __FUNCTION__;

        return Build<TYtReadTable>(ctx, read->Pos())
            .InitFrom(maybeReadTable.Cast())
            .Input()
                .Add(std::move(sections))
                .Build()
            .Done().Ptr();
    }

    TExprNode::TPtr TrimWriteContent(const TExprNode::TPtr& write, TExprContext& ctx) override {
        auto maybeWriteTable = TMaybeNode<TYtWriteTable>(write);
        if (!maybeWriteTable) {
            return write;
        }

        YQL_CLOG(DEBUG, ProviderYt) << __FUNCTION__;

        auto* listType = maybeWriteTable.Cast().Content().Ref().GetTypeAnn();
        auto* itemType = listType->Cast<TListExprType>()->GetItemType();

        return Build<TYtWriteTable>(ctx, write->Pos())
            .InitFrom(maybeWriteTable.Cast())
            .Content<TYtflowReadStub>()
                .World(ctx.NewWorld(TPositionHandle{}))
                .ItemType(ExpandType(TPositionHandle{}, *itemType, ctx))
                .Build()
            .Done().Ptr();
    }

private:
    TYtState* State_;
};

THolder<IYtflowOptimization> CreateYtYtflowOptimization(TYtState* state) {
    Y_ABORT_UNLESS(state);
    return MakeHolder<TYtYtflowOptimization>(state);
}

} // namespace NYql
