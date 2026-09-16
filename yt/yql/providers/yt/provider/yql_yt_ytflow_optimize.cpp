#include "yql_yt_ytflow_optimize.h"
#include "yql_yt_helpers.h"
#include "yql_yt_table.h"

#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/utils/log/log.h>

#include <yt/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <yt/yql/providers/yt/lib/row_spec/yql_row_spec.h>
#include <yt/yql/providers/yt/opt/yql_yt_key_selector.h>
#include <yt/yql/providers/ytflow/expr_nodes/yql_ytflow_expr_nodes.h>

#include <library/cpp/yt/string/format.h>
#include <library/cpp/iterator/zip.h>


namespace NYql {

using namespace NNodes;


class TYtYtflowOptimization: public TEmptyYtflowOptimization {
public:
    TYtYtflowOptimization(TYtState::TWeakPtr state)
        : State_(state)
    {
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

    TExprNode::TPtr ApplySort(
        const TExprNode::TPtr& write, const TExprNode::TPtr& sort, TExprContext& ctx
    ) override {
        auto maybeWriteTable = TMaybeNode<TYtWriteTable>(write);
        if (!maybeWriteTable) {
            return write;
        }

        auto maybeSort = TMaybeNode<TCoSort>(sort);
        YQL_ENSURE(maybeSort, "Unexpected node: " << sort->Content());

        auto checkAscendingSortOrder = [](const auto& sortDirection) {
            auto maybeSortDirection = sortDirection.template Maybe<TCoBool>();
            YQL_ENSURE(maybeSortDirection, "Unexpected node: " << sortDirection.Ref().Content());

            if (!::FromString<bool>(maybeSortDirection.Cast().Literal())) {
                return false;
            }

            return true;
        };

        auto keySelectorLambda = maybeSort.Cast().KeySelectorLambda();
        auto body = keySelectorLambda.Body();
        auto sortDirections = maybeSort.Cast().SortDirections();
        bool hasOnlySimpleKeys = true;
        bool hasOnlyAscendingSortDirection = true;
        if (auto maybeBodyList = body.Maybe<TExprList>()) {
            auto maybeSortDirectionsList = sortDirections.Maybe<TExprList>();
            YQL_ENSURE(maybeSortDirectionsList, "Unexpected node: " << sortDirections.Ref().Content());

            auto bodyList = maybeBodyList.Cast();
            auto sortDirectionsList = maybeSortDirectionsList.Cast();
            for (auto [bodyChild, sortDirectionsChild] : Zip(bodyList, sortDirectionsList)) {
                if (!bodyChild.Maybe<TCoMember>()) {
                    hasOnlySimpleKeys = false;
                }

                if (!checkAscendingSortOrder(sortDirectionsChild)) {
                    hasOnlyAscendingSortDirection = false;
                }
            }
        } else {
            if (!body.Maybe<TCoMember>()) {
                hasOnlySimpleKeys = false;
            }

            if (!checkAscendingSortOrder(sortDirections)) {
                hasOnlyAscendingSortDirection = false;
            }
        }

        if (!hasOnlySimpleKeys) {
            ctx.AddError(TIssue(ctx.GetPosition(keySelectorLambda.Pos()),
                "Only simple keys are supported in ORDER BY for ytflow engine"));
            return {};
        }

        if (!hasOnlyAscendingSortDirection) {
            ctx.AddError(TIssue(ctx.GetPosition(keySelectorLambda.Pos()),
                "Only ascending sort direction is supported in ORDER BY for ytflow engine"));
            return {};
        }

        TVector<TStringBuf> sortColumns;
        ExtractSimpleKeys(keySelectorLambda.Ref(), sortColumns);

        auto writeTable = maybeWriteTable.Cast();

        const auto* itemType = writeTable.Content().Ref().GetTypeAnn()
            ->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();

        for (auto column : sortColumns) {
            if (!itemType->FindItem(column)) {
                ctx.AddError(TIssue(ctx.GetPosition(keySelectorLambda.Pos()), NYT::Format(
                    "Sort column %Qv is missing in the written row type",  column)));
                return {};
            }
        }

        TKeySelectorBuilder builder(write->Pos(), ctx, false, itemType);
        builder.ProcessKeySelector(keySelectorLambda.Ptr(), sortDirections.Ptr());

        TYqlRowSpecInfo rowSpec;
        builder.FillRowSpecSort(rowSpec, false);
        rowSpec.SetType(itemType);
        rowSpec.UniqueKeys = true;

        auto ytState = State_.lock();
        YQL_ENSURE(ytState);

        auto tableInfo = TYtTableInfo(writeTable.Table());
        const auto& tableDescription = ytState->TablesData->GetTable(tableInfo.Cluster, tableInfo.Name, 0);

        if (!tableDescription.Meta->DoesExist) {
            tableInfo.RowSpec = MakeIntrusive<TYqlRowSpecInfo>(std::move(rowSpec));
        } else {
            TVector<std::pair<TString, bool>> tableSortColumns;
            for (const auto& [column, ascending] : tableDescription.RowSpec->GetForeignSort()) {
                if (!tableDescription.RowSpec->ExpressionColumns.contains(column)) {
                    tableSortColumns.emplace_back(column, ascending);
                }
            }

            if (rowSpec.GetForeignSort() != tableSortColumns) {
                TVector<TString> tableSortColumnNames;
                tableSortColumnNames.reserve(tableSortColumns.size());
                for (const auto& [column, _] : tableSortColumns) {
                    tableSortColumnNames.push_back(column);
                }

                ctx.AddError(TIssue(ctx.GetPosition(keySelectorLambda.Pos()), NYT::Format(
                    "ORDER BY columns do not match non expression key columns for table: %Qv (OrderByColumns: %v, TableKeyColumns: %v)",
                    tableInfo.Name, JoinSeq(", ", sortColumns), JoinSeq(", ", tableSortColumnNames))));
                return {};
            }
        }

        YQL_CLOG(DEBUG, ProviderYt) << __FUNCTION__;

        return Build<TYtWriteTable>(ctx, write->Pos())
            .InitFrom(writeTable)
            .Table(tableInfo.ToExprNode(ctx, writeTable.Table().Pos()).Cast<TYtTable>())
            .Done().Ptr();
    }

    TExprNode::TPtr TrimWriteContent(const TExprNode::TPtr& write, TExprContext& ctx) override {
        auto maybeWriteTable = TMaybeNode<TYtWriteTable>(write);
        if (!maybeWriteTable) {
            return write;
        }

        YQL_CLOG(DEBUG, ProviderYt) << __FUNCTION__;

        auto listType = maybeWriteTable.Cast().Content().Ref().GetTypeAnn();
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
    TYtState::TWeakPtr State_;
};

THolder<IYtflowOptimization> CreateYtYtflowOptimization(TYtState::TWeakPtr state) {
    YQL_ENSURE(!state.expired());
    return MakeHolder<TYtYtflowOptimization>(state);
}

} // namespace NYql
