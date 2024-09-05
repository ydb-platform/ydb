#include "yql_clickhouse_provider_impl.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/providers/clickhouse/expr_nodes/yql_clickhouse_expr_nodes.h>

#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>

#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

using namespace NNodes;

class TClickHouseDataSourceTypeAnnotationTransformer : public TVisitorTransformerBase {
public:
    TClickHouseDataSourceTypeAnnotationTransformer(TClickHouseState::TPtr state)
        : TVisitorTransformerBase(true)
        , State_(state)
    {
        using TSelf = TClickHouseDataSourceTypeAnnotationTransformer;
        AddHandler({TClReadTable::CallableName()}, Hndl(&TSelf::HandleReadTable));
        AddHandler({TClSourceSettings::CallableName()}, Hndl(&TSelf::HandleSourceSettings));
    }

    TStatus HandleSourceSettings(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureArgsCount(*input, 3U, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(TClSourceSettings::idx_Table), ctx)) {
            return TStatus::Error;
        }

        if (input->ChildrenSize() > TClSourceSettings::idx_Token && !TCoSecureParam::Match(input->Child(TClSourceSettings::idx_Token))) {
            ctx.AddError(TIssue(ctx.GetPosition(input->Child(TClSourceSettings::idx_Token)->Pos()), TStringBuilder() << "Expected " << TCoSecureParam::CallableName()));
            return TStatus::Error;
        }

        input->SetTypeAnn(ctx.MakeType<TStreamExprType>(ctx.MakeType<TDataExprType>(EDataSlot::String)));
        return TStatus::Ok;
    }

    TStatus HandleReadTable(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 5, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureWorldType(*input->Child(TClReadTable::idx_World), ctx)) {
            return TStatus::Error;
        }

        if (!EnsureSpecificDataSource(*input->Child(TClReadTable::idx_DataSource), ClickHouseProviderName, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(TClReadTable::idx_Table), ctx)) {
            return TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(TClReadTable::idx_Timezone), ctx)) {
            return TStatus::Error;
        }

        TMaybe<THashSet<TStringBuf>> columnsSet;
        auto columns = input->Child(TClReadTable::idx_Columns);
        if (!columns->IsCallable(TCoVoid::CallableName())) {
            if (!EnsureTuple(*columns, ctx)) {
                return TStatus::Error;
            }

            columnsSet.ConstructInPlace();
            for (auto& child : columns->Children()) {
                if (!EnsureAtom(*child, ctx)) {
                    return TStatus::Error;
                }

                auto name = child->Content();
                if (!columnsSet->insert(name).second) {
                    ctx.AddError(TIssue(ctx.GetPosition(child->Pos()), TStringBuilder() << "Duplicate column name: " << name));
                    return TStatus::Error;
                }
            }
        }

        TString cluster{ input->Child(TClReadTable::idx_DataSource)->Child(1)->Content() };
        TString table{ input->Child(TClReadTable::idx_Table)->Content() };
        auto found = State_->Tables.FindPtr(std::make_pair(cluster, table));
        if (!found) {
            ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder() <<
                "No metadata for table: `" << cluster << "`.`" << table << "`"));
            return TStatus::Error;
        }

        auto itemType = found->ItemType;
        auto columnOrder = found->ColumnOrder;
        if (columnsSet) {
            TVector<const TItemExprType*> items = itemType->GetItems();
            EraseIf(items, [&](const TItemExprType* item) { return !columnsSet->contains(item->GetName()); });
            EraseIf(columnOrder, [&](const TString& col) { return !columnsSet->contains(col); });
            itemType = ctx.MakeType<TStructExprType>(items);
        }

        input->SetTypeAnn(ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
            input->Child(TClReadTable::idx_World)->GetTypeAnn(),
            ctx.MakeType<TListExprType>(itemType)
        }));

        return State_->Types->SetColumnOrder(*input, TColumnOrder(columnOrder), ctx);
    }

private:
    TClickHouseState::TPtr State_;
};

THolder<TVisitorTransformerBase> CreateClickHouseDataSourceTypeAnnotationTransformer(TClickHouseState::TPtr state) {
    return MakeHolder<TClickHouseDataSourceTypeAnnotationTransformer>(state);
}

} // namespace NYql
