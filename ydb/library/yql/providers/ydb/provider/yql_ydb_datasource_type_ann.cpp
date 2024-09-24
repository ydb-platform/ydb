#include "yql_ydb_provider_impl.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/providers/ydb/expr_nodes/yql_ydb_expr_nodes.h>

#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>

#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

using namespace NNodes;

namespace {

class TYdbDataSourceTypeAnnotationTransformer : public TVisitorTransformerBase {
public:
    TYdbDataSourceTypeAnnotationTransformer(TYdbState::TPtr state)
        : TVisitorTransformerBase(true)
        , State_(state)
    {
        using TSelf = TYdbDataSourceTypeAnnotationTransformer;
        AddHandler({TYdbReadTable::CallableName()}, Hndl(&TSelf::HandleReadTable));
        AddHandler({TYdbReadTableScheme::CallableName()}, Hndl(&TSelf::HandleReadTableScheme));
        AddHandler({TYdbSourceSettings::CallableName()}, Hndl(&TSelf::HandleYdbSourceSettings));
    }

    TStatus HandleYdbSourceSettings(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 3U, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(TYdbSourceSettings::idx_Table), ctx)) {
            return TStatus::Error;
        }

        if (!TCoSecureParam::Match(input->Child(TYdbSourceSettings::idx_Token))) {
            ctx.AddError(TIssue(ctx.GetPosition(input->Child(TYdbSourceSettings::idx_Token)->Pos()), TStringBuilder() << "Expected " << TCoSecureParam::CallableName()));
            return TStatus::Error;
        }

        if (const auto columns = input->Child(TYdbSourceSettings::idx_Columns); !columns->IsCallable(TCoVoid::CallableName())) {
            if (!EnsureTupleOfAtoms(*columns, ctx)) {
                return TStatus::Error;
            }

            std::unordered_set<std::string_view> columnsSet(columns->ChildrenSize());
            for (const auto& child : columns->Children()) {
                if (const auto& name = child->Content(); !columnsSet.emplace(name).second) {
                    ctx.AddError(TIssue(ctx.GetPosition(child->Pos()), TStringBuilder() << "Duplicate column name: " << name));
                    return TStatus::Error;
                }
            }
        }

        input->SetTypeAnn(ctx.MakeType<TStreamExprType>(ctx.MakeType<TDataExprType>(EDataSlot::String)));
        return TStatus::Ok;
    }

    TStatus HandleReadTable(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureMinMaxArgsCount(*input, 4U, 5U, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureWorldType(*input->Child(TYdbReadTable::idx_World), ctx)) {
            return TStatus::Error;
        }

        if (!EnsureSpecificDataSource(*input->Child(TYdbReadTable::idx_DataSource), YdbProviderName, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(TYdbReadTable::idx_Table), ctx)) {
            return TStatus::Error;
        }

        if (input->ChildrenSize() > TYdbReadTable::idx_LimitHint && !EnsureSpecificDataType(*input->Child(TYdbReadTable::idx_LimitHint), EDataSlot::Uint64, ctx)) {
            return TStatus::Error;
        }

        std::optional<std::unordered_set<std::string_view>> columnsSet;
        if (const auto columns = input->Child(TYdbReadTable::idx_Columns); !columns->IsCallable(TCoVoid::CallableName())) {
            if (!EnsureTuple(*columns, ctx)) {
                return TStatus::Error;
            }

            columnsSet.emplace(columns->ChildrenSize());
            for (const auto& child : columns->Children()) {
                if (!EnsureAtom(*child, ctx)) {
                    return TStatus::Error;
                }

                if (const auto& name = child->Content(); !columnsSet->emplace(name).second) {
                    ctx.AddError(TIssue(ctx.GetPosition(child->Pos()), TStringBuilder() << "Duplicate column name: " << name));
                    return TStatus::Error;
                }
            }
        }

        TString cluster{ input->Child(TYdbReadTable::idx_DataSource)->Child(1)->Content() };
        TString table{ input->Child(TYdbReadTable::idx_Table)->Content() };
        const auto found = State_->Tables.find(std::make_pair(cluster, table));
        if (State_->Tables.cend() == found) {
            ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder() <<
                "No metadata for table: `" << cluster << "`.`" << table << "`"));
            return TStatus::Error;
        }

        auto itemType = found->second.ItemType;
        auto columnOrder = found->second.ColumnOrder;
        if (columnsSet) {
            auto items = itemType->GetItems();
            EraseIf(items, [&](const TItemExprType* item) { return !columnsSet->contains(item->GetName()); });
            EraseIf(columnOrder, [&](const TString& col) { return !columnsSet->contains(col); });
            itemType = ctx.MakeType<TStructExprType>(items);
        }

        input->SetTypeAnn(ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
            input->Child(TYdbReadTable::idx_World)->GetTypeAnn(),
            ctx.MakeType<TListExprType>(itemType)
        }));

        return State_->Types->SetColumnOrder(*input, TColumnOrder(columnOrder), ctx);
    }

    TStatus HandleReadTableScheme(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 3U, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureWorldType(*input->Child(TYdbReadTable::idx_World), ctx)) {
            return TStatus::Error;
        }

        if (!EnsureSpecificDataSource(*input->Child(TYdbReadTable::idx_DataSource), YdbProviderName, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(TYdbReadTable::idx_Table), ctx)) {
            return TStatus::Error;
        }


        TString cluster{ input->Child(TYdbReadTable::idx_DataSource)->Child(1)->Content() };
        TString table{ input->Child(TYdbReadTable::idx_Table)->Content() };
        if (!State_->Tables.contains(std::make_pair(cluster, table))) {
            ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder() <<
                "No metadata for table: `" << cluster << "`.`" << table << "`"));
            return TStatus::Error;
        }

        input->SetTypeAnn(ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
            input->Child(TYdbReadTable::idx_World)->GetTypeAnn(),
            ctx.MakeType<TDataExprType>(EDataSlot::Yson)
        }));

        return TStatus::Ok;
    }
private:
    const TYdbState::TPtr State_;
};

}

THolder<TVisitorTransformerBase> CreateYdbDataSourceTypeAnnotationTransformer(TYdbState::TPtr state) {
    return MakeHolder<TYdbDataSourceTypeAnnotationTransformer>(state);
}

} // namespace NYql
