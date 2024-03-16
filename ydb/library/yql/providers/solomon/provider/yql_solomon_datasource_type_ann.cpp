#include "yql_solomon_provider_impl.h"

#include <ydb/library/yql/providers/solomon/expr_nodes/yql_solomon_expr_nodes.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>

namespace NYql {

using namespace NNodes;

class TSolomonDataSourceTypeAnnotationTransformer : public TVisitorTransformerBase {
public:
    explicit TSolomonDataSourceTypeAnnotationTransformer(TSolomonState::TPtr state)
        : TVisitorTransformerBase(true)
        , State_(state)
    {
        using TSelf = TSolomonDataSourceTypeAnnotationTransformer;
        AddHandler({TSoReadObject::CallableName()}, Hndl(&TSelf::HandleRead));
        AddHandler({TSoObject::CallableName()}, Hndl(&TSelf::HandleSoObject));
        AddHandler({TSoSourceSettings::CallableName()}, Hndl(&TSelf::HandleSoSourceSettings));
    }

    TStatus HandleSoSourceSettings(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureArgsCount(*input, 3U, ctx)) {
            return TStatus::Error;
        }

        if (!TCoSecureParam::Match(input->Child(TSoSourceSettings::idx_Token))) {
            ctx.AddError(TIssue(ctx.GetPosition(input->Child(TSoSourceSettings::idx_Token)->Pos()), TStringBuilder() << "Expected " << TCoSecureParam::CallableName()));
            return TStatus::Error;
        }

        // todo: remove this hardcode
        const TTypeAnnotationNode* item1 = ctx.MakeType<TDataExprType>(EDataSlot::String);
        const TTypeAnnotationNode* item2 = ctx.MakeType<TDataExprType>(EDataSlot::String);
        const TTypeAnnotationNode* item3 = ctx.MakeType<TDataExprType>(EDataSlot::Datetime);
        const TTypeAnnotationNode* item4 = ctx.MakeType<TDataExprType>(EDataSlot::Double);
        const TTypeAnnotationNode* item5 = ctx.MakeType<TDictExprType>(item1, item1);
        TVector<const TTypeAnnotationNode*> items = {item1, item2, item3, item4, item5};
        const TTypeAnnotationNode* itemType = ctx.MakeType<TTupleExprType>(items);
        input->SetTypeAnn(ctx.MakeType<TStreamExprType>(itemType));
        return TStatus::Ok;
    }

    TStatus HandleSoObject(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureArgsCount(*input, 1U, ctx)) {
            return TStatus::Error;
        }

        // todo: check settings
        input->SetTypeAnn(ctx.MakeType<TUnitExprType>());
        return TStatus::Ok;
    }

    TStatus HandleRead(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureMinMaxArgsCount(*input, 4U, 5U, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureWorldType(*input->Child(TSoReadObject::idx_World), ctx)) {
            return TStatus::Error;
        }

        if (!EnsureSpecificDataSource(*input->Child(TSoReadObject::idx_DataSource), SolomonProviderName, ctx)) {
            return TStatus::Error;
        }

        const auto& rowTypeNode = *input->Child(TSoReadObject::idx_RowType);
        if (!EnsureType(rowTypeNode, ctx)) {
            return TStatus::Error;
        }

        const TTypeAnnotationNode* rowType = rowTypeNode.GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (!EnsureStructType(rowTypeNode.Pos(), *rowType, ctx)) {
            return TStatus::Error;
        }

        input->SetTypeAnn(ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
            input->Child(TSoReadObject::idx_World)->GetTypeAnn(),
            ctx.MakeType<TListExprType>(rowType)
        }));

        if (input->ChildrenSize() > TSoReadObject::idx_ColumnOrder) {
            auto& order = *input->Child(TSoReadObject::idx_ColumnOrder);
            if (!EnsureTupleOfAtoms(order, ctx)) {
                return TStatus::Error;
            }
            TVector<TString> columnOrder;
            THashSet<TStringBuf> uniqs;
            columnOrder.reserve(order.ChildrenSize());
            uniqs.reserve(order.ChildrenSize());

            for (auto& child : order.ChildrenList()) {
                TStringBuf col = child->Content();
                if (!uniqs.emplace(col).second) {
                    ctx.AddError(TIssue(ctx.GetPosition(child->Pos()), TStringBuilder() << "Duplicate column '" << col << "' in column order list"));
                    return TStatus::Error;
                }
                columnOrder.push_back(ToString(col));
            }
            return State_->Types->SetColumnOrder(*input, columnOrder, ctx);
        }

        return TStatus::Ok;
    }

private:
    TSolomonState::TPtr State_;
};

THolder<TVisitorTransformerBase> CreateSolomonDataSourceTypeAnnotationTransformer(TSolomonState::TPtr state) {
    return THolder(new TSolomonDataSourceTypeAnnotationTransformer(state));
}

} // namespace NYql
