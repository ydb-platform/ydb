#include "yql_pg_provider_impl.h"

#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/providers/pg/expr_nodes/yql_pg_expr_nodes.h>

#include <ydb/library/yql/parser/pg_catalog/catalog.h>

namespace NYql {

using namespace NNodes;

class TPgDataSourceTypeAnnotationTransformer : public TVisitorTransformerBase {
public:
    TPgDataSourceTypeAnnotationTransformer(TPgState::TPtr state)
        : TVisitorTransformerBase(true)
        , State_(state)
    {
        using TSelf = TPgDataSourceTypeAnnotationTransformer;
        AddHandler({TPgReadTable::CallableName()}, Hndl(&TSelf::HandleReadTable));
        AddHandler({TPgTableContent::CallableName()}, Hndl(&TSelf::HandleTableContent));
    }

    TStatus HandleTableContent(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        return HandleReadTableImpl<TPgTableContent>(input, output, ctx, 4);
    }

    TStatus HandleReadTable(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        return HandleReadTableImpl<TPgReadTable>(input, output, ctx, 5);
    }

    template <typename TNode>
    TStatus HandleReadTableImpl(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx,
        ui32 expectedArgs) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, expectedArgs, ctx)) {
            return TStatus::Error;
        }

        TString cluster;
        if constexpr (std::is_same_v<TNode, TPgReadTable>) {
            if (!EnsureWorldType(*input->Child(TNode::idx_World), ctx)) {
                return TStatus::Error;
            }

            if (!EnsureSpecificDataSource(*input->Child(TNode::idx_DataSource), PgProviderName, ctx)) {
                return TStatus::Error;
            }

            cluster = input->Child(TNode::idx_DataSource)->Tail().Content();
        } else {
            if (!EnsureAtom(*input->Child(TNode::idx_Cluster), ctx)) {
                return TStatus::Error;
            }

            cluster = input->Child(TNode::idx_Cluster)->Content();
        }

        if (cluster != "pg_catalog" && cluster != "information_schema") {
            ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder() << "Unexpected cluster: " << cluster));
            return TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(TNode::idx_Table), ctx)) {
            return TStatus::Error;
        }

        TMaybe<THashSet<TStringBuf>> columnsSet;
        auto columns = input->Child(TNode::idx_Columns);
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

        auto settings = input->Child(TNode::idx_Settings);
        if (!EnsureTuple(*settings, ctx)) {
            return TStatus::Error;
        }

        for (const auto& setting: settings->Children()) {
            if (!EnsureTupleMinSize(*setting, 1, ctx)) {
                return TStatus::Error;
            }

            if (!EnsureAtom(setting->Head(), ctx)) {
                return TStatus::Error;
            }

            auto name = setting->Head().Content();
            ctx.AddError(TIssue(ctx.GetPosition(setting->Head().Pos()), TStringBuilder() << "Unsupported setting: " << name));
            return TStatus::Error;
        }

        auto tableName = input->Child(TNode::idx_Table)->Content();
        TVector<const TItemExprType*> items;
        auto columnsPtr = NPg::GetStaticColumns().FindPtr(NPg::TTableInfoKey{ cluster, TString(tableName) });
        if (!columnsPtr) {
            ctx.AddError(TIssue(ctx.GetPosition(input->Child(TPgReadTable::idx_Table)->Pos()), TStringBuilder() << "Unsupported table: " << tableName));
            return TStatus::Error;
        }

        for (const auto& c: *columnsPtr) {
            AddColumn(items, ctx, c.Name, c.UdtType);
        }

        const auto relKind = NPg::LookupStaticTable(NPg::TTableInfoKey{ cluster, TString(tableName) }).Kind;
        if (relKind == NPg::ERelKind::Relation) {
            AddSystemColumn(items, ctx, "tableoid", "oid");
            AddSystemColumn(items, ctx, "xmin", "xid");
            AddSystemColumn(items, ctx, "cmin", "cid");
            AddSystemColumn(items, ctx, "xmax", "xid");
            AddSystemColumn(items, ctx, "cmax", "cid");
            AddSystemColumn(items, ctx, "ctid", "tid");
        }

        TVector<TString> columnOrder;
        for (const auto& item : items) {
            columnOrder.emplace_back(TString(item->GetName()));
        }

        const TStructExprType* itemType = ctx.MakeType<TStructExprType>(items);
        if (columnsSet) {
            TVector<const TItemExprType*> items = itemType->GetItems();
            EraseIf(items, [&](const TItemExprType* item) { return !columnsSet->contains(item->GetName()); });
            EraseIf(columnOrder, [&](const TString& col) { return !columnsSet->contains(col); });
            itemType = ctx.MakeType<TStructExprType>(items);
        }

        const TTypeAnnotationNode* resType = ctx.MakeType<TListExprType>(itemType);
        if constexpr (std::is_same_v<TNode, TPgReadTable>) {
            resType = ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
                input->Child(TPgReadTable::idx_World)->GetTypeAnn(),
                resType
            });
        }

        input->SetTypeAnn(resType);
        return State_->Types->SetColumnOrder(*input, TColumnOrder(columnOrder), ctx);
    }

private:
    void AddColumn(TVector<const TItemExprType*>& items, TExprContext& ctx, const TString& name, const TString& type) {
        items.push_back(ctx.MakeType<TItemExprType>(name, ctx.MakeType<TPgExprType>(NPg::LookupType(type).TypeId)));
    }

    void AddSystemColumn(TVector<const TItemExprType*>& items, TExprContext& ctx, const TString& name, const TString& type) {
        AddColumn(items, ctx, YqlVirtualPrefix + name, type);
    }

private:
    TPgState::TPtr State_;
};

THolder<TVisitorTransformerBase> CreatePgDataSourceTypeAnnotationTransformer(TPgState::TPtr state) {
    return MakeHolder<TPgDataSourceTypeAnnotationTransformer>(state);
}

}
