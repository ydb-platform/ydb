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

        if (cluster != "pg_catalog") {
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
        if (tableName == "pg_type") {
            FillPgTypeSchema(items, ctx);
        } else if (tableName == "pg_database") {
            FillPgDatabaseSchema(items, ctx);
        } else if (tableName == "pg_tablespace") {
            FillPgTablespaceSchema(items, ctx);
        } else if (tableName == "pg_shdescription") {
            FillPgShDescriptionSchema(items, ctx);
        } else if (tableName == "pg_trigger") {
            FillPgTriggerSchema(items, ctx);
        } else if (tableName == "pg_locks") {
            FillPgLocksSchema(items, ctx);
        } else if (tableName == "pg_stat_gssapi") {
            FillPgStatGssapiSchema(items, ctx);
        } else if (tableName == "pg_inherits") {
            FillPgInheritsSchema(items, ctx);
        } else if (tableName == "pg_stat_activity") {
            FillPgStatActivitySchema(items, ctx);
        } else if (tableName == "pg_timezone_names") {
            FillPgTimezoneNamesSchema(items, ctx);
        } else if (tableName == "pg_timezone_abbrevs") {
            FillPgTimezoneAbbrevsSchema(items, ctx);
        } else {
            ctx.AddError(TIssue(ctx.GetPosition(input->Child(TPgReadTable::idx_Table)->Pos()), TStringBuilder() << "Unsupported table: " << tableName));
            return TStatus::Error;
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
        return State_->Types->SetColumnOrder(*input, columnOrder, ctx);
    }

private:
    void FillPgTypeSchema(TVector<const TItemExprType*>& items, TExprContext& ctx) {
        AddColumn(items, ctx, "oid", "oid");
        AddColumn(items, ctx, "typname", "name");
        AddColumn(items, ctx, "typinput", "regproc");
        AddColumn(items, ctx, "typnamespace", "oid");
        AddColumn(items, ctx, "typtype", "char");
    }

    void FillPgDatabaseSchema(TVector<const TItemExprType*>& items, TExprContext& ctx) {
        AddColumn(items, ctx, "oid", "oid");
        AddColumn(items, ctx, "datname", "name");
        AddColumn(items, ctx, "encoding", "int4");
        AddColumn(items, ctx, "datallowconn", "bool");
        AddColumn(items, ctx, "datistemplate", "bool");
        AddColumn(items, ctx, "datdba", "oid");
    }

    void FillPgTablespaceSchema(TVector<const TItemExprType*>& items, TExprContext& ctx) {
        AddColumn(items, ctx, "oid", "oid");
        AddColumn(items, ctx, "spcname", "name");
    }

    void FillPgShDescriptionSchema(TVector<const TItemExprType*>& items, TExprContext& ctx) {
        AddColumn(items, ctx, "objoid", "oid");
        AddColumn(items, ctx, "classoid", "oid");
        AddColumn(items, ctx, "description", "text");
    }

    void FillPgTriggerSchema(TVector<const TItemExprType*>& items, TExprContext& ctx) {
        AddColumn(items, ctx, "tgrelid", "oid");
        AddColumn(items, ctx, "tgenabled", "char");
    }

    void FillPgLocksSchema(TVector<const TItemExprType*>& items, TExprContext& ctx) {
        AddColumn(items, ctx, "transactionid", "xid");
    }

    void FillPgStatGssapiSchema(TVector<const TItemExprType*>& items, TExprContext& ctx) {
        AddColumn(items, ctx, "encrypted", "bool");
        AddColumn(items, ctx, "gss_authenticated", "bool");
        AddColumn(items, ctx, "pid", "int4");
    }

    void FillPgInheritsSchema(TVector<const TItemExprType*>& items, TExprContext& ctx) {
        AddColumn(items, ctx, "inhrelid", "oid");
        AddColumn(items, ctx, "inhparent", "oid");
    }

    void FillPgStatActivitySchema(TVector<const TItemExprType*>& items, TExprContext& ctx) {
        AddColumn(items, ctx, "application_name", "text");
        AddColumn(items, ctx, "backend_start", "timestamptz");
        AddColumn(items, ctx, "backend_type", "text");
        AddColumn(items, ctx, "client_addr", "inet");
        AddColumn(items, ctx, "datname", "name");
        AddColumn(items, ctx, "pid", "int4");
        AddColumn(items, ctx, "query", "text");
        AddColumn(items, ctx, "query_start", "timestamptz");
        AddColumn(items, ctx, "state", "text");
        AddColumn(items, ctx, "state_change", "timestamptz");
        AddColumn(items, ctx, "usename", "name");
        AddColumn(items, ctx, "wait_event", "text");
        AddColumn(items, ctx, "wait_event_type", "text");
        AddColumn(items, ctx, "xact_start", "timestamptz");
    }

    void FillPgTimezoneNamesSchema(TVector<const TItemExprType*>& items, TExprContext& ctx) {
        AddColumn(items, ctx, "name", "text");
        AddColumn(items, ctx, "is_dst", "bool");
    }

    void FillPgTimezoneAbbrevsSchema(TVector<const TItemExprType*>& items, TExprContext& ctx) {
        AddColumn(items, ctx, "abbrev", "text");
        AddColumn(items, ctx, "is_dst", "bool");
    }

    void AddColumn(TVector<const TItemExprType*>& items, TExprContext& ctx, const TString& name, const TString& type) {
        items.push_back(ctx.MakeType<TItemExprType>(name, ctx.MakeType<TPgExprType>(NPg::LookupType(type).TypeId)));
    }

private:
    TPgState::TPtr State_;
};

THolder<TVisitorTransformerBase> CreatePgDataSourceTypeAnnotationTransformer(TPgState::TPtr state) {
    return MakeHolder<TPgDataSourceTypeAnnotationTransformer>(state);
}

}
