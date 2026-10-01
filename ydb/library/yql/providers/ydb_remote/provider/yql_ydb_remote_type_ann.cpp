#include <ydb/library/yql/providers/ydb_remote/common/provider_names.h>
#include "yql_ydb_remote_provider_impl.h"

#include <yql/essentials/core/sql_types/block.h>

#include <ydb/library/yql/providers/ydb_remote/expr_nodes/yql_ydb_remote_expr_nodes.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>

namespace NYql::NYdbRemote {
namespace {

using namespace NNodes;

class TTypeAnnotationTransformer final : public TVisitorTransformerBase {
public:
    explicit TTypeAnnotationTransformer(TState::TPtr state)
        : TVisitorTransformerBase(true)
        , State_(std::move(state))
    {
        AddHandler({TYdbRemoteReadTable::CallableName()}, Hndl(&TTypeAnnotationTransformer::HandleRead));
        AddHandler({TYdbRemoteSourceSettings::CallableName()}, Hndl(&TTypeAnnotationTransformer::HandleSource));
    }

private:
    const TStructExprType* SelectColumns(const TString& cluster, const TString& tableName,
            TExprNode& columns, TExprContext& ctx, TVector<TString>* order = nullptr) {
        const auto it = State_->Tables.find(TState::TTableKey(cluster, tableName));
        if (it == State_->Tables.end()) {
            ctx.AddError(TIssue(ctx.GetPosition(columns.Pos()), "Native YDB table metadata is missing"));
            return nullptr;
        }
        const auto& table = it->second;
        if (columns.IsCallable("Void")) {
            if (order) {
                *order = table.ColumnOrder;
            }
            return table.RowType;
        }
        if (!EnsureTuple(columns, ctx)) {
            return nullptr;
        }
        THashSet<TStringBuf> names;
        TVector<const TItemExprType*> items;
        for (const auto& name : columns.Children()) {
            if (!EnsureAtom(*name, ctx)) {
                return nullptr;
            }
            if (!names.insert(name->Content()).second) {
                ctx.AddError(TIssue(ctx.GetPosition(name->Pos()), "Duplicate native YDB column"));
                return nullptr;
            }
            const auto index = table.RowType->FindItem(name->Content());
            if (!index) {
                ctx.AddError(TIssue(ctx.GetPosition(name->Pos()), TStringBuilder() << "Unknown native YDB column: " << name->Content()));
                return nullptr;
            }
            items.emplace_back(table.RowType->GetItems()[*index]);
        }
        if (order) {
            for (const auto& name : table.ColumnOrder) {
                if (names.contains(name)) {
                    order->push_back(name);
                }
            }
        }
        return ctx.MakeType<TStructExprType>(items);
    }

    TStatus HandleRead(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureArgsCount(*input, 4, ctx) || !EnsureWorldType(*input->Child(0), ctx) ||
            !EnsureSpecificDataSource(*input->Child(1), YdbRemoteProviderName, ctx) ||
            !EnsureAtom(*input->Child(2), ctx)) {
            return TStatus::Error;
        }
        const TYdbRemoteReadTable read(input);
        TVector<TString> order;
        const auto* row = SelectColumns(read.DataSource().Cluster().StringValue(), read.Table().StringValue(),
            read.Columns().MutableRef(), ctx, &order);
        if (!row) {
            return TStatus::Error;
        }
        input->SetTypeAnn(ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
            read.World().Ref().GetTypeAnn(), ctx.MakeType<TListExprType>(row)}));
        return State_->Types->SetColumnOrder(*input, TColumnOrder(order), ctx);
    }

    TStatus HandleSource(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureArgsCount(*input, 5, ctx) || !EnsureWorldType(*input->Child(0), ctx) ||
            !EnsureAtom(*input->Child(1), ctx) || !EnsureAtom(*input->Child(2), ctx) ||
            !EnsureCallable(*input->Child(3), ctx) || !TCoSecureParam::Match(input->Child(3)) ||
            !EnsureTuple(*input->Child(4), ctx)) {
            return TStatus::Error;
        }
        const TYdbRemoteSourceSettings settings(input);
        const auto* row = SelectColumns(settings.Cluster().StringValue(), settings.Table().StringValue(),
            settings.Columns().MutableRef(), ctx);
        if (!row) {
            return TStatus::Error;
        }
        TVector<const TItemExprType*> items;
        for (const auto* column : row->GetItems()) {
            items.emplace_back(ctx.MakeType<TItemExprType>(column->GetName(), ctx.MakeType<TBlockExprType>(column->GetItemType())));
        }
        items.emplace_back(ctx.MakeType<TItemExprType>(BlockLengthColumnName,
            ctx.MakeType<TScalarExprType>(ctx.MakeType<TDataExprType>(EDataSlot::Uint64))));
        input->SetTypeAnn(ctx.MakeType<TStreamExprType>(ctx.MakeType<TStructExprType>(items)));
        return TStatus::Ok;
    }

    const TState::TPtr State_;
};

} // namespace

THolder<TVisitorTransformerBase> CreateTypeAnnotationTransformer(TState::TPtr state) {
    return MakeHolder<TTypeAnnotationTransformer>(std::move(state));
}

} // namespace NYql::NYdbRemote
