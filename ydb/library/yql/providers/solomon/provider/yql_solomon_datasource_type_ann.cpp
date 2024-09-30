#include "yql_solomon_provider_impl.h"

#include <ydb/library/yql/providers/solomon/expr_nodes/yql_solomon_expr_nodes.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>

namespace NYql {

using namespace NNodes;

namespace {

bool ValidateDatetimeFormat(TStringBuf settingName, const TExprNode& settingValue, TExprContext& ctx) {
    TInstant unused;
    if (!TInstant::TryParseIso8601(settingValue.Content(), unused)) {
        ctx.AddError(TIssue(ctx.GetPosition(settingValue.Pos()), TStringBuilder() << settingName << " must be correct datetime, e.g. 2010-03-27T21:27:00Z, but has " << settingValue.Content()));
        return false;
    }
    return true;
}

}

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
        if (!EnsureArgsCount(*input, 12U, ctx)) {
            return TStatus::Error;
        }

        auto& project = *input->Child(TSoSourceSettings::idx_Project);
        if (!EnsureAtom(project, ctx)) {
            return TStatus::Error;
        }

        if (!TCoSecureParam::Match(input->Child(TSoSourceSettings::idx_Token))) {
            ctx.AddError(TIssue(ctx.GetPosition(input->Child(TSoSourceSettings::idx_Token)->Pos()), TStringBuilder() << "Expected " << TCoSecureParam::CallableName()));
            return TStatus::Error;
        }

        const auto& rowType = *input->Child(TSoSourceSettings::idx_RowType);
        if (!EnsureType(rowType, ctx)) {
            return TStatus::Error;
        }

        auto& systemColumns = *input->Child(TSoSourceSettings::idx_SystemColumns);
        if (!EnsureTupleOfAtoms(systemColumns, ctx)) {
            return TStatus::Error;
        }

        auto& labelNames = *input->Child(TSoSourceSettings::idx_LabelNames);
        if (!EnsureTupleOfAtoms(labelNames, ctx)) {
            return TStatus::Error;
        }
        
        auto& from = *input->Child(TSoSourceSettings::idx_From);
        if (!EnsureAtom(from, ctx) || !ValidateDatetimeFormat("from", from, ctx)) {
            return TStatus::Error;
        }

        auto& to = *input->Child(TSoSourceSettings::idx_To);
        if (!EnsureAtom(to, ctx) || !ValidateDatetimeFormat("to", to, ctx)) {
            return TStatus::Error;
        }

        auto& program = *input->Child(TSoSourceSettings::idx_Program);
        if (!EnsureAtom(program, ctx)) {
            return TStatus::Error;
        }

        if (program.Content().empty()) {
            ctx.AddError(TIssue(ctx.GetPosition(program.Pos()), "program must be specified"));
            return TStatus::Error;
        }

        auto& downsamplingDisabled = *input->Child(TSoSourceSettings::idx_DownsamplingDisabled);
        if (!downsamplingDisabled.IsCallable("Bool")) {
            ctx.AddError(TIssue(ctx.GetPosition(downsamplingDisabled.Pos()), "downsampling.disabled must be bool"));
            return TStatus::Error;
        }

        auto& downsamplingAggregation = *input->Child(TSoSourceSettings::idx_DownsamplingAggregation);
        if (!EnsureAtom(downsamplingAggregation, ctx)) {
            return TStatus::Error;
        }

        auto& downsamplingFill = *input->Child(TSoSourceSettings::idx_DownsamplingFill);
        if (!EnsureAtom(downsamplingFill, ctx)) {
            return TStatus::Error;
        }

        auto& downsamplingGridSec = *input->Child(TSoSourceSettings::idx_DownsamplingGridSec);
        if (!downsamplingGridSec.IsCallable("Uint32")) {
            ctx.AddError(TIssue(ctx.GetPosition(downsamplingGridSec.Pos()), "downsampling.grid_interval must be uint32 in seconds"));
            return TStatus::Error;
        }

        const auto type = rowType.GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        input->SetTypeAnn(ctx.MakeType<TStreamExprType>(type));
        return TStatus::Ok;
    }

    TStatus HandleSoObject(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureArgsCount(*input, 2U, ctx)) {
            return TStatus::Error;
        }

        auto& project = *input->Child(TSoObject::idx_Project);
        if (!EnsureAtom(project, ctx)) {
            return TStatus::Error;
        }

        input->SetTypeAnn(ctx.MakeType<TUnitExprType>());
        return TStatus::Ok;
    }

    TStatus HandleRead(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureMinMaxArgsCount(*input, 6U, 7U, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureWorldType(*input->Child(TSoReadObject::idx_World), ctx)) {
            return TStatus::Error;
        }

        if (!EnsureSpecificDataSource(*input->Child(TSoReadObject::idx_DataSource), SolomonProviderName, ctx)) {
            return TStatus::Error;
        }

        auto& systemColumns = *input->Child(TSoReadObject::idx_SystemColumns);
        if (!EnsureTupleOfAtoms(systemColumns, ctx)) {
            return TStatus::Error;
        }

        auto& labelNames = *input->Child(TSoReadObject::idx_LabelNames);
        if (!EnsureTupleOfAtoms(labelNames, ctx)) {
            return TStatus::Error;
        }

        const auto& rowType = *input->Child(TSoReadObject::idx_RowType);
        if (!EnsureType(rowType, ctx)) {
            return TStatus::Error;
        }

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
                    ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder() << "Duplicate column '" << col << "' in column order list"));
                    return TStatus::Error;
                }
                columnOrder.push_back(ToString(col));
            }
            return State_->Types->SetColumnOrder(*input, TColumnOrder(columnOrder), ctx);
        }

        const auto type = rowType.GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        input->SetTypeAnn(ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
            input->Child(TSoReadObject::idx_World)->GetTypeAnn(),
            ctx.MakeType<TListExprType>(type)
        }));

        return TStatus::Ok;
    }

private:
    TSolomonState::TPtr State_;
};

THolder<TVisitorTransformerBase> CreateSolomonDataSourceTypeAnnotationTransformer(TSolomonState::TPtr state) {
    return THolder(new TSolomonDataSourceTypeAnnotationTransformer(state));
}

} // namespace NYql
