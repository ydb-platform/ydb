#include "yql_solomon_provider_impl.h"

#include <ydb/library/yql/providers/solomon/expr_nodes/yql_solomon_expr_nodes.h>
#include <ydb/library/yql/providers/solomon/scheme/yql_solomon_scheme.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>

namespace NYql {

using namespace NNodes;

namespace {

bool ExtractSettingValue(const TExprNode& value, TStringBuf settingName, TExprContext& ctx, TStringBuf& settingValue) {
    if (value.IsAtom()) {
        settingValue = value.Content();
        return true;
    }

    if (!value.IsCallable({ "String", "Utf8" })) {
        ctx.AddError(TIssue(ctx.GetPosition(value.Pos()), TStringBuilder() << settingName << " must be literal value"));
        return false;
    }
    settingValue = value.Head().Content();
    return true;
}

bool ValidateDatetimeFormat(const TExprNode& value, TStringBuf settingName, TExprContext& ctx) {
    TStringBuf settingValue;
    if (!ExtractSettingValue(value, settingName, ctx, settingValue)) {
        return false;
    }

    TInstant unused;
    if (!TInstant::TryParseIso8601(settingValue, unused)) {
        ctx.AddError(TIssue(ctx.GetPosition(value.Pos()), TStringBuilder() << settingName << " must be correct datetime, e.g. 2010-03-27T21:27:00Z, but has " << settingValue));
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
        if (!EnsureArgsCount(*input, 10U, ctx)) {
            return TStatus::Error;
        }

        if (!TCoSecureParam::Match(input->Child(TSoSourceSettings::idx_Token))) {
            ctx.AddError(TIssue(ctx.GetPosition(input->Child(TSoSourceSettings::idx_Token)->Pos()), TStringBuilder() << "Expected " << TCoSecureParam::CallableName()));
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
        
        auto* scheme = BuildScheme(systemColumns, labelNames, ctx);
        if (!scheme) {
            return TStatus::Error;
        }

        auto& from = *input->Child(TSoSourceSettings::idx_From);
        if (!EnsureAtom(from, ctx) || !ValidateDatetimeFormat(from, "from"sv, ctx)) {
            return TStatus::Error;
        }

        auto& to = *input->Child(TSoSourceSettings::idx_To);
        if (!EnsureAtom(to, ctx) || !ValidateDatetimeFormat(to, "to"sv, ctx)) {
            return TStatus::Error;
        }

        auto& program = *input->Child(TSoSourceSettings::idx_Program);
        if (!EnsureAtom(program, ctx)) {
            return TStatus::Error;
        }

        auto& downsamplingDisabled = *input->Child(TSoSourceSettings::idx_DownsamplingDisabled);
        if (!EnsureAtom(downsamplingDisabled, ctx)) {
            return TStatus::Error;
        }

        TStringBuf downsamplingDisabledAsString;
        if (!ExtractSettingValue(downsamplingDisabled, "downsampling.disabled", ctx, downsamplingDisabledAsString)) {
            return TStatus::Error;
        }

        if (!IsIn({ "true"sv, "false"sv }, downsamplingDisabledAsString)) {
            ctx.AddError(TIssue(ctx.GetPosition(downsamplingDisabled.Pos()), TStringBuilder() << "downsampling.disabled must be either 'true' or 'false', but has " << downsamplingDisabledAsString));
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
        if (!EnsureAtom(downsamplingGridSec, ctx)) {
            return TStatus::Error;
        }

        TVector<const TTypeAnnotationNode*> items;
        items.reserve(scheme->GetSize());
        for (auto& item : scheme->GetItems()) {
            items.push_back(item->GetItemType());
        }
        const TTypeAnnotationNode* itemType = ctx.MakeType<TTupleExprType>(items);
        input->SetTypeAnn(ctx.MakeType<TStreamExprType>(itemType));
        return TStatus::Ok;
    }

    static const TStructExprType* BuildScheme(const TExprNode& systemColumns, const TExprNode& labelNames, TExprContext& ctx) {
        TCoAtomList systemColumnsAsList(&systemColumns);
        TCoAtomList labelNamesAsList(&labelNames);
        TVector<const TItemExprType*> columnTypes;
        columnTypes.reserve(systemColumnsAsList.Size() + labelNamesAsList.Size());
        const TTypeAnnotationNode* stringType = ctx.MakeType<TDataExprType>(EDataSlot::String);
        for (const auto& atom : systemColumnsAsList) {
            const TTypeAnnotationNode* type = nullptr;
            auto v = atom.Value();
            if (v == SOLOMON_SCHEME_TS) {
                type = ctx.MakeType<TDataExprType>(EDataSlot::Datetime);
            } else if (v == SOLOMON_SCHEME_VALUE) {
                type = ctx.MakeType<TDataExprType>(EDataSlot::Double);
            } else if (v == SOLOMON_SCHEME_LABELS) {
                type = ctx.MakeType<NYql::TDictExprType>(stringType, stringType);
            } else if (IsIn({ SOLOMON_SCHEME_KIND, SOLOMON_SCHEME_TYPE }, v)) {
                type = ctx.MakeType<TOptionalExprType>(stringType);
            } else {
                ctx.AddError(TIssue(ctx.GetPosition(systemColumns.Pos()), TStringBuilder() << "Unknown system column " << v));
                return nullptr;
            }

            columnTypes.push_back(ctx.MakeType<TItemExprType>(v, type));
        }

        for (const auto& atom : labelNamesAsList) {
            auto v = atom.Value();
            if (IsIn({ SOLOMON_SCHEME_TS, SOLOMON_SCHEME_KIND, SOLOMON_SCHEME_TYPE, SOLOMON_SCHEME_LABELS, SOLOMON_SCHEME_VALUE }, atom.Value())) {
                // tmp constraint
                ctx.AddError(TIssue(ctx.GetPosition(systemColumns.Pos()), TStringBuilder() << "System column should not be used as label name: " << v));
                return nullptr;
            }
            const TOptionalExprType* type = ctx.MakeType<TOptionalExprType>(stringType);
            columnTypes.push_back(ctx.MakeType<TItemExprType>(v, type));
        }

        return ctx.MakeType<TStructExprType>(columnTypes);
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
        if (!EnsureMinMaxArgsCount(*input, 5U, 6U, ctx)) {
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
            return State_->Types->SetColumnOrder(*input, columnOrder, ctx);
        }

        auto* scheme = BuildScheme(systemColumns, labelNames, ctx);
        if (!scheme) {
            return TStatus::Error;
        }

        input->SetTypeAnn(ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
            input->Child(TSoReadObject::idx_World)->GetTypeAnn(),
            ctx.MakeType<TListExprType>(scheme)
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
