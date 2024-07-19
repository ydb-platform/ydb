#include "yql_yt_op_settings.h"

#include <ydb/library/yql/providers/yt/lib/expr_traits/yql_expr_traits.h>
#include <ydb/library/yql/providers/yt/common/yql_yt_settings.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/yql_opt_utils.h>

#include <util/string/cast.h>
#include <util/generic/hash_set.h>

#include <library/cpp/yson/node/node_io.h>


namespace NYql {

namespace {

bool ValidateColumnSettings(TExprNode& columnsSettings, TExprContext& ctx, TVector<TString>& columns, bool maybeEmpty) {
    if (maybeEmpty) {
        if (!EnsureTuple(columnsSettings, ctx)) {
            return false;
        }
    } else {
        if (!EnsureTupleMinSize(columnsSettings, 1, ctx)) {
            return false;
        }
    }

    for (auto& child : columnsSettings.Children()) {
        if (!EnsureAtom(*child, ctx)) {
            return false;
        }
        columns.push_back(TString(child->Content()));
    }
    return true;
}

bool ValidateColumnPairSettings(TExprNode& columnsSettings, TExprContext& ctx, TVector<TString>& columns) {
    if (!EnsureTupleMinSize(columnsSettings, 1, ctx)) {
        return false;
    }

    for (auto& child : columnsSettings.Children()) {
        if (child->IsList()) {
            if (!EnsureTupleSize(*child, 2, ctx)) {
                return false;
            }
            if (!EnsureAtom(*child->Child(0), ctx)) {
                return false;
            }
            if (!EnsureAtom(*child->Child(1), ctx)) {
                return false;
            }
            bool res = false;
            if (!TryFromString(child->Child(1)->Content(), res)) {
                ctx.AddError(TIssue(ctx.GetPosition(child->Child(1)->Pos()), TStringBuilder() << "Expected bool value, but got: " << child->Child(1)->Content()));
                return false;
            }
            columns.emplace_back(child->Child(0)->Content());
        } else {
            if (HasError(child->GetTypeAnn(), ctx)) {
                return false;
            }

            if (!child->IsAtom()) {
                ctx.AddError(TIssue(ctx.GetPosition(child->Pos()), TStringBuilder() << "Expected tuple or atom, but got: " << child->Type()));
                return false;
            }
            columns.emplace_back(child->Content());
        }
    }
    return true;
}

bool ValidateColumnSubset(const TPositionHandle& pos, TExprContext& ctx, const TVector<TString>& sortBy, const TVector<TString>& reduceBy) {

    if (sortBy.size() < reduceBy.size()) {
        ctx.AddError(TIssue(ctx.GetPosition(pos), "reduceBy column list must be prefix of sortBy column list"));
        return false;
    }
    for (size_t i = 0; i < reduceBy.size(); ++i) {
        if (sortBy[i] != reduceBy[i]) {
            ctx.AddError(TIssue(ctx.GetPosition(pos), "reduceBy column list must be prefix of sortBy column list"));
            return false;
        }
    }
    return true;
}

const THashSet<TStringBuf> SYS_COLUMNS = {
    "path",
    "record",
    "index",
    "num",
};

} // unnamed

bool ValidateSettings(const TExprNode& settingsNode, EYtSettingTypes accepted, TExprContext& ctx) {
    TMaybe<TVector<TString>> sortBy;
    TMaybe<TVector<TString>> reduceBy;
    bool isTruncate = true;
    EYtSettingTypes used;
    for (auto setting: settingsNode.Children()) {
        if (!EnsureTupleMinSize(*setting, 1, ctx)) {
            return false;
        }
        auto nameNode = setting->Child(0);
        if (!EnsureAtom(*nameNode, ctx)) {
            return false;
        }
        EYtSettingType type;
        if (!TryFromString(nameNode->Content(), type)) {
            ctx.AddError(TIssue(ctx.GetPosition(nameNode->Pos()), TStringBuilder() << "Unknown setting name: " << nameNode->Content()));
            return false;
        }
        if (!accepted.HasFlags(type)) {
            ctx.AddError(TIssue(ctx.GetPosition(nameNode->Pos()), TStringBuilder() << "Setting " << nameNode->Content() << " is not accepted here"));
            return false;
        }
        used |= type;

        TIssueScopeGuard issueScope(ctx.IssueManager, [setting, type, &ctx]() {
            return MakeIntrusive<TIssue>(ctx.GetPosition(setting->Pos()), TStringBuilder() << "Setting " << type);
        });

        switch (type) {
        case EYtSettingType::KeyFilter: {
            if (!EnsureTupleSize(*setting, 2, ctx)) {
                return false;
            }
            auto value = setting->Child(1);
            if (!EnsureTupleMaxSize(*value, 2, ctx)) {
                return false;
            }
            if (value->ChildrenSize() > 0) {
                if (value->ChildrenSize() == 2) {
                    if (!EnsureAtom(*value->Child(1), ctx)) {
                        return false;
                    }
                    size_t tableIndex = 0;
                    if (!TryFromString(value->Child(1)->Content(), tableIndex)) {
                        ctx.AddError(TIssue(ctx.GetPosition(value->Child(1)->Pos()), TStringBuilder()
                            << "Bad table index value: " << value->Child(1)->Content()));
                        return false;
                    }
                }
                auto andList = value->Child(0);
                if (!EnsureTupleMinSize(*andList, 1, ctx)) {
                    return false;
                }
                for (auto keyPredicates: andList->Children()) {
                    if (!EnsureTupleSize(*keyPredicates, 2, ctx)) {
                        return false;
                    }

                    if (!EnsureAtom(*keyPredicates->Child(0), ctx)) {
                        return false;
                    }
                    if (keyPredicates->Child(0)->Content().empty()) {
                        ctx.AddError(TIssue(ctx.GetPosition(keyPredicates->Child(0)->Pos()), "Column name should be a non-empty atom"));
                        return false;
                    }
                    if (!EnsureTupleMinSize(*keyPredicates->Child(1), 1, ctx)) {
                        return false;
                    }

                    for (auto cmp: keyPredicates->Child(1)->Children()) {
                        if (!EnsureTupleSize(*cmp, 2, ctx)) {
                            return false;
                        }

                        if (!EnsureAtom(*cmp->Child(0), ctx)) {
                            return false;
                        }
                        if (cmp->Child(0)->Content().empty()) {
                            ctx.AddError(TIssue(ctx.GetPosition(cmp->Child(0)->Pos()), "Comparison should be a non-empty atom"));
                            return false;
                        }
                        if (!IsRangeComparison(cmp->Child(0)->Content())) {
                            ctx.AddError(TIssue(ctx.GetPosition(cmp->Child(0)->Pos()), TStringBuilder()
                                << "Unsupported compare operation "
                                << TString{cmp->Child(0)->Content()}.Quote()));
                            return false;
                        }

                        if (!IsNull(*cmp->Child(1))) {
                            bool isOptional = false;
                            const TDataExprType* dataType = nullptr;
                            if (!EnsureDataOrOptionalOfData(*cmp->Child(1), isOptional, dataType, ctx)) {
                                return false;
                            }
                            if (!EnsureComparableDataType(cmp->Child(1)->Pos(), dataType->GetSlot(), ctx)) {
                                return false;
                            }
                        }
                    }
                }
            }
            break;
        }
        case EYtSettingType::KeyFilter2: {
            if (!EnsureTupleSize(*setting, 2, ctx)) {
                return false;
            }
            auto value = setting->Child(1);
            if (!EnsureTupleMaxSize(*value, 3, ctx)) {
                return false;
            }
            if (value->ChildrenSize() > 0) {
                if (!EnsureTupleMinSize(*value, 2, ctx)) {
                    return false;
                }
                if (value->ChildrenSize() == 3) {
                    if (!EnsureTupleOfAtoms(value->Tail(), ctx)) {
                        return false;
                    }

                    THashSet<size_t> indexes;
                    for (auto& indexNode : value->Tail().ChildrenList()) {
                        size_t tableIndex = 0;
                        if (!TryFromString(indexNode->Content(), tableIndex)) {
                            ctx.AddError(TIssue(ctx.GetPosition(indexNode->Pos()), TStringBuilder()
                                << "Bad table index value: " << indexNode->Content()));
                            return false;
                        }
                        if (!indexes.insert(tableIndex).second) {
                            ctx.AddError(TIssue(ctx.GetPosition(indexNode->Pos()), TStringBuilder()
                                << "Duplicate table index value: " << tableIndex));
                            return false;
                        }
                    }
                }

                auto& computeNode = value->Head();
                const TTypeAnnotationNode* computeNodeType = computeNode.GetTypeAnn();
                YQL_ENSURE(computeNodeType);
                if (!EnsureListType(computeNode.Pos(), *computeNodeType, ctx)) {
                    return false;
                }

                auto computeNodeListItemType = computeNodeType->Cast<TListExprType>()->GetItemType();
                if (!EnsureTupleTypeSize(computeNode.Pos(), computeNodeListItemType, 2, ctx)) {
                    return false;
                }

                auto computeNodeTupleItems = computeNodeListItemType->Cast<TTupleExprType>()->GetItems();
                YQL_ENSURE(computeNodeTupleItems.size() == 2);
                if (!IsSameAnnotation(*computeNodeTupleItems[0], *computeNodeTupleItems[1])) {
                    ctx.AddError(TIssue(ctx.GetPosition(computeNode.Pos()), TStringBuilder()
                        << "Expecting compute expression to return list of 2-element tuples of same type, got: "
                        << *computeNodeType));
                    return false;
                }

                if (!EnsureTupleType(computeNode.Pos(), *computeNodeTupleItems.front(), ctx)) {
                    return false;
                }

                auto rangeTypeItems = computeNodeTupleItems.front()->Cast<TTupleExprType>()->GetItems();
                if (rangeTypeItems.size() < 2) {
                    ctx.AddError(TIssue(ctx.GetPosition(computeNode.Pos()), TStringBuilder()
                        << "Expecting range boundary to be a tuple with at least two elements, got: "
                        << *computeNodeTupleItems.front()));
                    return false;
                }

                if (!EnsureSpecificDataType(computeNode.Pos(), *rangeTypeItems.back(), EDataSlot::Int32,ctx)) {
                    return false;
                }

                for (size_t i = 0; i + 1 < rangeTypeItems.size(); ++i) {
                    auto boundaryType = rangeTypeItems[i];
                    YQL_ENSURE(boundaryType);
                    if (!EnsureOptionalType(computeNode.Pos(), *boundaryType, ctx)) {
                        return false;
                    }

                    bool isOptional;
                    const TDataExprType* data = nullptr;
                    if (!EnsureDataOrOptionalOfData(computeNode.Pos(), RemoveOptionalType(boundaryType), isOptional, data, ctx)) {
                        return false;
                    }

                    if (!EnsureComparableDataType(computeNode.Pos(), data->GetSlot(), ctx)) {
                        return false;
                    }
                }

                auto& settingsNode = *value->Child(1);
                if (!EnsureTuple(settingsNode, ctx)) {
                    return false;
                }

                for (auto& setting : settingsNode.ChildrenList()) {
                    if (!EnsureTupleMinSize(*setting, 1, ctx)) {
                        return false;
                    }

                    if (!EnsureAtom(setting->Head(), ctx)) {
                        return false;
                    }

                    auto name = setting->Head().Content();
                    if (name == "usedKeys") {
                        if (!EnsureTupleSize(*setting, 2, ctx)) {
                            return false;
                        }

                        if (!EnsureTupleMinSize(setting->Tail(), 1,  ctx)) {
                            return false;
                        }

                        THashSet<TStringBuf> keys;
                        for (auto& key : setting->Tail().ChildrenList()) {
                            if (!EnsureAtom(*key, ctx)) {
                                return false;
                            }

                            if (!keys.insert(key->Content()).second) {
                                ctx.AddError(TIssue(ctx.GetPosition(key->Pos()), TStringBuilder()
                                    << "Duplicate key column name: " << key->Content()));
                                return false;
                            }
                        }
                    } else {
                        ctx.AddError(TIssue(ctx.GetPosition(setting->Head().Pos()), TStringBuilder()
                            << "Unknown option : '" << setting->Head().Content() << "' for "
                            << ToString(EYtSettingType::KeyFilter).Quote()));
                        return false;
                    }
                }
            }
            break;
        }
        case EYtSettingType::Take:
        case EYtSettingType::Skip:
            if (!EnsureTupleSize(*setting, 2, ctx)) {
                return false;
            }
            if (!EnsureSpecificDataType(*setting->Child(1), EDataSlot::Uint64, ctx)) {
                return false;
            }
            break;
        case EYtSettingType::UserSchema:
        case EYtSettingType::UserColumns: {
            if (!EnsureTupleSize(*setting, 2, ctx)) {
                return false;
            }
            if (setting->Child(1)->GetTypeAnn()) {
                if (!EnsureType(*setting->Child(1), ctx)) {
                    return false;
                }
                auto type = setting->Child(1)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
                if (!EnsureStructType(setting->Child(1)->Pos(), *type, ctx)) {
                    return false;
                }

                if (!EnsurePersistableType(setting->Child(1)->Pos(), *type, ctx)) {
                    return false;
                }
            }

            break;
        }
        case EYtSettingType::StatColumns: {
            if (!EnsureTupleSize(*setting, 2, ctx)) {
                return false;
            }
            TVector<TString> columns;
            const bool maybeEmpty = true;
            if (!ValidateColumnSettings(*setting->Child(1), ctx, columns, maybeEmpty)) {
                return false;
            }
            break;
        }
        case EYtSettingType::SysColumns: {
            if (!EnsureTupleSize(*setting, 2, ctx)) {
                return false;
            }
            TVector<TString> columns;
            const bool maybeEmpty = false;
            if (!ValidateColumnSettings(*setting->Child(1), ctx, columns, maybeEmpty)) {
                return false;
            }
            for (auto col: columns) {
                if (!SYS_COLUMNS.contains(col)) {
                    ctx.AddError(TIssue(ctx.GetPosition(setting->Child(1)->Pos()), TStringBuilder()
                        << "Unsupported system column " << col.Quote()));
                    return false;
                }
            }
            break;
        }
        case EYtSettingType::InferScheme:
        case EYtSettingType::ForceInferScheme:
            if (!EnsureTupleMinSize(*setting, 1, ctx)) {
                return false;
            }
            if (!EnsureTupleMaxSize(*setting, 2, ctx)) {
                return false;
            }
            if (setting->ChildrenSize() == 2) {
                if (!EnsureAtom(setting->Tail(), ctx)) {
                    return false;
                }
                ui32 val;
                if (!TryFromString(setting->Tail().Content(), val) || val < 1 || val > 1000) {
                    ctx.AddError(TIssue(ctx.GetPosition(setting->Tail().Pos()), TStringBuilder()
                        << "Bad value " << TString{setting->Tail().Content()}.Quote()));
                    return false;
                }
            }
            break;
        case EYtSettingType::Ordered:
        case EYtSettingType::Initial:
        case EYtSettingType::DoNotFailOnInvalidSchema:
        case EYtSettingType::DirectRead:
        case EYtSettingType::Scheme:
        case EYtSettingType::WeakConcat:
        case EYtSettingType::IgnoreNonExisting:
        case EYtSettingType::WarnNonExisting:
        case EYtSettingType::ForceTransform:
        case EYtSettingType::CombineChunks:
        case EYtSettingType::WithQB:
        case EYtSettingType::Inline:
        case EYtSettingType::WeakFields:
        case EYtSettingType::Sharded:
        case EYtSettingType::XLock:
        case EYtSettingType::JoinReduce:
        case EYtSettingType::Unordered:
        case EYtSettingType::NonUnique:
        case EYtSettingType::KeepSorted:
        case EYtSettingType::KeySwitch:
        case EYtSettingType::IgnoreTypeV3:
        case EYtSettingType::NoDq:
        case EYtSettingType::Split:
        case EYtSettingType::KeepMeta:
        case EYtSettingType::MonotonicKeys:
            if (!EnsureTupleSize(*setting, 1, ctx)) {
                return false;
            }
            break;
        case EYtSettingType::Flow:
        case EYtSettingType::Anonymous:
            if (!EnsureTupleMinSize(*setting, 1, ctx)) {
                return false;
            }
            if (!EnsureTupleMaxSize(*setting, 2, ctx)) {
                return false;
            }
            if (setting->ChildrenSize() == 2) {
                if (!EnsureAtom(*setting->Child(1), ctx)) {
                    return false;
                }
            }
            break;
        case EYtSettingType::FirstAsPrimary:
            if (!EnsureTupleMinSize(*setting, 1, ctx)) {
                return false;
            }
            if (!EnsureTupleMaxSize(*setting, 2, ctx)) {
                return false;
            }
            if (setting->ChildrenSize() == 2) {
                for (auto childSetting : setting->Child(1)->Children()) {
                    if (!EnsureTupleMinSize(*childSetting, 1, ctx)) {
                        return false;
                    }
                    if (!EnsureAtom(*childSetting->Child(0), ctx)) {
                        return false;
                    }

                    const auto childSettingName = childSetting->Child(0)->Content();
                    if (childSettingName == MaxJobSizeForFirstAsPrimaryName) {
                        if (!EnsureTupleSize(*childSetting, 2, ctx)) {
                            return false;
                        }
                        ui64 value;
                        if (!EnsureAtom(*childSetting->Child(1), ctx)) {
                            return false;
                        }
                        if (!TryFromString(childSetting->Child(1)->Content(), value)) {
                            ctx.AddError(TIssue(ctx.GetPosition(childSetting->Child(1)->Pos()), TStringBuilder()
                                << "Bad value " << TString{childSetting->Child(1)->Content()}.Quote()));
                            return false;
                        }
                        if (!value) {
                            ctx.AddError(TIssue(ctx.GetPosition(childSetting->Child(1)->Pos()), TStringBuilder()
                                << MaxJobSizeForFirstAsPrimaryName << " value should not be zero"));
                            return false;
                        }
                    } else if (childSettingName == JoinReduceForSecondAsPrimaryName) {
                        if (!EnsureTupleSize(*childSetting, 1, ctx)) {
                            return false;
                        }
                    } else {
                        ctx.AddError(TIssue(ctx.GetPosition(childSetting->Pos()), TStringBuilder()
                            << "Unknown " << ToString(EYtSettingType::FirstAsPrimary) << " subsetting " << TString{childSettingName}.Quote()));
                        return false;
                    }
                }
            }
            break;
        case EYtSettingType::JobCount:
        case EYtSettingType::MemUsage:
        case EYtSettingType::ItemsCount:
        case EYtSettingType::RowFactor: {
            ui64 value;
            if (!EnsureAtom(*setting->Child(1), ctx)) {
                return false;
            }
            if (!TryFromString(setting->Child(1)->Content(), value)) {
                ctx.AddError(TIssue(ctx.GetPosition(setting->Child(1)->Pos()), TStringBuilder()
                    << "Bad value " << TString{setting->Child(1)->Content()}.Quote()));
                return false;
            }
            if ((EYtSettingType::JobCount == type || EYtSettingType::FirstAsPrimary == type) && 0 == value) {
                ctx.AddError(TIssue(ctx.GetPosition(setting->Child(1)->Pos()), "Value should not be zero"));
                return false;
            }
            break;
        }
        case EYtSettingType::View:
        case EYtSettingType::OpHash:
            if (!EnsureArgsCount(*setting, 2, ctx)) {
                return false;
            }
            if (!EnsureAtom(*setting->Child(1), ctx)) {
                return false;
            }
            break;
        case EYtSettingType::Mode: {
            if (!EnsureTupleSize(*setting, 2, ctx)) {
                return false;
            }
            if (!EnsureAtom(*setting->Child(1), ctx)) {
                return false;
            }
            EYtWriteMode mode;
            if (!TryFromString(setting->Child(1)->Content(), mode)) {
                ctx.AddError(TIssue(ctx.GetPosition(setting->Child(1)->Pos()), TStringBuilder()
                    << "Unsupported mode value " << TString{setting->Child(1)->Content()}.Quote()));
                return false;
            }
            isTruncate = EYtWriteMode::Renew == mode;
            break;
        }
        case EYtSettingType::Limit:
            if (!EnsureTupleSize(*setting, 2, ctx)) {
                return false;
            }
            if (!EnsureTuple(*setting->Child(1), ctx)) {
                return false;
            }
            for (auto expr: setting->Child(1)->Children()) {
                if (!EnsureTuple(*expr, ctx)) {
                    return false;
                }
                for (auto item: expr->Children()) {
                    if (!EnsureTupleMinSize(*item, 1, ctx)) {
                        return false;
                    }
                    if (!EnsureAtom(*item->Child(0), ctx)) {
                        return false;
                    }
                    if (FromString<EYtSettingType>(item->Child(0)->Content()) & (EYtSettingType::Take | EYtSettingType::Skip)) {
                        if (!EnsureArgsCount(*item, 2, ctx)) {
                            return false;
                        }

                        if (!EnsureSpecificDataType(*item->Child(1), EDataSlot::Uint64, ctx)) {
                            return false;
                        }
                    } else {
                        ctx.AddError(TIssue(ctx.GetPosition(item->Pos()), TStringBuilder()
                            << "Unsupported item " << TString{item->Child(0)->Content()}.Quote()));
                        return false;
                    }
                }
            }
            break;
        case EYtSettingType::ReduceFilterBy:
        case EYtSettingType::UniqueBy: {
            if (!EnsureTupleSize(*setting, 2, ctx)) {
                return false;
            }
            TVector<TString> columns;
            if (!ValidateColumnSettings(*setting->Child(1), ctx, columns, EYtSettingType::ReduceFilterBy == type)) {
                return false;
            }
            break;
        }
        case EYtSettingType::SortLimitBy: {
            if (!EnsureTupleSize(*setting, 2, ctx)) {
                return false;
            }
            TVector<TString> columns;
            if (!ValidateColumnPairSettings(*setting->Child(1), ctx, columns)) {
                return false;
            }
            break;
        }
        case EYtSettingType::SortBy:
            if (!EnsureTupleSize(*setting, 2, ctx)) {
                return false;
            }

            sortBy.ConstructInPlace();
            if (!ValidateColumnPairSettings(*setting->Child(1), ctx, *sortBy)) {
                return false;
            }
            if (reduceBy.Defined() && !ValidateColumnSubset(setting->Child(1)->Pos(), ctx, *sortBy, *reduceBy)) {
                return false;
            }
            break;
        case EYtSettingType::ReduceBy:
            if (!EnsureTupleSize(*setting, 2, ctx)) {
                return false;
            }

            reduceBy.ConstructInPlace();
            if (!ValidateColumnPairSettings(*setting->Child(1), ctx, *reduceBy)) {
                return false;
            }

            if (sortBy.Defined() && !ValidateColumnSubset(setting->Child(1)->Pos(), ctx, *sortBy, *reduceBy)) {
                return false;
            }
            break;
        case EYtSettingType::Sample:
            if (!EnsureTupleSize(*setting, 2, ctx)) {
                return false;
            }
            if (!EnsureTupleSize(*setting->Child(1), 3, ctx)) {
                return false;
            }
            if (!EnsureAtom(*setting->Child(1)->Child(0), ctx)) {
                return false;
            }
            {
                EYtSampleMode sampleMode = EYtSampleMode::System;
                if (!TryFromString(setting->Child(1)->Child(0)->Content(), sampleMode)) {
                    ctx.AddError(TIssue(ctx.GetPosition(setting->Child(1)->Child(0)->Pos()), TStringBuilder()
                        << "Bad sample mode value: " << setting->Child(1)->Child(0)->Content()));
                    return false;
                }
            }
            if (!EnsureAtom(*setting->Child(1)->Child(1), ctx)) {
                return false;
            }
            {
                double samplePercentage = 0;
                if (!TryFromString(setting->Child(1)->Child(1)->Content(), samplePercentage) ||
                    !(samplePercentage >= 0. && samplePercentage <= 100.))
                {
                    ctx.AddError(TIssue(ctx.GetPosition(setting->Child(1)->Child(1)->Pos()), TStringBuilder()
                        << "Bad sample percentage value: " << setting->Child(1)->Child(1)->Content()));
                    return false;
                }
            }
            if (!EnsureAtom(*setting->Child(1)->Child(2), ctx)) {
                return false;
            }
            {
                ui64 sampleRepeat = 0;
                if (!TryFromString(setting->Child(1)->Child(2)->Content(), sampleRepeat)) {
                    ctx.AddError(TIssue(ctx.GetPosition(setting->Child(1)->Child(2)->Pos()), TStringBuilder()
                        << "Bad sample repeat value: " << setting->Child(1)->Child(2)->Content()));
                    return false;
                }
            }
            break;
        case EYtSettingType::JoinLabel:
            if (!EnsureTupleSize(*setting, 2, ctx)) {
                return false;
            }
            break;
        case EYtSettingType::MapOutputType:
        case EYtSettingType::ReduceInputType:
            if (!EnsureTupleSize(*setting, 2, ctx)) {
                return false;
            }
            if (!EnsureType(setting->Tail(), ctx)) {
                return false;
            }
            break;
        case EYtSettingType::ErasureCodec: {
            if (!EnsureTupleSize(*setting, 2, ctx)) {
                return false;
            }
            NYT::EErasureCodecAttr codec;
            if (!TryFromString(setting->Tail().Content(), codec)) {
                ctx.AddError(TIssue(ctx.GetPosition(setting->Tail().Pos()), TStringBuilder()
                    << "Unsupported erasure codec value " << TString{setting->Tail().Content()}.Quote()));
                return false;
            }
            break;
        }
        case EYtSettingType::CompressionCodec:
            if (!EnsureTupleSize(*setting, 2, ctx)) {
                return false;
            }
            if (!ValidateCompressionCodecValue(setting->Tail().Content())) {
                ctx.AddError(TIssue(ctx.GetPosition(setting->Tail().Pos()), TStringBuilder()
                    << "Unsupported compression codec value " << TString{setting->Tail().Content()}.Quote()));
                return false;
            }
            break;
        case EYtSettingType::Expiration: {
            if (!EnsureTupleSize(*setting, 2, ctx) || !EnsureAtom(setting->Tail(), ctx)) {
                return false;
            }
            TInstant ti;
            TDuration td;
            if (!TInstant::TryParseIso8601(setting->Tail().Content(), ti) &&
                !TDuration::TryParse(setting->Tail().Content(), td))
            {
                ctx.AddError(TIssue(ctx.GetPosition(setting->Tail().Pos()), TStringBuilder()
                    << "Unsupported Expiration value " << TString{setting->Tail().Content()}.Quote()));
                return false;
            }
            break;
        }
        case EYtSettingType::ReplicationFactor: {
            if (!EnsureTupleSize(*setting, 2, ctx)) {
                return false;
            }
            if (!EnsureAtom(setting->Tail(), ctx)) {
                return false;
            }
            ui32 replicationFactor;
            if (!TryFromString(setting->Tail().Content(), replicationFactor) ||
                replicationFactor < 1 || replicationFactor > 10)
            {
                ctx.AddError(TIssue(ctx.GetPosition(setting->Tail().Pos()), TStringBuilder()
                    << "Unsupported Replication Factor value:"
                    << TString{setting->Tail().Content()}.Quote()));
                return false;
            }
            break;
        }
        case EYtSettingType::UserAttrs: {
            if (!EnsureTupleSize(*setting, 2, ctx)) {
                return false;
            }
            if (!EnsureAtom(setting->Tail(), ctx)) {
                return false;
            }
            NYT::TNode mapNode;
            try {
                mapNode = NYT::NodeFromYsonString(setting->Tail().Content());
            } catch (const std::exception& e) {
                ctx.AddError(TIssue(ctx.GetPosition(setting->Tail().Pos()), TStringBuilder()
                    << "Failed to parse Yson: " << e.what()));
                return false;
            }
            if (!mapNode.IsMap()) {
                ctx.AddError(TIssue(ctx.GetPosition(setting->Tail().Pos()), TStringBuilder()
                    << "Expected Yson map, got: " << mapNode.GetType()));
                return false;
            }
            const auto& map = mapNode.AsMap();
            for (auto it = map.cbegin(); it != map.cend(); ++it) {
                if (!it->second.HasValue()) {
                    ctx.AddError(TIssue(ctx.GetPosition(setting->Tail().Pos()), TStringBuilder()
                        << "Expected Yson map key having value: "
                        << it->first.Quote()));
                    return false;
                }
            }
            break;
        }
        case EYtSettingType::Media:
            if (!EnsureTupleSize(*setting, 2, ctx)) {
                return false;
            }
            if (!EnsureAtom(setting->Tail(), ctx)) {
                return false;
            }
            try {
                MediaValidator(NYT::NodeFromYsonString(setting->Tail().Content()));
            } catch (const std::exception& e) {
                ctx.AddError(TIssue(ctx.GetPosition(setting->Tail().Pos()), TStringBuilder()
                    << "Incorrect media: " << e.what()));
                return false;
            }
            break;
        case EYtSettingType::PrimaryMedium:
            if (!EnsureTupleSize(*setting, 2, ctx)) {
                return false;
            }
            if (!EnsureAtom(setting->Tail(), ctx)) {
                return false;
            }
            break;
        case EYtSettingType::MutationId:
            if (!EnsureTupleSize(*setting, 2, ctx)) {
                return false;
            }
            if (!EnsureAtom(*setting->Child(1), ctx)) {
                return false;
            }
            ui32 mutationId;
            if (!TryFromString(setting->Child(1)->Content(), mutationId)) {
                ctx.AddError(TIssue(ctx.GetPosition(setting->Child(1)->Pos()), TStringBuilder()
                    << "Expected a number, but got: " << TString{setting->Child(1)->Content()}.Quote()));
                return false;
            }
            break;
        case EYtSettingType::ColumnGroups: {
            if (!EnsureTupleSize(*setting, 2, ctx)) {
                return false;
            }
            if (!EnsureAtom(setting->Tail(), ctx)) {
                return false;
            }
            NYT::TNode mapNode;
            try {
                mapNode = NYT::NodeFromYsonString(setting->Tail().Content());
            } catch (const std::exception& e) {
                ctx.AddError(TIssue(ctx.GetPosition(setting->Tail().Pos()), TStringBuilder()
                    << "Failed to parse Yson: " << e.what()));
                return false;
            }
            if (!mapNode.IsMap()) {
                ctx.AddError(TIssue(ctx.GetPosition(setting->Tail().Pos()), TStringBuilder()
                    << "Expected Yson map, got: " << mapNode.GetType()));
                return false;
            }
            bool hasDef = false;
            std::unordered_set<TString> uniqColumns;
            const auto& map = mapNode.AsMap();
            for (auto it = map.cbegin(); it != map.cend(); ++it) {
                if (it->second.IsEntity()) {
                    if (hasDef) {
                        ctx.AddError(TIssue(ctx.GetPosition(setting->Tail().Pos()), TStringBuilder()
                            << "Not more than one group should have # value: "
                            << it->first.Quote()));
                        return false;
                    }
                    hasDef = true;
                } else if (!it->second.IsList()) {
                    ctx.AddError(TIssue(ctx.GetPosition(setting->Tail().Pos()), TStringBuilder()
                        << "Expected list value, group: "
                        << it->first.Quote()));
                    return false;
                } else if (it->second.AsList().size() < 2) {
                    ctx.AddError(TIssue(ctx.GetPosition(setting->Tail().Pos()), TStringBuilder()
                        << "Expected list with at least two columns, group: "
                        << it->first.Quote()));
                    return false;
                } else {
                    for (const auto& item: it->second.AsList()) {
                        if (!item.IsString()) {
                            ctx.AddError(TIssue(ctx.GetPosition(setting->Tail().Pos()), TStringBuilder()
                                << "Expected string value in list, found "
                                << item.GetType() << ", group: " << it->first.Quote()));
                            return false;
                        }
                        if (!uniqColumns.insert(item.AsString()).second) {
                            ctx.AddError(TIssue(ctx.GetPosition(setting->Tail().Pos()), TStringBuilder()
                                << "Duplicate column " << item.AsString().Quote()));
                            return false;
                        }
                    }
                }
            }
            break;
        }
        case EYtSettingType::SecurityTags: {
            if (!EnsureTupleSize(*setting, 2, ctx)) {
                return false;
            }
            if (!EnsureAtom(setting->Tail(), ctx)) {
                return false;
            }
            NYT::TNode securityTagsNode;
            try {
                securityTagsNode = NYT::NodeFromYsonString(setting->Tail().Content());
            } catch (const std::exception& e) {
                ctx.AddError(TIssue(ctx.GetPosition(setting->Tail().Pos()), TStringBuilder()
                    << "Failed to parse Yson: " << e.what()));
                return false;
            }
            if (!securityTagsNode.IsList()) {
                ctx.AddError(TIssue(ctx.GetPosition(setting->Tail().Pos()), TStringBuilder()
                    << "Expected YSON list of strings"));
                return false;
            }
            for (const auto &child : securityTagsNode.AsList()) {
                if (!child.IsString()) {
                    ctx.AddError(TIssue(ctx.GetPosition(setting->Tail().Pos()), TStringBuilder()
                        << "Expected YSON list of strings"));
                    return false;
                }
            }
            return true;
        }
        case EYtSettingType::LAST: {
            YQL_ENSURE(false, "Unexpected EYtSettingType");
        }
        }
    }

    if (used.HasFlags(EYtSettingType::InferScheme) && used.HasFlags(EYtSettingType::ForceInferScheme)) {
        ctx.AddError(TIssue(ctx.GetPosition(settingsNode.Pos()), TStringBuilder()
            << "Setting " << ToString(EYtSettingType::InferScheme).Quote()
            << " is not allowed with " << ToString(EYtSettingType::ForceInferScheme).Quote()));
        return false;
    }
    if (used.HasFlags(EYtSettingType::SortLimitBy) && !used.HasFlags(EYtSettingType::Limit)) {
        ctx.AddError(TIssue(ctx.GetPosition(settingsNode.Pos()), TStringBuilder()
            << "Setting " << ToString(EYtSettingType::SortLimitBy).Quote()
            << " is not allowed without " << ToString(EYtSettingType::Limit).Quote()));
        return false;
    }
    if (used.HasFlags(EYtSettingType::KeyFilter) && used.HasFlags(EYtSettingType::KeyFilter2)) {
        ctx.AddError(TIssue(ctx.GetPosition(settingsNode.Pos()), TStringBuilder()
             << "Settings " << ToString(EYtSettingType::KeyFilter).Quote()
             << " and " << ToString(EYtSettingType::KeyFilter2).Quote() << " can not be used together"));
        return false;
    }
    if (used.HasFlags(EYtSettingType::Anonymous) && used.HasFlags(EYtSettingType::Expiration)) {
        ctx.AddError(TIssue(ctx.GetPosition(settingsNode.Pos()), TStringBuilder()
            << ToString(EYtSettingType::Expiration).Quote()
            << " setting cannot be used for anonymous tables"));
        return false;
    }

    if (isTruncate) {
        for (auto type: {EYtSettingType::MonotonicKeys}) {
            if (used.HasFlags(type)) {
                ctx.AddError(TIssue(ctx.GetPosition(settingsNode.Pos()), TStringBuilder()
                    << ToString(type).Quote()
                    << " setting can not be used with TRUNCATE mode"));
                return false;
            }
        }
    } else {
        for (auto type: {EYtSettingType::Expiration, EYtSettingType::Media, EYtSettingType::PrimaryMedium, EYtSettingType::KeepMeta}) {
            if (used.HasFlags(type)) {
                ctx.AddError(TIssue(ctx.GetPosition(settingsNode.Pos()), TStringBuilder()
                    << ToString(type).Quote()
                    << " setting can only be used with TRUNCATE mode"));
                return false;
            }
        }
    }

    return true;
}

bool ValidateColumnGroups(const TExprNode& setting, const TStructExprType& rowType, TExprContext& ctx) {
    const auto columnGroups = NYT::NodeFromYsonString(setting.Tail().Content());
    TIssueScopeGuard issueScope(ctx.IssueManager, [&]() {
        return MakeIntrusive<TIssue>(ctx.GetPosition(setting.Pos()), TStringBuilder() << "Setting " << setting.Head().Content());
    });

    for (const auto& grp: columnGroups.AsMap()) {
        if (!grp.second.IsEntity()) {
            for (const auto& col: grp.second.AsList()) {
                if (!rowType.FindItem(col.AsString())) {
                    ctx.AddError(TIssue(ctx.GetPosition(setting.Pos()), TStringBuilder()
                        << "Column group " << grp.first.Quote() << " refers to unknown column " << col.AsString().Quote()));
                    return false;
                }
            }
        }
    }
    return true;
}

TString NormalizeColumnGroupSpec(const TStringBuf spec) {
    try {
        auto columnGroups = NYT::NodeFromYsonString(spec);
        for (auto& grp: columnGroups.AsMap()) {
            if (!grp.second.IsEntity()) {
                std::stable_sort(grp.second.AsList().begin(), grp.second.AsList().end(), [](const auto& l, const auto& r) { return l.AsString() < r.AsString(); });
            }
        }
        return NYT::NodeToCanonicalYsonString(columnGroups);
    } catch (...) {
        // Keep as is. Type annotation will add user friendly error later
        return TString{spec};
    }
}

const TString& GetSingleColumnGroupSpec() {
    static TString GROUP = NYT::NodeToCanonicalYsonString(NYT::TNode::CreateMap()("default", NYT::TNode::CreateEntity()), NYson::EYsonFormat::Text);
    return GROUP;
}

TExprNode::TPtr GetSetting(const TExprNode& settings, EYtSettingType type) {
    for (auto& setting : settings.Children()) {
        if (setting->ChildrenSize() != 0 && FromString<EYtSettingType>(setting->Child(0)->Content()) == type) {
            return setting;
        }
    }
    return nullptr;
}

TExprNode::TPtr UpdateSettingValue(const TExprNode& settings, EYtSettingType type, TExprNode::TPtr&& value, TExprContext& ctx) {
    for (ui32 index = 0U; index < settings.ChildrenSize(); ++index) {
        if (settings.Child(index)->ChildrenSize() != 0 && FromString<EYtSettingType>(settings.Child(index)->Head().Content()) == type) {
            const auto setting = settings.Child(index);

            auto newSetting = ctx.ChangeChildren(*setting, value ? TExprNode::TListType{setting->HeadPtr(), std::move(value)} : TExprNode::TListType{setting->HeadPtr()});
            return ctx.ChangeChild(settings, index, std::move(newSetting));
        }
    }
    return {};
}

TExprNode::TPtr AddOrUpdateSettingValue(const TExprNode& settings, EYtSettingType type, TExprNode::TPtr&& value, TExprContext& ctx) {
    for (ui32 index = 0U; index < settings.ChildrenSize(); ++index) {
        if (settings.Child(index)->ChildrenSize() != 0 && FromString<EYtSettingType>(settings.Child(index)->Head().Content()) == type) {
            const auto setting = settings.Child(index);

            auto newSetting = ctx.ChangeChildren(*setting, value ? TExprNode::TListType{setting->HeadPtr(), std::move(value)} : TExprNode::TListType{setting->HeadPtr()});
            return ctx.ChangeChild(settings, index, std::move(newSetting));
        }
    }
    return AddSetting(settings, type, value, ctx);
}

TExprNode::TListType GetAllSettingValues(const TExprNode& settings, EYtSettingType type) {
    TExprNode::TListType res;
    for (auto& setting : settings.Children()) {
        if (setting->ChildrenSize() == 2 && FromString<EYtSettingType>(setting->Child(0)->Content()) == type) {
            res.push_back(setting->ChildPtr(1));
        }
    }
    return res;
}

bool HasSetting(const TExprNode& settings, EYtSettingType type) {
    return bool(GetSetting(settings, type));
}

bool HasAnySetting(const TExprNode& settings, EYtSettingTypes types) {
    for (auto& child: settings.Children()) {
        if (child->ChildrenSize() != 0 && types.HasFlags(FromString<EYtSettingType>(child->Child(0)->Content()))) {
            return true;
        }
    }
    return false;
}

bool HasSettingsExcept(const TExprNode& settings, EYtSettingTypes types) {
    for (auto& child: settings.Children()) {
        if (child->ChildrenSize() != 0 && !types.HasFlags(FromString<EYtSettingType>(child->Child(0)->Content()))) {
            return true;
        }
    }
    return false;
}

bool EqualSettingsExcept(const TExprNode& lhs, const TExprNode& rhs, EYtSettingTypes types) {
    if (&lhs == &rhs)
        return true;

    TNodeSet set(lhs.ChildrenSize());
    for (const auto& child: lhs.Children()) {
        if (child->ChildrenSize() != 0 && !types.HasFlags(FromString<EYtSettingType>(child->Head().Content()))) {
            set.emplace(child.Get());
        }
    }
    for (const auto& child: rhs.Children()) {
        if (child->ChildrenSize() != 0 && !types.HasFlags(FromString<EYtSettingType>(child->Head().Content()))) {
            if (!set.erase(child.Get()))
                return false;
        }
    }
    return set.empty();
}

TExprNode::TPtr RemoveSetting(const TExprNode& settings, EYtSettingType type, TExprContext& ctx) {
    TExprNode::TListType children;
    for (auto child: settings.Children()) {
        if (child->ChildrenSize() != 0 && FromString<EYtSettingType>(child->Child(0)->Content()) == type) {
            continue;
        }
        children.push_back(child);
    }

    return ctx.NewList(settings.Pos(), std::move(children));
}

TExprNode::TPtr RemoveSettings(const TExprNode& settings, EYtSettingTypes types, TExprContext& ctx) {
    TExprNode::TListType children;
    for (auto child: settings.Children()) {
        if (child->ChildrenSize() != 0 && types.HasFlags(FromString<EYtSettingType>(child->Child(0)->Content()))) {
            continue;
        }
        children.push_back(child);
    }

    return ctx.NewList(settings.Pos(), std::move(children));
}

TExprNode::TPtr KeepOnlySettings(const TExprNode& settings, EYtSettingTypes types, TExprContext& ctx) {
    TExprNode::TListType children;
    for (auto child: settings.Children()) {
        if (child->ChildrenSize() != 0 && !types.HasFlags(FromString<EYtSettingType>(child->Child(0)->Content()))) {
            continue;
        }
        children.push_back(child);
    }

    return ctx.NewList(settings.Pos(), std::move(children));
}

TExprNode::TPtr AddSetting(const TExprNode& settings, EYtSettingType type, const TExprNode::TPtr& value, TExprContext& ctx) {
    return AddSetting(settings, settings.Pos(), ToString(type), value, ctx);
}

TVector<TString> GetSettingAsColumnList(const TExprNode& settings, EYtSettingType type) {
    TVector<TString> result;
    if (auto node = GetSetting(settings, type)) {
        for (auto& column : node->Child(1)->Children()) {
            if (column->IsAtom()) {
                result.emplace_back(column->Content());
            } else {
                YQL_ENSURE(column->IsList() && column->ChildrenSize() > 0);
                result.emplace_back(column->Child(0)->Content());
            }
        }
    }
    return result;
}

TVector<std::pair<TString, bool>> GetSettingAsColumnPairList(const TExprNode& settings, EYtSettingType type) {
    TVector<std::pair<TString, bool>> result;
    if (auto node = GetSetting(settings, type)) {
        for (auto& column : node->Child(1)->Children()) {
            if (column->IsAtom()) {
                result.emplace_back(column->Content(), true);
            } else {
                result.emplace_back(column->Child(0)->Content(), FromString<bool>(column->Child(1)->Content()));
            }
        }
    }
    return result;
}

TExprNode::TListType GetSettingAsColumnAtomList(const TExprNode& settings, EYtSettingType type) {
    TExprNode::TListType result;
    if (const auto node = GetSetting(settings, type)) {
        for (const auto& column : node->Child(1)->Children()) {
            if (column->IsAtom()) {
                result.emplace_back(column);
            } else {
                YQL_ENSURE(column->IsList() && column->ChildrenSize() > 0);
                result.emplace_back(column->HeadPtr());
            }
        }
    }
    return result;
}

std::vector<std::pair<TExprNode::TPtr, bool>> GetSettingAsColumnAtomPairList(const TExprNode& settings, EYtSettingType type) {
    std::vector<std::pair<TExprNode::TPtr, bool>> result;
    if (const auto node = GetSetting(settings, type)) {
        for (const auto& column : node->Child(1)->Children()) {
            if (column->IsAtom()) {
                result.emplace_back(column, true);
            } else {
                result.emplace_back(column->HeadPtr(), FromString<bool>(column->Child(1)->Content()));
            }
        }
    }
    return result;
}

TExprNode::TPtr AddSettingAsColumnList(const TExprNode& settings, EYtSettingType type,
    const TVector<TString>& columns, TExprContext& ctx)
{
    return AddSetting(settings, type, ToAtomList(columns, settings.Pos(), ctx), ctx);
}

TExprNode::TPtr ToColumnPairList(const TVector<std::pair<TString, bool>>& columns, TPositionHandle pos, TExprContext& ctx) {
    if (AllOf(columns, [](const auto& pair) { return pair.second; })) {
        TExprNode::TListType children;
        for (auto& pair: columns) {
            children.push_back(ctx.NewAtom(pos, pair.first));
        }

        return ctx.NewList(pos, std::move(children));
    } else {
        TExprNode::TListType children;
        for (auto& pair: columns) {
            children.push_back(ctx.NewList(pos, {
                ctx.NewAtom(pos, pair.first),
                ctx.NewAtom(pos, pair.second ? "1" : "0", TNodeFlags::Default),
            }));
        }

        return ctx.NewList(pos, std::move(children));
    }
}

TExprNode::TPtr AddSettingAsColumnPairList(const TExprNode& settings, EYtSettingType type,
    const TVector<std::pair<TString, bool>>& columns, TExprContext& ctx)
{
    return AddSetting(settings, type, ToColumnPairList(columns, settings.Pos(), ctx), ctx);
}

TMaybe<TSampleParams> GetSampleParams(const TExprNode& settings) {
    if (auto setting = GetSetting(settings, EYtSettingType::Sample)) {
        auto value = setting->Child(1);
        YQL_ENSURE(value->ChildrenSize() == 3);
        return TSampleParams {
            FromString<EYtSampleMode>(value->Child(0)->Content()),
            FromString<double>(value->Child(1)->Content()),
            FromString<ui64>(value->Child(2)->Content())
        };
    }

    return Nothing();
}

TMaybe<ui64> GetMaxJobSizeForFirstAsPrimary(const TExprNode& settings) {
    if (auto setting = NYql::GetSetting(settings, EYtSettingType::FirstAsPrimary)) {
        if (setting->ChildrenSize() > 1) {
            if (auto subsetting = NYql::GetSetting(*setting->Child(1), MaxJobSizeForFirstAsPrimaryName)) {
                return FromString<ui64>(subsetting->Child(1)->Content());
            }
        }
    }

    return Nothing();
}

bool UseJoinReduceForSecondAsPrimary(const TExprNode &settings) {
    if (auto setting = NYql::GetSetting(settings, EYtSettingType::FirstAsPrimary)) {
        if (setting->ChildrenSize() > 1) {
            return NYql::HasSetting(*setting->Child(1), JoinReduceForSecondAsPrimaryName);
        }
    }

    return false;
}

ui32 GetMinChildrenForIndexedKeyFilter(EYtSettingType type) {
    if (type == EYtSettingType::KeyFilter) {
        return 2u;
    }
    YQL_ENSURE(type == EYtSettingType::KeyFilter2);
    return 3u;
}

EYtSettingTypes operator|(EYtSettingTypes left, const EYtSettingTypes& right) {
    return left |= right;
}

EYtSettingTypes operator&(EYtSettingTypes left, const EYtSettingTypes& right) {
    return left &= right;
}

EYtSettingTypes operator|(EYtSettingType left, EYtSettingType right) {
    return EYtSettingTypes(left) | EYtSettingTypes(right);
}

} // NYql
