#include "yql_yt_provider_impl.h"
#include "yql_yt_op_settings.h"
#include "yql_yt_helpers.h"

#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_table.h>
#include <ydb/library/yql/providers/yt/common/yql_names.h>
#include <ydb/library/yql/providers/yt/common/yql_configuration.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/transform/yql_visit.h>
#include <ydb/library/yql/providers/common/codec/yql_codec_type_flags.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/yql_join.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/yql_type_helpers.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/yql/utils/log/log.h>

#include <util/generic/xrange.h>

namespace NYql {

namespace {

bool IsWideRepresentation(const TTypeAnnotationNode* leftType, const TTypeAnnotationNode* rightType) {
    const auto structType = dynamic_cast<const TStructExprType*>(leftType);
    const auto multiType = dynamic_cast<const TMultiExprType*>(rightType);
    if (!structType || !multiType || structType->GetSize() != multiType->GetSize())
        return false;

    const auto& structItems = structType->GetItems();
    const auto& multiItems = multiType->GetItems();

    for (auto i = 0U; i < multiItems.size(); ++i)
        if (!IsSameAnnotation(*multiItems[i], *structItems[i]->GetItemType()))
            return false;

    return true;
}

const TTypeAnnotationNode* MakeInputType(const TTypeAnnotationNode* itemType, const TExprNode::TPtr& setting, TExprContext& ctx) {
    if (!setting)
        return ctx.MakeType<TStreamExprType>(itemType);

    if (const auto structType = dynamic_cast<const TStructExprType*>(itemType)) {
        if (ui32 limit; structType && 2U == setting->ChildrenSize() && TryFromString<ui32>(setting->Tail().Content(), limit) && structType->GetSize() < limit && structType->GetSize() > 0U) {
            TTypeAnnotationNode::TListType types;
            const auto& items = structType->GetItems();
            types.reserve(items.size());
            std::transform(items.cbegin(), items.cend(), std::back_inserter(types), std::bind(&TItemExprType::GetItemType, std::placeholders::_1));
            return ctx.MakeType<TFlowExprType>(ctx.MakeType<TMultiExprType>(types));
        }
    }

    return ctx.MakeType<TFlowExprType>(itemType);
}

using namespace NNodes;

class TYtDataSinkTypeAnnotationTransformer : public TVisitorTransformerBase {
public:
    TYtDataSinkTypeAnnotationTransformer(TYtState::TPtr state)
        : TVisitorTransformerBase(true)
        , State_(state)
    {
        AddHandler({TYtOutTable::CallableName()}, Hndl(&TYtDataSinkTypeAnnotationTransformer::HandleOutTable));
        AddHandler({TYtOutput::CallableName()}, Hndl(&TYtDataSinkTypeAnnotationTransformer::HandleOutput));
        AddHandler({TYtSort::CallableName()}, Hndl(&TYtDataSinkTypeAnnotationTransformer::HandleSort));
        AddHandler({TYtCopy::CallableName()}, Hndl(&TYtDataSinkTypeAnnotationTransformer::HandleCopy));
        AddHandler({TYtMerge::CallableName()}, Hndl(&TYtDataSinkTypeAnnotationTransformer::HandleMerge));
        AddHandler({TYtMap::CallableName()}, Hndl(&TYtDataSinkTypeAnnotationTransformer::HandleMap));
        AddHandler({TYtReduce::CallableName()}, Hndl(&TYtDataSinkTypeAnnotationTransformer::HandleReduce));
        AddHandler({TYtMapReduce::CallableName()}, Hndl(&TYtDataSinkTypeAnnotationTransformer::HandleMapReduce));
        AddHandler({TYtWriteTable::CallableName()}, Hndl(&TYtDataSinkTypeAnnotationTransformer::HandleWriteTable));
        AddHandler({TYtFill::CallableName()}, Hndl(&TYtDataSinkTypeAnnotationTransformer::HandleFill));
        AddHandler({TYtTouch::CallableName()}, Hndl(&TYtDataSinkTypeAnnotationTransformer::HandleTouch));
        AddHandler({TYtDropTable::CallableName()}, Hndl(&TYtDataSinkTypeAnnotationTransformer::HandleDropTable));
        AddHandler({TCoCommit::CallableName()}, Hndl(&TYtDataSinkTypeAnnotationTransformer::HandleCommit));
        AddHandler({TYtPublish::CallableName()}, Hndl(&TYtDataSinkTypeAnnotationTransformer::HandlePublish));
        AddHandler({TYtEquiJoin::CallableName()}, Hndl(&TYtDataSinkTypeAnnotationTransformer::HandleEquiJoin));
        AddHandler({TYtStatOutTable::CallableName()}, Hndl(&TYtDataSinkTypeAnnotationTransformer::HandleStatOutTable));
        AddHandler({TYtStatOut::CallableName()}, Hndl(&TYtDataSinkTypeAnnotationTransformer::HandleStatOut));
        AddHandler({TYtDqProcessWrite ::CallableName()}, Hndl(&TYtDataSinkTypeAnnotationTransformer::HandleYtDqProcessWrite));
        AddHandler({TYtDqWrite::CallableName()}, Hndl(&TYtDataSinkTypeAnnotationTransformer::HandleDqWrite<false>));
        AddHandler({TYtDqWideWrite::CallableName()}, Hndl(&TYtDataSinkTypeAnnotationTransformer::HandleDqWrite<true>));
        AddHandler({TYtTryFirst::CallableName()}, Hndl(&TYtDataSinkTypeAnnotationTransformer::HandleTryFirst));
    }

private:
    static bool ValidateOpBase(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureWorldType(*input->Child(TYtOpBase::idx_World), ctx)) {
            return false;
        }

        if (!EnsureSpecificDataSink(*input->Child(TYtOpBase::idx_DataSink), YtProviderName, ctx)) {
            return false;
        }
        return true;
    }

    static bool ValidateOutputOpBase(const TExprNode::TPtr& input, TExprContext& ctx, bool multiIO) {
        if (!ValidateOpBase(input, ctx)) {
            return false;
        }

        // Output
        if (multiIO) {
            if (!EnsureTupleMinSize(*input->Child(TYtOutputOpBase::idx_Output), 1, ctx)) {
                return false;
            }
        } else {
            if (!EnsureTupleSize(*input->Child(TYtOutputOpBase::idx_Output), 1, ctx)) {
                return false;
            }
        }

        return true;
    }

    TStatus ValidateAndUpdateTransientOpBase(const TExprNode::TPtr& input, TExprNode::TPtr& output,
        TExprContext& ctx, bool multiIO, EYtSettingTypes allowedSectionSettings) const {
        if (!ValidateOutputOpBase(input, ctx, multiIO)) {
            return TStatus::Error;
        }

        // Input
        if (multiIO) {
            if (!EnsureTupleMinSize(*input->Child(TYtTransientOpBase::idx_Input), 1, ctx)) {
                return TStatus::Error;
            }
        } else {
            if (!EnsureTupleSize(*input->Child(TYtTransientOpBase::idx_Input), 1, ctx)) {
                return TStatus::Error;
            }
        }

        for (auto& section: input->Child(TYtTransientOpBase::idx_Input)->Children()) {
            if (!section->IsCallable(TYtSection::CallableName())) {
                ctx.AddError(TIssue(ctx.GetPosition(section->Pos()), TStringBuilder() << "Expected " << TYtSection::CallableName()));
                return TStatus::Error;
            }
        }

        auto clusterName = TString{TYtDSink(input->ChildPtr(TYtTransientOpBase::idx_DataSink)).Cluster().Value()};
        TMaybe<bool> yamrFormat;
        TMaybe<TSampleParams> sampling;
        for (size_t i = 0; i < input->Child(TYtTransientOpBase::idx_Input)->ChildrenSize(); ++i) {
            auto section = TYtSection(input->Child(TYtTransientOpBase::idx_Input)->ChildPtr(i));
            for (auto setting: section.Settings()) {
                if (!allowedSectionSettings.HasFlags(FromString<EYtSettingType>(setting.Name().Value()))) {
                    ctx.AddError(TIssue(ctx.GetPosition(section.Pos()), TStringBuilder()
                        << "Settings " << TString{setting.Name().Value()}.Quote() << " is not allowed in "
                        << input->Content() << " section"));
                    return TStatus::Error;
                }
            }

            if (0 == i) {
                sampling = NYql::GetSampleParams(section.Settings().Ref());
            } else if (NYql::GetSampleParams(section.Settings().Ref()) != sampling) {
                ctx.AddError(TIssue(ctx.GetPosition(section.Pos()), "Sections have different sample values"));
                return TStatus::Error;
            }

            for (auto path: section.Paths()) {
                if (auto maybeTable = path.Table().Maybe<TYtTable>()) {
                    auto table = maybeTable.Cast();
                    auto tableName = table.Name().Value();
                    if (!NYql::HasSetting(table.Settings().Ref(), EYtSettingType::UserSchema)) {
                        // Don't validate already substituted anonymous tables
                        if (!TYtTableInfo::HasSubstAnonymousLabel(table)) {
                            const TYtTableDescription& tableDesc = State_->TablesData->GetTable(clusterName,
                                TString{tableName},
                                TEpochInfo::Parse(table.Epoch().Ref()));

                            if (!tableDesc.Validate(ctx.GetPosition(table.Pos()), clusterName, tableName,
                                NYql::HasSetting(table.Settings().Ref(), EYtSettingType::WithQB), State_->AnonymousLabels, ctx)) {
                                return TStatus::Error;
                            }
                        }
                    }
                }
                if (yamrFormat) {
                    if (*yamrFormat != path.Table().Maybe<TYtTable>().RowSpec().Maybe<TCoVoid>().IsValid()) {
                        ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder() << "Mixed Yamr/Yson input table formats"));
                        return TStatus::Error;
                    }
                } else {
                    yamrFormat = path.Table().Maybe<TYtTable>().RowSpec().Maybe<TCoVoid>().IsValid();
                }
            }
        }

        // Basic Settings validation
        if (!EnsureTuple(*input->Child(TYtTransientOpBase::idx_Settings), ctx)) {
            return TStatus::Error;
        }

        for (auto& setting: input->Child(TYtTransientOpBase::idx_Settings)->Children()) {
            if (!EnsureTupleMinSize(*setting, 1, ctx)) {
                return TStatus::Error;
            }

            if (!EnsureAtom(*setting->Child(0), ctx)) {
                return TStatus::Error;
            }
        }

        auto opInput = input->ChildPtr(TYtTransientOpBase::idx_Input);
        auto newInput = ValidateAndUpdateTablesMeta(opInput, clusterName, State_->TablesData, State_->Types->UseTableMetaFromGraph, ctx);
        if (!newInput) {
            return TStatus::Error;
        }
        else if (newInput != opInput) {
            output = ctx.ChangeChild(*input, TYtTransientOpBase::idx_Input, std::move(newInput));
            return TStatus::Repeat;
        }

        return TStatus::Ok;
    }

    static bool ValidateOutputType(const TTypeAnnotationNode* itemType, TPositionHandle positionHandle, TYtOutSection outTables,
        size_t beginIdx, size_t endIdx, bool useExtendedType, TExprContext& ctx)
    {
        YQL_ENSURE(beginIdx <= endIdx);
        YQL_ENSURE(endIdx <= outTables.Ref().ChildrenSize());
        YQL_ENSURE(itemType);
        const size_t outTablesSize = endIdx - beginIdx;
        TPosition pos = ctx.GetPosition(positionHandle);

        if (!EnsurePersistableType(positionHandle, *itemType, ctx)) {
            return false;
        }

        if (itemType->GetKind() == ETypeAnnotationKind::Variant) {
            if (itemType->Cast<TVariantExprType>()->GetUnderlyingType()->GetKind() == ETypeAnnotationKind::Tuple) {
                auto tupleType = itemType->Cast<TVariantExprType>()->GetUnderlyingType()->Cast<TTupleExprType>();
                if (tupleType->GetSize() != outTablesSize) {
                    ctx.AddError(TIssue(pos, TStringBuilder() << "Expected variant of "
                        << outTablesSize << " items, but got: " << tupleType->GetSize()));
                    return false;
                }
                for (size_t i = beginIdx; i < endIdx; ++i) {
                    auto rowSpec = TYqlRowSpecInfo(outTables.Item(i).RowSpec());
                    const TTypeAnnotationNode* tableItemType = nullptr;
                    if (useExtendedType) {
                        tableItemType = rowSpec.GetExtendedType(ctx);
                    } else if (!GetSequenceItemType(outTables.Item(i).Ref(), tableItemType, ctx)) {
                        return false;
                    }
                    if (tableItemType->HasBareYson() && 0 != rowSpec.GetNativeYtTypeFlags()) {
                        ctx.AddError(TIssue(pos, TStringBuilder() << "Strict Yson type is not allowed to write, please use Optional<Yson>, item type: "
                            << *tableItemType));
                        return false;
                    }

                    if (!IsSameAnnotation(*tupleType->GetItems()[i - beginIdx], *tableItemType)) {
                        ctx.AddError(TIssue(pos, TStringBuilder()
                            << "Output table " << i << " row type differs from the write row type: "
                            << GetTypeDiff(*tableItemType, *tupleType->GetItems()[i - beginIdx])));
                        return false;
                    }
                }
            } else {
                auto structType = itemType->Cast<TVariantExprType>()->GetUnderlyingType()->Cast<TStructExprType>();
                if (structType->GetSize() != outTablesSize) {
                    ctx.AddError(TIssue(pos, TStringBuilder() << "Expected variant of "
                        << outTablesSize << " items, but got: " << structType->GetSize()));
                    return false;
                }
                for (size_t i = beginIdx; i < endIdx; ++i) {
                    auto rowSpec = TYqlRowSpecInfo(outTables.Item(i).RowSpec());
                    const TTypeAnnotationNode* tableItemType = nullptr;
                    if (useExtendedType) {
                        tableItemType = rowSpec.GetExtendedType(ctx);
                    } else if (!GetSequenceItemType(outTables.Item(i).Ref(), tableItemType, ctx)) {
                        return false;
                    }

                    if (tableItemType->HasBareYson() && 0 != rowSpec.GetNativeYtTypeFlags()) {
                        ctx.AddError(TIssue(pos, TStringBuilder() << "Strict Yson type is not allowed to write, please use Optional<Yson>, item type: "
                            << *tableItemType));
                        return false;
                    }

                    if (!IsSameAnnotation(*structType->GetItems()[i - beginIdx]->GetItemType(), *tableItemType)) {
                        ctx.AddError(TIssue(pos, TStringBuilder()
                            << "Output table " << i << " row type differs from the write row type: "
                            << GetTypeDiff(*tableItemType, *structType->GetItems()[i - beginIdx]->GetItemType())));
                        return false;
                    }
                }
            }
        } else {
            if (outTablesSize != 1) {
                ctx.AddError(TIssue(pos, TStringBuilder() << "Expected variant of "
                    << outTablesSize << " items, but got single item: " << *itemType));
                return false;
            }
            auto rowSpec = TYqlRowSpecInfo(outTables.Item(0).RowSpec());
            const TTypeAnnotationNode* tableItemType = nullptr;
            if (useExtendedType) {
                tableItemType = rowSpec.GetExtendedType(ctx);
            } else if (!GetSequenceItemType(outTables.Item(0).Ref(), tableItemType, ctx)) {
                return false;
            }

            if (tableItemType->HasBareYson() && 0 != rowSpec.GetNativeYtTypeFlags()) {
                ctx.AddError(TIssue(pos, TStringBuilder() << "Strict Yson type is not allowed to write, please use Optional<Yson>, item type: "
                    << *tableItemType));
                return false;
            }

            if (!(IsSameAnnotation(*itemType, *tableItemType) || IsWideRepresentation(tableItemType, itemType))) {
                ctx.AddError(TIssue(pos, TStringBuilder()
                    << "Output table row type differs from the write row type: "
                    << GetTypeDiff(*tableItemType, *itemType)));
                return false;
            }
        }
        return true;
    }

    static bool ValidateOutputType(const TTypeAnnotationNode* itemType, TPositionHandle positionHandle, TYtOutSection outTables,
        TExprContext& ctx, bool useExtendedType = false)
    {
        return ValidateOutputType(itemType, positionHandle, outTables, 0, outTables.Ref().ChildrenSize(), useExtendedType, ctx);
    }

    static bool ValidateOutputType(const TExprNode& list, TYtOutSection outTables, TExprContext& ctx, bool useExtendedType = false) {
        const TTypeAnnotationNode* itemType = GetSequenceItemType(list.Pos(), list.GetTypeAnn(), true, ctx);
        if (nullptr == itemType) {
            return false;
        }
        return ValidateOutputType(itemType, list.Pos(), outTables, ctx, useExtendedType);
    }

    static bool ValidateColumnListSetting(TPosition pos, TExprContext& ctx, const TTypeAnnotationNode* itemType,
        const TVector<TString>& columns, EYtSettingType settingType, bool isInput)
    {
        TStringBuf inOut = isInput ? " input" : " output";
        if (itemType->GetKind() == ETypeAnnotationKind::Variant) {
            TTypeAnnotationNode::TListType columnTypes(columns.size(), nullptr);
            auto tupleType = itemType->Cast<TVariantExprType>()->GetUnderlyingType()->Cast<TTupleExprType>();
            for (size_t t: xrange(tupleType->GetSize())) {
                auto structType = tupleType->GetItems()[t]->Cast<TStructExprType>();
                for (size_t i: xrange(columns.size())) {
                    auto id = structType->FindItem(columns[i]);
                    if (!id.Defined()) {
                        ctx.AddError(TIssue(pos, TStringBuilder()
                            << settingType << " field " << columns[i] << " is missing in "
                            << t << inOut << " section type"));
                        return false;
                    }
                    auto columnType = structType->GetItems()[*id]->GetItemType();
                    // Clear optionality before comparing
                    if (columnType->GetKind() == ETypeAnnotationKind::Optional) {
                        columnType = columnType->Cast<TOptionalExprType>()->GetItemType();
                    }
                    if (0 == t) {
                        columnTypes[i] = columnType;
                    } else if (!IsSameAnnotation(*columnTypes[i], *columnType)) {
                        ctx.AddError(TIssue(pos, TStringBuilder()
                            << settingType << " field " << columns[i] << " has different type in "
                            << t << inOut << " section type: "
                            << GetTypeDiff(*columnTypes[i], *columnType)));
                        return false;
                    }
                }
            }
        } else if (ETypeAnnotationKind::Struct ==  itemType->GetKind()) {
            auto structType = itemType->Cast<TStructExprType>();
            for (size_t i: xrange(columns.size())) {
                auto id = structType->FindItem(columns[i]);
                if (!id.Defined()) {
                    ctx.AddError(TIssue(pos, TStringBuilder()
                        << settingType << " field " << columns[i] << " is missing in" << inOut << " type"));
                    return false;
                }
            }
        }
        return true;
    }

    TStatus ValidateTableWrite(const TPosition& pos, const TExprNode::TPtr& table, TExprNode::TPtr& content, const TTypeAnnotationNode* itemType,
        const TVector<TYqlRowSpecInfo::TPtr>& contentRowSpecs, const TString& cluster, const TExprNode& settings, TExprContext& ctx) const
    {
        YQL_ENSURE(itemType);
        if (content && !EnsurePersistableType(content->Pos(), *itemType, ctx)) {
            return TStatus::Error;
        }

        EYtWriteMode mode = EYtWriteMode::Renew;
        if (auto modeSetting = NYql::GetSetting(settings, EYtSettingType::Mode)) {
            mode = FromString<EYtWriteMode>(modeSetting->Child(1)->Content());
        }
        const bool initialWrite = NYql::HasSetting(settings, EYtSettingType::Initial);
        const bool monotonicKeys = NYql::HasSetting(settings, EYtSettingType::MonotonicKeys);
        TString columnGroups;
        if (auto setting = NYql::GetSetting(settings, EYtSettingType::ColumnGroups)) {
            if (!ValidateColumnGroups(*setting, *itemType->Cast<TStructExprType>(), ctx)) {
                return TStatus::Error;
            }
            columnGroups.assign(setting->Tail().Content());
        }

        if (!initialWrite && mode != EYtWriteMode::Append) {
            ctx.AddError(TIssue(pos, TStringBuilder() <<
                "Replacing " << TString{table->Child(TYtTable::idx_Name)->Content()}.Quote() << " table content after another table modifications in the same transaction"));
            return TStatus::Error;
        }

        auto outTableInfo = TYtTableInfo(table);
        TYtTableDescription& description = State_->TablesData->GetModifTable(cluster, outTableInfo.Name, outTableInfo.Epoch);

        auto meta = description.Meta;
        YQL_ENSURE(meta);

        if (meta->SqlView) {
            ctx.AddError(TIssue(pos, TStringBuilder()
                << "Modification of " << outTableInfo.Name.Quote() << " view is not supported"));
            return TStatus::Error;
        }

        if (meta->IsDynamic) {
            ctx.AddError(TIssue(pos, TStringBuilder() <<
                "Modification of dynamic table " << outTableInfo.Name.Quote() << " is not supported"));
            return TStatus::Error;
        }

        bool replaceMeta = !meta->DoesExist || (mode != EYtWriteMode::Append && mode != EYtWriteMode::RenewKeepMeta);
        bool checkLayout = meta->DoesExist && (mode == EYtWriteMode::Append || mode == EYtWriteMode::RenewKeepMeta || description.IsReplaced);

        if (monotonicKeys && initialWrite && replaceMeta) {
            ctx.AddError(TIssue(pos, TStringBuilder()
                << "Insert with "
                << ToString(EYtSettingType::MonotonicKeys).Quote()
                << " setting cannot be used with a non-existent table"));
            return TStatus::Error;
        }

        if (initialWrite && !replaceMeta && columnGroups != description.ColumnGroupSpec) {
            ctx.AddError(TIssue(pos, TStringBuilder()
                << "Insert with "
                << (outTableInfo.Epoch.GetOrElse(0) ? "different " : "")
                << ToString(EYtSettingType::ColumnGroups).Quote()
                << " to existing table is not allowed"));
            return TStatus::Error;
        }

        if (auto commitEpoch = outTableInfo.CommitEpoch.GetOrElse(0)) {
            // Check type compatibility with previous epoch
            if (auto nextDescription = State_->TablesData->FindTable(cluster, outTableInfo.Name, commitEpoch)) {
                if (nextDescription->Meta) {
                    if (!nextDescription->Meta->DoesExist) {
                        ctx.AddError(TIssue(pos, TStringBuilder() <<
                            "Table " << outTableInfo.Name << " is modified and dropped in the same transaction"));
                        return TStatus::Error;
                    }
                    checkLayout = !nextDescription->IsReplaced;
                } else {
                    checkLayout = !replaceMeta;
                }
            } else {
                checkLayout = !replaceMeta;
            }
        }

        TMaybe<TColumnOrder> contentColumnOrder;
        if (content) {
            contentColumnOrder = State_->Types->LookupColumnOrder(*content);
            if (content->IsCallable("AssumeColumnOrder")) {
                YQL_ENSURE(contentColumnOrder);
                YQL_CLOG(INFO, ProviderYt) << "Dropping top level " << content->Content() << " from WriteTable input";
                content = content->HeadPtr();
            }
        }

        if (content && TCoPgSelect::Match(content.Get())) {
            auto pgSelect = TCoPgSelect(content);
            if (NCommon::NeedToRenamePgSelectColumns(pgSelect)) {
                TExprNode::TPtr output;

                const auto& columnOrder = (outTableInfo.RowSpec)
                    ? outTableInfo.RowSpec->GetColumnOrder()
                    : contentColumnOrder;

                bool result = NCommon::RenamePgSelectColumns(pgSelect, output, columnOrder, ctx, *State_->Types);
                if (!result) {
                    return TStatus::Error;
                }
                if (output != content) {
                    content = output;
                    return TStatus::Repeat;
                }
            }
        }

        if (checkLayout) {
            auto rowSpec = description.RowSpec;
            TString modeStr = EYtWriteMode::RenewKeepMeta == mode ? "truncate with keep meta" : ToString(mode);
            if (!rowSpec) {
                ctx.AddError(TIssue(pos, TStringBuilder()
                    << "Table " << outTableInfo.Name.Quote()
                    << " does not have any scheme attribute supported by YQL, " << modeStr << " is not allowed"));
                return TStatus::Error;
            }
            if (description.IgnoreTypeV3) {
                ctx.AddError(TIssue(pos, TStringBuilder()
                    << "Table " << outTableInfo.Name.Quote() << " has IgnoreTypeV3 remapper, " << modeStr << " is not allowed"));
                return TStatus::Error;
            }
            if (description.HasUdfApply) {
                ctx.AddError(TIssue(pos, TStringBuilder()
                    << "Table " << outTableInfo.Name.Quote() << " has udf remappers, " << modeStr << " is not allowed"));
                return TStatus::Error;
            }
            if (meta->InferredScheme) {
                ctx.AddError(TIssue(pos, TStringBuilder()
                    << "Table " << outTableInfo.Name.Quote() << " has inferred schema, " << modeStr << " is not allowed"));
                return TStatus::Error;
            }
            if (!rowSpec->StrictSchema) {
                ctx.AddError(TIssue(pos, TStringBuilder()
                    << "Table " << outTableInfo.Name.Quote() << " has non-strict schema, " << modeStr << " is not allowed"));
                return TStatus::Error;
            }
            if (meta->Attrs.contains(QB2Premapper)) {
                ctx.AddError(TIssue(pos, TStringBuilder()
                    << "Table " << outTableInfo.Name.Quote() << " has qb2 remapper, " << modeStr << " is not allowed"));
                return TStatus::Error;
            }

            if (!IsSameAnnotation(*description.RowType, *itemType)) {
                if (content) {
                    auto expectedType = ctx.MakeType<TListExprType>(description.RowType);
                    auto status = TryConvertTo(content, *expectedType, ctx);
                    if (status.Level != TStatus::Error) {
                        return status;
                    }
                }

                ctx.AddError(TIssue(pos, TStringBuilder()
                    << "Table " << outTableInfo.Name.Quote() << " row type differs from the written row type: "
                    << GetTypeDiff(*description.RowType, *itemType)));
                return TStatus::Error;
            }
        }

        if (auto commitEpoch = outTableInfo.CommitEpoch.GetOrElse(0)) {
            TYtTableDescription& nextDescription = State_->TablesData->GetOrAddTable(cluster, outTableInfo.Name, commitEpoch);

            if (!nextDescription.Meta) {
                nextDescription.RowType = itemType;
                nextDescription.RawRowType = itemType;
                nextDescription.IsReplaced = replaceMeta;

                TYtTableMetaInfo::TPtr nextMetadata = (nextDescription.Meta = MakeIntrusive<TYtTableMetaInfo>());
                nextMetadata->DoesExist = true;
                nextMetadata->YqlCompatibleScheme = true;

                TYqlRowSpecInfo::TPtr nextRowSpec = (nextDescription.RowSpec = MakeIntrusive<TYqlRowSpecInfo>());
                if (replaceMeta) {
                    nextRowSpec->SetType(itemType->Cast<TStructExprType>(), State_->Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES) ? NTCF_ALL : NTCF_NONE);
                    YQL_CLOG(INFO, ProviderYt) << "Saving column order: " << FormatColumnOrder(contentColumnOrder, 10);
                    nextRowSpec->SetColumnOrder(contentColumnOrder);
                } else {
                    nextRowSpec->CopyType(*description.RowSpec);
                }

                if (!replaceMeta) {
                    nextRowSpec->StrictSchema = !description.RowSpec || description.RowSpec->StrictSchema;
                    nextMetadata->Attrs = meta->Attrs;
                }
            }
            else {
                if (!IsSameAnnotation(*nextDescription.RowType, *itemType)) {
                    if (content) {
                        auto expectedType = ctx.MakeType<TListExprType>(nextDescription.RowType);
                        auto status = TryConvertTo(content, *expectedType, ctx);
                        if (status.Level != TStatus::Error) {
                            return status;
                        }
                    }

                    ctx.AddError(TIssue(pos, TStringBuilder()
                        << "Table " << outTableInfo.Name.Quote() << " row type differs from the appended row type: "
                        << GetTypeDiff(*nextDescription.RowType, *itemType)));
                    return TStatus::Error;
                }
            }

            if (initialWrite) {
                nextDescription.ColumnGroupSpec = columnGroups;
            } else if (columnGroups != nextDescription.ColumnGroupSpec) {
                ctx.AddError(TIssue(pos, TStringBuilder()
                    << "All appends within the same commit should have the equal "
                    << ToString(EYtSettingType::ColumnGroups).Quote()
                    << " value"));
                return TStatus::Error;
            }

            YQL_ENSURE(nextDescription.RowSpec);
            if (contentRowSpecs) {
                size_t from = 0;
                if (initialWrite) {
                    ++nextDescription.WriteValidateCount;
                    if (nextDescription.IsReplaced) {
                        nextDescription.RowSpec->CopySortness(ctx, *contentRowSpecs.front(), TYqlRowSpecInfo::ECopySort::Exact);
                        if (auto contentNativeType = contentRowSpecs.front()->GetNativeYtType()) {
                            nextDescription.RowSpec->CopyTypeOrders(*contentNativeType);
                        }
                        from = 1;
                    } else {
                        nextDescription.MonotonicKeys = monotonicKeys;
                        if (description.RowSpec) {
                            nextDescription.RowSpec->CopySortness(ctx, *description.RowSpec, TYqlRowSpecInfo::ECopySort::Exact);
                            const auto currNativeType = description.RowSpec->GetNativeYtType();
                            if (currNativeType && nextDescription.RowSpec->GetNativeYtType() != currNativeType) {
                                nextDescription.RowSpec->CopyTypeOrders(*currNativeType);
                            }
                        }
                    }
                    if (monotonicKeys && !nextDescription.RowSpec->IsSorted()) {
                        ctx.AddError(TIssue(pos, TStringBuilder()
                            << "Insert with "
                            << ToString(EYtSettingType::MonotonicKeys).Quote()
                            << " setting cannot be used with a unsorted table"));
                        return TStatus::Error;
                    }
                } else {
                    if (!nextDescription.MonotonicKeys) {
                        nextDescription.MonotonicKeys = monotonicKeys;
                    } else if (*nextDescription.MonotonicKeys != monotonicKeys) {
                        ctx.AddError(TIssue(pos, TStringBuilder()
                            << "All appends within the same commit should have the same "
                            << ToString(EYtSettingType::MonotonicKeys).Quote()
                            << " flag"));
                        return TStatus::Error;
                    }
                }

                const bool uniqueKeys = nextDescription.RowSpec->UniqueKeys;
                for (size_t s = from; s < contentRowSpecs.size(); ++s) {
                    const bool hasSortChanges = nextDescription.RowSpec->MakeCommonSortness(ctx, *contentRowSpecs[s]);
                    const bool breaksSorting = hasSortChanges || !nextDescription.RowSpec->CompareSortness(*contentRowSpecs[s], false);
                    if (monotonicKeys) {
                        if (breaksSorting) {
                            ctx.AddError(TIssue(pos, TStringBuilder()
                                << "Inserts with "
                                << ToString(EYtSettingType::MonotonicKeys).Quote()
                                << " setting must not change output table sorting"));
                            return TStatus::Error;
                        }
                        nextDescription.RowSpec->UniqueKeys = uniqueKeys;
                    }
                    if (nextDescription.WriteValidateCount < 2) {
                        TStringBuilder warning;
                        if (breaksSorting) {
                            warning << "Sort order of written data differs from the order of "
                                << outTableInfo.Name.Quote() << " table content. Result table content will be ";
                            if (nextDescription.RowSpec->IsSorted()) {
                                warning << "ordered by ";
                                for (size_t i: xrange(nextDescription.RowSpec->SortMembers.size())) {
                                    if (i != 0) {
                                        warning << ',';
                                    }
                                    warning << nextDescription.RowSpec->SortMembers[i] << '('
                                        << (nextDescription.RowSpec->SortDirections[i] ? "asc" : "desc") << ")";
                                }
                            } else {
                                warning << "unordered";
                            }
                        } else if (uniqueKeys && !nextDescription.RowSpec->UniqueKeys) {
                            warning << "Result table content will have non unique keys";
                        }

                        if (warning && !ctx.AddWarning(YqlIssue(pos, EYqlIssueCode::TIssuesIds_EIssueCode_YT_SORT_ORDER_CHANGE, warning))) {
                            return TStatus::Error;
                        }
                    }
                }
            }
        }
        else if (replaceMeta) {
            description.RowType = itemType;
            description.RawRowType = itemType;
            description.IsReplaced = true;

            description.Meta->DoesExist = true;
            description.Meta->YqlCompatibleScheme = true;
            if (!description.RowSpec) {
                description.RowSpec = MakeIntrusive<TYqlRowSpecInfo>();
            }
            description.RowSpec->SetType(itemType->Cast<TStructExprType>());
            YQL_CLOG(INFO, ProviderYt) << "Saving column order: " << FormatColumnOrder(contentColumnOrder, 10);
            description.RowSpec->SetColumnOrder(contentColumnOrder);
        }

        return TStatus::Ok;
    }

    static const TTypeAnnotationNode* GetInputItemType(TYtSectionList input, TExprContext& ctx) {
        TTypeAnnotationNode::TListType items;
        for (auto section: input.Cast<TYtSectionList>()) {
            items.push_back(section.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType());
        }
        return items.size() == 1
            ? items.front()
            : ctx.MakeType<TVariantExprType>(ctx.MakeType<TTupleExprType>(items));
    }

    static const TTypeAnnotationNode* MakeOutputOperationType(TYtOutputOpBase op, TExprContext& ctx) {
        TTypeAnnotationNode::TListType items;
        for (auto out: op.Output()) {
            items.push_back(out.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType());
        }

        const TTypeAnnotationNode* itemType = items.size() == 1
            ? items.front()
            : ctx.MakeType<TVariantExprType>(ctx.MakeType<TTupleExprType>(items));

        return ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
            op.World().Ref().GetTypeAnn(),
            ctx.MakeType<TListExprType>(itemType)
        });
    }

private:
    TStatus HandleOutTable(TExprBase input, TExprContext& ctx) {
        if (!TYtOutTableInfo::Validate(input.Ref(), ctx)) {
            return TStatus::Error;
        }
        input.Ptr()->SetTypeAnn(ctx.MakeType<TListExprType>(input.Cast<TYtOutTable>().RowSpec().Ref().GetTypeAnn()));
        return TStatus::Ok;
    }

    TStatus HandleOutput(TExprBase input, TExprContext& ctx) {
        if (!EnsureMinMaxArgsCount(input.Ref(), 2, 3, ctx)) {
            return TStatus::Error;
        }

        const auto op = input.Ref().Child(TYtOutput::idx_Operation);
        if (!(TYtOutputOpBase::Match(op) || TYtTryFirst::Match(op))) {
            ctx.AddError(TIssue(ctx.GetPosition(op->Pos()), TStringBuilder() << "Expect YT operation, but got: "
                << op->Content()));
            return TStatus::Error;
        }
        const auto opOut = TYtTryFirst::Match(op) ?
            op->Tail().Child(TYtOutputOpBase::idx_Output):
            op->Child(TYtOutputOpBase::idx_Output);

        if (!EnsureAtom(*input.Ptr()->Child(TYtOutput::idx_OutIndex), ctx)) {
            return TStatus::Error;
        }

        size_t ndx = 0;
        if (!TryFromString<size_t>(input.Ptr()->Child(TYtOutput::idx_OutIndex)->Content(), ndx) || ndx >= opOut->ChildrenSize()) {
            ctx.AddError(TIssue(ctx.GetPosition(input.Ptr()->Child(TYtOutput::idx_Operation)->Pos()), TStringBuilder()
                << "Bad " << TYtOutput::CallableName() << " output index value: " << input.Ptr()->Child(TYtOutput::idx_OutIndex)->Content()));
            return TStatus::Error;
        }

        if (input.Ref().ChildrenSize() == 3) {
            if (!EnsureAtom(*input.Ref().Child(TYtOutput::idx_Mode), ctx)) {
                return TStatus::Error;
            }
            if (input.Ref().Child(TYtOutput::idx_Mode)->Content() != ToString(EYtSettingType::Unordered)) {
                ctx.AddError(TIssue(ctx.GetPosition(input.Ref().Child(TYtOutput::idx_Mode)->Pos()), TStringBuilder()
                    << "Bad " << TYtOutput::CallableName() << " mode: " << input.Ref().Child(TYtOutput::idx_Mode)->Content()));
                return TStatus::Error;
            }
        }

        input.Ptr()->SetTypeAnn(opOut->Child(ndx)->GetTypeAnn());
        return TStatus::Ok;
    }

    TStatus HandleSort(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        if (!EnsureArgsCount(*input, 5, ctx)) {
            return TStatus::Error;
        }

        auto status = ValidateAndUpdateTransientOpBase(input, output, ctx, false,
            EYtSettingType::KeyFilter | EYtSettingType::KeyFilter2 | EYtSettingType::Take | EYtSettingType::Skip);
        if (status.Level != TStatus::Ok) {
            return status;
        }

        auto sort = TYtSort(input);

        TYtOutTableInfo outTableInfo(sort.Output().Item(0));
        if (!outTableInfo.RowSpec) {
            ctx.AddError(TIssue(ctx.GetPosition(sort.Output().Item(0).Pos()),
                TStringBuilder() << TString{YqlRowSpecAttribute}.Quote() << " of " << TYtSort::CallableName() << " output table should be filled"));
            return TStatus::Error;
        }

        if (outTableInfo.RowSpec->SortedBy.empty()) {
            ctx.AddError(TIssue(ctx.GetPosition(sort.Output().Item(0).Pos()),
                TStringBuilder() << "SortedBy attribute of " << TString{YqlRowSpecAttribute}.Quote()
                    << " should be filled in output table"));
            return TStatus::Error;
        }
        auto inputType = sort.Input().Item(0).Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
        for (auto& field: outTableInfo.RowSpec->SortedBy) {
            if (!inputType->FindItem(field)) {
                ctx.AddError(TIssue(ctx.GetPosition(sort.Output().Item(0).Pos()),
                    TStringBuilder() << "SortedBy attribute of " << TString{YqlRowSpecAttribute}.Quote()
                        << " refers to unknown field " << field.Quote()));
                return TStatus::Error;
            }
        }

        if (!ValidateSettings(sort.Settings().Ref(), EYtSettingType::Limit | EYtSettingType::NoDq, ctx)) {
            return TStatus::Error;
        }

        for (TYtSection section: sort.Input()) {
            for (TYtPath path: section.Paths()) {
                TYtPathInfo pathInfo(path);
                if (pathInfo.RequiresRemap()) {
                    ctx.AddError(TIssue(ctx.GetPosition(path.Pos()), TStringBuilder() << TYtSort::CallableName()
                        << " cannot be applied to tables with QB2 premapper, inferred, yamred_dsv, or non-strict schemas"));
                    return TStatus::Error;
                }
                if (pathInfo.Table->RowSpec && pathInfo.GetNativeYtTypeFlags() != outTableInfo.RowSpec->GetNativeYtTypeFlags()) {
                    ctx.AddError(TIssue(ctx.GetPosition(path.Pos()), TStringBuilder() << TYtSort::CallableName()
                        << " has different input/output native YT types"));
                    return TStatus::Error;
                }
            }
        }

        auto outTypeItems = outTableInfo.RowSpec->GetType()->GetItems();
        const auto& directions = outTableInfo.RowSpec->SortDirections;
        const auto& sortedBy = outTableInfo.RowSpec->SortedBy;
        for (auto& aux: outTableInfo.RowSpec->GetAuxColumns()) {
            bool adjust = false;
            for (ui32 i = 0; i < directions.size(); ++i) {
                if (!directions[i] && sortedBy[i] == aux.first) {
                    adjust = true;
                    break;
                }
            }

            auto type = aux.second;
            if (adjust) {
                type = ctx.MakeType<TDataExprType>(EDataSlot::String);
            }

            outTypeItems.push_back(ctx.MakeType<TItemExprType>(aux.first, type));
        }
        auto outType = ctx.MakeType<TStructExprType>(outTypeItems);

        if (!IsSameAnnotation(*inputType, *outType)) {
            ctx.AddError(TIssue(ctx.GetPosition(sort.Output().Item(0).Pos()), TStringBuilder()
                << "Sort's output row type differs from input row type: "
                << GetTypeDiff(*inputType, *outType)));
            return TStatus::Error;
        }

        input->SetTypeAnn(MakeOutputOperationType(sort, ctx));
        return TStatus::Ok;
    }

    TStatus HandleCopy(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        if (!EnsureArgsCount(*input, 5, ctx)) {
            return TStatus::Error;
        }

        auto status = ValidateAndUpdateTransientOpBase(input, output, ctx, false, {});
        if (status.Level != TStatus::Ok) {
            return status;
        }

        auto copy = TYtCopy(input);

        // YtCopy! has exactly one input table
        if (!EnsureArgsCount(copy.Input().Item(0).Paths().Ref(), 1, ctx)) {
            return TStatus::Error;
        }

        TYqlRowSpecInfo outRowSpec(copy.Output().Item(0).RowSpec());
        for (TYtSection section: copy.Input()) {
            for (TYtPath path: section.Paths()) {
                if (!path.Ranges().Maybe<TCoVoid>()) {
                    ctx.AddError(TIssue(ctx.GetPosition(path.Pos()), TStringBuilder() << TYtCopy::CallableName() << " cannot be used with range selection"));
                    return TStatus::Error;
                }
                auto tableInfo = TYtTableBaseInfo::Parse(path.Table());
                if (!tableInfo->IsTemp) {
                    ctx.AddError(TIssue(ctx.GetPosition(path.Pos()), TStringBuilder() << TYtCopy::CallableName() << " cannot be used with non-temporary tables"));
                    return TStatus::Error;
                }
                if (tableInfo->Meta->IsDynamic) {
                    ctx.AddError(TIssue(ctx.GetPosition(path.Pos()), TStringBuilder() << TYtCopy::CallableName() << " cannot be used with dynamic tables"));
                    return TStatus::Error;
                }
                auto tableColumnsSize = tableInfo->RowSpec ? tableInfo->RowSpec->GetType()->GetSize() : YAMR_FIELDS.size();
                if (!path.Columns().Maybe<TCoVoid>() && path.Columns().Cast<TExprList>().Size() != tableColumnsSize) {
                    ctx.AddError(TIssue(ctx.GetPosition(path.Pos()), TStringBuilder() << TYtCopy::CallableName() << " cannot be used with column selection"));
                    return TStatus::Error;
                }
                if (tableInfo->RequiresRemap()) {
                    ctx.AddError(TIssue(ctx.GetPosition(path.Pos()), TStringBuilder() << TYtCopy::CallableName()
                        << " cannot be applied to tables with QB2 premapper, inferred, yamred_dsv, or non-strict schemas"));
                    return TStatus::Error;
                }
                if (tableInfo->RowSpec && tableInfo->RowSpec->GetNativeYtTypeFlags() != outRowSpec.GetNativeYtTypeFlags()) {
                    ctx.AddError(TIssue(ctx.GetPosition(path.Pos()), TStringBuilder() << TYtCopy::CallableName()
                        << " has different input/output native YT types"));
                    return TStatus::Error;
                }
            }
        }

        auto inputRowSpec = TYtTableBaseInfo::GetRowSpec(copy.Input().Item(0).Paths().Item(0).Table());
        if ((inputRowSpec && !inputRowSpec->CompareSortness(outRowSpec)) || (!inputRowSpec && outRowSpec.IsSorted())) {
            ctx.AddError(TIssue(ctx.GetPosition(copy.Output().Item(0).Pos()), TStringBuilder()
                << "Input/output tables have different sort order"));
            return TStatus::Error;
        }

        // YtCopy! has no settings
        if (!EnsureTupleSize(copy.Settings().MutableRef(), 0, ctx)) {
            return TStatus::Error;
        }

        if (!ValidateOutputType(copy.Input().Item(0).Paths().Item(0).Ref(), copy.Output(), ctx)) {
            return TStatus::Error;
        }

        input->SetTypeAnn(MakeOutputOperationType(copy, ctx));
        return TStatus::Ok;
    }

    TStatus HandleMerge(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        if (!EnsureArgsCount(*input, 5, ctx)) {
            return TStatus::Error;
        }

        auto status = ValidateAndUpdateTransientOpBase(input, output, ctx, false,
            EYtSettingType::KeyFilter | EYtSettingType::KeyFilter2 | EYtSettingType::Take | EYtSettingType::Skip | EYtSettingType::Sample);
        if (status.Level != TStatus::Ok) {
            return status;
        }

        auto merge = TYtMerge(input);

        if (!ValidateSettings(merge.Settings().Ref(), EYtSettingType::ForceTransform | EYtSettingType::CombineChunks | EYtSettingType::Limit | EYtSettingType::KeepSorted | EYtSettingType::NoDq, ctx)) {
            return TStatus::Error;
        }

        TYqlRowSpecInfo outRowSpec(merge.Output().Item(0).RowSpec());
        TVector<TString> outSortedBy = outRowSpec.SortedBy;
        for (auto section: merge.Input()) {
            for (auto path: section.Paths()) {
                if (!IsSameAnnotation(*path.Ref().GetTypeAnn(), *merge.Output().Item(0).Ref().GetTypeAnn())) {
                    ctx.AddError(TIssue(ctx.GetPosition(path.Pos()), TStringBuilder()
                        << "Input/output tables should have the same scheme. Found diff: "
                        << GetTypeDiff(*path.Ref().GetTypeAnn(), *merge.Output().Item(0).Ref().GetTypeAnn())));
                    return TStatus::Error;
                }
                TYtPathInfo pathInfo(path);
                if (pathInfo.RequiresRemap()) {
                    ctx.AddError(TIssue(ctx.GetPosition(path.Pos()), TStringBuilder() << TYtMerge::CallableName()
                        << " cannot be applied to tables with QB2 premapper, inferred, yamred_dsv, or non-strict schemas"));
                    return TStatus::Error;
                }
                if (pathInfo.GetNativeYtTypeFlags() != outRowSpec.GetNativeYtTypeFlags()) {
                    ctx.AddError(TIssue(ctx.GetPosition(path.Pos()), TStringBuilder() << TYtMerge::CallableName()
                        << " has different input/output native YT types"));
                    return TStatus::Error;
                }
                auto inputRowSpec = pathInfo.Table->RowSpec;
                TVector<TString> inSortedBy;
                TVector<bool> inSortDir;
                if (inputRowSpec) {
                    inSortedBy = inputRowSpec->SortedBy;
                    inSortDir = inputRowSpec->SortDirections;
                }
                if (!outSortedBy.empty()) {
                    if (outSortedBy.size() > inSortedBy.size() ||
                        !std::equal(outSortedBy.begin(), outSortedBy.end(), inSortedBy.begin())) {

                        ctx.AddError(TIssue(ctx.GetPosition(path.Pos()), TStringBuilder()
                            << "Output table has sortedBy columns " << JoinSeq(",", outSortedBy).Quote()
                            << ", which is not a subset of " << TString{TYtTableBaseInfo::GetTableName(path.Table())}.Quote()
                            << " input table sortedBy columns " << JoinSeq(",", inSortedBy).Quote()));
                        return TStatus::Error;
                    }
                    if (!std::equal(outRowSpec.SortDirections.begin(), outRowSpec.SortDirections.end(), inSortDir.begin())) {
                        ctx.AddError(TIssue(ctx.GetPosition(path.Pos()), TStringBuilder()
                            << "Input/output tables have different sort directions"));
                        return TStatus::Error;
                    }
                }
            }
        }

        if (!ValidateOutputType(merge.Input().Item(0).Ref(), merge.Output(), ctx)) {
            return TStatus::Error;
        }

        input->SetTypeAnn(MakeOutputOperationType(merge, ctx));
        return TStatus::Ok;
    }

    TStatus HandleMap(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        if (!EnsureArgsCount(*input, 6, ctx)) {
            return TStatus::Error;
        }

        auto status = ValidateAndUpdateTransientOpBase(input, output, ctx, true,
            EYtSettingType::KeyFilter | EYtSettingType::KeyFilter2 | EYtSettingType::Take | EYtSettingType::Skip | EYtSettingType::Sample | EYtSettingType::SysColumns);
        if (status.Level != TStatus::Ok) {
            return status;
        }

        auto map = TYtMap(input);

        const EYtSettingTypes accpeted = EYtSettingType::Ordered
            | EYtSettingType::Limit
            | EYtSettingType::SortLimitBy
            | EYtSettingType::WeakFields
            | EYtSettingType::Sharded
            | EYtSettingType::JobCount
            | EYtSettingType::Flow
            | EYtSettingType::KeepSorted
            | EYtSettingType::NoDq;
        if (!ValidateSettings(map.Settings().Ref(), accpeted, ctx)) {
            return TStatus::Error;
        }
        if (map.Output().Size() != 1 && NYql::HasSetting(map.Settings().Ref(), EYtSettingType::Limit)) {
            ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder()
                << EYtSettingType::Limit << " setting is not allowed for operation with multiple outputs"));
            return TStatus::Error;
        }
        TVector<TString> sortLimitBy = NYql::GetSettingAsColumnList(map.Settings().Ref(), EYtSettingType::SortLimitBy);
        if (!sortLimitBy.empty()) {
            auto outItemType = TYqlRowSpecInfo(map.Output().Item(0).RowSpec()).GetExtendedType(ctx);
            if (!ValidateColumnListSetting(ctx.GetPosition(input->Pos()), ctx, outItemType, sortLimitBy, EYtSettingType::SortLimitBy, false)) {
                return TStatus::Error;
            }
        }

        status = ConvertToLambda(input->ChildRef(TYtMap::idx_Mapper), ctx, 1);
        if (status.Level != TStatus::Ok) {
            return status;
        }

        const auto inputItemType = GetInputItemType(map.Input(), ctx);
        const auto useFlow = NYql::GetSetting(map.Settings().Ref(), EYtSettingType::Flow);
        const auto lambdaInputType = MakeInputType(inputItemType, useFlow, ctx);

        auto& lambda = input->ChildRef(TYtMap::idx_Mapper);
        if (!UpdateLambdaAllArgumentsTypes(lambda, {lambdaInputType}, ctx)) {
            return TStatus::Error;
        }

        if (!lambda->GetTypeAnn()) {
            return TStatus::Repeat;
        }

        if (!(useFlow ? EnsureFlowType(*lambda, ctx) : EnsureStreamType(*lambda, ctx))) {
            return TStatus::Error;
        }

        if (!ValidateOutputType(*lambda, map.Output(), ctx, true)) {
            lambda->SetTypeAnn(nullptr);
            lambda->SetState(TExprNode::EState::Initial);
            return TStatus::Error;
        }

        input->SetTypeAnn(MakeOutputOperationType(map, ctx));
        return TStatus::Ok;
    }

    TStatus HandleReduce(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        if (!EnsureArgsCount(*input, 6, ctx)) {
            return TStatus::Error;
        }

        auto status = ValidateAndUpdateTransientOpBase(input, output, ctx, true,
            EYtSettingType::KeyFilter | EYtSettingType::KeyFilter2 | EYtSettingType::Take | EYtSettingType::Skip | EYtSettingType::Sample | EYtSettingType::SysColumns);
        if (status.Level != TStatus::Ok) {
            return status;
        }

        auto reduce = TYtReduce(input);

        const EYtSettingTypes accepted = EYtSettingType::ReduceBy
            | EYtSettingType::Limit
            | EYtSettingType::SortLimitBy
            | EYtSettingType::SortBy
            | EYtSettingType::JoinReduce
            | EYtSettingType::FirstAsPrimary
            | EYtSettingType::Flow
            | EYtSettingType::KeepSorted
            | EYtSettingType::KeySwitch
            | EYtSettingType::ReduceInputType
            | EYtSettingType::NoDq;

        if (!ValidateSettings(reduce.Settings().Ref(), accepted, ctx)) {
            return TStatus::Error;
        }

        if (reduce.Output().Size() != 1 && NYql::HasSetting(reduce.Settings().Ref(), EYtSettingType::Limit)) {
            ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder()
                << EYtSettingType::Limit << " setting is not allowed in operation with multiple outputs"));
            return TStatus::Error;
        }
        TVector<TString> sortLimitBy = NYql::GetSettingAsColumnList(reduce.Settings().Ref(), EYtSettingType::SortLimitBy);
        if (!sortLimitBy.empty()) {
            auto outItemType = reduce.Output().Item(0).Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType();
            if (!ValidateColumnListSetting(ctx.GetPosition(input->Pos()), ctx, outItemType, sortLimitBy, EYtSettingType::SortLimitBy, false)) {
                return TStatus::Error;
            }
        }

        status = ConvertToLambda(input->ChildRef(TYtReduce::idx_Reducer), ctx, 1);
        if (status.Level != TStatus::Ok) {
            return status;
        }

        TVector<TString> reduceBy = NYql::GetSettingAsColumnList(reduce.Settings().Ref(), EYtSettingType::ReduceBy);
        if (reduceBy.empty()) {
            ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder() << "Empty reduceBy option is not allowed in " << input->Content()));
            return TStatus::Error;
        }

        auto inputItemType = GetInputItemType(reduce.Input(), ctx);
        if (!ValidateColumnListSetting(ctx.GetPosition(input->Pos()), ctx, inputItemType, reduceBy, EYtSettingType::ReduceBy, true)) {
            return TStatus::Error;
        }

        if (NYql::HasSetting(reduce.Settings().Ref(), EYtSettingType::SortBy)) {
            TVector<TString> sortBy = NYql::GetSettingAsColumnList(reduce.Settings().Ref(), EYtSettingType::SortBy);
            if (!ValidateColumnListSetting(ctx.GetPosition(input->Pos()), ctx, inputItemType, sortBy, EYtSettingType::SortBy, true)) {
                return TStatus::Error;
            }
        }

        if (NYql::HasSetting(reduce.Settings().Ref(), EYtSettingType::KeySwitch)) {
            if (inputItemType->GetKind() == ETypeAnnotationKind::Variant) {
                auto underTupleType = inputItemType->Cast<TVariantExprType>()->GetUnderlyingType()->Cast<TTupleExprType>();
                TTypeAnnotationNode::TListType newTupleItems;
                for (size_t i = 0; i < underTupleType->GetSize(); ++i) {
                    auto tupleItemType = underTupleType->GetItems()[i];
                    auto items = tupleItemType->Cast<TStructExprType>()->GetItems();
                    items.push_back(ctx.MakeType<TItemExprType>(YqlSysColumnKeySwitch, ctx.MakeType<TDataExprType>(EDataSlot::Bool)));
                    newTupleItems.push_back(ctx.MakeType<TStructExprType>(items));
                }
                inputItemType = ctx.MakeType<TVariantExprType>(ctx.MakeType<TTupleExprType>(newTupleItems));
            } else {
                auto items = inputItemType->Cast<TStructExprType>()->GetItems();
                items.push_back(ctx.MakeType<TItemExprType>(YqlSysColumnKeySwitch, ctx.MakeType<TDataExprType>(EDataSlot::Bool)));
                inputItemType = ctx.MakeType<TStructExprType>(items);
            }
        }

        if (NYql::HasSetting(reduce.Settings().Ref(), EYtSettingType::JoinReduce)) {
            // Require at least two sections
            if (!EnsureTupleMinSize(reduce.Input().MutableRef(), 2, ctx)) {
                return TStatus::Error;
            }
        }

        const auto useFlow = NYql::GetSetting(reduce.Settings().Ref(), EYtSettingType::Flow);
        const auto lambdaInputType = MakeInputType(inputItemType, useFlow, ctx);

        auto& lambda = input->ChildRef(TYtReduce::idx_Reducer);
        if (!UpdateLambdaAllArgumentsTypes(lambda, {lambdaInputType}, ctx)) {
            return TStatus::Error;
        }

        if (!lambda->GetTypeAnn()) {
            return TStatus::Repeat;
        }

        if (!(useFlow ? EnsureFlowType(*lambda, ctx) : EnsureStreamType(*lambda, ctx))) {
            return TStatus::Error;
        }

        if (!ValidateOutputType(*lambda, reduce.Output(), ctx)) {
            lambda->SetTypeAnn(nullptr);
            lambda->SetState(TExprNode::EState::Initial);
            return TStatus::Error;
        }

        input->SetTypeAnn(MakeOutputOperationType(reduce, ctx));
        return TStatus::Ok;
    }

    TStatus HandleMapReduce(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        if (!EnsureArgsCount(*input, 7, ctx)) {
            return TStatus::Error;
        }

        const bool hasMapLambda = !TCoVoid::Match(input->Child(TYtMapReduce::idx_Mapper));
        EYtSettingTypes sectionSettings = EYtSettingType::KeyFilter | EYtSettingType::KeyFilter2 | EYtSettingType::Take | EYtSettingType::Skip | EYtSettingType::Sample;
        if (hasMapLambda) {
            sectionSettings |= EYtSettingType::SysColumns;
        }
        auto status = ValidateAndUpdateTransientOpBase(input, output, ctx, true, sectionSettings);
        if (status.Level != TStatus::Ok) {
            return status;
        }

        auto mapReduce = TYtMapReduce(input);

        const auto acceptedSettings = EYtSettingType::ReduceBy
            | EYtSettingType::ReduceFilterBy
            | EYtSettingType::SortBy
            | EYtSettingType::Limit
            | EYtSettingType::SortLimitBy
            | EYtSettingType::WeakFields
            | EYtSettingType::Flow
            | EYtSettingType::KeySwitch
            | EYtSettingType::MapOutputType
            | EYtSettingType::ReduceInputType
            | EYtSettingType::NoDq;
        if (!ValidateSettings(mapReduce.Settings().Ref(), acceptedSettings, ctx)) {
            return TStatus::Error;
        }

        if (mapReduce.Output().Size() != 1 && NYql::HasSetting(mapReduce.Settings().Ref(), EYtSettingType::Limit)) {
            ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder()
                << EYtSettingType::Limit << " setting is not allowed in operation with multiple outputs"));
            return TStatus::Error;
        }
        TVector<TString> sortLimitBy = NYql::GetSettingAsColumnList(mapReduce.Settings().Ref(), EYtSettingType::SortLimitBy);
        if (!sortLimitBy.empty()) {
            auto outItemType = mapReduce.Output().Item(0).Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType();
            if (!ValidateColumnListSetting(ctx.GetPosition(input->Pos()), ctx, outItemType, sortLimitBy, EYtSettingType::SortLimitBy, false)) {
                return TStatus::Error;
            }
        }

        // Ensure output table is not sorted
        for (auto out: mapReduce.Output()) {
            if (TYqlRowSpecInfo(out.RowSpec()).IsSorted()) {
                ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder()
                    << TYtMapReduce::CallableName() << " cannot produce sorted output"));
                return TStatus::Error;
            }
        }

        status = TStatus::Ok;
        if (hasMapLambda) {
            status = status.Combine(ConvertToLambda(input->ChildRef(TYtMapReduce::idx_Mapper), ctx, 1));
        }
        status = status.Combine(ConvertToLambda(input->ChildRef(TYtMapReduce::idx_Reducer), ctx, 1));
        if (status.Level != TStatus::Ok) {
            return status;
        }

        auto itemType = GetInputItemType(mapReduce.Input(), ctx);
        const auto useFlow = NYql::GetSetting(mapReduce.Settings().Ref(), EYtSettingType::Flow);

        auto& mapLambda = input->ChildRef(TYtMapReduce::idx_Mapper);
        TTypeAnnotationNode::TListType mapDirectOutputTypes;
        if (hasMapLambda) {
            const auto mapLambdaInputType = MakeInputType(itemType, useFlow, ctx);

            if (!UpdateLambdaAllArgumentsTypes(mapLambda, {mapLambdaInputType}, ctx)) {
                return TStatus::Error;
            }

            if (!mapLambda->GetTypeAnn()) {
                return TStatus::Repeat;
            }

            if (!(useFlow ? EnsureFlowType(*mapLambda, ctx) : EnsureStreamType(*mapLambda, ctx))) {
                return TStatus::Error;
            }

            itemType = GetSequenceItemType(mapLambda->Pos(), mapLambda->GetTypeAnn(), true, ctx);
            if (!itemType) {
                return TStatus::Error;
            }

            if (!EnsurePersistableType(mapLambda->Pos(), *itemType, ctx)) {
                return TStatus::Error;
            }

            if (itemType->GetKind() == ETypeAnnotationKind::Variant) {
                auto tupleType = itemType->Cast<TVariantExprType>()->GetUnderlyingType();
                if (tupleType->GetKind() != ETypeAnnotationKind::Tuple) {
                    ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder()
                        << "Expected Variant over Tuple as map output type, but got: " << *itemType));
                    return TStatus::Error;
                }

                auto mapOutputs = tupleType->Cast<TTupleExprType>()->GetItems();
                YQL_ENSURE(!mapOutputs.empty(), "Got Variant over empty tuple");
                itemType = mapOutputs.front();
                for (size_t i = 1; i < mapOutputs.size(); ++i) {
                    if (mapOutputs[i]->GetKind() != ETypeAnnotationKind::Struct) {
                        ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder()
                            << "Expected Struct as map direct output #" << i << ", but got: " << *mapOutputs[i]));
                        return TStatus::Error;
                    }
                }
                mapDirectOutputTypes.assign(mapOutputs.begin() + 1, mapOutputs.end());
            }

            if (const auto kind = itemType->GetKind(); ETypeAnnotationKind::Multi != kind && ETypeAnnotationKind::Struct != kind) {
                ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder()
                    << "Expected Struct or Multi as map output item type, but got: " << *itemType));
                return TStatus::Error;
            }
        }

        TVector<TString> reduceBy = NYql::GetSettingAsColumnList(mapReduce.Settings().Ref(), EYtSettingType::ReduceBy);
        if (reduceBy.empty()) {
            ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder()
                << "Empty " << EYtSettingType::ReduceBy << " option is not allowed in " << input->Content()));
            return TStatus::Error;
        }

        if (!ValidateColumnListSetting(ctx.GetPosition(input->Pos()), ctx, itemType, reduceBy, EYtSettingType::ReduceBy, true)) {
            return TStatus::Error;
        }

        if (NYql::HasSetting(mapReduce.Settings().Ref(), EYtSettingType::SortBy)) {
            TVector<TString> sortBy = NYql::GetSettingAsColumnList(mapReduce.Settings().Ref(), EYtSettingType::SortBy);
            if (!ValidateColumnListSetting(ctx.GetPosition(input->Pos()), ctx, itemType, sortBy, EYtSettingType::SortBy, true)) {
                return TStatus::Error;
            }
        }

        if (const auto reduceInputTypeSetting = NYql::GetSetting(mapReduce.Settings().Ref(), EYtSettingType::ReduceInputType)) {
            itemType = reduceInputTypeSetting->Tail().GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        } else {
            if (NYql::HasSetting(mapReduce.Settings().Ref(), EYtSettingType::ReduceFilterBy)) {
                TVector<TString> reduceFilterBy = NYql::GetSettingAsColumnList(mapReduce.Settings().Ref(), EYtSettingType::ReduceFilterBy);
                if (!ValidateColumnListSetting(ctx.GetPosition(input->Pos()), ctx, itemType, reduceFilterBy, EYtSettingType::ReduceFilterBy, true)) {
                    return TStatus::Error;
                }

                auto structType = itemType->Cast<TStructExprType>();
                TVector<const TItemExprType*> filteredItems;
                for (auto& column: reduceFilterBy) {
                    filteredItems.push_back(structType->GetItems()[*structType->FindItem(column)]);
                }

                itemType = ctx.MakeType<TStructExprType>(filteredItems);
            }

            if (NYql::HasSetting(mapReduce.Settings().Ref(), EYtSettingType::KeySwitch)) {
                if (itemType->GetKind() == ETypeAnnotationKind::Variant) {
                    auto underTupleType = itemType->Cast<TVariantExprType>()->GetUnderlyingType()->Cast<TTupleExprType>();
                    TTypeAnnotationNode::TListType newTupleItems;
                    for (size_t i = 0; i < underTupleType->GetSize(); ++i) {
                        auto tupleItemType = underTupleType->GetItems()[i];
                        auto items = tupleItemType->Cast<TStructExprType>()->GetItems();
                        items.push_back(ctx.MakeType<TItemExprType>(YqlSysColumnKeySwitch, ctx.MakeType<TDataExprType>(EDataSlot::Bool)));
                        newTupleItems.push_back(ctx.MakeType<TStructExprType>(items));
                    }
                    itemType = ctx.MakeType<TVariantExprType>(ctx.MakeType<TTupleExprType>(newTupleItems));
                } else {
                    auto items = itemType->Cast<TStructExprType>()->GetItems();
                    items.push_back(ctx.MakeType<TItemExprType>(YqlSysColumnKeySwitch, ctx.MakeType<TDataExprType>(EDataSlot::Bool)));
                    itemType = ctx.MakeType<TStructExprType>(items);
                }
            }
        }

        auto& reduceLambda = input->ChildRef(TYtMapReduce::idx_Reducer);
        const auto reduceLambdaInputType = MakeInputType(itemType, useFlow, ctx);

        if (!UpdateLambdaAllArgumentsTypes(reduceLambda, {reduceLambdaInputType}, ctx)) {
            return TStatus::Error;
        }

        if (!reduceLambda->GetTypeAnn()) {
            return TStatus::Repeat;
        }

        if (!(useFlow ? EnsureFlowType(*reduceLambda, ctx) : EnsureStreamType(*reduceLambda, ctx))) {
            return TStatus::Error;
        }

        size_t mapDirects = mapDirectOutputTypes.size();
        if (mapDirects == 0) {
            if (!ValidateOutputType(*reduceLambda, mapReduce.Output(), ctx)) {
                return TStatus::Error;
            }
        } else {
            const size_t outputsSize = mapReduce.Output().Ref().ChildrenSize();
            if (mapDirects >= outputsSize) {
                ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder()
                    << "Too many map lambda direct outputs: " << mapDirects << ". Total outputs is " << outputsSize));
                return TStatus::Error;
            }

            const TTypeAnnotationNode* mapOut = ctx.MakeType<TVariantExprType>(ctx.MakeType<TTupleExprType>(mapDirectOutputTypes));
            if (!ValidateOutputType(mapOut, mapLambda->Pos(), mapReduce.Output(), 0, mapDirects, false, ctx)) {
                return TStatus::Error;
            }

            const TTypeAnnotationNode* reduceOutType = GetSequenceItemType(reduceLambda->Pos(), reduceLambda->GetTypeAnn(), true, ctx);
            if (!reduceOutType) {
                return TStatus::Error;
            }

            if (!ValidateOutputType(reduceOutType, reduceLambda->Pos(), mapReduce.Output(), mapDirects, outputsSize, false, ctx)) {
                return TStatus::Error;
            }
        }

        input->SetTypeAnn(MakeOutputOperationType(mapReduce, ctx));
        return TStatus::Ok;
    }

    TStatus HandleWriteTable(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        if (!EnsureArgsCount(*input, 5, ctx)) {
            return TStatus::Error;
        }

        if (input->Child(TYtWriteTable::idx_Content)->IsList()) {
            output = ctx.ChangeChild(*input, TYtWriteTable::idx_Content,
                Build<TCoAsList>(ctx, input->Pos())
                    .Add(input->Child(TYtWriteTable::idx_Content)->ChildrenList())
                    .Done()
                    .Ptr()
                );
            return TStatus::Repeat;
        }

        if (input->Child(TYtWriteTable::idx_Content)->GetTypeAnn()->GetKind() == ETypeAnnotationKind::EmptyList) {
            output = ctx.ChangeChild(*input, TYtWriteTable::idx_Content,
                Build<TCoList>(ctx, input->Pos())
                   .ListType<TCoListType>()
                       .ItemType<TCoStructType>()
                       .Build()
                   .Build()
                   .Done().Ptr()
                );
            return TStatus::Repeat;
        }

        if (!ValidateOpBase(input, ctx)) {
            return TStatus::Error;
        }

        auto table = input->ChildPtr(TYtWriteTable::idx_Table);
        if (!EnsureCallable(*table, ctx)) {
            return TStatus::Error;
        }
        if (!table->IsCallable(TYtTable::CallableName())) {
            ctx.AddError(TIssue(ctx.GetPosition(table->Pos()), TStringBuilder()
                << "Unexpected callable: " << table->Content()));
            return TStatus::Error;
        }

        auto settings = input->Child(TYtWriteTable::idx_Settings);
        if (!EnsureTuple(*settings, ctx)) {
            return TStatus::Error;
        }

        if (!ValidateSettings(*settings, EYtSettingType::Mode
            | EYtSettingType::Initial
            | EYtSettingType::CompressionCodec
            | EYtSettingType::ErasureCodec
            | EYtSettingType::ReplicationFactor
            | EYtSettingType::UserAttrs
            | EYtSettingType::Media
            | EYtSettingType::PrimaryMedium
            | EYtSettingType::Expiration
            | EYtSettingType::MonotonicKeys
            | EYtSettingType::MutationId
            | EYtSettingType::ColumnGroups
            | EYtSettingType::SecurityTags
            , ctx))
        {
            return TStatus::Error;
        }

        TExprNode::TPtr newTable;
        auto status = UpdateTableMeta(table, newTable, State_->TablesData, false, State_->Types->UseTableMetaFromGraph, ctx);
        if (TStatus::Ok != status.Level) {
            if (TStatus::Error != status.Level && newTable != table) {
                output = ctx.ChangeChild(*input, TYtWriteTable::idx_Table, std::move(newTable));
            }
            return status.Combine(TStatus::Repeat);
        }

        auto writeTable = TYtWriteTable(input);
        auto cluster = writeTable.DataSink().Cluster().StringValue();

        const TTypeAnnotationNode* itemType = nullptr;
        if (!GetSequenceItemType(writeTable.Content().Ref(), itemType, ctx)) {
            return TStatus::Error;
        }

        auto content =  writeTable.Content().Ptr();
        status = ValidateTableWrite(ctx.GetPosition(input->Pos()), table, content, itemType, {}, cluster, *settings, ctx);
        if (TStatus::Error == status.Level) {
            return status;
        }
        else if (content != writeTable.Content().Ptr()) {
            output = ctx.ChangeChild(*input, TYtWriteTable::idx_Content, std::move(content));
            return status.Combine(TStatus::Repeat);
        }

        input->SetTypeAnn(writeTable.World().Ref().GetTypeAnn());
        return TStatus::Ok;
    }

    TStatus HandleFill(const TExprNode::TPtr& input, TExprNode::TPtr& /*output*/, TExprContext& ctx) {
        if (!EnsureArgsCount(*input, 5, ctx)) {
            return TStatus::Error;
        }

        if (!ValidateOutputOpBase(input, ctx, true)) {
            return TStatus::Error;
        }

        auto status = ConvertToLambda(input->ChildRef(TYtFill::idx_Content), ctx, 0);
        if (status.Level != TStatus::Ok) {
            return status;
        }

        // Basic Settings validation
        if (!EnsureTuple(*input->Child(TYtFill::idx_Settings), ctx)) {
            return TStatus::Error;
        }

        for (auto& setting: input->Child(TYtFill::idx_Settings)->Children()) {
            if (!EnsureTupleMinSize(*setting, 1, ctx)) {
                return TStatus::Error;
            }

            if (!EnsureAtom(*setting->Child(0), ctx)) {
                return TStatus::Error;
            }
        }

        const EYtSettingTypes accepted = EYtSettingType::Flow | EYtSettingType::KeepSorted | EYtSettingType::NoDq;
        if (!ValidateSettings(*input->Child(TYtFill::idx_Settings), accepted, ctx)) {
            return TStatus::Error;
        }

        auto fill = TYtFill(input);
        auto lambda = fill.Content();
        if (!lambda.Args().Ref().GetTypeAnn()) {
            if (!UpdateLambdaArgumentsType(lambda.Ref(), ctx))
                return TStatus::Error;
            return TStatus::Repeat;
        }

        if (!lambda.Ref().GetTypeAnn()) {
            return TStatus::Repeat;
        }

        const bool useFlow = NYql::HasSetting(fill.Settings().Ref(), EYtSettingType::Flow);
        if (!(useFlow ? EnsureFlowType(lambda.Ref(), ctx) : EnsureStreamType(lambda.Ref(), ctx))) {
            return TStatus::Error;
        }

        if (!ValidateOutputType(lambda.Ref(), fill.Output(), ctx, true)) {
            return TStatus::Error;
        }

        input->SetTypeAnn(MakeOutputOperationType(fill, ctx));
        return TStatus::Ok;
    }

    TStatus HandleTouch(TExprBase input, TExprContext& ctx) {
        if (!EnsureArgsCount(input.Ref(), 3, ctx)) {
            return TStatus::Error;
        }

        if (!ValidateOutputOpBase(input.Ptr(), ctx, true)) {
            return TStatus::Error;
        }

        TYtTouch touch = input.Cast<TYtTouch>();

        input.Ptr()->SetTypeAnn(MakeOutputOperationType(touch, ctx));
        return TStatus::Ok;
    }

    TStatus HandleDropTable(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        if (!EnsureArgsCount(*input, 3, ctx)) {
            return TStatus::Error;
        }

        if (!ValidateOpBase(input, ctx)) {
            return TStatus::Error;
        }

        auto table = input->ChildPtr(TYtDropTable::idx_Table);
        if (!EnsureCallable(*table, ctx)) {
            return TStatus::Error;
        }
        if (!table->IsCallable(TYtTable::CallableName())) {
            ctx.AddError(TIssue(ctx.GetPosition(table->Pos()), TStringBuilder() << "Expected " << TYtTable::CallableName()
                << " callable, but got " << table->Content()));
            return TStatus::Error;
        }

        auto dropTable = TYtDropTable(input);
        if (!TYtTableInfo::HasSubstAnonymousLabel(dropTable.Table())) {
            TExprNode::TPtr newTable;
            auto status = UpdateTableMeta(table, newTable, State_->TablesData, false, State_->Types->UseTableMetaFromGraph, ctx);
            if (TStatus::Ok != status.Level) {
                if (TStatus::Error != status.Level && newTable != table) {
                    output = ctx.ChangeChild(*input, TYtWriteTable::idx_Table, std::move(newTable));
                }
                return status.Combine(TStatus::Repeat);
            }
            auto tableInfo = TYtTableInfo(dropTable.Table());
            YQL_ENSURE(tableInfo.Meta);
            if (tableInfo.Meta->SqlView) {
                ctx.AddError(TIssue(ctx.GetPosition(dropTable.Table().Pos()), TStringBuilder()
                    << "Drop of " << tableInfo.Name.Quote() << " view is not supported"));
                return TStatus::Error;
            }

            if (tableInfo.Meta->IsDynamic) {
                ctx.AddError(TIssue(ctx.GetPosition(dropTable.Table().Pos()), TStringBuilder() <<
                    "Drop of dynamic table " << tableInfo.Name.Quote() << " is not supported"));
                return TStatus::Error;
            }

            if (auto commitEpoch = tableInfo.CommitEpoch) {
                TYtTableDescription& nextDescription = State_->TablesData->GetOrAddTable(
                    TString{dropTable.DataSink().Cluster().Value()},
                    tableInfo.Name,
                    commitEpoch
                );

                TYtTableMetaInfo::TPtr nextMetadata = nextDescription.Meta;
                if (!nextMetadata) {
                    nextDescription.RowType = nullptr;
                    nextDescription.RawRowType = nullptr;

                    nextMetadata = nextDescription.Meta = MakeIntrusive<TYtTableMetaInfo>();
                    nextMetadata->DoesExist = false;
                }
                else if (nextMetadata->DoesExist) {
                    ctx.AddError(TIssue(ctx.GetPosition(dropTable.Table().Pos()), TStringBuilder() <<
                        "Table " << tableInfo.Name << " is modified and dropped in the same transaction"));
                    return TStatus::Error;
                }
            }
        }

        input->SetTypeAnn(dropTable.World().Ref().GetTypeAnn());
        return TStatus::Ok;
    }

    TStatus HandleCommit(TExprBase input, TExprContext& ctx) {
        auto commit = input.Cast<TCoCommit>();
        auto settings = NCommon::ParseCommitSettings(commit, ctx);

        if (settings.Epoch) {
            ui32 epoch = 0;
            if (!TryFromString(settings.Epoch.Cast().Value(), epoch)) {
                ctx.AddError(TIssue(ctx.GetPosition(commit.Pos()), TStringBuilder() << "Bad commit epoch: "
                    << settings.Epoch.Cast().Value()));
                return TStatus::Error;
            }
        }

        if (!settings.EnsureModeEmpty(ctx)) {
            return TStatus::Error;
        }
        if (!settings.EnsureOtherEmpty(ctx)) {
            return TStatus::Error;
        }

        input.Ptr()->SetTypeAnn(commit.World().Ref().GetTypeAnn());
        return TStatus::Ok;
    }

    TStatus HandlePublish(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        if (!EnsureArgsCount(*input, 5, ctx)) {
            return TStatus::Error;
        }

        if (!ValidateOpBase(input, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureTupleMinSize(*input->Child(TYtPublish::idx_Input), 1, ctx)) {
            return TStatus::Error;
        }

        const TTypeAnnotationNode* itemType = nullptr;
        for (auto& child: input->Child(TYtPublish::idx_Input)->Children()) {
            if (!EnsureCallable(*child, ctx)) {
                return TStatus::Error;
            }

            if (!child->IsCallable(TYtOutput::CallableName())) {
                ctx.AddError(TIssue(ctx.GetPosition(child->Pos()), TStringBuilder()
                    << "Expected " << TYtOutput::CallableName() << " callable, but got "
                    << child->Content()));
                return TStatus::Error;
            }

            const TTypeAnnotationNode* childItemType = nullptr;
            if (!GetSequenceItemType(*child, childItemType, ctx)) {
                return TStatus::Error;
            }
            if (nullptr == itemType) {
                itemType = childItemType;
            } else if (!IsSameAnnotation(*itemType, *childItemType)) {
                ctx.AddError(TIssue(ctx.GetPosition(child->Pos()), TStringBuilder()
                    << TYtPublish::CallableName() << " inputs have different item types: "
                    << GetTypeDiff(*itemType, *childItemType)));
                return TStatus::Error;
            }
        }

        auto table = input->ChildPtr(TYtPublish::idx_Publish);
        if (!EnsureCallable(*table, ctx)) {
            return TStatus::Error;
        }

        if (!table->IsCallable(TYtTable::CallableName())) {
            ctx.AddError(TIssue(ctx.GetPosition(table->Pos()), TStringBuilder() << "Expected " << TYtTable::CallableName()
                << " callable, but got " << table->Content()));
            return TStatus::Error;
        }

        auto settings = input->Child(TYtPublish::idx_Settings);
        if (!EnsureTuple(*settings, ctx)) {
            return TStatus::Error;
        }

        if (!ValidateSettings(*settings, EYtSettingType::Mode
            | EYtSettingType::Initial
            | EYtSettingType::CompressionCodec
            | EYtSettingType::ErasureCodec
            | EYtSettingType::ReplicationFactor
            | EYtSettingType::UserAttrs
            | EYtSettingType::Media
            | EYtSettingType::PrimaryMedium
            | EYtSettingType::Expiration
            | EYtSettingType::MonotonicKeys
            | EYtSettingType::MutationId
            | EYtSettingType::ColumnGroups
            | EYtSettingType::SecurityTags
            , ctx))
        {
            return TStatus::Error;
        }

        if (!NYql::HasSetting(*table->Child(TYtTable::idx_Settings), EYtSettingType::Anonymous)
            || !table->Child(TYtTable::idx_Name)->Content().StartsWith("tmp/"))
        {
            auto publish = TYtPublish(input);

            TVector<TYqlRowSpecInfo::TPtr> contentRowSpecs;
            for (auto out: publish.Input()) {
                contentRowSpecs.push_back(MakeIntrusive<TYqlRowSpecInfo>(GetOutTable(out).Cast<TYtOutTable>().RowSpec()));
                if (IsUnorderedOutput(out)) {
                    contentRowSpecs.back()->ClearSortness(ctx);
                }
            }
            TExprNode::TPtr content; // Don't try to convert content
            auto status = ValidateTableWrite(ctx.GetPosition(input->Pos()), table, content, itemType, contentRowSpecs, publish.DataSink().Cluster().StringValue(), *settings, ctx);
            if (TStatus::Ok != status.Level) {
                return status;
            }

            TExprNode::TPtr newTable;
            status = UpdateTableMeta(table, newTable, State_->TablesData, false, State_->Types->UseTableMetaFromGraph, ctx);
            if (TStatus::Ok != status.Level) {
                if (TStatus::Error != status.Level && newTable != table) {
                    output = ctx.ChangeChild(*input, TYtPublish::idx_Publish, std::move(newTable));
                }
                return status.Combine(TStatus::Repeat);
            }
        }

        input->SetTypeAnn(input->Child(TYtPublish::idx_World)->GetTypeAnn());
        return TStatus::Ok;
    }

    TStatus HandleEquiJoin(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        if (!EnsureMinArgsCount(*input, 8, ctx)) {
            return TStatus::Error;
        }

        auto status = ValidateAndUpdateTransientOpBase(input, output, ctx, true,
            EYtSettingType::KeyFilter | EYtSettingType::KeyFilter2 | EYtSettingType::Take | EYtSettingType::Skip | EYtSettingType::JoinLabel | EYtSettingType::StatColumns | EYtSettingType::SysColumns);
        if (status.Level != TStatus::Ok) {
            return status;
        }

        // Only one output
        if (!EnsureTupleSize(*input->Child(TYtEquiJoin::idx_Output), 1, ctx)) {
            return TStatus::Error;
        }

        if (!ValidateSettings(*input->Child(TYtEquiJoin::idx_Settings), EYtSettingType::Limit, ctx)) {
            return TStatus::Error;
        }

        const auto inputsCount = input->ChildPtr(TYtEquiJoin::idx_Input)->ChildrenSize();
        if (!EnsureArgsCount(*input, 7U + inputsCount, ctx)) {
            return TStatus::Error;
        }

        TJoinLabels labels;
        for (ui32 i = 0; i < inputsCount; ++i) {
            const TYtSection section(input->ChildPtr(TYtEquiJoin::idx_Input)->ChildPtr(i));
            if (auto label = NYql::GetSetting(section.Settings().Ref(), EYtSettingType::JoinLabel)) {
                auto itemType = GetSequenceItemType(section, false, ctx);
                if (!itemType || !EnsurePersistableType(section.Pos(), *itemType, ctx)) {
                    return TStatus::Error;
                }

                if (auto& lambda = input->ChildRef(i + 7U); lambda->IsLambda()) {
                    if (!UpdateLambdaAllArgumentsTypes(lambda, {GetSequenceItemType(section, false, ctx)}, ctx))
                        return TStatus::Error;

                    if (!lambda->GetTypeAnn()) {
                        return TStatus::Repeat;
                    }

                    itemType = GetSequenceItemType(TExprBase(lambda), false, ctx);
                    if (!itemType || !EnsurePersistableType(lambda->Pos(), *itemType, ctx)) {
                        return TStatus::Error;
                    }
                } else if (!lambda->IsCallable("Void")) {
                    ctx.AddError(TIssue(ctx.GetPosition(lambda->Pos()), TStringBuilder()
                        << "Premap node should be either lambda or Void, got: " << lambda->Content()));
                    return TStatus::Error;
                }

                if (auto err = labels.Add(ctx, *label->Child(1), itemType->Cast<TStructExprType>())) {
                    ctx.AddError(*err);
                    return TStatus::Error;
                }
            }
            else {
                ctx.AddError(TIssue(ctx.GetPosition(section.Pos()), TStringBuilder()
                    << "Setting \"" << EYtSettingType::JoinLabel
                    << "\" is required in " << input->Content() << " section"));
                return TStatus::Error;
            }
        }

        TJoinOptions joinOptions;
        status = ValidateEquiJoinOptions(input->Pos(), *input->Child(TYtEquiJoin::idx_JoinOptions), joinOptions, ctx);
        if (status != TStatus::Ok) {
            return status;
        }

        const TStructExprType* resultType = nullptr;
        status = EquiJoinAnnotation(input->Pos(), resultType, labels,
            *input->Child(TYtEquiJoin::idx_Joins), joinOptions, ctx);
        if (status != TStatus::Ok) {
            return status;
        }

        const TYtEquiJoin equiJoin(input);
        if (!ValidateOutputType(resultType, equiJoin.Pos(), equiJoin.Output(), ctx)) {
            return TStatus::Error;
        }

        input->SetTypeAnn(MakeOutputOperationType(equiJoin, ctx));
        return TStatus::Ok;
    }

    TStatus HandleStatOutTable(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        Y_UNUSED(output);

        if (!EnsureMinArgsCount(*input, 3, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(TYtStatOutTable::idx_Name), ctx)) {
            return TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(TYtStatOutTable::idx_Scale), ctx)) {
            return TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(TYtStatOutTable::idx_Cluster), ctx)) {
            return TStatus::Error;
        }

        input->SetTypeAnn(ctx.MakeType<TUnitExprType>());

        return TStatus::Ok;
    }

    TStatus HandleStatOut(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        Y_UNUSED(output);

        if (!EnsureArgsCount(*input, 6, ctx)) {
            return TStatus::Error;
        }

        if (!ValidateOpBase(input, ctx)) {
            return TStatus::Error;
        }

        auto inputChild = input->Child(TYtStatOut::idx_Input);
        if (!TMaybeNode<TYtOutput>(inputChild)) {
            ctx.AddError(TIssue(ctx.GetPosition(inputChild->Pos()), TStringBuilder()
                << "Unexpected input: " << inputChild->Content()));
            return TStatus::Error;
        }

        auto table = input->Child(TYtStatOut::idx_Table);
        if (!table->IsCallable(TYtStatOutTable::CallableName())) {
            ctx.AddError(TIssue(ctx.GetPosition(table->Pos()), TStringBuilder() <<
                "Unexpected callable: " << table->Content()
            ));
            return TStatus::Error;
        }

        auto replaceMask = input->Child(TYtStatOut::idx_ReplaceMask);
        if (!EnsureTuple(*replaceMask, ctx)) {
            return TStatus::Error;
        }

        for (const auto& child: replaceMask->Children()) {
            if (!EnsureAtom(*child, ctx)) {
                return TStatus::Error;
            }
        }

        auto settings = input->Child(TYtStatOut::idx_Settings);
        if (!EnsureTuple(*settings, ctx)) {
            return TStatus::Error;
        }

        for (auto& child: settings->Children()) {
            if (!EnsureTupleMinSize(*child, 1, ctx)) {
                return TStatus::Error;
            }

            if (!EnsureAtom(*child->Child(0), ctx)) {
                return TStatus::Error;
            }
        }

        input->SetTypeAnn(input->Child(TYtStatOut::idx_World)->GetTypeAnn());

        return TStatus::Ok;
    }

    TStatus HandleYtDqProcessWrite(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!ValidateOutputOpBase(input, ctx, false)) {
            return TStatus::Error;
        }

        if (!EnsureMinMaxArgsCount(*input, 4, 5, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureCallable(*input->Child(3), ctx)) {
            return TStatus::Error;
        }

        if (input->ChildrenSize() > 4 && !EnsureTupleOfAtoms(input->Tail(), ctx)) {
            return TStatus::Error;
        }

        auto processWrite = TYtDqProcessWrite(input);

        input->SetTypeAnn(MakeOutputOperationType(processWrite, ctx));
        return TStatus::Ok;
    }

    template <bool Wide>
    TStatus HandleDqWrite(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx)) {
            return TStatus::Error;
        }

        if constexpr (Wide) {
            if (!EnsureWideFlowType(input->Head(), ctx)) {
                return TStatus::Error;
            }
        } else {
            if (!EnsureNewSeqType<false, false, true>(input->Head(), ctx)) {
                return TStatus::Error;
            }
        }

        if (!EnsureTuple(input->Tail(), ctx)) {
            return TStatus::Error;
        }

        for (auto& setting: input->Tail().Children()) {
            if (!EnsureTupleMinSize(*setting, 1, ctx)) {
                return TStatus::Error;
            }

            if (!EnsureAtom(*setting->Child(0), ctx)) {
                return TStatus::Error;
            }
        }

        input->SetTypeAnn(MakeSequenceType(input->Head().GetTypeAnn()->GetKind(), *ctx.MakeType<TVoidExprType>(), ctx));
        return TStatus::Ok;
    }

    TStatus HandleTryFirst(TExprBase input, TExprContext& ctx) {
        if (!EnsureArgsCount(input.Ref(), 2, ctx)) {
            return TStatus::Error;
        }

        if (!TYtOutputOpBase::Match(input.Ref().Child(TYtTryFirst::idx_First))) {
            ctx.AddError(TIssue(ctx.GetPosition(input.Ref().Child(TYtTryFirst::idx_First)->Pos()), TStringBuilder() << "Expect YT operation, but got: "
                << input.Ref().Child(TYtTryFirst::idx_First)->Content()));
            return TStatus::Error;
        }

        if (!TYtOutputOpBase::Match(input.Ref().Child(TYtTryFirst::idx_Second))) {
            ctx.AddError(TIssue(ctx.GetPosition(input.Ref().Child(TYtTryFirst::idx_Second)->Pos()), TStringBuilder() << "Expect YT operation, but got: "
                << input.Ref().Child(TYtTryFirst::idx_Second)->Content()));
            return TStatus::Error;
        }

        if (!IsSameAnnotation(*input.Ref().Child(TYtTryFirst::idx_First)->GetTypeAnn(), *input.Ref().Child(TYtTryFirst::idx_Second)->GetTypeAnn())) {
            ctx.AddError(TIssue(ctx.GetPosition(input.Pos()), TStringBuilder() << "Both argumensts must be same type."));
            return TStatus::Error;
        }

        input.Ptr()->SetTypeAnn(input.Ref().Head().GetTypeAnn());
        return TStatus::Ok;
    }
private:
    const TYtState::TPtr State_;
};

}

THolder<TVisitorTransformerBase> CreateYtDataSinkTypeAnnotationTransformer(TYtState::TPtr state) {
    return THolder(new TYtDataSinkTypeAnnotationTransformer(state));
}

}
