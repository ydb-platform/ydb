#include "yql_yt_provider_impl.h"
#include "yql_yt_op_settings.h"
#include "yql_yt_helpers.h"

#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_table.h>
#include <ydb/library/yql/providers/yt/common/yql_names.h>
#include <ydb/library/yql/providers/yt/common/yql_configuration.h>
#include <ydb/library/yql/providers/yt/lib/mkql_helpers/mkql_helpers.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/transform/yql_visit.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/yql_type_helpers.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <util/string/builder.h>
#include <util/generic/xrange.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>

namespace NYql {

using namespace NNodes;

namespace {

class TYtDataSourceTypeAnnotationTransformer : public TVisitorTransformerBase {
public:
    TYtDataSourceTypeAnnotationTransformer(TYtState::TPtr state)
        : TVisitorTransformerBase(true)
        , State_(state)
    {
        AddHandler({TEpoch::CallableName()}, Hndl(&TYtDataSourceTypeAnnotationTransformer::HandleAux<TEpochInfo>));
        AddHandler({TYtMeta::CallableName()}, Hndl(&TYtDataSourceTypeAnnotationTransformer::HandleAux<TYtTableMetaInfo>));
        AddHandler({TYtStat::CallableName()}, Hndl(&TYtDataSourceTypeAnnotationTransformer::HandleAux<TYtTableStatInfo>));
        AddHandler({TYqlRowSpec::CallableName()}, Hndl(&TYtDataSourceTypeAnnotationTransformer::HandleRowSpec));
        AddHandler({TYtTable::CallableName()}, Hndl(&TYtDataSourceTypeAnnotationTransformer::HandleTable));
        AddHandler({TYtRow::CallableName()}, Hndl(&TYtDataSourceTypeAnnotationTransformer::HandleRow));
        AddHandler({TYtRowRange::CallableName()}, Hndl(&TYtDataSourceTypeAnnotationTransformer::HandleRowRange));
        AddHandler({TYtKeyExact::CallableName()}, Hndl(&TYtDataSourceTypeAnnotationTransformer::HandleKeyExact));
        AddHandler({TYtKeyRange::CallableName()}, Hndl(&TYtDataSourceTypeAnnotationTransformer::HandleKeyRange));
        AddHandler({TYtPath::CallableName()}, Hndl(&TYtDataSourceTypeAnnotationTransformer::HandlePath));
        AddHandler({TYtSection::CallableName()}, Hndl(&TYtDataSourceTypeAnnotationTransformer::HandleSection));
        AddHandler({TYtReadTable::CallableName()}, Hndl(&TYtDataSourceTypeAnnotationTransformer::HandleReadTable));
        AddHandler({TYtReadTableScheme::CallableName()}, Hndl(&TYtDataSourceTypeAnnotationTransformer::HandleReadTableScheme));
        AddHandler({TYtTableContent::CallableName()}, Hndl(&TYtDataSourceTypeAnnotationTransformer::HandleTableContent));
        AddHandler({TYtLength::CallableName()}, Hndl(&TYtDataSourceTypeAnnotationTransformer::HandleLength));
        AddHandler({TCoConfigure::CallableName()}, Hndl(&TYtDataSourceTypeAnnotationTransformer::HandleConfigure));
        AddHandler({TYtConfigure::CallableName()}, Hndl(&TYtDataSourceTypeAnnotationTransformer::HandleYtConfigure));
        AddHandler({TYtTablePath::CallableName()}, Hndl(&TYtDataSourceTypeAnnotationTransformer::HandleTablePath));
        AddHandler({TYtTableRecord::CallableName()}, Hndl(&TYtDataSourceTypeAnnotationTransformer::HandleTableRecord));
        AddHandler({TYtRowNumber::CallableName()}, Hndl(&TYtDataSourceTypeAnnotationTransformer::HandleTableRecord));
        AddHandler({TYtTableIndex::CallableName()}, Hndl(&TYtDataSourceTypeAnnotationTransformer::HandleTableIndex));
        AddHandler({TYtIsKeySwitch::CallableName()}, Hndl(&TYtDataSourceTypeAnnotationTransformer::HandleIsKeySwitch));
        AddHandler({TYtTableName::CallableName()}, Hndl(&TYtDataSourceTypeAnnotationTransformer::HandleTableName));
    }

    TStatus HandleUnit(TExprBase input, TExprContext& ctx) {
        input.Ptr()->SetTypeAnn(ctx.MakeType<TUnitExprType>());
        return TStatus::Ok;
    }

    template <class TValidator>
    TStatus HandleAux(TExprBase input, TExprContext& ctx) {
        if (!TValidator::Validate(input.Ref(), ctx)) {
            return TStatus::Error;
        }
        input.Ptr()->SetTypeAnn(ctx.MakeType<TUnitExprType>());
        return TStatus::Ok;
    }

    TStatus HandleRowSpec(TExprBase input, TExprContext& ctx) {
        const TStructExprType* type = nullptr;
        TMaybe<TColumnOrder> columnOrder;
        if (!TYqlRowSpecInfo::Validate(input.Ref(), ctx, type, columnOrder)) {
            return TStatus::Error;
        }
        input.Ptr()->SetTypeAnn(type);
        if (columnOrder) {
            return State_->Types->SetColumnOrder(input.Ref(), *columnOrder, ctx);
        }
        return TStatus::Ok;
    }

    TStatus HandleTable(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        EYtSettingTypes accepted = EYtSettingType::InferScheme | EYtSettingType::ForceInferScheme
            | EYtSettingType::DoNotFailOnInvalidSchema | EYtSettingType::Anonymous
            | EYtSettingType::WithQB | EYtSettingType::View | EYtSettingType::UserSchema
            | EYtSettingType::UserColumns | EYtSettingType::IgnoreTypeV3;

        if (!TYtTableInfo::Validate(*input, accepted, ctx)) {
            return TStatus::Error;
        }

        if (!TYtTableInfo::HasSubstAnonymousLabel(TExprBase(input))) {
            auto status = UpdateTableMeta(input, output, State_->TablesData, false, State_->Types->UseTableMetaFromGraph, ctx);
            if (status.Level != TStatus::Ok) {
                return status;
            }
        }

        auto table = TYtTable(input);

        const TTypeAnnotationNode* itemType = nullptr;
        TMaybe<TColumnOrder> columnOrder;
        if (table.RowSpec().Maybe<TCoVoid>()) {
            columnOrder.ConstructInPlace();
            TVector<const TItemExprType*> items;
            for (auto& name : YAMR_FIELDS) {
                items.push_back(ctx.MakeType<TItemExprType>(name, ctx.MakeType<TDataExprType>(EDataSlot::String)));
                columnOrder->AddColumn(TString(name));
            }
            itemType = ctx.MakeType<TStructExprType>(items);
        } else {
            itemType = table.RowSpec().Ref().GetTypeAnn();
            columnOrder = State_->Types->LookupColumnOrder(table.RowSpec().Ref());
        }

        input->SetTypeAnn(ctx.MakeType<TListExprType>(itemType));
        if (columnOrder) {
            return State_->Types->SetColumnOrder(*input, *columnOrder, ctx);
        }
        return TStatus::Ok;
    }

    TStatus HandleRow(TExprBase input, TExprContext& ctx) {
        if (!EnsureArgsCount(input.Ref(), 1, ctx)) {
            return TStatus::Error;
        }

        if (!input.Ref().Child(TYtRow::idx_Index)->IsCallable(TCoUint64::CallableName())) {
            ctx.AddError(TIssue(ctx.GetPosition(input.Ref().Child(TYtRow::idx_Index)->Pos()), TStringBuilder() << "Expected " << TCoUint64::CallableName()));
            return TStatus::Error;
        }

        return HandleUnit(input, ctx);
    }

    TStatus HandleRowRange(TExprBase input, TExprContext& ctx) {
        if (!EnsureArgsCount(input.Ref(), 2, ctx)) {
            return TStatus::Error;
        }

        if (!input.Ref().Child(TYtRowRange::idx_Lower)->IsCallable(TCoVoid::CallableName())
            && !input.Ref().Child(TYtRowRange::idx_Lower)->IsCallable(TCoUint64::CallableName()))
        {
            ctx.AddError(TIssue(ctx.GetPosition(input.Ref().Child(TYtRowRange::idx_Lower)->Pos()), TStringBuilder()
                << "Expected " << TCoUint64::CallableName() << " or " << TCoVoid::CallableName()));
            return TStatus::Error;
        }

        if (!input.Ref().Child(TYtRowRange::idx_Upper)->IsCallable(TCoVoid::CallableName())
            && !input.Ref().Child(TYtRowRange::idx_Upper)->IsCallable(TCoUint64::CallableName()))
        {
            ctx.AddError(TIssue(ctx.GetPosition(input.Ref().Child(TYtRowRange::idx_Upper)->Pos()), TStringBuilder()
                << "Expected " << TCoUint64::CallableName() << " or " << TCoVoid::CallableName()));
            return TStatus::Error;
        }

        if (input.Ref().Child(TYtRowRange::idx_Lower)->IsCallable(TCoVoid::CallableName())
            && input.Ref().Child(TYtRowRange::idx_Upper)->IsCallable(TCoVoid::CallableName())) {
            ctx.AddError(TIssue(ctx.GetPosition(input.Pos()), TStringBuilder()
                << "Either Lower or Upper part of " << TYtRowRange::CallableName() << " should be specified"));
            return TStatus::Error;
        }

        return HandleUnit(input, ctx);
    }

    static bool ValidateKeyLiteral(TExprBase node, TExprContext& ctx) {
        if (node.Maybe<TCoNull>()) {
            return true;
        }

        bool isOptional;
        const TDataExprType* dataType;
        if (!EnsureDataOrOptionalOfData(node.Ref(), isOptional, dataType, ctx)) {
            return false;
        }

        if (isOptional) {
            if (node.Maybe<TCoNothing>()) {
                return true;
            }
            auto maybeJust = node.Maybe<TCoJust>();
            if (!maybeJust) {
                ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected Optional literal - Nothing or Just"));
                return false;
            }
            node = maybeJust.Cast().Input();
        }

        if (!node.Maybe<TCoDataCtor>()) {
            ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected one of Data value"));
            return false;
        }
        return true;
    }

    TStatus HandleKeyExact(TExprBase input, TExprContext& ctx) {
        if (!EnsureArgsCount(input.Ref(), 1, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureTupleMinSize(*input.Ref().Child(TYtKeyExact::idx_Key), 1, ctx)) {
            return TStatus::Error;
        }

        TExprNodeList keyValues = input.Ref().Child(TYtKeyExact::idx_Key)->ChildrenList();
        if (AnyOf(keyValues, [&ctx](const auto& child) { return !ValidateKeyLiteral(TExprBase(child), ctx); })) {
            return TStatus::Error;
        }

        return HandleUnit(input, ctx);
    }

    TStatus HandleKeyRange(TExprBase input, TExprContext& ctx) {
        if (!EnsureMinArgsCount(input.Ref(), 2, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureMaxArgsCount(input.Ref(), 3, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureTuple(*input.Ref().Child(TYtKeyRange::idx_Lower), ctx)) {
            return TStatus::Error;
        }

        const TExprNodeList& lowerValues = input.Ref().Child(TYtKeyRange::idx_Lower)->ChildrenList();
        if (AnyOf(lowerValues, [&ctx](const auto& child) { return !ValidateKeyLiteral(TExprBase(child), ctx); })) {
            return TStatus::Error;
        }

        if (!EnsureTuple(*input.Ref().Child(TYtKeyRange::idx_Upper), ctx)) {
            return TStatus::Error;
        }

        const TExprNodeList& upperValues = input.Ref().Child(TYtKeyRange::idx_Upper)->ChildrenList();
        if (AnyOf(upperValues, [&ctx](const auto& child) { return !ValidateKeyLiteral(TExprBase(child), ctx); })) {
            return TStatus::Error;
        }

        if (lowerValues.empty() && upperValues.empty()) {
            ctx.AddError(TIssue(ctx.GetPosition(input.Pos()), TStringBuilder()
                << "Either Lower or Upper part of " << TYtKeyRange::CallableName() << " should be specified"));
            return TStatus::Error;
        }

        for (size_t i : xrange(std::min(lowerValues.size(), upperValues.size()))) {
            const TTypeAnnotationNode* lowerType = lowerValues[i]->GetTypeAnn();
            const TTypeAnnotationNode* upperType = upperValues[i]->GetTypeAnn();
            if (lowerType->GetKind() == ETypeAnnotationKind::Null || upperType->GetKind() == ETypeAnnotationKind::Null) {
                continue;
            }

            if (!IsSameAnnotation(*lowerType, *upperType)) {
                ctx.AddError(TIssue(ctx.GetPosition(input.Pos()), TStringBuilder()
                    << "Lower/Upper type mismatch at index " << i << ": lower: " << *lowerType << ", upper: " << *upperType));
                return TStatus::Error;
            }
        }

        if (input.Ref().ChildrenSize() == 3) {
            if (!EnsureTupleMinSize(*input.Ref().Child(TYtKeyRange::idx_Flags), 1, ctx)) {
                return TStatus::Error;
            }
            for (auto& atom: input.Ref().Child(TYtKeyRange::idx_Flags)->Children()) {
                if (!EnsureAtom(*atom, ctx)) {
                    return TStatus::Error;
                }
                if (atom->Content() == TStringBuf("excludeLower")) {
                    if (lowerValues.empty()) {
                        ctx.AddError(TIssue(ctx.GetPosition(atom->Pos()), TStringBuilder()
                            << "Expected non-empty Lower part for 'excludeLower' setting"));
                        return TStatus::Error;
                    }
                } else if (atom->Content() == TStringBuf("includeUpper")) {
                    if (upperValues.empty()) {
                        ctx.AddError(TIssue(ctx.GetPosition(atom->Pos()), TStringBuilder()
                            << "Expected non-empty Upper part for 'includeUpper' setting"));
                        return TStatus::Error;
                    }
                } else if (atom->Content() != TStringBuf("useKeyBound")) {
                    ctx.AddError(TIssue(ctx.GetPosition(atom->Pos()), TStringBuilder()
                        << "Expected 'excludeLower', 'includeUpper' or 'useKeyBound', but got " << atom->Content()));
                    return TStatus::Error;
                }
            }
        }

        return HandleUnit(input, ctx);
    }

    TStatus HandlePath(TExprBase input, TExprContext& ctx) {
        if (!TYtPathInfo::Validate(input.Ref(), ctx)) {
            return TStatus::Error;
        }

        TYtPathInfo pathInfo(input);
        if (pathInfo.Table->Meta && !pathInfo.Table->Meta->DoesExist) {
            if (NYql::HasSetting(pathInfo.Table->Settings.Ref(), EYtSettingType::Anonymous)) {
                ctx.AddError(TIssue(ctx.GetPosition(input.Pos()), TStringBuilder()
                    << "Anonymous table " << pathInfo.Table->Name.Quote() << " must be materialized. Use COMMIT before reading from it."));
            }
            else {
                ctx.AddError(TIssue(ctx.GetPosition(input.Pos()), TStringBuilder()
                    << "Table " <<  pathInfo.Table->Name.Quote() << " does not exist").SetCode(TIssuesIds::YT_TABLE_NOT_FOUND, TSeverityIds::S_ERROR));
            }
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* itemType = nullptr;
        TExprNode::TPtr newFields;
        auto status = pathInfo.GetType(itemType, newFields, ctx, input.Pos());
        if (newFields) {
            input.Ptr()->ChildRef(TYtPath::idx_Columns) = newFields;
        }
        if (status.Level != TStatus::Ok) {
            return status;
        }

        input.Ptr()->SetTypeAnn(ctx.MakeType<TListExprType>(itemType));
        if (auto columnOrder = State_->Types->LookupColumnOrder(input.Ref().Head())) {
            if (pathInfo.Columns) {
                auto& renames = pathInfo.Columns->GetRenames();
                if (renames) {
                    TColumnOrder renamedOrder;
                    for (auto& [col, gen_col] : *columnOrder) {
                        if (auto renamed = renames->FindPtr(col)) {
                            renamedOrder.AddColumn(*renamed);
                        } else {
                            renamedOrder.AddColumn(col);
                        }
                    }
                    *columnOrder = renamedOrder;
                }
            }

            // sync with output type (add weak columns, etc.)
            TSet<TStringBuf> allColumns = GetColumnsOfStructOrSequenceOfStruct(*itemType);
            columnOrder->EraseIf([&](const TString& col) { return !allColumns.contains(col); });
            for (auto& [col, gen_col] : *columnOrder) {
                allColumns.erase(allColumns.find(gen_col));
            }
            for (auto& col : allColumns) {
                columnOrder->AddColumn(TString(col));
            }

            return State_->Types->SetColumnOrder(input.Ref(), *columnOrder, ctx);
        }

        return TStatus::Ok;
    }

    TStatus HandleSection(TExprBase input, TExprContext& ctx) {
        if (!EnsureArgsCount(input.Ref(), 2, ctx)) {
            return TStatus::Error;
        }
        if (!EnsureTupleMinSize(*input.Ref().Child(TYtSection::idx_Paths), 1, ctx)) {
            return TStatus::Error;
        }
        for (auto& child: input.Ref().Child(TYtSection::idx_Paths)->Children()) {
            if (!child->IsCallable(TYtPath::CallableName())) {
                ctx.AddError(TIssue(ctx.GetPosition(child->Pos()), TStringBuilder() << "Expected " << TYtPath::CallableName()));
                return TStatus::Error;
            }
        }
        if (!EnsureTuple(*input.Ref().Child(TYtSection::idx_Settings), ctx)) {
            return TStatus::Error;
        }
        EYtSettingTypes acceptedSettings = EYtSettingType::KeyFilter
            | EYtSettingType::KeyFilter2
            | EYtSettingType::Take
            | EYtSettingType::Skip
            | EYtSettingType::DirectRead
            | EYtSettingType::Sample
            | EYtSettingType::JoinLabel
            | EYtSettingType::Unordered
            | EYtSettingType::NonUnique
            | EYtSettingType::UserSchema
            | EYtSettingType::UserColumns
            | EYtSettingType::StatColumns
            | EYtSettingType::SysColumns;
        if (!ValidateSettings(*input.Ref().Child(TYtSection::idx_Settings), acceptedSettings, ctx)) {
            return TStatus::Error;
        }

        const TTypeAnnotationNode* tableType = nullptr;
        TMaybe<bool> yamrFormat;
        for (auto child: input.Cast<TYtSection>().Paths()) {
            if (tableType) {
                if (!IsSameAnnotation(*tableType, *child.Ref().GetTypeAnn())) {
                    ctx.AddError(TIssue(ctx.GetPosition(input.Pos()), TStringBuilder()
                        << "All section tables must have the same scheme. Found diff: "
                        << GetTypeDiff(*tableType, *child.Ref().GetTypeAnn())));
                    return TStatus::Error;
                }
            } else {
                tableType = child.Ref().GetTypeAnn();
            }
            if (yamrFormat) {
                if (*yamrFormat != child.Table().Maybe<TYtTable>().RowSpec().Maybe<TCoVoid>().IsValid()) {
                    ctx.AddError(TIssue(ctx.GetPosition(input.Pos()), TStringBuilder() << "Mixed Yamr/Yson input table formats"));
                    return TStatus::Error;
                }
            } else {
                yamrFormat = child.Table().Maybe<TYtTable>().RowSpec().Maybe<TCoVoid>().IsValid();
            }
        }

        const auto sysColumns = NYql::GetSettingAsColumnList(*input.Ref().Child(TYtSection::idx_Settings), EYtSettingType::SysColumns);
        if (!sysColumns.empty()) {
            if (AnyOf(sysColumns, [](const TString& col) { return col == "path" || col == "record"; })) {
                for (auto& child: input.Ref().Child(TYtSection::idx_Paths)->Children()) {
                    if (!TYtTable::Match(child->Child(TYtPath::idx_Table))) {
                        ctx.AddError(TIssue(ctx.GetPosition(child->Pos()),
                            TStringBuilder() << EYtSettingType::SysColumns << " setting cannot be used with operation results"));
                        return TStatus::Error;
                    }
                }
            }

            const auto structType = tableType->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
            auto columns = structType->GetItems();
            for (auto sys: sysColumns) {
                auto sysColName = TString(YqlSysColumnPrefix).append(sys);
                if (!structType->FindItem(sysColName)) {
                    try {
                        columns.push_back(ctx.MakeType<TItemExprType>(sysColName, ctx.MakeType<TDataExprType>(NUdf::GetDataSlot(GetSysColumnTypeId(sys)))));
                    } catch (const TYqlPanic&) {
                        ctx.AddError(TIssue(ctx.GetPosition(input.Ref().Child(TYtSection::idx_Settings)->Pos()),
                            TStringBuilder() << "Unsupported system column " << sys.Quote()));
                        return TStatus::Error;
                    }
                }
            }
            tableType = ctx.MakeType<TListExprType>(ctx.MakeType<TStructExprType>(columns));
        }

        auto keyFilters = GetAllSettingValues(*input.Ref().Child(TYtSection::idx_Settings), EYtSettingType::KeyFilter);
        if (keyFilters.size() > 0) {
            TVector<std::pair<TString, TYqlRowSpecInfo::TPtr>> rowSpecs;
            for (auto path: input.Cast<TYtSection>().Paths()) {
                TYtPathInfo pathInfo(path);
                rowSpecs.emplace_back(pathInfo.Table->Name, pathInfo.Table->RowSpec);
            }

            TIssueScopeGuard issueScope(ctx.IssueManager, [pos = ctx.GetPosition(input.Pos())]() {
                return MakeIntrusive<TIssue>(pos, TStringBuilder() << "Setting " << EYtSettingType::KeyFilter);
            });

            for (auto keyFilter: keyFilters) {
                if (keyFilter->ChildrenSize() > 0) {
                    TMaybe<size_t> tableIndex;
                    if (keyFilter->ChildrenSize() > 1) {
                        tableIndex = FromString<size_t>(keyFilter->Child(1)->Content());
                        if (tableIndex >= rowSpecs.size()) {
                            ctx.AddError(TIssue(ctx.GetPosition(keyFilter->Child(1)->Pos()), TStringBuilder()
                                << "Invalid table index value: " << *tableIndex));
                            return TStatus::Error;
                        }
                    }
                    auto andGrp = TCoNameValueTupleList(keyFilter->ChildPtr(0));
                    size_t memberIndex = 0;
                    for (auto keyPredicates: andGrp) {
                        auto validateMember = [&] (const TString& tableName, const TYqlRowSpecInfo::TPtr& rowSpec) {
                            if (!rowSpec || !rowSpec->IsSorted()) {
                                ctx.AddError(TIssue(ctx.GetPosition(input.Pos()), TStringBuilder()
                                    << "Setting " << EYtSettingType::KeyFilter << " cannot be used with unsorted table "
                                    << tableName));
                                return false;
                            }
                            if (memberIndex >= rowSpec->SortMembers.size()
                                || rowSpec->SortMembers[memberIndex] != keyPredicates.Name().Value())
                            {
                                ctx.AddError(TIssue(ctx.GetPosition(input.Pos()), TStringBuilder()
                                    << "Predicate column " << TString{keyPredicates.Name().Value()}.Quote() << " doesn't match "
                                    << tableName.Quote() << " table sort columns"));
                                return false;
                            }
                            if (!rowSpec->SortDirections[memberIndex]) {
                                ctx.AddError(TIssue(ctx.GetPosition(input.Pos()), TStringBuilder()
                                    << "Predicate column " << TString{keyPredicates.Name().Value()}.Quote()
                                    << " has descending sort order in "
                                    << tableName.Quote() << " table"));
                                return false;
                            }
                            return true;
                        };

                        if (tableIndex) {
                            auto& rowSpec = rowSpecs[*tableIndex];
                            if (!validateMember(rowSpec.first, rowSpec.second)) {
                                return TStatus::Error;
                            }
                        }
                        else {
                            for (auto& rowSpec: rowSpecs) {
                                if (!validateMember(rowSpec.first, rowSpec.second)) {
                                    return TStatus::Error;
                                }
                            }
                        }
                        for (auto cmp: keyPredicates.Value().Cast<TCoNameValueTupleList>()) {
                            TExprBase value = cmp.Value().Cast();
                            if (!IsNull(value.Ref())) {
                                bool isOptional = false;
                                const TDataExprType* valueType = nullptr;
                                if (!EnsureDataOrOptionalOfData(value.Ref(), isOptional, valueType, ctx)) {
                                    return TStatus::Error;
                                }

                                auto& rowSpec = rowSpecs[tableIndex.GetOrElse(0)];
                                const TDataExprType* columnType = nullptr;
                                if (!EnsureDataOrOptionalOfData(rowSpec.second->FromNode.Cast().Pos(), rowSpec.second->SortedByTypes[memberIndex], isOptional, columnType, ctx)) {
                                    return TStatus::Error;
                                }
                                if (valueType->GetSlot() != columnType->GetSlot()
                                    && !GetSuperType(valueType->GetSlot(), columnType->GetSlot()))
                                {
                                    ctx.AddError(TIssue(ctx.GetPosition(input.Pos()), TStringBuilder()
                                        << "Predicate " << TString{keyPredicates.Name().Value()}.Quote() << " value type "
                                        << *value.Ref().GetTypeAnn() << " is incompatible with "
                                        << *rowSpec.second->SortedByTypes[memberIndex] << " column type of "
                                        << rowSpec.first.Quote() << " table"));
                                    return TStatus::Error;
                                }
                            }
                        }
                        ++memberIndex;
                    }
                }
            }
        }

        keyFilters = GetAllSettingValues(*input.Ref().Child(TYtSection::idx_Settings), EYtSettingType::KeyFilter2);
        if (keyFilters.size() > 0) {
            TVector<std::pair<TString, TYqlRowSpecInfo::TPtr>> rowSpecs;
            for (auto path: input.Cast<TYtSection>().Paths()) {
                TYtPathInfo pathInfo(path);
                rowSpecs.emplace_back(pathInfo.Table->Name, pathInfo.Table->RowSpec);
            }

            TIssueScopeGuard issueScope(ctx.IssueManager, [pos = ctx.GetPosition(input.Pos())]() {
                return MakeIntrusive<TIssue>(pos, TStringBuilder() << "Setting " << EYtSettingType::KeyFilter2);
            });

            TSet<size_t> processedIndexes;
            for (auto keyFilter: keyFilters) {
                if (keyFilter->ChildrenSize() == 0) {
                    continue;
                }

                TMaybe<TVector<size_t>> indexes;
                if (keyFilter->ChildrenSize() == 3) {
                    indexes.ConstructInPlace();
                    for (auto& idxNode : keyFilter->Tail().ChildrenList()) {
                        YQL_ENSURE(idxNode->IsAtom());
                        indexes->push_back(FromString<size_t>(idxNode->Content()));
                    }
                }

                TVector<TStringBuf> usedKeys;
                if (auto usedKeysSetting = GetSetting(*keyFilter->Child(1), "usedKeys")) {
                    for (auto& key : usedKeysSetting->Tail().ChildrenList()) {
                        YQL_ENSURE(key->IsAtom());
                        usedKeys.push_back(key->Content());
                    }
                }

                auto& computeNode = keyFilter->Head();
                auto boundaryTypes = computeNode.GetTypeAnn()->Cast<TListExprType>()->GetItemType()
                    ->Cast<TTupleExprType>()->GetItems().front()->Cast<TTupleExprType>()->GetItems();

                YQL_ENSURE(boundaryTypes.size() > 1);
                // drop include/exclude flag
                boundaryTypes.resize(boundaryTypes.size() - 1);

                for (size_t i = 0; i < (indexes ? indexes->size() : rowSpecs.size()); ++i) {
                    size_t idx = indexes ? (*indexes)[i] : i;
                    if (idx >= rowSpecs.size()) {
                        ctx.AddError(TIssue(ctx.GetPosition(keyFilter->Pos()), TStringBuilder()
                            << "Invalid table index " << idx << ": got only " << rowSpecs.size() << " input tables"));
                        return TStatus::Error;
                    }
                    if (!processedIndexes.insert(idx).second) {
                        ctx.AddError(TIssue(ctx.GetPosition(input.Pos()), TStringBuilder()
                            << "Duplicate table index " << idx));
                        return TStatus::Error;
                    }

                    const auto& nameAndSpec = rowSpecs[idx];
                    if (!nameAndSpec.second || !nameAndSpec.second->IsSorted()) {
                        ctx.AddError(TIssue(ctx.GetPosition(input.Pos()), TStringBuilder()
                            << "Setting " << EYtSettingType::KeyFilter2 << " cannot be used with unsorted table "
                            << nameAndSpec.first.Quote()));
                        return TStatus::Error;
                    }

                    if (nameAndSpec.second->SortedByTypes.size() < boundaryTypes.size()) {
                        ctx.AddError(TIssue(ctx.GetPosition(input.Pos()), TStringBuilder()
                            << "KeyFilter type size mismatch for index " << idx));
                        return TStatus::Error;
                    }

                    if (nameAndSpec.second->SortedBy.size() < usedKeys.size()) {
                        ctx.AddError(TIssue(ctx.GetPosition(input.Pos()), TStringBuilder()
                            << "KeyFilter used keys size mismatch for index " << idx));
                        return TStatus::Error;
                    }

                    YQL_ENSURE(usedKeys.size() <= boundaryTypes.size());

                    for (size_t j = 0; j < boundaryTypes.size(); ++j) {
                        auto specType = nameAndSpec.second->SortedByTypes[j];
                        auto boundaryType = boundaryTypes[j]->Cast<TOptionalExprType>()->GetItemType();
                        if (!IsSameAnnotation(*specType, *boundaryType)) {
                            ctx.AddError(TIssue(ctx.GetPosition(input.Pos()), TStringBuilder()
                                << "KeyFilter type mismatch for index " << idx << ", table name "
                                << nameAndSpec.first.Quote() << ", types: " << *specType
                                << " vs " << *boundaryType));
                            return TStatus::Error;
                        }

                        if (j < usedKeys.size()) {
                            auto specName = nameAndSpec.second->SortedBy[j];
                            auto usedKey = usedKeys[j];
                            if (specName != usedKey) {
                                ctx.AddError(TIssue(ctx.GetPosition(input.Pos()), TStringBuilder()
                                    << "KeyFilter key column name mismatch for index " << idx << ", table name "
                                    << nameAndSpec.first.Quote() << ", names: " << specName << " vs " << usedKey));
                                return TStatus::Error;
                            }
                        }
                    }
                }
            }
        }

        input.Ptr()->SetTypeAnn(tableType);

        auto paths = input.Ref().Child(TYtSection::idx_Paths)->ChildrenList();
        YQL_ENSURE(!paths.empty());
        auto common = State_->Types->LookupColumnOrder(*paths.front());
        if (!common) {
            return TStatus::Ok;
        }

        for (ui32 i = 1; i < paths.size(); ++i) {
            auto current = State_->Types->LookupColumnOrder(*paths[i]);
            if (!current || *common != *current) {
                return TStatus::Ok;
            }
        }

        // add system columns
        auto extraColumns = sysColumns;
        for (auto& sys: extraColumns) {
            sys = TString(YqlSysColumnPrefix).append(sys);
        }
        Sort(extraColumns);
        for (auto &e: extraColumns) {
            common->AddColumn(e);
        }
        return State_->Types->SetColumnOrder(input.Ref(), *common,  ctx);
    }

    TStatus HandleReadTable(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        if (!EnsureArgsCount(*input, 3, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureWorldType(*input->Child(TYtReadTable::idx_World), ctx)) {
            return TStatus::Error;
        }

        if (!EnsureSpecificDataSource(*input->Child(TYtReadTable::idx_DataSource), YtProviderName, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureTupleMinSize(*input->Child(TYtReadTable::idx_Input), 1, ctx)) {
            return TStatus::Error;
        }

        for (auto& section: input->Child(TYtReadTable::idx_Input)->Children()) {
            if (!section->IsCallable(TYtSection::CallableName())) {
                ctx.AddError(TIssue(ctx.GetPosition(section->Pos()), TStringBuilder() << "Expected " << TYtSection::CallableName()));
                return TStatus::Error;
            }
        }

        auto cluster = TString{TYtDSource(input->ChildPtr(TYtReadTable::idx_DataSource)).Cluster().Value()};
        TMaybe<bool> yamrFormat;
        TMaybe<TSampleParams> sampling;
        for (size_t i = 0; i < input->Child(TYtReadTable::idx_Input)->ChildrenSize(); ++i) {
            auto section = TYtSection(input->Child(TYtReadTable::idx_Input)->ChildPtr(i));
            if (NYql::HasSetting(section.Settings().Ref(), EYtSettingType::JoinLabel)) {
                ctx.AddError(TIssue(ctx.GetPosition(section.Pos()), TStringBuilder()
                    << "Setting \"" << EYtSettingType::JoinLabel << "\" is not allowed in " << TYtReadTable::CallableName()));
                return TStatus::Error;
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
                        if (!NYql::HasSetting(table.Settings().Ref(), EYtSettingType::Anonymous) || !tableName.StartsWith("tmp/")) {
                            const TYtTableDescription& tableDesc = State_->TablesData->GetTable(cluster,
                                TString{tableName},
                                TEpochInfo::Parse(table.Epoch().Ref()));

                            if (!tableDesc.Validate(ctx.GetPosition(table.Pos()), cluster, tableName,
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

        auto readInput = input->ChildPtr(TYtReadTable::idx_Input);
        auto newInput = ValidateAndUpdateTablesMeta(readInput, cluster, State_->TablesData, State_->Types->UseTableMetaFromGraph, ctx);
        if (!newInput) {
            return TStatus::Error;
        }
        else if (newInput != readInput) {
            output = ctx.ChangeChild(*input, TYtReadTable::idx_Input, std::move(newInput));
            return TStatus::Repeat;
        }

        TTypeAnnotationNode::TListType items;
        for (auto section: TYtSectionList(input->ChildPtr(TYtReadTable::idx_Input))) {
            items.push_back(section.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType());
        }
        auto itemType = items.size() == 1
            ? items.front()
            : ctx.MakeType<TVariantExprType>(ctx.MakeType<TTupleExprType>(items));

        input->SetTypeAnn(ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
            input->Child(TYtReadTable::idx_World)->GetTypeAnn(),
            ctx.MakeType<TListExprType>(itemType)
        }));

        if (items.size() == 1) {
            if (auto columnOrder = State_->Types->LookupColumnOrder(input->Child(TYtReadTable::idx_Input)->Head())) {
                return State_->Types->SetColumnOrder(*input, *columnOrder, ctx);
            }
        }
        return TStatus::Ok;
    }

    TStatus HandleReadTableScheme(const TExprNode::TPtr& input, TExprNode::TPtr& /*output*/, TExprContext& ctx) {
        if (!EnsureArgsCount(*input, 4, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureWorldType(*input->Child(TYtReadTableScheme::idx_World), ctx)) {
            return TStatus::Error;
        }

        if (!EnsureSpecificDataSource(*input->Child(TYtReadTableScheme::idx_DataSource), YtProviderName, ctx)) {
            return TStatus::Error;
        }

        if (!input->Child(TYtReadTableScheme::idx_Table)->IsCallable(TYtTable::CallableName())) {
            ctx.AddError(TIssue(ctx.GetPosition(input->Child(TYtReadTableScheme::idx_Table)->Pos()), TStringBuilder()
                << "Expected " << TYtTable::CallableName()));
            return TStatus::Error;
        }

        if (!EnsureType(*input->Child(TYtReadTableScheme::idx_Type), ctx)) {
            return TStatus::Error;
        }

        auto tableType = input->Child(TYtReadTableScheme::idx_Type)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (!EnsureListType(input->Child(TYtReadTableScheme::idx_Type)->Pos(), *tableType, ctx)) {
            return TStatus::Error;
        }

        auto rowType = tableType->Cast<TListExprType>()->GetItemType();
        if (!EnsureStructType(input->Child(TYtReadTableScheme::idx_Type)->Pos(), *rowType, ctx)) {
            return TStatus::Error;
        }

        auto readScheme = TYtReadTableScheme(input);
        auto cluster = TString{readScheme.DataSource().Cluster().Value()};
        auto tableName = TString{readScheme.Table().Name().Value()};
        auto view = NYql::GetSetting(readScheme.Table().Settings().Ref(), EYtSettingType::View);
        TString viewName = view ? TString{view->Child(1)->Content()} : TString();

        TYtTableDescription& tableDesc = State_->TablesData->GetOrAddTable(cluster, tableName,
            TEpochInfo::Parse(readScheme.Table().Epoch().Ref()));

        // update RowType in description
        if (!viewName.empty()) {
            tableDesc.Views[viewName].RowType = rowType;
        } else if (tableDesc.View.Defined()) {
            tableDesc.View->RowType = rowType;
        } else {
            tableDesc.RowType = rowType;
        }

        input->SetTypeAnn(ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
            input->Child(TYtReadTableScheme::idx_World)->GetTypeAnn(),
            ctx.MakeType<TDataExprType>(EDataSlot::Yson)
        }));
        return TStatus::Ok;
    }

    TStatus HandleTableContent(TExprBase input, TExprContext& ctx) {
        if (!EnsureArgsCount(input.Ref(), 2, ctx)) {
            return TStatus::Error;
        }

        const auto tableContent = input.Cast<TYtTableContent>();

        if (!tableContent.Input().Ref().IsCallable(TYtReadTable::CallableName())
            && !tableContent.Input().Ref().IsCallable(TYtOutput::CallableName())) {
            ctx.AddError(TIssue(ctx.GetPosition(tableContent.Input().Pos()), TStringBuilder()
                << "Expected " << TYtReadTable::CallableName() << " or " << TYtOutput::CallableName()));
            return TStatus::Error;
        }

        if (!EnsureTuple(tableContent.Settings().MutableRef(), ctx)) {
            return TStatus::Error;
        }

        if (!ValidateSettings(tableContent.Settings().Ref(), EYtSettingType::MemUsage | EYtSettingType::ItemsCount | EYtSettingType::RowFactor | EYtSettingType::Split, ctx)) {
            return TStatus::Error;
        }

        input.Ptr()->SetTypeAnn(tableContent.Input().Maybe<TYtOutput>()
            ? tableContent.Input().Ref().GetTypeAnn()
            : tableContent.Input().Ref().GetTypeAnn()->Cast<TTupleExprType>()->GetItems().back());

        if (auto columnOrder = State_->Types->LookupColumnOrder(tableContent.Input().Ref())) {
            return State_->Types->SetColumnOrder(input.Ref(), *columnOrder, ctx);
        }
        return TStatus::Ok;
    }

    TStatus HandleLength(const TExprNode::TPtr& input, TExprNode::TPtr& /*output*/, TExprContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx)) {
            return TStatus::Error;
        }

        auto lenInput = input->Child(TYtLength::idx_Input);
        if (lenInput->IsCallable(TYtReadTable::CallableName())) {
            if (TYtReadTable(lenInput).Input().Size() != 1) {
                ctx.AddError(TIssue(ctx.GetPosition(lenInput->Pos()), TStringBuilder()
                    << "Unsupported " << TYtReadTable::CallableName() << " with multiple sections"));
                return TStatus::Error;
            }
        }
        else if (!lenInput->IsCallable(TYtOutput::CallableName())) {
            ctx.AddError(TIssue(ctx.GetPosition(lenInput->Pos()), TStringBuilder()
                << "Expected " << TYtReadTable::CallableName() << " or " << TYtOutput::CallableName()));
            return TStatus::Error;
        }

        input->SetTypeAnn(ctx.MakeType<TDataExprType>(EDataSlot::Uint64));;
        return TStatus::Ok;
    }

    TStatus HandleConfigure(TExprBase input, TExprContext& ctx) {
        if (!EnsureMinArgsCount(input.Ref(), 2, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureWorldType(*input.Ptr()->Child(TCoConfigure::idx_World), ctx)) {
            return TStatus::Error;
        }

        if (!EnsureSpecificDataSource(*input.Ptr()->Child(TCoConfigure::idx_DataSource), YtProviderName, ctx)) {
            return TStatus::Error;
        }

        input.Ptr()->SetTypeAnn(input.Ref().Child(TCoConfigure::idx_World)->GetTypeAnn());
        return TStatus::Ok;
    }

    TStatus HandleYtConfigure(TExprBase input, TExprContext& ctx) {
        if (!EnsureMinArgsCount(input.Ref(), 2, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureWorldType(*input.Ptr()->Child(TYtConfigure::idx_World), ctx)) {
            return TStatus::Error;
        }

        if (!EnsureSpecificDataSource(*input.Ptr()->Child(TYtConfigure::idx_DataSource), YtProviderName, ctx)) {
            return TStatus::Error;
        }

        input.Ptr()->SetTypeAnn(input.Ref().Child(TYtConfigure::idx_World)->GetTypeAnn());
        return TStatus::Ok;
    }

    TStatus HandleTablePath(TExprBase input, TExprContext& ctx) {
        if (State_->Configuration->UseSystemColumns.Get().GetOrElse(DEFAULT_USE_SYS_COLUMNS)) {
            ctx.AddError(TIssue(ctx.GetPosition(input.Pos()),
                TStringBuilder() << "Unsupported callable " << input.Ref().Content()));
            return TStatus::Error;
        }

        if (!EnsureArgsCount(input.Ref(), 1, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureDependsOn(*input.Ptr()->Child(TYtTablePath::idx_DependsOn), ctx)) {
            return TStatus::Error;
        }

        input.Ptr()->SetTypeAnn(ctx.MakeType<TDataExprType>(EDataSlot::String));
        return TStatus::Ok;
    }

    TStatus HandleTableRecord(TExprBase input, TExprContext& ctx) {
        if (State_->Configuration->UseSystemColumns.Get().GetOrElse(DEFAULT_USE_SYS_COLUMNS)) {
            ctx.AddError(TIssue(ctx.GetPosition(input.Pos()),
                TStringBuilder() << "Unsupported callable " << input.Ref().Content()));
            return TStatus::Error;
        }

        if (!EnsureArgsCount(input.Ref(), 1, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureDependsOn(*input.Ptr()->Child(TYtTablePropBase::idx_DependsOn), ctx)) {
            return TStatus::Error;
        }

        input.Ptr()->SetTypeAnn(ctx.MakeType<TDataExprType>(EDataSlot::Uint64));
        return TStatus::Ok;
    }

    TStatus HandleTableIndex(TExprBase input, TExprContext& ctx) {
        if (State_->Configuration->UseSystemColumns.Get().GetOrElse(DEFAULT_USE_SYS_COLUMNS)) {
            ctx.AddError(TIssue(ctx.GetPosition(input.Pos()),
                TStringBuilder() << "Unsupported callable " << input.Ref().Content()));
            return TStatus::Error;
        }

        if (!EnsureArgsCount(input.Ref(), 1, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureDependsOn(*input.Ptr()->Child(TYtTableIndex::idx_DependsOn), ctx)) {
            return TStatus::Error;
        }

        input.Ptr()->SetTypeAnn(ctx.MakeType<TDataExprType>(EDataSlot::Uint32));
        return TStatus::Ok;
    }

    TStatus HandleIsKeySwitch(TExprBase input, TExprContext& ctx) {
        if (State_->Configuration->UseSystemColumns.Get().GetOrElse(DEFAULT_USE_SYS_COLUMNS)) {
            ctx.AddError(TIssue(ctx.GetPosition(input.Pos()),
                TStringBuilder() << "Unsupported callable " << input.Ref().Content()));
            return TStatus::Error;
        }

        if (!EnsureArgsCount(input.Ref(), 1, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureDependsOn(*input.Ptr()->Child(TYtIsKeySwitch::idx_DependsOn), ctx)) {
            return TStatus::Error;
        }

        input.Ptr()->SetTypeAnn(ctx.MakeType<TDataExprType>(EDataSlot::Bool));
        return TStatus::Ok;
    }

    TStatus HandleTableName(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        if (!EnsureArgsCount(*input, 1, ctx)) {
            return TStatus::Error;
        }

        output = ctx.Builder(input->Pos())
            .Callable("Substring")
                .Add(0, input->ChildPtr(0))
                .Callable(1, "Inc")
                    .Callable(0, "RFind")
                        .Add(0, input->ChildPtr(0))
                        .Callable(1, "String").Atom(0, "/").Seal()
                        .Callable(2, "Null").Seal()
                    .Seal()
                .Seal()
                .Callable(2, "Null").Seal()
            .Seal()
            .Build();

        return TStatus::Repeat;
    }
private:
    const TYtState::TPtr State_;
};

}

THolder<TVisitorTransformerBase> CreateYtDataSourceTypeAnnotationTransformer(TYtState::TPtr state) {
    return THolder(new TYtDataSourceTypeAnnotationTransformer(state));
}

}
