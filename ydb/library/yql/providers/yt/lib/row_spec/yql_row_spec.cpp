#include "yql_row_spec.h"

#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <ydb/library/yql/providers/yt/lib/schema/schema.h>
#include <ydb/library/yql/providers/yt/common/yql_names.h>
#include <ydb/library/yql/providers/yt/common/yql_yt_settings.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/codec/yql_codec_type_flags.h>
#include <ydb/library/yql/providers/common/schema/expr/yql_expr_schema.h>
#include <ydb/library/yql/providers/common/schema/yql_schema_utils.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/issue/yql_issue.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/utils/log/log.h>

#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/yson/node/node_builder.h>

#include <util/generic/cast.h>
#include <util/generic/xrange.h>
#include <util/string/builder.h>

#include <algorithm>

namespace NYql {

namespace {

ui64 GetNativeYtTypeFlagsImpl(const TTypeAnnotationNode* itemType) {
    switch (itemType->GetKind()) {
        case ETypeAnnotationKind::Pg: {
            auto name = itemType->Cast<TPgExprType>()->GetName();
            if (name == "float4") {
                return NTCF_FLOAT | NTCF_NO_YT_SUPPORT;
            }

            return NTCF_NO_YT_SUPPORT;
        }
        case ETypeAnnotationKind::Data:
            switch (itemType->Cast<TDataExprType>()->GetSlot()) {
            case EDataSlot::Date:
            case EDataSlot::Datetime:
            case EDataSlot::Timestamp:
            case EDataSlot::Interval:
                return NTCF_DATE;
            case EDataSlot::Date32:
            case EDataSlot::Datetime64:
            case EDataSlot::Timestamp64:
            case EDataSlot::Interval64:
                return NTCF_BIGDATE;
            case EDataSlot::Json:
                return NTCF_JSON;
            case EDataSlot::Float:
                return NTCF_FLOAT;
            case EDataSlot::Decimal:
                return NTCF_DECIMAL;
            case EDataSlot::Uuid:
            case EDataSlot::TzDate:
            case EDataSlot::TzDatetime:
            case EDataSlot::TzTimestamp:
            case EDataSlot::TzDate32:
            case EDataSlot::TzDatetime64:
            case EDataSlot::TzTimestamp64:
            case EDataSlot::DyNumber:
            case EDataSlot::JsonDocument:
                return NTCF_NO_YT_SUPPORT;
            default:
                return NTCF_NONE;
            }
        case ETypeAnnotationKind::Null:
            return NTCF_NULL;
        case ETypeAnnotationKind::Void:
            return NTCF_VOID;
        case ETypeAnnotationKind::Optional:
            return NTCF_COMPLEX | GetNativeYtTypeFlagsImpl(itemType->Cast<TOptionalExprType>()->GetItemType());
        case ETypeAnnotationKind::List:
            return NTCF_COMPLEX | GetNativeYtTypeFlagsImpl(itemType->Cast<TListExprType>()->GetItemType());
        case ETypeAnnotationKind::Dict: {
            auto dictType = itemType->Cast<TDictExprType>();
            return NTCF_COMPLEX | GetNativeYtTypeFlagsImpl(dictType->GetKeyType()) | GetNativeYtTypeFlagsImpl(dictType->GetPayloadType());
        }
        case ETypeAnnotationKind::Variant:
            return NTCF_COMPLEX | GetNativeYtTypeFlagsImpl(itemType->Cast<TVariantExprType>()->GetUnderlyingType());
        case ETypeAnnotationKind::Struct: {
            ui64 flags = NTCF_COMPLEX;
            for (auto item: itemType->Cast<TStructExprType>()->GetItems()) {
                flags |= GetNativeYtTypeFlagsImpl(item->GetItemType());
            }
            return flags;
        }
        case ETypeAnnotationKind::Tuple: {
            ui64 flags = NTCF_COMPLEX;
            for (auto item: itemType->Cast<TTupleExprType>()->GetItems()) {
                flags |= GetNativeYtTypeFlagsImpl(item);
            }
            return flags;
        }
        case ETypeAnnotationKind::Tagged:
            return NTCF_COMPLEX | GetNativeYtTypeFlagsImpl(itemType->Cast<TTaggedExprType>()->GetBaseType());
        case ETypeAnnotationKind::EmptyDict:
        case ETypeAnnotationKind::EmptyList:
            return NTCF_COMPLEX;
        case ETypeAnnotationKind::World:
        case ETypeAnnotationKind::Unit:
        case ETypeAnnotationKind::Item:
        case ETypeAnnotationKind::Callable:
        case ETypeAnnotationKind::Generic:
        case ETypeAnnotationKind::Error:
        case ETypeAnnotationKind::Resource:
        case ETypeAnnotationKind::Stream:
        case ETypeAnnotationKind::Flow:
        case ETypeAnnotationKind::Multi:
        case ETypeAnnotationKind::Type:
        case ETypeAnnotationKind::Block:
        case ETypeAnnotationKind::Scalar:
        case ETypeAnnotationKind::LastType:
            break;
    }
    return NTCF_NONE;
}

}

ui64 GetNativeYtTypeFlags(const TStructExprType& type, const NCommon::TStructMemberMapper& mapper) {
    ui64 flags = 0;
    for (auto item: type.GetItems()) {
        if (!mapper || mapper(item->GetName())) {
            const TTypeAnnotationNode* itemType = item->GetItemType();
            bool wasOptional = false;
            if (itemType->GetKind() == ETypeAnnotationKind::Optional) {
                wasOptional = true;
                itemType = itemType->Cast<TOptionalExprType>()->GetItemType();
            }

            if (wasOptional && itemType->GetKind() == ETypeAnnotationKind::Pg) {
                flags |= NTCF_COMPLEX;
            }

            flags |= GetNativeYtTypeFlagsImpl(itemType);
        }
    }
    flags &= ~NTCF_NO_YT_SUPPORT;
    return flags;
}

using namespace NNodes;

bool TYqlRowSpecInfo::Parse(const TString& rowSpecYson, TExprContext& ctx, const TPositionHandle& pos) {
    try {
        return Parse(NYT::NodeFromYsonString(rowSpecYson), ctx, pos);
    } catch (const std::exception& e) {
        ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "Failed to parse row spec: " << e.what()));
        return false;
    }
}

bool TYqlRowSpecInfo::Parse(const NYT::TNode& rowSpecAttr, TExprContext& ctx, const TPositionHandle& pos) {
    *this = {};
    try {
        if (!ParseType(rowSpecAttr, ctx, pos) || !ParseSort(rowSpecAttr, ctx, pos)) {
            return false;
        }
        ParseFlags(rowSpecAttr);
        ParseDefValues(rowSpecAttr);
        ParseConstraints(rowSpecAttr);
        ParseConstraintsNode(ctx);
    } catch (const std::exception& e) {
        ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "Failed to parse row spec: " << e.what()));
        return false;
    }

    return Validate(ctx, pos);
}

bool TYqlRowSpecInfo::ParsePatched(const NYT::TNode& rowSpecAttr, const THashMap<TString, TString>& attrs, TExprContext& ctx, const TPositionHandle& pos) {
    auto schemaAttr = attrs.FindPtr(SCHEMA_ATTR_NAME);
    if (!schemaAttr) {
        YQL_LOG_CTX_THROW yexception() << YqlRowSpecAttribute << " with " << RowSpecAttrTypePatch << " attribute requires YT schema to be present";
    }
    auto schema = NYT::NodeFromYsonString(*schemaAttr);
    auto strict = schema.GetAttributes()["strict"];
    if (!strict.IsUndefined() && !NYT::GetBool(strict)) {
        YQL_LOG_CTX_THROW yexception() << YqlRowSpecAttribute << " with " << RowSpecAttrTypePatch << " attribute can only be used with 'strict' schema";
    }
    auto mode = schema.GetAttributes()[SCHEMA_MODE_ATTR_NAME];
    if (!mode.IsUndefined() && mode.AsString() == "weak") {
        YQL_LOG_CTX_THROW yexception() << YqlRowSpecAttribute << " with " << RowSpecAttrTypePatch << " attribute can only be used with 'strong' schema";
    }

    auto schemaAsRowSpec = YTSchemaToRowSpec(schema);
    if (!ParseType(schemaAsRowSpec, ctx, pos) || !ParseSort(schemaAsRowSpec, ctx, pos)) {
        return false;
    }
    ParseFlags(schemaAsRowSpec);

    auto typePatch = NCommon::ParseTypeFromYson(rowSpecAttr[RowSpecAttrTypePatch], ctx, ctx.GetPosition(pos));
    if (!typePatch) {
        return false;
    }

    if (typePatch->GetKind() != ETypeAnnotationKind::Struct) {
        YQL_LOG_CTX_THROW yexception() << "Row spec TypePatch has a non struct type: " << *typePatch;
    }

    if (!ParseSort(rowSpecAttr, ctx, pos)) {
        return false;
    }

    TSet<TString> auxFields;
    for (size_t i = 0; i < SortedBy.size(); ++i) {
        if ((i >= SortMembers.size() || SortedBy[i] != SortMembers[i]) && IsSystemMember(SortedBy[i])) {
            auxFields.insert(SortedBy[i]);
        }
    }
    auto typePatchStruct = typePatch->Cast<TStructExprType>();
    if (typePatchStruct->GetSize() || !auxFields.empty()) {
        // Patch Type
        auto updatedItems = Type->GetItems();
        for (auto& patchItem: typePatchStruct->GetItems()) {
            auto name = patchItem->GetName();
            YQL_ENSURE(!auxFields.contains(name));
            auto itemPos = Type->FindItem(name);
            if (!itemPos) {
                throw yexception() << "Row spec TypePatch refers to unknown field: " << name;
            }
            updatedItems[*itemPos] = patchItem;
        }
        for (auto it = auxFields.rbegin(); it != auxFields.rend(); ++it) {
            auto itemPos = Type->FindItem(*it);
            if (!itemPos) {
                throw yexception() << "Row spec SortedBy refers to unknown field: " << *it;
            }
            YQL_ENSURE(*itemPos < updatedItems.size(), "Something wrong!");
            updatedItems.erase(updatedItems.begin() + *itemPos);
        }
        Type = ctx.MakeType<TStructExprType>(updatedItems);

        // Patch TypeNode
        THashMap<TString, const NYT::TNode*> patchNodes;
        for (auto& item: rowSpecAttr[RowSpecAttrTypePatch][1].AsList()) {
            patchNodes.emplace(item[0].AsString(), &item);
        }
        if (auxFields.empty()) {
            for (auto& item: TypeNode[1].AsList()) {
                if (auto p = patchNodes.FindPtr(item[0].AsString())) {
                    item = **p;
                }
            }
        } else {
            auto& membersList = TypeNode[1].AsList();
            NYT::TNode::TListType newMembers;
            newMembers.reserve(membersList.size());
            for (auto& item: membersList) {
                if (!auxFields.contains(item[0].AsString())) {
                    if (auto p = patchNodes.FindPtr(item[0].AsString())) {
                        newMembers.push_back(**p);
                    } else {
                        newMembers.push_back(item);
                    }
                }
            }
            membersList = std::move(newMembers);

            // Patch Columns
            TColumnOrder newColumns;
            for (auto& [col, gen_col]: *Columns) {
                if (!auxFields.contains(col)) {
                    newColumns.AddColumn(col);
                }
            }
            Columns = std::move(newColumns);
        }
        YQL_ENSURE(Type->GetSize() == TypeNode[1].AsList().size());
    }

    TYTSortInfo sortInfo = KeyColumnsFromSchema(schema);
    if (!ValidateSort(sortInfo, ctx, pos)) {
        return false;
    }

    ParseFlags(rowSpecAttr);
    ParseDefValues(rowSpecAttr);
    ParseConstraints(rowSpecAttr);
    ParseConstraintsNode(ctx);
    return true;
}

bool TYqlRowSpecInfo::ParseFull(const NYT::TNode& rowSpecAttr, const THashMap<TString, TString>& attrs, TExprContext& ctx, const TPositionHandle& pos) {
    if (!ParseType(rowSpecAttr, ctx, pos) || !ParseSort(rowSpecAttr, ctx, pos)) {
        return false;
    }
    ParseFlags(rowSpecAttr);
    ParseDefValues(rowSpecAttr);
    ParseConstraints(rowSpecAttr);
    ParseConstraintsNode(ctx);

    if (auto schemaAttr = attrs.FindPtr(SCHEMA_ATTR_NAME)) {
        auto schema = NYT::NodeFromYsonString(*schemaAttr);
        auto modeAttr = schema.GetAttributes()[SCHEMA_MODE_ATTR_NAME];
        const bool weak = !modeAttr.IsUndefined() && modeAttr.AsString() == "weak";
        // Validate type for non weak schema only
        if (!weak) {
            auto schemaAsRowSpec = YTSchemaToRowSpec(schema);
            auto type = NCommon::ParseTypeFromYson(schemaAsRowSpec[RowSpecAttrType], ctx, ctx.GetPosition(pos));
            if (!type) {
                return false;
            }
            if (type->GetKind() != ETypeAnnotationKind::Struct) {
                YQL_LOG_CTX_THROW yexception() << "YT schema type has a non struct type: " << *type;
            }
            THashSet<TStringBuf> auxFields(SortedBy.cbegin(), SortedBy.cend());
            TStringBuilder hiddenFields;
            for (auto item: type->Cast<TStructExprType>()->GetItems()) {
                if (const auto name = item->GetName(); !Type->FindItem(name) && !auxFields.contains(name)) {
                    if (hiddenFields.size() > 100) {
                        hiddenFields << ", ...";
                        break;
                    }
                    if (!hiddenFields.empty()) {
                        hiddenFields << ", ";
                    }
                    hiddenFields << item->GetName();
                }
            }
            if (!hiddenFields.empty()) {
                hiddenFields.prepend("Table attribute '_yql_row_spec' hides fields: ");
                if (!ctx.AddWarning(YqlIssue(ctx.GetPosition(pos), EYqlIssueCode::TIssuesIds_EIssueCode_YT_ROWSPEC_HIDES_FIELDS, hiddenFields))) {
                    return false;
                }
            }
        }
        TYTSortInfo sortInfo = KeyColumnsFromSchema(schema);
        if (!ValidateSort(sortInfo, ctx, pos)) {
            return false;
        }
    }
    return true;
}

// Priority:
// 1. YqlRowSpec(TypePatch) + Schema
// 2. YqlRowSpec(Type)
// 3. _infer_schema + schema(SortBy)
// 4. _read_schema + schema(SortBy)
// 5. schema
bool TYqlRowSpecInfo::Parse(const THashMap<TString, TString>& attrs, TExprContext& ctx, const TPositionHandle& pos) {
    *this = {};
    try {
        if (auto rowSpecAttr = attrs.FindPtr(YqlRowSpecAttribute)) {
            auto rowSpec = NYT::NodeFromYsonString(*rowSpecAttr);
            if (rowSpec.HasKey(RowSpecAttrTypePatch)) {
                if (!ParsePatched(rowSpec, attrs, ctx, pos)) {
                    return false;
                }
            } else {
                if (!ParseFull(rowSpec, attrs, ctx, pos)) {
                    return false;
                }
            }
        } else if (auto inferSchemaAttr = attrs.FindPtr(INFER_SCHEMA_ATTR_NAME)) {
            auto inferSchema = NYT::NodeFromYsonString(*inferSchemaAttr);

            TYTSortInfo sortInfo;
            auto schemaAttr = attrs.FindPtr(SCHEMA_ATTR_NAME);
            if (schemaAttr) {
                auto schema = NYT::NodeFromYsonString(*schemaAttr);
                sortInfo = KeyColumnsFromSchema(schema);
                MergeInferredSchemeWithSort(inferSchema, sortInfo);
            }
            auto schemaAsRowSpec = YTSchemaToRowSpec(inferSchema, schemaAttr ? &sortInfo : nullptr);
            if (!ParseType(schemaAsRowSpec, ctx, pos) || !ParseSort(schemaAsRowSpec, ctx, pos)) {
                return false;
            }
            ParseFlags(schemaAsRowSpec);
        } else if (auto readSchema = attrs.FindPtr(READ_SCHEMA_ATTR_NAME)) {
            TYTSortInfo sortInfo;
            if (auto schemaAttr = attrs.FindPtr(SCHEMA_ATTR_NAME)) {
                sortInfo = KeyColumnsFromSchema(NYT::NodeFromYsonString(*schemaAttr));
            }
            auto schemaAsRowSpec = YTSchemaToRowSpec(NYT::NodeFromYsonString(*readSchema), &sortInfo);
            if (!ParseType(schemaAsRowSpec, ctx, pos) || !ParseSort(schemaAsRowSpec, ctx, pos)) {
                return false;
            }
            ParseFlags(schemaAsRowSpec);
        } else if (auto schema = attrs.FindPtr(SCHEMA_ATTR_NAME)) {
            auto schemaAsRowSpec = YTSchemaToRowSpec(NYT::NodeFromYsonString(*schema));
            if (!ParseType(schemaAsRowSpec, ctx, pos) || !ParseSort(schemaAsRowSpec, ctx, pos)) {
                return false;
            }
            ParseFlags(schemaAsRowSpec);
        } else {
            YQL_LOG_CTX_THROW yexception() << "Table has no supported schema attributes";
        }
    } catch (const std::exception& e) {
        ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "Failed to parse row spec: " << e.what()));
        return false;
    }

    return Validate(ctx, pos);
}

bool TYqlRowSpecInfo::ParseType(const NYT::TNode& rowSpecAttr, TExprContext& ctx, const TPositionHandle& pos) {
    if (!rowSpecAttr.HasKey(RowSpecAttrType)) {
        YQL_LOG_CTX_THROW yexception() << "Row spec doesn't have mandatory Type attribute";
    }
    TColumnOrder columns;
    auto type = NCommon::ParseOrderAwareTypeFromYson(rowSpecAttr[RowSpecAttrType], columns, ctx, ctx.GetPosition(pos));
    if (!type) {
        return false;
    }
    if (type->GetKind() != ETypeAnnotationKind::Struct) {
        YQL_LOG_CTX_THROW yexception() << "Row spec defines not a struct type";
    }

    Type = type->Cast<TStructExprType>();
    TypeNode = rowSpecAttr[RowSpecAttrType];
    Columns = std::move(columns);

    if (rowSpecAttr.HasKey(RowSpecAttrStrictSchema)) {
        // Backward compatible parse. Old code saves 'StrictSchema' as Int64
        StrictSchema = rowSpecAttr[RowSpecAttrStrictSchema].IsInt64()
            ? rowSpecAttr[RowSpecAttrStrictSchema].AsInt64() != 0
            : NYT::GetBool(rowSpecAttr[RowSpecAttrStrictSchema]);
        if (!StrictSchema) {
            auto items = Type->GetItems();
            auto dictType = ctx.MakeType<TDictExprType>(
                ctx.MakeType<TDataExprType>(EDataSlot::String),
                ctx.MakeType<TDataExprType>(EDataSlot::String));
            items.push_back(ctx.MakeType<TItemExprType>(YqlOthersColumnName, dictType));
            Type = ctx.MakeType<TStructExprType>(items);
            Columns->AddColumn(TString(YqlOthersColumnName));
        }
    }

    return true;
}

bool TYqlRowSpecInfo::ParseSort(const NYT::TNode& rowSpecAttr, TExprContext& ctx, const TPositionHandle& pos) {
    if (rowSpecAttr.HasKey(RowSpecAttrSortMembers) || rowSpecAttr.HasKey(RowSpecAttrSortedBy) || rowSpecAttr.HasKey(RowSpecAttrSortDirections)) {
        ClearSortness();
    }
    if (rowSpecAttr.HasKey(RowSpecAttrSortDirections)) {
        for (auto& item: rowSpecAttr[RowSpecAttrSortDirections].AsList()) {
            SortDirections.push_back(item.AsInt64() != 0);
        }
    }

    auto loadColumnList = [&] (TStringBuf name, TVector<TString>& columns) {
        if (rowSpecAttr.HasKey(name)) {
            auto& list = rowSpecAttr[name].AsList();
            for (const auto& item : list) {
                columns.push_back(item.AsString());
            }
        }
    };

    loadColumnList(RowSpecAttrSortMembers, SortMembers);
    loadColumnList(RowSpecAttrSortedBy, SortedBy);

    if (rowSpecAttr.HasKey(RowSpecAttrSortedByTypes)) {
        auto& list = rowSpecAttr[RowSpecAttrSortedByTypes].AsList();
        for (auto& type : list) {
            if (auto sortType = NCommon::ParseTypeFromYson(type, ctx, ctx.GetPosition(pos))) {
                SortedByTypes.push_back(sortType);
            } else {
                return false;
            }
        }
    }

    if (rowSpecAttr.HasKey(RowSpecAttrUniqueKeys)) {
        UniqueKeys = NYT::GetBool(rowSpecAttr[RowSpecAttrUniqueKeys]);
    }
    return true;
}

void TYqlRowSpecInfo::ParseFlags(const NYT::TNode& rowSpecAttr) {
    if (rowSpecAttr.HasKey(RowSpecAttrNativeYtTypeFlags)) {
        NativeYtTypeFlags = rowSpecAttr[RowSpecAttrNativeYtTypeFlags].AsUint64();
    } else {
        if (rowSpecAttr.HasKey(RowSpecAttrUseNativeYtTypes)) {
            NativeYtTypeFlags = NYT::GetBool(rowSpecAttr[RowSpecAttrUseNativeYtTypes]) ? NTCF_LEGACY : NTCF_NONE;
        } else  if (rowSpecAttr.HasKey(RowSpecAttrUseTypeV2)) {
            NativeYtTypeFlags = NYT::GetBool(rowSpecAttr[RowSpecAttrUseTypeV2]) ? NTCF_LEGACY : NTCF_NONE;
        }
    }
    if (NativeYtTypeFlags) {
        NativeYtTypeFlags &= NYql::GetNativeYtTypeFlags(*Type);
    }
}

void TYqlRowSpecInfo::ParseDefValues(const NYT::TNode& rowSpecAttr) {
    if (rowSpecAttr.HasKey(RowSpecAttrDefaultValues)) {
        for (auto& value : rowSpecAttr[RowSpecAttrDefaultValues].AsMap()) {
            DefaultValues[value.first] = NYT::NodeFromYsonString(value.second.AsString()).AsString();
        }
    }
}

bool TYqlRowSpecInfo::HasNonTrivialSort() const {
    return Sorted && (HasAuxColumns() || std::any_of(Sorted->GetContent().cbegin(), Sorted->GetContent().cend(),
        [](const TSortedConstraintNode::TContainerType::value_type& item) {return 1U != item.first.size() || 1U != item.first.front().size(); }));
}

NYT::TNode TYqlRowSpecInfo::GetConstraintsNode() const {
    if (ConstraintsNode.HasValue())
        return ConstraintsNode;

    auto map = NYT::TNode::CreateMap();

    const auto pathToNode = [](const TPartOfConstraintBase::TPathType& path) -> NYT::TNode {
        if (1U == path.size())
            return TStringBuf(path.front());

        auto list = NYT::TNode::CreateList();
        for (const auto& col : path)
            list.Add(TStringBuf(col));
        return list;
    };

    const auto setToNode = [pathToNode](const TPartOfConstraintBase::TSetType& set) -> NYT::TNode {
        if (1U == set.size() && 1U == set.front().size())
            return TStringBuf(set.front().front());

        auto list = NYT::TNode::CreateList();
        for (const auto& path : set)
            list.Add(pathToNode(path));
        return list;
    };

    if (HasNonTrivialSort()) {
        auto list = NYT::TNode::CreateList();
        for (const auto& item : Sorted->GetContent()) {
            auto pair = NYT::TNode::CreateList();
            auto set = NYT::TNode::CreateList();
            for (const auto& path : item.first)
                set.Add(pathToNode(path));
            pair.Add(set).Add(item.second);
            list.Add(pair);
        }
        map[Sorted->GetName()] = list;
    }

    if (Unique) {
        auto list = NYT::TNode::CreateList();
        for (const auto& sets : Unique->GetContent()) {
            auto part = NYT::TNode::CreateList();
            for (const auto& set : sets)
                part.Add(setToNode(set));
            list.Add(part);
        }
        map[Unique->GetName()] = list;
    }

    if (Distinct) {
        auto list = NYT::TNode::CreateList();
        for (const auto& sets : Distinct->GetContent()) {
            auto part = NYT::TNode::CreateList();
            for (const auto& set : sets)
                part.Add(setToNode(set));
            list.Add(part);
        }
        map[Distinct->GetName()] = list;
    }
    return map;
}

void TYqlRowSpecInfo::FillConstraints(NYT::TNode& attrs) const {
    if (HasNonTrivialSort() || Unique || Distinct)
        attrs[RowSpecAttrConstraints] = GetConstraintsNode();
}

void TYqlRowSpecInfo::ParseConstraintsNode(TExprContext& ctx) {
    if (!ConstraintsNode.HasValue())
        return;

    try  {
        const auto nodeToPath = [&ctx](const NYT::TNode& node) {
            if (node.IsString())
                return TPartOfConstraintBase::TPathType{ctx.AppendString(node.AsString())};

            TPartOfConstraintBase::TPathType path;
            for (const auto& col : node.AsList())
                path.emplace_back(ctx.AppendString(col.AsString()));
            return path;
        };

        const auto nodeToSet = [&ctx, nodeToPath](const NYT::TNode& node) {
            if (node.IsString())
                return TPartOfConstraintBase::TSetType{TPartOfConstraintBase::TPathType(1U, ctx.AppendString(node.AsString()))};

            TPartOfConstraintBase::TSetType set;
            for (const auto& col : node.AsList())
                set.insert_unique(nodeToPath(col));
            return set;
        };

        const auto& constraints = ConstraintsNode.AsMap();

        if (const auto it = constraints.find(TSortedConstraintNode::Name()); constraints.cend() != it) {
            TSortedConstraintNode::TContainerType sorted;
            for (const auto& pair : it->second.AsList()) {
                TPartOfConstraintBase::TSetType set;
                for (const auto& path : pair.AsList().front().AsList())
                    set.insert_unique(nodeToPath(path));
                sorted.emplace_back(std::move(set), pair.AsList().back().AsBool());
            }
            if (!sorted.empty())
                Sorted = ctx.MakeConstraint<TSortedConstraintNode>(std::move(sorted));
        }
        if (const auto it = constraints.find(TUniqueConstraintNode::Name()); constraints.cend() != it) {
            TUniqueConstraintNode::TContentType content;
            for (const auto& item : it->second.AsList()) {
                TPartOfConstraintBase::TSetOfSetsType sets;
                for (const auto& part : item.AsList())
                    sets.insert_unique(nodeToSet(part));
                content.insert_unique(std::move(sets));
            }
            if (!content.empty())
                Unique = ctx.MakeConstraint<TUniqueConstraintNode>(std::move(content));
        }
        if (const auto it = constraints.find(TDistinctConstraintNode::Name()); constraints.cend() != it) {
            TDistinctConstraintNode::TContentType content;
            for (const auto& item : it->second.AsList()) {
                TPartOfConstraintBase::TSetOfSetsType sets;
                for (const auto& part : item.AsList())
                    sets.insert_unique(nodeToSet(part));
                content.insert_unique(std::move(sets));
            }
            if (!content.empty())
                Distinct = ctx.MakeConstraint<TDistinctConstraintNode>(std::move(content));
        }
    } catch (const yexception& error) {
        Sorted = nullptr;
        Unique = nullptr;
        Distinct = nullptr;
        YQL_CLOG(WARN, ProviderDq) << " Error '" << error << "' on parse constraints node: " << ConstraintsNode.AsString();
    }
}

TConstraintSet TYqlRowSpecInfo::GetSomeConstraints(ui64 mask, TExprContext& ctx) {
    TConstraintSet set;
    if (mask) {
        ParseConstraintsNode(ctx);
        if (Sorted && (ui64(EStoredConstraint::Sorted) & mask))
            set.AddConstraint(Sorted);
        if (Unique && (ui64(EStoredConstraint::Unique) & mask))
            set.AddConstraint(Unique);
        if (Distinct && (ui64(EStoredConstraint::Distinct) & mask))
            set.AddConstraint(Distinct);
    }
    return set;
}

TConstraintSet TYqlRowSpecInfo::GetAllConstraints(TExprContext& ctx) {
    return GetSomeConstraints(ui64(EStoredConstraint::Sorted) | ui64(EStoredConstraint::Unique) | ui64(EStoredConstraint::Distinct), ctx);
}

void TYqlRowSpecInfo::ParseConstraints(const NYT::TNode& rowSpecAttr) {
    if (rowSpecAttr.HasKey(RowSpecAttrConstraints))
        ConstraintsNode = rowSpecAttr[RowSpecAttrConstraints];
}

bool TYqlRowSpecInfo::ValidateSort(const TYTSortInfo& sortInfo, TExprContext& ctx, const TPositionHandle& pos) {
    if (sortInfo.Keys.empty() && IsSorted()) {
        ClearSortness();
        if (!ctx.AddWarning(YqlIssue(ctx.GetPosition(pos), EYqlIssueCode::TIssuesIds_EIssueCode_YT_ROWSPEC_DIFF_SORT,
            "Table attribute '_yql_row_spec' defines sorting, but the table is not really sorted. The sorting will be ignored."))) {
            return false;
        }
    }
    else if (!sortInfo.Keys.empty() && !IsSorted()) {
        if (!ctx.AddWarning(YqlIssue(ctx.GetPosition(pos), EYqlIssueCode::TIssuesIds_EIssueCode_YT_ROWSPEC_DIFF_SORT,
            "Table attribute '_yql_row_spec' hides the table sorting. The sorting will not be used in query optimization."))) {
            return false;
        }
    } else if (IsSorted()) {
        bool diff = false;
        if (SortedBy.size() > sortInfo.Keys.size()) {
            ClearSortness(sortInfo.Keys.size());
            diff = true;
        }
        auto backendSort = GetForeignSort();
        for (size_t i = 0; i < backendSort.size(); ++i) {
            if (backendSort[i].first != sortInfo.Keys[i].first || backendSort[i].second != (bool)sortInfo.Keys[i].second) {
                ClearSortness(i);
                diff = true;
                break;
            }
        }
        if (diff) {
            TStringBuilder warning;
            warning << "Table attribute '_yql_row_spec' defines sorting, which differs from the actual one. The table will be assumed ";
            if (IsSorted()) {
                warning << "ordered by ";
                for (size_t i: xrange(SortMembers.size())) {
                    if (i != 0) {
                        warning << ',';
                    }
                    warning << SortMembers[i] << '('
                        << (SortDirections[i] ? "asc" : "desc") << ")";
                }
            } else {
                warning << "unordered";
            }
            if (!ctx.AddWarning(YqlIssue(ctx.GetPosition(pos), EYqlIssueCode::TIssuesIds_EIssueCode_YT_ROWSPEC_DIFF_SORT, warning))) {
                return false;
            }
        }
    }
    return true;
}

bool TYqlRowSpecInfo::Validate(const TExprNode& node, TExprContext& ctx, const TStructExprType*& type, TMaybe<TColumnOrder>& columnOrder) {
    type = nullptr;
    columnOrder = {};
    if (!EnsureCallable(node, ctx)) {
        return false;
    }
    if (!node.IsCallable(TYqlRowSpec::CallableName())) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Expected " << TYqlRowSpec::CallableName()
            << " callable, but got " << node.Content()));
        return false;
    }

    TVector<TString> sortedBy;
    TVector<TString> sortMembers;
    size_t sortDirectionsCount = 0;
    TTypeAnnotationNode::TListType sortedByTypes;
    THashSet<TStringBuf> defaultNames;
    TVector<TString> explicitYson;
    bool extendNonStrict = false;
    bool strict = true;
    for (auto child: node.Children()) {
        if (!EnsureTupleSize(*child, 2, ctx)) {
            return false;
        }
        const TExprNode* name = child->Child(0);
        TExprNode* value = child->Child(1);
        if (!EnsureAtom(*name, ctx)) {
            return false;
        }
        bool flagValue = false;
        if (name->Content() == RowSpecAttrStrictSchema) {
            if (!EnsureAtom(*value, ctx)) {
                return false;
            }
            if (!TryFromString(value->Content(), strict)) {
                ctx.AddError(TIssue(ctx.GetPosition(value->Pos()), TStringBuilder() << "Bad value of "
                    << TString{RowSpecAttrStrictSchema}.Quote() << " attribute: " << value->Content()));
                return false;
            }
        } else if (name->Content() == RowSpecAttrUseTypeV2 || name->Content() == RowSpecAttrUseNativeYtTypes) {
            if (!EnsureAtom(*value, ctx)) {
                return false;
            }
            if (!TryFromString(value->Content(), flagValue)) {
                ctx.AddError(TIssue(ctx.GetPosition(value->Pos()), TStringBuilder() << "Bad value of "
                    << TString{name->Content()}.Quote() << " attribute: " << value->Content()));
                return false;
            }
        } else if (name->Content() == RowSpecAttrNativeYtTypeFlags) {
            if (!EnsureAtom(*value, ctx)) {
                return false;
            }
            ui64 flags = 0;
            if (!TryFromString(value->Content(), flags)) {
                ctx.AddError(TIssue(ctx.GetPosition(value->Pos()), TStringBuilder() << "Bad value of "
                    << TString{name->Content()}.Quote() << " attribute: " << value->Content()));
                return false;
            }
        } else if (name->Content() == RowSpecAttrUniqueKeys) {
            if (!EnsureAtom(*value, ctx)) {
                return false;
            }
            if (!TryFromString(value->Content(), flagValue)) {
                ctx.AddError(TIssue(ctx.GetPosition(value->Pos()), TStringBuilder() << "Bad value of "
                    << TString{RowSpecAttrUniqueKeys}.Quote() << " attribute: " << value->Content()));
                return false;
            }
        } else if (name->Content() == RowSpecAttrType) {
            const TTypeAnnotationNode* rawType = nullptr;
            if (value->Type() == TExprNode::Atom) {
                columnOrder.ConstructInPlace();
                rawType = NCommon::ParseOrderAwareTypeFromYson(value->Content(), *columnOrder, ctx, ctx.GetPosition(value->Pos()));
                if (!rawType) {
                    return false;
                }
                extendNonStrict = true;
            } else {
                if (!EnsureType(*value, ctx)) {
                    return false;
                }
                rawType = value->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
            }
            if (!EnsureStructType(value->Pos(), *rawType, ctx)) {
                return false;
            }
            type = rawType->Cast<TStructExprType>();
        } else if (name->Content() == RowSpecAttrSortedBy) {
            if (!EnsureTuple(*value, ctx)) {
                return false;
            }
            for (const TExprNode::TPtr& item: value->Children()) {
                if (!EnsureAtom(*item, ctx)) {
                    return false;
                }
                sortedBy.push_back(TString{item->Content()});
            }
        } else if (name->Content() == RowSpecAttrSortMembers) {
            if (!EnsureTuple(*value, ctx)) {
                return false;
            }
            for (const TExprNode::TPtr& item: value->Children()) {
                if (!EnsureAtom(*item, ctx)) {
                    return false;
                }
                sortMembers.push_back(TString{item->Content()});
            }
        } else if (name->Content() == RowSpecAttrSortDirections) {
            if (!EnsureTuple(*value, ctx)) {
                return false;
            }
            for (const TExprNode::TPtr& item: value->Children()) {
                if (!EnsureCallable(*item, ctx)) {
                    return false;
                }
                if (!item->IsCallable(TCoBool::CallableName())) {
                    ctx.AddError(TIssue(ctx.GetPosition(item->Pos()), TStringBuilder() << "Expected " << TCoBool::CallableName()
                        << ", but got " << item->Content()));
                    return false;
                }
            }
            sortDirectionsCount = value->Children().size();
        } else if (name->Content() == RowSpecAttrSortedByTypes) {
            if (!EnsureTuple(*value, ctx)) {
                return false;
            }
            for (const TExprNode::TPtr& item: value->Children()) {
                if (!EnsureType(*item, ctx)) {
                    return false;
                }
                sortedByTypes.push_back(item->GetTypeAnn()->Cast<TTypeExprType>()->GetType());
            }
        } else if (name->Content() == RowSpecAttrDefaultValues) {
            if (!EnsureTupleMinSize(*value, 1, ctx)) {
                return false;
            }
            for (const TExprNode::TPtr& item: value->Children()) {
                if (!EnsureTupleSize(*item, 2, ctx)) {
                    return false;
                }
                for (const TExprNode::TPtr& atom: item->Children()) {
                    if (!EnsureAtom(*atom, ctx)) {
                        return false;
                    }
                }
                if (!defaultNames.insert(item->Child(0)->Content()).second) {
                    ctx.AddError(TIssue(ctx.GetPosition(item->Child(0)->Pos()), TStringBuilder() << "Duplicate "
                        << TString{RowSpecAttrDefaultValues}.Quote() << " key: " << item->Child(0)->Content()));
                    return false;
                }
            }
        } else if (name->Content() == RowSpecAttrExplicitYson) {
            if (!EnsureTuple(*value, ctx)) {
                return false;
            }
            for (const TExprNode::TPtr& item: value->Children()) {
                if (!EnsureAtom(*item, ctx)) {
                    return false;
                }
                explicitYson.emplace_back(item->Content());
            }
        } else if (name->Content() == RowSpecAttrConstraints) {
            if (!EnsureAtom(*value, ctx)) {
                return false;
            }
        } else {
            ctx.AddError(TIssue(ctx.GetPosition(child->Pos()), TStringBuilder() << "Unsupported "
                << TYqlRowSpec::CallableName() << " option: " << name->Content()));
            return false;
        }
    }
    if (!type) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << TString{RowSpecAttrType}.Quote()
            << " option is mandatory for " << TYqlRowSpec::CallableName()));
        return false;
    }
    if (sortedBy.size() != sortDirectionsCount) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << TString{RowSpecAttrSortDirections}.Quote()
            << " should have the same size as " << TString{RowSpecAttrSortedBy}.Quote()));
        return false;
    }
    if (sortedBy.size() != sortedByTypes.size()) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << TString{RowSpecAttrSortedByTypes}.Quote()
            << " should have the same size as " << TString{RowSpecAttrSortedBy}.Quote()));
        return false;
    }
    if (sortMembers.size() > sortedBy.size()) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << TString{RowSpecAttrSortMembers}.Quote()
            << " should have the size not greater than " << TString{RowSpecAttrSortedBy}.Quote() << " size"));
        return false;
    }
    for (auto& field: sortMembers) {
        if (!type->FindItem(field)) {
            ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << TString{RowSpecAttrSortMembers}.Quote()
                << " uses unknown field " << field.Quote()));
            return false;
        }
    }
    for (size_t i: xrange(sortedBy.size())) {
        if (auto ndx = type->FindItem(sortedBy[i])) {
            if (!IsSameAnnotation(*type->GetItems()[*ndx]->GetItemType(), *sortedByTypes[i])) {
                ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << TString{RowSpecAttrSortedByTypes}.Quote()
                    << " for " << sortedBy[i].Quote() << " field uses unequal type"));
                return false;
            }
        }
    }
    for (auto& field: defaultNames) {
        if (!type->FindItem(field)) {
            ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << TString{RowSpecAttrDefaultValues}.Quote()
                << " uses unknown field " << TString{field}.Quote()));
            return false;
        }
    }
    for (auto& field: explicitYson) {
        if (!type->FindItem(field)) {
            ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << TString{RowSpecAttrExplicitYson}.Quote()
                << " uses unknown field " << field.Quote()));
            return false;
        }
    }
    if (!strict && extendNonStrict) {
        auto items = type->GetItems();
        auto dictType = ctx.MakeType<TDictExprType>(
            ctx.MakeType<TDataExprType>(EDataSlot::String),
            ctx.MakeType<TDataExprType>(EDataSlot::String));
        items.push_back(ctx.MakeType<TItemExprType>(YqlOthersColumnName, dictType));
        if (columnOrder) {
            columnOrder->AddColumn(TString(YqlOthersColumnName));
        }
        type = ctx.MakeType<TStructExprType>(items);
    }

    return true;
}

bool TYqlRowSpecInfo::Validate(TExprContext& ctx, TPositionHandle positionHandle) {
    auto pos = ctx.GetPosition(positionHandle);
    if (SortedBy.size() != SortDirections.size()) {
        ctx.AddError(TIssue(pos, TStringBuilder() << TString{RowSpecAttrSortDirections}.Quote()
            << " should have the same size as " << TString{RowSpecAttrSortedBy}.Quote()));
        return false;
    }
    if (SortedBy.size() != SortedByTypes.size()) {
        ctx.AddError(TIssue(pos, TStringBuilder() << TString{RowSpecAttrSortedByTypes}.Quote()
            << " should have the same size as " << TString{RowSpecAttrSortedBy}.Quote()));
        return false;
    }
    if (SortMembers.size() > SortedBy.size()) {
        ctx.AddError(TIssue(pos, TStringBuilder() << TString{RowSpecAttrSortMembers}.Quote()
            << " should have the size not greater than " << TString{RowSpecAttrSortedBy}.Quote() << " size"));
        return false;
    }
    for (auto& field: SortMembers) {
        if (!Type->FindItem(field)) {
            ctx.AddError(TIssue(pos, TStringBuilder() << TString{RowSpecAttrSortMembers}.Quote()
                << " uses unknown field " << field.Quote()));
            return false;
        }
    }
    for (size_t i: xrange(SortedBy.size())) {
        if (auto ndx = Type->FindItem(SortedBy[i])) {
            if (!IsSameAnnotation(*Type->GetItems()[*ndx]->GetItemType(), *SortedByTypes[i])) {
                ctx.AddError(TIssue(pos, TStringBuilder() << TString{RowSpecAttrSortedByTypes}.Quote()
                    << " for " << SortedBy[i].Quote() << " field uses unequal type"));
                return false;
            }
        }
    }
    for (auto& field: DefaultValues) {
        if (!Type->FindItem(field.first)) {
            ctx.AddError(TIssue(pos, TStringBuilder() << TString{RowSpecAttrDefaultValues}.Quote()
                << " uses unknown field " << field.first.Quote()));
            return false;
        }
    }
    return true;
}

void TYqlRowSpecInfo::Parse(NNodes::TExprBase node, bool withTypes) {
    *this = {};
    FromNode = node;
    for (auto child: node.Cast<TYqlRowSpec>()) {
        auto setting = child.Cast<TCoNameValueTuple>();

        if (setting.Name().Value() == RowSpecAttrNativeYtTypeFlags) {
            NativeYtTypeFlags = FromString<ui64>(setting.Value().Cast<TCoAtom>().Value());
        } else if (setting.Name().Value() == RowSpecAttrUseNativeYtTypes) {
            NativeYtTypeFlags = FromString<bool>(setting.Value().Cast<TCoAtom>().Value()) ? NTCF_LEGACY : NTCF_NONE;
        } else if (setting.Name().Value() == RowSpecAttrUseTypeV2) {
            NativeYtTypeFlags = FromString<bool>(setting.Value().Cast<TCoAtom>().Value()) ? NTCF_LEGACY : NTCF_NONE;
        } else if (setting.Name().Value() == RowSpecAttrStrictSchema) {
            StrictSchema = FromString<bool>(setting.Value().Cast<TCoAtom>().Value());
        } else if (setting.Name().Value() == RowSpecAttrUniqueKeys) {
            UniqueKeys = FromString<bool>(setting.Value().Cast<TCoAtom>().Value());
        } else if (setting.Name().Value() == RowSpecAttrType) {
            auto& val = setting.Value().Cast().Ref();
            if (withTypes) {
                if (val.Type() == TExprNode::Atom) {
                    TypeNode = NYT::NodeFromYsonString(val.Content());
                    Columns = TColumnOrder(NCommon::ExtractColumnOrderFromYsonStructType(TypeNode));
                }
                Type = node.Ref().GetTypeAnn()->Cast<TStructExprType>();
            }
        } else if (setting.Name().Value() == RowSpecAttrConstraints) {
            ConstraintsNode = NYT::NodeFromYsonString(setting.Value().Cast().Ref().Content());
        } else if (setting.Name().Value() == RowSpecAttrSortedBy) {
            for (auto item: setting.Value().Cast<TCoAtomList>()) {
                SortedBy.push_back(TString{item.Value()});
            }
        } else if (setting.Name().Value() == RowSpecAttrSortMembers) {
            for (auto item: setting.Value().Cast<TCoAtomList>()) {
                SortMembers.push_back(TString{item.Value()});
            }
        } else if (setting.Name().Value() == RowSpecAttrSortDirections) {
            for (auto item: setting.Value().Cast<TExprList>()) {
                SortDirections.push_back(TStringBuf("true") == item.Cast<TCoBool>().Literal().Value());
            }
        } else if (setting.Name().Value() == RowSpecAttrSortedByTypes) {
            for (auto item: setting.Value().Cast<TExprList>()) {
                SortedByTypes.push_back(withTypes ? item.Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType() : nullptr);
            }
        } else if (setting.Name().Value() == RowSpecAttrDefaultValues) {
            for (auto item: setting.Value().Cast<TExprList>()) {
                auto atomList = item.Cast<TCoAtomList>();
                DefaultValues[TString{atomList.Item(0).Value()}] = TString{atomList.Item(1).Value()};
            }
        } else if (setting.Name().Value() == RowSpecAttrExplicitYson) {
            for (auto item: setting.Value().Cast<TCoAtomList>()) {
                ExplicitYson.emplace_back(item.Value());
            }
        } else {
            YQL_ENSURE(false, "Unexpected option " << setting.Name().Value());
        }
    }
    if (Columns && !StrictSchema) {
        Columns->AddColumn(TString(YqlOthersColumnName));
    }
}

ui64 TYqlRowSpecInfo::GetNativeYtTypeFlags(const NCommon::TStructMemberMapper& mapper) const {
    return mapper ? (NativeYtTypeFlags & NYql::GetNativeYtTypeFlags(*Type, mapper)) : NativeYtTypeFlags;
}

NYT::TNode TYqlRowSpecInfo::GetTypeNode(const NCommon::TStructMemberMapper& mapper) const {
    if (!TypeNode.IsUndefined()) {
        if (!mapper) {
            return TypeNode;
        }
        YQL_ENSURE(TypeNode.IsList() && TypeNode.Size() == 2 && TypeNode[0].IsString() && TypeNode[0].AsString() == "StructType" && TypeNode[1].IsList());

        NYT::TNode members = NYT::TNode::CreateList();
        for (auto& member : TypeNode[1].AsList()) {
            YQL_ENSURE(member.IsList() && member.Size() == 2 && member[0].IsString());

            if (auto name = mapper(member[0].AsString())) {
                members.Add(NYT::TNode::CreateList().Add(*name).Add(member[1]));
            }
        }
        return NYT::TNode::CreateList().Add("StructType").Add(members);
    }
    NYT::TNode typeNode;
    NYT::TNodeBuilder nodeBuilder(&typeNode);
    NCommon::SaveStructTypeToYson(nodeBuilder, Type, Columns, mapper);
    return typeNode;
}

void TYqlRowSpecInfo::SetType(const TStructExprType* type, TMaybe<ui64> nativeYtTypeFlags) {
    Type = type;
    Columns = {};
    TypeNode = {};
    if (nativeYtTypeFlags) {
        NativeYtTypeFlags = *nativeYtTypeFlags;
    }
    NativeYtTypeFlags &= NYql::GetNativeYtTypeFlags(*Type);
}

void TYqlRowSpecInfo::SetColumnOrder(const TMaybe<TColumnOrder>& columns) {
    TypeNode = {};
    Columns = columns;
}

TString TYqlRowSpecInfo::ToYsonString() const {
    NYT::TNode attrs = NYT::TNode::CreateMap();
    FillCodecNode(attrs[YqlRowSpecAttribute]);
    return NYT::NodeToCanonicalYsonString(attrs);
}

void TYqlRowSpecInfo::CopyTypeOrders(const NYT::TNode& typeNode) {
    YQL_ENSURE(Type);
    if (!TypeNode.IsUndefined() || 0 == NativeYtTypeFlags) {
        return;
    }

    YQL_ENSURE(typeNode.IsList() && typeNode.Size() == 2 && typeNode[0].IsString() && typeNode[0].AsString() == "StructType" && typeNode[1].IsList());

    THashMap<TString, NYT::TNode> fromMembers;
    for (auto& member : typeNode[1].AsList()) {
        YQL_ENSURE(member.IsList() && member.Size() == 2 && member[0].IsString());

        fromMembers.emplace(member[0].AsString(), member[1]);
    }

    NYT::TNode members = NYT::TNode::CreateList();
    TColumnOrder columns;
    if (Columns.Defined() && Columns->Size() == Type->GetSize()) {
        columns = *Columns;
    } else {
        for (auto& item : Type->GetItems()) {
            columns.AddColumn(TString(item->GetName()));
        }
    }
    for (auto& [name, gen_name]: columns) {
        if (!StrictSchema && name == YqlOthersColumnName) {
            continue;
        }
        auto origType = Type->FindItemType(name);
        YQL_ENSURE(origType);
        auto origTypeNode = NCommon::TypeToYsonNode(origType);
        auto it = fromMembers.find(name);
        if (it == fromMembers.end() || !NCommon::EqualsYsonTypesIgnoreStructOrder(origTypeNode, it->second)) {
            members.Add(NYT::TNode::CreateList().Add(name).Add(origTypeNode));
        } else {
            members.Add(NYT::TNode::CreateList().Add(name).Add(it->second));
        }
    }

    TypeNode = NYT::TNode::CreateList().Add("StructType").Add(members);
}

void TYqlRowSpecInfo::FillTypeTransform(NYT::TNode& attrs, TStringBuf typeNameAttr, const NCommon::TStructMemberMapper& mapper) const {
    if (!TypeNode.IsUndefined()) {
        YQL_ENSURE(TypeNode.IsList() && TypeNode.Size() == 2 && TypeNode[0].IsString() && TypeNode[0].AsString() == "StructType" && TypeNode[1].IsList());

        NYT::TNode members = NYT::TNode::CreateList();
        for (auto& member : TypeNode[1].AsList()) {
            YQL_ENSURE(member.IsList() && member.Size() == 2 && member[0].IsString());

            if (auto name = mapper(member[0].AsString())) {
                members.Add(NYT::TNode::CreateList().Add(*name).Add(member[1]));
            }
        }
        attrs[typeNameAttr] = NYT::TNode::CreateList().Add("StructType").Add(members);

    } else {
        NYT::TNodeBuilder specAttrBuilder(&attrs[typeNameAttr]);
        NCommon::SaveStructTypeToYson(specAttrBuilder, Type, Columns, mapper);
    }
}

void TYqlRowSpecInfo::FillSort(NYT::TNode& attrs, const NCommon::TStructMemberMapper& mapper) const {
    TVector<bool> sortDirections;
    TVector<TString> sortedBy;
    TVector<TString> sortMembers;
    TTypeAnnotationNode::TListType sortedByTypes;

    bool curUniqueKeys = UniqueKeys;
    const TVector<bool>* curSortDirections = &SortDirections;
    const TVector<TString>* curSortedBy = &SortedBy;
    const TVector<TString>* curSortMembers = &SortMembers;
    const TTypeAnnotationNode::TListType* curSortedByTypes = &SortedByTypes;
    if (mapper) {
        sortDirections = SortDirections;
        sortedBy = SortedBy;
        sortMembers = SortMembers;
        sortedByTypes = SortedByTypes;

        curSortDirections = &sortDirections;
        curSortedBy = &sortedBy;
        curSortMembers = &sortMembers;
        curSortedByTypes = &sortedByTypes;
        for (size_t i = 0; i < sortedBy.size(); ++i) {
            if (Type->FindItem(sortedBy[i])) {
                if (auto name = mapper(sortedBy[i])) {
                    sortedBy[i] = TString{*name};
                    if (i < sortMembers.size()) {
                        sortMembers[i] = sortedBy[i];
                    }
                } else {
                    if (i < sortMembers.size()) {
                        sortMembers.erase(sortMembers.begin() + i, sortMembers.end());
                    }
                    sortedBy.erase(sortedBy.begin() + i, sortedBy.end());
                    sortedByTypes.erase(sortedByTypes.begin() + i, sortedByTypes.end());
                    sortDirections.erase(sortDirections.begin() + i, sortDirections.end());
                    curUniqueKeys = false;
                    break;
                }
            }
        }
    }
    if (!curSortedBy->empty()) {
        attrs[RowSpecAttrUniqueKeys] = curUniqueKeys;
    }

    if (!curSortDirections->empty()) {
        auto list = NYT::TNode::CreateList();
        for (bool dir: *curSortDirections) {
            list.Add(dir ? 1 : 0);
        }
        attrs[RowSpecAttrSortDirections] = list;
    }

    auto saveColumnList = [&attrs] (TStringBuf name, const TVector<TString>& columns) {
        if (!columns.empty()) {
            auto list = NYT::TNode::CreateList();
            for (const auto& item : columns) {
                list.Add(item);
            }
            attrs[name] = list;
        }
    };

    saveColumnList(RowSpecAttrSortMembers, *curSortMembers);
    saveColumnList(RowSpecAttrSortedBy, *curSortedBy);

    if (!curSortedByTypes->empty()) {
        auto list = NYT::TNode::CreateList();
        for (auto type: *curSortedByTypes) {
            list.Add(NCommon::TypeToYsonNode(type));
        }
        attrs[RowSpecAttrSortedByTypes] = list;
    }
}

void TYqlRowSpecInfo::FillDefValues(NYT::TNode& attrs, const NCommon::TStructMemberMapper& mapper) const {
    if (!DefaultValues.empty()) {
        auto map = NYT::TNode::CreateMap();
        if (mapper) {
            for (const auto& val: DefaultValues) {
                if (auto name = mapper(val.first)) {
                    map[*name] = NYT::NodeToYsonString(NYT::TNode(val.second));
                }
            }
        } else {
            for (const auto& val: DefaultValues) {
                map[val.first] = NYT::NodeToYsonString(NYT::TNode(val.second));
            }
        }
        if (!map.AsMap().empty()) {
            attrs[RowSpecAttrDefaultValues] = map;
        }
    }
}

void TYqlRowSpecInfo::FillFlags(NYT::TNode& attrs) const {
    attrs[RowSpecAttrStrictSchema] = StrictSchema;
    attrs[RowSpecAttrNativeYtTypeFlags] = NativeYtTypeFlags;
    // Backward compatibility. TODO: remove after releasing compatibility flags
    if (NativeYtTypeFlags != 0) {
        attrs[RowSpecAttrUseNativeYtTypes] = true;
    }
}

void TYqlRowSpecInfo::FillExplicitYson(NYT::TNode& attrs, const NCommon::TStructMemberMapper& mapper) const {
    TVector<TString> localExplicitYson;
    const TVector<TString>* curExplicitYson = &ExplicitYson;
    if (mapper) {
        for (size_t i = 0; i < ExplicitYson.size(); ++i) {
            if (Type->FindItem(ExplicitYson[i])) {
                if (auto name = mapper(ExplicitYson[i])) {
                    localExplicitYson.emplace_back(TString{*name});
                }
            }
        }
        curExplicitYson = &localExplicitYson;
    }
    if (!curExplicitYson->empty()) {
        auto list = NYT::TNode::CreateList();
        for (const auto& item : *curExplicitYson) {
            list.Add(item);
        }
        attrs[RowSpecAttrExplicitYson] = list;
    }
}

void TYqlRowSpecInfo::FillCodecNode(NYT::TNode& attrs, const NCommon::TStructMemberMapper& mapper) const {
    attrs = NYT::TNode::CreateMap();

    attrs[RowSpecAttrType] = GetTypeNode(mapper);
    FillSort(attrs, mapper);
    FillDefValues(attrs, mapper);
    FillFlags(attrs);
    FillExplicitYson(attrs, mapper);
}

void TYqlRowSpecInfo::FillAttrNode(NYT::TNode& attrs, ui64 nativeTypeCompatibility, bool useCompactForm) const {
    attrs = NYT::TNode::CreateMap();

    if (!useCompactForm) {
        auto otherFilter = [strict = StrictSchema](TStringBuf name) -> TMaybe<TStringBuf> {
            if (!strict && name == YqlOthersColumnName) {
                return Nothing();
            }
            return MakeMaybe(name);
        };
        attrs[RowSpecAttrType] = GetTypeNode(otherFilter);
    }

    THashSet<TStringBuf> patchedFields;
    for (auto item: Type->GetItems()) {
        const TTypeAnnotationNode* itemType = item->GetItemType();
        // Top-level strict Yson is converted to Yson? in YT schema
        if (itemType->GetKind() == ETypeAnnotationKind::Data && itemType->Cast<TDataExprType>()->GetSlot() == EDataSlot::Yson) {
            patchedFields.insert(item->GetName());
        } else {
            if (itemType->GetKind() == ETypeAnnotationKind::Optional) {
                itemType = itemType->Cast<TOptionalExprType>()->GetItemType();
            }
            auto flags = GetNativeYtTypeFlagsImpl(itemType);
            if (flags != (flags & NativeYtTypeFlags & nativeTypeCompatibility)) {
                patchedFields.insert(item->GetName());
            }
        }
    }

    attrs[RowSpecAttrTypePatch] = GetTypeNode([&patchedFields](TStringBuf name) -> TMaybe<TStringBuf> {
        return patchedFields.contains(name) ?  MakeMaybe(name) : Nothing();
    });

    if (!useCompactForm || HasAuxColumns() || AnyOf(SortedBy, [&patchedFields](const auto& name) { return patchedFields.contains(name); } )) {
        FillSort(attrs);
    }
    FillDefValues(attrs);
    FillFlags(attrs);
    FillConstraints(attrs);
}

NNodes::TExprBase TYqlRowSpecInfo::ToExprNode(TExprContext& ctx, const TPositionHandle& pos) const {
    auto rowSpecBuilder = Build<TYqlRowSpec>(ctx, pos);

    auto otherFilter = [strict = StrictSchema](TStringBuf name) -> TMaybe<TStringBuf> {
        if (!strict && name == YqlOthersColumnName) {
            return Nothing();
        }
        return MakeMaybe(name);
    };
    rowSpecBuilder
        .Add()
            .Name()
                .Value(RowSpecAttrNativeYtTypeFlags, TNodeFlags::Default)
            .Build()
            .Value<TCoAtom>()
                .Value(ToString(NativeYtTypeFlags), TNodeFlags::Default)
            .Build()
        .Build()
        .Add()
            .Name()
                .Value(RowSpecAttrStrictSchema, TNodeFlags::Default)
            .Build()
            .Value<TCoAtom>()
                .Value(StrictSchema ? TStringBuf("1") : TStringBuf("0"), TNodeFlags::Default)
            .Build()
        .Build()
        .Add()
            .Name()
                .Value(RowSpecAttrUniqueKeys, TNodeFlags::Default)
            .Build()
            .Value<TCoAtom>()
                .Value(UniqueKeys ? TStringBuf("1") : TStringBuf("0"), TNodeFlags::Default)
            .Build()
        .Build()
        .Add()
            .Name()
                .Value(RowSpecAttrType, TNodeFlags::Default)
            .Build()
            .Value<TCoAtom>()
                .Value(NYT::NodeToYsonString(GetTypeNode(otherFilter), NYson::EYsonFormat::Text), TNodeFlags::MultilineContent)
            .Build()
        .Build();

    if (ConstraintsNode.HasValue() || HasNonTrivialSort() || Unique || Distinct) {
        rowSpecBuilder.Add()
            .Name()
                .Value(RowSpecAttrConstraints, TNodeFlags::Default)
            .Build()
            .Value<TCoAtom>()
                .Value(NYT::NodeToYsonString(GetConstraintsNode(), NYson::EYsonFormat::Text), TNodeFlags::MultilineContent)
            .Build()
        .Build();
    }

    if (!SortDirections.empty()) {
        auto listBuilder = Build<TExprList>(ctx, pos);
        for (bool dir: SortDirections) {
            listBuilder.Add<TCoBool>()
                .Literal<TCoAtom>()
                    .Value(dir ? TStringBuf("true") : TStringBuf("false"), TNodeFlags::Default)
                .Build()
            .Build();
        }
        rowSpecBuilder
            .Add()
                .Name()
                    .Value(RowSpecAttrSortDirections, TNodeFlags::Default)
                .Build()
                .Value(listBuilder.Done())
            .Build();
    }

    auto saveColumnList = [&] (TStringBuf name, const TVector<TString>& columns) {
        if (!columns.empty()) {
            auto listBuilder = Build<TExprList>(ctx, pos);
            for (auto& column: columns) {
                listBuilder.Add<TCoAtom>()
                    .Value(column)
                    .Build();
            }
            rowSpecBuilder
                .Add()
                    .Name()
                        .Value(name, TNodeFlags::Default)
                    .Build()
                    .Value(listBuilder.Done())
                .Build();
        }
    };

    saveColumnList(RowSpecAttrSortMembers, SortMembers);
    saveColumnList(RowSpecAttrSortedBy, SortedBy);

    if (!SortedByTypes.empty()) {
        auto listBuilder = Build<TExprList>(ctx, pos);
        for (auto type: SortedByTypes) {
            listBuilder.Add(TExprBase(NCommon::BuildTypeExpr(pos, *type, ctx)));
        }
        rowSpecBuilder
            .Add()
                .Name()
                    .Value(RowSpecAttrSortedByTypes, TNodeFlags::Default)
                .Build()
                .Value(listBuilder.Done())
            .Build();
    }

    if (!DefaultValues.empty()) {
        auto listBuilder = Build<TExprList>(ctx, pos);
        for (const auto& val: DefaultValues) {
            listBuilder.Add<TCoAtomList>()
                .Add().Value(val.first).Build()
                .Add().Value(val.second).Build()
            .Build();
        }
        rowSpecBuilder
            .Add()
                .Name()
                    .Value(RowSpecAttrDefaultValues, TNodeFlags::Default)
                .Build()
                .Value(listBuilder.Done())
            .Build();
    }
    saveColumnList(RowSpecAttrExplicitYson, ExplicitYson);

    return rowSpecBuilder.Done();
}

bool TYqlRowSpecInfo::HasAuxColumns() const {
    for (auto& x: SortedBy) {
        if (!Type->FindItem(x)) {
            return true;
        }
    }
    return false;
}

TVector<std::pair<TString, const TTypeAnnotationNode*>> TYqlRowSpecInfo::GetAuxColumns() const {
    TVector<std::pair<TString, const TTypeAnnotationNode*>> res;
    for (size_t i: xrange(SortedBy.size())) {
        if (!Type->FindItem(SortedBy[i])) {
            res.emplace_back(SortedBy[i], SortedByTypes[i]);
        }
    }
    return res;
}

const TStructExprType* TYqlRowSpecInfo::GetExtendedType(TExprContext& ctx) const {
    if (!IsSorted()) {
        return Type;
    }
    bool extended = false;
    TVector<const TItemExprType*> items = Type->GetItems();
    for (size_t i: xrange(SortedBy.size())) {
        if (!Type->FindItem(SortedBy[i])) {
            items.push_back(ctx.MakeType<TItemExprType>(SortedBy[i], SortedByTypes[i]));
            extended = true;
        }
    }
    return extended ? ctx.MakeType<TStructExprType>(items) : Type;
}

bool TYqlRowSpecInfo::CopySortness(const TYqlRowSpecInfo& from, ECopySort mode) {
    SortDirections = from.SortDirections;
    SortMembers = from.SortMembers;
    SortedBy = from.SortedBy;
    SortedByTypes = from.SortedByTypes;
    UniqueKeys = from.UniqueKeys;
    bool sortIsChanged = false;
    if (ECopySort::Exact != mode) {
        YQL_ENSURE(SortMembers.size() <= SortedBy.size());
        for (size_t i = 0; i < SortMembers.size(); ++i) {
            const auto itemNdx = Type->FindItem(SortMembers[i]);
            if (!itemNdx || (SortedBy[i] == SortMembers[i] && Type->GetItems()[*itemNdx]->GetItemType() != SortedByTypes[i])) {
                sortIsChanged = ClearSortness(i);
                break;
            } else if (ECopySort::Pure == mode && SortedBy[i] != SortMembers[i]) {
                sortIsChanged = ClearSortness(i);
                break;
            }
        }
        if (ECopySort::WithCalc != mode) {
            if (SortMembers.size() < SortedBy.size()) {
                sortIsChanged = ClearSortness(SortMembers.size()) || sortIsChanged;
            }
        }
    }
    return sortIsChanged;
}

void TYqlRowSpecInfo::CopyConstraints(const TYqlRowSpecInfo& from) {
    ConstraintsNode = from.ConstraintsNode;
    Sorted = from.Sorted;
    Unique = from.Unique;
    Distinct = from.Distinct;
}

bool TYqlRowSpecInfo::KeepPureSortOnly() {
    bool sortIsChanged = false;
    for (size_t i = 0; i < SortMembers.size(); ++i) {
        if (!Type->FindItem(SortMembers[i])) {
            sortIsChanged = ClearSortness(i);
            break;
        } else if (SortedBy[i] != SortMembers[i]) {
            sortIsChanged = ClearSortness(i);
            break;
        }
    }
    if (SortMembers.size() < SortedBy.size()) {
        sortIsChanged = ClearSortness(SortMembers.size()) || sortIsChanged;
    }
    return sortIsChanged;
}

bool TYqlRowSpecInfo::ClearNativeDescendingSort() {
    for (size_t i = 0; i < SortDirections.size(); ++i) {
        if (!SortDirections[i] && Type->FindItem(SortedBy[i])) {
            return ClearSortness(i);
        }
    }
    return false;
}

bool TYqlRowSpecInfo::MakeCommonSortness(const TYqlRowSpecInfo& from) {
    bool sortIsChanged = false;
    UniqueKeys = false; // Merge of two and more tables cannot have unique keys
    const size_t resultSize = Min<size_t>(SortMembers.size(), from.SortMembers.size()); // Truncate all calculated columns
    if (SortedBy.size() > resultSize) {
        sortIsChanged = ClearSortness(resultSize);
    }
    for (size_t i = 0; i < resultSize; ++i) {
        if (SortMembers[i] != from.SortMembers[i] || SortedBy[i] != from.SortedBy[i] || SortedByTypes[i] != from.SortedByTypes[i] || SortDirections[i] != from.SortDirections[i]) {
            sortIsChanged = ClearSortness(i) || sortIsChanged;
            break;
        }
    }
    return sortIsChanged;
}

bool TYqlRowSpecInfo::CompareSortness(const TYqlRowSpecInfo& with, bool checkUniqueFlag) const {
    return SortDirections == with.SortDirections
        && SortMembers == with.SortMembers
        && SortedBy == with.SortedBy
        && SortedByTypes.size() == with.SortedByTypes.size()
        && std::equal(SortedByTypes.cbegin(), SortedByTypes.cend(), with.SortedByTypes.cbegin(), TTypeAnnotationNode::TEqual())
        && (!checkUniqueFlag || UniqueKeys == with.UniqueKeys);
}

bool TYqlRowSpecInfo::ClearSortness(size_t fromMember) {
    if (fromMember <= SortMembers.size()) {
        SortMembers.erase(SortMembers.begin() + fromMember, SortMembers.end());
        SortedBy.erase(SortedBy.begin() + fromMember, SortedBy.end());
        SortedByTypes.erase(SortedByTypes.begin() + fromMember, SortedByTypes.end());
        SortDirections.erase(SortDirections.begin() + fromMember, SortDirections.end());
        UniqueKeys = false;
        return true;
    }
    return false;
}

const TSortedConstraintNode* TYqlRowSpecInfo::MakeSortConstraint(TExprContext& ctx) const {
    if (!SortMembers.empty()) {
        TSortedConstraintNode::TContainerType sorted;
        for (auto i = 0U; i < SortMembers.size(); ++i) {
            const auto column = ctx.AppendString(SortMembers[i]);
            sorted.emplace_back(TPartOfConstraintBase::TSetType{TPartOfConstraintBase::TPathType{column}}, i >= SortDirections.size() || SortDirections[i]);
        }
        return ctx.MakeConstraint<TSortedConstraintNode>(std::move(sorted));
    }
    return nullptr;
}

const TDistinctConstraintNode* TYqlRowSpecInfo::MakeDistinctConstraint(TExprContext& ctx) const {
    if (UniqueKeys && !SortMembers.empty() && SortedBy.size() == SortMembers.size()) {
        std::vector<std::string_view> uniqColumns(SortMembers.size());
        std::transform(SortMembers.cbegin(), SortMembers.cend(), uniqColumns.begin(), std::bind(&TExprContext::AppendString, std::ref(ctx), std::placeholders::_1));
        return ctx.MakeConstraint<TDistinctConstraintNode>(uniqColumns);
    }
    return nullptr;
}

TVector<std::pair<TString, bool>> TYqlRowSpecInfo::GetForeignSort() const {
    TVector<std::pair<TString, bool>> res;
    for (size_t i = 0; i < SortedBy.size(); ++i) {
        res.emplace_back(SortedBy[i], Type->FindItem(SortedBy[i]) ? SortDirections.at(i) : true);
    }
    return res;
}

void TYqlRowSpecInfo::SetConstraints(const TConstraintSet& constraints) {
    ConstraintsNode.Clear();
    Sorted = constraints.GetConstraint<TSortedConstraintNode>();
    Unique = constraints.GetConstraint<TUniqueConstraintNode>();
    Distinct = constraints.GetConstraint<TDistinctConstraintNode>();
}

TConstraintSet TYqlRowSpecInfo::GetConstraints() const {
    TConstraintSet set;
    if (Sorted)
        set.AddConstraint(Sorted);
    if (Unique)
        set.AddConstraint(Unique);
    if (Distinct)
        set.AddConstraint(Distinct);
    return set;
}

}
