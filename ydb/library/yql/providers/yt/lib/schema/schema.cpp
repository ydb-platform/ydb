#include "schema.h"

#include <ydb/library/yql/providers/yt/common/yql_names.h>
#include <ydb/library/yql/providers/common/codec/yql_codec_type_flags.h>
#include <ydb/library/yql/providers/common/schema/expr/yql_expr_schema.h>

#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/yson/node/node_builder.h>

#include <util/string/cast.h>
#include <util/generic/yexception.h>
#include <util/generic/hash_set.h>
#include <util/generic/set.h>
#include <util/generic/hash.h>


namespace NYql {

static TString ConvertYtDataType(const TString& ytType, ui64& nativeYtTypeFlags) {
    TString yqlType;
    if (ytType == "string") {
        yqlType = "String";
    }
    else if (ytType == "utf8") {
        yqlType = "Utf8";
    }
    else if (ytType == "int64") {
        yqlType = "Int64";
    }
    else if (ytType == "uint64") {
        yqlType = "Uint64";
    }
    else if (ytType == "int32") {
        yqlType = "Int32";
    }
    else if (ytType == "uint32") {
        yqlType = "Uint32";
    }
    else if (ytType == "int16") {
        yqlType = "Int16";
    }
    else if (ytType == "uint16") {
        yqlType = "Uint16";
    }
    else if (ytType == "int8") {
        yqlType = "Int8";
    }
    else if (ytType == "uint8") {
        yqlType = "Uint8";
    }
    else if (ytType == "double") {
        yqlType = "Double";
    }
    else if (ytType == "float") {
        nativeYtTypeFlags |= NTCF_FLOAT;
        yqlType = "Float";
    }
    else if (ytType == "boolean") { // V2
        yqlType = "Bool";
    }
    else if (ytType == "bool") { // V3
        yqlType = "Bool";
    }
    else if (ytType == "date") {
        nativeYtTypeFlags |= NTCF_DATE;
        yqlType = "Date";
    }
    else if (ytType == "datetime") {
        nativeYtTypeFlags |= NTCF_DATE;
        yqlType = "Datetime";
    }
    else if (ytType == "timestamp") {
        nativeYtTypeFlags |= NTCF_DATE;
        yqlType = "Timestamp";
    }
    else if (ytType == "interval") {
        nativeYtTypeFlags |= NTCF_DATE;
        yqlType = "Interval";
    }
    else if (ytType == "date32") {
        nativeYtTypeFlags |= NTCF_BIGDATE;
        yqlType = "Date32";
    }
    else if (ytType == "datetime64") {
        nativeYtTypeFlags |= NTCF_BIGDATE;
        yqlType = "Datetime64";
    }
    else if (ytType == "timestamp64") {
        nativeYtTypeFlags |= NTCF_BIGDATE;
        yqlType = "Timestamp64";
    }
    else if (ytType == "interval64") {
        nativeYtTypeFlags |= NTCF_BIGDATE;
        yqlType = "Interval64";
    }
    else if (ytType == "yson") { // V3
        yqlType = "Yson";
    }
    else if (ytType == "json") {
        nativeYtTypeFlags |= NTCF_JSON;
        yqlType = "Json";
    }
    else if (ytType == "any") {
        yqlType = "Yson";
    } else {
        YQL_LOG_CTX_THROW yexception() << "Unknown type " << ytType.Quote() << " in yson schema";
    }

    return yqlType;
}

static NYT::TNode ConvertNativeYtType(const NYT::TNode& raw, bool root, bool& hasYson, ui64& nativeYtTypeFlags) {
    if (raw.IsString()) {
        if (raw.AsString() == "null") {
            nativeYtTypeFlags |= NTCF_NULL;
            return NYT::TNode().Add("NullType");
        }

        if (raw.AsString() == "void") {
            nativeYtTypeFlags |= NTCF_VOID;
            return NYT::TNode().Add("VoidType");
        }

        hasYson = hasYson || "yson" == raw.AsString();
        return NYT::TNode()
            .Add("DataType")
            .Add(ConvertYtDataType(raw.AsString(), nativeYtTypeFlags));
    }

    const auto& typeName = raw["type_name"].AsString();
    if (typeName == "decimal") {
        nativeYtTypeFlags |= NTCF_DECIMAL;
        return NYT::TNode()
            .Add("DataType")
            .Add("Decimal")
            .Add(ToString(raw["precision"].AsInt64()))
            .Add(ToString(raw["scale"].AsInt64()));
    } else if (typeName == "list") {
        nativeYtTypeFlags |= NTCF_COMPLEX;
        return NYT::TNode()
            .Add("ListType")
            .Add(ConvertNativeYtType(raw["item"], false, hasYson, nativeYtTypeFlags));
    } else if (typeName == "optional") {
        if (!root) {
            nativeYtTypeFlags |= NTCF_COMPLEX;
        }
        return NYT::TNode()
            .Add("OptionalType")
            .Add(ConvertNativeYtType(raw["item"], false, hasYson, nativeYtTypeFlags));
    } else if (typeName == "tuple") {
        nativeYtTypeFlags |= NTCF_COMPLEX;
        auto list = NYT::TNode::CreateList();
        for (const auto& x : raw["elements"].AsList()) {
            list.Add(ConvertNativeYtType(x["type"], false, hasYson, nativeYtTypeFlags));
        }
        return NYT::TNode()
            .Add("TupleType")
            .Add(list);
    } else if (typeName == "struct") {
        nativeYtTypeFlags |= NTCF_COMPLEX;
        auto list = NYT::TNode::CreateList();
        for (const auto& x : raw["members"].AsList()) {
            list.Add(NYT::TNode()
                .Add(x["name"].AsString())
                .Add(ConvertNativeYtType(x["type"], false, hasYson, nativeYtTypeFlags)));
        }
        return NYT::TNode()
            .Add("StructType")
            .Add(list);
    } else if (typeName == "variant") {
        nativeYtTypeFlags |= NTCF_COMPLEX;
        auto list = NYT::TNode::CreateList();
        if (raw.HasKey("elements")) {
            for (const auto& x : raw["elements"].AsList()) {
                list.Add(ConvertNativeYtType(x["type"], false, hasYson, nativeYtTypeFlags));
            }

            return NYT::TNode()
                .Add("VariantType")
                .Add(NYT::TNode()
                    .Add("TupleType")
                    .Add(list));
        } else {
            for (const auto& x : raw["members"].AsList()) {
                list.Add(NYT::TNode()
                    .Add(x["name"].AsString())
                    .Add(ConvertNativeYtType(x["type"], false, hasYson, nativeYtTypeFlags)));
            }

            return NYT::TNode()
                .Add("VariantType")
                .Add(NYT::TNode()
                    .Add("StructType")
                   .Add(list));
        }
    } else if (typeName == "tagged") {
        nativeYtTypeFlags |= NTCF_COMPLEX;
        auto tag = raw["tag"].AsString();
        if (tag == "_EmptyList") {
            return NYT::TNode().Add("EmptyListType");
        }

        if (tag == "_EmptyDict") {
            return NYT::TNode().Add("EmptyDictType");
        }

        if (tag == "_Void") {
            return NYT::TNode().Add("VoidType");
        }

        if (tag == "_Null") {
            return NYT::TNode().Add("NullType");
        }

        return NYT::TNode()
            .Add("TaggedType")
            .Add(tag)
            .Add(ConvertNativeYtType(raw["item"], false, hasYson, nativeYtTypeFlags));
    } else if (typeName == "dict") {
        nativeYtTypeFlags |= NTCF_COMPLEX;
        return NYT::TNode()
            .Add("DictType")
            .Add(ConvertNativeYtType(raw["key"], false, hasYson, nativeYtTypeFlags))
            .Add(ConvertNativeYtType(raw["value"], false, hasYson, nativeYtTypeFlags));
    } else {
        YQL_LOG_CTX_THROW yexception() << "Unknown metatype " << typeName.Quote() << " in yson schema";
    }
}

static std::pair<TString, NYT::TNode> ExtractYtType(const NYT::TNode& entry, bool strictSchema,
    THashSet<TString>& anyColumns, ui64& nativeYtTypeFlags) {
    TMaybe<NYT::TNode> nativeTypeRaw;
    TMaybe<TString> fieldName;
    TMaybe<TString> fieldType;
    bool nullable = true;
    bool hasYson = false;

    for (const auto& it : entry.AsMap()) {
        if (it.first == "name") {
            fieldName = it.second.AsString();
            if (!strictSchema && *fieldName == YqlOthersColumnName) {
                YQL_LOG_CTX_THROW yexception() << "Non-strict schema contains '_other' column, which conflicts with YQL virtual column";
            }
        }
        else if (it.first == "type") {
            fieldType = it.second.AsString();
            hasYson = hasYson || ("any" == fieldType);
        }
        else if (it.first == "nullable") {
            nullable = it.second.AsBool();
        }
        else if (it.first == "required") {
            nullable = !it.second.AsBool();
        }
        else if (it.first == "type_v3") {
            nativeTypeRaw = it.second;
        }
    }

    if (!fieldName) {
        YQL_LOG_CTX_THROW yexception() << "No 'name' in schema tuple";
    }
    if (!fieldType && !nativeTypeRaw) {
        YQL_LOG_CTX_THROW yexception() << "No 'type' or 'type_v3' in schema tuple";
    }

    if (nativeTypeRaw) {
        fieldType.Clear();
    }

    if (nativeTypeRaw && nativeTypeRaw->IsString()) {
        fieldType = nativeTypeRaw->AsString();
    }

    NYT::TNode typeNode;
    if (fieldType && fieldType != "null" && fieldType != "void" && fieldType != "decimal") {
        TString yqlType = ConvertYtDataType(*fieldType, nativeYtTypeFlags);
        auto dataTypeNode = NYT::TNode()
            .Add("DataType")
            .Add(yqlType);

        typeNode = nullable ? NYT::TNode()
            .Add("OptionalType")
            .Add(dataTypeNode) : dataTypeNode;

    } else {
        typeNode = ConvertNativeYtType(nativeTypeRaw ? *nativeTypeRaw : NYT::TNode(fieldType.GetOrElse({})), true, hasYson, nativeYtTypeFlags);
    }
    if (hasYson) {
        anyColumns.insert(*fieldName);
    }
    return { *fieldName, typeNode };
}

NYT::TNode YTSchemaToRowSpec(const NYT::TNode& schema, const TYTSortInfo* sortInfo) {
    NYT::TNode rowSpec;
    auto& rowType = rowSpec[RowSpecAttrType];
    rowType.Add("StructType");
    auto& resultTypes = rowType.Add();
    resultTypes = NYT::TNode::CreateList();

    const auto* strictAttr = schema.GetAttributes().AsMap().FindPtr("strict");
    const bool strictSchema = !strictAttr || NYT::GetBool(*strictAttr);
    rowSpec[RowSpecAttrStrictSchema] = static_cast<int>(strictSchema);

    THashSet<TString> anyColumns;
    NYT::TNode sortedBy = NYT::TNode::CreateList();
    NYT::TNode sortedByTypes;
    NYT::TNode sortDirections;
    ui64 nativeYtTypeFlags = 0;

    THashMap<TString, NYT::TNode> types;
    for (const auto& entry : schema.AsList()) {
        if (!entry.IsMap()) {
            YQL_LOG_CTX_THROW yexception() << "Invalid table schema: list element is not a map node";
        }

        auto ft = ExtractYtType(entry, strictSchema, anyColumns, nativeYtTypeFlags);
        types[ft.first] = ft.second;
    }
    rowSpec[RowSpecAttrNativeYtTypeFlags] = nativeYtTypeFlags;

    TYTSortInfo localSortInfo;
    if (!sortInfo) {
        localSortInfo = KeyColumnsFromSchema(schema);
        sortInfo = &localSortInfo;
    }
    bool uniqueKeys = sortInfo->Unique;
    for (const auto& field : sortInfo->Keys) {
        const auto& fieldName = field.first;
        auto* type = types.FindPtr(fieldName);
        if (!type) {
            uniqueKeys = false;
            break;
        }
        sortedBy.Add(fieldName);
        sortedByTypes.Add(*type);
        sortDirections.Add(field.second);
    }

    for (const auto& entry : schema.AsList()) {
        for (const auto& it : entry.AsMap()) {
            if (it.first != "name") {
                continue;
            }
            const auto& fieldName = it.second.AsString();
            auto* type = types.FindPtr(fieldName);
            if (!type) {
                continue;
            }
            if (YqlFakeColumnName != fieldName || schema.AsList().size() > 1) {
                resultTypes.Add(NYT::TNode()
                    .Add(fieldName)
                    .Add(*type));
            }
        }
    }

    if (!sortedBy.Empty()) {
        bool hasAnyInKey = false;
        for (const auto& key : sortedBy.AsList()) {
            if (anyColumns.contains(key.AsString())) {
                hasAnyInKey = true;
                break;
            }
        }

        if (!hasAnyInKey) {
            rowSpec[RowSpecAttrSortedBy] = sortedBy;
            rowSpec[RowSpecAttrSortedByTypes] = sortedByTypes;
            rowSpec[RowSpecAttrSortDirections] = sortDirections;
            rowSpec[RowSpecAttrSortMembers] = sortedBy;
            if (uniqueKeys) {
                rowSpec[RowSpecAttrUniqueKeys] = uniqueKeys;
            }
        }
    }

    return rowSpec;
}

NYT::TNode QB2PremapperToRowSpec(const NYT::TNode& qb2, const NYT::TNode& originalScheme) {
    NYT::TNode rowSpec;
    auto& rowType = rowSpec[RowSpecAttrType];
    rowType.Add("StructType");
    auto& resultTypes = rowType.Add();

    rowSpec[RowSpecAttrStrictSchema] = 1;

    THashSet<TString> anyColumns;
    THashMap<TString, NYT::TNode> types;
    THashSet<TString> passthroughFields;
    for (const auto& field: qb2["fields"].AsMap()) {
        auto ytType = field.second["type"].AsString();
        const auto& reqColumns = field.second["required_columns"].AsList();

        bool isNullable = true;
        if (field.second.HasKey("nullable")) {
            isNullable = field.second["nullable"].AsBool();
        }

        bool passthrough = false;
        if (field.second.HasKey("passthrough")) {
            passthrough = field.second["passthrough"].AsBool();
        }

        // check real field name
        if (passthrough && (reqColumns.size() != 1 || reqColumns[0].AsString() != field.first)) {
            passthrough = false;
        }

        if (passthrough) {
            passthroughFields.insert(field.first);
        }

        if ("yson" == ytType || "any" == ytType) {
            anyColumns.insert(field.first);
        }

        ui64 nativeYtTypeFlags = 0;
        TString yqlType = ConvertYtDataType(ytType, nativeYtTypeFlags);
        YQL_ENSURE(0 == nativeYtTypeFlags, "QB2 premapper with native YT types is not supported");
        auto dataTypeNode =  NYT::TNode()
            .Add("DataType")
            .Add(yqlType);

        auto typeNode = isNullable ? NYT::TNode()
            .Add("OptionalType")
            .Add(dataTypeNode) : dataTypeNode;

        types[field.first] = typeNode;
    }

    auto sortInfo = KeyColumnsFromSchema(originalScheme);

    // switch off sort if some key isn't marked as passtrough or has 'any' type
    for (auto& x: sortInfo.Keys) {
        if (!passthroughFields.contains(x.first) || anyColumns.contains(x.first)) {
            sortInfo.Keys.clear();
            break;
        }
    }

    NYT::TNode sortedBy = NYT::TNode::CreateList();
    NYT::TNode sortedByTypes;
    NYT::TNode sortDirections;

    for (const auto& field : sortInfo.Keys) {
        const auto& fieldName = field.first;
        auto* type = types.FindPtr(fieldName);
        if (!type) {
            sortInfo.Unique = false;
            break;
        }
        resultTypes.Add(NYT::TNode()
            .Add(fieldName)
            .Add(*type));

        sortedBy.Add(fieldName);
        sortedByTypes.Add(*type);
        sortDirections.Add(field.second);

        types.erase(fieldName);
    }

    for (auto entry: types) {
        resultTypes.Add(NYT::TNode()
            .Add(entry.first)
            .Add(entry.second));
    }

    if (!sortedBy.Empty()) {
        rowSpec[RowSpecAttrSortedBy] = sortedBy;
        rowSpec[RowSpecAttrSortedByTypes] = sortedByTypes;
        rowSpec[RowSpecAttrSortDirections] = sortDirections;
        rowSpec[RowSpecAttrSortMembers] = sortedBy;
        if (sortInfo.Unique) {
            rowSpec[RowSpecAttrUniqueKeys] = sortInfo.Unique;
        }
    }

    return rowSpec;
}

NYT::TNode GetSchemaFromAttributes(const NYT::TNode& attributes, bool onlySystem, bool ignoreWeakSchema) {
    NYT::TNode result;
    auto trySchema = [&] (const TStringBuf& attrName) -> TMaybe<NYT::TNode> {
        if (attributes.HasKey(attrName)) {
            const auto& attr = attributes[attrName];
            if (attr.IsList() && !attr.Empty()) {
                return attr;
            }
        }
        return Nothing();
    };

    auto readSchema = !onlySystem ? trySchema(READ_SCHEMA_ATTR_NAME) : Nothing();
    auto schema = trySchema(SCHEMA_ATTR_NAME);

    if (readSchema) {
        result[READ_SCHEMA_ATTR_NAME] = *readSchema;
    }

    if (schema) {
        if (!ignoreWeakSchema || !attributes.HasKey(SCHEMA_MODE_ATTR_NAME) ||
            attributes[SCHEMA_MODE_ATTR_NAME].AsString() != TStringBuf("weak")) {
            result[SCHEMA_ATTR_NAME] = *schema;
        }
    }
    return result;
}

TYTSortInfo KeyColumnsFromSchema(const NYT::TNode& schema) {
    TYTSortInfo result;
    for (const auto& entry : schema.AsList()) {
        if (!entry.IsMap()) {
            YQL_LOG_CTX_THROW yexception() << "Invalid schema: list element is not a map node";
        }

        TMaybe<TString> fieldName;
        TMaybe<int> sortDirection;

        for (const auto& it : entry.AsMap()) {
            if (it.first == "name") {
                fieldName = it.second.AsString();
            } else if (it.first == "sort_order") {
                if (!it.second.IsEntity()) {
                    if (!it.second.IsString()) {
                        YQL_LOG_CTX_THROW yexception() << "Invalid schema sort order: " << NYT::NodeToYsonString(entry);
                    }
                    if (it.second == "ascending") {
                        sortDirection = 1;
                    } else if (it.second == "descending") {
                        sortDirection = 0;
                    } else {
                        YQL_LOG_CTX_THROW yexception() << "Invalid schema sort order: " << NYT::NodeToYsonString(entry);
                    }
                }
            }
        }

        if (fieldName && sortDirection) {
            result.Keys.emplace_back(*fieldName, *sortDirection);
        }
    }
    result.Unique = schema.GetAttributes().HasKey("unique_keys") && NYT::GetBool(schema.GetAttributes()["unique_keys"]);
    return result;
}

bool ValidateTableSchema(
    const TString& tableName, const NYT::TNode& attributes,
    bool ignoreYamrDsv, bool ignoreWeakSchema
) {
    if (attributes.HasKey(YqlRowSpecAttribute)) {
        return true;
    }

    auto hasYTSchema = [&] (const TStringBuf& attrName) {
        if (attributes.HasKey(attrName)) {
            const auto& schema = attributes[attrName];
            if (!schema.IsList()) {
                YQL_LOG_CTX_THROW yexception() << "Invalid schema, " <<
                    tableName << "/@" << attrName << " is not a list node";
            }
            return !schema.Empty();
        }
        return false;
    };

    if (hasYTSchema(READ_SCHEMA_ATTR_NAME)) {
        return true;
    }

    if (hasYTSchema(SCHEMA_ATTR_NAME) && !(
            ignoreWeakSchema &&
            attributes.HasKey(SCHEMA_MODE_ATTR_NAME) &&
            attributes[SCHEMA_MODE_ATTR_NAME].AsString() == TStringBuf("weak")))
    {
        return true;
    }

    if (!ignoreYamrDsv && attributes.HasKey(FORMAT_ATTR_NAME) &&
        attributes[FORMAT_ATTR_NAME].AsString() == TStringBuf("yamred_dsv"))
    {
        return true;
    }

    return false;
}

void MergeInferredSchemeWithSort(NYT::TNode& schema, TYTSortInfo& sortInfo) {
    if (sortInfo.Keys.empty()) {
        return;
    }

    TSet<TString> keys;
    for (const auto& x : sortInfo.Keys) {
        keys.insert(x.first);
    }

    TSet<TString> unusedKeys = keys;
    for (const auto& column : schema.AsList()) {
        const auto& name = column["name"].AsString();
        unusedKeys.erase(name);
    }

    // add all unused keys with yson type, update sortness
    for (const auto& key : unusedKeys) {
        auto map = NYT::TNode::CreateMap();
        map["name"] = key;
        map["type"] = "any";
        schema.Add(map);
    }

    for (ui32 i = 0; i < sortInfo.Keys.size(); ++i) {
        if (unusedKeys.contains(sortInfo.Keys[i].first)) {
            sortInfo.Keys.erase(sortInfo.Keys.begin() + i, sortInfo.Keys.end());
            sortInfo.Unique = false;
            break;
        }
    }
}

std::pair<NYT::EValueType, bool> RowSpecYqlTypeToYtType(const NYT::TNode& rowSpecType, ui64 nativeYtTypeFlags) {
    const auto* type = &rowSpecType;

    while ((*type)[0] == TStringBuf("TaggedType")) {
        type = &(*type)[2];
    }

    if ((*type)[0] == TStringBuf("PgType")) {
        const auto& name = (*type)[1].AsString();
        NYT::EValueType ytType;
        if (name == "bool") {
            ytType = NYT::VT_BOOLEAN;
        } else if (name == "int2") {
            ytType = NYT::VT_INT16;
        } else if (name == "int4") {
            ytType = NYT::VT_INT32;
        } else if (name == "int8") {
            ytType = NYT::VT_INT64;
        } else if (name == "float8") {
            ytType = NYT::VT_DOUBLE;
        } else if (name == "float4") {
            ytType = (nativeYtTypeFlags & NTCF_FLOAT) ? NYT::VT_FLOAT : NYT::VT_DOUBLE;
        } else if (name == "text" || name == "varchar" || name == "cstring") {
            ytType = NYT::VT_UTF8;
        } else {
            ytType = NYT::VT_STRING;
        }

        return { ytType, false };
    }

    bool required = true;
    if ((*type)[0] == TStringBuf("OptionalType")) {
        type = &(*type)[1];
        required = false;
    }

    if ((*type)[0] == TStringBuf("NullType")) {
        return {NYT::VT_NULL, false};
    }

    if ((*type)[0] == TStringBuf("VoidType")) {
        return {NYT::VT_VOID, false};
    }

    if ((*type)[0] != TStringBuf("DataType")) {
        return {NYT::VT_ANY, false};
    }

    const auto& yqlType = (*type)[1].AsString();
    NYT::EValueType ytType;
    if (yqlType == TStringBuf("String") || yqlType == TStringBuf("Longint") || yqlType == TStringBuf("Uuid") || yqlType == TStringBuf("JsonDocument") || yqlType == TStringBuf("DyNumber")) {
        ytType = NYT::VT_STRING;
    } else if (yqlType == TStringBuf("Json")) {
        ytType = (nativeYtTypeFlags & NTCF_JSON) ? NYT::VT_JSON : NYT::VT_STRING;
    } else if (yqlType == TStringBuf("Decimal")) {
        ytType = NYT::VT_STRING;
    } else if (yqlType == TStringBuf("Utf8")) {
        ytType = NYT::VT_UTF8;
    } else if (yqlType == TStringBuf("Int64")) {
        ytType = NYT::VT_INT64;
    } else if (yqlType == TStringBuf("Interval")) {
        ytType = (nativeYtTypeFlags & NTCF_DATE) ? NYT::VT_INTERVAL : NYT::VT_INT64;
    } else if (yqlType == TStringBuf("Int32")) {
        ytType = NYT::VT_INT32;
    } else if (yqlType == TStringBuf("Int16")) {
        ytType = NYT::VT_INT16;
    } else if (yqlType == TStringBuf("Int8")) {
        ytType = NYT::VT_INT8;
    } else if (yqlType == TStringBuf("Uint64")) {
        ytType = NYT::VT_UINT64;
    } else if (yqlType == TStringBuf("Timestamp")) {
        ytType = (nativeYtTypeFlags & NTCF_DATE) ? NYT::VT_TIMESTAMP : NYT::VT_UINT64;
    } else if (yqlType == TStringBuf("Uint32")) {
        ytType = NYT::VT_UINT32;
    } else if (yqlType == TStringBuf("Datetime")) {
        ytType = (nativeYtTypeFlags & NTCF_DATE) ? NYT::VT_DATETIME : NYT::VT_UINT32;
    } else if (yqlType == TStringBuf("Uint16")) {
        ytType = NYT::VT_UINT16;
    } else if (yqlType == TStringBuf("Date")) {
        ytType = (nativeYtTypeFlags & NTCF_DATE) ? NYT::VT_DATE : NYT::VT_UINT16;
    } else if (yqlType == TStringBuf("Date32")) {
        ytType = (nativeYtTypeFlags & NTCF_BIGDATE) ? NYT::VT_DATE32 : NYT::VT_INT32;
    } else if (yqlType == TStringBuf("Datetime64")) {
        ytType = (nativeYtTypeFlags & NTCF_BIGDATE) ? NYT::VT_DATETIME64 : NYT::VT_INT64;
    } else if (yqlType == TStringBuf("Timestamp64")) {
        ytType = (nativeYtTypeFlags & NTCF_BIGDATE) ? NYT::VT_TIMESTAMP64 : NYT::VT_INT64;
    } else if (yqlType == TStringBuf("Interval64")) {
        ytType = (nativeYtTypeFlags & NTCF_BIGDATE) ? NYT::VT_INTERVAL64 : NYT::VT_INT64;
    } else if (yqlType == TStringBuf("Uint8")) {
        ytType = NYT::VT_UINT8;
    } else if (yqlType == TStringBuf("Float")) {
        ytType = (nativeYtTypeFlags & NTCF_FLOAT) ? NYT::VT_FLOAT : NYT::VT_DOUBLE;
    } else if (yqlType == TStringBuf("Double")) {
        ytType = NYT::VT_DOUBLE;
    } else if (yqlType == TStringBuf("Bool")) {
        ytType = NYT::VT_BOOLEAN;
    } else if (yqlType == TStringBuf("Yson")) {
        ytType = NYT::VT_ANY;
        required = false;
    } else if (yqlType == TStringBuf("TzDate") || yqlType == TStringBuf("TzDatetime") || yqlType == TStringBuf("TzTimestamp") ||
        yqlType == TStringBuf("TzDate32") || yqlType == TStringBuf("TzDatetime64") || yqlType == TStringBuf("TzTimestamp64")) {
        ytType = NYT::VT_STRING;
    } else {
        YQL_LOG_CTX_THROW yexception() << "Unknown type " << yqlType.Quote() << " in row spec";
    }

    return { ytType, required };
}

NYT::TNode RowSpecYqlTypeToYtNativeType(const NYT::TNode& rowSpecType, ui64 nativeYtTypeFlags) {
    const auto* type = &rowSpecType;

    if ((*type)[0] == TStringBuf("DataType")) {
        const auto& yqlType = (*type)[1].AsString();
        TString ytType;
        if (yqlType == TStringBuf("String") || yqlType == TStringBuf("Longint") || yqlType == TStringBuf("Uuid") || yqlType == TStringBuf("JsonDocument") || yqlType == TStringBuf("DyNumber")) {
            ytType = "string";
        } else if (yqlType == TStringBuf("Json")) {
            ytType = (nativeYtTypeFlags & NTCF_JSON) ? "json" : "string";
        } else if (yqlType == TStringBuf("Utf8")) {
            ytType = "utf8";
        } else if (yqlType == TStringBuf("Int64")) {
            ytType = "int64";
        } else if (yqlType == TStringBuf("Int32")) {
            ytType = "int32";
        } else if (yqlType == TStringBuf("Int16")) {
            ytType = "int16";
        } else if (yqlType == TStringBuf("Int8")) {
            ytType = "int8";
        } else if (yqlType == TStringBuf("Uint64")) {
            ytType = "uint64";
        } else if (yqlType == TStringBuf("Uint32")) {
            ytType = "uint32";
        } else if (yqlType == TStringBuf("Uint16")) {
            ytType = "uint16";
        } else if (yqlType == TStringBuf("Uint8")) {
            ytType = "uint8";
        } else if (yqlType == TStringBuf("Double")) {
            ytType = "double";
        } else if (yqlType == TStringBuf("Float")) {
            ytType = (nativeYtTypeFlags & NTCF_FLOAT) ? "float" : "double";
        } else if (yqlType == TStringBuf("Bool")) {
            ytType = "bool";
        } else if (yqlType == TStringBuf("Yson")) {
            ytType = "yson";
        } else if (yqlType == TStringBuf("TzDate") || yqlType == TStringBuf("TzDatetime") || yqlType == TStringBuf("TzTimestamp") ||
            yqlType == TStringBuf("TzDate32") || yqlType == TStringBuf("TzDatetime64") || yqlType == TStringBuf("TzTimestamp64")) {
            ytType = "string";
        } else if (yqlType == TStringBuf("Date")) {
            ytType = (nativeYtTypeFlags & NTCF_DATE) ? "date" : "uint16";
        } else if (yqlType == TStringBuf("Datetime")) {
            ytType = (nativeYtTypeFlags & NTCF_DATE) ? "datetime" : "uint32";
        } else if (yqlType == TStringBuf("Timestamp")) {
            ytType = (nativeYtTypeFlags & NTCF_DATE) ? "timestamp" : "uint64";
        } else if (yqlType == TStringBuf("Interval")) {
            ytType = (nativeYtTypeFlags & NTCF_DATE) ? "interval" : "int64";
        } else if (yqlType == TStringBuf("Date32")) {
            ytType = (nativeYtTypeFlags & NTCF_BIGDATE) ? "date32" : "int32";
        } else if (yqlType == TStringBuf("Datetime64")) {
            ytType = (nativeYtTypeFlags & NTCF_BIGDATE) ? "datetime64" : "int64";
        } else if (yqlType == TStringBuf("Timestamp64")) {
            ytType = (nativeYtTypeFlags & NTCF_BIGDATE) ? "timestamp64" : "int64";
        } else if (yqlType == TStringBuf("Interval64")) {
            ytType = (nativeYtTypeFlags & NTCF_BIGDATE) ? "interval64" : "int64";
        } else if (yqlType == TStringBuf("Decimal")) {
            if (nativeYtTypeFlags & NTCF_DECIMAL) {
                try {
                    return NYT::TNode::CreateMap()
                        ("type_name", "decimal")
                        ("precision", FromString<int>((*type)[2].AsString()))
                        ("scale", FromString<int>((*type)[3].AsString()))
                        ;
                } catch (...) {
                    YQL_LOG_CTX_THROW yexception() << "Invalid Decimal type in row spec: " << CurrentExceptionMessage();
                }
            } else {
                ytType = "string";
            }
        } else {
            YQL_LOG_CTX_THROW yexception() << "Not supported data type: " << yqlType;
        }

        return NYT::TNode(ytType);
    } else if ((*type)[0] == TStringBuf("OptionalType")) {
        return NYT::TNode::CreateMap()
            ("type_name", "optional")
            ("item", RowSpecYqlTypeToYtNativeType((*type)[1], nativeYtTypeFlags))
            ;
    } else if ((*type)[0] == TStringBuf("ListType")) {
        return NYT::TNode::CreateMap()
            ("type_name", "list")
            ("item", RowSpecYqlTypeToYtNativeType((*type)[1], nativeYtTypeFlags))
            ;
    } else if ((*type)[0] == TStringBuf("TupleType")) {
        auto elements = NYT::TNode::CreateList();
        for (const auto& x : (*type)[1].AsList()) {
            elements.Add(NYT::TNode::CreateMap()("type", RowSpecYqlTypeToYtNativeType(x, nativeYtTypeFlags)));
        }

        return NYT::TNode::CreateMap()
            ("type_name", "tuple")
            ("elements", elements)
            ;
    } else if ((*type)[0] == TStringBuf("StructType")) {
        auto members = NYT::TNode::CreateList();
        for (const auto& x : (*type)[1].AsList()) {
            members.Add(NYT::TNode::CreateMap()
                ("name", x[0].AsString())
                ("type", RowSpecYqlTypeToYtNativeType(x[1], nativeYtTypeFlags))
            );
        }

        return NYT::TNode::CreateMap()
            ("type_name", "struct")
            ("members", members)
            ;
    } else if ((*type)[0] == TStringBuf("VariantType")) {
        auto base = (*type)[1];
        if (base[0] == TStringBuf("TupleType")) {
            auto elements = NYT::TNode::CreateList();
            for (const auto& x : base[1].AsList()) {
                elements.Add(NYT::TNode::CreateMap()("type", RowSpecYqlTypeToYtNativeType(x, nativeYtTypeFlags)));
            }

            return NYT::TNode::CreateMap()
                ("type_name", "variant")
                ("elements", elements)
                ;

        } else if (base[0] == TStringBuf("StructType")) {
            auto members = NYT::TNode::CreateList();
            for (const auto& x : base[1].AsList()) {
                members.Add(NYT::TNode::CreateMap()
                    ("name", x[0].AsString())
                    ("type", RowSpecYqlTypeToYtNativeType(x[1], nativeYtTypeFlags))
                );
            }

            return NYT::TNode::CreateMap()
                ("type_name", "variant")
                ("members", members)
                ;
        } else {
            YQL_ENSURE(false, "Not supported variant base type: " << base[0].AsString());
        }
    } else if ((*type)[0] == TStringBuf("VoidType")) {
        if (nativeYtTypeFlags & NTCF_VOID) {
            return NYT::TNode("void");
        }
        return NYT::TNode::CreateMap({
            {"type_name", "tagged"},
            {"tag", "_Void"},
            {"item", "null"}
        });

    } else if ((*type)[0] == TStringBuf("EmptyListType")) {
        return NYT::TNode::CreateMap({
            {"type_name", "tagged"},
            {"tag", "_EmptyList"},
            {"item", "null"}
        });
    } else if ((*type)[0] == TStringBuf("EmptyDictType")) {
        return NYT::TNode::CreateMap({
            {"type_name", "tagged"},
            {"tag", "_EmptyDict"},
            {"item", "null"}
        });
    } else if ((*type)[0] == TStringBuf("NullType")) {
        if (nativeYtTypeFlags & NTCF_NULL) {
            return NYT::TNode("null");
        }
        return NYT::TNode::CreateMap({
            {"type_name", "tagged"},
            {"tag", "_Null"},
            {"item", "null"}
        });
    } else if ((*type)[0] == TStringBuf("TaggedType")) {
        return NYT::TNode::CreateMap()
            ("type_name", "tagged")
            ("tag", (*type)[1])
            ("item", RowSpecYqlTypeToYtNativeType((*type)[2], nativeYtTypeFlags))
            ;
    } else if ((*type)[0] == TStringBuf("DictType")) {
        return NYT::TNode::CreateMap()
            ("type_name", "dict")
            ("key", RowSpecYqlTypeToYtNativeType((*type)[1], nativeYtTypeFlags))
            ("value", RowSpecYqlTypeToYtNativeType((*type)[2], nativeYtTypeFlags))
            ;
    } else if ((*type)[0] == TStringBuf("PgType")) {
        const auto& name = (*type)[1].AsString();
        TString ytType;
        if (name == "bool") {
            ytType = "bool";
        } else if (name == "int2") {
            ytType = "int16";
        } else if (name == "int4") {
            ytType = "int32";
        } else if (name == "int8") {
            ytType = "int64";
        } else if (name == "float8") {
            ytType = "double";;
        } else if (name == "float4") {
            ytType = (nativeYtTypeFlags & NTCF_FLOAT) ? "float" : "double";
        } else if (name == "text" || name == "varchar" || name == "cstring") {
            ytType = "utf8";
        } else {
            ytType = "string";
        }

        return NYT::TNode::CreateMap()
            ("type_name", "optional")
            ("item", NYT::TNode(ytType));
    }

    YQL_ENSURE(false, "Not supported type: " << (*type)[0].AsString());
}

NYT::TTableSchema RowSpecToYTSchema(const NYT::TNode& rowSpec, ui64 nativeTypeCompatibility, const NYT::TNode& columnGroupsSpec) {

    TString defaultGroup;
    THashMap<TString, TString> columnGroups;
    if (!columnGroupsSpec.IsUndefined()) {
        for (const auto& grp: columnGroupsSpec.AsMap()) {
            if (grp.second.IsEntity()) {
                defaultGroup = grp.first;
            } else {
                for (const auto& col: grp.second.AsList()) {
                    columnGroups[col.AsString()] = grp.first;
                }
            }
        }
    }

    NYT::TTableSchema schema;
    const auto& rowSpecMap = rowSpec.AsMap();

    const auto* rootType = rowSpecMap.FindPtr(RowSpecAttrType);
    if (!rootType || !rootType->IsList()) {
        YQL_LOG_CTX_THROW yexception() << "Invalid Type in row spec";
    }

    ui64 nativeYtTypeFlags = 0;
    if (rowSpec.HasKey(RowSpecAttrNativeYtTypeFlags)) {
        nativeYtTypeFlags = rowSpec[RowSpecAttrNativeYtTypeFlags].AsUint64();
    } else {
        if (rowSpec.HasKey(RowSpecAttrUseNativeYtTypes)) {
            nativeYtTypeFlags = NYT::GetBool(rowSpec[RowSpecAttrUseNativeYtTypes]) ? NTCF_LEGACY : NTCF_NONE;
        } else if (rowSpec.HasKey(RowSpecAttrUseTypeV2)) {
            nativeYtTypeFlags = NYT::GetBool(rowSpec[RowSpecAttrUseTypeV2]) ? NTCF_LEGACY : NTCF_NONE;
        }
    }
    nativeYtTypeFlags &= nativeTypeCompatibility;
    bool useNativeTypes = nativeYtTypeFlags & (NTCF_COMPLEX | NTCF_DECIMAL);

    THashMap<TString, std::pair<NYT::EValueType, bool>> fieldTypes;
    THashMap<TString, NYT::TNode> fieldNativeTypes;
    TVector<TString> columns;

    for (const auto& entry : rootType->AsList()[1].AsList()) {
        TString column = entry[0].AsString();
        if (useNativeTypes) {
            fieldNativeTypes[column] = RowSpecYqlTypeToYtNativeType(entry[1], nativeYtTypeFlags);
        } else {
            fieldTypes[column] = RowSpecYqlTypeToYtType(entry[1], nativeYtTypeFlags);
        }
        columns.push_back(column);
    }

    const auto* sortedBy = rowSpecMap.FindPtr(RowSpecAttrSortedBy);
    const auto* sortedByTypes = rowSpecMap.FindPtr(RowSpecAttrSortedByTypes);
    const auto* sortDirections = rowSpecMap.FindPtr(RowSpecAttrSortDirections);

    THashSet<TString> keyColumns;
    if (sortedBy && sortedByTypes) {
        auto sortedByType = sortedByTypes->AsList().begin();
        auto sortDir = sortDirections->AsList().begin();
        for (const auto& column : sortedBy->AsList()) {
            const auto& columnString = column.AsString();

            keyColumns.insert(columnString);

            auto columnNode = NYT::TColumnSchema()
                .Name(columnString);

            if (auto group = columnGroups.Value(columnString, defaultGroup)) {
                columnNode.Group(std::move(group));
            }

            bool auxField = false;
            if (useNativeTypes) {
                auto ytType = RowSpecYqlTypeToYtNativeType(*sortedByType, nativeYtTypeFlags);
                columnNode.RawTypeV3(ytType);
                auxField = 0 == fieldNativeTypes.erase(columnString);
            } else {
                auto ytType = RowSpecYqlTypeToYtType(*sortedByType, nativeYtTypeFlags);
                columnNode.Type(ytType.first, /*required*/ ytType.second);
                auxField = 0 == fieldTypes.erase(columnString);
            }

            columnNode.SortOrder(sortDir->AsInt64() || auxField ? NYT::SO_ASCENDING : NYT::SO_DESCENDING);
            schema.AddColumn(columnNode);
            ++sortedByType;
            ++sortDir;
        }
    }

    for (const auto& column : columns) {
        if (keyColumns.contains(column)) {
            continue;
        }
        auto columnNode = NYT::TColumnSchema().Name(column);
        if (auto group = columnGroups.Value(column, defaultGroup)) {
            columnNode.Group(std::move(group));
        }
        if (useNativeTypes) {
            auto field = fieldNativeTypes.find(column);
            YQL_ENSURE(field != fieldNativeTypes.end());
            columnNode.RawTypeV3(field->second);

        } else {
            auto field = fieldTypes.find(column);
            YQL_ENSURE(field != fieldTypes.end());
            columnNode.Type(field->second.first, /*required*/ field->second.second);
        }
        schema.AddColumn(std::move(columnNode));
    }

    // add fake column to avoid slow 0-columns YT schema
    if (schema.Columns().empty()) {
        schema.AddColumn(NYT::TColumnSchema()
            .Name(TString{YqlFakeColumnName})
            .Type(NYT::EValueType::VT_BOOLEAN, /*required*/ false));
    }

    if (rowSpec.HasKey(RowSpecAttrUniqueKeys)) {
        schema.UniqueKeys(NYT::GetBool(rowSpec[RowSpecAttrUniqueKeys]));
    }

    return schema;
}

NYT::TSortColumns ToYTSortColumns(const TVector<std::pair<TString, bool>>& sortColumns) {
    NYT::TSortColumns res;
    for (auto& item: sortColumns) {
        res.Add(NYT::TSortColumn().Name(item.first).SortOrder(item.second ? NYT::ESortOrder::SO_ASCENDING : NYT::ESortOrder::SO_DESCENDING));
    }
    return res;
}

TString GetTypeV3String(const TTypeAnnotationNode& type, ui64 nativeTypeCompatibility) {
    NYT::TNode typeNode;
    NYT::TNodeBuilder nodeBuilder(&typeNode);
    NCommon::WriteTypeToYson(nodeBuilder, &type);
    return NYT::NodeToCanonicalYsonString(RowSpecYqlTypeToYtNativeType(typeNode, nativeTypeCompatibility));
}

} // NYql
