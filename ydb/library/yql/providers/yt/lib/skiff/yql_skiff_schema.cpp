#include "yql_skiff_schema.h"

#include <ydb/library/yql/providers/yt/common/yql_names.h>
#include <ydb/library/yql/providers/common/codec/yql_codec_type_flags.h>
#include <ydb/library/yql/providers/common/schema/skiff/yql_skiff_schema.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <library/cpp/yson/node/node.h>

#include <util/generic/hash_set.h>
#include <util/generic/xrange.h>
#include <util/generic/map.h>


namespace NYql {

namespace {

NYT::TNode RowSpecToInputSkiff(const NYT::TNode& attrs, const THashMap<TString, ui32>& structColumns,
    const THashSet<TString>& auxColumns, bool rowIndex, bool rangeIndex, bool keySwitch)
{

    YQL_ENSURE(attrs.HasKey(YqlRowSpecAttribute), "Missing mandatory "
        << TString{YqlRowSpecAttribute}.Quote() << " attribute");

    const auto& rowSpec = attrs[YqlRowSpecAttribute];

    ui64 nativeYtTypeFlags = 0;
    if (rowSpec.HasKey(RowSpecAttrNativeYtTypeFlags)) {
        nativeYtTypeFlags = rowSpec[RowSpecAttrNativeYtTypeFlags].AsUint64();
    } else {
        if (rowSpec.HasKey(RowSpecAttrUseNativeYtTypes)) {
            nativeYtTypeFlags = NYT::GetBool(rowSpec[RowSpecAttrUseNativeYtTypes]) ? NTCF_LEGACY : NTCF_NONE;
        } else  if (rowSpec.HasKey(RowSpecAttrUseTypeV2)) {
            nativeYtTypeFlags = NYT::GetBool(rowSpec[RowSpecAttrUseTypeV2]) ? NTCF_LEGACY : NTCF_NONE;
        }
    }

    THashSet<TString> fieldsWithDefValue;
    if (rowSpec.HasKey(RowSpecAttrDefaultValues)) {
        for (auto& value : rowSpec[RowSpecAttrDefaultValues].AsMap()) {
            fieldsWithDefValue.insert(value.first);
        }
    }

    THashSet<TString> fieldsWithExplicitYson;
    if (rowSpec.HasKey(RowSpecAttrExplicitYson)) {
        for (auto& value : rowSpec[RowSpecAttrExplicitYson].AsList()) {
            fieldsWithExplicitYson.emplace(value.AsString());
        }
    }

    auto typeNode = NCommon::ParseSkiffTypeFromYson(rowSpec[RowSpecAttrType], nativeYtTypeFlags);
    TMap<TString, NYT::TNode> typeColumns; // Must be ordered
    const auto optType = NYT::TNode("variant8");
    for (auto item: typeNode["children"].AsList()) {
        auto name = item["name"].AsString();
        if (fieldsWithExplicitYson.contains(name)) {
            item = NYT::TNode()
                ("name", name)
                ("wire_type", "variant8")
                ("children", NYT::TNode()
                    .Add(NYT::TNode()("wire_type", "nothing"))
                    .Add(NYT::TNode()("wire_type", "yson32"))
                );

        } else if (fieldsWithDefValue.contains(name) && item["wire_type"] != optType) {
            item = NYT::TNode()
                ("name", name)
                ("wire_type", "variant8")
                ("children", NYT::TNode()
                    .Add(NYT::TNode()("wire_type", "nothing"))
                    .Add(std::move(item))
                );
        }
        YQL_ENSURE(typeColumns.emplace(name, std::move(item)).second, "Duplicate struct column: " << name);
    }

    THashMap<TString, NYT::TNode> sortAuxColumns;
    if (rowSpec.HasKey(RowSpecAttrSortedBy)) {
        auto& sortedBy = rowSpec[RowSpecAttrSortedBy].AsList();
        auto& sortedByType = rowSpec[RowSpecAttrSortedByTypes].AsList();
        auto& sortedDirections = rowSpec[RowSpecAttrSortDirections].AsList();
        for (size_t i: xrange(sortedBy.size())) {
            auto name = sortedBy[i].AsString();
            if (!typeColumns.contains(name)) {
                NYT::TNode fieldType;
                if (!sortedDirections.empty() && !sortedDirections[i].AsInt64()) {
                    fieldType = NYT::TNode()("wire_type", "string32");
                } else {
                    fieldType = NCommon::ParseSkiffTypeFromYson(sortedByType[i], nativeYtTypeFlags);
                }
                fieldType["name"] = name;
                sortAuxColumns.emplace(name, std::move(fieldType));
            }
        }
    }

    bool strictSchema = true;
    if (rowSpec.HasKey(RowSpecAttrStrictSchema)) {
        strictSchema = rowSpec[RowSpecAttrStrictSchema].IsInt64()
            ? rowSpec[RowSpecAttrStrictSchema].AsInt64() != 0
            : NYT::GetBool(rowSpec[RowSpecAttrStrictSchema]);
    }

    if (!strictSchema) {
        if (rowSpec.HasKey(RowSpecAttrWeakFields)) {
            for (auto& field: rowSpec[RowSpecAttrWeakFields].AsList()) {
                if (!typeColumns.contains(field.AsString())) {
                    auto column = NYT::TNode()
                        ("name", field)
                        ("wire_type", "variant8")
                        ("children", NYT::TNode()
                            .Add(NYT::TNode()("wire_type", "nothing"))
                            .Add(NYT::TNode()("wire_type", "yson32"))
                        );

                    typeColumns.emplace(field.AsString(), std::move(column));
                }
            }
        }
    }

    NYT::TNode tableSchema;
    tableSchema["wire_type"] = "tuple";
    auto& tableColumns = tableSchema["children"];
    tableColumns = NYT::TNode::CreateList();

    if (rangeIndex) {
        tableColumns.Add(NYT::TNode()
            ("name", "$range_index")
            ("wire_type", "variant8")
            ("children", NYT::TNode()
                .Add(NYT::TNode()("wire_type", "nothing"))
                .Add(NYT::TNode()("wire_type", "int64"))
            )
        );
    }

    const bool dynamic = attrs.HasKey(YqlDynamicAttribute) && attrs[YqlDynamicAttribute].AsBool();

    if (!dynamic && rowIndex) {
        tableColumns.Add(NYT::TNode()
            ("name", "$row_index")
            ("wire_type", "variant8")
            ("children", NYT::TNode()
                .Add(NYT::TNode()("wire_type", "nothing"))
                .Add(NYT::TNode()("wire_type", "int64"))
            )
        );
    }

    if (keySwitch) {
        tableColumns.Add(NYT::TNode()
            ("name", "$key_switch")
            ("wire_type", "boolean")
        );
    }

    TMap<ui32, NYT::TNode> columns;
    const bool hasOthers = !strictSchema && structColumns.contains(YqlOthersColumnName);
    for (auto& item: typeColumns) {
        if (auto p = structColumns.FindPtr(item.first)) {
            ui32 pos = *p;
            YQL_ENSURE(columns.emplace(pos, item.second).second, "Reused column position");
        }
    }
    for (auto& item: sortAuxColumns) {
        if (auto p = structColumns.FindPtr(item.first)) {
            ui32 pos = *p;
            YQL_ENSURE(columns.emplace(pos, item.second).second, "Reused column position");
        }
    }
    for (auto& item: columns) {
        tableColumns.Add(item.second);
    }

    if (hasOthers || !auxColumns.empty()) {
        for (auto& item: typeColumns) {
            if (!structColumns.contains(item.first)) {
                if (hasOthers || auxColumns.contains(item.first)) {
                    tableColumns.Add(item.second);
                }
            }
        }
        if (hasOthers) {
            tableColumns.Add(NYT::TNode()
                ("name", "$other_columns")
                ("wire_type", "yson32")
            );
        }
    }
    return tableSchema;
}

NYT::TNode RowSpecToOutputSkiff(const NYT::TNode& attrs) {
    YQL_ENSURE(attrs.HasKey(YqlRowSpecAttribute), "Missing mandatory "
        << TString{YqlRowSpecAttribute}.Quote() << " attribute");

    const auto& rowSpec = attrs[YqlRowSpecAttribute];
    ui64 nativeYtTypeFlags = 0;
    if (rowSpec.HasKey(RowSpecAttrNativeYtTypeFlags)) {
        nativeYtTypeFlags = rowSpec[RowSpecAttrNativeYtTypeFlags].AsUint64();
    } else {
        if (rowSpec.HasKey(RowSpecAttrUseNativeYtTypes)) {
            nativeYtTypeFlags = NYT::GetBool(rowSpec[RowSpecAttrUseNativeYtTypes]) ? NTCF_LEGACY : NTCF_NONE;
        } else  if (rowSpec.HasKey(RowSpecAttrUseTypeV2)) {
            nativeYtTypeFlags = NYT::GetBool(rowSpec[RowSpecAttrUseTypeV2]) ? NTCF_LEGACY : NTCF_NONE;
        }
    }

    auto typeNode = NCommon::ParseSkiffTypeFromYson(rowSpec[RowSpecAttrType], nativeYtTypeFlags);
    TMap<TString, NYT::TNode> typeColumns; // Must be ordered
    for (auto item: typeNode["children"].AsList()) {
        auto name = item["name"].AsString();
        YQL_ENSURE(typeColumns.emplace(name, std::move(item)).second, "Duplicate struct column: " << name);
    }

    if (rowSpec.HasKey(RowSpecAttrSortedBy)) {
        auto& sortedBy = rowSpec[RowSpecAttrSortedBy].AsList();
        auto& sortedByType = rowSpec[RowSpecAttrSortedByTypes].AsList();
        auto& sortedDirections = rowSpec[RowSpecAttrSortDirections].AsList();
        for (size_t i: xrange(sortedBy.size())) {
            auto name = sortedBy[i].AsString();
            if (!typeColumns.contains(name)) {
                NYT::TNode fieldType;
                if (!sortedDirections.empty() && !sortedDirections[i].AsInt64()) {
                    fieldType = NYT::TNode()("wire_type", "string32");
                } else {
                    fieldType = NCommon::ParseSkiffTypeFromYson(sortedByType[i], nativeYtTypeFlags);
                }
                fieldType["name"] = name;
                YQL_ENSURE(typeColumns.emplace(name, std::move(fieldType)).second, "Duplicate struct aux column: " << name);
            }
        }
    }

    if (typeColumns.empty()) {
        typeColumns.emplace(YqlFakeColumnName, NYT::TNode()
            ("name", YqlFakeColumnName)
            ("wire_type", "variant8")
            ("children", NYT::TNode()
                .Add(NYT::TNode()("wire_type", "nothing"))
                .Add(NYT::TNode()("wire_type", "boolean"))
            )
        );
    }

    NYT::TNode tableSchema;
    tableSchema["wire_type"] = "tuple";
    auto& tableColumns = tableSchema["children"];
    tableColumns = NYT::TNode::CreateList();
    for (auto& item: typeColumns) {
        tableColumns.Add(item.second);
    }

    return tableSchema;
}

}

NYT::TNode SingleTableSpecToInputSkiff(const NYT::TNode& spec, const THashMap<TString, ui32>& structColumns, bool rowIndex, bool rangeIndex, bool keySwitch) {
    NYT::TNode formatConfig = NYT::TNode("skiff");
    auto& skiffConfig = formatConfig.Attributes();
    auto& skiffSchemas = skiffConfig["table_skiff_schemas"];
    skiffSchemas.Add(RowSpecToInputSkiff(spec, structColumns, {}, rowIndex, rangeIndex, keySwitch));

    return formatConfig;
}

NYT::TNode SingleTableSpecToInputSkiffSchema(const NYT::TNode& spec, size_t tableIndex, const THashMap<TString, ui32>& structColumns, const THashSet<TString>& auxColumns, bool rowIndex, bool rangeIndex, bool keySwitch) {
    YQL_ENSURE(spec.IsMap(), "Expect Map type of input meta attrs, but got type " << spec.GetType());
    YQL_ENSURE(spec.HasKey(YqlIOSpecTables), "Expect " << TString{YqlIOSpecTables}.Quote() << " key");

    const auto& inputSpecs = spec[YqlIOSpecTables].AsList();
    YQL_ENSURE(tableIndex < inputSpecs.size());

    if (inputSpecs[tableIndex].IsString()) {
        auto refName = inputSpecs[tableIndex].AsString();
        YQL_ENSURE(spec.HasKey(YqlIOSpecRegistry) && spec[YqlIOSpecRegistry].HasKey(refName), "Bad input registry reference: " << refName);
        return RowSpecToInputSkiff(spec[YqlIOSpecRegistry][refName], structColumns, auxColumns, rowIndex, rangeIndex, keySwitch);
    } else {
        return RowSpecToInputSkiff(inputSpecs[tableIndex], structColumns, auxColumns, rowIndex, rangeIndex, keySwitch);
    }
}

NYT::TNode TablesSpecToInputSkiff(const NYT::TNode& spec, const THashMap<TString, ui32>& structColumns, bool rowIndex, bool rangeIndex, bool keySwitch) {
    YQL_ENSURE(spec.IsMap(), "Expect Map type of input meta attrs, but got type " << spec.GetType());
    YQL_ENSURE(spec.HasKey(YqlIOSpecTables), "Expect " << TString{YqlIOSpecTables}.Quote() << " key");

    const auto& inputSpecs = spec[YqlIOSpecTables].AsList();

    NYT::TNode formatConfig = NYT::TNode("skiff");
    auto& skiffConfig = formatConfig.Attributes();
    auto skiffSchemas = NYT::TNode::CreateList();
    THashMap<TString, size_t> uniqSchemas;
    for (size_t inputIndex = 0; inputIndex < inputSpecs.size(); ++inputIndex) {
        if (inputSpecs[inputIndex].IsString()) {
            auto refName = inputSpecs[inputIndex].AsString();

            size_t schemaId = uniqSchemas.size();
            auto p = uniqSchemas.emplace(refName, schemaId);
            if (p.second) {
                YQL_ENSURE(spec.HasKey(YqlIOSpecRegistry) && spec[YqlIOSpecRegistry].HasKey(refName), "Bad input registry reference: " << refName);
                NYT::TNode tableSchema = RowSpecToInputSkiff(spec[YqlIOSpecRegistry][refName], structColumns, {}, rowIndex, rangeIndex, keySwitch);
                skiffConfig["skiff_schema_registry"][TStringBuilder() << "table" << schemaId] = std::move(tableSchema);
            }
            else {
                schemaId = p.first->second;
            }

            skiffSchemas.Add(NYT::TNode(TStringBuilder() << "$table" << schemaId));
        } else {
            NYT::TNode tableSchema = RowSpecToInputSkiff(inputSpecs[inputIndex], structColumns, {}, rowIndex, rangeIndex, keySwitch);
            skiffSchemas.Add(std::move(tableSchema));
        }
    }

    skiffConfig["table_skiff_schemas"] = std::move(skiffSchemas);
    return formatConfig;
}

NYT::TNode TablesSpecToOutputSkiff(const NYT::TNode& spec) {
    YQL_ENSURE(spec.IsMap(), "Expect Map type of output meta attrs, but got type " << spec.GetType());
    YQL_ENSURE(spec.HasKey(YqlIOSpecTables), "Expect " << TString{YqlIOSpecTables}.Quote() << " key");

    const auto& outSpecs = spec[YqlIOSpecTables].AsList();

    NYT::TNode formatConfig = NYT::TNode("skiff");
    auto& skiffConfig = formatConfig.Attributes();
    auto skiffSchemas = NYT::TNode::CreateList();
    THashMap<TString, size_t> uniqSchemas;
    for (size_t outIndex = 0; outIndex < outSpecs.size(); ++outIndex) {
        if (outSpecs[outIndex].IsString()) {
            auto refName = outSpecs[outIndex].AsString();

            size_t schemaId = uniqSchemas.size();
            auto p = uniqSchemas.emplace(refName, schemaId);
            if (p.second) {
                YQL_ENSURE(spec.HasKey(YqlIOSpecRegistry) && spec[YqlIOSpecRegistry].HasKey(refName), "Bad output registry reference: " << refName);
                NYT::TNode tableSchema = RowSpecToOutputSkiff(spec[YqlIOSpecRegistry][refName]);
                skiffConfig["skiff_schema_registry"][TStringBuilder() << "table" << schemaId] = std::move(tableSchema);
            }
            else {
                schemaId = p.first->second;
            }

            skiffSchemas.Add(NYT::TNode(TStringBuilder() << "$table" << schemaId));
        } else {
            NYT::TNode tableSchema = RowSpecToOutputSkiff(outSpecs[outIndex]);
            skiffSchemas.Add(std::move(tableSchema));
        }
    }

    skiffConfig["table_skiff_schemas"] = std::move(skiffSchemas);
    return formatConfig;
}

NYT::TNode SingleTableSpecToOutputSkiff(const NYT::TNode& spec, size_t tableIndex) {
    YQL_ENSURE(spec.IsMap(), "Expect Map type of output meta attrs, but got type " << spec.GetType());
    YQL_ENSURE(spec.HasKey(YqlIOSpecTables), "Expect " << TString{YqlIOSpecTables}.Quote() << " key");

    const auto& outSpecs = spec[YqlIOSpecTables].AsList();
    YQL_ENSURE(tableIndex < outSpecs.size());

    NYT::TNode formatConfig = NYT::TNode("skiff");
    auto& skiffConfig = formatConfig.Attributes();
    auto skiffSchemas = NYT::TNode::CreateList();
    THashMap<TString, size_t> uniqSchemas;
    if (outSpecs[tableIndex].IsString()) {
        auto refName = outSpecs[tableIndex].AsString();
        YQL_ENSURE(spec.HasKey(YqlIOSpecRegistry) && spec[YqlIOSpecRegistry].HasKey(refName), "Bad output registry reference: " << refName);
        NYT::TNode tableSchema = RowSpecToOutputSkiff(spec[YqlIOSpecRegistry][refName]);
        skiffSchemas.Add(std::move(tableSchema));
    } else {
        NYT::TNode tableSchema = RowSpecToOutputSkiff(outSpecs[tableIndex]);
        skiffSchemas.Add(std::move(tableSchema));
    }

    skiffConfig["table_skiff_schemas"] = std::move(skiffSchemas);
    return formatConfig;
}

} // NYql
