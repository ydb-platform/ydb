#include "serialize.h"

#include "common.h"
#include "fluent.h"

#include <library/cpp/yson/parser.h>
#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/yson/node/serialize.h>

#include <library/cpp/type_info/type_io.h>

#include <util/generic/string.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// const auto& nodeMap = node.AsMap();
#define DESERIALIZE_ITEM(NAME, MEMBER) \
    if (const auto* item = nodeMap.FindPtr(NAME)) { \
        Deserialize(MEMBER, *item); \
    }

// const auto& attributesMap = node.GetAttributes().AsMap();
#define DESERIALIZE_ATTR(NAME, MEMBER) \
    if (const auto* attr = attributesMap.FindPtr(NAME)) { \
        Deserialize(MEMBER, *attr); \
    }

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TSortColumn& sortColumn, NYson::IYsonConsumer* consumer)
{
    if (sortColumn.SortOrder() == ESortOrder::SO_ASCENDING) {
        Serialize(sortColumn.Name(), consumer);
    } else {
        BuildYsonFluently(consumer).BeginMap()
            .Item("name").Value(sortColumn.Name())
            .Item("sort_order").Value(ToString(sortColumn.SortOrder()))
        .EndMap();
    }
}

void Deserialize(TSortColumn& sortColumn, const TNode& node)
{
    if (node.IsString()) {
        sortColumn = TSortColumn(node.AsString());
    } else if (node.IsMap()) {
        const auto& name = node["name"].AsString();
        const auto& sortOrderString = node["sort_order"].AsString();
        sortColumn = TSortColumn(name, ::FromString<ESortOrder>(sortOrderString));
    } else {
        ythrow yexception() << "Expected sort column to be string or map, got " << node.GetType();
    }
}

template <class T, class TDerived>
void SerializeOneOrMany(const TOneOrMany<T, TDerived>& oneOrMany, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer).List(oneOrMany.Parts_);
}

template <class T, class TDerived>
void DeserializeOneOrMany(TOneOrMany<T, TDerived>& oneOrMany, const TNode& node)
{
    Deserialize(oneOrMany.Parts_, node);
}

void Serialize(const TKey& key, NYson::IYsonConsumer* consumer)
{
    SerializeOneOrMany(key, consumer);
}

void Deserialize(TKey& key, const TNode& node)
{
    DeserializeOneOrMany(key, node);
}

void Serialize(const TSortColumns& sortColumns, NYson::IYsonConsumer* consumer)
{
    SerializeOneOrMany(sortColumns, consumer);
}

void Deserialize(TSortColumns& sortColumns, const TNode& node)
{
    DeserializeOneOrMany(sortColumns, node);
}

void Serialize(const TColumnNames& columnNames, NYson::IYsonConsumer* consumer)
{
    SerializeOneOrMany(columnNames, consumer);
}

void Deserialize(TColumnNames& columnNames, const TNode& node)
{
    DeserializeOneOrMany(columnNames, node);
}

////////////////////////////////////////////////////////////////////////////////

void Deserialize(EValueType& valueType, const TNode& node)
{
    const auto& nodeStr = node.AsString();
    static const THashMap<TString, EValueType> str2ValueType = {
        {"int8",  VT_INT8},
        {"int16", VT_INT16},
        {"int32", VT_INT32},
        {"int64", VT_INT64},

        {"uint8",   VT_UINT8},
        {"uint16",  VT_UINT16},
        {"uint32",  VT_UINT32},
        {"uint64",  VT_UINT64},

        {"boolean", VT_BOOLEAN},
        {"double",  VT_DOUBLE},

        {"string", VT_STRING},
        {"utf8",   VT_UTF8},

        {"any", VT_ANY},

        {"null", VT_NULL},
        {"void", VT_VOID},

        {"date", VT_DATE},
        {"datetime", VT_DATETIME},
        {"timestamp", VT_TIMESTAMP},
        {"interval", VT_INTERVAL},
        {"float", VT_FLOAT},
        {"json", VT_JSON},

        {"date32", VT_DATE32},
        {"datetime64", VT_DATETIME64},
        {"timestamp64", VT_TIMESTAMP64},
        {"interval64", VT_INTERVAL64},
    };

    auto it = str2ValueType.find(nodeStr);
    if (it == str2ValueType.end()) {
        ythrow yexception() << "Invalid value type '" << nodeStr << "'";
    }

    valueType = it->second;
}

void Deserialize(ESortOrder& sortOrder, const TNode& node)
{
    sortOrder = FromString<ESortOrder>(node.AsString());
}

void Deserialize(EOptimizeForAttr& optimizeFor, const TNode& node)
{
    optimizeFor = FromString<EOptimizeForAttr>(node.AsString());
}

void Deserialize(EErasureCodecAttr& erasureCodec, const TNode& node)
{
    erasureCodec = FromString<EErasureCodecAttr>(node.AsString());
}

void Deserialize(ESchemaModificationAttr& schemaModification, const TNode& node)
{
    schemaModification = FromString<ESchemaModificationAttr>(node.AsString());
}

void Serialize(const TColumnSchema& columnSchema, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer).BeginMap()
        .Item("name").Value(columnSchema.Name())
        .DoIf(!columnSchema.RawTypeV3().Defined(),
            [&] (TFluentMap fluent) {
                static const auto optionalYson = NTi::Optional(NTi::Yson());

                fluent.Item("type").Value(NDetail::ToString(columnSchema.Type()));
                fluent.Item("required").Value(columnSchema.Required());
                if (
                    (columnSchema.Type() == VT_ANY && *columnSchema.TypeV3() != *optionalYson) ||
                    // See https://github.com/ytsaurus/ytsaurus/issues/173
                    columnSchema.TypeV3()->IsDecimal() ||
                    (columnSchema.TypeV3()->IsOptional() && columnSchema.TypeV3()->AsOptional()->GetItemType()->IsDecimal()))

                {
                    // A lot of user canonize serialized schema.
                    // To be backward compatible we only set type_v3 for new types.
                    fluent.Item("type_v3").Value(columnSchema.TypeV3());
                }
            }
        )
        .DoIf(columnSchema.RawTypeV3().Defined(), [&] (TFluentMap fluent) {
            const auto& rawTypeV3 = *columnSchema.RawTypeV3();
            fluent.Item("type_v3").Value(rawTypeV3);

            // We going set old fields `type` and `required` to be compatible
            // with old clusters that doesn't support type_v3 yet.

            // if type is simple return its name otherwise return empty optional
            auto isRequired = [](TStringBuf simpleType) {
                return simpleType != "null" && simpleType != "void";
            };
            auto getSimple = [] (const TNode& typeV3) -> TMaybe<TString> {
                static const THashMap<TString,TString> typeV3ToOld = {
                    {"bool", "boolean"},
                    {"yson", "any"},
                };
                TMaybe<TString> result;
                if (typeV3.IsString()) {
                    result = typeV3.AsString();
                } else if (typeV3.IsMap() && typeV3.Size() == 1) {
                    Y_ABORT_UNLESS(typeV3["type_name"].IsString(), "invalid type is passed");
                    result = typeV3["type_name"].AsString();
                }
                if (result) {
                    auto it = typeV3ToOld.find(*result);
                    if (it != typeV3ToOld.end()) {
                        result = it->second;
                    }
                }
                return result;
            };
            auto simplify = [&](const TNode& typeV3) -> TMaybe<std::pair<TString, bool>> {
                auto simple = getSimple(typeV3);
                if (simple) {
                    return std::pair(*simple, isRequired(*simple));
                }
                if (typeV3.IsMap() && typeV3["type_name"] == "optional") {
                    auto simpleItem = getSimple(typeV3["item"]);
                    if (simpleItem && isRequired(*simpleItem)) {
                        return std::pair(*simpleItem, false);
                    }
                }
                return {};
            };

            auto simplified = simplify(rawTypeV3);

            if (simplified) {
                const auto& [simpleType, required] = *simplified;
                fluent
                    .Item("type").Value(simpleType)
                    .Item("required").Value(required);
                return;
            }
        })
        .DoIf(columnSchema.SortOrder().Defined(), [&] (TFluentMap fluent) {
            fluent.Item("sort_order").Value(ToString(*columnSchema.SortOrder()));
        })
        .DoIf(columnSchema.Lock().Defined(), [&] (TFluentMap fluent) {
            fluent.Item("lock").Value(*columnSchema.Lock());
        })
        .DoIf(columnSchema.Expression().Defined(), [&] (TFluentMap fluent) {
            fluent.Item("expression").Value(*columnSchema.Expression());
        })
        .DoIf(columnSchema.Aggregate().Defined(), [&] (TFluentMap fluent) {
            fluent.Item("aggregate").Value(*columnSchema.Aggregate());
        })
        .DoIf(columnSchema.Group().Defined(), [&] (TFluentMap fluent) {
            fluent.Item("group").Value(*columnSchema.Group());
        })
        .DoIf(columnSchema.StableName().Defined(), [&] (TFluentMap fluent) {
            fluent.Item("stable_name").Value(*columnSchema.StableName());
        })
        .DoIf(columnSchema.Deleted().Defined(), [&] (TFluentMap fluent) {
            fluent.Item("deleted").Value(*columnSchema.Deleted());
        })
    .EndMap();
}

void Deserialize(TColumnSchema& columnSchema, const TNode& node)
{
    const auto& nodeMap = node.AsMap();
    DESERIALIZE_ITEM("name", columnSchema.Name_);
    DESERIALIZE_ITEM("type_v3", columnSchema.RawTypeV3_);
    DESERIALIZE_ITEM("sort_order", columnSchema.SortOrder_);
    DESERIALIZE_ITEM("lock", columnSchema.Lock_);
    DESERIALIZE_ITEM("expression", columnSchema.Expression_);
    DESERIALIZE_ITEM("aggregate", columnSchema.Aggregate_);
    DESERIALIZE_ITEM("group", columnSchema.Group_);
    DESERIALIZE_ITEM("stable_name", columnSchema.StableName_);
    DESERIALIZE_ITEM("deleted", columnSchema.Deleted_);

    if (nodeMap.contains("type_v3")) {
        NTi::TTypePtr type;
        DESERIALIZE_ITEM("type_v3", type);
        columnSchema.Type(type);
    } else {
        EValueType oldType = VT_INT64;
        bool required = false;
        DESERIALIZE_ITEM("type", oldType);
        DESERIALIZE_ITEM("required", required);
        columnSchema.Type(ToTypeV3(oldType, required));
    }
}

void Serialize(const TTableSchema& tableSchema, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer).BeginAttributes()
        .Item("strict").Value(tableSchema.Strict())
        .Item("unique_keys").Value(tableSchema.UniqueKeys())
    .EndAttributes()
    .List(tableSchema.Columns());
}

void Deserialize(TTableSchema& tableSchema, const TNode& node)
{
    const auto& attributesMap = node.GetAttributes().AsMap();
    DESERIALIZE_ATTR("strict", tableSchema.Strict_);
    DESERIALIZE_ATTR("unique_keys", tableSchema.UniqueKeys_);
    Deserialize(tableSchema.Columns_, node);
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TKeyBound& keyBound, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer).BeginList()
        .Item().Value(ToString(keyBound.Relation()))
        .Item().Value(keyBound.Key())
    .EndList();
}

void Deserialize(TKeyBound& keyBound, const TNode& node)
{
    const auto& nodeList = node.AsList();
    Y_ENSURE(nodeList.size() == 2);

    const auto& relationNode = nodeList[0];
    keyBound.Relation(::FromString<ERelation>(relationNode.AsString()));

    const auto& keyNode = nodeList[1];
    TKey key;
    Deserialize(key, keyNode);
    keyBound.Key(std::move(key));
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TReadLimit& readLimit, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer).BeginMap()
        .DoIf(readLimit.KeyBound_.Defined(), [&] (TFluentMap fluent) {
            fluent.Item("key_bound").Value(*readLimit.KeyBound_);
        })
        .DoIf(readLimit.Key_.Defined(), [&] (TFluentMap fluent) {
            fluent.Item("key").Value(*readLimit.Key_);
        })
        .DoIf(readLimit.RowIndex_.Defined(), [&] (TFluentMap fluent) {
            fluent.Item("row_index").Value(*readLimit.RowIndex_);
        })
        .DoIf(readLimit.Offset_.Defined(), [&] (TFluentMap fluent) {
            fluent.Item("offset").Value(*readLimit.Offset_);
        })
        .DoIf(readLimit.TabletIndex_.Defined(), [&] (TFluentMap fluent) {
            fluent.Item("tablet_index").Value(*readLimit.TabletIndex_);
        })
    .EndMap();
}

void Deserialize(TReadLimit& readLimit, const TNode& node)
{
    const auto& nodeMap = node.AsMap();
    DESERIALIZE_ITEM("key_bound", readLimit.KeyBound_);
    DESERIALIZE_ITEM("key", readLimit.Key_);
    DESERIALIZE_ITEM("row_index", readLimit.RowIndex_);
    DESERIALIZE_ITEM("offset", readLimit.Offset_);
    DESERIALIZE_ITEM("tablet_index", readLimit.TabletIndex_);
}

void Serialize(const TReadRange& readRange, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer).BeginMap()
        .DoIf(!IsTrivial(readRange.LowerLimit_), [&] (TFluentMap fluent) {
            fluent.Item("lower_limit").Value(readRange.LowerLimit_);
        })
        .DoIf(!IsTrivial(readRange.UpperLimit_), [&] (TFluentMap fluent) {
            fluent.Item("upper_limit").Value(readRange.UpperLimit_);
        })
        .DoIf(!IsTrivial(readRange.Exact_), [&] (TFluentMap fluent) {
            fluent.Item("exact").Value(readRange.Exact_);
        })
    .EndMap();
}

void Deserialize(TReadRange& readRange, const TNode& node)
{
    const auto& nodeMap = node.AsMap();
    DESERIALIZE_ITEM("lower_limit", readRange.LowerLimit_);
    DESERIALIZE_ITEM("upper_limit", readRange.UpperLimit_);
    DESERIALIZE_ITEM("exact", readRange.Exact_);
}

void Serialize(const THashMap<TString, TString>& renameColumns, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .DoMapFor(renameColumns, [] (TFluentMap fluent, const auto& item) {
            fluent.Item(item.first).Value(item.second);
        });
}

void Serialize(const TRichYPath& path, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer).BeginAttributes()
        .DoIf(path.GetRanges().Defined(), [&] (TFluentAttributes fluent) {
            fluent.Item("ranges").List(*path.GetRanges());
        })
        .DoIf(path.Columns_.Defined(), [&] (TFluentAttributes fluent) {
            fluent.Item("columns").Value(*path.Columns_);
        })
        .DoIf(path.Append_.Defined(), [&] (TFluentAttributes fluent) {
            fluent.Item("append").Value(*path.Append_);
        })
        .DoIf(path.PartiallySorted_.Defined(), [&] (TFluentAttributes fluent) {
            fluent.Item("partially_sorted").Value(*path.PartiallySorted_);
        })
        .DoIf(!path.SortedBy_.Parts_.empty(), [&] (TFluentAttributes fluent) {
            fluent.Item("sorted_by").Value(path.SortedBy_);
        })
        .DoIf(path.Teleport_.Defined(), [&] (TFluentAttributes fluent) {
            fluent.Item("teleport").Value(*path.Teleport_);
        })
        .DoIf(path.Primary_.Defined(), [&] (TFluentAttributes fluent) {
            fluent.Item("primary").Value(*path.Primary_);
        })
        .DoIf(path.Foreign_.Defined(), [&] (TFluentAttributes fluent) {
            fluent.Item("foreign").Value(*path.Foreign_);
        })
        .DoIf(path.RowCountLimit_.Defined(), [&] (TFluentAttributes fluent) {
            fluent.Item("row_count_limit").Value(*path.RowCountLimit_);
        })
        .DoIf(path.FileName_.Defined(), [&] (TFluentAttributes fluent) {
            fluent.Item("file_name").Value(*path.FileName_);
        })
        .DoIf(path.OriginalPath_.Defined(), [&] (TFluentAttributes fluent) {
            fluent.Item("original_path").Value(*path.OriginalPath_);
        })
        .DoIf(path.Executable_.Defined(), [&] (TFluentAttributes fluent) {
            fluent.Item("executable").Value(*path.Executable_);
        })
        .DoIf(path.Format_.Defined(), [&] (TFluentAttributes fluent) {
            fluent.Item("format").Value(*path.Format_);
        })
        .DoIf(path.Schema_.Defined(), [&] (TFluentAttributes fluent) {
            fluent.Item("schema").Value(*path.Schema_);
        })
        .DoIf(path.Timestamp_.Defined(), [&] (TFluentAttributes fluent) {
            fluent.Item("timestamp").Value(*path.Timestamp_);
        })
        .DoIf(path.CompressionCodec_.Defined(), [&] (TFluentAttributes fluent) {
            fluent.Item("compression_codec").Value(*path.CompressionCodec_);
        })
        .DoIf(path.ErasureCodec_.Defined(), [&] (TFluentAttributes fluent) {
            fluent.Item("erasure_codec").Value(ToString(*path.ErasureCodec_));
        })
        .DoIf(path.SchemaModification_.Defined(), [&] (TFluentAttributes fluent) {
            fluent.Item("schema_modification").Value(ToString(*path.SchemaModification_));
        })
        .DoIf(path.OptimizeFor_.Defined(), [&] (TFluentAttributes fluent) {
            fluent.Item("optimize_for").Value(ToString(*path.OptimizeFor_));
        })
        .DoIf(path.TransactionId_.Defined(), [&] (TFluentAttributes fluent) {
            fluent.Item("transaction_id").Value(GetGuidAsString(*path.TransactionId_));
        })
        .DoIf(path.RenameColumns_.Defined(), [&] (TFluentAttributes fluent) {
            fluent.Item("rename_columns").Value(*path.RenameColumns_);
        })
        .DoIf(path.BypassArtifactCache_.Defined(), [&] (TFluentAttributes fluent) {
            fluent.Item("bypass_artifact_cache").Value(*path.BypassArtifactCache_);
        })
    .EndAttributes()
    .Value(path.Path_);
}

void Deserialize(TRichYPath& path, const TNode& node)
{
    path = {};

    const auto& attributesMap = node.GetAttributes().AsMap();
    DESERIALIZE_ATTR("ranges", path.MutableRanges());
    DESERIALIZE_ATTR("columns", path.Columns_);
    DESERIALIZE_ATTR("append", path.Append_);
    DESERIALIZE_ATTR("partially_sorted", path.PartiallySorted_);
    DESERIALIZE_ATTR("sorted_by", path.SortedBy_);
    DESERIALIZE_ATTR("teleport", path.Teleport_);
    DESERIALIZE_ATTR("primary", path.Primary_);
    DESERIALIZE_ATTR("foreign", path.Foreign_);
    DESERIALIZE_ATTR("row_count_limit", path.RowCountLimit_);
    DESERIALIZE_ATTR("file_name", path.FileName_);
    DESERIALIZE_ATTR("original_path", path.OriginalPath_);
    DESERIALIZE_ATTR("executable", path.Executable_);
    DESERIALIZE_ATTR("format", path.Format_);
    DESERIALIZE_ATTR("schema", path.Schema_);
    DESERIALIZE_ATTR("timestamp", path.Timestamp_);
    DESERIALIZE_ATTR("compression_codec", path.CompressionCodec_);
    DESERIALIZE_ATTR("erasure_codec", path.ErasureCodec_);
    DESERIALIZE_ATTR("schema_modification", path.SchemaModification_);
    DESERIALIZE_ATTR("optimize_for", path.OptimizeFor_);
    DESERIALIZE_ATTR("transaction_id", path.TransactionId_);
    DESERIALIZE_ATTR("rename_columns", path.RenameColumns_);
    DESERIALIZE_ATTR("bypass_artifact_cache", path.BypassArtifactCache_);
    Deserialize(path.Path_, node);
}

void Serialize(const TAttributeFilter& filter, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer).List(filter.Attributes_);
}

void Deserialize(TTableColumnarStatistics& statistics, const TNode& node)
{
    const auto& nodeMap = node.AsMap();
    DESERIALIZE_ITEM("column_data_weights", statistics.ColumnDataWeight);
    DESERIALIZE_ITEM("column_estimated_unique_counts", statistics.ColumnEstimatedUniqueCounts);
    DESERIALIZE_ITEM("legacy_chunks_data_weight", statistics.LegacyChunksDataWeight);
    DESERIALIZE_ITEM("timestamp_total_weight", statistics.TimestampTotalWeight);
}

void Deserialize(TMultiTablePartition::TStatistics& statistics, const TNode& node)
{
    const auto& nodeMap = node.AsMap();
    DESERIALIZE_ITEM("chunk_count", statistics.ChunkCount);
    DESERIALIZE_ITEM("data_weight", statistics.DataWeight);
    DESERIALIZE_ITEM("row_count", statistics.RowCount);
}

void Deserialize(TMultiTablePartition& partition, const TNode& node)
{
    const auto& nodeMap = node.AsMap();
    DESERIALIZE_ITEM("table_ranges", partition.TableRanges);
    DESERIALIZE_ITEM("aggregate_statistics", partition.AggregateStatistics);
}

void Deserialize(TMultiTablePartitions& partitions, const TNode& node)
{
    const auto& nodeMap = node.AsMap();
    DESERIALIZE_ITEM("partitions", partitions.Partitions);
}

void Serialize(const TGUID& value, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer).Value(GetGuidAsString(value));
}

void Deserialize(TGUID& value, const TNode& node)
{
    value = GetGuid(node.AsString());
}

void Deserialize(TTabletInfo& value, const TNode& node)
{
    auto nodeMap = node.AsMap();
    DESERIALIZE_ITEM("total_row_count", value.TotalRowCount)
    DESERIALIZE_ITEM("trimmed_row_count", value.TrimmedRowCount)
    DESERIALIZE_ITEM("barrier_timestamp", value.BarrierTimestamp)
}

void Serialize(const NTi::TTypePtr& type, NYson::IYsonConsumer* consumer)
{
    auto yson = NTi::NIo::SerializeYson(type.Get());
    ::NYson::ParseYsonStringBuffer(yson, consumer);
}

void Deserialize(NTi::TTypePtr& type, const TNode& node)
{
    auto yson = NodeToYsonString(node, NYson::EYsonFormat::Binary);
    type = NTi::NIo::DeserializeYson(*NTi::HeapFactory(), yson);
}

#undef DESERIALIZE_ITEM
#undef DESERIALIZE_ATTR

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
