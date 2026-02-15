#include "create_table_formatter.h"
#include "formatters_common.h"

#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/formats/arrow/serializer/parsing.h>
#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>
#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/core/ydb_convert/ydb_convert.h>

#include <ydb/public/lib/ydb_cli/dump/util/query_utils.h>

#include <yql/essentials/minikql/mkql_type_ops.h>

#include <library/cpp/json/json_writer.h>
#include <library/cpp/protobuf/json/proto2json.h>

#include <util/generic/yexception.h>

namespace NKikimr {
namespace NSysView {

using namespace NKikimrSchemeOp;
using namespace Ydb::Table;
using namespace NYdb;

namespace {
    const ui64 defaultSizeToSplit = 2ul << 30; // 2048 Mb
}

void TCreateTableFormatter::FormatValue(NYdb::TValueParser& parser, bool isPartition, TString del) {
    TGuard<NMiniKQL::TScopedAlloc> guard(Alloc);
    switch (parser.GetKind()) {
        case NYdb::TTypeParser::ETypeKind::Primitive: {
            Stream << del;
            FormatPrimitive(parser);
            return;
        }
        case NYdb::TTypeParser::ETypeKind::Optional: {
            parser.OpenOptional();
            if (parser.IsNull()) {
                if (!isPartition) {
                    Stream << del << "NULL";
                }
            } else {
                FormatValue(parser, isPartition, del);
            }
            parser.CloseOptional();
            return;
        }
        case NYdb::TTypeParser::ETypeKind::Tuple: {
            parser.OpenTuple();
            bool first = true;
            Stream << "(";
            while (parser.TryNextElement()) {
                if (!first) {
                    FormatValue(parser, isPartition, ", ");
                } else {
                    FormatValue(parser, isPartition);
                }
                first = false;
            }
            Stream << ")";
            parser.CloseTuple();
            return;
        }
        case TTypeParser::ETypeKind::Decimal: {
            auto decimal = parser.GetDecimal();
            auto precision = decimal.DecimalType_.Precision;
            auto scale = decimal.DecimalType_.Scale;
            Stream << "CAST(";
            EscapeString(decimal.ToString(), Stream);
            Stream << " AS Decimal(" << ui32(precision) << "," << ui32(scale) << ")";
            Stream << ")";
            return;
        }
        default:
            ythrow TFormatFail(Ydb::StatusIds::UNSUPPORTED, "Unsupported value type for SHOW CREATE TABLE");
    }
}
void TCreateTableFormatter::FormatPrimitive(NYdb::TValueParser& parser) {
    switch (parser.GetPrimitiveType()) {
        case NYdb::EPrimitiveType::Bool: {
            if (parser.GetBool()) {
                Stream << "true";
            } else {
                Stream << "false";
            }
            break;
        }
        case NYdb::EPrimitiveType::Int8: {
            const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Int8, NUdf::TUnboxedValuePod(parser.GetInt8()));
            Y_ENSURE(str.HasValue());
            Stream << TString(str.AsStringRef());
            break;
        }
        case NYdb::EPrimitiveType::Uint8: {
            const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Uint8, NUdf::TUnboxedValuePod(parser.GetUint8()));
            Y_ENSURE(str.HasValue());
            Stream << TString(str.AsStringRef());
            break;
        }
        case NYdb::EPrimitiveType::Int16: {
            const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Int16, NUdf::TUnboxedValuePod(parser.GetInt16()));
            Y_ENSURE(str.HasValue());
            Stream << TString(str.AsStringRef());
            break;
        }
        case NYdb::EPrimitiveType::Uint16: {
            const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Uint16, NUdf::TUnboxedValuePod(parser.GetUint16()));
            Y_ENSURE(str.HasValue());
            Stream << TString(str.AsStringRef());
            break;
        }
        case NYdb::EPrimitiveType::Int32: {
            const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Int32, NUdf::TUnboxedValuePod(parser.GetInt32()));
            Y_ENSURE(str.HasValue());
            Stream << TString(str.AsStringRef());
            break;
        }
        case NYdb::EPrimitiveType::Uint32: {
            const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Uint32, NUdf::TUnboxedValuePod(parser.GetUint32()));
            Y_ENSURE(str.HasValue());
            Stream << TString(str.AsStringRef());
            break;
        }
        case NYdb::EPrimitiveType::Int64: {
            const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Int64, NUdf::TUnboxedValuePod(static_cast<i64>(parser.GetInt64())));
            Y_ENSURE(str.HasValue());
            Stream << TString(str.AsStringRef());
            break;
        }
        case NYdb::EPrimitiveType::Uint64: {
            const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Uint64, NUdf::TUnboxedValuePod(static_cast<ui64>(parser.GetUint64())));
            Y_ENSURE(str.HasValue());
            Stream << TString(str.AsStringRef());
            break;
        }
        case NYdb::EPrimitiveType::Float: {
            const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Float, NUdf::TUnboxedValuePod(parser.GetFloat()));
            Y_ENSURE(str.HasValue());
            Stream << TString(str.AsStringRef());
            break;
        }
        case NYdb::EPrimitiveType::Double: {
            const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Double, NUdf::TUnboxedValuePod(parser.GetDouble()));
            Y_ENSURE(str.HasValue());
            Stream << TString(str.AsStringRef());
            break;
        }
        case NYdb::EPrimitiveType::Utf8: {
            EscapeString(TString(parser.GetUtf8()), Stream);
            break;
        }
        case NYdb::EPrimitiveType::Date: {
            Stream << "DATE(";
            EscapeString(parser.GetDate().FormatGmTime("%Y-%m-%d"), Stream);
            Stream << ")";
            break;
        }
        case NYdb::EPrimitiveType::Datetime: {
            Stream << "DATETIME(";
            EscapeString(parser.GetDatetime().ToStringUpToSeconds(), Stream);
            Stream << ")";
            break;
        }
        case NYdb::EPrimitiveType::Timestamp: {
            Stream << "TIMESTAMP(";
            EscapeString(parser.GetTimestamp().ToString(), Stream);
            Stream << ")";
            break;
        }
        case NYdb::EPrimitiveType::Interval: {
            Stream << "INTERVAL(";
            const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Interval, NUdf::TUnboxedValuePod(static_cast<i64>(parser.GetInterval())));
            Y_ENSURE(str.HasValue());
            EscapeString(TString(str.AsStringRef()), Stream);
            Stream << ")";
            break;
        }
        case NYdb::EPrimitiveType::Date32: {
            Stream << "DATE32(";
            const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Date32, NUdf::TUnboxedValuePod(parser.GetDate32().time_since_epoch().count()));
            Y_ENSURE(str.HasValue());
            EscapeString(TString(str.AsStringRef()), Stream);
            Stream << ")";
            break;
        }
        case NYdb::EPrimitiveType::Datetime64: {
            Stream << "DATETIME64(";
            const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Datetime64, NUdf::TUnboxedValuePod(static_cast<i64>(parser.GetDatetime64().time_since_epoch().count())));
            Y_ENSURE(str.HasValue());
            EscapeString(TString(str.AsStringRef()), Stream);
            Stream << ")";
            break;
        }
        case NYdb::EPrimitiveType::Timestamp64: {
            Stream << "TIMESTAMP64(";
            const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Timestamp64, NUdf::TUnboxedValuePod(static_cast<i64>(parser.GetTimestamp64().time_since_epoch().count())));
            Y_ENSURE(str.HasValue());
            EscapeString(TString(str.AsStringRef()), Stream);
            Stream << ")";
            break;
        }
        case NYdb::EPrimitiveType::Interval64: {
            Stream << "INTERVAL64(";
            const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Interval64, NUdf::TUnboxedValuePod(static_cast<i64>(parser.GetInterval64().count())));
            Y_ENSURE(str.HasValue());
            EscapeString(TString(str.AsStringRef()), Stream);
            Stream << ")";
            break;
        }
        case NYdb::EPrimitiveType::String:
            EscapeString(TString(parser.GetString()), Stream);
            break;
        case NYdb::EPrimitiveType::Yson:
            EscapeString(TString(parser.GetYson()), Stream);
            break;
        case NYdb::EPrimitiveType::Json:
            EscapeString(TString(parser.GetJson()), Stream);
            break;
        case NYdb::EPrimitiveType::DyNumber: {
            Stream << "DyNumber(";
            EscapeString(TString(parser.GetDyNumber()), Stream);
            Stream << ")";
            break;
        }
        case NYdb::EPrimitiveType::Uuid: {
            Stream << "UUID(";
            EscapeString(TString(parser.GetUuid().ToString()), Stream);
            Stream << ")";
            break;
        }
        default:
            ythrow TFormatFail(Ydb::StatusIds::UNSUPPORTED, "Unsupported primitive type for SHOW CREATE TABLE");
    }
}

void TCreateTableFormatter::Format(const Ydb::TypedValue& value, bool isPartition) {
    NYdb::TValueParser parser(NYdb::TValue(value.type(), value.value()));
    FormatValue(parser, isPartition);
}

class TStringStreamWrapper {
public:
    TStringStreamWrapper(TStringStream& stream)
        : Stream(stream)
    {}

    ~TStringStreamWrapper() {
        Stream.Clear();
    }

private:
    TStringStream& Stream;
};

TFormatResult TCreateTableFormatter::Format(const TString& tablePath, const TString& fullPath, const NKikimrSchemeOp::TTableDescription& tableDesc,
        bool temporary, const THashMap<TString, THolder<NKikimrSchemeOp::TPersQueueGroupDescription>>& persQueues,
        const THashMap<TPathId, THolder<NSequenceProxy::TEvSequenceProxy::TEvGetSequenceResult>>& sequences) {
    Stream.Clear();

    TStringStreamWrapper wrapper(Stream);

    Ydb::Table::CreateTableRequest createRequest;
    if (temporary) {
        Stream << "CREATE TEMPORARY TABLE ";
    } else {
        Stream << "CREATE TABLE ";
    }
    EscapeName(tablePath, Stream);
    Stream << " (\n";

    NKikimrMiniKQL::TType mkqlKeyType;
    try {
        FillColumnDescription(createRequest, mkqlKeyType, tableDesc);
    } catch (const yexception& e) {
        return TFormatResult(Ydb::StatusIds::UNSUPPORTED, e.what());
    }

    Y_ENSURE(!tableDesc.GetColumns().empty());
    Y_ENSURE(tableDesc.GetColumns().size() == createRequest.columns().size());

    std::map<ui32, const TColumnDescription*> columns;

    for (const auto& column : tableDesc.GetColumns()) {
        columns[column.GetId()] = &column;
    }

    try {
        auto it = columns.cbegin();
        Format(*it->second);
        std::advance(it, 1);
        for (; it != columns.end(); ++it) {
            Stream << ",\n";
            Format(*it->second);
        }
    } catch (const TFormatFail& ex) {
        return TFormatResult(ex.Status, ex.Error);
    } catch (const yexception& e) {
        return TFormatResult(Ydb::StatusIds::UNSUPPORTED, e.what());
    }

    try {
        FillTableBoundary(createRequest, tableDesc, mkqlKeyType);
        FillIndexDescription(createRequest, tableDesc);
    } catch (const yexception& e) {
        return TFormatResult(Ydb::StatusIds::UNSUPPORTED, e.what());;
    }

    if (!createRequest.indexes().empty()) {
        Stream << ",\n";
        try {
            Format(createRequest.indexes(0));
            for (int i = 1; i < createRequest.indexes().size(); i++) {
                Stream << ",\n";
                Format(createRequest.indexes(i));
            }
        } catch (const TFormatFail& ex) {
            return TFormatResult(ex.Status, ex.Error);
        } catch (const yexception& e) {
            return TFormatResult(Ydb::StatusIds::UNSUPPORTED, e.what());
        }
    }
    Stream << ",\n";

    bool isFamilyPrinted = false;
    if (tableDesc.HasPartitionConfig()) {
        const auto partitionConfig = tableDesc.GetPartitionConfig();

        if (!partitionConfig.GetColumnFamilies().empty()) {
            try {
                isFamilyPrinted = Format(partitionConfig.GetColumnFamilies(0));
                for (int i = 1; i < partitionConfig.GetColumnFamilies().size(); i++) {
                    if (isFamilyPrinted) {
                        Stream << ",\n";
                    }
                    isFamilyPrinted = Format(partitionConfig.GetColumnFamilies(i));
                }
            } catch (const TFormatFail& ex) {
                return TFormatResult(ex.Status, ex.Error);
            } catch (const yexception& e) {
                return TFormatResult(Ydb::StatusIds::UNSUPPORTED, e.what());
            }
        }
    }

    Y_ENSURE(!tableDesc.GetKeyColumnIds().empty());
    if (isFamilyPrinted) {
        Stream << ",\n";
    }
    Stream << "\tPRIMARY KEY (";
    EscapeName(columns[tableDesc.GetKeyColumnIds(0)]->GetName(), Stream);
    for (int i = 1; i < tableDesc.GetKeyColumnIds().size(); i++) {
        Stream << ", ";
        EscapeName(columns[tableDesc.GetKeyColumnIds(i)]->GetName(), Stream);
    }
    Stream << ")\n";
    Stream << ")";

    TString del = "";
    bool printed = false;

    if (tableDesc.HasPartitionConfig()) {
        if (tableDesc.GetPartitionConfig().HasPartitioningPolicy()) {
            ui32 shardsToCreate = NSchemeShard::TTableInfo::ShardsToCreate(tableDesc);
            printed |= Format(tableDesc.GetPartitionConfig().GetPartitioningPolicy(), shardsToCreate, del, !printed);
        }
    }

    if (createRequest.partitions_case() == Ydb::Table::CreateTableRequest::kPartitionAtKeys) {
        try {
            printed |= Format(createRequest.partition_at_keys(), del, !printed);
        } catch (const TFormatFail& ex) {
            return TFormatResult(ex.Status, ex.Error);
        } catch (const yexception& e) {
            return TFormatResult(Ydb::StatusIds::UNSUPPORTED, e.what());
        }
    }

    FillReadReplicasSettings(createRequest, tableDesc);

    if (createRequest.has_read_replicas_settings()) {
        printed |= Format(createRequest.read_replicas_settings(), del, !printed);
    }

    FillKeyBloomFilter(createRequest, tableDesc);

    if (createRequest.key_bloom_filter() == Ydb::FeatureFlag::ENABLED) {
        if (!printed) {
            Stream << " WITH (\n";
        }
        Stream << del << "\tKEY_BLOOM_FILTER = ENABLED";
        printed = true;
        del = ",\n";
    } else if (createRequest.key_bloom_filter() == Ydb::FeatureFlag::DISABLED) {
        if (!printed) {
            Stream << " WITH (\n";
        }
        Stream << del << "\tKEY_BLOOM_FILTER = DISABLED";
        printed = true;
        del = ",\n";
    }

    if (createRequest.has_ttl_settings()) {
        try {
            printed |= Format(createRequest.ttl_settings(), del, !printed);
        } catch (const TFormatFail& ex) {
            return TFormatResult(ex.Status, ex.Error);
        } catch (const yexception& e) {
            return TFormatResult(Ydb::StatusIds::UNSUPPORTED, e.what());
        }
    }

    if (printed) {
        Stream << "\n)";
    }

    Stream << ";";

    if (!tableDesc.GetCdcStreams().empty()) {
        Y_ENSURE((ui32)tableDesc.GetCdcStreams().size() == persQueues.size());
        auto firstColumnTypeId = columns[tableDesc.GetKeyColumnIds(0)]->GetTypeId();
        try {
            for (int i = 0; i < tableDesc.GetCdcStreams().size(); i++) {
                Format(tablePath, tableDesc.GetCdcStreams(i), persQueues, firstColumnTypeId);
            }
        } catch (const TFormatFail& ex) {
            return TFormatResult(ex.Status, ex.Error);
        } catch (const yexception& e) {
            return TFormatResult(Ydb::StatusIds::UNSUPPORTED, e.what());
        }
    }

    if (!tableDesc.GetSequences().empty()) {
        try {
            for (int i = 0; i < tableDesc.GetSequences().size(); i++) {
                Format(fullPath, tableDesc.GetSequences(i), sequences);
            }
        } catch (const TFormatFail& ex) {
            return TFormatResult(ex.Status, ex.Error);
        } catch (const yexception& e) {
            return TFormatResult(Ydb::StatusIds::INTERNAL_ERROR, e.what());
        }
    }

    if (!tableDesc.GetTableIndexes().empty()) {
        try {
            for (const auto& indexDesc: tableDesc.GetTableIndexes()) {
                if (indexDesc.GetType() != NKikimrSchemeOp::EIndexType::EIndexTypeGlobalVectorKmeansTree) {
                    FormatIndexImplTable(tablePath, indexDesc.GetName(), indexDesc.GetIndexImplTableDescriptions(0));
                }
            }
        } catch (const TFormatFail& ex) {
            return TFormatResult(ex.Status, ex.Error);
        } catch (const yexception& e) {
            return TFormatResult(Ydb::StatusIds::INTERNAL_ERROR, e.what());
        }
    }

    TString createQuery = Stream.Str();
    TString formattedCreateQuery;
    NYql::TIssues issues;
    if (!NYdb::NDump::Format(createQuery, formattedCreateQuery, issues)) {
        return TFormatResult(Ydb::StatusIds::INTERNAL_ERROR, issues.ToString());
    }

    auto result = TFormatResult(std::move(formattedCreateQuery));

    return result;
}

void TCreateTableFormatter::Format(const NKikimrSchemeOp::TColumnDescription& columnDesc) {
    Stream << "\t";
    EscapeName(columnDesc.GetName(), Stream);
    Stream << " ";

    auto type = columnDesc.GetType();
    std::optional<Ydb::TypedValue> defaultFromLiteral;
    switch (columnDesc.GetDefaultValueCase()) {
        case NKikimrSchemeOp::TColumnDescription::kDefaultFromLiteral: {
            defaultFromLiteral = columnDesc.GetDefaultFromLiteral();
            break;
        }
        case NKikimrSchemeOp::TColumnDescription::kDefaultFromSequence: {
            auto lowerType = to_lower(type);
            if (lowerType == "int64") {
                type = "Serial8";
            } else if (lowerType == "int32") {
                type = "Serial4";
            } else if (lowerType == "int16") {
                type = "Serial2";
            }
            break;
        }
        default: break;
    }

    Stream << type;

    if (columnDesc.HasFamilyName()) {
        Stream << " FAMILY ";
        EscapeName(columnDesc.GetFamilyName(), Stream);
    }
    if (columnDesc.GetNotNull()) {
        Stream << " NOT NULL";
    }
    if (defaultFromLiteral) {
        Stream << " DEFAULT ";
        Format(defaultFromLiteral.value());
    }
}

void TCreateTableFormatter::Format(const TableIndex& index) {
    Stream << "\tINDEX ";
    EscapeName(index.name(), Stream);
    std::optional<KMeansTreeSettings> kMeansTreeSettings;
    std::optional<FulltextIndexSettings> fulltextIndexSettings;
    switch (index.type_case()) {
        case TableIndex::kGlobalIndex: {
            Stream << " GLOBAL SYNC ON ";
            break;
        }
        case TableIndex::kGlobalAsyncIndex: {
            Stream << " GLOBAL ASYNC ON ";
            break;
        }
        case TableIndex::kGlobalUniqueIndex: {
            Stream << " GLOBAL UNIQUE SYNC ON ";
            break;
        }
        case TableIndex::kGlobalVectorKmeansTreeIndex: {
            Stream << " GLOBAL USING vector_kmeans_tree ON ";
            kMeansTreeSettings = index.global_vector_kmeans_tree_index().vector_settings();
            break;
        }
        case Ydb::Table::TableIndex::kGlobalFulltextPlainIndex: {
            Stream << " GLOBAL USING fulltext_plain ON ";
            fulltextIndexSettings = index.global_fulltext_plain_index().fulltext_settings();
            break;
        }
        case Ydb::Table::TableIndex::kGlobalFulltextRelevanceIndex: {
            Stream << " GLOBAL USING fulltext_relevance ON ";
            fulltextIndexSettings = index.global_fulltext_relevance_index().fulltext_settings();
            break;
        }
        case Ydb::Table::TableIndex::TYPE_NOT_SET:
            ythrow TFormatFail(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected Ydb::Table::TableIndex::TYPE_NOT_SET");
    }

    Y_ENSURE(!index.index_columns().empty());
    Stream << "(";
    EscapeName(index.index_columns(0), Stream);
    for (int i = 1; i < index.index_columns().size(); i++) {
        Stream << ", ";
        EscapeName(index.index_columns(i), Stream);
    }
    Stream << ")";

    if (!index.data_columns().empty()) {
        Stream << " COVER ";
        Stream << "(";
        EscapeName(index.data_columns(0), Stream);
        for (int i = 1; i < index.data_columns().size(); i++) {
            Stream << ", ";
            EscapeName(index.data_columns(i), Stream);
        }
        Stream << ")";
    }

    if (kMeansTreeSettings) {
        Stream << " WITH (";

        switch (kMeansTreeSettings->settings().metric()) {
            case Ydb::Table::VectorIndexSettings::SIMILARITY_INNER_PRODUCT:
                Stream << "similarity=product";
                break;
            case Ydb::Table::VectorIndexSettings::SIMILARITY_COSINE:
                Stream << "similarity=cosine";
                break;
            case Ydb::Table::VectorIndexSettings::DISTANCE_COSINE:
                Stream << "distance=cosine";
                break;
            case Ydb::Table::VectorIndexSettings::DISTANCE_MANHATTAN:
                Stream << "distance=manhattan";
                break;
            case Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN:
                Stream << "distance=euclidean";
                break;
            default:
                ythrow TFormatFail(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected Ydb::Table::VectorIndexSettings");
        }

        TString del = "";
        if (kMeansTreeSettings->settings().metric() != Ydb::Table::VectorIndexSettings::METRIC_UNSPECIFIED) {
            del = ", ";
        }

        switch (kMeansTreeSettings->settings().vector_type()) {
            case Ydb::Table::VectorIndexSettings::VECTOR_TYPE_BIT:
                Stream << del << "vector_type=\"bit\"";
                break;
            case Ydb::Table::VectorIndexSettings::VECTOR_TYPE_INT8:
                Stream << del << "vector_type=\"int8\"";
                break;
            case Ydb::Table::VectorIndexSettings::VECTOR_TYPE_UINT8:
                Stream << del << "vector_type=\"uint8\"";
                break;
            case Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT:
                Stream << del << "vector_type=\"float\"";
                break;
            default:
                ythrow TFormatFail(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected Ydb::Table::VectorIndexSettings");
        }

        if (kMeansTreeSettings->settings().vector_type() != Ydb::Table::VectorIndexSettings::VECTOR_TYPE_UNSPECIFIED) {
            del = ", ";
        }

        if (kMeansTreeSettings->settings().vector_dimension() != 0) {
            Stream << del << "vector_dimension=" << kMeansTreeSettings->settings().vector_dimension();
            del = ", ";
        }

        if (kMeansTreeSettings->clusters() != 0) {
            Stream << del << "clusters=" << kMeansTreeSettings->clusters();
            del = ", ";
        }

        if (kMeansTreeSettings->levels() != 0) {
            Stream << del << "levels=" << kMeansTreeSettings->levels();
            del = ", ";
        }

        if (kMeansTreeSettings->overlap_clusters() != 0) {
            Stream << del << "overlap_clusters=" << kMeansTreeSettings->overlap_clusters();
            del = ", ";
        }

        if (kMeansTreeSettings->overlap_ratio() != 0) {
            Stream << del << "overlap_ratio=\"" << kMeansTreeSettings->overlap_ratio() << "\"";
            del = ", ";
        }

        Stream << ")";
    }

    if (fulltextIndexSettings) {
        Stream << " WITH (";

        Y_ENSURE(fulltextIndexSettings->columns().size() == 1);
        auto analyzers = fulltextIndexSettings->columns().at(0).analyzers();
        Y_ENSURE(analyzers.has_tokenizer());
        Stream << "tokenizer=";
        switch (analyzers.tokenizer()) {
            case Ydb::Table::FulltextIndexSettings_Tokenizer_WHITESPACE:
                Stream << "whitespace";
                break;
            case Ydb::Table::FulltextIndexSettings_Tokenizer_STANDARD:
                Stream << "standard";
                break;
            case Ydb::Table::FulltextIndexSettings_Tokenizer_KEYWORD:
                Stream << "keyword";
                break;
            default:
                ythrow TFormatFail(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected Ydb::Table::FulltextIndexSettings::Tokenizer");
        }
        if (analyzers.has_language()) {
            Stream << ", language=" << analyzers.language();
        }
        if (analyzers.has_use_filter_lowercase()) {
            Stream << ", use_filter_lowercase=" << (analyzers.use_filter_lowercase() ? "true" : "false");
        }
        if (analyzers.has_use_filter_stopwords()) {
            Stream << ", use_filter_stopwords=" << (analyzers.use_filter_stopwords() ? "true" : "false");
        }
        if (analyzers.has_use_filter_ngram()) {
            Stream << ", use_filter_ngram=" << (analyzers.use_filter_ngram() ? "true" : "false");
        }
        if (analyzers.has_use_filter_edge_ngram()) {
            Stream << ", use_filter_edge_ngram=" << (analyzers.use_filter_edge_ngram() ? "true" : "false");
        }
        if (analyzers.has_filter_ngram_min_length()) {
            Stream << ", filter_ngram_min_length=" << analyzers.filter_ngram_min_length();
        }
        if (analyzers.has_filter_ngram_max_length()) {
            Stream << ", filter_ngram_max_length=" << analyzers.filter_ngram_max_length();
        }
        if (analyzers.has_use_filter_length()) {
            Stream << ", use_filter_length=" << (analyzers.use_filter_length() ? "true" : "false");
        }
        if (analyzers.has_filter_length_min()) {
            Stream << ", filter_length_min=" << analyzers.filter_length_min();
        }
        if (analyzers.has_filter_length_max()) {
            Stream << ", filter_length_max=" << analyzers.filter_length_max();
        }

        Stream << ")";
    }
}

bool TCreateTableFormatter::Format(const TFamilyDescription& familyDesc) {
    TString familyName;
    if (familyDesc.HasName() && !familyDesc.GetName().empty()) {
        familyName = familyDesc.GetName();
    } else if (familyDesc.HasId() && familyDesc.GetId() != 0) {
        ythrow TFormatFail(Ydb::StatusIds::UNSUPPORTED, "Family by id is not supported");
    } else {
        familyName = "default";
    }

    TString dataName;
    if (familyDesc.HasStorageConfig() && familyDesc.GetStorageConfig().HasData()) {
        const auto& data = familyDesc.GetStorageConfig().GetData();
        if (!data.GetAllowOtherKinds()) {
            if (!data.GetPreferredPoolKind().empty()) {
                dataName = data.GetPreferredPoolKind();
            }
        }
    }

    TString compression;
    if (familyDesc.HasColumnCodec()) {
        switch (familyDesc.GetColumnCodec()) {
            case NKikimrSchemeOp::ColumnCodecPlain:
                compression = "off";
                break;
            case NKikimrSchemeOp::ColumnCodecLZ4:
                compression = "lz4";
                break;
            case NKikimrSchemeOp::ColumnCodecZSTD:
                compression = "zstd";
                break;
        }
    } else if (familyDesc.HasCodec()) {
        if (familyDesc.GetCodec() == 1) {
            compression  = "lz4";
        } else {
            compression = "off";
        }
    }

    TString cacheMode;
    if (familyDesc.HasColumnCacheMode()) {
        switch (familyDesc.GetColumnCacheMode()) {
            case NKikimrSchemeOp::ColumnCacheModeRegular:
                cacheMode = "regular";
                break;
            case NKikimrSchemeOp::ColumnCacheModeTryKeepInMemory:
                cacheMode = "in_memory";
                break;
        }
    }

    if (familyName == "default") {
        if (!dataName && !compression && !cacheMode) {
            return false;
        }
    }

    Y_ENSURE(familyName);

    Stream << "\tFAMILY ";
    EscapeName(familyName, Stream);
    Stream << " (";

    TString del = "";
    if (dataName) {
        Stream << "DATA = " << "\"" << dataName << "\"";
        del = ", ";
    }

    if (compression) {
        Stream << del << "COMPRESSION = " << "\"" << compression << "\"";
        del = ", ";
    }

    if (cacheMode) {
        Stream << del << "CACHE_MODE = " << "\"" << cacheMode << "\"";
    }

    Stream << ")";
    return true;
}

bool TCreateTableFormatter::Format(const NKikimrSchemeOp::TPartitioningPolicy& policy, ui32 shardsToCreate, TString& del, bool needWith) {
    bool printed = false;
    if (policy.HasSizeToSplit()) {
        if (needWith) {
            Stream << " WITH (\n";
            needWith = false;
        }
        if (policy.GetSizeToSplit()) {
            Stream << del << "\tAUTO_PARTITIONING_BY_SIZE = ENABLED,\n";
            auto partitionBySize = policy.GetSizeToSplit() / (1 << 20);
            Stream << "\tAUTO_PARTITIONING_PARTITION_SIZE_MB = " << partitionBySize;
        } else {
            Stream << del << "\tAUTO_PARTITIONING_BY_SIZE = DISABLED";
        }
        del = ",\n";
        printed = true;
    }

    if (policy.HasSplitByLoadSettings()) {
        if (needWith) {
            Stream << " WITH (\n";
            needWith = false;
        }
        if (policy.GetSplitByLoadSettings().GetEnabled()) {
            Stream << del << "\tAUTO_PARTITIONING_BY_LOAD = ENABLED";
        } else {
            Stream << del << "\tAUTO_PARTITIONING_BY_LOAD = DISABLED";
        }
        del = ",\n";
        printed = true;
    }

    if (policy.HasMinPartitionsCount() && policy.GetMinPartitionsCount() && policy.GetMinPartitionsCount() != shardsToCreate) {
        if (needWith) {
            Stream << " WITH (\n";
            needWith = false;
        }
        Stream << del << "\tAUTO_PARTITIONING_MIN_PARTITIONS_COUNT = " << policy.GetMinPartitionsCount();
        del = ",\n";
        printed = true;
    }

    if (policy.HasMaxPartitionsCount() && policy.GetMaxPartitionsCount()) {
        if (needWith) {
            Stream << " WITH (\n";
        }
        Stream << del << "\tAUTO_PARTITIONING_MAX_PARTITIONS_COUNT = " << policy.GetMaxPartitionsCount();
        del = ",\n";
        printed = true;
    }
    return printed;
}

bool TCreateTableFormatter::Format(const Ydb::Table::ExplicitPartitions& explicitPartitions, TString& del, bool needWith) {
    if (explicitPartitions.split_points().empty()) {
        return false;
    }
    if (needWith) {
        Stream << " WITH (\n";
    }
    Stream << del << "\tPARTITION_AT_KEYS = (";
    del = ",\n";
    Format(explicitPartitions.split_points(0), true);
    for (int i = 1; i < explicitPartitions.split_points().size(); i++) {
        Stream << ", ";
        Format(explicitPartitions.split_points(i), true);
    }
    Stream << ")";
    return true;
}

bool TCreateTableFormatter::Format(const Ydb::Table::ReadReplicasSettings& readReplicasSettings, TString& del, bool needWith) {
    switch (readReplicasSettings.settings_case()) {
        case Ydb::Table::ReadReplicasSettings::kPerAzReadReplicasCount:
        {
            if (needWith) {
                Stream << " WITH (\n";
            }
            Stream << del << "\tREAD_REPLICAS_SETTINGS = \"PER_AZ:" << readReplicasSettings.per_az_read_replicas_count() << "\"";
            del = ",\n";
            return true;
        }
        case Ydb::Table::ReadReplicasSettings::kAnyAzReadReplicasCount:
        {
            if (needWith) {
                Stream << " WITH (\n";
            }
            Stream << del << "\tREAD_REPLICAS_SETTINGS = \"ANY_AZ:" << readReplicasSettings.any_az_read_replicas_count() << "\"";
            del = ",\n";
            return true;
        }
        default:
            break;
    }
    return false;
}

void TCreateTableFormatter::Format(ui64 expireAfterSeconds, std::optional<TString> storage) {
    TGuard<NMiniKQL::TScopedAlloc> guard(Alloc);
    Stream << "INTERVAL(";
    const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Interval, NUdf::TUnboxedValuePod(expireAfterSeconds * 1000000));
    Y_ENSURE(str.HasValue());
    EscapeString(TString(str.AsStringRef()), Stream);
    Stream << ") ";
    if (storage) {
        Stream << "TO EXTERNAL DATA SOURCE ";
        EscapeName(*storage, Stream);
    } else {
        Stream << "DELETE";
    }
}

bool TCreateTableFormatter::Format(const Ydb::Table::TtlSettings& ttlSettings, TString& del, bool needWith) {
    if (needWith) {
        Stream << " WITH (\n";
    }
    Stream << del;
    Stream << "\tTTL =\n\t  ";
    del = ",\n";
    bool first = true;
    std::optional<TString> columnName;
    std::optional<Ydb::Table::ValueSinceUnixEpochModeSettings::Unit> columnUnit;
    std::optional<Ydb::Table::TtlTier::ExpressionCase> expressionType;
    switch (ttlSettings.mode_case()) {
        case Ydb::Table::TtlSettings::kDateTypeColumn: {
            const auto& mode = ttlSettings.date_type_column();
            columnName = mode.column_name();
            Format(mode.expire_after_seconds());
            break;
        }
        case Ydb::Table::TtlSettings::kValueSinceUnixEpoch: {
            const auto& mode = ttlSettings.value_since_unix_epoch();
            columnName = mode.column_name();
            columnUnit = mode.column_unit();
            Format(mode.expire_after_seconds());
            break;
        }
        case Ydb::Table::TtlSettings::kTieredTtl: {
            if (!ttlSettings.tiered_ttl().tiers_size()) {
                ythrow TFormatFail(Ydb::StatusIds::INTERNAL_ERROR, "No tiers in TTL settings");
            }

            for (const auto& tier : ttlSettings.tiered_ttl().tiers()) {
                TString tierColumnName;
                ui32 expireAfterSeconds;
                switch (tier.expression_case()) {
                    case Ydb::Table::TtlTier::kDateTypeColumn: {
                        const auto& mode = tier.date_type_column();
                        expireAfterSeconds = mode.expire_after_seconds();
                        tierColumnName = mode.column_name();
                        break;
                    }
                    case Ydb::Table::TtlTier::kValueSinceUnixEpoch: {
                        const auto& mode = tier.value_since_unix_epoch();
                        tierColumnName = mode.column_name();
                        expireAfterSeconds = mode.expire_after_seconds();
                        if (columnUnit) {
                            if (*columnUnit != mode.column_unit()) {
                                ythrow TFormatFail(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder()
                                    << "Unit of the TTL columns must be the same for all tiers: "
                                    << Ydb::Table::ValueSinceUnixEpochModeSettings::Unit_Name(*columnUnit)
                                    << " != " << Ydb::Table::ValueSinceUnixEpochModeSettings::Unit_Name(mode.column_unit()));
                            }
                        } else {
                            columnUnit = mode.column_unit();
                        }
                        break;
                    }
                    case Ydb::Table::TtlTier::EXPRESSION_NOT_SET:
                        ythrow TFormatFail(Ydb::StatusIds::INTERNAL_ERROR, "Tier expression is undefined");
                }

                if (columnName) {
                    if (*columnName != tierColumnName) {
                        ythrow TFormatFail(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "TTL columns must be the same for all tiers: " << *columnName << " != " << tierColumnName);
                    }
                } else {
                    columnName = tierColumnName;
                }

                if (expressionType) {
                    if (*expressionType != tier.expression_case()) {
                        ythrow TFormatFail(Ydb::StatusIds::INTERNAL_ERROR, "Expression type must be the same for all tiers");
                    }
                } else {
                    expressionType = tier.expression_case();
                }

                if (!first) {
                    Stream << ", ";
                }

                switch (tier.action_case()) {
                    case Ydb::Table::TtlTier::kDelete:
                        Format(expireAfterSeconds);
                        break;
                    case Ydb::Table::TtlTier::kEvictToExternalStorage:
                        Format(expireAfterSeconds, tier.evict_to_external_storage().storage());
                        break;
                    case Ydb::Table::TtlTier::ACTION_NOT_SET:
                        ythrow TFormatFail(Ydb::StatusIds::INTERNAL_ERROR, "Tier action is undefined");
                }
                first = false;
            }
        } break;

        case Ydb::Table::TtlSettings::MODE_NOT_SET:
            ythrow TFormatFail(Ydb::StatusIds::INTERNAL_ERROR, "TTL mode is undefined");
    }

    Stream << "\n\t  ON " << columnName;
    if (columnUnit) {
        Stream << " AS ";
        switch (*columnUnit) {
            case Ydb::Table::ValueSinceUnixEpochModeSettings::UNIT_SECONDS:
                Stream << "SECONDS";
                break;
            case Ydb::Table::ValueSinceUnixEpochModeSettings::UNIT_MILLISECONDS:
                Stream << "MILLISECONDS";
                break;
            case Ydb::Table::ValueSinceUnixEpochModeSettings::UNIT_MICROSECONDS:
                Stream << "MICROSECONDS";
                break;
            case Ydb::Table::ValueSinceUnixEpochModeSettings::UNIT_NANOSECONDS:
                Stream << "NANOSECONDS";
                break;
            default:
                ythrow TFormatFail(Ydb::StatusIds::INTERNAL_ERROR, "Unsupported unit");
        }
    }
    return true;
}

void TCreateTableFormatter::Format(const TString& tablePath, const NKikimrSchemeOp::TCdcStreamDescription& cdcStream,
        const THashMap<TString, THolder<NKikimrSchemeOp::TPersQueueGroupDescription>>& persQueues, ui32 firstColumnTypeId) {
    Stream << "ALTER TABLE ";
    EscapeName(tablePath, Stream);
    Stream << "\n\t";
    auto persQueuePath = JoinPath({tablePath, cdcStream.GetName(), "streamImpl"});
    auto it = persQueues.find(persQueuePath);
    if (it == persQueues.end() || !it->second) {
        ythrow TFormatFail(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected topic path");
    }
    const auto& persQueue = *it->second;

    Stream << "ADD CHANGEFEED ";
    EscapeName(cdcStream.GetName(), Stream);
    Stream << " WITH (";

    TString del = "";
    switch (cdcStream.GetMode()) {
        case NKikimrSchemeOp::ECdcStreamMode::ECdcStreamModeKeysOnly: {
            Stream << "MODE = \'KEYS_ONLY\'";
            del = ", ";
            break;
        }
        case NKikimrSchemeOp::ECdcStreamMode::ECdcStreamModeUpdate: {
            Stream << "MODE = \'UPDATES\'";
            del = ", ";
            break;
        }
        case NKikimrSchemeOp::ECdcStreamMode::ECdcStreamModeNewImage: {
            Stream << "MODE = \'NEW_IMAGE\'";
            del = ", ";
            break;
        }
        case NKikimrSchemeOp::ECdcStreamMode::ECdcStreamModeOldImage: {
            Stream << "MODE = \'OLD_IMAGE\'";
            del = ", ";
            break;
        }
        case NKikimrSchemeOp::ECdcStreamMode::ECdcStreamModeNewAndOldImages: {
            Stream << "MODE = \'NEW_AND_OLD_IMAGES\'";
            del = ", ";
            break;
        }
        default:
            ythrow TFormatFail(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected cdc stream mode");
    }

    switch (cdcStream.GetFormat()) {
        case NKikimrSchemeOp::ECdcStreamFormat::ECdcStreamFormatJson: {
            Stream << del << "FORMAT = \'JSON\'";
            del = ", ";
            break;
        }
        case NKikimrSchemeOp::ECdcStreamFormat::ECdcStreamFormatDebeziumJson: {
            Stream << del << "FORMAT = \'DEBEZIUM_JSON\'";
            del = ", ";
            break;
        }
        case NKikimrSchemeOp::ECdcStreamFormat::ECdcStreamFormatDynamoDBStreamsJson: {
            Stream << del << "FORMAT = \'DYNAMODB_STREAMS_JSON\'";
            del = ", ";
            break;
        }
        default:
            ythrow TFormatFail(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected cdc stream format");
    }

    if (cdcStream.GetVirtualTimestamps()) {
        Stream << del << "VIRTUAL_TIMESTAMPS = TRUE";
        del = ", ";
    }

    if (cdcStream.GetSchemaChanges()) {
        Stream << del << "SCHEMA_CHANGES = TRUE";
        del = ", ";
    }

    if (cdcStream.HasAwsRegion() && !cdcStream.GetAwsRegion().empty()) {
        Stream << del << "AWS_REGION = \'" << cdcStream.GetAwsRegion() << "\'";
        del = ", ";
    }

    const auto& pqConfig = persQueue.GetPQTabletConfig();
    const auto& partitionConfig = pqConfig.GetPartitionConfig();

    if (partitionConfig.HasLifetimeSeconds()) {
        Stream << del << "RETENTION_PERIOD = ";
        TGuard<NMiniKQL::TScopedAlloc> guard(Alloc);
        Stream << "INTERVAL(";
        ui64 retentionPeriod = partitionConfig.GetLifetimeSeconds();
        const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Interval, NUdf::TUnboxedValuePod(retentionPeriod * 1000000));
        Y_ENSURE(str.HasValue());
        EscapeString(TString(str.AsStringRef()), Stream);
        Stream << ")";
        del = ", ";
    }

    if (persQueue.HasTotalGroupCount()) {
        switch (firstColumnTypeId) {
            case NScheme::NTypeIds::Uint32:
            case NScheme::NTypeIds::Uint64:
            case NScheme::NTypeIds::Uuid: {
                Stream << del << "TOPIC_MIN_ACTIVE_PARTITIONS = ";
                Stream << persQueue.GetTotalGroupCount();
                del = ", ";
                break;
            }
        }
    }

    if (cdcStream.GetState() == NKikimrSchemeOp::ECdcStreamState::ECdcStreamStateScan || cdcStream.HasScanProgress()) {
        Stream << del << "INITIAL_SCAN = TRUE";
    }

    Stream << ");";
}

void TCreateTableFormatter::Format(const TString& tablePath, const NKikimrSchemeOp::TSequenceDescription& sequence, const THashMap<TPathId, THolder<NSequenceProxy::TEvSequenceProxy::TEvGetSequenceResult>>& sequences) {
    auto it = sequences.find(TPathId::FromProto(sequence.GetPathId()));
    if (it == sequences.end() || !it->second) {
        ythrow TFormatFail(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected sequence path id");
    }
    const auto& getSequenceResult = *it->second;

    if (getSequenceResult.StartValue == 1 && getSequenceResult.Increment == 1
            && getSequenceResult.NextValue == 1) {
        return;
    }

    Stream << "ALTER SEQUENCE ";
    auto sequencePath = JoinPath({tablePath, sequence.GetName()});
    EscapeName(sequencePath, Stream);

    if (getSequenceResult.StartValue != 1) {
        Stream << " START WITH " << getSequenceResult.StartValue;
    }

    if (getSequenceResult.Increment != 1) {
        Stream << " INCREMENT BY " << getSequenceResult.Increment;
    }

    if (getSequenceResult.NextValue != 1) {
        if (getSequenceResult.NextValue == getSequenceResult.StartValue) {
            Stream << " RESTART";
        } else {
            Stream << " RESTART WITH " << getSequenceResult.NextValue;
        }
    }

    Stream << ";";
}

void TCreateTableFormatter::FormatIndexImplTable(const TString& tablePath, const TString& indexName, const NKikimrSchemeOp::TTableDescription& indexImplDesc) {
    if (!indexImplDesc.HasPartitionConfig() || !indexImplDesc.GetPartitionConfig().HasPartitioningPolicy()) {
        return;
    }

    const auto& policy = indexImplDesc.GetPartitionConfig().GetPartitioningPolicy();

    ui32 shardsToCreate = NSchemeShard::TTableInfo::ShardsToCreate(indexImplDesc);

    bool printed = false;
    if ((policy.HasSizeToSplit() && (policy.GetSizeToSplit() != defaultSizeToSplit)) || policy.HasSplitByLoadSettings()
            || (policy.HasMinPartitionsCount() && policy.GetMinPartitionsCount() && policy.GetMinPartitionsCount() != shardsToCreate)
            || (policy.HasMaxPartitionsCount() && policy.GetMaxPartitionsCount())) {
        printed = true;
    }
    if (!printed) {
        return;
    }

    Stream << "ALTER TABLE ";
    EscapeName(tablePath, Stream);
    Stream << " ALTER INDEX ";
    EscapeName(indexName, Stream);

    Stream << " SET (\n";

    TString del = "";
    if (policy.HasSizeToSplit()) {
        if (policy.GetSizeToSplit()) {
            if (policy.GetSizeToSplit() != defaultSizeToSplit) {
                Stream << "\tAUTO_PARTITIONING_BY_SIZE = ENABLED,\n";
                auto partitionBySize = policy.GetSizeToSplit() / (1 << 20);
                Stream << "\tAUTO_PARTITIONING_PARTITION_SIZE_MB = " << partitionBySize;
                del = ",\n";
            }
        } else {
            Stream << "\tAUTO_PARTITIONING_BY_SIZE = DISABLED";
            del = ",\n";
        }
    }

    if (policy.HasSplitByLoadSettings()) {
        if (policy.GetSplitByLoadSettings().GetEnabled()) {
            Stream << del << "\tAUTO_PARTITIONING_BY_LOAD = ENABLED";
        } else {
            Stream << del << "\tAUTO_PARTITIONING_BY_LOAD = DISABLED";
        }
        del = ",\n";
    }

    if (policy.HasMinPartitionsCount() && policy.GetMinPartitionsCount() && policy.GetMinPartitionsCount() != shardsToCreate) {
        Stream << del << "\tAUTO_PARTITIONING_MIN_PARTITIONS_COUNT = " << policy.GetMinPartitionsCount();
        del = ",\n";
    }

    if (policy.HasMaxPartitionsCount() && policy.GetMaxPartitionsCount()) {
        Stream << del << "\tAUTO_PARTITIONING_MAX_PARTITIONS_COUNT = " << policy.GetMaxPartitionsCount();
    }

    Stream << "\n);";
}


TFormatResult TCreateTableFormatter::Format(const TString& tablePath, const TString& fullPath, const TColumnTableDescription& tableDesc, bool temporary) {
    Stream.Clear();

    TStringStreamWrapper wrapper(Stream);

    Ydb::Table::CreateTableRequest createRequest;
    if (temporary) {
        Stream << "CREATE TEMPORARY TABLE ";
    } else {
        Stream << "CREATE TABLE ";
    }
    EscapeName(tablePath, Stream);
    Stream << " (\n";

    const auto& schema = tableDesc.GetSchema();

    std::map<ui32, const TOlapColumnDescription*> columns;
    for (const auto& column : schema.GetColumns()) {
        columns[column.GetId()] = &column;
    }

    try {
        auto it = columns.cbegin();
        Format(*it->second);
        std::advance(it, 1);
        for (; it != columns.end(); ++it) {
            Stream << ",\n";
            Format(*it->second);
        }
    } catch (const TFormatFail& ex) {
        return TFormatResult(ex.Status, ex.Error);
    } catch (const yexception& e) {
        return TFormatResult(Ydb::StatusIds::UNSUPPORTED, e.what());
    }
    Stream << ",\n";

    std::map<ui32, const TFamilyDescription*> families;
    for (const auto& family : schema.GetColumnFamilies()) {
        families[family.GetId()] = &family;
    }

    bool isFamilyPrinted = false;
    if (!schema.GetColumnFamilies().empty()) {
        try {
            isFamilyPrinted = Format(schema.GetColumnFamilies(0));
            for (int i = 1; i < schema.GetColumnFamilies().size(); i++) {
                if (isFamilyPrinted) {
                    Stream << ",\n";
                }
                isFamilyPrinted = Format(schema.GetColumnFamilies(i));
            }
        } catch (const TFormatFail& ex) {
            return TFormatResult(ex.Status, ex.Error);
        } catch (const yexception& e) {
            return TFormatResult(Ydb::StatusIds::UNSUPPORTED, e.what());
        }
    }

    Y_ENSURE(!schema.GetKeyColumnNames().empty());
    if (isFamilyPrinted) {
        Stream << ",\n";
    }
    Stream << "\tPRIMARY KEY (";
    EscapeName(schema.GetKeyColumnNames(0), Stream);
    for (int i = 1; i < schema.GetKeyColumnNames().size(); i++) {
        Stream << ", ";
        EscapeName(schema.GetKeyColumnNames(i), Stream);
    }
    Stream << ")\n";
    Stream << ") ";

    if (tableDesc.HasSharding()) {
        Format(tableDesc.GetSharding());
    }

    Stream << "WITH (\n";
    Stream << "\tSTORE = COLUMN";

    if (tableDesc.HasColumnShardCount()) {
        Stream << ",\n";
        Stream << "\tAUTO_PARTITIONING_MIN_PARTITIONS_COUNT = " << tableDesc.GetColumnShardCount();
    }

    if (tableDesc.HasTtlSettings()) {
        Format(tableDesc.GetTtlSettings());
    }

    Stream << "\n);";

    try {
        for (const auto& column: columns) {
            FormatAlterColumn(fullPath, *column.second);
        }
    } catch (const TFormatFail& ex) {
        return TFormatResult(ex.Status, ex.Error);
    } catch (const yexception& e) {
        return TFormatResult(Ydb::StatusIds::UNSUPPORTED, e.what());
    }

    if (!schema.GetIndexes().empty()) {
        try {
            for (const auto& index : schema.GetIndexes()) {
                FormatUpsertIndex(fullPath, index, columns);
            }
        } catch (const TFormatFail& ex) {
            return TFormatResult(ex.Status, ex.Error);
        } catch (const yexception& e) {
            return TFormatResult(Ydb::StatusIds::UNSUPPORTED, e.what());
        }
    }

    if (schema.HasOptions()) {
        try {
            FormatUpsertOptions(fullPath, schema.GetOptions());
        } catch (const TFormatFail& ex) {
            return TFormatResult(ex.Status, ex.Error);
        } catch (const yexception& e) {
            return TFormatResult(Ydb::StatusIds::UNSUPPORTED, e.what());
        }
    }

    TString createQuery = Stream.Str();
    TString formattedCreateQuery;
    NYql::TIssues issues;
    if (!NYdb::NDump::Format(createQuery, formattedCreateQuery, issues)) {
        return TFormatResult(Ydb::StatusIds::INTERNAL_ERROR, issues.ToString());
    }

    auto result = TFormatResult(std::move(formattedCreateQuery));

    return result;
}

void TCreateTableFormatter::Format(const TOlapColumnDescription& olapColumnDesc) {
    Stream << "\t";
    EscapeName(olapColumnDesc.GetName(), Stream);
    Stream << " " << olapColumnDesc.GetType();

    if (olapColumnDesc.GetNotNull()) {
        Stream << " NOT NULL";
    }

    if (olapColumnDesc.HasStorageId() && !olapColumnDesc.GetStorageId().empty()) {
        ythrow TFormatFail(Ydb::StatusIds::UNSUPPORTED, "Unsupported setting: STORAGE_ID");
    }

    if (olapColumnDesc.HasSerializer()) {
        Stream << " COMPRESSION (";
        auto compression = olapColumnDesc.GetSerializer();
        if (compression.HasArrowCompression()) {
            if (compression.GetArrowCompression().HasCodec()) {
                Stream << "algorithm=";
                switch (compression.GetArrowCompression().GetCodec()) {
                    case NKikimrSchemeOp::ColumnCodecPlain:
                        Stream << "off";
                        break;
                    case NKikimrSchemeOp::ColumnCodecLZ4:
                        Stream << "lz4";
                        break;
                    case NKikimrSchemeOp::ColumnCodecZSTD:
                        Stream << "zstd";
                        break;
                }
            }
            if (compression.GetArrowCompression().HasLevel()) {
                Stream << ", level=" << compression.GetArrowCompression().GetLevel();
            }
        }
        Stream << ')';
    }
}

TString TCreateTableFormatter::ValueToString(const NKikimrColumnShardColumnDefaults::TColumnDefault& defaultValue) {
    TStringStream stream;
    if (!defaultValue.HasScalar()) {
        return "";
    }

    TGuard<NMiniKQL::TScopedAlloc> guard(Alloc);
    const auto& scalar = defaultValue.GetScalar();
    if (scalar.HasBool()) {
        if (scalar.GetBool() == true) {
            stream << "true";
        } else {
            stream << "false";
        }
    } else if (scalar.HasUint8()) {
        const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Uint8, NUdf::TUnboxedValuePod(scalar.GetUint8()));
        Y_ENSURE(str.HasValue());
        stream << TString(str.AsStringRef());
    } else if (scalar.HasUint16()) {
        const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Uint16, NUdf::TUnboxedValuePod(scalar.GetUint16()));
        Y_ENSURE(str.HasValue());
        stream << TString(str.AsStringRef());
    } else if (scalar.HasUint32()) {
        const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Uint32, NUdf::TUnboxedValuePod(scalar.GetUint32()));
        Y_ENSURE(str.HasValue());
        stream << TString(str.AsStringRef());
    } else if (scalar.HasUint64()) {
        const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Uint64, NUdf::TUnboxedValuePod(static_cast<ui64>(scalar.GetUint64())));
        Y_ENSURE(str.HasValue());
        stream << TString(str.AsStringRef());
    } else if (scalar.HasInt8()) {
        const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Int8, NUdf::TUnboxedValuePod(scalar.GetInt8()));
        Y_ENSURE(str.HasValue());
        stream << TString(str.AsStringRef());
    } else if (scalar.HasInt16()) {
        const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Int16, NUdf::TUnboxedValuePod(scalar.GetInt16()));
        Y_ENSURE(str.HasValue());
        stream << TString(str.AsStringRef());
    } else if (scalar.HasInt32()) {
        const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Int32, NUdf::TUnboxedValuePod(scalar.GetInt32()));
        Y_ENSURE(str.HasValue());
        stream << TString(str.AsStringRef());
    } else if (scalar.HasInt64()) {
        const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Int64, NUdf::TUnboxedValuePod(static_cast<i64>(scalar.GetInt64())));
        Y_ENSURE(str.HasValue());
        stream << TString(str.AsStringRef());
    } else if (scalar.HasDouble()) {
        const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Double, NUdf::TUnboxedValuePod(scalar.GetDouble()));
        Y_ENSURE(str.HasValue());
        stream << TString(str.AsStringRef());
    } else if (scalar.HasFloat()) {
        const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Float, NUdf::TUnboxedValuePod(scalar.GetFloat()));
        Y_ENSURE(str.HasValue());
        stream << TString(str.AsStringRef());
    } else if (scalar.HasTimestamp()) {
        ui64 value = scalar.GetTimestamp().GetValue();
        arrow::TimeUnit::type unit = arrow::TimeUnit::type(scalar.GetTimestamp().GetUnit());
        switch (unit) {
            case arrow::TimeUnit::SECOND:
                value *= 1000000;
                break;
            case arrow::TimeUnit::MILLI:
                value *= 1000;
                break;
            case arrow::TimeUnit::MICRO:
                break;
            case arrow::TimeUnit::NANO:
                value /= 1000;
                break;
        }
        stream << "TIMESTAMP(";
        const NUdf::TUnboxedValue str = NMiniKQL::ValueToString(NUdf::EDataSlot::Timestamp, NUdf::TUnboxedValuePod(value));
        Y_ENSURE(str.HasValue());
        EscapeString(TString(str.AsStringRef()), stream);
        stream << ")";
    } else if (scalar.HasString()) {
        EscapeString(TString(scalar.GetString()), stream);
    } else {
        ythrow TFormatFail(Ydb::StatusIds::UNSUPPORTED, "Unsupported type for default value");
    }

    return stream.Str();
}

void TCreateTableFormatter::Format(const NKikimrSchemeOp::TColumnTableSharding& sharding) {
    switch (sharding.GetMethodCase()) {
        case NKikimrSchemeOp::TColumnTableSharding::kHashSharding: {
            const auto& hashSharding = sharding.GetHashSharding();
            Y_ENSURE(!hashSharding.GetColumns().empty());
            Stream << "PARTITION BY HASH(";
            EscapeName(hashSharding.GetColumns(0), Stream);
            for (int i = 1; i < hashSharding.GetColumns().size(); i++) {
                Stream << ", ";
                EscapeName(hashSharding.GetColumns(i), Stream);
            }
            Stream << ")\n";
            break;
        }
        case NKikimrSchemeOp::TColumnTableSharding::kRandomSharding:
            ythrow TFormatFail(Ydb::StatusIds::UNSUPPORTED, "Random sharding is not supported yet.");
        default:
            ythrow TFormatFail(Ydb::StatusIds::INTERNAL_ERROR, "Unsupported unit");
    }
}

void TCreateTableFormatter::Format(const NKikimrSchemeOp::TColumnDataLifeCycle& ttlSettings) {
    if (!ttlSettings.HasEnabled()) {
        return;
    }

    const auto& enabled = ttlSettings.GetEnabled();

    if (enabled.HasExpireAfterBytes()) {
        ythrow TFormatFail(Ydb::StatusIds::UNSUPPORTED, "TTL by size is not supported.");
    }

    Stream << ",\n";
    Stream << "\tTTL =\n\t  ";
    bool first = true;

    if (!enabled.TiersSize()) {
        Y_ENSURE(enabled.HasExpireAfterSeconds());
        Format(enabled.GetExpireAfterSeconds());
    } else {
        for (const auto& tier : enabled.GetTiers()) {
            if (!first) {
                Stream << ", ";
            }
            switch (tier.GetActionCase()) {
                case NKikimrSchemeOp::TTTLSettings::TTier::ActionCase::kDelete:
                    Format(tier.GetApplyAfterSeconds());
                    break;
                case NKikimrSchemeOp::TTTLSettings::TTier::ActionCase::kEvictToExternalStorage:
                    Format(tier.GetApplyAfterSeconds(), tier.GetEvictToExternalStorage().GetStorage());
                    break;
                case NKikimrSchemeOp::TTTLSettings::TTier::ActionCase::ACTION_NOT_SET:
                    ythrow TFormatFail(Ydb::StatusIds::UNSUPPORTED, "Undefined tier action");
            }
            first = false;
        }
    }

    Stream << "\n\t  ON " << enabled.GetColumnName();
    switch (enabled.GetColumnUnit()) {
        case NKikimrSchemeOp::TTTLSettings::UNIT_AUTO:
            break;
        case NKikimrSchemeOp::TTTLSettings::UNIT_SECONDS:
            Stream << " AS SECONDS";
            break;
        case NKikimrSchemeOp::TTTLSettings::UNIT_MILLISECONDS:
            Stream << " AS MILLISECONDS";
            break;
        case NKikimrSchemeOp::TTTLSettings::UNIT_MICROSECONDS:
            Stream << " AS MICROSECONDS";
            break;
        case NKikimrSchemeOp::TTTLSettings::UNIT_NANOSECONDS:
            Stream << " AS NANOSECONDS";
            break;
        default:
            ythrow TFormatFail(Ydb::StatusIds::INTERNAL_ERROR, "Unsupported unit");
    }
}

void TCreateTableFormatter::FormatAlterColumn(const TString& fullPath, const NKikimrSchemeOp::TOlapColumnDescription& columnDesc) {
    TStringStream paramsStr;
    TString del = "";

    if (columnDesc.HasDefaultValue()) {
        auto defaultValue = ValueToString(columnDesc.GetDefaultValue());
        if (defaultValue) {
            EscapeName("DEFAULT_VALUE", paramsStr);
            paramsStr << "=";
            EscapeValue(defaultValue, paramsStr);
            del = ", ";
        }
    }

    if (columnDesc.HasStorageId() && !columnDesc.GetStorageId().empty()) {
        paramsStr << del;
        EscapeName("STORAGE_ID", paramsStr);
        paramsStr << "=";
        EscapeValue(columnDesc.GetStorageId(), paramsStr);
        del = ", ";
    }

    if (columnDesc.HasDataAccessorConstructor()) {
        const auto& dataAccessorConstructor = columnDesc.GetDataAccessorConstructor();
        if (columnDesc.GetDataAccessorConstructor().HasClassName()
                && !columnDesc.GetDataAccessorConstructor().GetClassName().empty()) {
            paramsStr << del;
            EscapeName("DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME", paramsStr);
            paramsStr << "=";
            EscapeValue(columnDesc.GetDataAccessorConstructor().GetClassName(), paramsStr);
            del = ", ";
            if (dataAccessorConstructor.HasSubColumns()) {
                const auto& subColumns = dataAccessorConstructor.GetSubColumns();
                if (subColumns.HasSettings()) {
                    const auto& settings = subColumns.GetSettings();
                    if (settings.HasSparsedDetectorKff() && settings.GetSparsedDetectorKff()) {
                        paramsStr << del;
                        EscapeName("SPARSED_DETECTOR_KFF", paramsStr);
                        paramsStr << "=";
                        EscapeValue(settings.GetSparsedDetectorKff(), paramsStr);
                        del = ", ";
                    }
                    if (settings.HasColumnsLimit() && settings.GetColumnsLimit()) {
                        paramsStr << del;
                        EscapeName("COLUMNS_LIMIT", paramsStr);
                        paramsStr << "=";
                        EscapeValue(settings.GetColumnsLimit(), paramsStr);
                        del = ", ";
                    }
                    if (settings.HasChunkMemoryLimit() && settings.GetChunkMemoryLimit()) {
                        paramsStr << del;
                        EscapeName("MEM_LIMIT_CHUNK", paramsStr);
                        paramsStr << "=";
                        EscapeValue(settings.GetChunkMemoryLimit(), paramsStr);
                        del = ", ";
                    }
                    if (settings.HasOthersAllowedFraction() && settings.GetOthersAllowedFraction()) {
                        paramsStr << del;
                        EscapeName("OTHERS_ALLOWED_FRACTION", paramsStr);
                        paramsStr << "=";
                        EscapeValue(settings.GetOthersAllowedFraction(), paramsStr);
                        del = ", ";
                    }
                    if (settings.HasDataExtractor()) {
                        const auto& dataExtractor = settings.GetDataExtractor();
                        if (dataExtractor.HasClassName() && !dataExtractor.GetClassName().empty()) {
                            paramsStr << del;
                            EscapeName("DATA_EXTRACTOR_CLASS_NAME", paramsStr);
                            paramsStr << "=";
                            EscapeValue(dataExtractor.GetClassName(), paramsStr);
                            del = ", ";
                        }
                        if (dataExtractor.HasJsonScanner()) {
                            const auto& jsonScanner = dataExtractor.GetJsonScanner();
                            paramsStr << del;
                            EscapeName("SCAN_FIRST_LEVEL_ONLY", paramsStr);
                            paramsStr << "=";
                            EscapeValue(jsonScanner.GetFirstLevelOnly(), paramsStr);
                            del = ", ";
                            paramsStr << del;
                            EscapeName("FORCE_SIMD_PARSING", paramsStr);
                            paramsStr << "=";
                            EscapeValue(jsonScanner.GetForceSIMDJsonParsing(), paramsStr);
                            del = ", ";
                        }
                        if (dataExtractor.HasSIMDJsonScanner()) {
                            const auto& simdJsonScanner = dataExtractor.GetSIMDJsonScanner();
                            paramsStr << del;
                            EscapeName("SCAN_FIRST_LEVEL_ONLY", paramsStr);
                            paramsStr << "=";
                            EscapeValue(simdJsonScanner.GetFirstLevelOnly(), paramsStr);
                            del = ", ";
                        }
                    }
                }
            }
        }
    }

    if (columnDesc.HasDictionaryEncoding()) {
        paramsStr << del;
        EscapeName("ENCODING.DICTIONARY.ENABLED", paramsStr);
        paramsStr << "=";
        EscapeValue(columnDesc.GetDictionaryEncoding().GetEnabled(), paramsStr);
        del = ", ";
    }

    TString params = paramsStr.Str();
    if (params.empty()) {
        return;
    }

    Stream << "ALTER OBJECT ";
    EscapeName(fullPath, Stream);
    Stream << " (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=";
    Stream << columnDesc.GetName();
    Stream << ", ";
    Stream << params;
    Stream << ");";
}

void TCreateTableFormatter::FormatUpsertIndex(const TString& fullPath, const NKikimrSchemeOp::TOlapIndexDescription& indexDesc,
        const std::map<ui32, const TOlapColumnDescription*>& columns) {
    Stream << "ALTER OBJECT ";
    EscapeName(fullPath, Stream);
    Stream << " (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=";
    Stream << indexDesc.GetName();
    switch (indexDesc.GetImplementationCase()) {
        case NKikimrSchemeOp::TOlapIndexDescription::kBloomFilter: {
            const auto& bloomFilter = indexDesc.GetBloomFilter();
            Stream << ", TYPE=BLOOM_FILTER, ";
            Stream << "FEATURES=";

            NJson::TJsonValue json;

            if (bloomFilter.GetColumnIds().size() != 1) {
                ythrow TFormatFail(Ydb::StatusIds::UNSUPPORTED,
                    TStringBuilder() << "Unsupported number of columns for BLOOM_FILTER index: " << bloomFilter.GetColumnIds().size());
            }
            const auto& columnName = columns.at(bloomFilter.GetColumnIds(0))->GetName();
            json["column_name"] = columnName;

            if (bloomFilter.HasDataExtractor()) {
                const auto& dataExtractor = bloomFilter.GetDataExtractor();
                NJson::TJsonValue jsonDataExtractor;
                jsonDataExtractor["class_name"] = dataExtractor.GetClassName();
                if (dataExtractor.HasSubColumn()) {
                    const auto& subColumn = dataExtractor.GetSubColumn();
                    if (subColumn.HasSubColumnName()) {
                        jsonDataExtractor["sub_column_name"] = subColumn.GetSubColumnName();
                    }
                }
                json["data_extractor"] = std::move(jsonDataExtractor);
            }

            if (bloomFilter.HasBitsStorage()) {
                const auto& bitsStorage = bloomFilter.GetBitsStorage();
                if (bitsStorage.HasClassName() && !bitsStorage.GetClassName().empty()) {
                    json["bits_storage_type"] = bitsStorage.GetClassName();
                }
            }

            if (bloomFilter.HasFalsePositiveProbability()) {
                json["false_positive_probability"] = bloomFilter.GetFalsePositiveProbability();
            }

            EscapeValue(NJson::WriteJson(json, /*formatOutput*/ false), Stream);
            break;
        }
        case NKikimrSchemeOp::TOlapIndexDescription::kMaxIndex: {
            const auto& maxIndex = indexDesc.GetMaxIndex();
            Stream << ", TYPE=MAX, ";
            Stream << "FEATURES=";
            NJson::TJsonValue json;

            if (maxIndex.HasColumnId()) {
                const auto& columnName = columns.at(maxIndex.GetColumnId())->GetName();
                json["column_name"] = columnName;
            } else {
                ythrow TFormatFail(Ydb::StatusIds::UNSUPPORTED,
                    TStringBuilder() << "ColumnId have to be in MAX index description");
            }

            EscapeValue(NJson::WriteJson(json, /*formatOutput*/ false), Stream);
            break;
        }
        case NKikimrSchemeOp::TOlapIndexDescription::kCountMinSketch: {
            const auto& countMinSketch = indexDesc.GetCountMinSketch();
            Stream << ", TYPE=COUNT_MIN_SKETCH, ";
            Stream << "FEATURES=";
            NJson::TJsonValue json;
            NJson::TJsonArray jsonColumnNames;
            for (const auto& columnId : countMinSketch.GetColumnIds()) {
                jsonColumnNames.AppendValue(columns.at(columnId)->GetName());
            }
            json["column_names"] = std::move(jsonColumnNames);

            EscapeValue(NJson::WriteJson(json, /*formatOutput*/ false), Stream);
            break;
        }
        case NKikimrSchemeOp::TOlapIndexDescription::kBloomNGrammFilter: {
            const auto& bloomNGrammFilter = indexDesc.GetBloomNGrammFilter();
            Stream << ", TYPE=BLOOM_NGRAMM_FILTER, ";
            Stream << "FEATURES=";

            NJson::TJsonValue json;

            if (bloomNGrammFilter.HasColumnId()) {
                const auto& columnName = columns.at(bloomNGrammFilter.GetColumnId())->GetName();
                json["column_name"] = columnName;
            } else {
                ythrow TFormatFail(Ydb::StatusIds::UNSUPPORTED,
                    TStringBuilder() << "ColumnId have to be in BLOOM_NGRAMM_FILTER index description");
            }

            if (bloomNGrammFilter.HasDataExtractor()) {
                const auto& dataExtractor = bloomNGrammFilter.GetDataExtractor();
                NJson::TJsonValue jsonDataExtractor;
                jsonDataExtractor["class_name"] = dataExtractor.GetClassName();
                if (dataExtractor.HasSubColumn()) {
                    const auto& subColumn = dataExtractor.GetSubColumn();
                    if (subColumn.HasSubColumnName()) {
                        jsonDataExtractor["sub_column_name"] = subColumn.GetSubColumnName();
                    }
                }
                json["data_extractor"] = std::move(jsonDataExtractor);
            }

            if (bloomNGrammFilter.HasBitsStorage()) {
                const auto& bitsStorage = bloomNGrammFilter.GetBitsStorage();
                if (bitsStorage.HasClassName() && !bitsStorage.GetClassName().empty()) {
                    json["bits_storage_type"] = bitsStorage.GetClassName();
                }
            }
            if (bloomNGrammFilter.HasRecordsCount()) {
                json["records_count"] = bloomNGrammFilter.GetRecordsCount();
            }
            if (bloomNGrammFilter.HasNGrammSize()) {
                json["ngramm_size"] = bloomNGrammFilter.GetNGrammSize();
            }
            if (bloomNGrammFilter.HasFilterSizeBytes()) {
                json["filter_size_bytes"] = bloomNGrammFilter.GetFilterSizeBytes();
            }
            if (bloomNGrammFilter.HasHashesCount()) {
                json["hashes_count"] = bloomNGrammFilter.GetHashesCount();
            }
            if (bloomNGrammFilter.HasCaseSensitive()) {
                json["case_sensitive"] = bloomNGrammFilter.GetCaseSensitive();
            }

            EscapeValue(NJson::WriteJson(json, /*formatOutput*/ false), Stream);
            break;
        }
        default: break;
    }
    Stream << ");";
}

void TCreateTableFormatter::FormatUpsertOptions(const TString& fullPath, const NKikimrSchemeOp::TColumnTableSchemeOptions& options) {
    TStringStream paramsStr;
    TString del = "";

    if (options.GetSchemeNeedActualization()) {
        paramsStr << del;
        EscapeName("SCHEME_NEED_ACTUALIZATION", paramsStr);
        paramsStr << "=";
        EscapeValue(options.GetSchemeNeedActualization(), paramsStr);
        del = ", ";
    }
    if (options.HasScanReaderPolicyName() && !options.GetScanReaderPolicyName().empty()) {
        paramsStr << del;
        EscapeName("SCAN_READER_POLICY_NAME", paramsStr);
        paramsStr << "=";
        EscapeString(options.GetScanReaderPolicyName(), paramsStr);
        del = ", ";
    }
    if (options.HasCompactionPlannerConstructor()) {
        const auto& compactionPlannerConstructor = options.GetCompactionPlannerConstructor();
        if (compactionPlannerConstructor.HasClassName() && !compactionPlannerConstructor.GetClassName().empty()) {
            paramsStr << del;
            EscapeName("COMPACTION_PLANNER.CLASS_NAME", paramsStr);
            paramsStr << "=";
            EscapeString(compactionPlannerConstructor.GetClassName(), paramsStr);
            del = ", ";
            switch (compactionPlannerConstructor.GetImplementationCase()) {
                case NKikimrSchemeOp::TCompactionPlannerConstructorContainer::kLBuckets: {
                    break;
                }
                case NKikimrSchemeOp::TCompactionPlannerConstructorContainer::kSBuckets: {
                    ythrow TFormatFail(Ydb::StatusIds::UNSUPPORTED, "Unsupported setting for s-buckets: COMPACTION_PLANNER.FEATURES");
                }
                case NKikimrSchemeOp::TCompactionPlannerConstructorContainer::kLCBuckets: {
                    const auto& lcBuckets = compactionPlannerConstructor.GetLCBuckets();
                    if (lcBuckets.GetLevels().empty()) {
                        break;
                    }
                    paramsStr << del;
                    EscapeName("COMPACTION_PLANNER.FEATURES", paramsStr);
                    paramsStr << "=";
                    NJson::TJsonValue json;
                    json["levels"] = NJson::TJsonArray();
                    auto& levels = json["levels"];
                    for (const auto& level: lcBuckets.GetLevels()) {
                        NJson::TJsonValue jsonLevel;
                        jsonLevel["class_name"] = level.GetClassName();
                        switch (level.GetImplementationCase()) {
                            case NKikimrSchemeOp::TCompactionLevelConstructorContainer::kZeroLevel:{
                                const auto& zeroLevel = level.GetZeroLevel();
                                if (zeroLevel.HasPortionsCountAvailable()) {
                                    jsonLevel["portions_count_available"] = zeroLevel.GetPortionsCountAvailable();
                                }
                                if (zeroLevel.HasPortionsLiveDurationSeconds()) {
                                    jsonLevel["portions_live_duration"] = TDuration::Seconds(zeroLevel.GetPortionsLiveDurationSeconds()).ToString();
                                }
                                if (zeroLevel.HasExpectedBlobsSize()) {
                                    jsonLevel["expected_blobs_size"] = zeroLevel.GetExpectedBlobsSize();
                                }
                                if (zeroLevel.HasPortionsCountLimit()) {
                                    jsonLevel["portions_count_limit"] = zeroLevel.GetPortionsCountLimit();
                                }
                                break;
                            }
                            case NKikimrSchemeOp::TCompactionLevelConstructorContainer::kOneLayer: {
                                const auto& oneLayer = level.GetOneLayer();
                                if (oneLayer.HasBytesLimitFraction()) {
                                    jsonLevel["bytes_limit_fraction"] = oneLayer.GetBytesLimitFraction();
                                }
                                if (oneLayer.HasExpectedPortionSize()) {
                                    jsonLevel["expected_portion_size"] = oneLayer.GetExpectedPortionSize();
                                }
                                break;
                            }
                            default:break;
                        }
                        levels.AppendValue(std::move(jsonLevel));
                    }
                    EscapeValue(NJson::WriteJson(json, /*formatOutput*/ false), paramsStr);
                    del = ", ";
                    break;
                }
                default: break;
            }
        }
    }
    if (options.HasMetadataManagerConstructor()) {
        const auto& metadataManagerConstructor = options.GetMetadataManagerConstructor();
        if (metadataManagerConstructor.HasClassName() && !metadataManagerConstructor.GetClassName().empty()) {
            paramsStr << del;
            EscapeName("METADATA_MEMORY_MANAGER.CLASS_NAME", paramsStr);
            paramsStr << "=";
            EscapeString(metadataManagerConstructor.GetClassName(), paramsStr);
            del = ", ";
        }
        if (metadataManagerConstructor.HasLocalDB()) {
            const auto& localDB = metadataManagerConstructor.GetLocalDB();
            NJson::TJsonValue jsonLocalDB;
            if (localDB.HasMemoryCacheSize()) {
                jsonLocalDB["memory_cache_size"] = localDB.GetMemoryCacheSize();
            }
            if (localDB.HasFetchOnStart()) {
                jsonLocalDB["fetch_on_start"] = localDB.GetFetchOnStart();
            }
            paramsStr << del;
            EscapeName("METADATA_MEMORY_MANAGER.FEATURES", paramsStr);
            paramsStr << "=";
            EscapeValue(NJson::WriteJson(jsonLocalDB, /*formatOutput*/ false), paramsStr);
            del = ", ";
        }
    }

    TString params = paramsStr.Str();
    if (params.empty()) {
        return;
    }

    Stream << "ALTER OBJECT ";
    EscapeName(fullPath, Stream);
    Stream << " (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, ";
    Stream << params;
    Stream << ");";
}

} // NSysView
} // NKikimr
