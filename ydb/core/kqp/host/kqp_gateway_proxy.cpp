#include "kqp_host_impl.h"

#include <ydb/core/grpc_services/table_settings.h>
#include <ydb/core/kqp/gateway/utils/scheme_helpers.h>
#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/core/ydb_convert/column_families.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/public/sdk/cpp/src/client/impl/internal/common/parser.h>
#include <ydb/services/metadata/abstract/kqp_common.h>
#include <ydb/services/lib/actors/pq_schema_actor.h>

#include <util/generic/overloaded.h>

namespace NKikimr::NKqp {

using namespace NThreading;
using namespace NYql;
using namespace NYql::NCommon;

namespace {

bool ConvertDataSlotToYdbTypedValue(NYql::EDataSlot fromType, const TString& fromValue, Ydb::Type* toType,
    Ydb::Value* toValue)
{
    switch (fromType) {
    case NYql::EDataSlot::Bool:
        toType->set_type_id(Ydb::Type::BOOL);
        toValue->set_bool_value(FromString<bool>(fromValue));
        break;
    case NYql::EDataSlot::Int8:
        toType->set_type_id(Ydb::Type::INT8);
        toValue->set_int32_value(FromString<i32>(fromValue));
        break;
    case NYql::EDataSlot::Uint8:
        toType->set_type_id(Ydb::Type::UINT8);
        toValue->set_uint32_value(FromString<ui32>(fromValue));
        break;
    case NYql::EDataSlot::Int16:
        toType->set_type_id(Ydb::Type::INT16);
        toValue->set_int32_value(FromString<i32>(fromValue));
        break;
    case NYql::EDataSlot::Uint16:
        toType->set_type_id(Ydb::Type::UINT16);
        toValue->set_uint32_value(FromString<ui32>(fromValue));
        break;
    case NYql::EDataSlot::Int32:
        toType->set_type_id(Ydb::Type::INT32);
        toValue->set_int32_value(FromString<i32>(fromValue));
        break;
    case NYql::EDataSlot::Uint32:
        toType->set_type_id(Ydb::Type::UINT32);
        toValue->set_uint32_value(FromString<ui32>(fromValue));
        break;
    case NYql::EDataSlot::Int64:
        toType->set_type_id(Ydb::Type::INT64);
        toValue->set_int64_value(FromString<i64>(fromValue));
        break;
    case NYql::EDataSlot::Uint64:
        toType->set_type_id(Ydb::Type::UINT64);
        toValue->set_uint64_value(FromString<ui64>(fromValue));
        break;
    case NYql::EDataSlot::Float:
        toType->set_type_id(Ydb::Type::FLOAT);
        toValue->set_float_value(FromString<float>(fromValue));
        break;
    case NYql::EDataSlot::Double:
        toType->set_type_id(Ydb::Type::DOUBLE);
        toValue->set_double_value(FromString<double>(fromValue));
        break;
    case NYql::EDataSlot::Json:
        toType->set_type_id(Ydb::Type::JSON);
        toValue->set_text_value(fromValue);
        break;
    case NYql::EDataSlot::String:
        toType->set_type_id(Ydb::Type::STRING);
        toValue->set_bytes_value(fromValue);
        break;
    case NYql::EDataSlot::Utf8:
        toType->set_type_id(Ydb::Type::UTF8);
        toValue->set_text_value(fromValue);
        break;
    case NYql::EDataSlot::Date:
        toType->set_type_id(Ydb::Type::DATE);
        toValue->set_uint32_value(FromString<ui32>(fromValue));
        break;
    case NYql::EDataSlot::Datetime:
        toType->set_type_id(Ydb::Type::DATETIME);
        toValue->set_uint32_value(FromString<ui32>(fromValue));
        break;
    case NYql::EDataSlot::Timestamp:
        toType->set_type_id(Ydb::Type::TIMESTAMP);
        toValue->set_uint64_value(FromString<ui64>(fromValue));
        break;
    case NYql::EDataSlot::Interval:
        toType->set_type_id(Ydb::Type::INTERVAL);
        toValue->set_int64_value(FromString<i64>(fromValue));
        break;
    case NYql::EDataSlot::Date32:
        toType->set_type_id(Ydb::Type::DATE32);
        toValue->set_int32_value(FromString<i32>(fromValue));
        break;
    case NYql::EDataSlot::Datetime64:
        toType->set_type_id(Ydb::Type::DATETIME64);
        toValue->set_int64_value(FromString<i64>(fromValue));
        break;
    case NYql::EDataSlot::Timestamp64:
        toType->set_type_id(Ydb::Type::TIMESTAMP64);
        toValue->set_int64_value(FromString<i64>(fromValue));
        break;
    case NYql::EDataSlot::Interval64:
        toType->set_type_id(Ydb::Type::INTERVAL64);
        toValue->set_int64_value(FromString<i64>(fromValue));
        break;
    default:
        return false;
    }
    return true;
}

bool ConvertCreateTableSettingsToProto(NYql::TKikimrTableMetadataPtr metadata, Ydb::Table::CreateTableRequest& proto,
    Ydb::StatusIds::StatusCode& code, TString& error)
{
    for (const auto& family : metadata->ColumnFamilies) {
        auto* familyProto = proto.add_column_families();
        familyProto->set_name(family.Name);
        if (family.Data) {
            familyProto->mutable_data()->set_media(family.Data.GetRef());
        }
        if (family.Compression) {
            if (to_lower(family.Compression.GetRef()) == "off") {
                familyProto->set_compression(Ydb::Table::ColumnFamily::COMPRESSION_NONE);
            } else if (to_lower(family.Compression.GetRef()) == "lz4") {
                familyProto->set_compression(Ydb::Table::ColumnFamily::COMPRESSION_LZ4);
            } else if (to_lower(family.Compression.GetRef()) == "zstd") {
                familyProto->set_compression(Ydb::Table::ColumnFamily::COMPRESSION_ZSTD);
            } else {
                code = Ydb::StatusIds::BAD_REQUEST;
                error = TStringBuilder() << "Unknown compression '" << family.Compression.GetRef() << "' for a column family";
                return false;
            }
        }
        if (family.CompressionLevel) {
            familyProto->set_compression_level(family.CompressionLevel.GetRef());
        }
        if (family.CacheMode) {
            if (to_lower(family.CacheMode.GetRef()) == "regular") {
                familyProto->set_cache_mode(Ydb::Table::ColumnFamily::CACHE_MODE_REGULAR);
            } else if (to_lower(family.CacheMode.GetRef()) == "in_memory") {
                familyProto->set_cache_mode(Ydb::Table::ColumnFamily::CACHE_MODE_IN_MEMORY);
            } else {
                code = Ydb::StatusIds::BAD_REQUEST;
                error = TStringBuilder() << "Unknown cache mode '" << family.CacheMode.GetRef() << "' for a column family";
                return false;
            }
        }
    }

    if (metadata->TableSettings.CompactionPolicy) {
        proto.set_compaction_policy(metadata->TableSettings.CompactionPolicy.GetRef());
    }

    if (metadata->TableSettings.PartitionBy) {
        if (metadata->TableSettings.PartitionBy.size() > metadata->KeyColumnNames.size()) {
            code = Ydb::StatusIds::BAD_REQUEST;
            error = "\"Partition by\" contains more columns than primary key does";
            return false;
        } else if (metadata->TableSettings.PartitionBy.size() == metadata->KeyColumnNames.size()) {
            for (size_t i = 0; i < metadata->TableSettings.PartitionBy.size(); ++i) {
                if (metadata->TableSettings.PartitionBy[i] != metadata->KeyColumnNames[i]) {
                    code = Ydb::StatusIds::BAD_REQUEST;
                    error = "\"Partition by\" doesn't match primary key";
                    return false;
                }
            }
        } else {
            code = Ydb::StatusIds::UNSUPPORTED;
            error = "\"Partition by\" is not supported yet";
            return false;
        }
    }

    if (metadata->TableSettings.AutoPartitioningBySize) {
        auto& partitioningSettings = *proto.mutable_partitioning_settings();
        TString value = to_lower(metadata->TableSettings.AutoPartitioningBySize.GetRef());
        if (value == "enabled") {
            partitioningSettings.set_partitioning_by_size(Ydb::FeatureFlag::ENABLED);
        } else if (value == "disabled") {
            partitioningSettings.set_partitioning_by_size(Ydb::FeatureFlag::DISABLED);
        } else {
            code = Ydb::StatusIds::BAD_REQUEST;
            error = TStringBuilder() << "Unknown feature flag '"
                << metadata->TableSettings.AutoPartitioningBySize.GetRef()
                << "' for auto partitioning by size";
            return false;
        }
    }

    if (metadata->TableSettings.PartitionSizeMb) {
        auto& partitioningSettings = *proto.mutable_partitioning_settings();
        partitioningSettings.set_partition_size_mb(metadata->TableSettings.PartitionSizeMb.GetRef());
    }

    if (metadata->TableSettings.AutoPartitioningByLoad) {
        auto& partitioningSettings = *proto.mutable_partitioning_settings();
        TString value = to_lower(metadata->TableSettings.AutoPartitioningByLoad.GetRef());
        if (value == "enabled") {
            partitioningSettings.set_partitioning_by_load(Ydb::FeatureFlag::ENABLED);
        } else if (value == "disabled") {
            partitioningSettings.set_partitioning_by_load(Ydb::FeatureFlag::DISABLED);
        } else {
            code = Ydb::StatusIds::BAD_REQUEST;
            error = TStringBuilder() << "Unknown feature flag '"
                << metadata->TableSettings.AutoPartitioningByLoad.GetRef()
                << "' for auto partitioning by load";
            return false;
        }
    }

    if (metadata->TableSettings.MinPartitions) {
        auto& partitioningSettings = *proto.mutable_partitioning_settings();
        partitioningSettings.set_min_partitions_count(metadata->TableSettings.MinPartitions.GetRef());
    }

    if (metadata->TableSettings.MaxPartitions) {
        auto& partitioningSettings = *proto.mutable_partitioning_settings();
        partitioningSettings.set_max_partitions_count(metadata->TableSettings.MaxPartitions.GetRef());
    }

    if (metadata->TableSettings.UniformPartitions) {
        if (metadata->TableSettings.PartitionAtKeys) {
            code = Ydb::StatusIds::BAD_REQUEST;
            error = TStringBuilder() << "Uniform partitions and partitions at keys settings are mutually exclusive."
                << " Use either one of them.";
            return false;
        }
        proto.set_uniform_partitions(metadata->TableSettings.UniformPartitions.GetRef());
    }

    if (metadata->TableSettings.PartitionAtKeys) {
        auto* borders = proto.mutable_partition_at_keys();
        for (const auto& splitPoint : metadata->TableSettings.PartitionAtKeys) {
            auto* border = borders->Addsplit_points();
            auto &keyType = *border->mutable_type()->mutable_tuple_type();
            for (const auto& key : splitPoint) {
                auto* type = keyType.add_elements()->mutable_optional_type()->mutable_item();
                auto* value = border->mutable_value()->add_items();
                if (!ConvertDataSlotToYdbTypedValue(key.first, key.second, type, value)) {
                    code = Ydb::StatusIds::BAD_REQUEST;
                    error = TStringBuilder() << "Unsupported type for PartitionAtKeys: '"
                        << key.first << "'";
                    return false;
                }
            }
        }
    }

    if (metadata->TableSettings.KeyBloomFilter) {
        TString value = to_lower(metadata->TableSettings.KeyBloomFilter.GetRef());
        if (value == "enabled") {
            proto.set_key_bloom_filter(Ydb::FeatureFlag::ENABLED);
        } else if (value == "disabled") {
            proto.set_key_bloom_filter(Ydb::FeatureFlag::DISABLED);
        } else {
            code = Ydb::StatusIds::BAD_REQUEST;
            error = TStringBuilder() << "Unknown feature flag '"
                << metadata->TableSettings.KeyBloomFilter.GetRef()
                << "' for key bloom filter";
            return false;
        }
    }

    if (metadata->TableSettings.ReadReplicasSettings) {
        if (!NYql::ConvertReadReplicasSettingsToProto(metadata->TableSettings.ReadReplicasSettings.GetRef(),
                *proto.mutable_read_replicas_settings(), code, error)) {
            return false;
        }
    }

    if (const auto& ttl = metadata->TableSettings.TtlSettings) {
        if (ttl.IsSet()) {
            ConvertTtlSettingsToProto(ttl.GetValueSet(), *proto.mutable_ttl_settings());
        } else {
            code = Ydb::StatusIds::BAD_REQUEST;
            error = "Can't reset TTL settings";
            return false;
        }
    }

    if (metadata->TableSettings.StoreExternalBlobs) {
        auto& storageSettings = *proto.mutable_storage_settings();
        TString value = to_lower(metadata->TableSettings.StoreExternalBlobs.GetRef());
        if (value == "enabled") {
            storageSettings.set_store_external_blobs(Ydb::FeatureFlag::ENABLED);
        } else if (value == "disabled") {
            storageSettings.set_store_external_blobs(Ydb::FeatureFlag::DISABLED);
        } else {
            code = Ydb::StatusIds::BAD_REQUEST;
            error = TStringBuilder() << "Unknown feature flag '"
                << metadata->TableSettings.StoreExternalBlobs.GetRef()
                << "' for store external blobs";
            return false;
        }
    }

    if (const auto count = metadata->TableSettings.ExternalDataChannelsCount) {
        proto.mutable_storage_settings()->set_external_data_channels_count(*count);
    }

    proto.set_temporary(metadata->Temporary);

    return true;
}

THashMap<TString, TString> GetDefaultFromSequences(NYql::TKikimrTableMetadataPtr metadata) {
    THashMap<TString, TString> sequences;
    for(const auto& [name, column]: metadata->Columns) {
        const auto& seq = column.DefaultFromSequence;
        if (!seq.empty()) {
            sequences.emplace(seq, column.Type);
        }
    }
    return sequences;
}

void FillCreateTableColumnDesc(NKikimrSchemeOp::TTableDescription& tableDesc, const TString& name,
    NYql::TKikimrTableMetadataPtr metadata)
{
    tableDesc.SetName(name);

    Y_ENSURE(metadata->ColumnOrder.size() == metadata->Columns.size());
    for (const auto& name : metadata->ColumnOrder) {
        auto columnIt = metadata->Columns.find(name);
        Y_ENSURE(columnIt != metadata->Columns.end());
        const auto& cMeta = columnIt->second;

        auto& columnDesc = *tableDesc.AddColumns();
        columnDesc.SetName(columnIt->second.Name);
        columnDesc.SetType(columnIt->second.Type);
        columnDesc.SetNotNull(columnIt->second.NotNull);
        if (columnIt->second.Families) {
            columnDesc.SetFamilyName(*columnIt->second.Families.begin());
        }

        if (cMeta.IsDefaultFromSequence()) {
            columnDesc.SetDefaultFromSequence(
                cMeta.DefaultFromSequence);
        }

        if (cMeta.IsDefaultFromLiteral()) {
            columnDesc.MutableDefaultFromLiteral()->CopyFrom(
                cMeta.DefaultFromLiteral);
        }

        if (NScheme::NTypeIds::IsParametrizedType(columnIt->second.TypeInfo.GetTypeId())) {
            ProtoFromTypeInfo(columnIt->second.TypeInfo, columnIt->second.TypeMod, *columnDesc.MutableTypeInfo());
        }
    }

    for (TString& keyColumn : metadata->KeyColumnNames) {
        tableDesc.AddKeyColumnNames(keyColumn);
    }
}

bool FillCreateTableDesc(NYql::TKikimrTableMetadataPtr metadata, NKikimrSchemeOp::TTableDescription& tableDesc,
    const TTableProfiles& profiles, Ydb::StatusIds::StatusCode& code, TString& error, TList<TString>& warnings)
{
    Ydb::Table::CreateTableRequest createTableProto;
    if (!profiles.ApplyTableProfile(*createTableProto.mutable_profile(), tableDesc, code, error)
        || !ConvertCreateTableSettingsToProto(metadata, createTableProto, code, error)) {
        return false;
    }

    TColumnFamilyManager families(tableDesc.MutablePartitionConfig());

    for (const auto& familySettings : createTableProto.column_families()) {
        if (!families.ApplyFamilySettings(familySettings, &code, &error)) {
            return false;
        }
    }

    if (families.Modified && !families.ValidateColumnFamilies(&code, &error)) {
        return false;
    }

    if (!NGRpcService::FillCreateTableSettingsDesc(tableDesc, createTableProto, profiles, code, error, warnings)) {
        return false;
    }
    return true;
}

template <typename T>
bool FillColumnTableSchema(NKikimrSchemeOp::TColumnTableSchema& schema, const T& metadata, Ydb::StatusIds::StatusCode& code, TString& error) {
    Y_ENSURE(metadata.ColumnOrder.size() == metadata.Columns.size());

    THashMap<TString, ui32> columnFamiliesByName;
    ui32 columnFamilyId = 1;
    for (const auto& family : metadata.ColumnFamilies) {
        if (family.Data.Defined()) {
            code = Ydb::StatusIds::BAD_REQUEST;
            error = TStringBuilder() << "Field `DATA` is not supported for OLAP tables in column family '" << family.Name << "'";
            return false;
        }
        if (family.CacheMode.Defined()) {
            code = Ydb::StatusIds::BAD_REQUEST;
            error = TStringBuilder() << "Field `CACHE_MODE` is not supported for OLAP tables in column family '" << family.Name << "'";
            return false;
        }
        auto columnFamilyIt = columnFamiliesByName.find(family.Name);
        if (!columnFamilyIt.IsEnd()) {
            code = Ydb::StatusIds::BAD_REQUEST;
            error = TStringBuilder() << "Duplicate column family `" << family.Name << '`';
            return false;
        }
        auto familyDescription = schema.AddColumnFamilies();
        familyDescription->SetName(family.Name);
        if (familyDescription->GetName() == "default") {
            familyDescription->SetId(0);
        } else {
            familyDescription->SetId(columnFamilyId++);
        }
        Y_ENSURE(columnFamiliesByName.emplace(familyDescription->GetName(), familyDescription->GetId()).second);
        if (family.Compression.Defined()) {
            NKikimrSchemeOp::EColumnCodec codec;
            auto codecName = to_lower(family.Compression.GetRef());
            if (codecName == "off") {
                codec = NKikimrSchemeOp::EColumnCodec::ColumnCodecPlain;
            } else if (codecName == "zstd") {
                codec = NKikimrSchemeOp::EColumnCodec::ColumnCodecZSTD;
            } else if (codecName == "lz4") {
                codec = NKikimrSchemeOp::EColumnCodec::ColumnCodecLZ4;
            } else {
                code = Ydb::StatusIds::BAD_REQUEST;
                error = TStringBuilder() << "Unknown compression '" << family.Compression.GetRef() << "' for a column family";
                return false;
            }
            familyDescription->SetColumnCodec(codec);
        } else {
            if (family.Name != "default") {
                code = Ydb::StatusIds::BAD_REQUEST;
                error = TStringBuilder() << "Compression is not set for non `default` column family '" << family.Name << "'";
                return false;
            }
        }

        if (family.CompressionLevel.Defined()) {
            if (!family.Compression.Defined()) {
                code = Ydb::StatusIds::BAD_REQUEST;
                error = TStringBuilder() << "Compression is not set for column family '" << family.Name << "', but compression level is set";
                return false;
            }
            familyDescription->SetColumnCodecLevel(family.CompressionLevel.GetRef());
        }
    }

    schema.SetNextColumnFamilyId(columnFamilyId);

    for (const auto& name : metadata.ColumnOrder) {
        auto columnIt = metadata.Columns.find(name);
        Y_ENSURE(columnIt != metadata.Columns.end());

        if (columnIt->second.IsDefaultFromLiteral()) {
            code = Ydb::StatusIds::BAD_REQUEST;
            error = TStringBuilder() << "Default values are not supported in column tables";
            return false;
        }

        if (columnIt->second.IsDefaultFromSequence()) {
            code = Ydb::StatusIds::BAD_REQUEST;
            error = TStringBuilder() << "Default sequences are not supported in column tables";
            return false;
        }

        NKikimrSchemeOp::TOlapColumnDescription& columnDesc = *schema.AddColumns();
        columnDesc.SetName(columnIt->second.Name);
        columnDesc.SetType(columnIt->second.Type);
        columnDesc.SetNotNull(columnIt->second.NotNull);

        auto columnType = NScheme::ProtoColumnTypeFromTypeInfoMod(columnIt->second.TypeInfo, columnIt->second.TypeMod);
        if (columnType.TypeInfo) {
            *columnDesc.MutableTypeInfo() = *columnType.TypeInfo;
        }

        if (!columnFamiliesByName.empty()) {
            TString columnFamilyName = "default";
            ui32 columnFamilyId = 0;
            if (columnIt->second.Families.size()) {
                columnFamilyName = *columnIt->second.Families.begin();
                auto columnFamilyIdIt = columnFamiliesByName.find(columnFamilyName);
                if (columnFamilyIdIt.IsEnd()) {
                    code = Ydb::StatusIds::BAD_REQUEST;
                    error = TStringBuilder() << "Unknown column family `" << columnFamilyName << "` for column `" << columnDesc.GetName() << "`";
                    return false;
                }
                columnFamilyId = columnFamilyIdIt->second;
            }
            columnDesc.SetColumnFamilyName(columnFamilyName);
            columnDesc.SetColumnFamilyId(columnFamilyId);
        }
    }

    for (const auto& keyColumn : metadata.KeyColumnNames) {
        schema.AddKeyColumnNames(keyColumn);
    }
    return true;
}

bool FillCreateColumnTableDesc(NYql::TKikimrTableMetadataPtr metadata,
        NKikimrSchemeOp::TColumnTableDescription& tableDesc, Ydb::StatusIds::StatusCode& code, TString& error)
{
    if (metadata->Columns.empty()) {
        tableDesc.SetSchemaPresetName("default");
    }

    auto& hashSharding = *tableDesc.MutableSharding()->MutableHashSharding();

    for (const TString& column : metadata->TableSettings.PartitionBy) {
        if (!metadata->Columns.count(column)) {
            code = Ydb::StatusIds::BAD_REQUEST;
            error = TStringBuilder() << "Unknown column '" << column << "' in partition by key";
            return false;
        }

        hashSharding.AddColumns(column);
    }

    if (metadata->TableSettings.PartitionByHashFunction) {
        if (to_lower(metadata->TableSettings.PartitionByHashFunction.GetRef()) == "cloud_logs") {
            hashSharding.SetFunction(NKikimrSchemeOp::TColumnTableSharding::THashSharding::HASH_FUNCTION_CLOUD_LOGS);
        } else if (to_lower(metadata->TableSettings.PartitionByHashFunction.GetRef()) == "consistency_hash_64") {
            hashSharding.SetFunction(NKikimrSchemeOp::TColumnTableSharding::THashSharding::HASH_FUNCTION_CONSISTENCY_64);
        } else if (to_lower(metadata->TableSettings.PartitionByHashFunction.GetRef()) == "modulo_n") {
            hashSharding.SetFunction(NKikimrSchemeOp::TColumnTableSharding::THashSharding::HASH_FUNCTION_MODULO_N);
        } else {
            code = Ydb::StatusIds::BAD_REQUEST;
            error = TStringBuilder() << "Unknown hash function '"
                << metadata->TableSettings.PartitionByHashFunction.GetRef() << "' to partition by";
            return false;
        }
    } else {
        hashSharding.SetFunction(NKikimrSchemeOp::TColumnTableSharding::THashSharding::HASH_FUNCTION_CONSISTENCY_64);
    }

    if (metadata->TableSettings.MinPartitions) {
        tableDesc.SetColumnShardCount(*metadata->TableSettings.MinPartitions);
    }

    if (metadata->TableSettings.TtlSettings.Defined() && metadata->TableSettings.TtlSettings.IsSet()) {
        const auto& inputSettings = metadata->TableSettings.TtlSettings.GetValueSet();
        auto& resultSettings = *tableDesc.MutableTtlSettings();
        resultSettings.MutableEnabled()->SetColumnName(inputSettings.ColumnName);
        for (const auto& tier : inputSettings.Tiers) {
            auto* tierProto = resultSettings.MutableEnabled()->AddTiers();
            tierProto->SetApplyAfterSeconds(tier.ApplyAfter.Seconds());
            if (tier.StorageName) {
                tierProto->MutableEvictToExternalStorage()->SetStorage(*tier.StorageName);
            } else {
                tierProto->MutableDelete();
            }
        }
        if (inputSettings.ColumnUnit) {
            resultSettings.MutableEnabled()->SetColumnUnit(static_cast<NKikimrSchemeOp::TTTLSettings::EUnit>(*inputSettings.ColumnUnit));
        }
    }

    tableDesc.SetTemporary(metadata->Temporary);

    return true;
}

template <class TResult>
static TFuture<TResult> PrepareUnsupported(const char* name) {
    TResult result;
    result.AddIssue(TIssue({}, TStringBuilder()
        <<"Operation is not supported in current execution mode, check query type. Operation: " << name));
    return MakeFuture(result);
}

template <class TResult>
static TFuture<TResult> PrepareSuccess() {
    TResult result;
    result.SetSuccess();
    return MakeFuture(result);
}

template<typename TResult>
TFuture<TResult> InvalidCluster(const TString& cluster) {
    return MakeFuture(ResultFromError<TResult>("Invalid cluster: " + cluster));
}

bool IsDdlPrepareAllowed(TKikimrSessionContext& sessionCtx) {
    auto queryType = sessionCtx.Query().Type;
    if (queryType != EKikimrQueryType::Query && queryType != EKikimrQueryType::Script) {
        return false;
    }

    return true;
}

#define FORWARD_ENSURE_NO_PREPARE(name, ...) \
    if (IsPrepare()) { \
        return PrepareUnsupported<TGenericResult>(#name); \
    } \
    return Gateway->name(__VA_ARGS__);

#define CHECK_PREPARED_DDL(name) \
    if (SessionCtx && SessionCtx->Query().SuppressDdlChecks) { \
        YQL_ENSURE(SessionCtx->Query().Type == EKikimrQueryType::YqlScript); \
        return PrepareSuccess<TGenericResult>(); \
    } \
    if (IsPrepare() && !IsDdlPrepareAllowed(*SessionCtx)) { \
        return PrepareUnsupported<TGenericResult>(#name); \
    }

class TKqpGatewayProxy : public IKikimrGateway {
public:
    TKqpGatewayProxy(const TIntrusivePtr<IKqpGateway>& gateway,
        const TIntrusivePtr<TKikimrSessionContext>& sessionCtx,
        TActorSystem* actorSystem)
        : Gateway(gateway)
        , SessionCtx(sessionCtx)
        , ActorSystem(actorSystem)
    {
        YQL_ENSURE(Gateway);
    }

public:
    bool HasCluster(const TString& cluster) override {
        return Gateway->HasCluster(cluster);
    }

    TVector<TString> GetClusters() override {
        return Gateway->GetClusters();
    }

    TString GetDefaultCluster() override {
        return Gateway->GetDefaultCluster();
    }

    TMaybe<TString> GetSetting(const TString& cluster, const TString& name) override {
        return Gateway->GetSetting(cluster, name);
    }

    void SetToken(const TString& cluster, const TIntrusiveConstPtr<NACLib::TUserToken>& token) override {
        Gateway->SetToken(cluster, token);
    }

    void SetClientAddress(const TString& clientAddress) override {
        Gateway->SetClientAddress(clientAddress);
    }

    TFuture<TListPathResult> ListPath(const TString& cluster, const TString& path) override {
        return Gateway->ListPath(cluster, path);
    }

    TFuture<TTableMetadataResult> LoadTableMetadata(const TString& cluster, const TString& table,
        TLoadTableMetadataSettings settings) override
    {
        return Gateway->LoadTableMetadata(cluster, table, settings);
    }

    TFuture<TGenericResult> SetConstraint(const TString& tablePath, TVector<TSetColumnConstraintSettings>&& settings) override {
        try {
            auto [dirname, tableName] = NSchemeHelpers::SplitPathByDirAndBaseNames(tablePath);
            if (!dirname.empty() && !IsStartWithSlash(dirname)) {
                dirname = JoinPath({GetDatabase(), dirname});
            }

            NKikimrSchemeOp::TSetColumnConstraintsInitiate setColumnConstraintsInitiate;
            for (auto& setting : settings) {
                auto* add = setColumnConstraintsInitiate.AddConstraintSettings();
                add->Swap(&setting);
            }

            setColumnConstraintsInitiate.SetTableName(tableName);

            NKikimrSchemeOp::TModifyScheme modifyScheme;
            *modifyScheme.MutableSetColumnConstraintsInitiate() = std::move(setColumnConstraintsInitiate);
            modifyScheme.SetWorkingDir(std::move(dirname));
            modifyScheme.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreateSetConstraintInitiate);

            return Gateway->ModifyScheme(std::move(modifyScheme));
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TGenericResult PrepareAlterDatabase(const TAlterDatabaseSettings& settings, NKikimrSchemeOp::TModifyScheme& modifyScheme) {
        if (TIssue error; !NSchemeHelpers::Validate(settings, error)) {
            TGenericResult result;
            result.SetStatus(YqlStatusFromYdbStatus(error.GetCode()));
            result.AddIssue(error);
            return result;
        }

        const auto [dirname, basename] = NSchemeHelpers::SplitPathByDirAndBaseNames(settings.DatabasePath);
        modifyScheme.SetWorkingDir(dirname);

        if (settings.Owner) {
            NSchemeHelpers::FillAlterDatabaseOwner(modifyScheme, basename, *settings.Owner);

            TGenericResult result;
            result.SetSuccess();
            return result;
        }

        if (settings.SchemeLimits) {
            NSchemeHelpers::FillAlterDatabaseSchemeLimits(modifyScheme, basename, *settings.SchemeLimits);

            TGenericResult result;
            result.SetSuccess();
            return result;
        }

        TGenericResult result;
        result.SetStatus(TIssuesIds_EIssueCode_KIKIMR_BAD_REQUEST);
        result.AddIssue(TIssue("Cannot execute ALTER DATABASE with these settings.").SetCode(result.Status(), TSeverityIds_ESeverityId_S_ERROR));
        return result;
    }

    TFuture<TGenericResult> AlterDatabase(const TString& cluster, const TAlterDatabaseSettings& settings) override {
        CHECK_PREPARED_DDL(AlterDatabase);
        try {
            if (cluster != SessionCtx->GetCluster()) {
                return InvalidCluster<TGenericResult>(cluster);
            }

            NKikimrSchemeOp::TModifyScheme modifyScheme;
            auto preparation = PrepareAlterDatabase(settings, modifyScheme);
            if (!preparation.Success()) {
                return MakeFuture<TGenericResult>(preparation);
            }
            if (IsPrepare()) {
                auto& phyTx = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery()->AddTransactions();
                phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);
                auto& schemeOp = *phyTx.MutableSchemeOperation();

                if (settings.Owner) {
                    *schemeOp.MutableModifyPermissions() = modifyScheme;
                }
                if (settings.SchemeLimits) {
                    *schemeOp.MutableAlterDatabase() = modifyScheme;
                }

                return MakeFuture<TGenericResult>(preparation);
            }

            return Gateway->ModifyScheme(std::move(modifyScheme));
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> CreateTable(TKikimrTableMetadataPtr metadata, bool createDir, bool existingOk, bool replaceIfExists) override {
        Y_UNUSED(replaceIfExists);
        CHECK_PREPARED_DDL(CreateTable);

        std::pair<TString, TString> pathPair;
        TString error;
        if (!NSchemeHelpers::SplitTablePath(metadata->Name, GetDatabase(), pathPair, error, createDir)) {
            return MakeFuture(ResultFromError<TGenericResult>(error));
        }

        bool isPrepare = IsPrepare();
        auto gateway = Gateway;
        auto sessionCtx = SessionCtx;
        auto profilesFuture = Gateway->GetTableProfiles();
        auto tablePromise = NewPromise<TGenericResult>();
        auto temporary = metadata->Temporary;
        profilesFuture.Subscribe([gateway, sessionCtx, metadata, tablePromise, pathPair, isPrepare, temporary, existingOk]
            (const TFuture<IKqpGateway::TKqpTableProfilesResult>& future) mutable {
                auto profilesResult = future.GetValue();
                if (!profilesResult.Success()) {
                    tablePromise.SetValue(ResultFromIssues<TGenericResult>(profilesResult.Status(),
                        profilesResult.Issues()));
                    return;
                }

                NKikimrSchemeOp::TModifyScheme schemeTx;
                schemeTx.SetWorkingDir(pathPair.first);
                const auto sequences = GetDefaultFromSequences(metadata);

                NKikimrSchemeOp::TTableDescription* tableDesc = nullptr;
                if (!metadata->Indexes.empty() || !sequences.empty()) {
                    schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateIndexedTable);
                    tableDesc = schemeTx.MutableCreateIndexedTable()->MutableTableDescription();
                    for (const auto& index : metadata->Indexes) {
                        auto indexDesc = schemeTx.MutableCreateIndexedTable()->AddIndexDescription();
                        indexDesc->SetName(index.Name);
                        indexDesc->SetType(TIndexDescription::ConvertIndexType(index.Type));
                        indexDesc->SetState(static_cast<::NKikimrSchemeOp::EIndexState>(index.State));
                        for (const auto& col : index.KeyColumns) {
                            indexDesc->AddKeyColumnNames(col);
                        }
                        for (const auto& col : index.DataColumns) {
                            indexDesc->AddDataColumnNames(col);
                        }

                        if (index.Type == TIndexDescription::EType::GlobalSyncVectorKMeansTree) {
                            *indexDesc->MutableVectorIndexKmeansTreeDescription()->MutableSettings() = std::get<NKikimrKqp::TVectorIndexKmeansTreeDescription>(index.SpecializedIndexDescription).GetSettings();
                        }
                    }
                    FillCreateTableColumnDesc(*tableDesc, pathPair.second, metadata);
                    for(const auto& [seq, seqType]: sequences) {
                        auto seqDesc = schemeTx.MutableCreateIndexedTable()->MutableSequenceDescription()->Add();
                        seqDesc->SetName(seq);
                        const auto type = to_lower(seqType);
                        seqDesc->SetMinValue(1);
                        if (type == "int64") {
                            seqDesc->SetMaxValue(9223372036854775807);
                        } else if (type == "int32") {
                            seqDesc->SetMaxValue(2147483647);
                        } else if (type == "int16") {
                            seqDesc->SetMaxValue(32767);
                        }
                        seqDesc->SetCycle(false);
                    }

                } else {
                    schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateTable);
                    tableDesc = schemeTx.MutableCreateTable();
                    FillCreateTableColumnDesc(*tableDesc, pathPair.second, metadata);
                }

                Ydb::StatusIds::StatusCode code;
                TList<TString> warnings;
                TString error;
                if (!FillCreateTableDesc(metadata, *tableDesc, profilesResult.Profiles, code, error, warnings)) {
                    IKqpGateway::TGenericResult errResult;
                    errResult.AddIssue(NYql::TIssue(error));
                    errResult.SetStatus(NYql::YqlStatusFromYdbStatus(code));
                    tablePromise.SetValue(errResult);
                    return;
                }

                if (isPrepare) {
                    auto& phyQuery = *sessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
                    auto& phyTx = *phyQuery.AddTransactions();
                    phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);
                    schemeTx.SetFailedOnAlreadyExists(!existingOk);
                    phyTx.MutableSchemeOperation()->MutableCreateTable()->Swap(&schemeTx);

                    TGenericResult result;
                    result.SetSuccess();
                    tablePromise.SetValue(result);
                } else {
                    if (temporary) {
                        auto code = Ydb::StatusIds::BAD_REQUEST;
                        auto error = TStringBuilder() << "Not allowed to create temp table";
                        IKqpGateway::TGenericResult errResult;
                        errResult.AddIssue(NYql::TIssue(error));
                        errResult.SetStatus(NYql::YqlStatusFromYdbStatus(code));
                        tablePromise.SetValue(errResult);
                    }
                    gateway->ModifyScheme(std::move(schemeTx)).Subscribe([tablePromise, warnings]
                        (const TFuture<TGenericResult>& future) mutable {
                            auto result = future.GetValue();
                            for (const auto& warning : warnings) {
                                result.AddIssue(
                                    NYql::TIssue(warning).SetCode(NKikimrIssues::TIssuesIds::WARNING,
                                        NYql::TSeverityIds::S_WARNING)
                                );
                            }

                            tablePromise.SetValue(result);
                        });
                }
            });

        return tablePromise.GetFuture();
    }

    TFuture<TGenericResult> PrepareAlterTable(const TString&, Ydb::Table::AlterTableRequest&& req,
        const TMaybe<TString>&, ui64 flags, NKikimrIndexBuilder::TIndexBuildSettings&& buildSettings)
    {
        YQL_ENSURE(SessionCtx->Query().PreparingQuery);
        auto promise = NewPromise<TGenericResult>();
        const auto ops = GetAlterOperationKinds(&req);
        if (ops.size() != 1) {
            auto code = Ydb::StatusIds::BAD_REQUEST;
            auto error = TStringBuilder() << "Unqualified alter table request.";
            IKqpGateway::TGenericResult errResult;
            errResult.AddIssue(NYql::TIssue(error));
            errResult.SetStatus(NYql::YqlStatusFromYdbStatus(code));
            promise.SetValue(errResult);
            return promise.GetFuture();
        }

        const auto opType = *ops.begin();
        auto tablePromise = NewPromise<TGenericResult>();
        if (opType == EAlterOperationKind::AddIndex) {
            auto &phyQuery =
                *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
            auto &phyTx = *phyQuery.AddTransactions();
            phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);
            auto buildOp = phyTx.MutableSchemeOperation()->MutableBuildOperation();
            Ydb::StatusIds::StatusCode code;
            TString error;
            if (!BuildAlterTableAddIndexRequest(&req, buildOp, flags, code, error)) {
                IKqpGateway::TGenericResult errResult;
                errResult.AddIssue(NYql::TIssue(error));
                errResult.SetStatus(NYql::YqlStatusFromYdbStatus(code));
                tablePromise.SetValue(errResult);
                return tablePromise.GetFuture();
            }
            TGenericResult result;
            result.SetSuccess();
            tablePromise.SetValue(result);
            return tablePromise.GetFuture();
        }

        auto profilesFuture = Gateway->GetTableProfiles();
        auto sessionCtx = SessionCtx;
        profilesFuture.Subscribe(
            [tablePromise, sessionCtx, alterReq = std::move(req), buildSettings = std::move(buildSettings)](
                const TFuture<IKqpGateway::TKqpTableProfilesResult> &future) mutable {
                auto profilesResult = future.GetValue();
                if (!profilesResult.Success()) {
                    tablePromise.SetValue(ResultFromIssues<TGenericResult>(
                        profilesResult.Status(), profilesResult.Issues()));
                    return;
                }

                auto &phyQuery =
                    *sessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
                auto &phyTx = *phyQuery.AddTransactions();
                phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);

                NKikimrSchemeOp::TModifyScheme modifyScheme;


                const TPathId invalidPathId;
                Ydb::StatusIds::StatusCode code;
                TString error;
                if (!BuildAlterTableModifyScheme(&alterReq, &modifyScheme, profilesResult.Profiles, invalidPathId, code, error)) {
                    IKqpGateway::TGenericResult errResult;
                    errResult.AddIssue(NYql::TIssue(error));
                    errResult.SetStatus(NYql::YqlStatusFromYdbStatus(code));
                    tablePromise.SetValue(errResult);
                    return;
                }

                if (buildSettings.has_column_build_operation()) {
                    buildSettings.MutableAlterMainTablePayload()->PackFrom(modifyScheme);
                    phyTx.MutableSchemeOperation()->MutableBuildOperation()->CopyFrom(buildSettings);
                } else {
                    phyTx.MutableSchemeOperation()->MutableAlterTable()->CopyFrom(modifyScheme);
                }

                TGenericResult result;
                result.SetSuccess();
                tablePromise.SetValue(result);
            });

        return tablePromise.GetFuture();
    }

    TFuture<TGenericResult> SendSchemeExecuterRequest(const TString& cluster, const TMaybe<TString>& requestType,
        const std::shared_ptr<const NKikimr::NKqp::TKqpPhyTxHolder> &phyTx) override
    {
        return Gateway->SendSchemeExecuterRequest(cluster, requestType, phyTx);
    }

    TFuture<TGenericResult> AlterTable(const TString& cluster, Ydb::Table::AlterTableRequest&& req,
        const TMaybe<TString>& requestType, ui64 flags, NKikimrIndexBuilder::TIndexBuildSettings&& buildSettings) override
    {
        CHECK_PREPARED_DDL(AlterTable);

        auto tablePromise = NewPromise<TGenericResult>();

        if (!IsPrepare()) {
            SessionCtx->Query().PrepareOnly = false;
            if (!SessionCtx->Query().PreparingQuery) {
                SessionCtx->Query().PreparingQuery = std::make_unique<NKikimrKqp::TPreparedQuery>();
            }

            if (SessionCtx->Query().PreparingQuery->MutablePhysicalQuery()->GetTransactions().size() > 0) {
                auto code = Ydb::StatusIds::BAD_REQUEST;
                auto error = TStringBuilder() << "multiple transactions are not supported for alter table operation.";
                IKqpGateway::TGenericResult errResult;
                errResult.AddIssue(NYql::TIssue(error));
                errResult.SetStatus(NYql::YqlStatusFromYdbStatus(code));
                tablePromise.SetValue(errResult);
                return tablePromise.GetFuture();
            }
        }

        auto prepareFuture = PrepareAlterTable(cluster, std::move(req), requestType, flags, std::move(buildSettings));
        if (IsPrepare())
            return prepareFuture;

        auto sessionCtx = SessionCtx;
        auto gateway = Gateway;
        prepareFuture.Subscribe([cluster, requestType, tablePromise, sessionCtx, gateway](const TFuture<IKqpGateway::TGenericResult> &future) mutable {
            auto result = future.GetValue();
            TPreparedQueryHolder::TConstPtr preparedQuery = std::make_shared<TPreparedQueryHolder>(sessionCtx->Query().PreparingQuery.release(), nullptr);
            if (result.Success()) {
                auto executeFuture = gateway->SendSchemeExecuterRequest(cluster, requestType, preparedQuery->GetPhyTx(0));
                executeFuture.Subscribe([tablePromise](const TFuture<IKqpGateway::TGenericResult> &future) mutable {
                    auto fresult = future.GetValue();
                    if (fresult.Success()) {
                        TGenericResult result;
                        result.SetSuccess();
                        tablePromise.SetValue(result);
                    } else {
                        tablePromise.SetValue(
                            ResultFromIssues<TGenericResult>(fresult.Status(), fresult.Issues())
                        );
                    }
                });
                return;
            } else {
                tablePromise.SetValue(ResultFromIssues<TGenericResult>(
                    result.Status(), result.Issues()));

                return;
            }
        });

        return tablePromise.GetFuture();
    }

    TFuture<TGenericResult> RenameTable(const TString& src, const TString& dst, const TString& cluster) override {
        CHECK_PREPARED_DDL(RenameTable);

        auto metadata = SessionCtx->Tables().GetTable(cluster, src).Metadata;

        std::pair<TString, TString> pathPair;
        TString error;
        if (!NSchemeHelpers::SplitTablePath(metadata->Name, GetDatabase(), pathPair, error, false)) {
            return MakeFuture(ResultFromError<TGenericResult>(error));
        }

        auto temporary = metadata->Temporary;
        auto renameTablePromise = NewPromise<TGenericResult>();

        NKikimrSchemeOp::TModifyScheme schemeTx;
        schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpMoveTable);
        schemeTx.SetWorkingDir(pathPair.first);

        auto* renameTable = schemeTx.MutableMoveTable();
        renameTable->SetSrcPath(src);
        renameTable->SetDstPath(dst);

        if (IsPrepare()) {
            auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
            auto& phyTx = *phyQuery.AddTransactions();
            phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);


            phyTx.MutableSchemeOperation()->MutableAlterTable()->Swap(&schemeTx);
            TGenericResult result;
            result.SetSuccess();
            renameTablePromise.SetValue(result);
        } else {
            if (temporary) {
                auto code = Ydb::StatusIds::BAD_REQUEST;
                auto error = TStringBuilder() << "Not allowed to rename temp table";
                IKqpGateway::TGenericResult errResult;
                errResult.AddIssue(NYql::TIssue(error));
                errResult.SetStatus(NYql::YqlStatusFromYdbStatus(code));
                renameTablePromise.SetValue(errResult);
            }
            return Gateway->RenameTable(src, dst, cluster);
        }

        return renameTablePromise.GetFuture();
    }

    TFuture<TGenericResult> DropTable(const TString& cluster, const TDropTableSettings& settings) override {
        CHECK_PREPARED_DDL(DropTable);

        auto metadata = SessionCtx->Tables().GetTable(cluster, settings.Table).Metadata;

        std::pair<TString, TString> pathPair;
        TString error;
        if (!NSchemeHelpers::SplitTablePath(metadata->Name, GetDatabase(), pathPair, error, false)) {
            return MakeFuture(ResultFromError<TGenericResult>(error));
        }

        auto temporary = metadata->Temporary;
        auto dropPromise = NewPromise<TGenericResult>();

        NKikimrSchemeOp::TModifyScheme schemeTx;
        schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpDropTable);
        schemeTx.SetWorkingDir(pathPair.first);

        auto* drop = schemeTx.MutableDrop();
        drop->SetName(pathPair.second);

        if (IsPrepare()) {
            auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
            auto& phyTx = *phyQuery.AddTransactions();
            phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);


            phyTx.MutableSchemeOperation()->MutableDropTable()->Swap(&schemeTx);
            phyTx.MutableSchemeOperation()->MutableDropTable()->SetSuccessOnNotExist(settings.SuccessOnNotExist);
            TGenericResult result;
            result.SetSuccess();
            dropPromise.SetValue(result);
        } else {
            if (temporary) {
                auto code = Ydb::StatusIds::BAD_REQUEST;
                auto error = TStringBuilder() << "Not allowed to drop temp table";
                IKqpGateway::TGenericResult errResult;
                errResult.AddIssue(NYql::TIssue(error));
                errResult.SetStatus(NYql::YqlStatusFromYdbStatus(code));
                dropPromise.SetValue(errResult);
            }
            return Gateway->DropTable(cluster, settings);
        }

        return dropPromise.GetFuture();
    }

    TFuture<TGenericResult> CreateTopic(const TString& cluster, Ydb::Topic::CreateTopicRequest&& request, bool existingOk) override {
        CHECK_PREPARED_DDL(CreateTopic);
        Y_UNUSED(cluster);

        std::pair<TString, TString> pathPair;
        TString error;
        auto createPromise = NewPromise<TGenericResult>();
        if (!NSchemeHelpers::SplitTablePath(request.path(), GetDatabase(), pathPair, error, false)) {
            return MakeFuture(ResultFromError<TGenericResult>(error));
        }
        NKikimrSchemeOp::TModifyScheme schemeTx;
        schemeTx.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreatePersQueueGroup);

        schemeTx.SetWorkingDir(pathPair.first);

        auto pqDescr = schemeTx.MutableCreatePersQueueGroup();
        pqDescr->SetName(pathPair.second);
        NKikimr::NGRpcProxy::V1::FillProposeRequestImpl(pathPair.second, request, schemeTx, AppData(ActorSystem), error, pathPair.first);

        if (IsPrepare()) {
            auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
            auto& phyTx = *phyQuery.AddTransactions();
            phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);


            phyTx.MutableSchemeOperation()->MutableCreateTopic()->Swap(&schemeTx);
            phyTx.MutableSchemeOperation()->MutableCreateTopic()->SetFailedOnAlreadyExists(!existingOk);
            TGenericResult result;
            result.SetSuccess();
            createPromise.SetValue(result);
        } else {
            return Gateway->CreateTopic(cluster, std::move(request), existingOk);
        }
        return createPromise.GetFuture();
    }

    TFuture<TGenericResult> AlterTopic(const TString& cluster, Ydb::Topic::AlterTopicRequest&& request, bool missingOk) override {
        CHECK_PREPARED_DDL(AlterTopic);
        Y_UNUSED(cluster);
        std::pair<TString, TString> pathPair;
        TString error;
        if (!NSchemeHelpers::SplitTablePath(request.path(), GetDatabase(), pathPair, error, false)) {
            return MakeFuture(ResultFromError<TGenericResult>(error));
        }
        auto alterPromise = NewPromise<TGenericResult>();

        if (IsPrepare()) {
            TAlterTopicSettings settings{std::move(request), pathPair.second, pathPair.first, missingOk};
            auto getModifySchemeFuture = Gateway->AlterTopicPrepared(std::move(settings));


            auto* phyQuery = SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();

            getModifySchemeFuture.Subscribe([=] (const auto future) mutable {
                TGenericResult result;
                auto modifySchemeResult = future.GetValue();
                if (modifySchemeResult.Status == Ydb::StatusIds::SUCCESS) {
                    if (modifySchemeResult.ModifyScheme.HasAlterPersQueueGroup()) {
                        auto* phyTx = phyQuery->AddTransactions();
                        phyTx->SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);
                        phyTx->MutableSchemeOperation()->MutableAlterTopic()->Swap(&modifySchemeResult.ModifyScheme);
                        phyTx->MutableSchemeOperation()->MutableAlterTopic()->SetSuccessOnNotExist(missingOk);
                    }
                    result.SetSuccess();

                } else {
                    result.SetStatus(NYql::YqlStatusFromYdbStatus(modifySchemeResult.Status));
                    result.AddIssues(modifySchemeResult.Issues);
                }
                alterPromise.SetValue(result);
            });

        } else {
            return Gateway->AlterTopic(cluster, std::move(request), missingOk);
        }
        return alterPromise.GetFuture();

    }

    NThreading::TFuture<NKikimr::NGRpcProxy::V1::TAlterTopicResponse> AlterTopicPrepared(TAlterTopicSettings&& settings) override {
        return Gateway->AlterTopicPrepared(std::move(settings));
    }

    TFuture<TGenericResult> DropTopic(const TString& cluster, const TString& topic, bool missingOk) override {
        CHECK_PREPARED_DDL(DropTopic);
        Y_UNUSED(cluster);

        std::pair<TString, TString> pathPair;
        TString error;
        auto dropPromise = NewPromise<TGenericResult>();
        if (!NSchemeHelpers::SplitTablePath(topic, GetDatabase(), pathPair, error, false)) {
            return MakeFuture(ResultFromError<TGenericResult>(error));
        }

        if (IsPrepare()) {
            auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
            auto& phyTx = *phyQuery.AddTransactions();
            phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);

            NKikimrSchemeOp::TModifyScheme schemeTx;
            schemeTx.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpDropPersQueueGroup);

            schemeTx.SetWorkingDir(pathPair.first);
            schemeTx.MutableDrop()->SetName(pathPair.second);

            phyTx.MutableSchemeOperation()->MutableDropTopic()->Swap(&schemeTx);
            phyTx.MutableSchemeOperation()->MutableDropTopic()->SetSuccessOnNotExist(missingOk);
            TGenericResult result;
            result.SetSuccess();
            dropPromise.SetValue(result);
        } else {
            return Gateway->DropTopic(cluster, topic, missingOk);
        }
        return dropPromise.GetFuture();
    }

    TFuture<TGenericResult> ModifyPermissions(const TString& cluster,
        const TModifyPermissionsSettings& settings) override
    {
        CHECK_PREPARED_DDL(ModifyPermissions);

        if (IsPrepare()) {
            auto modifyPermissionsPromise = NewPromise<TGenericResult>();

            if (settings.Permissions.empty() && !settings.IsPermissionsClear) {
                return MakeFuture(ResultFromError<TGenericResult>("No permissions names for modify permissions"));
            }

            if (settings.Paths.empty()) {
                return MakeFuture(ResultFromError<TGenericResult>("No paths for modify permissions"));
            }

            if (settings.Roles.empty()) {
                return MakeFuture(ResultFromError<TGenericResult>("No roles for modify permissions"));
            }

            NACLib::TDiffACL acl;
            switch (settings.Action) {
                case NYql::TModifyPermissionsSettings::EAction::Grant: {
                    for (const auto& sid : settings.Roles) {
                        for (const auto& permission : settings.Permissions) {
                            TACLAttrs aclAttrs = ConvertYdbPermissionNameToACLAttrs(permission);
                            acl.AddAccess(NACLib::EAccessType::Allow, aclAttrs.AccessMask, sid, aclAttrs.InheritanceType);
                        }
                    }
                    break;
                }
                case NYql::TModifyPermissionsSettings::EAction::Revoke: {
                    if (settings.IsPermissionsClear) {
                        for (const auto& sid : settings.Roles) {
                            acl.ClearAccessForSid(sid);
                        }
                    } else {
                        for (const auto& sid : settings.Roles) {
                            for (const auto& permission : settings.Permissions) {
                                TACLAttrs aclAttrs = ConvertYdbPermissionNameToACLAttrs(permission);
                                acl.RemoveAccess(NACLib::EAccessType::Allow, aclAttrs.AccessMask, sid, aclAttrs.InheritanceType);
                            }
                        }
                    }
                    break;
                }
                default: {
                    return MakeFuture(ResultFromError<TGenericResult>("Unknown permission action"));
                }
            }

            const auto serializedDiffAcl = acl.SerializeAsString();

            for (const auto& currentPath : settings.Paths) {
                auto [dirname, basename] = NSchemeHelpers::SplitPathByDirAndBaseNames(currentPath);
                if (!dirname.empty() && !IsStartWithSlash(dirname)) {
                    dirname = JoinPath({GetDatabase(), dirname});
                }

                NKikimrSchemeOp::TModifyScheme schemeTx;
                schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpModifyACL);
                schemeTx.SetWorkingDir(dirname);
                schemeTx.MutableModifyACL()->SetName(basename);
                schemeTx.MutableModifyACL()->SetDiffACL(serializedDiffAcl);

                auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
                auto& phyTx = *phyQuery.AddTransactions();
                phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);
                phyTx.MutableSchemeOperation()->MutableModifyPermissions()->Swap(&schemeTx);
            }

            TGenericResult result;
            result.SetSuccess();
            modifyPermissionsPromise.SetValue(result);
            return modifyPermissionsPromise;
        } else {
            return Gateway->ModifyPermissions(cluster, settings);
        }
    }

    TFuture<TGenericResult> CreateBackupCollection(const TString& cluster, const NYql::TCreateBackupCollectionSettings& settings) override {
        CHECK_PREPARED_DDL(CreateBackupCollection);

        try {
            if (cluster != SessionCtx->GetCluster()) {
                return InvalidCluster<TGenericResult>(cluster);
            }

            std::pair<TString, TString> pathPair;
            if (settings.Name.StartsWith("/")) {
                TString error;
                if (!NSchemeHelpers::SplitTablePath(settings.Name, GetDatabase(), pathPair, error, true)) {
                    return MakeFuture(ResultFromError<TGenericResult>(error));
                }
            } else {
                pathPair.second = ".backups/collections/" + settings.Name;
            }

            NKikimrSchemeOp::TModifyScheme tx;
            tx.SetWorkingDir(GetDatabase() ? GetDatabase() : NSchemeHelpers::GetDomainDatabase(AppData(ActorSystem)));
            tx.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateBackupCollection);

            auto& op = *tx.MutableCreateBackupCollection();
            op.SetName(pathPair.second);
            op.SetPrefix(settings.Prefix);

            if (settings.Settings.IncrementalBackupEnabled) {
                auto* config = op.MutableIncrementalBackupConfig();
                config->SetOmitIndexes(settings.Settings.OmitIndexes);
            }

            auto errOpt = std::visit(
                TOverloaded {
                    [](const TCreateBackupCollectionSettings::TDatabase&) -> std::optional<TString> {
                        return "Unimplemented";
                    },
                    [&](const TVector<TCreateBackupCollectionSettings::TTable>& tables) -> std::optional<TString> {
                        auto& dstTables = *op.MutableExplicitEntryList();
                        for (const auto& table : tables) {
                            auto& entry = *dstTables.AddEntries();
                            entry.SetType(NKikimrSchemeOp::TBackupCollectionDescription::TBackupEntry::ETypeTable);
                            entry.SetPath(table.Path);
                        }
                        return std::nullopt;
                    },
            }, settings.Entries);

            if (errOpt) {
                return MakeFuture(ResultFromError<TGenericResult>(*errOpt));
            }

            op.MutableCluster();

            if (IsPrepare()) {
                auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
                auto& phyTx = *phyQuery.AddTransactions();
                phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);
                phyTx.MutableSchemeOperation()->MutableCreateBackupCollection()->Swap(&tx);

                TGenericResult result;
                result.SetSuccess();
                return MakeFuture(result);
            } else {
                return Gateway->ModifyScheme(std::move(tx));
            }
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> AlterBackupCollection(const TString& cluster, const NYql::TAlterBackupCollectionSettings& settings) override {
        CHECK_PREPARED_DDL(AlterBackupCollection);

        try {
            if (cluster != SessionCtx->GetCluster()) {
                return MakeFuture(ResultFromError<TGenericResult>("Invalid cluster: " + cluster));
            }

            std::pair<TString, TString> pathPair;
            if (settings.Name.StartsWith("/")) {
                TString error;
                if (!NSchemeHelpers::SplitTablePath(settings.Name, GetDatabase(), pathPair, error, true)) {
                    return MakeFuture(ResultFromError<TGenericResult>(error));
                }
            } else {
                pathPair.second = ".backups/collections/" + settings.Name;
            }

            NKikimrSchemeOp::TModifyScheme tx;
            tx.SetWorkingDir(GetDatabase() ? GetDatabase() : NSchemeHelpers::GetDomainDatabase(AppData(ActorSystem)));
            tx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterBackupCollection);

            auto& op = *tx.MutableAlterBackupCollection();
            op.SetName(pathPair.second);
            op.SetPrefix(settings.Prefix);

            // TODO(innokentii): handle settings
            // TODO(innokentii): add/remove entries

            if (IsPrepare()) {
                auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
                auto& phyTx = *phyQuery.AddTransactions();
                phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);
                phyTx.MutableSchemeOperation()->MutableAlterBackupCollection()->Swap(&tx);

                TGenericResult result;
                result.SetSuccess();
                return MakeFuture(result);
            } else {
                return Gateway->ModifyScheme(std::move(tx));
            }
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> DropBackupCollection(const TString& cluster, const NYql::TDropBackupCollectionSettings& settings) override {
        CHECK_PREPARED_DDL(DropBackupCollection);

        try {
            if (cluster != SessionCtx->GetCluster()) {
                return MakeFuture(ResultFromError<TGenericResult>("Invalid cluster: " + cluster));
            }

            std::pair<TString, TString> pathPair;
            if (settings.Name.StartsWith("/")) {
                TString error;
                if (!NSchemeHelpers::SplitTablePath(settings.Name, GetDatabase(), pathPair, error, true)) {
                    return MakeFuture(ResultFromError<TGenericResult>(error));
                }
            } else {
                pathPair.second = ".backups/collections/" + settings.Name;
            }

            NKikimrSchemeOp::TModifyScheme tx;
            if (settings.Cascade) {
                return MakeFuture(ResultFromError<TGenericResult>("Unimplemented"));
            } else {
                tx.SetOperationType(NKikimrSchemeOp::ESchemeOpDropBackupCollection);
            }

            TString database = GetDatabase() ? GetDatabase() : NSchemeHelpers::GetDomainDatabase(AppData(ActorSystem));
            tx.SetWorkingDir(JoinPath({database, ".backups", "collections"}));

            auto& op = *tx.MutableDrop();
            op.SetName(settings.Name);
            
            auto& dropBackupOp = *tx.MutableDropBackupCollection();
            dropBackupOp.SetName(settings.Name);

            if (IsPrepare()) {
                auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
                auto& phyTx = *phyQuery.AddTransactions();
                phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);
                phyTx.MutableSchemeOperation()->MutableDropBackupCollection()->Swap(&tx);

                TGenericResult result;
                result.SetSuccess();
                return MakeFuture(result);
            } else {
                return Gateway->ModifyScheme(std::move(tx));
            }
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }


    TFuture<TGenericResult> Backup(const TString& cluster, const NYql::TBackupSettings& settings) override {
        CHECK_PREPARED_DDL(Backup);

        try {
            if (cluster != SessionCtx->GetCluster()) {
                return MakeFuture(ResultFromError<TGenericResult>("Invalid cluster: " + cluster));
            }

            std::pair<TString, TString> pathPair;
            if (settings.Name.StartsWith("/")) {
                TString error;
                if (!NSchemeHelpers::SplitTablePath(settings.Name, GetDatabase(), pathPair, error, true)) {
                    return MakeFuture(ResultFromError<TGenericResult>(error));
                }
            } else {
                pathPair.second = ".backups/collections/" + settings.Name;
            }

            NKikimrSchemeOp::TModifyScheme tx;
            tx.SetWorkingDir(GetDatabase() ? GetDatabase() : NSchemeHelpers::GetDomainDatabase(AppData(ActorSystem)));
            tx.SetOperationType(NKikimrSchemeOp::ESchemeOpBackupBackupCollection);

            auto& op = *tx.MutableBackupBackupCollection();
            op.SetName(pathPair.second);

            if (IsPrepare()) {
                auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
                auto& phyTx = *phyQuery.AddTransactions();
                phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);
                phyTx.MutableSchemeOperation()->MutableBackup()->Swap(&tx);

                TGenericResult result;
                result.SetSuccess();
                return MakeFuture(result);
            } else {
                return Gateway->ModifyScheme(std::move(tx));
            }
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> BackupIncremental(const TString& cluster, const NYql::TBackupSettings& settings) override {
        CHECK_PREPARED_DDL(BackupIncremental);

        try {
            if (cluster != SessionCtx->GetCluster()) {
                return MakeFuture(ResultFromError<TGenericResult>("Invalid cluster: " + cluster));
            }

            std::pair<TString, TString> pathPair;
            if (settings.Name.StartsWith("/")) {
                TString error;
                if (!NSchemeHelpers::SplitTablePath(settings.Name, GetDatabase(), pathPair, error, true)) {
                    return MakeFuture(ResultFromError<TGenericResult>(error));
                }
            } else {
                pathPair.second = ".backups/collections/" + settings.Name;
            }

            NKikimrSchemeOp::TModifyScheme tx;
            tx.SetWorkingDir(GetDatabase() ? GetDatabase() : NSchemeHelpers::GetDomainDatabase(AppData(ActorSystem)));
            tx.SetOperationType(NKikimrSchemeOp::ESchemeOpBackupIncrementalBackupCollection);

            auto& op = *tx.MutableBackupIncrementalBackupCollection();
            op.SetName(pathPair.second);

            if (IsPrepare()) {
                auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
                auto& phyTx = *phyQuery.AddTransactions();
                phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);
                phyTx.MutableSchemeOperation()->MutableBackupIncremental()->Swap(&tx);

                TGenericResult result;
                result.SetSuccess();
                return MakeFuture(result);
            } else {
                return Gateway->ModifyScheme(std::move(tx));
            }
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> Restore(const TString& cluster, const NYql::TBackupSettings& settings) override {
        CHECK_PREPARED_DDL(Restore);

        try {
            if (cluster != SessionCtx->GetCluster()) {
                return MakeFuture(ResultFromError<TGenericResult>("Invalid cluster: " + cluster));
            }

            std::pair<TString, TString> pathPair;
            if (settings.Name.StartsWith("/")) {
                TString error;
                if (!NSchemeHelpers::SplitTablePath(settings.Name, GetDatabase(), pathPair, error, true)) {
                    return MakeFuture(ResultFromError<TGenericResult>(error));
                }
            } else {
                pathPair.second = ".backups/collections/" + settings.Name;
            }

            NKikimrSchemeOp::TModifyScheme tx;
            tx.SetWorkingDir(GetDatabase() ? GetDatabase() : NSchemeHelpers::GetDomainDatabase(AppData(ActorSystem)));
            tx.SetOperationType(NKikimrSchemeOp::ESchemeOpRestoreBackupCollection);

            auto& op = *tx.MutableRestoreBackupCollection();
            op.SetName(pathPair.second);

            if (IsPrepare()) {
                auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
                auto& phyTx = *phyQuery.AddTransactions();
                phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);
                phyTx.MutableSchemeOperation()->MutableRestore()->Swap(&tx);

                TGenericResult result;
                result.SetSuccess();
                return MakeFuture(result);
            } else {
                return Gateway->ModifyScheme(std::move(tx));
            }
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> CreateUser(const TString& cluster, const TCreateUserSettings& settings) override {
        CHECK_PREPARED_DDL(CreateUser);

        if (IsPrepare()) {
            auto createUserPromise = NewPromise<TGenericResult>();

            TString database = NSchemeHelpers::SelectDatabaseForAlterLoginOperations(AppData(ActorSystem), GetDatabase());
            if (database.empty()) {
                return MakeFuture(ResultFromError<TGenericResult>("Couldn't get domain name"));
            }

            NKikimrSchemeOp::TModifyScheme schemeTx;
            schemeTx.SetWorkingDir(database);
            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterLogin);
            auto& createUser = *schemeTx.MutableAlterLogin()->MutableCreateUser();

            createUser.SetUser(settings.UserName);
            if (settings.Password) {
                createUser.SetPassword(settings.Password);
                createUser.SetIsHashedPassword(settings.IsHashedPassword);
            }

            createUser.SetCanLogin(settings.CanLogin);

            auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
            auto& phyTx = *phyQuery.AddTransactions();
            phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);

            phyTx.MutableSchemeOperation()->MutableCreateUser()->Swap(&schemeTx);

            TGenericResult result;
            result.SetSuccess();
            createUserPromise.SetValue(result);
            return createUserPromise.GetFuture();
        } else {
            return Gateway->CreateUser(cluster, settings);
        }
    }

    TFuture<TGenericResult> AlterUser(const TString& cluster, const TAlterUserSettings& settings) override {
        CHECK_PREPARED_DDL(AlterUser);

        if (IsPrepare()) {
            auto alterUserPromise = NewPromise<TGenericResult>();

            TString database = NSchemeHelpers::SelectDatabaseForAlterLoginOperations(AppData(ActorSystem), GetDatabase());
            if (database.empty()) {
                return MakeFuture(ResultFromError<TGenericResult>("Couldn't get domain name"));
            }

            NKikimrSchemeOp::TModifyScheme schemeTx;
            schemeTx.SetWorkingDir(database);
            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterLogin);
            auto& alterUser = *schemeTx.MutableAlterLogin()->MutableModifyUser();

            alterUser.SetUser(settings.UserName);

            if (settings.Password.has_value()) {
                alterUser.SetPassword(settings.Password.value());
                alterUser.SetIsHashedPassword(settings.IsHashedPassword);
            }

            if (settings.CanLogin.has_value()) {
                alterUser.SetCanLogin(settings.CanLogin.value());
            }

            auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
            auto& phyTx = *phyQuery.AddTransactions();
            phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);

            phyTx.MutableSchemeOperation()->MutableAlterUser()->Swap(&schemeTx);
            TGenericResult result;
            result.SetSuccess();
            alterUserPromise.SetValue(result);
            return alterUserPromise.GetFuture();
        } else {
            return Gateway->AlterUser(cluster, settings);
        }
    }

    TFuture<TGenericResult> DropUser(const TString& cluster, const TDropUserSettings& settings) override {
        CHECK_PREPARED_DDL(DropUser);

        if (IsPrepare()) {
            auto dropUserPromise = NewPromise<TGenericResult>();

            TString database = NSchemeHelpers::SelectDatabaseForAlterLoginOperations(AppData(ActorSystem), GetDatabase());
            if (database.empty()) {
                return MakeFuture(ResultFromError<TGenericResult>("Couldn't get domain name"));
            }

            NKikimrSchemeOp::TModifyScheme schemeTx;
            schemeTx.SetWorkingDir(database);
            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterLogin);
            auto& dropUser = *schemeTx.MutableAlterLogin()->MutableRemoveUser();
            dropUser.SetUser(settings.UserName);
            dropUser.SetMissingOk(settings.MissingOk);

            auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
            auto& phyTx = *phyQuery.AddTransactions();
            phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);

            phyTx.MutableSchemeOperation()->MutableAlterUser()->Swap(&schemeTx);
            TGenericResult result;
            result.SetSuccess();
            dropUserPromise.SetValue(result);
            return dropUserPromise.GetFuture();
        } else {
            return Gateway->DropUser(cluster, settings);
        }
    }

    struct TRemoveLastPhyTxHelper {
        TRemoveLastPhyTxHelper() = default;

        ~TRemoveLastPhyTxHelper() {
            if (Query) {
                Query->MutableTransactions()->RemoveLast();
            }
        }

        NKqpProto::TKqpPhyTx& Capture(NKqpProto::TKqpPhyQuery* query) {
            Query = query;
            return *Query->AddTransactions();
        }

        void Forget() {
            Query = nullptr;
        }
    private:
        NKqpProto::TKqpPhyQuery* Query = nullptr;
    };

    template <class TSettings>
    TGenericResult PrepareObjectOperation(const TString& cluster, const TSettings& settings,
        NMetadata::NModifications::IOperationsManager::TYqlConclusionStatus (NMetadata::NModifications::IOperationsManager::* prepareMethod)(NKqpProto::TKqpSchemeOperation&, const TSettings&, const NMetadata::IClassBehaviour::TPtr&, const NMetadata::NModifications::IOperationsManager::TExternalModificationContext&) const)
    {
        TRemoveLastPhyTxHelper phyTxRemover;
        try {
            if (cluster != SessionCtx->GetCluster()) {
                return ResultFromError<TGenericResult>("Invalid cluster: " + cluster);
            }

            TString database = NSchemeHelpers::SelectDatabaseForAlterLoginOperations(AppData(ActorSystem), GetDatabase());
            if (database.empty()) {
                return ResultFromError<TGenericResult>("Couldn't get domain name");
            }

            NMetadata::IClassBehaviour::TPtr cBehaviour(NMetadata::IClassBehaviour::TPtr(NMetadata::IClassBehaviour::TFactory::Construct(settings.GetTypeId())));
            if (!cBehaviour) {
                return ResultFromError<TGenericResult>(TStringBuilder() << "Incorrect object type: \"" << settings.GetTypeId() << "\"");
            }

            if (!cBehaviour->GetOperationsManager()) {
                return ResultFromError<TGenericResult>(TStringBuilder() << "Object type \"" << settings.GetTypeId() << "\" does not have manager for operations");
            }

            NMetadata::NModifications::IOperationsManager::TExternalModificationContext context;
            context.SetDatabase(SessionCtx->GetDatabase());
            context.SetDatabaseId(SessionCtx->GetDatabaseId());
            context.SetActorSystem(ActorSystem);
            if (SessionCtx->GetUserToken()) {
                context.SetUserToken(*SessionCtx->GetUserToken());
            }
            context.SetTranslationSettings(SessionCtx->Query().TranslationSettings);

            auto& phyTx = phyTxRemover.Capture(SessionCtx->Query().PreparingQuery->MutablePhysicalQuery());
            phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);
            phyTx.MutableSchemeOperation()->SetObjectType(settings.GetTypeId());

            NMetadata::NModifications::IOperationsManager::TYqlConclusionStatus prepareStatus =
                (cBehaviour->GetOperationsManager().get()->*prepareMethod)(
                    *phyTx.MutableSchemeOperation(), settings, cBehaviour, context);

            TGenericResult result;
            if (prepareStatus.Ok()) {
                result.SetSuccess();
                phyTxRemover.Forget();
            } else {
                result.AddIssue(NYql::TIssue(prepareStatus.GetErrorMessage()));
                result.SetStatus(prepareStatus.GetStatus());
            }
            return result;
        } catch (const std::exception& e) {
            return ResultFromException<TGenericResult>(e);
        }
    }

    TFuture<TGenericResult> UpsertObject(const TString& cluster, const TUpsertObjectSettings& settings) override {
        CHECK_PREPARED_DDL(UpsertObject);

        if (IsPrepare()) {
            return MakeFuture(PrepareObjectOperation(cluster, settings, &NMetadata::NModifications::IOperationsManager::PrepareUpsertObjectSchemeOperation));
        } else {
            return Gateway->UpsertObject(cluster, settings);
        }
    }

    TFuture<TGenericResult> CreateObject(const TString& cluster, const TCreateObjectSettings& settings) override {
        CHECK_PREPARED_DDL(CreateObject);

        if (IsPrepare()) {
            return MakeFuture(PrepareObjectOperation(cluster, settings, &NMetadata::NModifications::IOperationsManager::PrepareCreateObjectSchemeOperation));
        } else {
            return Gateway->CreateObject(cluster, settings);
        }
    }

    TFuture<TGenericResult> AlterObject(const TString& cluster, const TAlterObjectSettings& settings) override {
        CHECK_PREPARED_DDL(AlterObject);

        if (IsPrepare()) {
            return MakeFuture(PrepareObjectOperation(cluster, settings, &NMetadata::NModifications::IOperationsManager::PrepareAlterObjectSchemeOperation));
        } else {
            return Gateway->AlterObject(cluster, settings);
        }
    }

    TFuture<TGenericResult> DropObject(const TString& cluster, const TDropObjectSettings& settings) override {
        CHECK_PREPARED_DDL(DropObject);

        if (IsPrepare()) {
            return MakeFuture(PrepareObjectOperation(cluster, settings, &NMetadata::NModifications::IOperationsManager::PrepareDropObjectSchemeOperation));
        } else {
            return Gateway->DropObject(cluster, settings);
        }
    }

    TFuture<TGenericResult> CreateGroup(const TString& cluster, const TCreateGroupSettings& settings) override {
        CHECK_PREPARED_DDL(CreateGroup);

        if (IsPrepare()) {
            auto createGroupPromise = NewPromise<TGenericResult>();

            TString database = NSchemeHelpers::SelectDatabaseForAlterLoginOperations(AppData(ActorSystem), GetDatabase());
            if (database.empty()) {
                return MakeFuture(ResultFromError<TGenericResult>("Couldn't get domain name"));
            }

            NKikimrSchemeOp::TModifyScheme schemeTx;
            schemeTx.SetWorkingDir(database);
            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterLogin);
            auto& createGroup = *schemeTx.MutableAlterLogin()->MutableCreateGroup();
            createGroup.SetGroup(settings.GroupName);

            auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
            auto& phyTx = *phyQuery.AddTransactions();
            phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);

            phyTx.MutableSchemeOperation()->MutableCreateGroup()->Swap(&schemeTx);

            if (settings.Roles.size()) {
                AddUsersToGroup(database, settings.GroupName, settings.Roles, NYql::TAlterGroupSettings::EAction::AddRoles);
            }

            TGenericResult result;
            result.SetSuccess();
            createGroupPromise.SetValue(result);
            return createGroupPromise.GetFuture();
        } else {
            return Gateway->CreateGroup(cluster, settings);
        }
    }

    TFuture<TGenericResult> AlterGroup(const TString& cluster, TAlterGroupSettings& settings) override {
        CHECK_PREPARED_DDL(UpdateGroup);

        if (IsPrepare()) {
            auto alterGroupPromise = NewPromise<TGenericResult>();

            TString database = NSchemeHelpers::SelectDatabaseForAlterLoginOperations(AppData(ActorSystem), GetDatabase());
            if (database.empty()) {
                return MakeFuture(ResultFromError<TGenericResult>("Couldn't get domain name"));
            }

            if (!settings.Roles.size()) {
                return MakeFuture(ResultFromError<TGenericResult>("No roles given for AlterGroup request"));
            }

            AddUsersToGroup(database, settings.GroupName, settings.Roles, settings.Action);

            TGenericResult result;
            result.SetSuccess();
            alterGroupPromise.SetValue(result);
            return alterGroupPromise.GetFuture();
        } else {
            return Gateway->AlterGroup(cluster, settings);
        }
    }

    TFuture<TGenericResult> RenameGroup(const TString& cluster, TRenameGroupSettings& settings) override {
        CHECK_PREPARED_DDL(RenameGroup);

        if (IsPrepare()) {
            auto renameGroupPromise = NewPromise<TGenericResult>();

            TString database = NSchemeHelpers::SelectDatabaseForAlterLoginOperations(AppData(ActorSystem), GetDatabase());
            if (database.empty()) {
                return MakeFuture(ResultFromError<TGenericResult>("Couldn't get domain name"));
            }

            NKikimrSchemeOp::TModifyScheme schemeTx;
            schemeTx.SetWorkingDir(database);
            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterLogin);
            auto& renameGroup = *schemeTx.MutableAlterLogin()->MutableRenameGroup();
            renameGroup.SetGroup(settings.GroupName);
            renameGroup.SetNewName(settings.NewName);

            auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
            auto& phyTx = *phyQuery.AddTransactions();
            phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);

            phyTx.MutableSchemeOperation()->MutableRenameGroup()->Swap(&schemeTx);
            TGenericResult result;
            result.SetSuccess();
            renameGroupPromise.SetValue(result);
            return renameGroupPromise.GetFuture();
        } else {
            return Gateway->RenameGroup(cluster, settings);
        }
    }

    TFuture<TGenericResult> DropGroup(const TString& cluster, const TDropGroupSettings& settings) override {
        CHECK_PREPARED_DDL(DropGroup);

        if (IsPrepare()) {
            auto dropGroupPromise = NewPromise<TGenericResult>();

            TString database = NSchemeHelpers::SelectDatabaseForAlterLoginOperations(AppData(ActorSystem), GetDatabase());
            if (database.empty()) {
                return MakeFuture(ResultFromError<TGenericResult>("Couldn't get domain name"));
            }

            NKikimrSchemeOp::TModifyScheme schemeTx;
            schemeTx.SetWorkingDir(database);
            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterLogin);
            auto& dropGroup = *schemeTx.MutableAlterLogin()->MutableRemoveGroup();
            dropGroup.SetGroup(settings.GroupName);
            dropGroup.SetMissingOk(settings.MissingOk);

            auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
            auto& phyTx = *phyQuery.AddTransactions();
            phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);

            phyTx.MutableSchemeOperation()->MutableAlterUser()->Swap(&schemeTx);
            TGenericResult result;
            result.SetSuccess();
            dropGroupPromise.SetValue(result);
            return dropGroupPromise.GetFuture();
        } else {
            return Gateway->DropGroup(cluster, settings);
        }
    }

    TFuture<TGenericResult> CreateColumnTable(TKikimrTableMetadataPtr metadata,
            bool createDir, bool existingOk) override {
        CHECK_PREPARED_DDL(CreateColumnTable);

        try {
            const auto& cluster = metadata->Cluster;

            if (cluster != SessionCtx->GetCluster()) {
                return MakeFuture(ResultFromError<TGenericResult>("Invalid cluster: " + cluster));
            }

            std::pair<TString, TString> pathPair;
            {
                TString error;
                if (!NSchemeHelpers::SplitTablePath(metadata->Name, GetDatabase(), pathPair, error, createDir)) {
                    return MakeFuture(ResultFromError<TGenericResult>(error));
                }
            }

            NKikimrSchemeOp::TModifyScheme schemeTx;
            schemeTx.SetWorkingDir(pathPair.first);

            Ydb::StatusIds::StatusCode code;
            TString error;

            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateColumnTable);
            schemeTx.SetFailedOnAlreadyExists(!existingOk);

            NKikimrSchemeOp::TColumnTableDescription* tableDesc = schemeTx.MutableCreateColumnTable();

            tableDesc->SetName(pathPair.second);
            if (!FillColumnTableSchema(*tableDesc->MutableSchema(), *metadata, code, error)) {
                IKqpGateway::TGenericResult errResult;
                errResult.AddIssue(NYql::TIssue(error));
                errResult.SetStatus(NYql::YqlStatusFromYdbStatus(code));
                return MakeFuture(std::move(errResult));
            }

            if (!FillCreateColumnTableDesc(metadata, *tableDesc, code, error)) {
                IKqpGateway::TGenericResult errResult;
                errResult.AddIssue(NYql::TIssue(error));
                errResult.SetStatus(NYql::YqlStatusFromYdbStatus(code));
                return MakeFuture(std::move(errResult));
            }

            if (IsPrepare()) {
                auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
                auto& phyTx = *phyQuery.AddTransactions();
                phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);
                phyTx.MutableSchemeOperation()->MutableCreateTable()->Swap(&schemeTx);

                TGenericResult result;
                result.SetSuccess();
                return MakeFuture(result);
            } else {
                return Gateway->ModifyScheme(std::move(schemeTx));
            }
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> AlterColumnTable(const TString& cluster, Ydb::Table::AlterTableRequest&& req) override {
        CHECK_PREPARED_DDL(AlterColumnTable);

        try {
            if (cluster != SessionCtx->GetCluster()) {
                return MakeFuture(ResultFromError<TGenericResult>("Invalid cluster: " + cluster));
            }

            NKikimrSchemeOp::TModifyScheme schemeTx;

            Ydb::StatusIds::StatusCode code;
            TString error;
            if (!BuildAlterColumnTableModifyScheme(&req, &schemeTx, code, error)) {
                IKqpGateway::TGenericResult errResult;
                errResult.AddIssue(NYql::TIssue(error));
                errResult.SetStatus(NYql::YqlStatusFromYdbStatus(code));
                return MakeFuture(errResult);
            }

            if (IsPrepare()) {
                auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
                auto& phyTx = *phyQuery.AddTransactions();
                phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);
                phyTx.MutableSchemeOperation()->MutableAlterColumnTable()->Swap(&schemeTx);

                TGenericResult result;
                result.SetSuccess();
                return MakeFuture(result);
            } else {
                return Gateway->ModifyScheme(std::move(schemeTx));
            }
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> CreateSequence(const TString& cluster,
            const TCreateSequenceSettings& settings, bool existingOk) override {
        CHECK_PREPARED_DDL(CreateSequence);

        try {

            if (cluster != SessionCtx->GetCluster()) {
                return MakeFuture(ResultFromError<TGenericResult>("Invalid cluster: " + cluster));
            }

            std::pair<TString, TString> pathPair;
            {
                TString error;
                if (!NSchemeHelpers::SplitTablePath(settings.Name, GetDatabase(), pathPair, error, false)) {
                    return MakeFuture(ResultFromError<TGenericResult>(error));
                }
            }

            NKikimrSchemeOp::TModifyScheme schemeTx;
            schemeTx.SetWorkingDir(pathPair.first);

            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateSequence);
            schemeTx.SetFailedOnAlreadyExists(!existingOk);

            NKikimrSchemeOp::TSequenceDescription* seqDesc = schemeTx.MutableSequence();

            seqDesc->SetName(pathPair.second);

            if (settings.SequenceSettings.MinValue) {
                seqDesc->SetMinValue(*settings.SequenceSettings.MinValue);
            }
            if (settings.SequenceSettings.MaxValue) {
                seqDesc->SetMaxValue(*settings.SequenceSettings.MaxValue);
            }
            if (settings.SequenceSettings.Increment) {
                seqDesc->SetIncrement(*settings.SequenceSettings.Increment);
            }
            if (settings.SequenceSettings.StartValue) {
                seqDesc->SetStartValue(*settings.SequenceSettings.StartValue);
            }
            if (settings.SequenceSettings.Cache) {
                seqDesc->SetCache(*settings.SequenceSettings.Cache);
            }
            if (settings.SequenceSettings.Cycle) {
                seqDesc->SetCycle(*settings.SequenceSettings.Cycle);
            }
            if (settings.SequenceSettings.DataType) {
                if (settings.SequenceSettings.DataType == "int8") {
                    seqDesc->SetDataType("pgint8");
                } else if (settings.SequenceSettings.DataType == "int4") {
                    seqDesc->SetDataType("pgint4");
                } else if (settings.SequenceSettings.DataType == "int2") {
                    seqDesc->SetDataType("pgint2");
                }
            }

            if (IsPrepare()) {
                auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
                auto& phyTx = *phyQuery.AddTransactions();
                phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);
                phyTx.MutableSchemeOperation()->MutableCreateSequence()->Swap(&schemeTx);

                TGenericResult result;
                result.SetSuccess();
                return MakeFuture(result);
            } else {
                return Gateway->ModifyScheme(std::move(schemeTx));
            }
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> DropSequence(const TString& cluster,
            const NYql::TDropSequenceSettings& settings, bool missingOk) override {
        CHECK_PREPARED_DDL(DropSequence);

        try {
            if (cluster != SessionCtx->GetCluster()) {
                return MakeFuture(ResultFromError<TGenericResult>("Invalid cluster: " + cluster));
            }

            std::pair<TString, TString> pathPair;
            {
                TString error;
                if (!NSchemeHelpers::SplitTablePath(settings.Name, GetDatabase(), pathPair, error, false)) {
                    return MakeFuture(ResultFromError<TGenericResult>(error));
                }
            }

            NKikimrSchemeOp::TModifyScheme schemeTx;
            schemeTx.SetWorkingDir(pathPair.first);
            schemeTx.SetSuccessOnNotExist(missingOk);
            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpDropSequence);

            NKikimrSchemeOp::TDrop* drop = schemeTx.MutableDrop();
            drop->SetName(pathPair.second);

            if (IsPrepare()) {
                auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
                auto& phyTx = *phyQuery.AddTransactions();
                phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);
                phyTx.MutableSchemeOperation()->MutableDropSequence()->Swap(&schemeTx);

                TGenericResult result;
                result.SetSuccess();
                return MakeFuture(result);
            } else {
                return Gateway->ModifyScheme(std::move(schemeTx));
            }
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> AlterSequence(const TString& cluster,
            const TAlterSequenceSettings& settings, bool missingOk) override {
        CHECK_PREPARED_DDL(AlterSequence);

        try {

            if (cluster != SessionCtx->GetCluster()) {
                return MakeFuture(ResultFromError<TGenericResult>("Invalid cluster: " + cluster));
            }

            std::pair<TString, TString> pathPair;
            {
                TString error;
                if (!NSchemeHelpers::SplitTablePath(settings.Name, GetDatabase(), pathPair, error, false)) {
                    return MakeFuture(ResultFromError<TGenericResult>(error));
                }
            }

            NKikimrSchemeOp::TModifyScheme schemeTx;
            schemeTx.SetWorkingDir(pathPair.first);
            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterSequence);
            schemeTx.SetSuccessOnNotExist(missingOk);

            NKikimrSchemeOp::TSequenceDescription* seqDesc = schemeTx.MutableSequence();
            seqDesc->SetName(pathPair.second);

            if (settings.SequenceSettings.MinValue) {
                seqDesc->SetMinValue(*settings.SequenceSettings.MinValue);
            }
            if (settings.SequenceSettings.MaxValue) {
                seqDesc->SetMaxValue(*settings.SequenceSettings.MaxValue);
            }
            if (settings.SequenceSettings.Increment) {
                seqDesc->SetIncrement(*settings.SequenceSettings.Increment);
            }
            if (settings.SequenceSettings.StartValue) {
                seqDesc->SetStartValue(*settings.SequenceSettings.StartValue);
            }
            if (settings.SequenceSettings.Cache) {
                seqDesc->SetCache(*settings.SequenceSettings.Cache);
            }
            if (settings.SequenceSettings.Cycle) {
                seqDesc->SetCycle(*settings.SequenceSettings.Cycle);
            }
            if (settings.SequenceSettings.DataType) {
                if (settings.SequenceSettings.DataType == "int8") {
                    seqDesc->SetDataType("pgint8");
                } else if (settings.SequenceSettings.DataType == "int4") {
                    seqDesc->SetDataType("pgint4");
                } else if (settings.SequenceSettings.DataType == "int2") {
                    seqDesc->SetDataType("pgint2");
                }
            }

            if (settings.SequenceSettings.Restart) {
                seqDesc->SetRestart(true);
                if (settings.SequenceSettings.RestartValue) {
                    seqDesc->MutableSetVal()->SetNextValue(*settings.SequenceSettings.RestartValue);
                }
            }

            if (IsPrepare()) {
                auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
                auto& phyTx = *phyQuery.AddTransactions();
                phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);
                phyTx.MutableSchemeOperation()->MutableAlterSequence()->Swap(&schemeTx);

                TGenericResult result;
                result.SetSuccess();
                return MakeFuture(result);
            } else {
                return Gateway->ModifyScheme(std::move(schemeTx));
            }
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> CreateTableStore(const TString& cluster,
        const TCreateTableStoreSettings& settings, bool existingOk) override
    {
        CHECK_PREPARED_DDL(CreateTableStore);

        try {
            if (cluster != SessionCtx->GetCluster()) {
                return MakeFuture(ResultFromError<TGenericResult>("Invalid cluster: " + cluster));
            }

            std::pair<TString, TString> pathPair;
            {
                TString error;
                if (!NSchemeHelpers::SplitTablePath(settings.TableStore, GetDatabase(), pathPair, error, false)) {
                    return MakeFuture(ResultFromError<TGenericResult>(error));
                }
            }

            NKikimrSchemeOp::TModifyScheme schemeTx;
            schemeTx.SetWorkingDir(pathPair.first);

            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateColumnStore);
            schemeTx.SetFailedOnAlreadyExists(!existingOk);

            NKikimrSchemeOp::TColumnStoreDescription* storeDesc = schemeTx.MutableCreateColumnStore();
            storeDesc->SetName(pathPair.second);
            storeDesc->SetColumnShardCount(settings.ShardsCount);

            NKikimrSchemeOp::TColumnTableSchemaPreset* schemaPreset = storeDesc->AddSchemaPresets();
            schemaPreset->SetName("default");

            if (!settings.ColumnFamilies.empty()) {
                IKqpGateway::TGenericResult errResult;
                errResult.AddIssue(NYql::TIssue("TableStore does not support column families"));
                errResult.SetStatus(NYql::YqlStatusFromYdbStatus(Ydb::StatusIds::BAD_REQUEST));
                return MakeFuture(std::move(errResult));
            }

            Ydb::StatusIds::StatusCode code;
            TString error;
            if (!FillColumnTableSchema(*schemaPreset->MutableSchema(), settings, code, error)) {
                IKqpGateway::TGenericResult errResult;
                errResult.AddIssue(NYql::TIssue(error));
                errResult.SetStatus(NYql::YqlStatusFromYdbStatus(code));
                return MakeFuture(std::move(errResult));
            }

            if (IsPrepare()) {
                auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
                auto& phyTx = *phyQuery.AddTransactions();
                phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);
                phyTx.MutableSchemeOperation()->MutableCreateTableStore()->Swap(&schemeTx);

                TGenericResult result;
                result.SetSuccess();
                return MakeFuture(result);
            } else {
                return Gateway->ModifyScheme(std::move(schemeTx));
            }
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> AlterTableStore(const TString& cluster,
        const TAlterTableStoreSettings& settings) override
    {
        CHECK_PREPARED_DDL(AlterTableStore);

        try {
            if (cluster != SessionCtx->GetCluster()) {
                return MakeFuture(ResultFromError<TGenericResult>("Invalid cluster: " + cluster));
            }

            std::pair<TString, TString> pathPair;
            {
                TString error;
                if (!NSchemeHelpers::SplitTablePath(settings.TableStore, GetDatabase(), pathPair, error, false)) {
                    return MakeFuture(ResultFromError<TGenericResult>(error));
                }
            }

            NKikimrSchemeOp::TModifyScheme schemeTx;
            schemeTx.SetWorkingDir(pathPair.first);

            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterColumnStore);
            NKikimrSchemeOp::TAlterColumnStore* alter = schemeTx.MutableAlterColumnStore();
            alter->SetName(pathPair.second);

            if (IsPrepare()) {
                auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
                auto& phyTx = *phyQuery.AddTransactions();
                phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);
                phyTx.MutableSchemeOperation()->MutableAlterTableStore()->Swap(&schemeTx);

                TGenericResult result;
                result.SetSuccess();
                return MakeFuture(result);
            } else {
                return Gateway->ModifyScheme(std::move(schemeTx));
            }
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> DropTableStore(const TString& cluster,
        const TDropTableStoreSettings& settings, bool missingOk) override
    {
        CHECK_PREPARED_DDL(DropTableStore);

        try {
            if (cluster != SessionCtx->GetCluster()) {
                return MakeFuture(ResultFromError<TGenericResult>("Invalid cluster: " + cluster));
            }

            std::pair<TString, TString> pathPair;
            {
                TString error;
                if (!NSchemeHelpers::SplitTablePath(settings.TableStore, GetDatabase(), pathPair, error, false)) {
                    return MakeFuture(ResultFromError<TGenericResult>(error));
                }
            }

            NKikimrSchemeOp::TModifyScheme schemeTx;
            schemeTx.SetWorkingDir(pathPair.first);
            schemeTx.SetSuccessOnNotExist(missingOk);
            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpDropColumnStore);
            NKikimrSchemeOp::TDrop* drop = schemeTx.MutableDrop();
            drop->SetName(pathPair.second);

            if (IsPrepare()) {
                auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
                auto& phyTx = *phyQuery.AddTransactions();
                phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);
                phyTx.MutableSchemeOperation()->MutableDropTableStore()->Swap(&schemeTx);

                TGenericResult result;
                result.SetSuccess();
                return MakeFuture(result);
            } else {
                return Gateway->ModifyScheme(std::move(schemeTx));
            }
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> CreateExternalTable(const TString& cluster, const TCreateExternalTableSettings& settings,
        bool createDir, bool existingOk, bool replaceIfExists) override
    {
        CHECK_PREPARED_DDL(CreateExternalTable);

        if (IsPrepare()) {
            if (cluster != SessionCtx->GetCluster()) {
                return MakeFuture(ResultFromError<TGenericResult>("Invalid cluster: " + cluster));
            }

            std::pair<TString, TString> pathPair;
            {
                TString error;
                if (!NSchemeHelpers::SplitTablePath(settings.ExternalTable, GetDatabase(), pathPair, error, createDir)) {
                    return MakeFuture(ResultFromError<TGenericResult>(error));
                }
            }

            TRemoveLastPhyTxHelper phyTxRemover;
            auto& phyTx = phyTxRemover.Capture(SessionCtx->Query().PreparingQuery->MutablePhysicalQuery());
            phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);

            auto& schemeTx = *phyTx.MutableSchemeOperation()->MutableCreateExternalTable();
            schemeTx.SetWorkingDir(pathPair.first);
            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateExternalTable);
            schemeTx.SetFailedOnAlreadyExists(!existingOk);

            NKikimrSchemeOp::TExternalTableDescription& externalTableDesc = *schemeTx.MutableCreateExternalTable();
            NSchemeHelpers::FillCreateExternalTableColumnDesc(externalTableDesc, pathPair.second, replaceIfExists, settings);
            TGenericResult result;
            result.SetSuccess();
            phyTxRemover.Forget();
            return MakeFuture(result);
        } else {
            return Gateway->CreateExternalTable(cluster, settings, createDir, existingOk, replaceIfExists);
        }
    }

    TFuture<TGenericResult> AlterExternalTable(const TString& cluster,
        const TAlterExternalTableSettings& settings) override
    {
        FORWARD_ENSURE_NO_PREPARE(AlterExternalTable, cluster, settings);
    }

    TFuture<TGenericResult> DropExternalTable(const TString& cluster,
        const TDropExternalTableSettings& settings,
        bool missingOk) override
    {
        CHECK_PREPARED_DDL(DropExternalTable);

        if (IsPrepare()) {
            if (cluster != SessionCtx->GetCluster()) {
                return MakeFuture(ResultFromError<TGenericResult>("Invalid cluster: " + cluster));
            }

            std::pair<TString, TString> pathPair;
            {
                TString error;
                if (!NSchemeHelpers::SplitTablePath(settings.ExternalTable, GetDatabase(), pathPair, error, false)) {
                    return MakeFuture(ResultFromError<TGenericResult>(error));
                }
            }

            TRemoveLastPhyTxHelper phyTxRemover;
            auto& phyTx = phyTxRemover.Capture(SessionCtx->Query().PreparingQuery->MutablePhysicalQuery());
            phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);

            auto& schemeTx = *phyTx.MutableSchemeOperation()->MutableDropExternalTable();
            schemeTx.SetWorkingDir(pathPair.first);
            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpDropExternalTable);
            schemeTx.SetSuccessOnNotExist(missingOk);

            NKikimrSchemeOp::TDrop& drop = *schemeTx.MutableDrop();
            drop.SetName(pathPair.second);

            TGenericResult result;
            result.SetSuccess();
            phyTxRemover.Forget();
            return MakeFuture(result);
        } else {
            return Gateway->DropExternalTable(cluster, settings, missingOk);
        }
    }

    static TString AdjustPath(const TString& path, const TString& database) {
        if (path.StartsWith('/')) {
            if (!path.StartsWith(database)) {
                throw yexception() << "Path '" << path << "' not in database '" << database << "'";
            }
            return path;
        } else {
            return database + '/' + path;
        }
    }

    TFuture<TGenericResult> CreateReplication(const TString& cluster, const NYql::TCreateReplicationSettings& settings) override {
        CHECK_PREPARED_DDL(CreateReplication);

        try {
            if (cluster != SessionCtx->GetCluster()) {
                return MakeFuture(ResultFromError<TGenericResult>("Invalid cluster: " + cluster));
            }

            TString error;
            std::pair<TString, TString> pathPair;
            if (!NSchemeHelpers::SplitTablePath(settings.Name, GetDatabase(), pathPair, error, true)) {
                return MakeFuture(ResultFromError<TGenericResult>(error));
            }

            NKikimrSchemeOp::TModifyScheme tx;
            tx.SetWorkingDir(pathPair.first);
            tx.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateReplication);

            auto& op = *tx.MutableReplication();
            op.SetName(pathPair.second);

            auto& config = *op.MutableConfig();
            auto& params = *config.MutableSrcConnectionParams();
            if (const auto& connectionString = settings.Settings.ConnectionString) {
                const auto parseResult = NYdb::ParseConnectionString(*connectionString);
                params.SetEndpoint(TString{parseResult.Endpoint});
                params.SetDatabase(TString{parseResult.Database});
                params.SetEnableSsl(parseResult.EnableSsl);
            }
            if (const auto& endpoint = settings.Settings.Endpoint) {
                params.SetEndpoint(*endpoint);
            }
            if (const auto& database = settings.Settings.Database) {
                params.SetDatabase(*database);
            }
            if (const auto& oauth = settings.Settings.OAuthToken) {
                oauth->Serialize(*params.MutableOAuthToken());
            }
            if (const auto& staticCreds = settings.Settings.StaticCredentials) {
                staticCreds->Serialize(*params.MutableStaticCredentials());
            }
            if (const auto& iamCreds = settings.Settings.IamCredentials) {
                iamCreds->Serialize(*params.MutableIamCredentials());
            }
            if (const auto& caCert = settings.Settings.CaCert) {
                params.SetCaCert(*caCert);
            }
            if (settings.Settings.RowConsistency) {
                config.MutableConsistencySettings()->MutableRow();
            }
            if (const auto& consistency = settings.Settings.GlobalConsistency) {
                consistency->Serialize(*config.MutableConsistencySettings()->MutableGlobal());
            }

            auto& targets = *config.MutableSpecific();
            for (const auto& [src, dst] : settings.Targets) {
                auto& target = *targets.AddTargets();
                target.SetSrcPath(AdjustPath(src, params.GetDatabase()));
                target.SetDstPath(AdjustPath(dst, GetDatabase()));
            }

            if (IsPrepare()) {
                auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
                auto& phyTx = *phyQuery.AddTransactions();
                phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);
                phyTx.MutableSchemeOperation()->MutableCreateReplication()->Swap(&tx);

                TGenericResult result;
                result.SetSuccess();
                return MakeFuture(result);
            } else {
                return Gateway->ModifyScheme(std::move(tx));
            }
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> AlterReplication(const TString& cluster, const NYql::TAlterReplicationSettings& settings) override {
        CHECK_PREPARED_DDL(AlterReplication);

        try {
            if (cluster != SessionCtx->GetCluster()) {
                return MakeFuture(ResultFromError<TGenericResult>("Invalid cluster: " + cluster));
            }

            std::pair<TString, TString> pathPair;
            {
                TString error;
                if (!NSchemeHelpers::SplitTablePath(settings.Name, GetDatabase(), pathPair, error, false)) {
                    return MakeFuture(ResultFromError<TGenericResult>(error));
                }
            }

            NKikimrSchemeOp::TModifyScheme tx;
            tx.SetWorkingDir(pathPair.first);
            tx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterReplication);

            auto& op = *tx.MutableAlterReplication();
            op.SetName(pathPair.second);

            if (const auto& done = settings.Settings.StateDone) {
                auto& state = *op.MutableState();
                state.MutableDone()->SetFailoverMode(
                    static_cast<NKikimrReplication::TReplicationState::TDone::EFailoverMode>(done->FailoverMode));
            } else if (const auto& paused = settings.Settings.StatePaused) {
                auto& state = *op.MutableState();
                state.MutablePaused();
            } else if (const auto& standBy = settings.Settings.StateStandBy) {
                auto& state = *op.MutableState();
                state.MutableStandBy();
            }

            if (settings.Settings.ConnectionString
                || settings.Settings.Endpoint
                || settings.Settings.Database
                || settings.Settings.OAuthToken
                || settings.Settings.StaticCredentials
                || settings.Settings.IamCredentials
                || settings.Settings.CaCert
            ) {
                auto& config = *op.MutableConfig();
                auto& params = *config.MutableSrcConnectionParams();
                if (const auto& connectionString = settings.Settings.ConnectionString) {
                    const auto parseResult = NYdb::ParseConnectionString(*connectionString);
                    params.SetEndpoint(TString{parseResult.Endpoint});
                    params.SetDatabase(TString{parseResult.Database});
                    params.SetEnableSsl(parseResult.EnableSsl);
                }
                if (const auto& endpoint = settings.Settings.Endpoint) {
                    params.SetEndpoint(*endpoint);
                }
                if (const auto& database = settings.Settings.Database) {
                    params.SetDatabase(*database);
                }
                if (const auto& oauth = settings.Settings.OAuthToken) {
                    oauth->Serialize(*params.MutableOAuthToken());
                }
                if (const auto& staticCreds = settings.Settings.StaticCredentials) {
                    staticCreds->Serialize(*params.MutableStaticCredentials());
                }
                if (const auto& iamCreds = settings.Settings.IamCredentials) {
                    iamCreds->Serialize(*params.MutableIamCredentials());
                }
                if (const auto& caCert = settings.Settings.CaCert) {
                    params.SetCaCert(*caCert);
                }
            }

            if (IsPrepare()) {
                auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
                auto& phyTx = *phyQuery.AddTransactions();
                phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);
                phyTx.MutableSchemeOperation()->MutableAlterReplication()->Swap(&tx);

                TGenericResult result;
                result.SetSuccess();
                return MakeFuture(result);
            } else {
                return Gateway->ModifyScheme(std::move(tx));
            }
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> DropReplication(const TString& cluster, const NYql::TDropReplicationSettings& settings) override {
        CHECK_PREPARED_DDL(DropReplication);

        try {
            if (cluster != SessionCtx->GetCluster()) {
                return MakeFuture(ResultFromError<TGenericResult>("Invalid cluster: " + cluster));
            }

            std::pair<TString, TString> pathPair;
            {
                TString error;
                if (!NSchemeHelpers::SplitTablePath(settings.Name, GetDatabase(), pathPair, error, false)) {
                    return MakeFuture(ResultFromError<TGenericResult>(error));
                }
            }

            NKikimrSchemeOp::TModifyScheme tx;
            tx.SetWorkingDir(pathPair.first);
            if (settings.Cascade) {
                tx.SetOperationType(NKikimrSchemeOp::ESchemeOpDropReplicationCascade);
            } else {
                tx.SetOperationType(NKikimrSchemeOp::ESchemeOpDropReplication);
            }

            auto& op = *tx.MutableDrop();
            op.SetName(pathPair.second);

            if (IsPrepare()) {
                auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
                auto& phyTx = *phyQuery.AddTransactions();
                phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);
                phyTx.MutableSchemeOperation()->MutableDropReplication()->Swap(&tx);

                TGenericResult result;
                result.SetSuccess();
                return MakeFuture(result);
            } else {
                return Gateway->ModifyScheme(std::move(tx));
            }
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> CreateTransfer(const TString& cluster, const NYql::TCreateTransferSettings& settings) override {
        CHECK_PREPARED_DDL(CreateTransfer);

        try {
            if (cluster != SessionCtx->GetCluster()) {
                return MakeFuture(ResultFromError<TGenericResult>("Invalid cluster: " + cluster));
            }

            TString error;
            std::pair<TString, TString> pathPair;
            if (!NSchemeHelpers::SplitTablePath(settings.Name, GetDatabase(), pathPair, error, true)) {
                return MakeFuture(ResultFromError<TGenericResult>(error));
            }

            NKikimrSchemeOp::TModifyScheme tx;
            tx.SetWorkingDir(pathPair.first);
            tx.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateTransfer);

            auto& op = *tx.MutableReplication();
            op.SetName(pathPair.second);

            auto& config = *op.MutableConfig();
            auto& params = *config.MutableSrcConnectionParams();
            if (const auto& connectionString = settings.Settings.ConnectionString) {
                const auto parseResult = NYdb::ParseConnectionString(*connectionString);
                params.SetEndpoint(TString{parseResult.Endpoint});
                params.SetDatabase(TString{parseResult.Database});
                params.SetEnableSsl(parseResult.EnableSsl);
            }
            if (const auto& endpoint = settings.Settings.Endpoint) {
                params.SetEndpoint(*endpoint);
            }
            if (const auto& database = settings.Settings.Database) {
                params.SetDatabase(*database);
            }
            if (const auto& oauth = settings.Settings.OAuthToken) {
                oauth->Serialize(*params.MutableOAuthToken());
            }
            if (const auto& staticCreds = settings.Settings.StaticCredentials) {
                staticCreds->Serialize(*params.MutableStaticCredentials());
            }
            if (const auto& iamCreds = settings.Settings.IamCredentials) {
                iamCreds->Serialize(*params.MutableIamCredentials());
            }
            if (const auto& caCert = settings.Settings.CaCert) {
                params.SetCaCert(*caCert);
            }

            {
                const auto& [src, dst, lambda] = settings.Target;
                auto& target = *config.MutableTransferSpecific()->MutableTarget();
                target.SetSrcPath(AdjustPath(src, params.GetDatabase()));
                target.SetDstPath(AdjustPath(dst, GetDatabase()));
                target.SetTransformLambda(lambda);
                if (settings.Settings.Batching && settings.Settings.Batching->BatchSizeBytes) {
                    config.MutableTransferSpecific()->MutableBatching()->SetBatchSizeBytes(settings.Settings.Batching->BatchSizeBytes.value());
                }
                if (settings.Settings.Batching && settings.Settings.Batching->FlushInterval) {
                    config.MutableTransferSpecific()->MutableBatching()->SetFlushIntervalMilliSeconds(settings.Settings.Batching->FlushInterval.MilliSeconds());
                }
                if (settings.Settings.ConsumerName) {
                    target.SetConsumerName(*settings.Settings.ConsumerName);
                }
                if (settings.Settings.DirectoryPath) {
                    target.SetDirectoryPath(*settings.Settings.DirectoryPath);
                }
            }

            if (IsPrepare()) {
                auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
                auto& phyTx = *phyQuery.AddTransactions();
                phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);
                phyTx.MutableSchemeOperation()->MutableCreateReplication()->Swap(&tx);

                TGenericResult result;
                result.SetSuccess();
                return MakeFuture(result);
            } else {
                return Gateway->ModifyScheme(std::move(tx));
            }
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> AlterTransfer(const TString& cluster, const NYql::TAlterTransferSettings& settings) override {
        CHECK_PREPARED_DDL(AlterTransfer);

        try {
            if (cluster != SessionCtx->GetCluster()) {
                return MakeFuture(ResultFromError<TGenericResult>("Invalid cluster: " + cluster));
            }

            std::pair<TString, TString> pathPair;
            {
                TString error;
                if (!NSchemeHelpers::SplitTablePath(settings.Name, GetDatabase(), pathPair, error, false)) {
                    return MakeFuture(ResultFromError<TGenericResult>(error));
                }
            }

            NKikimrSchemeOp::TModifyScheme tx;
            tx.SetWorkingDir(pathPair.first);
            tx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterTransfer);

            auto& op = *tx.MutableAlterReplication();
            op.SetName(pathPair.second);
            if (!settings.TranformLambda.empty()) {
                op.MutableAlterTransfer()->SetTransformLambda(settings.TranformLambda);
            }
            if (auto& batching = settings.Settings.Batching) {
                if (batching->FlushInterval) {
                    op.MutableAlterTransfer()->SetFlushIntervalMilliSeconds(batching->FlushInterval.MilliSeconds());
                }
                if (batching->BatchSizeBytes) {
                    op.MutableAlterTransfer()->SetBatchSizeBytes(batching->BatchSizeBytes.value());
                }
            }

            if (settings.Settings.DirectoryPath) {
                op.MutableAlterTransfer()->SetDirectoryPath(*settings.Settings.DirectoryPath);
            }

            if (const auto& done = settings.Settings.StateDone) {
                auto& state = *op.MutableState();
                state.MutableDone()->SetFailoverMode(
                    static_cast<NKikimrReplication::TReplicationState::TDone::EFailoverMode>(done->FailoverMode));
            } else if (const auto& paused = settings.Settings.StatePaused) {
                auto& state = *op.MutableState();
                state.MutablePaused();
            } else if (const auto& standBy = settings.Settings.StateStandBy) {
                auto& state = *op.MutableState();
                state.MutableStandBy();
            }

            if (settings.Settings.ConnectionString
                || settings.Settings.Endpoint
                || settings.Settings.Database
                || settings.Settings.OAuthToken
                || settings.Settings.StaticCredentials
                || settings.Settings.IamCredentials
                || settings.Settings.CaCert
            ) {
                auto& config = *op.MutableConfig();
                auto& params = *config.MutableSrcConnectionParams();
                if (const auto& connectionString = settings.Settings.ConnectionString) {
                    const auto parseResult = NYdb::ParseConnectionString(*connectionString);
                    params.SetEndpoint(TString{parseResult.Endpoint});
                    params.SetDatabase(TString{parseResult.Database});
                    params.SetEnableSsl(parseResult.EnableSsl);
                }
                if (const auto& endpoint = settings.Settings.Endpoint) {
                    params.SetEndpoint(*endpoint);
                }
                if (const auto& database = settings.Settings.Database) {
                    params.SetDatabase(*database);
                }
                if (const auto& oauth = settings.Settings.OAuthToken) {
                    oauth->Serialize(*params.MutableOAuthToken());
                }
                if (const auto& staticCreds = settings.Settings.StaticCredentials) {
                    staticCreds->Serialize(*params.MutableStaticCredentials());
                }
                if (const auto& iamCreds = settings.Settings.IamCredentials) {
                    iamCreds->Serialize(*params.MutableIamCredentials());
                }
                if (const auto& caCert = settings.Settings.CaCert) {
                    params.SetCaCert(*caCert);
                }
            }

            if (IsPrepare()) {
                auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
                auto& phyTx = *phyQuery.AddTransactions();
                phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);
                phyTx.MutableSchemeOperation()->MutableAlterTransfer()->Swap(&tx);

                TGenericResult result;
                result.SetSuccess();
                return MakeFuture(result);
            } else {
                return Gateway->ModifyScheme(std::move(tx));
            }
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> DropTransfer(const TString& cluster, const NYql::TDropTransferSettings& settings) override {
        CHECK_PREPARED_DDL(DropTransfer);

        try {
            if (cluster != SessionCtx->GetCluster()) {
                return MakeFuture(ResultFromError<TGenericResult>("Invalid cluster: " + cluster));
            }

            std::pair<TString, TString> pathPair;
            {
                TString error;
                if (!NSchemeHelpers::SplitTablePath(settings.Name, GetDatabase(), pathPair, error, false)) {
                    return MakeFuture(ResultFromError<TGenericResult>(error));
                }
            }

            NKikimrSchemeOp::TModifyScheme tx;
            tx.SetWorkingDir(pathPair.first);
            if (settings.Cascade) {
                tx.SetOperationType(NKikimrSchemeOp::ESchemeOpDropTransferCascade);
            } else {
                tx.SetOperationType(NKikimrSchemeOp::ESchemeOpDropTransfer);
            }

            auto& op = *tx.MutableDrop();
            op.SetName(pathPair.second);

            if (IsPrepare()) {
                auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
                auto& phyTx = *phyQuery.AddTransactions();
                phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);
                phyTx.MutableSchemeOperation()->MutableDropTransfer()->Swap(&tx);

                TGenericResult result;
                result.SetSuccess();
                return MakeFuture(result);
            } else {
                return Gateway->ModifyScheme(std::move(tx));
            }
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> Analyze(const TString& cluster, const NYql::TAnalyzeSettings& settings) override {
        CHECK_PREPARED_DDL(Analyze);

        try {
            if (cluster != SessionCtx->GetCluster()) {
                return MakeFuture(ResultFromError<TGenericResult>("Invalid cluster: " + cluster));
            }

            NKqpProto::TKqpAnalyzeOperation analyzeTx;
            analyzeTx.SetTablePath(settings.TablePath);
            for (const auto& column: settings.Columns) {
                *analyzeTx.AddColumns() = column;
            }

            if (IsPrepare()) {
                auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
                auto& phyTx = *phyQuery.AddTransactions();
                phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);

                phyTx.MutableSchemeOperation()->MutableAnalyzeTable()->Swap(&analyzeTx);

                TGenericResult result;
                result.SetSuccess();
                return MakeFuture(result);
            } else {
                return Gateway->Analyze(cluster, settings);
            }
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    template<class TSecretSchemaOp>
    class TSecretSchemaModifier {
    public:
        TSecretSchemaModifier(TIntrusivePtr<IKqpGateway> gateway, TIntrusivePtr<TKikimrSessionContext> sessionCtx)
            : Gateway_(gateway)
            , SessionCtx_(sessionCtx)
        {
        }

        TFuture<TGenericResult> StartModification(const TString& cluster, const NYql::TSecretSettings& settings) {
            if (!SessionCtx_->Config().FeatureFlags.GetEnableSchemaSecrets()) {
                return MakeErrorFuture<IKikimrGateway::TGenericResult>(
                    std::make_exception_ptr(yexception() << "Secrets are disabled. Please contact your system administrator to enable it")
                );
            }

            try {
                if (cluster != SessionCtx_->GetCluster()) {
                    return MakeFuture(ResultFromError<TGenericResult>("Invalid cluster: " + cluster));
                }

                TString error;
                std::pair<TString, TString> pathPair;
                if (!NSchemeHelpers::SplitTablePath(settings.Name, SessionCtx_->GetDatabase(), pathPair, error, /* createDir */ false)) {
                    return MakeFuture(ResultFromError<TGenericResult>(error));
                }

                NKikimrSchemeOp::TModifyScheme tx;
                tx.SetWorkingDir(pathPair.first);
                tx.SetOperationType(GetOperationType());

                TSecretSchemaOp& op = GetSecretSchemaOp(tx);
                op.SetName(pathPair.second);
                FillSchemaOperation(settings, op);

                if (SessionCtx_->Query().PrepareOnly) {
                    auto& phyQuery = *SessionCtx_->Query().PreparingQuery->MutablePhysicalQuery();
                    auto& phyTx = *phyQuery.AddTransactions();
                    phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);
                    FillKqpSchemeOperation(*phyTx.MutableSchemeOperation(), std::move(tx));

                    TGenericResult result;
                    result.SetSuccess();
                    return MakeFuture(result);
                } else {
                    return Gateway_->ModifyScheme(std::move(tx));
                }
            } catch (yexception& e) {
                return MakeFuture(ResultFromException<TGenericResult>(e));
            }
        }

    protected:
        virtual NKikimrSchemeOp::EOperationType GetOperationType() const = 0;
        virtual void FillKqpSchemeOperation(NKqpProto::TKqpSchemeOperation& op, NKikimrSchemeOp::TModifyScheme&& tx) const = 0;
        virtual TSecretSchemaOp& GetSecretSchemaOp(NKikimrSchemeOp::TModifyScheme& tx) const = 0;
        virtual void FillSchemaOperation(const NYql::TSecretSettings& settings, TSecretSchemaOp& op) const = 0;

    private:
        TIntrusivePtr<IKqpGateway> Gateway_;
        TIntrusivePtr<TKikimrSessionContext> SessionCtx_;
    };

    template<class TSecretSchemaOp>
    class TCreateSecretSchemaModifier : public TSecretSchemaModifier<TSecretSchemaOp> {
    public:
        TCreateSecretSchemaModifier(TIntrusivePtr<IKqpGateway> gateway, TIntrusivePtr<TKikimrSessionContext> sessionCtx)
            : TSecretSchemaModifier<TSecretSchemaOp>(gateway, sessionCtx)
        {
        }

    protected:
        NKikimrSchemeOp::EOperationType GetOperationType() const override {
            return NKikimrSchemeOp::ESchemeOpCreateSecret;
        }

        void FillKqpSchemeOperation(NKqpProto::TKqpSchemeOperation& op, NKikimrSchemeOp::TModifyScheme&& tx) const override {
            op.MutableCreateSecret()->Swap(&tx);
        }

        TSecretSchemaOp& GetSecretSchemaOp(NKikimrSchemeOp::TModifyScheme& tx) const override {
            return *tx.MutableCreateSecret();
        }

        void FillSchemaOperation(const NYql::TSecretSettings& settings, TSecretSchemaOp& op) const override {
            op.SetValue(settings.Value);
            op.SetInheritPermissions(settings.InheritPermissions);
        }

    private:
        TIntrusivePtr<IKqpGateway> Gateway_;
        TIntrusivePtr<TKikimrSessionContext> SessionCtx_;
    };

    template<class TSecretSchemaOp>
    class TAlterSecretSchemaModifier : public TSecretSchemaModifier<TSecretSchemaOp> {
    public:
        TAlterSecretSchemaModifier(TIntrusivePtr<IKqpGateway> gateway, TIntrusivePtr<TKikimrSessionContext> sessionCtx)
            : TSecretSchemaModifier<TSecretSchemaOp>(gateway, sessionCtx)
        {
        }

    protected:
        NKikimrSchemeOp::EOperationType GetOperationType() const override {
            return NKikimrSchemeOp::ESchemeOpAlterSecret;
        }

        void FillKqpSchemeOperation(NKqpProto::TKqpSchemeOperation& op, NKikimrSchemeOp::TModifyScheme&& tx) const override {
            op.MutableAlterSecret()->Swap(&tx);
        }

        TSecretSchemaOp& GetSecretSchemaOp(NKikimrSchemeOp::TModifyScheme& tx) const override {
            return *tx.MutableAlterSecret();
        }

        void FillSchemaOperation(const NYql::TSecretSettings& settings, TSecretSchemaOp& op) const override {
            op.SetValue(settings.Value);
        }

    private:
        TIntrusivePtr<IKqpGateway> Gateway_;
        TIntrusivePtr<TKikimrSessionContext> SessionCtx_;
    };

    template<class TSecretSchemaOp>
    class TDropSecretSchemaModifier : public TSecretSchemaModifier<TSecretSchemaOp> {
    public:
        TDropSecretSchemaModifier(TIntrusivePtr<IKqpGateway> gateway, TIntrusivePtr<TKikimrSessionContext> sessionCtx)
            : TSecretSchemaModifier<TSecretSchemaOp>(gateway, sessionCtx)
        {
        }

    protected:
        NKikimrSchemeOp::EOperationType GetOperationType() const override {
            return NKikimrSchemeOp::ESchemeOpDropSecret;
        }

        void FillKqpSchemeOperation(NKqpProto::TKqpSchemeOperation& op, NKikimrSchemeOp::TModifyScheme&& tx) const override {
            op.MutableDropSecret()->Swap(&tx);
        }

        TSecretSchemaOp& GetSecretSchemaOp(NKikimrSchemeOp::TModifyScheme& tx) const override {
            return *tx.MutableDrop();
        }

        void FillSchemaOperation(const NYql::TSecretSettings&, TSecretSchemaOp&) const override {
        }

    private:
        TIntrusivePtr<IKqpGateway> Gateway_;
        TIntrusivePtr<TKikimrSessionContext> SessionCtx_;
    };

    TFuture<TGenericResult> CreateSecret(const TString& cluster, const NYql::TSecretSettings& settings) override {
        CHECK_PREPARED_DDL(CreateSecret);

        return TCreateSecretSchemaModifier<NKikimrSchemeOp::TSecretSchemaOp>(Gateway, SessionCtx).StartModification(cluster, settings);
    }

    TFuture<TGenericResult> AlterSecret(const TString& cluster, const NYql::TSecretSettings& settings) override {
        CHECK_PREPARED_DDL(AlterSecret);

        return TAlterSecretSchemaModifier<NKikimrSchemeOp::TSecretSchemaOp>(Gateway, SessionCtx).StartModification(cluster, settings);
    }

    TFuture<TGenericResult> DropSecret(const TString& cluster, const NYql::TSecretSettings& settings) override {
        CHECK_PREPARED_DDL(DropSecret);

        return TDropSecretSchemaModifier<NKikimrSchemeOp::TDrop>(Gateway, SessionCtx).StartModification(cluster, settings);
    }

    TVector<NKikimrKqp::TKqpTableMetadataProto> GetCollectedSchemeData() override {
        return Gateway->GetCollectedSchemeData();
    }

    TExecuteLiteralResult ExecuteLiteralInstant(const TString& program, ui32 langVer,
        const NKikimrMiniKQL::TType& resultType, NKikimr::NKqp::TTxAllocatorState::TPtr txAlloc) override
    {
        return Gateway->ExecuteLiteralInstant(program, langVer, resultType, txAlloc);
    }

private:
    bool IsPrepare() const {
        if (!SessionCtx) {
            return false;
        }

        return SessionCtx->Query().PrepareOnly;
    }

    TString GetDatabase() const {
        if (SessionCtx) {
            return SessionCtx->GetDatabase();
        }

        return Gateway->GetDatabase();
    }

    void AddUsersToGroup(const TString& database, const TString& group, const std::vector<TString>& roles, const NYql::TAlterGroupSettings::EAction& action) {
        for (const auto& role : roles) {
            NKikimrSchemeOp::TModifyScheme schemeTx;
            schemeTx.SetWorkingDir(database);
            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterLogin);

            auto& phyQuery = *SessionCtx->Query().PreparingQuery->MutablePhysicalQuery();
            auto& phyTx = *phyQuery.AddTransactions();
            phyTx.SetType(NKqpProto::TKqpPhyTx::TYPE_SCHEME);

            switch (action) {
                case NYql::TAlterGroupSettings::EAction::AddRoles: {
                    auto& alterGroup = *schemeTx.MutableAlterLogin()->MutableAddGroupMembership();
                    alterGroup.SetGroup(group);
                    alterGroup.SetMember(role);
                    phyTx.MutableSchemeOperation()->MutableAddGroupMembership()->Swap(&schemeTx);
                    break;
                }
                case NYql::TAlterGroupSettings::EAction::RemoveRoles: {
                    auto& alterGroup = *schemeTx.MutableAlterLogin()->MutableRemoveGroupMembership();
                    alterGroup.SetGroup(group);
                    alterGroup.SetMember(role);
                    phyTx.MutableSchemeOperation()->MutableRemoveGroupMembership()->Swap(&schemeTx);
                    break;
                }
            }
        }
    }

private:
    TIntrusivePtr<IKqpGateway> Gateway;
    TIntrusivePtr<TKikimrSessionContext> SessionCtx;
    TActorSystem* ActorSystem = nullptr;
};

#undef FORWARD_ENSURE_NO_PREPARE
#undef CHECK_PREPARED_DDL

} // namespace

TIntrusivePtr<IKikimrGateway> CreateKqpGatewayProxy(const TIntrusivePtr<IKqpGateway>& gateway,
    const TIntrusivePtr<TKikimrSessionContext>& sessionCtx, TActorSystem* actorSystem)
{
    return MakeIntrusive<TKqpGatewayProxy>(gateway, sessionCtx, actorSystem);
}

} // namespace NKikimr::NKqp
