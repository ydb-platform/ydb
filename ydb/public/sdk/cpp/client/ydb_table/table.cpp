#include "table.h"

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/scheme_helpers/helpers.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/table_helpers/helpers.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/make_request/make.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_stats/stats.h>
#include <ydb/public/sdk/cpp/client/ydb_common_client/impl/client.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>
#include <ydb/public/sdk/cpp/client/ydb_value/value.h>
#include <ydb/public/sdk/cpp/client/ydb_table/impl/client_session.h>
#include <ydb/public/sdk/cpp/client/ydb_table/impl/data_query.h>
#include <ydb/public/sdk/cpp/client/ydb_table/impl/request_migrator.h>
#include <ydb/public/sdk/cpp/client/resources/ydb_resources.h>

#include <google/protobuf/util/time_util.h>

#include <library/cpp/cache/cache.h>

#include <util/generic/map.h>
#include <util/random/random.h>
#include <util/string/join.h>

#include <unordered_map>

namespace NYdb {
namespace NTable {

using namespace NThreading;

//How often run session pool keep alive check
constexpr TDuration PERIODIC_ACTION_INTERVAL = TDuration::Seconds(5);
//How ofter run host scan to perform session balancing
constexpr TDuration HOSTSCAN_PERIODIC_ACTION_INTERVAL = TDuration::Seconds(2);
constexpr ui64 KEEP_ALIVE_RANDOM_FRACTION = 4;
constexpr TDuration KEEP_ALIVE_CLIENT_TIMEOUT = TDuration::Seconds(5);
constexpr ui64 PERIODIC_ACTION_BATCH_SIZE = 10; //Max number of tasks to perform during one interval
constexpr ui32 MAX_BACKOFF_DURATION_MS = TDuration::Hours(1).MilliSeconds();

////////////////////////////////////////////////////////////////////////////////

class TStorageSettings::TImpl {
public:
    TImpl() { }

    explicit TImpl(const Ydb::Table::StorageSettings& proto)
        : Proto_(proto)
    { }

public:
    const Ydb::Table::StorageSettings Proto_;
};

TStorageSettings::TStorageSettings()
    : Impl_(std::make_shared<TImpl>())
{ }

TStorageSettings::TStorageSettings(const Ydb::Table::StorageSettings& proto)
    : Impl_(std::make_shared<TImpl>(proto))
{ }

const Ydb::Table::StorageSettings& TStorageSettings::GetProto() const {
    return Impl_->Proto_;
}

TMaybe<TString> TStorageSettings::GetTabletCommitLog0() const {
    if (GetProto().has_tablet_commit_log0()) {
        return GetProto().tablet_commit_log0().media();
    } else {
        return { };
    }
}

TMaybe<TString> TStorageSettings::GetTabletCommitLog1() const {
    if (GetProto().has_tablet_commit_log1()) {
        return GetProto().tablet_commit_log1().media();
    } else {
        return { };
    }
}

TMaybe<TString> TStorageSettings::GetExternal() const {
    if (GetProto().has_external()) {
        return GetProto().external().media();
    } else {
        return { };
    }
}

TMaybe<bool> TStorageSettings::GetStoreExternalBlobs() const {
    switch (GetProto().store_external_blobs()) {
        case Ydb::FeatureFlag::ENABLED:
            return true;
        case Ydb::FeatureFlag::DISABLED:
            return false;
        default:
            return { };
    }
}

////////////////////////////////////////////////////////////////////////////////

class TColumnFamilyDescription::TImpl {
public:
    explicit TImpl(const Ydb::Table::ColumnFamily& desc)
        : Proto_(desc)
    { }

public:
    const Ydb::Table::ColumnFamily Proto_;
};

TColumnFamilyDescription::TColumnFamilyDescription(const Ydb::Table::ColumnFamily& desc)
    : Impl_(std::make_shared<TImpl>(desc))
{ }

const Ydb::Table::ColumnFamily& TColumnFamilyDescription::GetProto() const {
    return Impl_->Proto_;
}

const TString& TColumnFamilyDescription::GetName() const {
    return GetProto().name();
}

TMaybe<TString> TColumnFamilyDescription::GetData() const {
    if (GetProto().has_data()) {
        return GetProto().data().media();
    } else {
        return { };
    }
}

TMaybe<EColumnFamilyCompression> TColumnFamilyDescription::GetCompression() const {
    switch (GetProto().compression()) {
        case Ydb::Table::ColumnFamily::COMPRESSION_NONE:
            return EColumnFamilyCompression::None;
        case Ydb::Table::ColumnFamily::COMPRESSION_LZ4:
            return EColumnFamilyCompression::LZ4;
        default:
            return { };
    }
}

TMaybe<bool> TColumnFamilyDescription::GetKeepInMemory() const {
    switch (GetProto().keep_in_memory()) {
        case Ydb::FeatureFlag::ENABLED:
            return true;
        case Ydb::FeatureFlag::DISABLED:
            return false;
        default:
            return { };
    }
}

TBuildIndexOperation::TBuildIndexOperation(TStatus &&status, Ydb::Operations::Operation &&operation)
    : TOperation(std::move(status), std::move(operation))
{
    Ydb::Table::IndexBuildMetadata metadata;
    GetProto().metadata().UnpackTo(&metadata);
    Metadata_.State = static_cast<EBuildIndexState>(metadata.state());
    Metadata_.Progress = metadata.progress();
    const auto& desc = metadata.description();
    Metadata_.Path = desc.path();
    Metadata_.Desctiption = TProtoAccessor::FromProto(desc.index());
}

const TBuildIndexOperation::TMetadata& TBuildIndexOperation::Metadata() const {
    return Metadata_;
}

////////////////////////////////////////////////////////////////////////////////

class TPartitioningSettings::TImpl {
public:
    TImpl() { }

    explicit TImpl(const Ydb::Table::PartitioningSettings& proto)
        : Proto_(proto)
    { }

public:
    const Ydb::Table::PartitioningSettings Proto_;
};

TPartitioningSettings::TPartitioningSettings()
    : Impl_(std::make_shared<TImpl>())
{ }

TPartitioningSettings::TPartitioningSettings(const Ydb::Table::PartitioningSettings& proto)
    : Impl_(std::make_shared<TImpl>(proto))
{ }

const Ydb::Table::PartitioningSettings& TPartitioningSettings::GetProto() const {
    return Impl_->Proto_;
}

TMaybe<bool> TPartitioningSettings::GetPartitioningBySize() const {
    switch (GetProto().partitioning_by_size()) {
    case Ydb::FeatureFlag::ENABLED:
        return true;
    case Ydb::FeatureFlag::DISABLED:
        return false;
    default:
        return { };
    }
}

TMaybe<bool> TPartitioningSettings::GetPartitioningByLoad() const {
    switch (GetProto().partitioning_by_load()) {
    case Ydb::FeatureFlag::ENABLED:
        return true;
    case Ydb::FeatureFlag::DISABLED:
        return false;
    default:
        return { };
    }
}

ui64 TPartitioningSettings::GetPartitionSizeMb() const {
    return GetProto().partition_size_mb();
}

ui64 TPartitioningSettings::GetMinPartitionsCount() const {
    return GetProto().min_partitions_count();
}

ui64 TPartitioningSettings::GetMaxPartitionsCount() const {
    return GetProto().max_partitions_count();
}

////////////////////////////////////////////////////////////////////////////////

struct TTableStats {
    ui64 Rows = 0;
    ui64 Size = 0;
    ui64 Partitions = 0;
    TInstant ModificationTime;
    TInstant CreationTime;
};

static TInstant ProtobufTimestampToTInstant(const NProtoBuf::Timestamp& timestamp) {
    ui64 lastModificationUs = timestamp.seconds() * 1000000;
    lastModificationUs += timestamp.nanos() / 1000;
    return TInstant::MicroSeconds(lastModificationUs);
}

static void SerializeTo(const TRenameIndex& rename, Ydb::Table::RenameIndexItem& proto) {
    proto.set_source_name(rename.SourceName_);
    proto.set_destination_name(rename.DestinationName_);
    proto.set_replace_destination(rename.ReplaceDestination_);
}

class TTableDescription::TImpl {
    using EUnit = TValueSinceUnixEpochModeSettings::EUnit;

    template <typename TProto>
    TImpl(const TProto& proto)
        : StorageSettings_(proto.storage_settings())
        , PartitioningSettings_(proto.partitioning_settings())
        , HasStorageSettings_(proto.has_storage_settings())
        , HasPartitioningSettings_(proto.has_partitioning_settings())
    {
        // primary key
        for (const auto& pk : proto.primary_key()) {
            PrimaryKey_.push_back(pk);
        }

        // columns
        for (const auto& col : proto.columns()) {
            Columns_.emplace_back(col.name(), col.type(), col.family());
        }

        // indexes
        Indexes_.reserve(proto.indexesSize());
        for (const auto& index : proto.indexes()) {
            Indexes_.emplace_back(TProtoAccessor::FromProto(index));
        }

        if constexpr (std::is_same_v<TProto, Ydb::Table::DescribeTableResult>) {
            // changefeeds
            Changefeeds_.reserve(proto.changefeedsSize());
            for (const auto& changefeed : proto.changefeeds()) {
                Changefeeds_.emplace_back(TProtoAccessor::FromProto(changefeed));
            }
        }

        // ttl settings
        switch (proto.ttl_settings().mode_case()) {
        case Ydb::Table::TtlSettings::kDateTypeColumn:
            TtlSettings_ = TTtlSettings(
                proto.ttl_settings().date_type_column(),
                proto.ttl_settings().run_interval_seconds()
            );
            break;

        case Ydb::Table::TtlSettings::kValueSinceUnixEpoch:
            TtlSettings_ = TTtlSettings(
                proto.ttl_settings().value_since_unix_epoch(),
                proto.ttl_settings().run_interval_seconds()
            );
            break;

        default:
            break;
        }

        // tiering
        if (proto.tiering().size()) {
            Tiering_ = proto.tiering();
        }

        // column families
        ColumnFamilies_.reserve(proto.column_families_size());
        for (const auto& family : proto.column_families()) {
            ColumnFamilies_.emplace_back(family);
        }

        // attributes
        for (auto [key, value] : proto.attributes()) {
            Attributes_[key] = value;
        }

        // key bloom filter
        switch (proto.key_bloom_filter()) {
        case Ydb::FeatureFlag::ENABLED:
            KeyBloomFilter_ = true;
            break;
        case Ydb::FeatureFlag::DISABLED:
            KeyBloomFilter_ = false;
            break;
        default:
            break;
        }

        // read replicas settings
        if (proto.has_read_replicas_settings()) {
            const auto settings = proto.read_replicas_settings();
            switch (settings.settings_case()) {
            case Ydb::Table::ReadReplicasSettings::kPerAzReadReplicasCount:
                ReadReplicasSettings_ = TReadReplicasSettings(
                    TReadReplicasSettings::EMode::PerAz,
                    settings.per_az_read_replicas_count());
                break;
            case Ydb::Table::ReadReplicasSettings::kAnyAzReadReplicasCount:
                ReadReplicasSettings_ = TReadReplicasSettings(
                    TReadReplicasSettings::EMode::AnyAz,
                    settings.any_az_read_replicas_count());
                break;
            default:
                break;
            }
        }
    }

public:
    TImpl(Ydb::Table::DescribeTableResult&& desc, const TDescribeTableSettings& describeSettings)
        : TImpl(desc)
    {
        Proto_ = std::move(desc);

        Owner_ = Proto_.self().owner();
        PermissionToSchemeEntry(Proto_.self().permissions(), &Permissions_);
        PermissionToSchemeEntry(Proto_.self().effective_permissions(), &EffectivePermissions_);

        TMaybe<TValue> leftValue;
        for (const auto& bound : Proto_.shard_key_bounds()) {
            TMaybe<TKeyBound> fromBound = leftValue
                ? TKeyBound::Inclusive(*leftValue)
                : TMaybe<TKeyBound>();

            TValue value(TType(bound.type()), bound.value());
            const TKeyBound& toBound = TKeyBound::Exclusive(value);

            Ranges_.emplace_back(TKeyRange(fromBound, toBound));
            leftValue = value;
        }

        for (const auto& shardStats : Proto_.table_stats().partition_stats()) {
            PartitionStats_.emplace_back(
                TPartitionStats{shardStats.rows_estimate(), shardStats.store_size()}
            );
        }

        TableStats.Rows = Proto_.table_stats().rows_estimate();
        TableStats.Size = Proto_.table_stats().store_size();
        TableStats.Partitions = Proto_.table_stats().partitions();

        TableStats.ModificationTime = ProtobufTimestampToTInstant(Proto_.table_stats().modification_time());
        TableStats.CreationTime = ProtobufTimestampToTInstant(Proto_.table_stats().creation_time());

        if (describeSettings.WithKeyShardBoundary_) {
            Ranges_.emplace_back(TKeyRange(
                leftValue ? TKeyBound::Inclusive(*leftValue) : TMaybe<TKeyBound>(),
                TMaybe<TKeyBound>()));
        }
    }

    struct TCreateTableRequestTag {}; // to avoid delegation cycle

    TImpl(const Ydb::Table::CreateTableRequest& request, TCreateTableRequestTag)
        : TImpl(request)
    {
        if (request.compaction_policy()) {
            SetCompactionPolicy(request.compaction_policy());
        }

        switch (request.partitions_case()) {
            case Ydb::Table::CreateTableRequest::kUniformPartitions:
                SetUniformPartitions(request.uniform_partitions());
                break;

            case Ydb::Table::CreateTableRequest::kPartitionAtKeys: {
                TExplicitPartitions partitionAtKeys;
                for (const auto& splitPoint : request.partition_at_keys().split_points()) {
                    TValue value(TType(splitPoint.type()), splitPoint.value());
                    partitionAtKeys.AppendSplitPoints(value);
                }

                SetPartitionAtKeys(partitionAtKeys);
                break;
            }

            default:
                break;
        }
    }

    TImpl() = default;

    const Ydb::Table::DescribeTableResult& GetProto() const {
        return Proto_;
    }

    void AddColumn(const TString& name, const Ydb::Type& type, const TString& family) {
        Columns_.emplace_back(name, type, family);
    }

    void SetPrimaryKeyColumns(const TVector<TString>& primaryKeyColumns) {
        PrimaryKey_ = primaryKeyColumns;
    }

    void AddSecondaryIndex(const TString& indexName, EIndexType type, const TVector<TString>& indexColumns) {
        Indexes_.emplace_back(TIndexDescription(indexName, type, indexColumns));
    }

    void AddSecondaryIndex(const TString& indexName, EIndexType type, const TVector<TString>& indexColumns, const TVector<TString>& dataColumns) {
        Indexes_.emplace_back(TIndexDescription(indexName, type, indexColumns, dataColumns));
    }

    void AddChangefeed(const TString& name, EChangefeedMode mode, EChangefeedFormat format) {
        Changefeeds_.emplace_back(name, mode, format);
    }

    void SetTtlSettings(TTtlSettings&& settings) {
        TtlSettings_ = std::move(settings);
    }

    void SetTtlSettings(const TTtlSettings& settings) {
        TtlSettings_ = settings;
    }

    void SetStorageSettings(const TStorageSettings& settings) {
        StorageSettings_ = settings;
        HasStorageSettings_ = true;
    }

    void AddColumnFamily(const TColumnFamilyDescription& desc) {
        ColumnFamilies_.emplace_back(desc);
    }

    void AddAttribute(const TString& key, const TString& value) {
        Attributes_[key] = value;
    }

    void SetAttributes(const THashMap<TString, TString>& attrs) {
        Attributes_ = attrs;
    }

    void SetAttributes(THashMap<TString, TString>&& attrs) {
        Attributes_ = std::move(attrs);
    }

    void SetCompactionPolicy(const TString& name) {
        CompactionPolicy_ = name;
    }

    void SetUniformPartitions(ui64 partitionsCount) {
        UniformPartitions_ = partitionsCount;
    }

    void SetPartitionAtKeys(const TExplicitPartitions& keys) {
        PartitionAtKeys_ = keys;
    }

    void SetPartitioningSettings(const TPartitioningSettings& settings) {
        PartitioningSettings_ = settings;
        HasPartitioningSettings_ = true;
    }

    void SetKeyBloomFilter(bool enabled) {
        KeyBloomFilter_ = enabled;
    }

    void SetReadReplicasSettings(TReadReplicasSettings::EMode mode, ui64 readReplicasCount) {
        ReadReplicasSettings_ = TReadReplicasSettings(mode, readReplicasCount);
    }

    const TVector<TString>& GetPrimaryKeyColumns() const {
        return PrimaryKey_;
    }

    const TVector<TTableColumn>& GetColumns() const {
        return Columns_;
    }

    const TVector<TIndexDescription>& GetIndexDescriptions() const {
        return Indexes_;
    }

    const TVector<TChangefeedDescription>& GetChangefeedDescriptions() const {
        return Changefeeds_;
    }

    const TMaybe<TTtlSettings>& GetTtlSettings() const {
        return TtlSettings_;
    }

    const TMaybe<TString>& GetTiering() const {
        return Tiering_;
    }

    const TString& GetOwner() const {
        return Owner_;
    }

    const TVector<NScheme::TPermissions>& GetPermissions() const {
        return Permissions_;
    }

    const TVector<NScheme::TPermissions>& GetEffectivePermissions() const {
        return EffectivePermissions_;
    }

    const TVector<TKeyRange>& GetKeyRanges() const {
        return Ranges_;
    }

    const TVector<TPartitionStats>& GetPartitionStats() const {
        return PartitionStats_;
    }

    const TTableStats& GetTableStats() const {
        return TableStats;
    }

    bool HasStorageSettings() const {
        return HasStorageSettings_;
    }

    const TStorageSettings& GetStorageSettings() const {
        return StorageSettings_;
    }

    const TVector<TColumnFamilyDescription>& GetColumnFamilies() const {
        return ColumnFamilies_;
    }

    const THashMap<TString, TString>& GetAttributes() const {
        return Attributes_;
    }

    const TString& GetCompactionPolicy() const {
        return CompactionPolicy_;
    }

    const TMaybe<ui64>& GetUniformPartitions() const {
        return UniformPartitions_;
    }

    const TMaybe<TExplicitPartitions>& GetPartitionAtKeys() const {
        return PartitionAtKeys_;
    }

    bool HasPartitioningSettings() const {
        return HasPartitioningSettings_;
    }

    const TPartitioningSettings& GetPartitioningSettings() const {
        return PartitioningSettings_;
    }

    TMaybe<bool> GetKeyBloomFilter() const {
        return KeyBloomFilter_;
    }

    const TMaybe<TReadReplicasSettings>& GetReadReplicasSettings() const {
        return ReadReplicasSettings_;
    }

private:
    Ydb::Table::DescribeTableResult Proto_;
    TStorageSettings StorageSettings_;
    TVector<TString> PrimaryKey_;
    TVector<TTableColumn> Columns_;
    TVector<TIndexDescription> Indexes_;
    TVector<TChangefeedDescription> Changefeeds_;
    TMaybe<TTtlSettings> TtlSettings_;
    TMaybe<TString> Tiering_;
    TString Owner_;
    TVector<NScheme::TPermissions> Permissions_;
    TVector<NScheme::TPermissions> EffectivePermissions_;
    TVector<TKeyRange> Ranges_;
    TVector<TPartitionStats> PartitionStats_;
    TTableStats TableStats;
    TVector<TColumnFamilyDescription> ColumnFamilies_;
    THashMap<TString, TString> Attributes_;
    TString CompactionPolicy_;
    TMaybe<ui64> UniformPartitions_;
    TMaybe<TExplicitPartitions> PartitionAtKeys_;
    TPartitioningSettings PartitioningSettings_;
    TMaybe<bool> KeyBloomFilter_;
    TMaybe<TReadReplicasSettings> ReadReplicasSettings_;
    bool HasStorageSettings_ = false;
    bool HasPartitioningSettings_ = false;
};

TTableDescription::TTableDescription()
    : Impl_(new TImpl)
{
}

TTableDescription::TTableDescription(Ydb::Table::DescribeTableResult&& desc,
    const TDescribeTableSettings& describeSettings)
    : Impl_(new TImpl(std::move(desc), describeSettings))
{
}

TTableDescription::TTableDescription(const Ydb::Table::CreateTableRequest& request)
    : Impl_(new TImpl(request, TImpl::TCreateTableRequestTag()))
{
}

const TVector<TString>& TTableDescription::GetPrimaryKeyColumns() const {
    return Impl_->GetPrimaryKeyColumns();
}

TVector<TColumn> TTableDescription::GetColumns() const {
    // Conversion to TColumn for API compatibility
    const auto& columns = Impl_->GetColumns();
    TVector<TColumn> legacy(Reserve(columns.size()));
    for (const auto& column : columns) {
        legacy.emplace_back(column.Name, column.Type);
    }
    return legacy;
}

TVector<TTableColumn> TTableDescription::GetTableColumns() const {
    return Impl_->GetColumns();
}

TVector<TIndexDescription> TTableDescription::GetIndexDescriptions() const {
    return Impl_->GetIndexDescriptions();
}

TVector<TChangefeedDescription> TTableDescription::GetChangefeedDescriptions() const {
    return Impl_->GetChangefeedDescriptions();
}

TMaybe<TTtlSettings> TTableDescription::GetTtlSettings() const {
    return Impl_->GetTtlSettings();
}

TMaybe<TString> TTableDescription::GetTiering() const {
    return Impl_->GetTiering();
}

const TString& TTableDescription::GetOwner() const {
    return Impl_->GetOwner();
}

const TVector<NScheme::TPermissions>& TTableDescription::GetPermissions() const {
    return Impl_->GetPermissions();
}

const TVector<NScheme::TPermissions>& TTableDescription::GetEffectivePermissions() const {
    return Impl_->GetEffectivePermissions();
}

const TVector<TKeyRange>& TTableDescription::GetKeyRanges() const {
    return Impl_->GetKeyRanges();
}

void TTableDescription::AddColumn(const TString& name, const Ydb::Type& type, const TString& family) {
    Impl_->AddColumn(name, type, family);
}

void TTableDescription::SetPrimaryKeyColumns(const TVector<TString>& primaryKeyColumns) {
    Impl_->SetPrimaryKeyColumns(primaryKeyColumns);
}

void TTableDescription::AddSecondaryIndex(const TString& indexName, EIndexType type, const TVector<TString>& indexColumns) {
    Impl_->AddSecondaryIndex(indexName, type, indexColumns);
}

void TTableDescription::AddSecondaryIndex(const TString& indexName, EIndexType type, const TVector<TString>& indexColumns, const TVector<TString>& dataColumns) {
    Impl_->AddSecondaryIndex(indexName, type, indexColumns, dataColumns);
}

void TTableDescription::AddSyncSecondaryIndex(const TString& indexName, const TVector<TString>& indexColumns) {
    AddSecondaryIndex(indexName, EIndexType::GlobalSync, indexColumns);
}

void TTableDescription::AddSyncSecondaryIndex(const TString& indexName, const TVector<TString>& indexColumns, const TVector<TString>& dataColumns) {
    AddSecondaryIndex(indexName, EIndexType::GlobalSync, indexColumns, dataColumns);
}

void TTableDescription::AddAsyncSecondaryIndex(const TString& indexName, const TVector<TString>& indexColumns) {
    AddSecondaryIndex(indexName, EIndexType::GlobalAsync, indexColumns);
}

void TTableDescription::AddAsyncSecondaryIndex(const TString& indexName, const TVector<TString>& indexColumns, const TVector<TString>& dataColumns) {
    AddSecondaryIndex(indexName, EIndexType::GlobalAsync, indexColumns, dataColumns);
}

void TTableDescription::AddSecondaryIndex(const TString& indexName, const TVector<TString>& indexColumns) {
    AddSyncSecondaryIndex(indexName, indexColumns);
}

void TTableDescription::AddSecondaryIndex(const TString& indexName, const TVector<TString>& indexColumns, const TVector<TString>& dataColumns) {
    AddSyncSecondaryIndex(indexName, indexColumns, dataColumns);
}

void TTableDescription::SetTtlSettings(TTtlSettings&& settings) {
    Impl_->SetTtlSettings(std::move(settings));
}

void TTableDescription::SetTtlSettings(const TTtlSettings& settings) {
    Impl_->SetTtlSettings(settings);
}

void TTableDescription::SetStorageSettings(const TStorageSettings& settings) {
    Impl_->SetStorageSettings(settings);
}

void TTableDescription::AddColumnFamily(const TColumnFamilyDescription& desc) {
    Impl_->AddColumnFamily(desc);
}

void TTableDescription::AddAttribute(const TString& key, const TString& value) {
    Impl_->AddAttribute(key, value);
}

void TTableDescription::SetAttributes(const THashMap<TString, TString>& attrs) {
    Impl_->SetAttributes(attrs);
}

void TTableDescription::SetAttributes(THashMap<TString, TString>&& attrs) {
    Impl_->SetAttributes(std::move(attrs));
}

void TTableDescription::SetCompactionPolicy(const TString& name) {
    Impl_->SetCompactionPolicy(name);
}

void TTableDescription::SetUniformPartitions(ui64 partitionsCount) {
    Impl_->SetUniformPartitions(partitionsCount);
}

void TTableDescription::SetPartitionAtKeys(const TExplicitPartitions& keys) {
    Impl_->SetPartitionAtKeys(keys);
}

void TTableDescription::SetPartitioningSettings(const TPartitioningSettings& settings) {
    Impl_->SetPartitioningSettings(settings);
}

void TTableDescription::SetKeyBloomFilter(bool enabled) {
    Impl_->SetKeyBloomFilter(enabled);
}

void TTableDescription::SetReadReplicasSettings(TReadReplicasSettings::EMode mode, ui64 readReplicasCount) {
    Impl_->SetReadReplicasSettings(mode, readReplicasCount);
}

const TVector<TPartitionStats>& TTableDescription::GetPartitionStats() const {
    return Impl_->GetPartitionStats();
}

TInstant TTableDescription::GetModificationTime() const {
    return Impl_->GetTableStats().ModificationTime;
}

TInstant TTableDescription::GetCreationTime() const {
    return Impl_->GetTableStats().CreationTime;
}

ui64 TTableDescription::GetTableSize() const {
    return Impl_->GetTableStats().Size;
}

ui64 TTableDescription::GetTableRows() const {
    return Impl_->GetTableStats().Rows;
}

ui64 TTableDescription::GetPartitionsCount() const {
    return Impl_->GetTableStats().Partitions;
}

const TStorageSettings& TTableDescription::GetStorageSettings() const {
    return Impl_->GetStorageSettings();
}

const TVector<TColumnFamilyDescription>& TTableDescription::GetColumnFamilies() const {
    return Impl_->GetColumnFamilies();
}

const THashMap<TString, TString>& TTableDescription::GetAttributes() const {
    return Impl_->GetAttributes();
}

const TPartitioningSettings& TTableDescription::GetPartitioningSettings() const {
    return Impl_->GetPartitioningSettings();
}

TMaybe<bool> TTableDescription::GetKeyBloomFilter() const {
    return Impl_->GetKeyBloomFilter();
}

TMaybe<TReadReplicasSettings> TTableDescription::GetReadReplicasSettings() const {
    return Impl_->GetReadReplicasSettings();
}

const Ydb::Table::DescribeTableResult& TTableDescription::GetProto() const {
    return Impl_->GetProto();
}

void TTableDescription::SerializeTo(Ydb::Table::CreateTableRequest& request) const {
    for (const auto& column : Impl_->GetColumns()) {
        auto& protoColumn = *request.add_columns();
        protoColumn.set_name(column.Name);
        protoColumn.mutable_type()->CopyFrom(TProtoAccessor::GetProto(column.Type));
        protoColumn.set_family(column.Family);
    }

    for (const auto& pk : Impl_->GetPrimaryKeyColumns()) {
        request.add_primary_key(pk);
    }

    for (const auto& index : Impl_->GetIndexDescriptions()) {
        index.SerializeTo(*request.add_indexes());
    }

    if (const auto& ttl = Impl_->GetTtlSettings()) {
        ttl->SerializeTo(*request.mutable_ttl_settings());
    }

    if (const auto& tiering = Impl_->GetTiering()) {
        request.set_tiering(*tiering);
    }

    if (Impl_->HasStorageSettings()) {
        request.mutable_storage_settings()->CopyFrom(Impl_->GetStorageSettings().GetProto());
    }

    for (const auto& family : Impl_->GetColumnFamilies()) {
        auto* f = request.add_column_families();
        f->CopyFrom(family.GetProto());
    }

    for (const auto& [key, value] : Impl_->GetAttributes()) {
        (*request.mutable_attributes())[key] = value;
    }

    if (Impl_->GetCompactionPolicy()) {
        request.set_compaction_policy(Impl_->GetCompactionPolicy());
    }

    if (const auto& uniformPartitions = Impl_->GetUniformPartitions()) {
        request.set_uniform_partitions(uniformPartitions.GetRef());
    }

    if (const auto& partitionAtKeys = Impl_->GetPartitionAtKeys()) {
        auto* borders = request.mutable_partition_at_keys();
        for (const auto& splitPoint : partitionAtKeys->SplitPoints_) {
            auto* border = borders->Addsplit_points();
            border->mutable_type()->CopyFrom(TProtoAccessor::GetProto(splitPoint.GetType()));
            border->mutable_value()->CopyFrom(TProtoAccessor::GetProto(splitPoint));
        }
    } else if (Impl_->GetProto().shard_key_bounds_size()) {
        request.mutable_partition_at_keys()->mutable_split_points()->CopyFrom(Impl_->GetProto().shard_key_bounds());
    }

    if (Impl_->HasPartitioningSettings()) {
        request.mutable_partitioning_settings()->CopyFrom(Impl_->GetPartitioningSettings().GetProto());
    }

    if (auto keyBloomFilter = Impl_->GetKeyBloomFilter()) {
        if (keyBloomFilter.GetRef()) {
            request.set_key_bloom_filter(Ydb::FeatureFlag::ENABLED);
        } else {
            request.set_key_bloom_filter(Ydb::FeatureFlag::DISABLED);
        }
    }

    if (const auto& settings = Impl_->GetReadReplicasSettings()) {
        switch (settings->GetMode()) {
        case TReadReplicasSettings::EMode::PerAz:
            request.mutable_read_replicas_settings()->set_per_az_read_replicas_count(settings->GetReadReplicasCount());
            break;
        case TReadReplicasSettings::EMode::AnyAz:
            request.mutable_read_replicas_settings()->set_any_az_read_replicas_count(settings->GetReadReplicasCount());
            break;
        default:
            break;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

class TStorageSettingsBuilder::TImpl {
public:
    Ydb::Table::StorageSettings Proto;
};

TStorageSettingsBuilder::TStorageSettingsBuilder()
    : Impl_(new TImpl)
{ }

TStorageSettingsBuilder::~TStorageSettingsBuilder() { }

TStorageSettingsBuilder& TStorageSettingsBuilder::SetTabletCommitLog0(const TString& media) {
    Impl_->Proto.mutable_tablet_commit_log0()->set_media(media);
    return *this;
}

TStorageSettingsBuilder& TStorageSettingsBuilder::SetTabletCommitLog1(const TString& media) {
    Impl_->Proto.mutable_tablet_commit_log1()->set_media(media);
    return *this;
}

TStorageSettingsBuilder& TStorageSettingsBuilder::SetExternal(const TString& media) {
    Impl_->Proto.mutable_external()->set_media(media);
    return *this;
}

TStorageSettingsBuilder& TStorageSettingsBuilder::SetStoreExternalBlobs(bool enabled) {
    Impl_->Proto.set_store_external_blobs(
        enabled ? Ydb::FeatureFlag::ENABLED : Ydb::FeatureFlag::DISABLED);
    return *this;
}

TStorageSettings TStorageSettingsBuilder::Build() const {
    return TStorageSettings(Impl_->Proto);
}

////////////////////////////////////////////////////////////////////////////////

class TPartitioningSettingsBuilder::TImpl {
public:
    Ydb::Table::PartitioningSettings Proto;
};

TPartitioningSettingsBuilder::TPartitioningSettingsBuilder()
    : Impl_(new TImpl)
{ }

TPartitioningSettingsBuilder::~TPartitioningSettingsBuilder() { }

TPartitioningSettingsBuilder& TPartitioningSettingsBuilder::SetPartitioningBySize(bool enabled) {
    Impl_->Proto.set_partitioning_by_size(
        enabled ? Ydb::FeatureFlag::ENABLED : Ydb::FeatureFlag::DISABLED);
    return *this;
}

TPartitioningSettingsBuilder& TPartitioningSettingsBuilder::SetPartitioningByLoad(bool enabled) {
    Impl_->Proto.set_partitioning_by_load(
        enabled ? Ydb::FeatureFlag::ENABLED : Ydb::FeatureFlag::DISABLED);
    return *this;
}

TPartitioningSettingsBuilder& TPartitioningSettingsBuilder::SetPartitionSizeMb(ui64 sizeMb) {
    Impl_->Proto.set_partition_size_mb(sizeMb);
    return *this;
}

TPartitioningSettingsBuilder& TPartitioningSettingsBuilder::SetMinPartitionsCount(ui64 count) {
    Impl_->Proto.set_min_partitions_count(count);
    return *this;
}

TPartitioningSettingsBuilder& TPartitioningSettingsBuilder::SetMaxPartitionsCount(ui64 count) {
    Impl_->Proto.set_max_partitions_count(count);
    return *this;
}

TPartitioningSettings TPartitioningSettingsBuilder::Build() const {
    return TPartitioningSettings(Impl_->Proto);
}

////////////////////////////////////////////////////////////////////////////////

class TColumnFamilyBuilder::TImpl {
public:
    Ydb::Table::ColumnFamily Proto;
};

TColumnFamilyBuilder::TColumnFamilyBuilder(const TString& name)
    : Impl_(new TImpl)
{
    Impl_->Proto.set_name(name);
}

TColumnFamilyBuilder::~TColumnFamilyBuilder() { }

TColumnFamilyBuilder& TColumnFamilyBuilder::SetData(const TString& media) {
    Impl_->Proto.mutable_data()->set_media(media);
    return *this;
}

TColumnFamilyBuilder& TColumnFamilyBuilder::SetCompression(EColumnFamilyCompression compression) {
    switch (compression) {
        case EColumnFamilyCompression::None:
            Impl_->Proto.set_compression(Ydb::Table::ColumnFamily::COMPRESSION_NONE);
            break;
        case EColumnFamilyCompression::LZ4:
            Impl_->Proto.set_compression(Ydb::Table::ColumnFamily::COMPRESSION_LZ4);
            break;
    }
    return *this;
}

TColumnFamilyDescription TColumnFamilyBuilder::Build() const {
    return TColumnFamilyDescription(Impl_->Proto);
}

////////////////////////////////////////////////////////////////////////////////

TTableBuilder& TTableBuilder::AddNullableColumn(const TString& name, const EPrimitiveType& type, const TString& family) {
    auto columnType = TTypeBuilder()
        .BeginOptional()
            .Primitive(type)
        .EndOptional()
        .Build();

    TableDescription_.AddColumn(name, TProtoAccessor::GetProto(columnType), family);
    return *this;
}

TTableBuilder& TTableBuilder::AddNullableColumn(const TString& name, const TDecimalType& type, const TString& family) {
    auto columnType = TTypeBuilder()
        .BeginOptional()
            .Decimal(type)
        .EndOptional()
        .Build();
    TableDescription_.AddColumn(name, TProtoAccessor::GetProto(columnType), family);
    return *this;
}

TTableBuilder& TTableBuilder::AddNullableColumn(const TString& name, const TPgType& type, const TString& family) {
    auto columnType = TTypeBuilder()
        .Pg(type)
        .Build();

    TableDescription_.AddColumn(name, TProtoAccessor::GetProto(columnType), family);
    return *this;
}

TTableBuilder& TTableBuilder::AddNonNullableColumn(const TString& name, const EPrimitiveType& type, const TString& family) {
    auto columnType = TTypeBuilder()
        .Primitive(type)
        .Build();

    TableDescription_.AddColumn(name, TProtoAccessor::GetProto(columnType), family);
    return *this;
}

TTableBuilder& TTableBuilder::AddNonNullableColumn(const TString& name, const TDecimalType& type, const TString& family) {
    auto columnType = TTypeBuilder()
        .Decimal(type)
        .Build();

    TableDescription_.AddColumn(name, TProtoAccessor::GetProto(columnType), family);
    return *this;
}

TTableBuilder& TTableBuilder::AddNonNullableColumn(const TString& name, const TPgType& type, const TString& family) {
    throw yexception() << "It is not allowed to create NOT NULL column with pg type";
    Y_UNUSED(name);
    Y_UNUSED(type);
    Y_UNUSED(family);
    return *this;
}

TTableBuilder& TTableBuilder::SetPrimaryKeyColumns(const TVector<TString>& primaryKeyColumns) {
    TableDescription_.SetPrimaryKeyColumns(primaryKeyColumns);
    return *this;
}

TTableBuilder& TTableBuilder::SetPrimaryKeyColumn(const TString& primaryKeyColumn) {
    TableDescription_.SetPrimaryKeyColumns(TVector<TString>{primaryKeyColumn});
    return *this;
}

TTableBuilder& TTableBuilder::AddSecondaryIndex(const TString& indexName, EIndexType type, const TVector<TString>& indexColumns, const TVector<TString>& dataColumns) {
    TableDescription_.AddSecondaryIndex(indexName, type, indexColumns, dataColumns);
    return *this;
}

TTableBuilder& TTableBuilder::AddSecondaryIndex(const TString& indexName, EIndexType type, const TVector<TString>& indexColumns) {
    TableDescription_.AddSecondaryIndex(indexName, type, indexColumns);
    return *this;
}

TTableBuilder& TTableBuilder::AddSecondaryIndex(const TString& indexName, EIndexType type, const TString& indexColumn) {
    TableDescription_.AddSecondaryIndex(indexName, type, TVector<TString>{indexColumn});
    return *this;
}

TTableBuilder& TTableBuilder::AddSyncSecondaryIndex(const TString& indexName, const TVector<TString>& indexColumns, const TVector<TString>& dataColumns) {
    return AddSecondaryIndex(indexName, EIndexType::GlobalSync, indexColumns, dataColumns);
}

TTableBuilder& TTableBuilder::AddSyncSecondaryIndex(const TString& indexName, const TVector<TString>& indexColumns) {
    return AddSecondaryIndex(indexName, EIndexType::GlobalSync, indexColumns);
}

TTableBuilder& TTableBuilder::AddSyncSecondaryIndex(const TString& indexName, const TString& indexColumn) {
    return AddSecondaryIndex(indexName, EIndexType::GlobalSync, indexColumn);
}

TTableBuilder& TTableBuilder::AddAsyncSecondaryIndex(const TString& indexName, const TVector<TString>& indexColumns, const TVector<TString>& dataColumns) {
    return AddSecondaryIndex(indexName, EIndexType::GlobalAsync, indexColumns, dataColumns);
}

TTableBuilder& TTableBuilder::AddAsyncSecondaryIndex(const TString& indexName, const TVector<TString>& indexColumns) {
    return AddSecondaryIndex(indexName, EIndexType::GlobalAsync, indexColumns);
}

TTableBuilder& TTableBuilder::AddAsyncSecondaryIndex(const TString& indexName, const TString& indexColumn) {
    return AddSecondaryIndex(indexName, EIndexType::GlobalAsync, indexColumn);
}

TTableBuilder& TTableBuilder::AddSecondaryIndex(const TString& indexName, const TVector<TString>& indexColumns, const TVector<TString>& dataColumns) {
    return AddSyncSecondaryIndex(indexName, indexColumns, dataColumns);
}

TTableBuilder& TTableBuilder::AddSecondaryIndex(const TString& indexName, const TVector<TString>& indexColumns) {
    return AddSyncSecondaryIndex(indexName, indexColumns);
}

TTableBuilder& TTableBuilder::AddSecondaryIndex(const TString& indexName, const TString& indexColumn) {
    return AddSyncSecondaryIndex(indexName, indexColumn);
}

TTableBuilder& TTableBuilder::SetTtlSettings(TTtlSettings&& settings) {
    TableDescription_.SetTtlSettings(std::move(settings));
    return *this;
}

TTableBuilder& TTableBuilder::SetTtlSettings(const TTtlSettings& settings) {
    TableDescription_.SetTtlSettings(settings);
    return *this;
}

TTableBuilder& TTableBuilder::SetTtlSettings(const TString& columnName, const TDuration& expireAfter) {
    return SetTtlSettings(TTtlSettings(columnName, expireAfter));
}

TTableBuilder& TTableBuilder::SetTtlSettings(const TString& columnName, EUnit columnUnit, const TDuration& expireAfter) {
    return SetTtlSettings(TTtlSettings(columnName, columnUnit, expireAfter));
}

TTableBuilder& TTableBuilder::SetStorageSettings(const TStorageSettings& settings) {
    TableDescription_.SetStorageSettings(settings);
    return *this;
}

TTableBuilder& TTableBuilder::AddColumnFamily(const TColumnFamilyDescription& desc) {
    TableDescription_.AddColumnFamily(std::move(desc));
    return *this;
}

TTableBuilder& TTableBuilder::AddAttribute(const TString& key, const TString& value) {
    TableDescription_.AddAttribute(key, value);
    return *this;
}

TTableBuilder& TTableBuilder::SetAttributes(const THashMap<TString, TString>& attrs) {
    TableDescription_.SetAttributes(attrs);
    return *this;
}

TTableBuilder& TTableBuilder::SetAttributes(THashMap<TString, TString>&& attrs) {
    TableDescription_.SetAttributes(std::move(attrs));
    return *this;
}

TTableBuilder& TTableBuilder::SetCompactionPolicy(const TString& name) {
    TableDescription_.SetCompactionPolicy(name);
    return *this;
}

TTableBuilder& TTableBuilder::SetUniformPartitions(ui64 partitionsCount) {
    TableDescription_.SetUniformPartitions(partitionsCount);
    return *this;
}

TTableBuilder& TTableBuilder::SetPartitionAtKeys(const TExplicitPartitions& keys) {
    TableDescription_.SetPartitionAtKeys(keys);
    return *this;
}

TTableBuilder& TTableBuilder::SetPartitioningSettings(const TPartitioningSettings& settings) {
    TableDescription_.SetPartitioningSettings(settings);
    return *this;
}

TTableBuilder& TTableBuilder::SetKeyBloomFilter(bool enabled) {
    TableDescription_.SetKeyBloomFilter(enabled);
    return *this;
}

TTableBuilder& TTableBuilder::SetReadReplicasSettings(TReadReplicasSettings::EMode mode, ui64 readReplicasCount) {
    TableDescription_.SetReadReplicasSettings(mode, readReplicasCount);
    return *this;
}

TTableDescription TTableBuilder::Build() {
    return TableDescription_;
}

////////////////////////////////////////////////////////////////////////////////

class TTablePartIterator::TReaderImpl {
public:
    using TSelf = TTablePartIterator::TReaderImpl;
    using TResponse = Ydb::Table::ReadTableResponse;
    using TStreamProcessorPtr = NGrpc::IStreamRequestReadProcessor<TResponse>::TPtr;
    using TReadCallback = NGrpc::IStreamRequestReadProcessor<TResponse>::TReadCallback;
    using TGRpcStatus = NGrpc::TGrpcStatus;
    using TBatchReadResult = std::pair<TResponse, TGRpcStatus>;

    TReaderImpl(TStreamProcessorPtr streamProcessor, const TString& endpoint)
        : StreamProcessor_(streamProcessor)
        , Finished_(false)
        , Endpoint_(endpoint)
    {}

    ~TReaderImpl() {
        StreamProcessor_->Cancel();
    }

    bool IsFinished() {
        return Finished_;
    }

    TAsyncSimpleStreamPart<TResultSet> ReadNext(std::shared_ptr<TSelf> self) {
        auto promise = NThreading::NewPromise<TSimpleStreamPart<TResultSet>>();
        // Capture self - guarantee no dtor call during the read
        auto readCb = [self, promise](TGRpcStatus&& grpcStatus) mutable {
            std::optional<TReadTableSnapshot> snapshot;
            if (self->Response_.has_snapshot()) {
                snapshot.emplace(
                    self->Response_.snapshot().plan_step(),
                    self->Response_.snapshot().tx_id());
            }
            if (!grpcStatus.Ok()) {
                self->Finished_ = true;
                promise.SetValue({TResultSet(std::move(*self->Response_.mutable_result()->mutable_result_set())),
                              TStatus(TPlainStatus(grpcStatus, self->Endpoint_)),
                              snapshot});
            } else {
                NYql::TIssues issues;
                NYql::IssuesFromMessage(self->Response_.issues(), issues);
                EStatus clientStatus = static_cast<EStatus>(self->Response_.status());
                promise.SetValue({TResultSet(std::move(*self->Response_.mutable_result()->mutable_result_set())),
                              TStatus(clientStatus, std::move(issues)),
                              snapshot});
            }
        };
        StreamProcessor_->Read(&Response_, readCb);
        return promise.GetFuture();
    }
private:
    TStreamProcessorPtr StreamProcessor_;
    TResponse Response_;
    bool Finished_;
    TString Endpoint_;
};

TTablePartIterator::TTablePartIterator(
    std::shared_ptr<TReaderImpl> impl,
    TPlainStatus&& status)
    : TStatus(std::move(status))
    , ReaderImpl_(impl)
{}

TAsyncSimpleStreamPart<TResultSet> TTablePartIterator::ReadNext() {
    if (ReaderImpl_->IsFinished())
        RaiseError("Attempt to perform read on invalid or finished stream");
    return ReaderImpl_->ReadNext(ReaderImpl_);
}

////////////////////////////////////////////////////////////////////////////////

class TScanQueryPartIterator::TReaderImpl {
public:
    using TSelf = TScanQueryPartIterator::TReaderImpl;
    using TResponse = Ydb::Table::ExecuteScanQueryPartialResponse;
    using TStreamProcessorPtr = NGrpc::IStreamRequestReadProcessor<TResponse>::TPtr;
    using TReadCallback = NGrpc::IStreamRequestReadProcessor<TResponse>::TReadCallback;
    using TGRpcStatus = NGrpc::TGrpcStatus;
    using TBatchReadResult = std::pair<TResponse, TGRpcStatus>;

    TReaderImpl(TStreamProcessorPtr streamProcessor, const TString& endpoint)
        : StreamProcessor_(streamProcessor)
        , Finished_(false)
        , Endpoint_(endpoint)
    {}

    ~TReaderImpl() {
        StreamProcessor_->Cancel();
    }

    bool IsFinished() const {
        return Finished_;
    }

    TAsyncScanQueryPart ReadNext(std::shared_ptr<TSelf> self) {
        auto promise = NThreading::NewPromise<TScanQueryPart>();
        // Capture self - guarantee no dtor call during the read
        auto readCb = [self, promise](TGRpcStatus&& grpcStatus) mutable {
            if (!grpcStatus.Ok()) {
                self->Finished_ = true;
                promise.SetValue({TStatus(TPlainStatus(grpcStatus, self->Endpoint_))});
            } else {
                NYql::TIssues issues;
                NYql::IssuesFromMessage(self->Response_.issues(), issues);
                EStatus clientStatus = static_cast<EStatus>(self->Response_.status());
                // TODO: Add headers for streaming calls.
                TPlainStatus plainStatus{clientStatus, std::move(issues), self->Endpoint_, {}};
                TStatus status{std::move(plainStatus)};
                TMaybe<TQueryStats> queryStats;

                if (self->Response_.result().has_query_stats()) {
                    queryStats = TQueryStats(self->Response_.result().query_stats());
                }

                if (self->Response_.result().has_result_set()) {
                    promise.SetValue({std::move(status),
                        TResultSet(std::move(*self->Response_.mutable_result()->mutable_result_set())), queryStats});
                } else {
                    promise.SetValue({std::move(status), queryStats});
                }
            }
        };
        StreamProcessor_->Read(&Response_, readCb);
        return promise.GetFuture();
    }
private:
    TStreamProcessorPtr StreamProcessor_;
    TResponse Response_;
    bool Finished_;
    TString Endpoint_;
};

TScanQueryPartIterator::TScanQueryPartIterator(
    std::shared_ptr<TReaderImpl> impl,
    TPlainStatus&& status)
    : TStatus(std::move(status))
    , ReaderImpl_(impl)
{}

TAsyncScanQueryPart TScanQueryPartIterator::ReadNext() {
    if (ReaderImpl_->IsFinished())
        RaiseError("Attempt to perform read on invalid or finished stream");
    return ReaderImpl_->ReadNext(ReaderImpl_);
}

////////////////////////////////////////////////////////////////////////////////

class TSessionPoolImpl {
    typedef TAsyncCreateSessionResult
        (*TAwareSessonProvider)
            (std::shared_ptr<TTableClient::TImpl> client, const TCreateSessionSettings& settings);
public:
    using TKeepAliveCmd = std::function<void(TSession session)>;
    using TDeletePredicate = std::function<bool(TSession::TImpl* session, TTableClient::TImpl* client, size_t sessionsCount)>;
    TSessionPoolImpl(ui32 maxActiveSessions);
    // TAwareSessonProvider:
    // function is called if session pool is empty,
    // this is used for additional total session count limitation
    TAsyncCreateSessionResult GetSession(
        std::shared_ptr<TTableClient::TImpl> client,
        const TCreateSessionSettings& settings);
    // Returns true if session was extracted from session pool and dropped via smart deleter
    // Returns false if session for given endpoint was not found
    // NOTE: O(n) under session pool lock, should not be used often
    bool DropSessionOnEndpoint(std::shared_ptr<TTableClient::TImpl> client, ui64 nodeId);
    // Returns true if session returned to pool successfully
    bool ReturnSession(TSession::TImpl* impl, bool active);
    TPeriodicCb CreatePeriodicTask(std::weak_ptr<TTableClient::TImpl> weakClient, TKeepAliveCmd&& cmd, TDeletePredicate&& predicate);
    i64 GetActiveSessions() const;
    i64 GetActiveSessionsLimit() const;
    i64 GetCurrentPoolSize() const;
    void DecrementActiveCounter();

    void Drain(std::function<bool(std::unique_ptr<TSession::TImpl>&&)> cb, bool close);
    void SetStatCollector(NSdkStats::TStatCollector::TSessionPoolStatCollector collector);

    static void CreateFakeSession(NThreading::TPromise<TCreateSessionResult>& promise,
        std::shared_ptr<TTableClient::TImpl> client);
private:
    void UpdateStats();

    mutable std::mutex Mtx_;
    TMultiMap<TInstant, std::unique_ptr<TSession::TImpl>> Sessions_;
    bool Closed_;
    i64 ActiveSessions_;
    const ui32 MaxActiveSessions_;
    NSdkStats::TSessionCounter ActiveSessionsCounter_;
    NSdkStats::TSessionCounter InPoolSessionsCounter_;
    NSdkStats::TAtomicCounter<::NMonitoring::TRate> FakeSessionsCounter_;
};

static TDuration RandomizeThreshold(TDuration duration) {
    TDuration::TValue value = duration.GetValue();
    if (KEEP_ALIVE_RANDOM_FRACTION) {
        const i64 randomLimit = value / KEEP_ALIVE_RANDOM_FRACTION;
        if (randomLimit < 2)
            return duration;
        value += static_cast<i64>(RandomNumber<ui64>(randomLimit));
    }
    return TDuration::FromValue(value);
}

static TDuration GetMinTimeToTouch(const TSessionPoolSettings& settings) {
    return Min(settings.CloseIdleThreshold_, settings.KeepAliveIdleThreshold_);
}

static TDuration GetMaxTimeToTouch(const TSessionPoolSettings& settings) {
    return Max(settings.CloseIdleThreshold_, settings.KeepAliveIdleThreshold_);
}

static TStatus GetStatus(const TOperation& operation) {
    return operation.Status();
}

static TStatus GetStatus(const TStatus& status) {
    return status;
}

static bool IsSessionCloseRequested(const TStatus& status) {
    const auto& meta = status.GetResponseMetadata();
    auto hints = meta.equal_range(NYdb::YDB_SERVER_HINTS);
    for(auto it = hints.first; it != hints.second; ++it) {
        if (it->second == NYdb::YDB_SESSION_CLOSE) {
            return true;
        }
    }

    return false;
}

template<typename TResponse>
NThreading::TFuture<TResponse> InjectSessionStatusInterception(
        std::shared_ptr<TSession::TImpl>& impl, NThreading::TFuture<TResponse> asyncResponse,
        bool updateTimeout,
        TDuration timeout,
        std::function<void(const TResponse&, TSession::TImpl&)> cb = {})
{
    auto promise = NThreading::NewPromise<TResponse>();
    asyncResponse.Subscribe([impl, promise, cb, updateTimeout, timeout](NThreading::TFuture<TResponse> future) mutable {
        Y_VERIFY(future.HasValue());

        // TResponse can hold refcounted user provided data (TSession for example)
        // and we do not want to have copy of it (for example it can cause delay dtor call)
        // so using move semantic here is mandatory.
        // Also we must reset captured shared pointer to session impl
        TResponse value = std::move(future.ExtractValue());

        const TStatus& status = GetStatus(value);
        // Exclude CLIENT_RESOURCE_EXHAUSTED from transport errors which can cause to session disconnect
        // since we have guarantee this request wasn't been started to execute.

        if (status.IsTransportError() && status.GetStatus() != EStatus::CLIENT_RESOURCE_EXHAUSTED) {
            impl->MarkBroken();
        } else if (status.GetStatus() == EStatus::SESSION_BUSY) {
            impl->MarkBroken();
        } else if (status.GetStatus() == EStatus::BAD_SESSION) {
            impl->MarkBroken();
        } else if (IsSessionCloseRequested(status)) {
            impl->MarkAsClosing();
        } else {
            // NOTE: About GetState and lock
            // Simultanious call multiple requests on the same session make no sence, due to server limitation.
            // But user can perform this call, right now we do not protect session from this, it may cause
            // raise on session state if respoise is not success.
            // It should not be a problem - in case of this race we close session
            // or put it in to settler.
            if (updateTimeout && status.GetStatus() != EStatus::CLIENT_RESOURCE_EXHAUSTED) {
                impl->ScheduleTimeToTouch(RandomizeThreshold(timeout), impl->GetState() == TSession::TImpl::EState::S_ACTIVE);
            }
        }
        if (cb) {
            cb(value, *impl);
        }
        impl.reset();
        promise.SetValue(std::move(value));
    });
    return promise.GetFuture();
}

static ui32 CalcBackoffTime(const TBackoffSettings& settings, ui32 retryNumber) {
    ui32 backoffSlots = 1 << std::min(retryNumber, settings.Ceiling_);
    TDuration maxDuration = settings.SlotDuration_ * backoffSlots;

    double uncertaintyRatio = std::max(std::min(settings.UncertainRatio_, 1.0), 0.0);
    double uncertaintyMultiplier = RandomNumber<double>() * uncertaintyRatio - uncertaintyRatio + 1.0;

    double durationMs = round(maxDuration.MilliSeconds() * uncertaintyMultiplier);

    return std::max(std::min(durationMs, (double)MAX_BACKOFF_DURATION_MS), 0.0);
}

////////////////////////////////////////////////////////////////////////////////

class TTableClient::TImpl: public TClientImplCommon<TTableClient::TImpl>, public IMigratorClient {
public:
    using TReadTableStreamProcessorPtr = TTablePartIterator::TReaderImpl::TStreamProcessorPtr;
    using TScanQueryProcessorPtr = TScanQueryPartIterator::TReaderImpl::TStreamProcessorPtr;

    TImpl(std::shared_ptr<TGRpcConnectionsImpl>&& connections, const TClientSettings& settings)
        : TClientImplCommon(std::move(connections), settings)
        , Settings_(settings)
        , SessionPool_(Settings_.SessionPoolSettings_.MaxActiveSessions_)
    {
        if (!DbDriverState_->StatCollector.IsCollecting()) {
            return;
        }

        SetStatCollector(DbDriverState_->StatCollector.GetClientStatCollector());
        SessionPool_.SetStatCollector(DbDriverState_->StatCollector.GetSessionPoolStatCollector());
    }

    ~TImpl() {
        if (Connections_->GetDrainOnDtors()) {
            Drain().Wait();
        }
    }

    bool LinkObjToEndpoint(const TEndpointKey& endpoint, TEndpointObj* obj, const void* tag) {
        return DbDriverState_->EndpointPool.LinkObjToEndpoint(endpoint, obj, tag);
    }

    void InitStopper() {
        std::weak_ptr<TTableClient::TImpl> weak = shared_from_this();
        auto cb = [weak]() mutable {
            auto strong = weak.lock();
            if (!strong) {
                auto promise = NThreading::NewPromise<void>();
                promise.SetException("no more client");
                return promise.GetFuture();
            }
            return strong->Drain();
        };

        DbDriverState_->AddCb(std::move(cb), TDbDriverState::ENotifyType::STOP);
    }

    NThreading::TFuture<void> Drain() {
        TVector<std::unique_ptr<TSession::TImpl>> sessions;
        // No realocations under lock
        sessions.reserve(Settings_.SessionPoolSettings_.MaxActiveSessions_);
        auto drainer = [&sessions](std::unique_ptr<TSession::TImpl>&& impl) mutable {
            sessions.push_back(std::move(impl));
            return true;
        };
        SessionPool_.Drain(drainer, true);
        TVector<TAsyncStatus> closeResults;
        for (auto& s : sessions) {
            if (s->GetId()) {
                closeResults.push_back(CloseInternal(s.get()));
            }
        }
        sessions.clear();
        return NThreading::WaitExceptionOrAll(closeResults);
    }

    NThreading::TFuture<void> Stop() {
        return Drain();
    }

    void ScheduleTask(const std::function<void()>& fn, TDuration timeout) {
        std::weak_ptr<TTableClient::TImpl> weak = shared_from_this();
        auto cbGuard = [weak, fn]() {
            auto strongClient = weak.lock();
            if (strongClient) {
                fn();
            }
        };
        Connections_->ScheduleOneTimeTask(std::move(cbGuard), timeout);
    }

    void ScheduleTaskUnsafe(std::function<void()>&& fn, TDuration timeout) {
        Connections_->ScheduleOneTimeTask(std::move(fn), timeout);
    }

    void AsyncBackoff(const TBackoffSettings& settings, ui32 retryNumber, const std::function<void()>& fn) {
        auto durationMs = CalcBackoffTime(settings, retryNumber);
        ScheduleTask(fn, TDuration::MilliSeconds(durationMs));
    }

    void StartPeriodicSessionPoolTask() {

        auto deletePredicate = [](TSession::TImpl* session, TTableClient::TImpl* client, size_t sessionsCount) {

            const auto sessionPoolSettings = client->Settings_.SessionPoolSettings_;
            const auto spentTime = session->GetTimeToTouchFast() - session->GetTimeInPastFast();

            if (spentTime >= sessionPoolSettings.CloseIdleThreshold_) {
                if (sessionsCount > sessionPoolSettings.MinPoolSize_) {
                    return true;
                }
            }

            return false;
        };

        auto keepAliveCmd = [](TSession session) {
            Y_VERIFY(session.GetId());

            const auto sessionPoolSettings = session.Client_->Settings_.SessionPoolSettings_;
            const auto spentTime = session.SessionImpl_->GetTimeToTouchFast() - session.SessionImpl_->GetTimeInPastFast();

            const auto maxTimeToTouch = GetMaxTimeToTouch(session.Client_->Settings_.SessionPoolSettings_);
            const auto minTimeToTouch = GetMinTimeToTouch(session.Client_->Settings_.SessionPoolSettings_);

            auto calcTimeToNextTouch = [maxTimeToTouch, minTimeToTouch] (const TDuration spent) {
                auto timeToNextTouch = minTimeToTouch;
                if (maxTimeToTouch > spent) {
                    auto t = maxTimeToTouch - spent;
                    timeToNextTouch = Min(t, minTimeToTouch);
                }
                return timeToNextTouch;
            };

            if (spentTime >= sessionPoolSettings.KeepAliveIdleThreshold_) {

                // Handle of session status will be done inside InjectSessionStatusInterception routine.
                // We just need to reschedule time to next call because InjectSessionStatusInterception doesn't
                // update timeInPast for calls from internal keep alive routine
                session.KeepAlive(KeepAliveSettings)
                    .Subscribe([spentTime, session, maxTimeToTouch, calcTimeToNextTouch](TAsyncKeepAliveResult asyncResult) {
                        if (!asyncResult.GetValue().IsSuccess())
                            return;

                        if (spentTime >= maxTimeToTouch) {
                            auto timeToNextTouch = calcTimeToNextTouch(spentTime);
                            session.SessionImpl_->ScheduleTimeToTouchFast(timeToNextTouch, true);
                        }
                    });
                return;
            }

            auto timeToNextTouch = calcTimeToNextTouch(spentTime);
            session.SessionImpl_->ScheduleTimeToTouchFast(
                RandomizeThreshold(timeToNextTouch),
                spentTime >= maxTimeToTouch
            );
        };

        std::weak_ptr<TTableClient::TImpl> weak = shared_from_this();
        Connections_->AddPeriodicTask(
            SessionPool_.CreatePeriodicTask(
                weak,
                std::move(keepAliveCmd),
                std::move(deletePredicate)
            ), PERIODIC_ACTION_INTERVAL);
    }

    static ui64 ScanForeignLocations(std::shared_ptr<TTableClient::TImpl> client) {
        size_t max = 0;
        ui64 result = 0;

        auto cb = [&result, &max](ui64 nodeId, const IObjRegistryHandle& handle) {
            const auto sz = handle.Size();
            if (sz > max) {
                result = nodeId;
                max = sz;
            }
        };

        client->DbDriverState_->ForEachForeignEndpoint(cb, client.get());

        return result;
    }

    static std::pair<ui64, size_t> ScanLocation(std::shared_ptr<TTableClient::TImpl> client,
        std::unordered_map<ui64, size_t>& sessions, bool allNodes)
    {
        std::pair<ui64, size_t> result = {0, 0};

        auto cb = [&result, &sessions](ui64 nodeId, const IObjRegistryHandle& handle) {
            const auto sz = handle.Size();
            sessions.insert({nodeId, sz});
            if (sz > result.second) {
                result.first = nodeId;
                result.second = sz;
            }
        };

        if (allNodes) {
            client->DbDriverState_->ForEachEndpoint(cb, client.get());
        } else {
            client->DbDriverState_->ForEachLocalEndpoint(cb, client.get());
        }

        return result;
    }

    static NMath::TStats CalcCV(const std::unordered_map<ui64, size_t>& in) {
        TVector<size_t> t;
        t.reserve(in.size());
        std::transform(in.begin(), in.end(), std::back_inserter(t), [](const std::pair<ui64, size_t>& pair) {
            return pair.second;
        });
        return NMath::CalcCV(t);
    }

    void StartPeriodicHostScanTask() {
        std::weak_ptr<TTableClient::TImpl> weak = shared_from_this();

        // The future in completed when we have finished current migrate task
        // and ready to accept new one
        std::pair<ui64, size_t> winner = {0, 0};

        auto periodicCb = [weak, winner](NYql::TIssues&&, EStatus status) mutable -> bool {

            if (status != EStatus::SUCCESS) {
                return false;
            }

            auto strongClient = weak.lock();
            if (!strongClient) {
                return false;
            } else {
                TRequestMigrator& migrator = strongClient->RequestMigrator_;

                const auto balancingPolicy = strongClient->DbDriverState_->GetBalancingPolicy();

                // Try to find any host at foreign locations if prefer local dc
                const ui64 foreignHost = (balancingPolicy == EBalancingPolicy::UsePreferableLocation) ?
                    ScanForeignLocations(strongClient) : 0;

                std::unordered_map<ui64, size_t> hostMap;

                winner = ScanLocation(strongClient, hostMap,
                   balancingPolicy == EBalancingPolicy::UseAllNodes);

                bool forceMigrate = false;

                // There is host in foreign locations
                if (foreignHost) {
                    // But no hosts at local
                    if (hostMap.empty()) {
                        Y_VERIFY(!winner.second);
                        // Scan whole cluster - we have no local dc
                        winner = ScanLocation(strongClient, hostMap, true);
                    } else {
                        // We have local and foreign hosts, so force migration to local one
                        forceMigrate = true;
                        // Just replace source
                        winner.first = foreignHost;
                        winner.second++;
                    }
                }

                const auto minCv = strongClient->Settings_.MinSessionCV_;

                const auto stats = CalcCV(hostMap);

                strongClient->DbDriverState_->StatCollector.SetSessionCV(stats.Cv);

                // Just scan to update monitoring counter ^^
                // Balancing feature is disabled.
                if (!minCv)
                    return true;

                if (hostMap.size() < 2)
                    return true;

                // Migrate last session only if move from foreign to local
                if (!forceMigrate && winner.second < 2)
                    return true;

                if (stats.Cv > minCv || forceMigrate) {
                    migrator.SetHost(winner.first);
                } else {
                    migrator.SetHost(0);
                }
                return true;
            }
        };

        Connections_->AddPeriodicTask(std::move(periodicCb), HOSTSCAN_PERIODIC_ACTION_INTERVAL);
    }

    TAsyncCreateSessionResult GetSession(const TCreateSessionSettings& settings) {
        return SessionPool_.GetSession(shared_from_this(), settings);
    }

    i64 GetActiveSessionCount() const {
        return SessionPool_.GetActiveSessions();
    }

    i64 GetActiveSessionsLimit() const {
        return SessionPool_.GetActiveSessionsLimit();
    }

    i64 GetCurrentPoolSize() const {
        return SessionPool_.GetCurrentPoolSize();
    }

    TAsyncCreateSessionResult CreateSession(const TCreateSessionSettings& settings, bool standalone,
        TString preferedLocation = TString())
    {
        auto request = MakeOperationRequest<Ydb::Table::CreateSessionRequest>(settings);

        auto createSessionPromise = NewPromise<TCreateSessionResult>();
        auto self = shared_from_this();
        auto rpcSettings = TRpcRequestSettings::Make(settings);
        rpcSettings.Header.push_back({NYdb::YDB_CLIENT_CAPABILITIES, NYdb::YDB_CLIENT_CAPABILITY_SESSION_BALANCER});

        auto createSessionExtractor = [createSessionPromise, self, standalone]
            (google::protobuf::Any* any, TPlainStatus status) mutable {
                Ydb::Table::CreateSessionResult result;
                if (any) {
                    any->UnpackTo(&result);
                }
                auto session = TSession(self, result.session_id(), status.Endpoint);
                if (status.Ok()) {
                    if (standalone) {
                        session.SessionImpl_->MarkStandalone();
                    } else {
                        session.SessionImpl_->MarkActive();
                    }
                    self->DbDriverState_->StatCollector.IncSessionsOnHost(status.Endpoint);
                } else {
                    // We do not use SessionStatusInterception for CreateSession request
                    session.SessionImpl_->MarkBroken();
                }
                TCreateSessionResult val(TStatus(std::move(status)), std::move(session));
                createSessionPromise.SetValue(std::move(val));
            };

        Connections_->RunDeferred<Ydb::Table::V1::TableService, Ydb::Table::CreateSessionRequest, Ydb::Table::CreateSessionResponse>(
            std::move(request),
            createSessionExtractor,
            &Ydb::Table::V1::TableService::Stub::AsyncCreateSession,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            rpcSettings,
            TEndpointKey(preferedLocation, 0));

        std::weak_ptr<TDbDriverState> state = DbDriverState_;

        return createSessionPromise.GetFuture();
    }

    TAsyncKeepAliveResult KeepAlive(const TSession::TImpl* session, const TKeepAliveSettings& settings) {
        auto request = MakeOperationRequest<Ydb::Table::KeepAliveRequest>(settings);
        request.set_session_id(session->GetId());

        auto keepAliveResultPromise = NewPromise<TKeepAliveResult>();
        auto self = shared_from_this();

        auto keepAliveExtractor = [keepAliveResultPromise, self]
            (google::protobuf::Any* any, TPlainStatus status) mutable {
                Ydb::Table::KeepAliveResult result;
                ESessionStatus sessionStatus = ESessionStatus::Unspecified;
                if (any) {
                    any->UnpackTo(&result);

                    switch (result.session_status()) {
                        case Ydb::Table::KeepAliveResult_SessionStatus_SESSION_STATUS_READY:
                            sessionStatus = ESessionStatus::Ready;
                        break;
                        case Ydb::Table::KeepAliveResult_SessionStatus_SESSION_STATUS_BUSY:
                            sessionStatus = ESessionStatus::Busy;
                        break;
                        default:
                            sessionStatus = ESessionStatus::Unspecified;
                    }
                }
                TKeepAliveResult val(TStatus(std::move(status)), sessionStatus);
                keepAliveResultPromise.SetValue(std::move(val));
            };

        Connections_->RunDeferred<Ydb::Table::V1::TableService, Ydb::Table::KeepAliveRequest, Ydb::Table::KeepAliveResponse>(
            std::move(request),
            keepAliveExtractor,
            &Ydb::Table::V1::TableService::Stub::AsyncKeepAlive,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings),
            session->GetEndpointKey());

        return keepAliveResultPromise.GetFuture();
    }

    TFuture<TStatus> CreateTable(Ydb::Table::CreateTableRequest&& request, const TCreateTableSettings& settings)
    {
        return RunSimple<Ydb::Table::V1::TableService, Ydb::Table::CreateTableRequest,Ydb::Table::CreateTableResponse>(
            std::move(request),
            &Ydb::Table::V1::TableService::Stub::AsyncCreateTable,
            TRpcRequestSettings::Make(settings));
    }

    TFuture<TStatus> AlterTable(Ydb::Table::AlterTableRequest&& request, const TAlterTableSettings& settings)
    {
        return RunSimple<Ydb::Table::V1::TableService, Ydb::Table::AlterTableRequest, Ydb::Table::AlterTableResponse>(
            std::move(request),
            &Ydb::Table::V1::TableService::Stub::AsyncAlterTable,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncOperation AlterTableLong(Ydb::Table::AlterTableRequest&& request, const TAlterTableSettings& settings)
    {
        using Ydb::Table::V1::TableService;
        using Ydb::Table::AlterTableRequest;
        using Ydb::Table::AlterTableResponse;
        return RunOperation<TableService, AlterTableRequest, AlterTableResponse, TOperation>(
            std::move(request),
            &Ydb::Table::V1::TableService::Stub::AsyncAlterTable,
            TRpcRequestSettings::Make(settings));
    }

    TFuture<TStatus> CopyTable(const TString& sessionId, const TString& src, const TString& dst,
        const TCopyTableSettings& settings)
    {
        auto request = MakeOperationRequest<Ydb::Table::CopyTableRequest>(settings);
        request.set_session_id(sessionId);
        request.set_source_path(src);
        request.set_destination_path(dst);

        return RunSimple<Ydb::Table::V1::TableService, Ydb::Table::CopyTableRequest, Ydb::Table::CopyTableResponse>(
            std::move(request),
            &Ydb::Table::V1::TableService::Stub::AsyncCopyTable,
            TRpcRequestSettings::Make(settings));
    }

    TFuture<TStatus> CopyTables(Ydb::Table::CopyTablesRequest&& request, const TCopyTablesSettings& settings)
    {
        return RunSimple<Ydb::Table::V1::TableService, Ydb::Table::CopyTablesRequest, Ydb::Table::CopyTablesResponse>(
            std::move(request),
            &Ydb::Table::V1::TableService::Stub::AsyncCopyTables,
            TRpcRequestSettings::Make(settings));
    }

    TFuture<TStatus> RenameTables(Ydb::Table::RenameTablesRequest&& request, const TRenameTablesSettings& settings)
    {
        return RunSimple<Ydb::Table::V1::TableService, Ydb::Table::RenameTablesRequest, Ydb::Table::RenameTablesResponse>(
            std::move(request),
            &Ydb::Table::V1::TableService::Stub::AsyncRenameTables,
            TRpcRequestSettings::Make(settings));
    }

    TFuture<TStatus> DropTable(const TString& sessionId, const TString& path, const TDropTableSettings& settings) {
        auto request = MakeOperationRequest<Ydb::Table::DropTableRequest>(settings);
        request.set_session_id(sessionId);
        request.set_path(path);

        return RunSimple<Ydb::Table::V1::TableService, Ydb::Table::DropTableRequest, Ydb::Table::DropTableResponse>(
            std::move(request),
            &Ydb::Table::V1::TableService::Stub::AsyncDropTable,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncDescribeTableResult DescribeTable(const TString& sessionId, const TString& path, const TDescribeTableSettings& settings) {
        auto request = MakeOperationRequest<Ydb::Table::DescribeTableRequest>(settings);
        request.set_session_id(sessionId);
        request.set_path(path);
        if (settings.WithKeyShardBoundary_) {
            request.set_include_shard_key_bounds(true);
        }

        if (settings.WithTableStatistics_) {
            request.set_include_table_stats(true);
        }

        if (settings.WithPartitionStatistics_) {
            request.set_include_partition_stats(true);
        }

        auto promise = NewPromise<TDescribeTableResult>();

        auto extractor = [promise, settings]
            (google::protobuf::Any* any, TPlainStatus status) mutable {
                Ydb::Table::DescribeTableResult result;
                if (any) {
                    any->UnpackTo(&result);
                }
                TDescribeTableResult describeTableResult(TStatus(std::move(status)),
                    std::move(result), settings);
                promise.SetValue(std::move(describeTableResult));
            };

        Connections_->RunDeferred<Ydb::Table::V1::TableService, Ydb::Table::DescribeTableRequest, Ydb::Table::DescribeTableResponse>(
            std::move(request),
            extractor,
            &Ydb::Table::V1::TableService::Stub::AsyncDescribeTable,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }

    template<typename TParamsType>
    TAsyncDataQueryResult ExecuteDataQuery(TSession& session, const TString& query, const TTxControl& txControl,
        TParamsType params, const TExecDataQuerySettings& settings)
    {
        auto maybeQuery = session.SessionImpl_->GetQueryFromCache(query, Settings_.AllowRequestMigration_);
        if (maybeQuery) {
            TDataQuery dataQuery(session, query, maybeQuery->QueryId, maybeQuery->ParameterTypes);
            return ExecuteDataQuery(session, dataQuery, txControl, params, settings, true);
        }

        CacheMissCounter.Inc();

        return InjectSessionStatusInterception(session.SessionImpl_,
            ExecuteDataQueryInternal(session, query, txControl, params, settings, false),
            true, GetMinTimeToTouch(Settings_.SessionPoolSettings_));
    }

    template<typename TParamsType>
    TAsyncDataQueryResult ExecuteDataQuery(TSession& session, const TDataQuery& dataQuery, const TTxControl& txControl,
        TParamsType params, const TExecDataQuerySettings& settings,
        bool fromCache)
    {
        TString queryKey = dataQuery.Impl_->GetTextHash();
        auto cb = [queryKey](const TDataQueryResult& result, TSession::TImpl& session) {
            if (result.GetStatus() == EStatus::NOT_FOUND) {
                session.InvalidateQueryInCache(queryKey);
            }
        };

        return InjectSessionStatusInterception<TDataQueryResult>(
            session.SessionImpl_,
            session.Client_->ExecuteDataQueryInternal(session, dataQuery, txControl, params, settings, fromCache),
            true,
            GetMinTimeToTouch(session.Client_->Settings_.SessionPoolSettings_),
            cb);
    }

    TAsyncPrepareQueryResult PrepareDataQuery(const TSession& session, const TString& query,
        const TPrepareDataQuerySettings& settings)
    {
        auto request = MakeOperationRequest<Ydb::Table::PrepareDataQueryRequest>(settings);
        request.set_session_id(session.GetId());
        request.set_yql_text(query);

        auto promise = NewPromise<TPrepareQueryResult>();

        //See ExecuteDataQueryInternal for explanation
        auto sessionPtr = new TSession(session);
        auto extractor = [promise, sessionPtr, query]
            (google::protobuf::Any* any, TPlainStatus status) mutable {
                TDataQuery dataQuery(*sessionPtr, query, "");

                if (any) {
                    Ydb::Table::PrepareQueryResult result;
                    any->UnpackTo(&result);

                    if (status.Ok()) {
                        dataQuery = TDataQuery(*sessionPtr, query, result.query_id(), result.parameters_types());
                        sessionPtr->SessionImpl_->AddQueryToCache(dataQuery);
                    }
                }

                TPrepareQueryResult prepareQueryResult(TStatus(std::move(status)),
                    dataQuery, false);
                delete sessionPtr;
                promise.SetValue(std::move(prepareQueryResult));
            };

        CollectQuerySize(query, QuerySizeHistogram);

        Connections_->RunDeferred<Ydb::Table::V1::TableService, Ydb::Table::PrepareDataQueryRequest, Ydb::Table::PrepareDataQueryResponse>(
            std::move(request),
            extractor,
            &Ydb::Table::V1::TableService::Stub::AsyncPrepareDataQuery,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings),
            session.SessionImpl_->GetEndpointKey());

        return promise.GetFuture();
    }

    TAsyncStatus ExecuteSchemeQuery(const TString& sessionId, const TString& query,
        const TExecSchemeQuerySettings& settings)
    {
        auto request = MakeOperationRequest<Ydb::Table::ExecuteSchemeQueryRequest>(settings);
        request.set_session_id(sessionId);
        request.set_yql_text(query);

        return RunSimple<Ydb::Table::V1::TableService, Ydb::Table::ExecuteSchemeQueryRequest, Ydb::Table::ExecuteSchemeQueryResponse>(
            std::move(request),
            &Ydb::Table::V1::TableService::Stub::AsyncExecuteSchemeQuery,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncBeginTransactionResult BeginTransaction(const TSession& session, const TTxSettings& txSettings,
        const TBeginTxSettings& settings)
    {
        auto request = MakeOperationRequest<Ydb::Table::BeginTransactionRequest>(settings);
        request.set_session_id(session.GetId());
        SetTxSettings(txSettings, request.mutable_tx_settings());

        auto promise = NewPromise<TBeginTransactionResult>();

        auto extractor = [promise, session]
            (google::protobuf::Any* any, TPlainStatus status) mutable {
                TString txId;
                if (any) {
                    Ydb::Table::BeginTransactionResult result;
                    any->UnpackTo(&result);
                    txId = result.tx_meta().id();
                }

                TBeginTransactionResult beginTxResult(TStatus(std::move(status)),
                    TTransaction(session, txId));
                promise.SetValue(std::move(beginTxResult));
            };

        Connections_->RunDeferred<Ydb::Table::V1::TableService, Ydb::Table::BeginTransactionRequest, Ydb::Table::BeginTransactionResponse>(
            std::move(request),
            extractor,
            &Ydb::Table::V1::TableService::Stub::AsyncBeginTransaction,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings),
            session.SessionImpl_->GetEndpointKey());

        return promise.GetFuture();
    }

    TAsyncCommitTransactionResult CommitTransaction(const TSession& session, const TTransaction& tx,
        const TCommitTxSettings& settings)
    {
        auto request = MakeOperationRequest<Ydb::Table::CommitTransactionRequest>(settings);
        request.set_session_id(session.GetId());
        request.set_tx_id(tx.GetId());
        request.set_collect_stats(GetStatsCollectionMode(settings.CollectQueryStats_));

        auto promise = NewPromise<TCommitTransactionResult>();

        auto extractor = [promise]
            (google::protobuf::Any* any, TPlainStatus status) mutable {
                TMaybe<TQueryStats> queryStats;
                if (any) {
                    Ydb::Table::CommitTransactionResult result;
                    any->UnpackTo(&result);

                    if (result.has_query_stats()) {
                        queryStats = TQueryStats(result.query_stats());
                    }
                }

                TCommitTransactionResult commitTxResult(TStatus(std::move(status)), queryStats);
                promise.SetValue(std::move(commitTxResult));
            };

        Connections_->RunDeferred<Ydb::Table::V1::TableService, Ydb::Table::CommitTransactionRequest, Ydb::Table::CommitTransactionResponse>(
            std::move(request),
            extractor,
            &Ydb::Table::V1::TableService::Stub::AsyncCommitTransaction,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings),
            session.SessionImpl_->GetEndpointKey());

        return promise.GetFuture();
    }

    TAsyncStatus RollbackTransaction(const TSession& session, const TTransaction& tx,
        const TRollbackTxSettings& settings)
    {
        auto request = MakeOperationRequest<Ydb::Table::RollbackTransactionRequest>(settings);
        request.set_session_id(session.GetId());
        request.set_tx_id(tx.GetId());

        return RunSimple<Ydb::Table::V1::TableService, Ydb::Table::RollbackTransactionRequest, Ydb::Table::RollbackTransactionResponse>(
            std::move(request),
            &Ydb::Table::V1::TableService::Stub::AsyncRollbackTransaction,
            TRpcRequestSettings::Make(settings),
            session.SessionImpl_->GetEndpointKey());
    }

    TAsyncExplainDataQueryResult ExplainDataQuery(const TSession& session, const TString& query,
        const TExplainDataQuerySettings& settings)
    {
        auto request = MakeOperationRequest<Ydb::Table::ExplainDataQueryRequest>(settings);
        request.set_session_id(session.GetId());
        request.set_yql_text(query);

        auto promise = NewPromise<TExplainQueryResult>();

        auto extractor = [promise]
            (google::protobuf::Any* any, TPlainStatus status) mutable {
                TString ast;
                TString plan;
                if (any) {
                    Ydb::Table::ExplainQueryResult result;
                    any->UnpackTo(&result);
                    ast = result.query_ast();
                    plan = result.query_plan();
                }
                TExplainQueryResult val(TStatus(std::move(status)),
                    std::move(plan), std::move(ast));
                promise.SetValue(std::move(val));
            };

        Connections_->RunDeferred<Ydb::Table::V1::TableService, Ydb::Table::ExplainDataQueryRequest, Ydb::Table::ExplainDataQueryResponse>(
            std::move(request),
            extractor,
            &Ydb::Table::V1::TableService::Stub::AsyncExplainDataQuery,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings),
            session.SessionImpl_->GetEndpointKey());

        return promise.GetFuture();
    }

    static void SetTypedValue(Ydb::TypedValue* protoValue, const TValue& value) {
        protoValue->mutable_type()->CopyFrom(TProtoAccessor::GetProto(value.GetType()));
        protoValue->mutable_value()->CopyFrom(TProtoAccessor::GetProto(value));
    }

    NThreading::TFuture<std::pair<TPlainStatus, TReadTableStreamProcessorPtr>> ReadTable(
        const TString& sessionId,
        const TString& path,
        const TReadTableSettings& settings)
    {
        auto request = MakeRequest<Ydb::Table::ReadTableRequest>();
        request.set_session_id(sessionId);
        request.set_path(path);
        request.set_ordered(settings.Ordered_);
        if (settings.RowLimit_) {
            request.set_row_limit(settings.RowLimit_.GetRef());
        }
        for (const auto& col : settings.Columns_) {
            request.add_columns(col);
        }
        if (settings.UseSnapshot_) {
            request.set_use_snapshot(
                settings.UseSnapshot_.GetRef()
                ? Ydb::FeatureFlag::ENABLED
                : Ydb::FeatureFlag::DISABLED);
        }

        if (settings.From_) {
            const auto& from = settings.From_.GetRef();
            if (from.IsInclusive()) {
                SetTypedValue(request.mutable_key_range()->mutable_greater_or_equal(), from.GetValue());
            } else {
                SetTypedValue(request.mutable_key_range()->mutable_greater(), from.GetValue());
            }
        }

        if (settings.To_) {
            const auto& to = settings.To_.GetRef();
            if (to.IsInclusive()) {
                SetTypedValue(request.mutable_key_range()->mutable_less_or_equal(), to.GetValue());
            } else {
                SetTypedValue(request.mutable_key_range()->mutable_less(), to.GetValue());
            }
        }

        auto promise = NewPromise<std::pair<TPlainStatus, TReadTableStreamProcessorPtr>>();

        Connections_->StartReadStream<Ydb::Table::V1::TableService, Ydb::Table::ReadTableRequest, Ydb::Table::ReadTableResponse>(
            std::move(request),
            [promise] (TPlainStatus status, TReadTableStreamProcessorPtr processor) mutable {
                promise.SetValue(std::make_pair(status, processor));
            },
            &Ydb::Table::V1::TableService::Stub::AsyncStreamReadTable,
            DbDriverState_,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();

    }

    TAsyncReadRowsResult ReadRows(const TString& path, TValue&& keys, const TReadRowsSettings& settings) {
        auto request = MakeRequest<Ydb::Table::ReadRowsRequest>();
        request.set_path(path);
        auto* protoKeys = request.mutable_keys();
        *protoKeys->mutable_type() = TProtoAccessor::GetProto(keys.GetType());
        *protoKeys->mutable_value() = TProtoAccessor::GetProto(keys);

        auto promise = NewPromise<TReadRowsResult>();

        auto responseCb = [promise]
            (Ydb::Table::ReadRowsResponse* response, TPlainStatus status) mutable {
                Y_VERIFY(response);
                TResultSet resultSet = TResultSet(response->result_set());
                TReadRowsResult val(TStatus(std::move(status)), std::move(resultSet));
                promise.SetValue(std::move(val));
            };

        Connections_->Run<Ydb::Table::V1::TableService, Ydb::Table::ReadRowsRequest, Ydb::Table::ReadRowsResponse>(
            std::move(request),
            responseCb,
            &Ydb::Table::V1::TableService::Stub::AsyncReadRows,
            DbDriverState_,
            TRpcRequestSettings::Make(settings), // requestSettings
            TEndpointKey() // preferredEndpoint
            );

        return promise.GetFuture();
    }

    TAsyncStatus Close(const TSession::TImpl* sessionImpl, const TCloseSessionSettings& settings) {
        auto request = MakeOperationRequest<Ydb::Table::DeleteSessionRequest>(settings);
        request.set_session_id(sessionImpl->GetId());
        return RunSimple<Ydb::Table::V1::TableService, Ydb::Table::DeleteSessionRequest, Ydb::Table::DeleteSessionResponse>(
            std::move(request),
            &Ydb::Table::V1::TableService::Stub::AsyncDeleteSession,
            TRpcRequestSettings::Make(settings),
            sessionImpl->GetEndpointKey());
    }

    TAsyncStatus CloseInternal(const TSession::TImpl* sessionImpl) {
        static const auto internalCloseSessionSettings = TCloseSessionSettings()
                .ClientTimeout(TDuration::Seconds(2));

        auto driver = Connections_;
        return Close(sessionImpl, internalCloseSessionSettings)
            .Apply([driver{std::move(driver)}](TAsyncStatus status) mutable
            {
                driver.reset();
                return status;
            });
    }

    bool ReturnSession(TSession::TImpl* sessionImpl) {
        Y_VERIFY(sessionImpl->GetState() == TSession::TImpl::S_ACTIVE ||
                 sessionImpl->GetState() == TSession::TImpl::S_IDLE);

        if (RequestMigrator_.DoCheckAndMigrate(sessionImpl)) {
            SessionRemovedDueBalancing.Inc();
            return false;
        }

        bool needUpdateCounter = sessionImpl->NeedUpdateActiveCounter();
        // Also removes NeedUpdateActiveCounter flag
        sessionImpl->MarkIdle();
        sessionImpl->SetTimeInterval(TDuration::Zero());
        if (!SessionPool_.ReturnSession(sessionImpl, needUpdateCounter)) {
            sessionImpl->SetNeedUpdateActiveCounter(needUpdateCounter);
            return false;
        }
        return true;
    }

    void DeleteSession(TSession::TImpl* sessionImpl) {
        if (sessionImpl->NeedUpdateActiveCounter()) {
            SessionPool_.DecrementActiveCounter();
        }

        if (sessionImpl->GetId()) {
            CloseInternal(sessionImpl);
            DbDriverState_->StatCollector.DecSessionsOnHost(sessionImpl->GetEndpoint());
        }

        delete sessionImpl;
    }

    ui32 GetSessionRetryLimit() const {
        return Settings_.SessionPoolSettings_.RetryLimit_;
    }

    void SetStatCollector(const NSdkStats::TStatCollector::TClientStatCollector& collector) {
        CacheMissCounter.Set(collector.CacheMiss);
        QuerySizeHistogram.Set(collector.QuerySize);
        ParamsSizeHistogram.Set(collector.ParamsSize);
        RetryOperationStatCollector = collector.RetryOperationStatCollector;
        SessionRemovedDueBalancing.Set(collector.SessionRemovedDueBalancing);
        RequestMigrated.Set(collector.RequestMigrated);
    }

    TAsyncBulkUpsertResult BulkUpsert(const TString& table, TValue&& rows, const TBulkUpsertSettings& settings) {
        auto request = MakeOperationRequest<Ydb::Table::BulkUpsertRequest>(settings);
        request.set_table(table);
        *request.mutable_rows()->mutable_type() = TProtoAccessor::GetProto(rows.GetType());
        *request.mutable_rows()->mutable_value() = std::move(rows.GetProto());

        auto promise = NewPromise<TBulkUpsertResult>();

        auto extractor = [promise]
            (google::protobuf::Any* any, TPlainStatus status) mutable {
                Y_UNUSED(any);
                TBulkUpsertResult val(TStatus(std::move(status)));
                promise.SetValue(std::move(val));
            };

        Connections_->RunDeferred<Ydb::Table::V1::TableService, Ydb::Table::BulkUpsertRequest, Ydb::Table::BulkUpsertResponse>(
            std::move(request),
            extractor,
            &Ydb::Table::V1::TableService::Stub::AsyncBulkUpsert,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }

    TAsyncBulkUpsertResult BulkUpsert(const TString& table, EDataFormat format,
        const TString& data, const TString& schema, const TBulkUpsertSettings& settings)
    {
        auto request = MakeOperationRequest<Ydb::Table::BulkUpsertRequest>(settings);
        request.set_table(table);
        if (format == EDataFormat::ApacheArrow) {
            request.mutable_arrow_batch_settings()->set_schema(schema);
        } else if (format == EDataFormat::CSV) {
            auto* csv_settings = request.mutable_csv_settings();
            const auto& format_settings = settings.FormatSettings_;
            if (!format_settings.empty()) {
                bool ok = csv_settings->ParseFromString(format_settings);
                if (!ok) {
                    return {};
                }
            }
        }
        request.set_data(data);

        auto promise = NewPromise<TBulkUpsertResult>();

        auto extractor = [promise]
            (google::protobuf::Any* any, TPlainStatus status) mutable {
                Y_UNUSED(any);
                TBulkUpsertResult val(TStatus(std::move(status)));
                promise.SetValue(std::move(val));
            };

        Connections_->RunDeferred<Ydb::Table::V1::TableService, Ydb::Table::BulkUpsertRequest, Ydb::Table::BulkUpsertResponse>(
            std::move(request),
            extractor,
            &Ydb::Table::V1::TableService::Stub::AsyncBulkUpsert,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }

    TFuture<std::pair<TPlainStatus, TScanQueryProcessorPtr>> StreamExecuteScanQueryInternal(const TString& query,
        const ::google::protobuf::Map<TString, Ydb::TypedValue>* params,
        const TStreamExecScanQuerySettings& settings)
    {
        auto request = MakeRequest<Ydb::Table::ExecuteScanQueryRequest>();
        request.mutable_query()->set_yql_text(query);

        if (params) {
            *request.mutable_parameters() = *params;
        }

        if (settings.Explain_) {
            request.set_mode(Ydb::Table::ExecuteScanQueryRequest::MODE_EXPLAIN);
        } else {
            request.set_mode(Ydb::Table::ExecuteScanQueryRequest::MODE_EXEC);
        }

        request.set_collect_stats(GetStatsCollectionMode(settings.CollectQueryStats_));

        auto promise = NewPromise<std::pair<TPlainStatus, TScanQueryProcessorPtr>>();

        Connections_->StartReadStream<
            Ydb::Table::V1::TableService,
            Ydb::Table::ExecuteScanQueryRequest,
            Ydb::Table::ExecuteScanQueryPartialResponse>
        (
            std::move(request),
            [promise] (TPlainStatus status, TScanQueryProcessorPtr processor) mutable {
                promise.SetValue(std::make_pair(status, processor));
            },
            &Ydb::Table::V1::TableService::Stub::AsyncStreamExecuteScanQuery,
            DbDriverState_,
            TRpcRequestSettings::Make(settings)
        );

        return promise.GetFuture();
    }

    TAsyncScanQueryPartIterator StreamExecuteScanQuery(const TString& query,
        const ::google::protobuf::Map<TString, Ydb::TypedValue>* params,
        const TStreamExecScanQuerySettings& settings)
    {
        auto promise = NewPromise<TScanQueryPartIterator>();

        auto iteratorCallback = [promise](TFuture<std::pair<TPlainStatus,
            TTableClient::TImpl::TScanQueryProcessorPtr>> future) mutable
        {
            Y_ASSERT(future.HasValue());
            auto pair = future.ExtractValue();
            promise.SetValue(TScanQueryPartIterator(
                pair.second
                    ? std::make_shared<TScanQueryPartIterator::TReaderImpl>(pair.second, pair.first.Endpoint)
                    : nullptr,
                std::move(pair.first))
            );
        };

        StreamExecuteScanQueryInternal(query, params, settings).Subscribe(iteratorCallback);
        return promise.GetFuture();
    }

    static void CloseAndDeleteSession(
        std::unique_ptr<TSession::TImpl>&& impl,
        std::shared_ptr<TTableClient::TImpl> client);
public:
    TClientSettings Settings_;

private:
    static void SetParams(
        ::google::protobuf::Map<TString, Ydb::TypedValue>* params,
        Ydb::Table::ExecuteDataQueryRequest* request)
    {
        if (params) {
            request->mutable_parameters()->swap(*params);
        }
    }

    static void SetParams(
        const ::google::protobuf::Map<TString, Ydb::TypedValue>& params,
        Ydb::Table::ExecuteDataQueryRequest* request)
    {
        *request->mutable_parameters() = params;
    }

    static void CollectParams(
        ::google::protobuf::Map<TString, Ydb::TypedValue>* params,
        NSdkStats::TAtomicHistogram<::NMonitoring::THistogram> histgoram)
    {

        if (params && histgoram.IsCollecting()) {
            size_t size = 0;
            for (auto& keyvalue: *params) {
                size += keyvalue.second.ByteSizeLong();
            }
            histgoram.Record(size);
        }
    }

    static void CollectParams(
        const ::google::protobuf::Map<TString, Ydb::TypedValue>& params,
        NSdkStats::TAtomicHistogram<::NMonitoring::THistogram> histgoram)
    {

        if (histgoram.IsCollecting()) {
            size_t size = 0;
            for (auto& keyvalue: params) {
                size += keyvalue.second.ByteSizeLong();
            }
            histgoram.Record(size);
        }
    }

    static void CollectQuerySize(const TString& query, NSdkStats::TAtomicHistogram<::NMonitoring::THistogram>& querySizeHistogram) {
        if (querySizeHistogram.IsCollecting()) {
            querySizeHistogram.Record(query.size());
        }
    }

    static void CollectQuerySize(const TDataQuery&, NSdkStats::TAtomicHistogram<::NMonitoring::THistogram>&) {}

    template <typename TQueryType, typename TParamsType>
    TAsyncDataQueryResult ExecuteDataQueryInternal(const TSession& session, const TQueryType& query,
        const TTxControl& txControl, TParamsType params,
        const TExecDataQuerySettings& settings, bool fromCache)
    {
        auto request = MakeOperationRequest<Ydb::Table::ExecuteDataQueryRequest>(settings);
        request.set_session_id(session.GetId());
        auto txControlProto = request.mutable_tx_control();
        txControlProto->set_commit_tx(txControl.CommitTx_);
        if (txControl.TxId_) {
            txControlProto->set_tx_id(*txControl.TxId_);
        } else {
            SetTxSettings(txControl.BeginTx_, txControlProto->mutable_begin_tx());
        }

        request.set_collect_stats(GetStatsCollectionMode(settings.CollectQueryStats_));

        SetQuery(query, request.mutable_query());
        CollectQuerySize(query, QuerySizeHistogram);

        SetParams(params, &request);
        CollectParams(params, ParamsSizeHistogram);

        SetQueryCachePolicy(query, settings, request.mutable_query_cache_policy());

        auto promise = NewPromise<TDataQueryResult>();
        bool keepInCache = settings.KeepInQueryCache_ && settings.KeepInQueryCache_.GetRef();

        // We don't want to delay call of TSession dtor, so we can't capture it by copy
        // otherwise we break session pool and other clients logic.
        // Same problem with TDataQuery and TTransaction
        //
        // The fast solution is:
        // - create copy of TSession out of lambda
        // - capture pointer
        // - call free just before SetValue call
        auto sessionPtr = new TSession(session);
        auto extractor = [promise, sessionPtr, query, fromCache, keepInCache]
            (google::protobuf::Any* any, TPlainStatus status) mutable {
                TVector<TResultSet> res;
                TMaybe<TTransaction> tx;
                TMaybe<TDataQuery> dataQuery;
                TMaybe<TQueryStats> queryStats;

                auto queryText = GetQueryText(query);
                if (any) {
                    Ydb::Table::ExecuteQueryResult result;
                    any->UnpackTo(&result);

                    for (size_t i = 0; i < result.result_setsSize(); i++) {
                        res.push_back(TResultSet(*result.mutable_result_sets(i)));
                    }

                    if (result.has_tx_meta()) {
                        tx = TTransaction(*sessionPtr, result.tx_meta().id());
                    }

                    if (result.has_query_meta()) {
                        if (queryText) {
                            auto& query_meta = result.query_meta();
                            dataQuery = TDataQuery(*sessionPtr, *queryText, query_meta.id(), query_meta.parameters_types());
                        }
                    }

                    if (result.has_query_stats()) {
                        queryStats = TQueryStats(result.query_stats());
                    }
                }

                if (keepInCache && dataQuery && queryText) {
                    sessionPtr->SessionImpl_->AddQueryToCache(*dataQuery);
                }

                TDataQueryResult dataQueryResult(TStatus(std::move(status)),
                    std::move(res), tx, dataQuery, fromCache, queryStats);

                delete sessionPtr;
                tx.Clear();
                dataQuery.Clear();
                promise.SetValue(std::move(dataQueryResult));
            };

        Connections_->RunDeferred<Ydb::Table::V1::TableService, Ydb::Table::ExecuteDataQueryRequest, Ydb::Table::ExecuteDataQueryResponse>(
            std::move(request),
            extractor,
            &Ydb::Table::V1::TableService::Stub::AsyncExecuteDataQuery,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings),
            session.SessionImpl_->GetEndpointKey());

        return promise.GetFuture();
    }

    static void SetTxSettings(const TTxSettings& txSettings, Ydb::Table::TransactionSettings* proto)
    {
        switch (txSettings.Mode_) {
            case TTxSettings::TS_SERIALIZABLE_RW:
                proto->mutable_serializable_read_write();
                break;
            case TTxSettings::TS_ONLINE_RO:
                proto->mutable_online_read_only()->set_allow_inconsistent_reads(
                    txSettings.OnlineSettings_.AllowInconsistentReads_);
                break;
            case TTxSettings::TS_STALE_RO:
                proto->mutable_stale_read_only();
                break;
            case TTxSettings::TS_SNAPSHOT_RO:
                proto->mutable_snapshot_read_only();
                break;
            default:
                throw TContractViolation("Unexpected transaction mode.");
        }
    }

    static void SetQuery(const TString& queryText, Ydb::Table::Query* query) {
        query->set_yql_text(queryText);
    }

    static void SetQuery(const TDataQuery& queryData, Ydb::Table::Query* query) {
        query->set_id(queryData.GetId());
    }

    static void SetQueryCachePolicy(const TString&, const TExecDataQuerySettings& settings,
        Ydb::Table::QueryCachePolicy* queryCachePolicy)
    {
        queryCachePolicy->set_keep_in_cache(settings.KeepInQueryCache_ ? settings.KeepInQueryCache_.GetRef() : false);
    }

    static void SetQueryCachePolicy(const TDataQuery&, const TExecDataQuerySettings& settings,
        Ydb::Table::QueryCachePolicy* queryCachePolicy) {
        queryCachePolicy->set_keep_in_cache(settings.KeepInQueryCache_ ? settings.KeepInQueryCache_.GetRef() : true);
    }

    static TMaybe<TString> GetQueryText(const TString& queryText) {
        return queryText;
    }

    static TMaybe<TString> GetQueryText(const TDataQuery& queryData) {
        return queryData.GetText();
    }

public:
    NSdkStats::TAtomicCounter<::NMonitoring::TRate> CacheMissCounter;
    NSdkStats::TStatCollector::TClientRetryOperationStatCollector RetryOperationStatCollector;
    NSdkStats::TAtomicHistogram<::NMonitoring::THistogram> QuerySizeHistogram;
    NSdkStats::TAtomicHistogram<::NMonitoring::THistogram> ParamsSizeHistogram;
    NSdkStats::TAtomicCounter<::NMonitoring::TRate> SessionRemovedDueBalancing;
    NSdkStats::TAtomicCounter<::NMonitoring::TRate> RequestMigrated;

private:
    TSessionPoolImpl SessionPool_;
    TRequestMigrator RequestMigrator_;
    static const TKeepAliveSettings KeepAliveSettings;
};

const TKeepAliveSettings TTableClient::TImpl::KeepAliveSettings = TKeepAliveSettings().ClientTimeout(KEEP_ALIVE_CLIENT_TIMEOUT);

TSessionPoolImpl::TSessionPoolImpl(ui32 maxActiveSessions)
    : Closed_(false)
    , ActiveSessions_(0)
    , MaxActiveSessions_(maxActiveSessions)
{}

void TTableClient::TImpl::CloseAndDeleteSession(std::unique_ptr<TSession::TImpl>&& impl,
                                  std::shared_ptr<TTableClient::TImpl> client) {
    std::shared_ptr<TSession::TImpl> deleteSession(
        impl.release(),
        TSession::TImpl::GetSmartDeleter(client));

    deleteSession->MarkBroken();
}

void TSessionPoolImpl::CreateFakeSession(
    NThreading::TPromise<TCreateSessionResult>& promise,
    std::shared_ptr<TTableClient::TImpl> client)
{
    TSession session(client, "", "");
    // Mark broken to prevent returning to session pool
    session.SessionImpl_->MarkBroken();
    TCreateSessionResult val(
        TStatus(
            TPlainStatus(
                EStatus::CLIENT_RESOURCE_EXHAUSTED,
                "Active sessions limit exceeded"
            )
        ),
        std::move(session)
    );

    client->ScheduleTaskUnsafe([promise, val{std::move(val)}]() mutable {
        promise.SetValue(std::move(val));
    }, TDuration());
}

TAsyncCreateSessionResult TSessionPoolImpl::GetSession(
    std::shared_ptr<TTableClient::TImpl> client,
    const TCreateSessionSettings& settings)
{
    auto createSessionPromise = NewPromise<TCreateSessionResult>();
    std::unique_ptr<TSession::TImpl> sessionImpl;
    bool needUpdateActiveSessionCounter = false;
    bool returnFakeSession = false;
    {
        std::lock_guard guard(Mtx_);
        if (MaxActiveSessions_) {
            if (ActiveSessions_ < MaxActiveSessions_) {
                ActiveSessions_++;
                needUpdateActiveSessionCounter = true;
            } else {
                returnFakeSession = true;
            }
        } else {
            ActiveSessions_++;
            needUpdateActiveSessionCounter = true;
        }
        if (!Sessions_.empty()) {
            auto it = std::prev(Sessions_.end());
            sessionImpl = std::move(it->second);
            Sessions_.erase(it);
        }
        UpdateStats();
    }
    if (returnFakeSession) {
        FakeSessionsCounter_.Inc();
        CreateFakeSession(createSessionPromise, client);
        return createSessionPromise.GetFuture();
    } else if (sessionImpl) {
        Y_VERIFY(sessionImpl->GetState() == TSession::TImpl::S_IDLE);
        Y_VERIFY(!sessionImpl->GetTimeInterval());
        Y_VERIFY(needUpdateActiveSessionCounter);
        sessionImpl->MarkActive();
        sessionImpl->SetNeedUpdateActiveCounter(true);

        TCreateSessionResult val(TStatus(TPlainStatus()),
            TSession(client, std::shared_ptr<TSession::TImpl>(
                sessionImpl.release(),
                TSession::TImpl::GetSmartDeleter(client))));

        client->ScheduleTaskUnsafe([createSessionPromise, val{std::move(val)}]() mutable {
            createSessionPromise.SetValue(std::move(val));
        }, TDuration());

        return createSessionPromise.GetFuture();
    } else {
        const auto& sessionResult = client->CreateSession(settings, false);
        sessionResult.Subscribe(TSession::TImpl::GetSessionInspector(createSessionPromise, client, settings, 0, needUpdateActiveSessionCounter));
        return createSessionPromise.GetFuture();
    }
}

bool TSessionPoolImpl::DropSessionOnEndpoint(std::shared_ptr<TTableClient::TImpl> client, ui64 nodeId) {
    std::unique_ptr<TSession::TImpl> sessionImpl;
    {
        std::lock_guard guard(Mtx_);
        for (auto it = Sessions_.begin(); it != Sessions_.end(); it++) {
            if (it->second->GetEndpointKey().GetNodeId() == nodeId) {
                sessionImpl = std::move(it->second);
                Sessions_.erase(it);
                break;
            }
        }
    }
    if (!sessionImpl)
        return false;

    auto deleteFn = TSession::TImpl::GetSmartDeleter(client);
    deleteFn(sessionImpl.release());

    return true;
}

bool TSessionPoolImpl::ReturnSession(TSession::TImpl* impl, bool active) {
    {
        std::lock_guard guard(Mtx_);
        if (Closed_)
            return false;
        Sessions_.emplace(std::make_pair(impl->GetTimeToTouchFast(), impl));
        if (active) {
            Y_VERIFY(ActiveSessions_);
            ActiveSessions_--;
            impl->SetNeedUpdateActiveCounter(false);
        }
        UpdateStats();
    }
    return true;
}

void TSessionPoolImpl::DecrementActiveCounter() {
    std::lock_guard guard(Mtx_);
    Y_VERIFY(ActiveSessions_);
    ActiveSessions_--;
    UpdateStats();
}

void TSessionPoolImpl::Drain(std::function<bool(std::unique_ptr<TSession::TImpl>&&)> cb, bool close) {
    std::lock_guard guard(Mtx_);
    Closed_ = close;
    for (auto it = Sessions_.begin(); it != Sessions_.end();) {
        const bool cont = cb(std::move(it->second));
        it = Sessions_.erase(it);
        if (!cont)
            break;
    }
    UpdateStats();
}

TPeriodicCb TSessionPoolImpl::CreatePeriodicTask(std::weak_ptr<TTableClient::TImpl> weakClient,
    TKeepAliveCmd&& cmd, TDeletePredicate&& deletePredicate)
{
    auto periodicCb = [this, weakClient, cmd=std::move(cmd), deletePredicate=std::move(deletePredicate)](NYql::TIssues&&, EStatus status) {
        if (status != EStatus::SUCCESS) {
            return false;
        }

        auto strongClient = weakClient.lock();
        if (!strongClient) {
            // No more clients alive - no need to run periodic,
            // moreover it is unsafe to touch this ptr!
            return false;
        } else {
            auto keepAliveBatchSize = PERIODIC_ACTION_BATCH_SIZE;
            TVector<std::unique_ptr<TSession::TImpl>> sessionsToTouch;
            sessionsToTouch.reserve(keepAliveBatchSize);
            TVector<std::unique_ptr<TSession::TImpl>> sessionsToDelete;
            sessionsToDelete.reserve(keepAliveBatchSize);
            auto now = TInstant::Now();
            {
                std::lock_guard guard(Mtx_);
                auto& sessions = Sessions_;

                auto it = sessions.begin();
                while (it != sessions.end() && keepAliveBatchSize--) {
                    if (now < it->second->GetTimeToTouchFast())
                        break;

                    if (deletePredicate(it->second.get(), strongClient.get(), sessions.size())) {
                        sessionsToDelete.emplace_back(std::move(it->second));
                    } else {
                        sessionsToTouch.emplace_back(std::move(it->second));
                    }
                    sessions.erase(it++);
                }
                UpdateStats();
            }

            for (auto& sessionImpl : sessionsToTouch) {
                if (sessionImpl) {
                    Y_VERIFY(sessionImpl->GetState() == TSession::TImpl::S_IDLE);
                    TSession session(strongClient, std::shared_ptr<TSession::TImpl>(
                        sessionImpl.release(),
                        TSession::TImpl::GetSmartDeleter(strongClient)));
                    cmd(session);
                }
            }

            for (auto& sessionImpl : sessionsToDelete) {
                if (sessionImpl) {
                    Y_VERIFY(sessionImpl->GetState() == TSession::TImpl::S_IDLE);
                    TTableClient::TImpl::CloseAndDeleteSession(std::move(sessionImpl), strongClient);
                }
            }
        }

        return true;
    };
    return periodicCb;
}

i64 TSessionPoolImpl::GetActiveSessions() const {
    std::lock_guard guard(Mtx_);
    return ActiveSessions_;
}

i64 TSessionPoolImpl::GetActiveSessionsLimit() const {
    return MaxActiveSessions_;
}

i64 TSessionPoolImpl::GetCurrentPoolSize() const {
    std::lock_guard guard(Mtx_);
    return Sessions_.size();
}

static bool IsSessionStatusRetriable(const TCreateSessionResult& res) {
    switch (res.GetStatus()) {
        case EStatus::OVERLOADED:
        // For CreateSession request we can retry some of transport errors
        // - endpoind will be pessimized and session will be created on the
        // another endpoint
        case EStatus::CLIENT_DEADLINE_EXCEEDED:
        case EStatus::CLIENT_RESOURCE_EXHAUSTED:
        case EStatus::TRANSPORT_UNAVAILABLE:
            return true;
        default:
            return false;
    }
}

TSessionInspectorFn TSession::TImpl::GetSessionInspector(
    NThreading::TPromise<TCreateSessionResult>& promise,
    std::shared_ptr<TTableClient::TImpl> client,
    const TCreateSessionSettings& settings,
    ui32 counter, bool needUpdateActiveSessionCounter)
{
    return [promise, client, settings, counter, needUpdateActiveSessionCounter](TAsyncCreateSessionResult future) mutable {
        Y_ASSERT(future.HasValue());
        auto session = future.ExtractValue();
        if (IsSessionStatusRetriable(session) && counter < client->GetSessionRetryLimit()) {
            counter++;
            client->CreateSession(settings, false)
                .Subscribe(GetSessionInspector(
                    promise,
                    client,
                    settings,
                    counter,
                    needUpdateActiveSessionCounter)
                );
        } else {
            session.Session_.SessionImpl_->SetNeedUpdateActiveCounter(needUpdateActiveSessionCounter);
            promise.SetValue(std::move(session));
        }
    };
}

void TSessionPoolImpl::SetStatCollector(NSdkStats::TStatCollector::TSessionPoolStatCollector statCollector) {
    ActiveSessionsCounter_.Set(statCollector.ActiveSessions);
    InPoolSessionsCounter_.Set(statCollector.InPoolSessions);
    FakeSessionsCounter_.Set(statCollector.FakeSessions);
}

void TSessionPoolImpl::UpdateStats() {
    ActiveSessionsCounter_.Apply(ActiveSessions_);
    InPoolSessionsCounter_.Apply(Sessions_.size());
}

TTableClient::TTableClient(const TDriver& driver, const TClientSettings& settings)
    : Impl_(new TImpl(CreateInternalInterface(driver), settings)) {
    Impl_->StartPeriodicSessionPoolTask();
    Impl_->StartPeriodicHostScanTask();
    Impl_->InitStopper();
}

TAsyncCreateSessionResult TTableClient::CreateSession(const TCreateSessionSettings& settings) {
    // Returns standalone session
    return Impl_->CreateSession(settings, true);
}

TAsyncCreateSessionResult TTableClient::GetSession(const TCreateSessionSettings& settings) {
    // Returns session from session pool
    return Impl_->GetSession(settings);
}

i64 TTableClient::GetActiveSessionCount() const {
    return Impl_->GetActiveSessionCount();
}

i64 TTableClient::GetActiveSessionsLimit() const {
    return Impl_->GetActiveSessionsLimit();
}

i64 TTableClient::GetCurrentPoolSize() const {
    return Impl_->GetCurrentPoolSize();
}

TTableBuilder TTableClient::GetTableBuilder() {
    return TTableBuilder();
}

TParamsBuilder TTableClient::GetParamsBuilder() const {
    return TParamsBuilder();
}

TTypeBuilder TTableClient::GetTypeBuilder() {
    return TTypeBuilder();
}

////////////////////////////////////////////////////////////////////////////////

struct TRetryState {
    using TAsyncFunc = std::function<TAsyncStatus()>;
    using THandleStatusFunc = std::function<TAsyncStatus(const std::shared_ptr<TRetryState>& state,
        const TStatus& status, const TAsyncFunc& func, ui32 retryNumber)>;

    TMaybe<TSession> Session;
    THandleStatusFunc HandleStatusFunc;
};

static void Backoff(const TBackoffSettings& settings, ui32 retryNumber) {
    auto durationMs = CalcBackoffTime(settings, retryNumber);
    Sleep(TDuration::MilliSeconds(durationMs));
}

class TRetryOperationContext : public TThrRefBase, TNonCopyable {
public:
    using TRetryContextPtr = TIntrusivePtr<TRetryOperationContext>;

protected:
    TRetryOperationSettings Settings;
    TTableClient TableClient;
    NThreading::TPromise<TStatus> Promise;
    ui32 RetryNumber;

public:
    virtual void Execute() = 0;

    TAsyncStatus GetFuture() {
        return Promise.GetFuture();
    }

protected:
    TRetryOperationContext(const TRetryOperationSettings& settings,
                           const TTableClient& tableClient)
        : Settings(settings)
        , TableClient(tableClient)
        , Promise(NThreading::NewPromise<TStatus>())
        , RetryNumber(0)
    {}

    static void RunOp(TRetryContextPtr self) {
        self->Execute();
    }

    virtual void Reset() {}

    static void DoRetry(TRetryContextPtr self, bool fast) {
        self->TableClient.Impl_->AsyncBackoff(
                    fast ? self->Settings.FastBackoffSettings_ : self->Settings.SlowBackoffSettings_,
                    self->RetryNumber,
                    [self]() {
                        RunOp(self);
                    }
        );
    }

    static void HandleStatus(TRetryContextPtr self, const TStatus& status) {
        if (status.IsSuccess()) {
            return self->Promise.SetValue(status);
        }

        if (self->RetryNumber >= self->Settings.MaxRetries_) {
            return self->Promise.SetValue(status);
        }
        if (self->Settings.Verbose_) {
            Cerr << "Previous query attempt was finished with unsuccessful status: "
                << status.GetIssues().ToString() << ". Status is " << status.GetStatus() << "."
                << "Send retry attempt " << self->RetryNumber << " of " << self->Settings.MaxRetries_ << Endl;
        }
        self->RetryNumber++;
        self->TableClient.Impl_->RetryOperationStatCollector.IncAsyncRetryOperation(status.GetStatus());

        switch (status.GetStatus()) {
            case EStatus::ABORTED:
                return RunOp(self);

            case EStatus::OVERLOADED:
            case EStatus::CLIENT_RESOURCE_EXHAUSTED:
                return DoRetry(self, false);

            case EStatus::UNAVAILABLE:
                return DoRetry(self, true);

            case EStatus::BAD_SESSION:
            case EStatus::SESSION_BUSY:
                self->Reset();
                return RunOp(self);

            case EStatus::NOT_FOUND:
                return self->Settings.RetryNotFound_
                    ? RunOp(self)
                    : self->Promise.SetValue(status);

            case EStatus::UNDETERMINED:
                return self->Settings.Idempotent_
                    ? DoRetry(self, true)
                    : self->Promise.SetValue(status);

            case EStatus::TRANSPORT_UNAVAILABLE:
                if (self->Settings.Idempotent_) {
                    self->Reset();
                    return DoRetry(self, true);
                } else {
                    return self->Promise.SetValue(status);
                }

            default:
                return self->Promise.SetValue(status);
        }
    }

    static void HandleException(TRetryContextPtr self, std::exception_ptr e) {
        self->Promise.SetException(e);
    }
};

class TRetryOperationWithSession : public TRetryOperationContext {
    using TFunc = TTableClient::TOperationFunc;

    TFunc Operation;
    TMaybe<TSession> Session;

public:
    explicit TRetryOperationWithSession(TFunc&& operation,
                                        const TRetryOperationSettings& settings,
                                        const TTableClient& tableClient)
        : TRetryOperationContext(settings, tableClient)
        , Operation(operation)
    {}

    void Execute() override {
        TRetryContextPtr self(this);
        if (!Session) {
            TableClient.GetSession(
                TCreateSessionSettings().ClientTimeout(Settings.GetSessionClientTimeout_)).Subscribe(
                [self](const TAsyncCreateSessionResult& resultFuture) {
                    try {
                        auto& result = resultFuture.GetValue();
                        if (!result.IsSuccess()) {
                            return HandleStatus(self, result);
                        }

                        auto* myself = dynamic_cast<TRetryOperationWithSession*>(self.Get());
                        myself->Session = result.GetSession();
                        myself->DoRunOp(self);
                    } catch (...) {
                        return HandleException(self, std::current_exception());
                    }
            });
        } else {
            DoRunOp(self);
        }
    }

private:
    void Reset() override {
        Session.Clear();
    }

    void DoRunOp(TRetryContextPtr self) {
        Operation(Session.GetRef()).Subscribe([self](const TAsyncStatus& result) {
            try {
                return HandleStatus(self, result.GetValue());
            } catch (...) {
                return HandleException(self, std::current_exception());
            }
        });
    }
};

TAsyncStatus TTableClient::RetryOperation(TOperationFunc&& operation, const TRetryOperationSettings& settings) {
    TRetryOperationContext::TRetryContextPtr ctx(new TRetryOperationWithSession(std::move(operation), settings, *this));
    ctx->Execute();
    return ctx->GetFuture();
}

class TRetryOperationWithoutSession : public TRetryOperationContext {
    using TFunc = TTableClient::TOperationWithoutSessionFunc;

    TFunc Operation;

public:
    explicit TRetryOperationWithoutSession(TFunc&& operation,
                                        const TRetryOperationSettings& settings,
                                        const TTableClient& tableClient)
        : TRetryOperationContext(settings, tableClient)
        , Operation(operation)
    {}

    void Execute() override {
        TRetryContextPtr self(this);
        Operation(TableClient).Subscribe([self](const TAsyncStatus& result) {
            try {
                return HandleStatus(self, result.GetValue());
            } catch (...) {
                return HandleException(self, std::current_exception());
            }
        });
    }
};

TAsyncStatus TTableClient::RetryOperation(TOperationWithoutSessionFunc&& operation, const TRetryOperationSettings& settings) {
    TRetryOperationContext::TRetryContextPtr ctx(new TRetryOperationWithoutSession(std::move(operation), settings, *this));
    ctx->Execute();
    return ctx->GetFuture();
}

TStatus TTableClient::RetryOperationSyncHelper(const TOperationWrapperSyncFunc& operationWrapper, const TRetryOperationSettings& settings) {
    TRetryState retryState;
    TMaybe<NYdb::TStatus> status;

    for (ui32 retryNumber = 0; retryNumber <= settings.MaxRetries_; ++retryNumber) {
        status = operationWrapper(retryState);

        if (status->IsSuccess()) {
            return *status;
        }

        if (retryNumber == settings.MaxRetries_) {
            break;
        }

        switch (status->GetStatus()) {
            case EStatus::ABORTED:
                break;

            case EStatus::OVERLOADED:
            case EStatus::CLIENT_RESOURCE_EXHAUSTED: {
                Backoff(settings.SlowBackoffSettings_, retryNumber);
                break;
            }

            case EStatus::UNAVAILABLE:{
                Backoff(settings.FastBackoffSettings_, retryNumber);
                break;
            }

            case EStatus::BAD_SESSION:
            case EStatus::SESSION_BUSY:
                retryState.Session.Clear();
                break;

            case EStatus::NOT_FOUND:
                if (!settings.RetryNotFound_) {
                    return *status;
                }
                break;

            case EStatus::UNDETERMINED:
                if (!settings.Idempotent_) {
                    return *status;
                }
                Backoff(settings.FastBackoffSettings_, retryNumber);
                break;

            case EStatus::TRANSPORT_UNAVAILABLE:
                if (!settings.Idempotent_) {
                    return *status;
                }
                retryState.Session.Clear();
                Backoff(settings.FastBackoffSettings_, retryNumber);
                break;

            default:
                return *status;
        }
        Impl_->RetryOperationStatCollector.IncSyncRetryOperation(status->GetStatus());
    }

    return *status;
}

TStatus TTableClient::RetryOperationSync(const TOperationWithoutSessionSyncFunc& operation, const TRetryOperationSettings& settings) {
    auto operationWrapper = [this, &operation] (TRetryState&) {
        return operation(*this);
    };

    return RetryOperationSyncHelper(operationWrapper, settings);
}

TStatus TTableClient::RetryOperationSync(const TOperationSyncFunc& operation, const TRetryOperationSettings& settings) {
    TRetryState retryState;

    auto operationWrapper = [this, &operation, &settings] (TRetryState& retryState) {
        TMaybe<NYdb::TStatus> status;

        if (!retryState.Session) {
            auto sessionResult = Impl_->GetSession(
                TCreateSessionSettings().ClientTimeout(settings.GetSessionClientTimeout_)).GetValueSync();
            if (sessionResult.IsSuccess()) {
                retryState.Session = sessionResult.GetSession();
            }
            status = sessionResult;
        }

        if (retryState.Session) {
            status = operation(retryState.Session.GetRef());
            if (status->IsSuccess()) {
                return *status;
            }
        }

        return *status;
    };

    return RetryOperationSyncHelper(operationWrapper, settings);
}

NThreading::TFuture<void> TTableClient::Stop() {
    return Impl_->Stop();
}

TAsyncBulkUpsertResult TTableClient::BulkUpsert(const TString& table, TValue&& rows,
    const TBulkUpsertSettings& settings)
{
    return Impl_->BulkUpsert(table, std::move(rows), settings);
}

TAsyncBulkUpsertResult TTableClient::BulkUpsert(const TString& table, EDataFormat format,
        const TString& data, const TString& schema, const TBulkUpsertSettings& settings)
{
    return Impl_->BulkUpsert(table, format, data, schema, settings);
}

TAsyncReadRowsResult TTableClient::ReadRows(const TString& table, TValue&& rows,
    const TReadRowsSettings& settings)
{
    return Impl_->ReadRows(table, std::move(rows), settings);
}

TAsyncScanQueryPartIterator TTableClient::StreamExecuteScanQuery(const TString& query, const TParams& params,
    const TStreamExecScanQuerySettings& settings)
{
    return Impl_->StreamExecuteScanQuery(query, &params.GetProtoMap(), settings);
}

TAsyncScanQueryPartIterator TTableClient::StreamExecuteScanQuery(const TString& query,
    const TStreamExecScanQuerySettings& settings)
{
    return Impl_->StreamExecuteScanQuery(query, nullptr, settings);
}

////////////////////////////////////////////////////////////////////////////////

static void ConvertCreateTableSettingsToProto(const TCreateTableSettings& settings, Ydb::Table::TableProfile* proto) {
    if (settings.PresetName_) {
        proto->set_preset_name(settings.PresetName_.GetRef());
    }
    if (settings.ExecutionPolicy_) {
        proto->mutable_execution_policy()->set_preset_name(settings.ExecutionPolicy_.GetRef());
    }
    if (settings.CompactionPolicy_) {
        proto->mutable_compaction_policy()->set_preset_name(settings.CompactionPolicy_.GetRef());
    }
    if (settings.PartitioningPolicy_) {
        const auto& policy = settings.PartitioningPolicy_.GetRef();
        if (policy.PresetName_) {
            proto->mutable_partitioning_policy()->set_preset_name(policy.PresetName_.GetRef());
        }
        if (policy.AutoPartitioning_) {
            proto->mutable_partitioning_policy()->set_auto_partitioning(static_cast<Ydb::Table::PartitioningPolicy_AutoPartitioningPolicy>(policy.AutoPartitioning_.GetRef()));
        }
        if (policy.UniformPartitions_) {
            proto->mutable_partitioning_policy()->set_uniform_partitions(policy.UniformPartitions_.GetRef());
        }
        if (policy.ExplicitPartitions_) {
            auto* borders = proto->mutable_partitioning_policy()->mutable_explicit_partitions();
            for (const auto& splitPoint : policy.ExplicitPartitions_->SplitPoints_) {
                auto* border = borders->Addsplit_points();
                border->mutable_type()->CopyFrom(TProtoAccessor::GetProto(splitPoint.GetType()));
                border->mutable_value()->CopyFrom(TProtoAccessor::GetProto(splitPoint));
            }
        }
    }
    if (settings.StoragePolicy_) {
        const auto& policy = settings.StoragePolicy_.GetRef();
        if (policy.PresetName_) {
            proto->mutable_storage_policy()->set_preset_name(policy.PresetName_.GetRef());
        }
        if (policy.SysLog_) {
            proto->mutable_storage_policy()->mutable_syslog()->set_media(policy.SysLog_.GetRef());
        }
        if (policy.Log_) {
            proto->mutable_storage_policy()->mutable_log()->set_media(policy.Log_.GetRef());
        }
        if (policy.Data_) {
            proto->mutable_storage_policy()->mutable_data()->set_media(policy.Data_.GetRef());
        }
        if (policy.External_) {
            proto->mutable_storage_policy()->mutable_external()->set_media(policy.External_.GetRef());
        }
        for (const auto& familyPolicy : policy.ColumnFamilies_) {
            auto* familyProto = proto->mutable_storage_policy()->add_column_families();
            if (familyPolicy.Name_) {
                familyProto->set_name(familyPolicy.Name_.GetRef());
            }
            if (familyPolicy.Data_) {
                familyProto->mutable_data()->set_media(familyPolicy.Data_.GetRef());
            }
            if (familyPolicy.External_) {
                familyProto->mutable_external()->set_media(familyPolicy.External_.GetRef());
            }
            if (familyPolicy.KeepInMemory_) {
                familyProto->set_keep_in_memory(
                    familyPolicy.KeepInMemory_.GetRef()
                    ? Ydb::FeatureFlag_Status::FeatureFlag_Status_ENABLED
                    : Ydb::FeatureFlag_Status::FeatureFlag_Status_DISABLED
                );
            }
            if (familyPolicy.Compressed_) {
                familyProto->set_compression(familyPolicy.Compressed_.GetRef()
                    ? Ydb::Table::ColumnFamilyPolicy::COMPRESSED
                    : Ydb::Table::ColumnFamilyPolicy::UNCOMPRESSED);
            }
        }
    }
    if (settings.ReplicationPolicy_) {
        const auto& policy = settings.ReplicationPolicy_.GetRef();
        if (policy.PresetName_) {
            proto->mutable_replication_policy()->set_preset_name(policy.PresetName_.GetRef());
        }
        if (policy.ReplicasCount_) {
            proto->mutable_replication_policy()->set_replicas_count(policy.ReplicasCount_.GetRef());
        }
        if (policy.CreatePerAvailabilityZone_) {
            proto->mutable_replication_policy()->set_create_per_availability_zone(
                policy.CreatePerAvailabilityZone_.GetRef()
                ? Ydb::FeatureFlag_Status::FeatureFlag_Status_ENABLED
                : Ydb::FeatureFlag_Status::FeatureFlag_Status_DISABLED
            );
        }
        if (policy.AllowPromotion_) {
            proto->mutable_replication_policy()->set_allow_promotion(
                policy.AllowPromotion_.GetRef()
                ? Ydb::FeatureFlag_Status::FeatureFlag_Status_ENABLED
                : Ydb::FeatureFlag_Status::FeatureFlag_Status_DISABLED
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TSession::TSession(std::shared_ptr<TTableClient::TImpl> client, const TString& sessionId, const TString& endpointId)
    : Client_(client)
    , SessionImpl_(new TSession::TImpl(
            sessionId,
            endpointId,
            client->Settings_.UseQueryCache_,
            client->Settings_.QueryCacheSize_),
        TSession::TImpl::GetSmartDeleter(client))
{
    if (endpointId) {
        Client_->LinkObjToEndpoint(SessionImpl_->GetEndpointKey(), SessionImpl_.get(), Client_.get());
    }
}

TSession::TSession(std::shared_ptr<TTableClient::TImpl> client, std::shared_ptr<TImpl> sessionid)
    : Client_(client)
    , SessionImpl_(sessionid)
{}

TFuture<TStatus> TSession::CreateTable(const TString& path, TTableDescription&& tableDesc,
        const TCreateTableSettings& settings)
{
    auto request = MakeOperationRequest<Ydb::Table::CreateTableRequest>(settings);
    request.set_session_id(SessionImpl_->GetId());
    request.set_path(path);

    tableDesc.SerializeTo(request);

    ConvertCreateTableSettingsToProto(settings, request.mutable_profile());

    return InjectSessionStatusInterception(
        SessionImpl_,
        Client_->CreateTable(std::move(request), settings),
        false,
        GetMinTimeToTouch(Client_->Settings_.SessionPoolSettings_));
}

TFuture<TStatus> TSession::DropTable(const TString& path, const TDropTableSettings& settings) {
    return InjectSessionStatusInterception(
        SessionImpl_,
        Client_->DropTable(SessionImpl_->GetId(), path, settings),
        false,
        GetMinTimeToTouch(Client_->Settings_.SessionPoolSettings_));
}

static Ydb::Table::AlterTableRequest MakeAlterTableProtoRequest(
    const TString& path, const TAlterTableSettings& settings, const TString& sessionId)
{
    auto request = MakeOperationRequest<Ydb::Table::AlterTableRequest>(settings);
    request.set_session_id(sessionId);
    request.set_path(path);

    for (const auto& column : settings.AddColumns_) {
        auto& protoColumn = *request.add_add_columns();
        protoColumn.set_name(column.Name);
        protoColumn.mutable_type()->CopyFrom(TProtoAccessor::GetProto(column.Type));
        protoColumn.set_family(column.Family);
    }

    for (const auto& columnName : settings.DropColumns_) {
        request.add_drop_columns(columnName);
    }

    for (const auto& alter : settings.AlterColumns_) {
        auto& protoAlter = *request.add_alter_columns();
        protoAlter.set_name(alter.Name);
        protoAlter.set_family(alter.Family);
    }

    for (const auto& addIndex : settings.AddIndexes_) {
        addIndex.SerializeTo(*request.add_add_indexes());
    }

    for (const auto& name : settings.DropIndexes_) {
        request.add_drop_indexes(name);
    }

    for (const auto& rename : settings.RenameIndexes_) {
        SerializeTo(rename, *request.add_rename_indexes());
    }

    for (const auto& addChangefeed : settings.AddChangefeeds_) {
        addChangefeed.SerializeTo(*request.add_add_changefeeds());
    }

    for (const auto& name : settings.DropChangefeeds_) {
        request.add_drop_changefeeds(name);
    }

    if (settings.AlterStorageSettings_) {
        request.mutable_alter_storage_settings()->CopyFrom(settings.AlterStorageSettings_->GetProto());
    }

    for (const auto& family : settings.AddColumnFamilies_) {
        request.add_add_column_families()->CopyFrom(family.GetProto());
    }

    for (const auto& family : settings.AlterColumnFamilies_) {
        request.add_alter_column_families()->CopyFrom(family.GetProto());
    }

    if (const auto& ttl = settings.GetAlterTtlSettings()) {
        switch (ttl->GetAction()) {
        case TAlterTtlSettings::EAction::Set:
            ttl->GetTtlSettings().SerializeTo(*request.mutable_set_ttl_settings());
            break;
        case TAlterTtlSettings::EAction::Drop:
            request.mutable_drop_ttl_settings();
            break;
        }
    }

    for (const auto& [key, value] : settings.AlterAttributes_) {
        (*request.mutable_alter_attributes())[key] = value;
    }

    if (settings.SetCompactionPolicy_) {
        request.set_set_compaction_policy(settings.SetCompactionPolicy_);
    }

    if (settings.AlterPartitioningSettings_) {
        request.mutable_alter_partitioning_settings()->CopyFrom(settings.AlterPartitioningSettings_->GetProto());
    }

    if (settings.SetKeyBloomFilter_.Defined()) {
        request.set_set_key_bloom_filter(
            settings.SetKeyBloomFilter_.GetRef() ? Ydb::FeatureFlag::ENABLED : Ydb::FeatureFlag::DISABLED);
    }

    if (settings.SetReadReplicasSettings_.Defined()) {
        const auto& replSettings = settings.SetReadReplicasSettings_.GetRef();
        switch (replSettings.GetMode()) {
        case TReadReplicasSettings::EMode::PerAz:
            request.mutable_set_read_replicas_settings()->set_per_az_read_replicas_count(
                replSettings.GetReadReplicasCount());
            break;
        case TReadReplicasSettings::EMode::AnyAz:
            request.mutable_set_read_replicas_settings()->set_any_az_read_replicas_count(
                replSettings.GetReadReplicasCount());
            break;
        default:
            break;
        }
    }

    return request;
}

TAsyncStatus TSession::AlterTable(const TString& path, const TAlterTableSettings& settings) {
    auto request = MakeAlterTableProtoRequest(path, settings, SessionImpl_->GetId());

    return InjectSessionStatusInterception(
        SessionImpl_,
        Client_->AlterTable(std::move(request), settings),
        false,
        GetMinTimeToTouch(Client_->Settings_.SessionPoolSettings_));
}

TAsyncOperation TSession::AlterTableLong(const TString& path, const TAlterTableSettings& settings) {
    auto request = MakeAlterTableProtoRequest(path, settings, SessionImpl_->GetId());

    return InjectSessionStatusInterception(
        SessionImpl_,
        Client_->AlterTableLong(std::move(request), settings),
        false,
        GetMinTimeToTouch(Client_->Settings_.SessionPoolSettings_));
}

TAsyncStatus TSession::RenameTables(const TVector<TRenameItem>& renameItems, const TRenameTablesSettings& settings) {
    auto request = MakeOperationRequest<Ydb::Table::RenameTablesRequest>(settings);
    request.set_session_id(SessionImpl_->GetId());

    for (const auto& item: renameItems) {
        auto add = request.add_tables();
        add->set_source_path(item.SourcePath());
        add->set_destination_path(item.DestinationPath());
        add->set_replace_destination(item.ReplaceDestination());
    }

    return InjectSessionStatusInterception(
        SessionImpl_,
        Client_->RenameTables(std::move(request), settings),
        false,
        GetMinTimeToTouch(Client_->Settings_.SessionPoolSettings_));
}

TAsyncStatus TSession::CopyTables(const TVector<TCopyItem>& copyItems, const TCopyTablesSettings& settings) {
    auto request = MakeOperationRequest<Ydb::Table::CopyTablesRequest>(settings);
    request.set_session_id(SessionImpl_->GetId());

    for (const auto& item: copyItems) {
        auto add = request.add_tables();
        add->set_source_path(item.SourcePath());
        add->set_destination_path(item.DestinationPath());
        add->set_omit_indexes(item.OmitIndexes());
    }

    return InjectSessionStatusInterception(
        SessionImpl_,
        Client_->CopyTables(std::move(request), settings),
        false,
        GetMinTimeToTouch(Client_->Settings_.SessionPoolSettings_));
}

TFuture<TStatus> TSession::CopyTable(const TString& src, const TString& dst, const TCopyTableSettings& settings) {
    return InjectSessionStatusInterception(
        SessionImpl_,
        Client_->CopyTable(SessionImpl_->GetId(), src, dst, settings),
        false,
        GetMinTimeToTouch(Client_->Settings_.SessionPoolSettings_));
}

TAsyncDescribeTableResult TSession::DescribeTable(const TString& path, const TDescribeTableSettings& settings) {
    return Client_->DescribeTable(SessionImpl_->GetId(), path, settings);
}

TAsyncDataQueryResult TSession::ExecuteDataQuery(const TString& query, const TTxControl& txControl,
    const TExecDataQuerySettings& settings)
{
    return Client_->ExecuteDataQuery(*this, query, txControl, nullptr, settings);
}

TAsyncDataQueryResult TSession::ExecuteDataQuery(const TString& query, const TTxControl& txControl,
    TParams&& params, const TExecDataQuerySettings& settings)
{
    auto paramsPtr = params.Empty() ? nullptr : params.GetProtoMapPtr();
    return Client_->ExecuteDataQuery(*this, query, txControl, paramsPtr, settings);
}

TAsyncDataQueryResult TSession::ExecuteDataQuery(const TString& query, const TTxControl& txControl,
    const TParams& params, const TExecDataQuerySettings& settings)
{
    if (params.Empty()) {
        return Client_->ExecuteDataQuery(
            *this,
            query,
            txControl,
            nullptr,
            settings);
    } else {
        using TProtoParamsType = const ::google::protobuf::Map<TString, Ydb::TypedValue>;
        return Client_->ExecuteDataQuery<TProtoParamsType&>(
            *this,
            query,
            txControl,
            params.GetProtoMap(),
            settings);
    }
}

TAsyncPrepareQueryResult TSession::PrepareDataQuery(const TString& query, const TPrepareDataQuerySettings& settings) {
    auto maybeQuery = SessionImpl_->GetQueryFromCache(query, Client_->Settings_.AllowRequestMigration_);
    if (maybeQuery) {
        TStatus status(EStatus::SUCCESS, NYql::TIssues());
        TDataQuery dataQuery(*this, query, maybeQuery->QueryId, maybeQuery->ParameterTypes);
        TPrepareQueryResult result(std::move(status), dataQuery, true);
        return MakeFuture(result);
    }

    Client_->CacheMissCounter.Inc();

    return InjectSessionStatusInterception(
        SessionImpl_,
        Client_->PrepareDataQuery(*this, query, settings),
        true,
        GetMinTimeToTouch(Client_->Settings_.SessionPoolSettings_));
}

TAsyncStatus TSession::ExecuteSchemeQuery(const TString& query, const TExecSchemeQuerySettings& settings) {
    return InjectSessionStatusInterception(
        SessionImpl_,
        Client_->ExecuteSchemeQuery(SessionImpl_->GetId(), query, settings),
        true,
        GetMinTimeToTouch(Client_->Settings_.SessionPoolSettings_));
}

TAsyncBeginTransactionResult TSession::BeginTransaction(const TTxSettings& txSettings,
    const TBeginTxSettings& settings)
{
    return InjectSessionStatusInterception(
        SessionImpl_,
        Client_->BeginTransaction(*this, txSettings, settings),
        true,
        GetMinTimeToTouch(Client_->Settings_.SessionPoolSettings_));
}

TAsyncExplainDataQueryResult TSession::ExplainDataQuery(const TString& query,
    const TExplainDataQuerySettings& settings)
{
    return InjectSessionStatusInterception(
        SessionImpl_,
        Client_->ExplainDataQuery(*this, query, settings),
        true,
        GetMinTimeToTouch(Client_->Settings_.SessionPoolSettings_));
}

TAsyncTablePartIterator TSession::ReadTable(const TString& path,
    const TReadTableSettings& settings)
{
    auto promise = NThreading::NewPromise<TTablePartIterator>();
    auto readTableIteratorBuilder = [promise](NThreading::TFuture<std::pair<TPlainStatus, TTableClient::TImpl::TReadTableStreamProcessorPtr>> future) mutable {
        Y_ASSERT(future.HasValue());
        auto pair = future.ExtractValue();
            promise.SetValue(TTablePartIterator(
                pair.second ? std::make_shared<TTablePartIterator::TReaderImpl>(
                pair.second, pair.first.Endpoint) : nullptr, std::move(pair.first))
            );
    };
    Client_->ReadTable(SessionImpl_->GetId(), path, settings).Subscribe(readTableIteratorBuilder);
    return InjectSessionStatusInterception(
        SessionImpl_,
        promise.GetFuture(),
        false,
        GetMinTimeToTouch(Client_->Settings_.SessionPoolSettings_));
}

void TSession::InvalidateQueryCache() {
    SessionImpl_->InvalidateQueryCache();
}

TAsyncStatus TSession::Close(const TCloseSessionSettings& settings) {
    return Client_->Close(SessionImpl_.get(), settings);
}

TAsyncKeepAliveResult TSession::KeepAlive(const TKeepAliveSettings &settings) {
    return InjectSessionStatusInterception(
        SessionImpl_,
        Client_->KeepAlive(SessionImpl_.get(), settings),
        true,
        GetMinTimeToTouch(Client_->Settings_.SessionPoolSettings_));
}

TTableBuilder TSession::GetTableBuilder() {
    return TTableBuilder();
}

TParamsBuilder TSession::GetParamsBuilder() {
    return TParamsBuilder();
}

TTypeBuilder TSession::GetTypeBuilder() {
    return TTypeBuilder();
}

const TString& TSession::GetId() const {
    return SessionImpl_->GetId();
}

////////////////////////////////////////////////////////////////////////////////

TTxControl::TTxControl(const TTransaction& tx)
    : TxId_(tx.GetId())
{}

TTxControl::TTxControl(const TTxSettings& begin)
    : BeginTx_(begin)
{}

////////////////////////////////////////////////////////////////////////////////

TTransaction::TTransaction(const TSession& session, const TString& txId)
    : Session_(session)
    , TxId_(txId)
{}

TAsyncCommitTransactionResult TTransaction::Commit(const TCommitTxSettings& settings) {
    return Session_.Client_->CommitTransaction(Session_, *this, settings);
}

TAsyncStatus TTransaction::Rollback(const TRollbackTxSettings& settings) {
    return Session_.Client_->RollbackTransaction(Session_, *this, settings);
}

////////////////////////////////////////////////////////////////////////////////

TDataQuery::TDataQuery(const TSession& session, const TString& text, const TString& id)
    : Impl_(new TImpl(session, text, session.Client_->Settings_.KeepDataQueryText_, id,
                      session.Client_->Settings_.AllowRequestMigration_))
{}

TDataQuery::TDataQuery(const TSession& session, const TString& text, const TString& id,
    const ::google::protobuf::Map<TString, Ydb::Type>& types)
    : Impl_(new TImpl(session, text, session.Client_->Settings_.KeepDataQueryText_, id,
                      session.Client_->Settings_.AllowRequestMigration_, types))
{}

const TString& TDataQuery::GetId() const {
    return Impl_->GetId();
}

const TMaybe<TString>& TDataQuery::GetText() const {
    return Impl_->GetText();
}

TParamsBuilder TDataQuery::GetParamsBuilder() const {
    return TParamsBuilder(Impl_->ParameterTypes_);
}

TAsyncDataQueryResult TDataQuery::Execute(const TTxControl& txControl,
    const TExecDataQuerySettings& settings)
{
    return Impl_->Session_.Client_->ExecuteDataQuery(Impl_->Session_, *this, txControl, nullptr, settings, false);
}

TAsyncDataQueryResult TDataQuery::Execute(const TTxControl& txControl, TParams&& params,
    const TExecDataQuerySettings& settings)
{
    auto paramsPtr = params.Empty() ? nullptr : params.GetProtoMapPtr();
    return Impl_->Session_.Client_->ExecuteDataQuery(
        Impl_->Session_,
        *this,
        txControl,
        paramsPtr,
        settings,
        false);
}

TAsyncDataQueryResult TDataQuery::Execute(const TTxControl& txControl, const TParams& params,
    const TExecDataQuerySettings& settings)
{
    if (params.Empty()) {
        return Impl_->Session_.Client_->ExecuteDataQuery(
            Impl_->Session_,
            *this,
            txControl,
            nullptr,
            settings,
            false);
    } else {
        using TProtoParamsType = const ::google::protobuf::Map<TString, Ydb::TypedValue>;
        return Impl_->Session_.Client_->ExecuteDataQuery<TProtoParamsType&>(
            Impl_->Session_,
            *this,
            txControl,
            params.GetProtoMap(),
            settings,
            false);
    }
}

////////////////////////////////////////////////////////////////////////////////

TCreateSessionResult::TCreateSessionResult(TStatus&& status, TSession&& session)
    : TStatus(std::move(status))
    , Session_(std::move(session))
{}

TSession TCreateSessionResult::GetSession() const {
    CheckStatusOk("TCreateSessionResult::GetSession");
    return Session_;
}

////////////////////////////////////////////////////////////////////////////////

TKeepAliveResult::TKeepAliveResult(TStatus&& status, ESessionStatus sessionStatus)
    : TStatus(std::move(status))
    , SessionStatus(sessionStatus)
{}

ESessionStatus TKeepAliveResult::GetSessionStatus() const {
    return SessionStatus;
}

////////////////////////////////////////////////////////////////////////////////

TPrepareQueryResult::TPrepareQueryResult(TStatus&& status, const TDataQuery& query, bool fromCache)
    : TStatus(std::move(status))
    , PreparedQuery_(query)
    , FromCache_(fromCache)
{}

TDataQuery TPrepareQueryResult::GetQuery() const {
    CheckStatusOk("TPrepareQueryResult");
    return PreparedQuery_;
}

bool TPrepareQueryResult::IsQueryFromCache() const {
    CheckStatusOk("TPrepareQueryResult");
    return FromCache_;
}

////////////////////////////////////////////////////////////////////////////////

TExplainQueryResult::TExplainQueryResult(TStatus&& status, TString&& plan, TString&& ast)
    : TStatus(std::move(status))
    , Plan_(std::move(plan))
    , Ast_(std::move(ast))
{}

const TString& TExplainQueryResult::GetPlan() const {
    CheckStatusOk("TExplainQueryResult::GetPlan");
    return Plan_;
}

const TString& TExplainQueryResult::GetAst() const {
    CheckStatusOk("TExplainQueryResult::GetAst");
    return Ast_;
}

////////////////////////////////////////////////////////////////////////////////

TDescribeTableResult::TDescribeTableResult(TStatus&& status, Ydb::Table::DescribeTableResult&& desc,
    const TDescribeTableSettings& describeSettings)
    : NScheme::TDescribePathResult(std::move(status), desc.self())
    , TableDescription_(std::move(desc), describeSettings)
{}

TTableDescription TDescribeTableResult::GetTableDescription() const {
    CheckStatusOk("TDescribeTableResult::GetTableDescription");
    return TableDescription_;
}

////////////////////////////////////////////////////////////////////////////////

TDataQueryResult::TDataQueryResult(TStatus&& status, TVector<TResultSet>&& resultSets,
    const TMaybe<TTransaction>& transaction, const TMaybe<TDataQuery>& dataQuery, bool fromCache, const TMaybe<TQueryStats> &queryStats)
    : TStatus(std::move(status))
    , Transaction_(transaction)
    , ResultSets_(std::move(resultSets))
    , DataQuery_(dataQuery)
    , FromCache_(fromCache)
    , QueryStats_(queryStats)
{}

const TVector<TResultSet>& TDataQueryResult::GetResultSets() const {
    return ResultSets_;
}

TResultSet TDataQueryResult::GetResultSet(size_t resultIndex) const {
    if (resultIndex >= ResultSets_.size()) {
        RaiseError(TString("Requested index out of range\n"));
    }

    return ResultSets_[resultIndex];
}

TResultSetParser TDataQueryResult::GetResultSetParser(size_t resultIndex) const {
    return TResultSetParser(GetResultSet(resultIndex));
}

TMaybe<TTransaction> TDataQueryResult::GetTransaction() const {
    return Transaction_;
}

TMaybe<TDataQuery> TDataQueryResult::GetQuery() const {
    return DataQuery_;
}

bool TDataQueryResult::IsQueryFromCache() const {
    return FromCache_;
}

const TMaybe<TQueryStats>& TDataQueryResult::GetStats() const {
    return QueryStats_;
}

const TString TDataQueryResult::GetQueryPlan() const {
    if (QueryStats_.Defined()) {
        return NYdb::TProtoAccessor::GetProto(*QueryStats_.Get()).query_plan();
    } else {
        return "";
    }
}

////////////////////////////////////////////////////////////////////////////////

TBeginTransactionResult::TBeginTransactionResult(TStatus&& status, TTransaction transaction)
    : TStatus(std::move(status))
    , Transaction_(transaction)
{}

const TTransaction& TBeginTransactionResult::GetTransaction() const {
    CheckStatusOk("TDataQueryResult::GetTransaction");
    return Transaction_;
}

////////////////////////////////////////////////////////////////////////////////

TCommitTransactionResult::TCommitTransactionResult(TStatus&& status, const TMaybe<TQueryStats>& queryStats)
    : TStatus(std::move(status))
    , QueryStats_(queryStats)
{}

const TMaybe<TQueryStats>& TCommitTransactionResult::GetStats() const {
    return QueryStats_;
}

////////////////////////////////////////////////////////////////////////////////

std::function<void(TSession::TImpl*)> TSession::TImpl::GetSmartDeleter(std::shared_ptr<TTableClient::TImpl> client) {
    return [client](TSession::TImpl* sessionImpl) {
        switch (sessionImpl->GetState()) {
            case TSession::TImpl::S_STANDALONE:
            case TSession::TImpl::S_BROKEN:
            case TSession::TImpl::S_CLOSING:
                client->DeleteSession(sessionImpl);
            break;
            case TSession::TImpl::S_IDLE:
            case TSession::TImpl::S_ACTIVE: {
                if (!client->ReturnSession(sessionImpl)) {
                    client->DeleteSession(sessionImpl);
                }
                break;
            }
            default:
            break;
        }
    };
}

////////////////////////////////////////////////////////////////////////////////

TCopyItem::TCopyItem(const TString& source, const TString& destination)
    : Source_(source)
    , Destination_(destination)
    , OmitIndexes_(false) {
}

const TString& TCopyItem::SourcePath() const {
    return Source_;
}

const TString& TCopyItem::DestinationPath() const {
    return Destination_;
}

TCopyItem& TCopyItem::SetOmitIndexes() {
    OmitIndexes_ = true;
    return *this;
}

bool TCopyItem::OmitIndexes() const {
    return OmitIndexes_;
}

////////////////////////////////////////////////////////////////////////////////

TRenameItem::TRenameItem(const TString& source, const TString& destination)
    : Source_(source)
    , Destination_(destination)
    , ReplaceDestination_(false) {
}

const TString& TRenameItem::SourcePath() const {
    return Source_;
}

const TString& TRenameItem::DestinationPath() const {
    return Destination_;
}

TRenameItem& TRenameItem::SetReplaceDestination() {
    ReplaceDestination_ = true;
    return *this;
}

bool TRenameItem::ReplaceDestination() const {
    return ReplaceDestination_;
}

////////////////////////////////////////////////////////////////////////////////

TIndexDescription::TIndexDescription(const TString& name, EIndexType type,
        const TVector<TString>& indexColumns, const TVector<TString>& dataColumns)
    : IndexName_(name)
    , IndexType_(type)
    , IndexColumns_(indexColumns)
    , DataColumns_(dataColumns)
{}

TIndexDescription::TIndexDescription(const TString& name, const TVector<TString>& indexColumns, const TVector<TString>& dataColumns)
    : TIndexDescription(name, EIndexType::GlobalSync, indexColumns, dataColumns)
{}

TIndexDescription::TIndexDescription(const Ydb::Table::TableIndex& tableIndex)
    : TIndexDescription(FromProto(tableIndex))
{}

TIndexDescription::TIndexDescription(const Ydb::Table::TableIndexDescription& tableIndexDesc)
    : TIndexDescription(FromProto(tableIndexDesc))
{}

const TString& TIndexDescription::GetIndexName() const {
    return IndexName_;
}

EIndexType TIndexDescription::GetIndexType() const {
    return IndexType_;
}

const TVector<TString>& TIndexDescription::GetIndexColumns() const {
    return IndexColumns_;
}

const TVector<TString>& TIndexDescription::GetDataColumns() const {
    return DataColumns_;
}

ui64 TIndexDescription::GetSizeBytes() const {
    return SizeBytes;
}

template <typename TProto>
TIndexDescription TIndexDescription::FromProto(const TProto& proto) {
    EIndexType type;
    TVector<TString> indexColumns;
    TVector<TString> dataColumns;

    indexColumns.assign(proto.index_columns().begin(), proto.index_columns().end());
    dataColumns.assign(proto.data_columns().begin(), proto.data_columns().end());

    switch (proto.type_case()) {
    case TProto::kGlobalIndex:
        type = EIndexType::GlobalSync;
        break;
    case TProto::kGlobalAsyncIndex:
        type = EIndexType::GlobalAsync;
        break;
    default: // fallback to global sync
        type = EIndexType::GlobalSync;
        break;
    }

    auto result = TIndexDescription(proto.name(), type, indexColumns, dataColumns);
    if constexpr (std::is_same_v<TProto, Ydb::Table::TableIndexDescription>) {
        result.SizeBytes = proto.size_bytes();
    }

    return result;
}

void TIndexDescription::SerializeTo(Ydb::Table::TableIndex& proto) const {
    proto.set_name(IndexName_);
    for (const auto& indexCol : IndexColumns_) {
        proto.add_index_columns(indexCol);
    }

    *proto.mutable_data_columns() = {DataColumns_.begin(), DataColumns_.end()};

    switch (IndexType_) {
    case EIndexType::GlobalSync:
        *proto.mutable_global_index() = Ydb::Table::GlobalIndex();
        break;
    case EIndexType::GlobalAsync:
        *proto.mutable_global_async_index() = Ydb::Table::GlobalAsyncIndex();
        break;
    case EIndexType::Unknown:
        break;
    }
}

TString TIndexDescription::ToString() const {
    TString result;
    TStringOutput out(result);
    Out(out);
    return result;
}

void TIndexDescription::Out(IOutputStream& o) const {
    o << "{ name: \"" << IndexName_ << "\"";
    o << ", type: " << IndexType_ << "";
    o << ", index_columns: [" << JoinSeq(", ", IndexColumns_) << "]";

    if (DataColumns_) {
        o << ", data_columns: [" << JoinSeq(", ", DataColumns_) << "]";
    }

    o << " }";
}

bool operator==(const TIndexDescription& lhs, const TIndexDescription& rhs) {
    return lhs.GetIndexName() == rhs.GetIndexName()
        && lhs.GetIndexType() == rhs.GetIndexType()
        && lhs.GetIndexColumns() == rhs.GetIndexColumns()
        && lhs.GetDataColumns() == rhs.GetDataColumns();
}

bool operator!=(const TIndexDescription& lhs, const TIndexDescription& rhs) {
    return !(lhs == rhs);
}

////////////////////////////////////////////////////////////////////////////////

TChangefeedDescription::TChangefeedDescription(const TString& name, EChangefeedMode mode, EChangefeedFormat format)
    : Name_(name)
    , Mode_(mode)
    , Format_(format)
{}

TChangefeedDescription::TChangefeedDescription(const Ydb::Table::Changefeed& proto)
    : TChangefeedDescription(FromProto(proto))
{}

TChangefeedDescription::TChangefeedDescription(const Ydb::Table::ChangefeedDescription& proto)
    : TChangefeedDescription(FromProto(proto))
{}

TChangefeedDescription& TChangefeedDescription::WithVirtualTimestamps() {
    VirtualTimestamps_ = true;
    return *this;
}

TChangefeedDescription& TChangefeedDescription::WithResolvedTimestamps(const TDuration& value) {
    ResolvedTimestamps_ = value;
    return *this;
}

TChangefeedDescription& TChangefeedDescription::WithRetentionPeriod(const TDuration& value) {
    RetentionPeriod_ = value;
    return *this;
}

TChangefeedDescription& TChangefeedDescription::WithInitialScan() {
    InitialScan_ = true;
    return *this;
}

TChangefeedDescription& TChangefeedDescription::AddAttribute(const TString& key, const TString& value) {
    Attributes_[key] = value;
    return *this;
}

TChangefeedDescription& TChangefeedDescription::SetAttributes(const THashMap<TString, TString>& attrs) {
    Attributes_ = attrs;
    return *this;
}

TChangefeedDescription& TChangefeedDescription::SetAttributes(THashMap<TString, TString>&& attrs) {
    Attributes_ = std::move(attrs);
    return *this;
}

TChangefeedDescription& TChangefeedDescription::WithAwsRegion(const TString& value) {
    AwsRegion_ = value;
    return *this;
}

const TString& TChangefeedDescription::GetName() const {
    return Name_;
}

EChangefeedMode TChangefeedDescription::GetMode() const {
    return Mode_;
}

EChangefeedFormat TChangefeedDescription::GetFormat() const {
    return Format_;
}

EChangefeedState TChangefeedDescription::GetState() const {
    return State_;
}

bool TChangefeedDescription::GetVirtualTimestamps() const {
    return VirtualTimestamps_;
}

const std::optional<TDuration>& TChangefeedDescription::GetResolvedTimestamps() const {
    return ResolvedTimestamps_;
}

bool TChangefeedDescription::GetInitialScan() const {
    return InitialScan_;
}

const THashMap<TString, TString>& TChangefeedDescription::GetAttributes() const {
    return Attributes_;
}

const TString& TChangefeedDescription::GetAwsRegion() const {
    return AwsRegion_;
}

template <typename TProto>
TChangefeedDescription TChangefeedDescription::FromProto(const TProto& proto) {
    EChangefeedMode mode;
    switch (proto.mode()) {
    case Ydb::Table::ChangefeedMode::MODE_KEYS_ONLY:
        mode = EChangefeedMode::KeysOnly;
        break;
    case Ydb::Table::ChangefeedMode::MODE_UPDATES:
        mode = EChangefeedMode::Updates;
        break;
    case Ydb::Table::ChangefeedMode::MODE_NEW_IMAGE:
        mode = EChangefeedMode::NewImage;
        break;
    case Ydb::Table::ChangefeedMode::MODE_OLD_IMAGE:
        mode = EChangefeedMode::OldImage;
        break;
    case Ydb::Table::ChangefeedMode::MODE_NEW_AND_OLD_IMAGES:
        mode = EChangefeedMode::NewAndOldImages;
        break;
    default:
        mode = EChangefeedMode::Unknown;
        break;
    }

    EChangefeedFormat format;
    switch (proto.format()) {
    case Ydb::Table::ChangefeedFormat::FORMAT_JSON:
        format = EChangefeedFormat::Json;
        break;
    case Ydb::Table::ChangefeedFormat::FORMAT_DYNAMODB_STREAMS_JSON:
        format = EChangefeedFormat::DynamoDBStreamsJson;
        break;
    default:
        format = EChangefeedFormat::Unknown;
        break;
    }

    auto ret = TChangefeedDescription(proto.name(), mode, format);
    if (proto.virtual_timestamps()) {
        ret.WithVirtualTimestamps();
    }
    if (proto.has_resolved_timestamps_interval()) {
        ret.WithResolvedTimestamps(TDuration::MilliSeconds(
            ::google::protobuf::util::TimeUtil::DurationToMilliseconds(proto.resolved_timestamps_interval())));
    }
    if (!proto.aws_region().empty()) {
        ret.WithAwsRegion(proto.aws_region());
    }

    if constexpr (std::is_same_v<TProto, Ydb::Table::ChangefeedDescription>) {
        switch (proto.state()) {
        case Ydb::Table::ChangefeedDescription::STATE_ENABLED:
            ret.State_= EChangefeedState::Enabled;
            break;
        case Ydb::Table::ChangefeedDescription::STATE_DISABLED:
            ret.State_ = EChangefeedState::Disabled;
            break;
        case Ydb::Table::ChangefeedDescription::STATE_INITIAL_SCAN:
            ret.State_ = EChangefeedState::InitialScan;
            break;
        default:
            ret.State_ = EChangefeedState::Unknown;
            break;
        }
    }

    for (const auto& [key, value] : proto.attributes()) {
        ret.Attributes_[key] = value;
    }

    return ret;
}

void TChangefeedDescription::SerializeTo(Ydb::Table::Changefeed& proto) const {
    proto.set_name(Name_);
    proto.set_virtual_timestamps(VirtualTimestamps_);
    proto.set_initial_scan(InitialScan_);
    proto.set_aws_region(AwsRegion_);

    switch (Mode_) {
    case EChangefeedMode::KeysOnly:
        proto.set_mode(Ydb::Table::ChangefeedMode::MODE_KEYS_ONLY);
        break;
    case EChangefeedMode::Updates:
        proto.set_mode(Ydb::Table::ChangefeedMode::MODE_UPDATES);
        break;
    case EChangefeedMode::NewImage:
        proto.set_mode(Ydb::Table::ChangefeedMode::MODE_NEW_IMAGE);
        break;
    case EChangefeedMode::OldImage:
        proto.set_mode(Ydb::Table::ChangefeedMode::MODE_OLD_IMAGE);
        break;
    case EChangefeedMode::NewAndOldImages:
        proto.set_mode(Ydb::Table::ChangefeedMode::MODE_NEW_AND_OLD_IMAGES);
        break;
    case EChangefeedMode::Unknown:
        break;
    }

    switch (Format_) {
    case EChangefeedFormat::Json:
        proto.set_format(Ydb::Table::ChangefeedFormat::FORMAT_JSON);
        break;
    case EChangefeedFormat::DynamoDBStreamsJson:
        proto.set_format(Ydb::Table::ChangefeedFormat::FORMAT_DYNAMODB_STREAMS_JSON);
        break;
    case EChangefeedFormat::Unknown:
        break;
    }

    if (ResolvedTimestamps_) {
        SetDuration(*ResolvedTimestamps_, *proto.mutable_resolved_timestamps_interval());
    }

    if (RetentionPeriod_) {
        SetDuration(*RetentionPeriod_, *proto.mutable_retention_period());
    }

    for (const auto& [key, value] : Attributes_) {
        (*proto.mutable_attributes())[key] = value;
    }
}

TString TChangefeedDescription::ToString() const {
    TString result;
    TStringOutput out(result);
    Out(out);
    return result;
}

void TChangefeedDescription::Out(IOutputStream& o) const {
    o << "{ name: \"" << Name_ << "\""
      << ", mode: " << Mode_ << ""
      << ", format: " << Format_ << ""
      << ", virtual_timestamps: " << (VirtualTimestamps_ ? "on": "off") << "";

    if (ResolvedTimestamps_) {
        o << ", resolved_timestamps: " << *ResolvedTimestamps_;
    }

    if (RetentionPeriod_) {
        o << ", retention_period: " << *RetentionPeriod_;
    }

    if (AwsRegion_) {
        o << ", aws_region: " << AwsRegion_;
    }

    o << " }";
}

bool operator==(const TChangefeedDescription& lhs, const TChangefeedDescription& rhs) {
    return lhs.GetName() == rhs.GetName()
        && lhs.GetMode() == rhs.GetMode()
        && lhs.GetFormat() == rhs.GetFormat()
        && lhs.GetVirtualTimestamps() == rhs.GetVirtualTimestamps()
        && lhs.GetResolvedTimestamps() == rhs.GetResolvedTimestamps()
        && lhs.GetAwsRegion() == rhs.GetAwsRegion();
}

bool operator!=(const TChangefeedDescription& lhs, const TChangefeedDescription& rhs) {
    return !(lhs == rhs);
}

////////////////////////////////////////////////////////////////////////////////

TDateTypeColumnModeSettings::TDateTypeColumnModeSettings(const TString& columnName, const TDuration& expireAfter)
    : ColumnName_(columnName)
    , ExpireAfter_(expireAfter)
{}

void TDateTypeColumnModeSettings::SerializeTo(Ydb::Table::DateTypeColumnModeSettings& proto) const {
    proto.set_column_name(ColumnName_);
    proto.set_expire_after_seconds(ExpireAfter_.Seconds());
}

const TString& TDateTypeColumnModeSettings::GetColumnName() const {
    return ColumnName_;
}

const TDuration& TDateTypeColumnModeSettings::GetExpireAfter() const {
    return ExpireAfter_;
}

TValueSinceUnixEpochModeSettings::TValueSinceUnixEpochModeSettings(const TString& columnName, EUnit columnUnit, const TDuration& expireAfter)
    : ColumnName_(columnName)
    , ColumnUnit_(columnUnit)
    , ExpireAfter_(expireAfter)
{}

void TValueSinceUnixEpochModeSettings::SerializeTo(Ydb::Table::ValueSinceUnixEpochModeSettings& proto) const {
    proto.set_column_name(ColumnName_);
    proto.set_column_unit(TProtoAccessor::GetProto(ColumnUnit_));
    proto.set_expire_after_seconds(ExpireAfter_.Seconds());
}

const TString& TValueSinceUnixEpochModeSettings::GetColumnName() const {
    return ColumnName_;
}

TValueSinceUnixEpochModeSettings::EUnit TValueSinceUnixEpochModeSettings::GetColumnUnit() const {
    return ColumnUnit_;
}

const TDuration& TValueSinceUnixEpochModeSettings::GetExpireAfter() const {
    return ExpireAfter_;
}

void TValueSinceUnixEpochModeSettings::Out(IOutputStream& out, EUnit unit) {
#define PRINT_UNIT(x) \
    case EUnit::x: \
        out << #x; \
        break

    switch (unit) {
    PRINT_UNIT(Seconds);
    PRINT_UNIT(MilliSeconds);
    PRINT_UNIT(MicroSeconds);
    PRINT_UNIT(NanoSeconds);
    PRINT_UNIT(Unknown);
    }

#undef PRINT_UNIT
}

TString TValueSinceUnixEpochModeSettings::ToString(EUnit unit) {
    TString result;
    TStringOutput out(result);
    Out(out, unit);
    return result;
}

TValueSinceUnixEpochModeSettings::EUnit TValueSinceUnixEpochModeSettings::UnitFromString(const TString& value) {
    const auto norm = to_lower(value);

    if (norm == "s" || norm == "sec" || norm == "seconds") {
        return EUnit::Seconds;
    } else if (norm == "ms" || norm == "msec" || norm == "milliseconds") {
        return EUnit::MilliSeconds;
    } else if (norm == "us" || norm == "usec" || norm == "microseconds") {
        return EUnit::MicroSeconds;
    } else if (norm == "ns" || norm == "nsec" || norm == "nanoseconds") {
        return EUnit::NanoSeconds;
    }

    return EUnit::Unknown;
}

TTtlSettings::TTtlSettings(const TString& columnName, const TDuration& expireAfter)
    : Mode_(TDateTypeColumnModeSettings(columnName, expireAfter))
{}

TTtlSettings::TTtlSettings(const Ydb::Table::DateTypeColumnModeSettings& mode, ui32 runIntervalSeconds)
    : TTtlSettings(mode.column_name(), TDuration::Seconds(mode.expire_after_seconds()))
{
    RunInterval_ = TDuration::Seconds(runIntervalSeconds);
}

const TDateTypeColumnModeSettings& TTtlSettings::GetDateTypeColumn() const {
    return std::get<TDateTypeColumnModeSettings>(Mode_);
}

TTtlSettings::TTtlSettings(const TString& columnName, EUnit columnUnit, const TDuration& expireAfter)
    : Mode_(TValueSinceUnixEpochModeSettings(columnName, columnUnit, expireAfter))
{}

TTtlSettings::TTtlSettings(const Ydb::Table::ValueSinceUnixEpochModeSettings& mode, ui32 runIntervalSeconds)
    : TTtlSettings(mode.column_name(), TProtoAccessor::FromProto(mode.column_unit()), TDuration::Seconds(mode.expire_after_seconds()))
{
    RunInterval_ = TDuration::Seconds(runIntervalSeconds);
}

const TValueSinceUnixEpochModeSettings& TTtlSettings::GetValueSinceUnixEpoch() const {
    return std::get<TValueSinceUnixEpochModeSettings>(Mode_);
}

void TTtlSettings::SerializeTo(Ydb::Table::TtlSettings& proto) const {
    switch (GetMode()) {
    case EMode::DateTypeColumn:
        GetDateTypeColumn().SerializeTo(*proto.mutable_date_type_column());
        break;
    case EMode::ValueSinceUnixEpoch:
        GetValueSinceUnixEpoch().SerializeTo(*proto.mutable_value_since_unix_epoch());
        break;
    }

    if (RunInterval_) {
        proto.set_run_interval_seconds(RunInterval_.Seconds());
    }
}

TTtlSettings::EMode TTtlSettings::GetMode() const {
    return static_cast<EMode>(Mode_.index());
}

TTtlSettings& TTtlSettings::SetRunInterval(const TDuration& value) {
    RunInterval_ = value;
    return *this;
}

const TDuration& TTtlSettings::GetRunInterval() const {
    return RunInterval_;
}

TAlterTtlSettings::EAction TAlterTtlSettings::GetAction() const {
    return static_cast<EAction>(Action_.index());
}

const TTtlSettings& TAlterTtlSettings::GetTtlSettings() const {
    return std::get<TTtlSettings>(Action_);
}

class TAlterTtlSettingsBuilder::TImpl {
    using EUnit = TValueSinceUnixEpochModeSettings::EUnit;

public:
    TImpl() { }

    void Drop() {
        AlterTtlSettings_ = TAlterTtlSettings::Drop();
    }

    void Set(TTtlSettings&& settings) {
        AlterTtlSettings_ = TAlterTtlSettings::Set(std::move(settings));
    }

    void Set(const TTtlSettings& settings) {
        AlterTtlSettings_ = TAlterTtlSettings::Set(settings);
    }

    const TMaybe<TAlterTtlSettings>& GetAlterTtlSettings() const {
        return AlterTtlSettings_;
    }

private:
    TMaybe<TAlterTtlSettings> AlterTtlSettings_;
};

TAlterTtlSettingsBuilder::TAlterTtlSettingsBuilder(TAlterTableSettings& parent)
    : Parent_(parent)
    , Impl_(std::make_shared<TImpl>())
{ }

TAlterTtlSettingsBuilder& TAlterTtlSettingsBuilder::Drop() {
    Impl_->Drop();
    return *this;
}

TAlterTtlSettingsBuilder& TAlterTtlSettingsBuilder::Set(TTtlSettings&& settings) {
    Impl_->Set(std::move(settings));
    return *this;
}

TAlterTtlSettingsBuilder& TAlterTtlSettingsBuilder::Set(const TTtlSettings& settings) {
    Impl_->Set(settings);
    return *this;
}

TAlterTtlSettingsBuilder& TAlterTtlSettingsBuilder::Set(const TString& columnName, const TDuration& expireAfter) {
    return Set(TTtlSettings(columnName, expireAfter));
}

TAlterTtlSettingsBuilder& TAlterTtlSettingsBuilder::Set(const TString& columnName, EUnit columnUnit, const TDuration& expireAfter) {
    return Set(TTtlSettings(columnName, columnUnit, expireAfter));
}

TAlterTableSettings& TAlterTtlSettingsBuilder::EndAlterTtlSettings() {
    return Parent_.AlterTtlSettings(Impl_->GetAlterTtlSettings());
}

class TAlterTableSettings::TImpl {
public:
    TImpl() { }

    void SetAlterTtlSettings(const TMaybe<TAlterTtlSettings>& value) {
        AlterTtlSettings_ = value;
    }

    const TMaybe<TAlterTtlSettings>& GetAlterTtlSettings() const {
        return AlterTtlSettings_;
    }

private:
    TMaybe<TAlterTtlSettings> AlterTtlSettings_;
};

TAlterTableSettings::TAlterTableSettings()
    : Impl_(std::make_shared<TImpl>())
{ }

TAlterTableSettings& TAlterTableSettings::AlterTtlSettings(const TMaybe<TAlterTtlSettings>& value) {
    Impl_->SetAlterTtlSettings(value);
    return *this;
}

const TMaybe<TAlterTtlSettings>& TAlterTableSettings::GetAlterTtlSettings() const {
    return Impl_->GetAlterTtlSettings();
}

////////////////////////////////////////////////////////////////////////////////

TReadReplicasSettings::TReadReplicasSettings(EMode mode, ui64 readReplicasCount)
    : Mode_(mode)
    , ReadReplicasCount_(readReplicasCount)
{}

TReadReplicasSettings::EMode TReadReplicasSettings::GetMode() const {
    return Mode_;
}

ui64 TReadReplicasSettings::GetReadReplicasCount() const {
    return ReadReplicasCount_;
}

////////////////////////////////////////////////////////////////////////////////

TBulkUpsertResult::TBulkUpsertResult(TStatus&& status)
    : TStatus(std::move(status))
{}

TReadRowsResult::TReadRowsResult(TStatus&& status, TResultSet&& resultSet)
    : TStatus(std::move(status))
    , ResultSet(std::move(resultSet))
{}

} // namespace NTable
} // namespace NYdb
