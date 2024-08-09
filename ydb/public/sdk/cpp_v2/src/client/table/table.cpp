#include <ydb-cpp-sdk/client/table/table.h>

#define INCLUDE_YDB_INTERNAL_H
#include <src/client/impl/ydb_internal/scheme_helpers/helpers.h>
#include <src/client/impl/ydb_internal/table_helpers/helpers.h>
#include <src/client/impl/ydb_internal/make_request/make.h>
#include <src/client/impl/ydb_internal/retry/retry.h>
#include <src/client/impl/ydb_internal/retry/retry_async.h>
#include <src/client/impl/ydb_internal/retry/retry_sync.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <src/client/impl/ydb_stats/stats.h>
#include <ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb-cpp-sdk/client/value/value.h>
#include <src/client/table/impl/client_session.h>
#include <src/client/table/impl/data_query.h>
#include <src/client/table/impl/request_migrator.h>
#include <src/client/table/impl/table_client.h>
#include <ydb-cpp-sdk/client/resources/ydb_resources.h>

#include <google/protobuf/util/time_util.h>

#include <library/cpp/cache/cache.h>
#include <ydb-cpp-sdk/library/string_utils/misc/misc.h>

#include <util/random/random.h>
#include <util/string/join.h>

#include <map>

namespace NYdb {
namespace NTable {

using namespace NThreading;
using namespace NSessionPool;

using TRetryContextAsync = NRetry::Async::TRetryContext<TTableClient, TAsyncStatus>;

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

std::optional<std::string> TStorageSettings::GetTabletCommitLog0() const {
    if (GetProto().has_tablet_commit_log0()) {
        return GetProto().tablet_commit_log0().media();
    } else {
        return { };
    }
}

std::optional<std::string> TStorageSettings::GetTabletCommitLog1() const {
    if (GetProto().has_tablet_commit_log1()) {
        return GetProto().tablet_commit_log1().media();
    } else {
        return { };
    }
}

std::optional<std::string> TStorageSettings::GetExternal() const {
    if (GetProto().has_external()) {
        return GetProto().external().media();
    } else {
        return { };
    }
}

std::optional<bool> TStorageSettings::GetStoreExternalBlobs() const {
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

const std::string& TColumnFamilyDescription::GetName() const {
    return GetProto().name();
}

std::optional<std::string> TColumnFamilyDescription::GetData() const {
    if (GetProto().has_data()) {
        return GetProto().data().media();
    } else {
        return { };
    }
}

std::optional<EColumnFamilyCompression> TColumnFamilyDescription::GetCompression() const {
    switch (GetProto().compression()) {
        case Ydb::Table::ColumnFamily::COMPRESSION_NONE:
            return EColumnFamilyCompression::None;
        case Ydb::Table::ColumnFamily::COMPRESSION_LZ4:
            return EColumnFamilyCompression::LZ4;
        default:
            return { };
    }
}

std::optional<bool> TColumnFamilyDescription::GetKeepInMemory() const {
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

std::optional<bool> TPartitioningSettings::GetPartitioningBySize() const {
    switch (GetProto().partitioning_by_size()) {
    case Ydb::FeatureFlag::ENABLED:
        return true;
    case Ydb::FeatureFlag::DISABLED:
        return false;
    default:
        return { };
    }
}

std::optional<bool> TPartitioningSettings::GetPartitioningByLoad() const {
    switch (GetProto().partitioning_by_load()) {
    case Ydb::FeatureFlag::ENABLED:
        return true;
    case Ydb::FeatureFlag::DISABLED:
        return false;
    default:
        return { };
    }
}

uint64_t TPartitioningSettings::GetPartitionSizeMb() const {
    return GetProto().partition_size_mb();
}

uint64_t TPartitioningSettings::GetMinPartitionsCount() const {
    return GetProto().min_partitions_count();
}

uint64_t TPartitioningSettings::GetMaxPartitionsCount() const {
    return GetProto().max_partitions_count();
}

////////////////////////////////////////////////////////////////////////////////

struct TTableStats {
    uint64_t Rows = 0;
    uint64_t Size = 0;
    uint64_t Partitions = 0;
    TInstant ModificationTime;
    TInstant CreationTime;
};

static TInstant ProtobufTimestampToTInstant(const google::protobuf::Timestamp& timestamp) {
    uint64_t lastModificationUs = timestamp.seconds() * 1000000;
    lastModificationUs += timestamp.nanos() / 1000;
    return TInstant::MicroSeconds(lastModificationUs);
}

static void SerializeTo(const TRenameIndex& rename, Ydb::Table::RenameIndexItem& proto) {
    proto.set_source_name(TStringType{rename.SourceName_});
    proto.set_destination_name(TStringType{rename.DestinationName_});
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
            std::optional<bool> not_null;
            if (col.has_not_null()) {
                not_null = col.not_null();
            }
            Columns_.emplace_back(col.name(), col.type(), col.family(), not_null);
        }

        // indexes
        Indexes_.reserve(proto.indexes_size());
        for (const auto& index : proto.indexes()) {
            Indexes_.emplace_back(TProtoAccessor::FromProto(index));
        }

        if constexpr (std::is_same_v<TProto, Ydb::Table::DescribeTableResult>) {
            // changefeeds
            Changefeeds_.reserve(proto.changefeeds_size());
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

        if (proto.store_type()) {
            StoreType_ = (proto.store_type() == Ydb::Table::STORE_TYPE_COLUMN) ? EStoreType::Column : EStoreType::Row;
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

        std::optional<TValue> leftValue;
        for (const auto& bound : Proto_.shard_key_bounds()) {
            std::optional<TKeyBound> fromBound = leftValue
                ? TKeyBound::Inclusive(*leftValue)
                : std::optional<TKeyBound>();

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
                leftValue ? TKeyBound::Inclusive(*leftValue) : std::optional<TKeyBound>(),
                std::optional<TKeyBound>()));
        }
    }

    struct TCreateTableRequestTag {}; // to avoid delegation cycle

    TImpl(const Ydb::Table::CreateTableRequest& request, TCreateTableRequestTag)
        : TImpl(request)
    {
        if (!request.compaction_policy().empty()) {
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

    void AddColumn(const std::string& name, const Ydb::Type& type, const std::string& family, std::optional<bool> notNull) {
        Columns_.emplace_back(name, type, family, notNull);
    }

    void SetPrimaryKeyColumns(const std::vector<std::string>& primaryKeyColumns) {
        PrimaryKey_ = primaryKeyColumns;
    }

    void AddSecondaryIndex(const std::string& indexName, EIndexType type, const std::vector<std::string>& indexColumns) {
        Indexes_.emplace_back(TIndexDescription(indexName, type, indexColumns));
    }

    void AddSecondaryIndex(const std::string& indexName, EIndexType type, const std::vector<std::string>& indexColumns, const std::vector<std::string>& dataColumns) {
        Indexes_.emplace_back(TIndexDescription(indexName, type, indexColumns, dataColumns));
    }

    void AddChangefeed(const std::string& name, EChangefeedMode mode, EChangefeedFormat format) {
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

    void AddAttribute(const std::string& key, const std::string& value) {
        Attributes_[key] = value;
    }

    void SetAttributes(const std::unordered_map<std::string, std::string>& attrs) {
        Attributes_ = attrs;
    }

    void SetAttributes(std::unordered_map<std::string, std::string>&& attrs) {
        Attributes_ = std::move(attrs);
    }

    void SetCompactionPolicy(const std::string& name) {
        CompactionPolicy_ = name;
    }

    void SetUniformPartitions(uint64_t partitionsCount) {
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

    void SetReadReplicasSettings(TReadReplicasSettings::EMode mode, uint64_t readReplicasCount) {
        ReadReplicasSettings_ = TReadReplicasSettings(mode, readReplicasCount);
    }

    void SetStoreType(EStoreType type) {
        StoreType_ = type;
    }

    const std::vector<std::string>& GetPrimaryKeyColumns() const {
        return PrimaryKey_;
    }

    const std::vector<TTableColumn>& GetColumns() const {
        return Columns_;
    }

    const std::vector<TIndexDescription>& GetIndexDescriptions() const {
        return Indexes_;
    }

    const std::vector<TChangefeedDescription>& GetChangefeedDescriptions() const {
        return Changefeeds_;
    }

    const std::optional<TTtlSettings>& GetTtlSettings() const {
        return TtlSettings_;
    }

    const std::optional<std::string>& GetTiering() const {
        return Tiering_;
    }

    EStoreType GetStoreType() const {
        return StoreType_;
    }

    const std::string& GetOwner() const {
        return Owner_;
    }

    const std::vector<NScheme::TPermissions>& GetPermissions() const {
        return Permissions_;
    }

    const std::vector<NScheme::TPermissions>& GetEffectivePermissions() const {
        return EffectivePermissions_;
    }

    const std::vector<TKeyRange>& GetKeyRanges() const {
        return Ranges_;
    }

    const std::vector<TPartitionStats>& GetPartitionStats() const {
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

    const std::vector<TColumnFamilyDescription>& GetColumnFamilies() const {
        return ColumnFamilies_;
    }

    const std::unordered_map<std::string, std::string>& GetAttributes() const {
        return Attributes_;
    }

    const std::string& GetCompactionPolicy() const {
        return CompactionPolicy_;
    }

    const std::optional<uint64_t>& GetUniformPartitions() const {
        return UniformPartitions_;
    }

    const std::optional<TExplicitPartitions>& GetPartitionAtKeys() const {
        return PartitionAtKeys_;
    }

    bool HasPartitioningSettings() const {
        return HasPartitioningSettings_;
    }

    const TPartitioningSettings& GetPartitioningSettings() const {
        return PartitioningSettings_;
    }

    std::optional<bool> GetKeyBloomFilter() const {
        return KeyBloomFilter_;
    }

    const std::optional<TReadReplicasSettings>& GetReadReplicasSettings() const {
        return ReadReplicasSettings_;
    }

private:
    Ydb::Table::DescribeTableResult Proto_;
    TStorageSettings StorageSettings_;
    std::vector<std::string> PrimaryKey_;
    std::vector<TTableColumn> Columns_;
    std::vector<TIndexDescription> Indexes_;
    std::vector<TChangefeedDescription> Changefeeds_;
    std::optional<TTtlSettings> TtlSettings_;
    std::optional<std::string> Tiering_;
    std::string Owner_;
    std::vector<NScheme::TPermissions> Permissions_;
    std::vector<NScheme::TPermissions> EffectivePermissions_;
    std::vector<TKeyRange> Ranges_;
    std::vector<TPartitionStats> PartitionStats_;
    TTableStats TableStats;
    std::vector<TColumnFamilyDescription> ColumnFamilies_;
    std::unordered_map<std::string, std::string> Attributes_;
    std::string CompactionPolicy_;
    std::optional<uint64_t> UniformPartitions_;
    std::optional<TExplicitPartitions> PartitionAtKeys_;
    TPartitioningSettings PartitioningSettings_;
    std::optional<bool> KeyBloomFilter_;
    std::optional<TReadReplicasSettings> ReadReplicasSettings_;
    bool HasStorageSettings_ = false;
    bool HasPartitioningSettings_ = false;
    EStoreType StoreType_ = EStoreType::Row;
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

const std::vector<std::string>& TTableDescription::GetPrimaryKeyColumns() const {
    return Impl_->GetPrimaryKeyColumns();
}

std::vector<TColumn> TTableDescription::GetColumns() const {
    // Conversion to TColumn for API compatibility
    const auto& columns = Impl_->GetColumns();
    std::vector<TColumn> legacy;
    legacy.reserve(columns.size());
    for (const auto& column : columns) {
        legacy.emplace_back(column.Name, column.Type);
    }
    return legacy;
}

std::vector<TTableColumn> TTableDescription::GetTableColumns() const {
    return Impl_->GetColumns();
}

std::vector<TIndexDescription> TTableDescription::GetIndexDescriptions() const {
    return Impl_->GetIndexDescriptions();
}

std::vector<TChangefeedDescription> TTableDescription::GetChangefeedDescriptions() const {
    return Impl_->GetChangefeedDescriptions();
}

std::optional<TTtlSettings> TTableDescription::GetTtlSettings() const {
    return Impl_->GetTtlSettings();
}

std::optional<std::string> TTableDescription::GetTiering() const {
    return Impl_->GetTiering();
}

EStoreType TTableDescription::GetStoreType() const {
    return Impl_->GetStoreType();
}

const std::string& TTableDescription::GetOwner() const {
    return Impl_->GetOwner();
}

const std::vector<NScheme::TPermissions>& TTableDescription::GetPermissions() const {
    return Impl_->GetPermissions();
}

const std::vector<NScheme::TPermissions>& TTableDescription::GetEffectivePermissions() const {
    return Impl_->GetEffectivePermissions();
}

const std::vector<TKeyRange>& TTableDescription::GetKeyRanges() const {
    return Impl_->GetKeyRanges();
}

void TTableDescription::AddColumn(const std::string& name, const Ydb::Type& type, const std::string& family, std::optional<bool> notNull) {
    Impl_->AddColumn(name, type, family, notNull);
}

void TTableDescription::SetPrimaryKeyColumns(const std::vector<std::string>& primaryKeyColumns) {
    Impl_->SetPrimaryKeyColumns(primaryKeyColumns);
}

void TTableDescription::AddSecondaryIndex(const std::string& indexName, EIndexType type, const std::vector<std::string>& indexColumns) {
    Impl_->AddSecondaryIndex(indexName, type, indexColumns);
}

void TTableDescription::AddSecondaryIndex(const std::string& indexName, EIndexType type, const std::vector<std::string>& indexColumns, const std::vector<std::string>& dataColumns) {
    Impl_->AddSecondaryIndex(indexName, type, indexColumns, dataColumns);
}

void TTableDescription::AddSyncSecondaryIndex(const std::string& indexName, const std::vector<std::string>& indexColumns) {
    AddSecondaryIndex(indexName, EIndexType::GlobalSync, indexColumns);
}

void TTableDescription::AddSyncSecondaryIndex(const std::string& indexName, const std::vector<std::string>& indexColumns, const std::vector<std::string>& dataColumns) {
    AddSecondaryIndex(indexName, EIndexType::GlobalSync, indexColumns, dataColumns);
}

void TTableDescription::AddAsyncSecondaryIndex(const std::string& indexName, const std::vector<std::string>& indexColumns) {
    AddSecondaryIndex(indexName, EIndexType::GlobalAsync, indexColumns);
}

void TTableDescription::AddAsyncSecondaryIndex(const std::string& indexName, const std::vector<std::string>& indexColumns, const std::vector<std::string>& dataColumns) {
    AddSecondaryIndex(indexName, EIndexType::GlobalAsync, indexColumns, dataColumns);
}

void TTableDescription::AddUniqueSecondaryIndex(const std::string& indexName, const std::vector<std::string>& indexColumns) {
    AddSecondaryIndex(indexName, EIndexType::GlobalUnique, indexColumns);
}

void TTableDescription::AddUniqueSecondaryIndex(const std::string& indexName, const std::vector<std::string>& indexColumns, const std::vector<std::string>& dataColumns) {
    AddSecondaryIndex(indexName, EIndexType::GlobalUnique, indexColumns, dataColumns);
}

void TTableDescription::AddSecondaryIndex(const std::string& indexName, const std::vector<std::string>& indexColumns) {
    AddSyncSecondaryIndex(indexName, indexColumns);
}

void TTableDescription::AddSecondaryIndex(const std::string& indexName, const std::vector<std::string>& indexColumns, const std::vector<std::string>& dataColumns) {
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

void TTableDescription::AddAttribute(const std::string& key, const std::string& value) {
    Impl_->AddAttribute(key, value);
}

void TTableDescription::SetAttributes(const std::unordered_map<std::string, std::string>& attrs) {
    Impl_->SetAttributes(attrs);
}

void TTableDescription::SetAttributes(std::unordered_map<std::string, std::string>&& attrs) {
    Impl_->SetAttributes(std::move(attrs));
}

void TTableDescription::SetCompactionPolicy(const std::string& name) {
    Impl_->SetCompactionPolicy(name);
}

void TTableDescription::SetUniformPartitions(uint64_t partitionsCount) {
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

void TTableDescription::SetReadReplicasSettings(TReadReplicasSettings::EMode mode, uint64_t readReplicasCount) {
    Impl_->SetReadReplicasSettings(mode, readReplicasCount);
}

void TTableDescription::SetStoreType(EStoreType type) {
    Impl_->SetStoreType(type);
}

const std::vector<TPartitionStats>& TTableDescription::GetPartitionStats() const {
    return Impl_->GetPartitionStats();
}

TInstant TTableDescription::GetModificationTime() const {
    return Impl_->GetTableStats().ModificationTime;
}

TInstant TTableDescription::GetCreationTime() const {
    return Impl_->GetTableStats().CreationTime;
}

uint64_t TTableDescription::GetTableSize() const {
    return Impl_->GetTableStats().Size;
}

uint64_t TTableDescription::GetTableRows() const {
    return Impl_->GetTableStats().Rows;
}

uint64_t TTableDescription::GetPartitionsCount() const {
    return Impl_->GetTableStats().Partitions;
}

const TStorageSettings& TTableDescription::GetStorageSettings() const {
    return Impl_->GetStorageSettings();
}

const std::vector<TColumnFamilyDescription>& TTableDescription::GetColumnFamilies() const {
    return Impl_->GetColumnFamilies();
}

const std::unordered_map<std::string, std::string>& TTableDescription::GetAttributes() const {
    return Impl_->GetAttributes();
}

const TPartitioningSettings& TTableDescription::GetPartitioningSettings() const {
    return Impl_->GetPartitioningSettings();
}

std::optional<bool> TTableDescription::GetKeyBloomFilter() const {
    return Impl_->GetKeyBloomFilter();
}

std::optional<TReadReplicasSettings> TTableDescription::GetReadReplicasSettings() const {
    return Impl_->GetReadReplicasSettings();
}

const Ydb::Table::DescribeTableResult& TTableDescription::GetProto() const {
    return Impl_->GetProto();
}

void TTableDescription::SerializeTo(Ydb::Table::CreateTableRequest& request) const {
    for (const auto& column : Impl_->GetColumns()) {
        auto& protoColumn = *request.add_columns();
        protoColumn.set_name(TStringType{column.Name});
        protoColumn.mutable_type()->CopyFrom(TProtoAccessor::GetProto(column.Type));
        protoColumn.set_family(TStringType{column.Family});
        if (column.NotNull.has_value()) {
            protoColumn.set_not_null(column.NotNull.value());
        }
    }

    for (const auto& pk : Impl_->GetPrimaryKeyColumns()) {
        request.add_primary_key(TStringType{pk});
    }

    for (const auto& index : Impl_->GetIndexDescriptions()) {
        index.SerializeTo(*request.add_indexes());
    }

    if (const auto& ttl = Impl_->GetTtlSettings()) {
        ttl->SerializeTo(*request.mutable_ttl_settings());
    }

    if (const auto& tiering = Impl_->GetTiering()) {
        request.set_tiering(TStringType{tiering.value()});
    }

    if (Impl_->GetStoreType() == EStoreType::Column) {
        request.set_store_type(Ydb::Table::StoreType::STORE_TYPE_COLUMN);
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

    if (!Impl_->GetCompactionPolicy().empty()) {
        request.set_compaction_policy(TStringType{Impl_->GetCompactionPolicy()});
    }

    if (const auto& uniformPartitions = Impl_->GetUniformPartitions()) {
        request.set_uniform_partitions(uniformPartitions.value());
    }

    if (const auto& partitionAtKeys = Impl_->GetPartitionAtKeys()) {
        auto* borders = request.mutable_partition_at_keys();
        for (const auto& splitPoint : partitionAtKeys->SplitPoints_) {
            auto* border = borders->add_split_points();
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
        if (keyBloomFilter.value()) {
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

TStorageSettingsBuilder& TStorageSettingsBuilder::SetTabletCommitLog0(const std::string& media) {
    Impl_->Proto.mutable_tablet_commit_log0()->set_media(TStringType{media});
    return *this;
}

TStorageSettingsBuilder& TStorageSettingsBuilder::SetTabletCommitLog1(const std::string& media) {
    Impl_->Proto.mutable_tablet_commit_log1()->set_media(TStringType{media});
    return *this;
}

TStorageSettingsBuilder& TStorageSettingsBuilder::SetExternal(const std::string& media) {
    Impl_->Proto.mutable_external()->set_media(TStringType{media});
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

TPartitioningSettingsBuilder& TPartitioningSettingsBuilder::SetPartitionSizeMb(uint64_t sizeMb) {
    Impl_->Proto.set_partition_size_mb(sizeMb);
    return *this;
}

TPartitioningSettingsBuilder& TPartitioningSettingsBuilder::SetMinPartitionsCount(uint64_t count) {
    Impl_->Proto.set_min_partitions_count(count);
    return *this;
}

TPartitioningSettingsBuilder& TPartitioningSettingsBuilder::SetMaxPartitionsCount(uint64_t count) {
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

TColumnFamilyBuilder::TColumnFamilyBuilder(const std::string& name)
    : Impl_(new TImpl)
{
    Impl_->Proto.set_name(TStringType{name});
}

TColumnFamilyBuilder::~TColumnFamilyBuilder() { }

TColumnFamilyBuilder& TColumnFamilyBuilder::SetData(const std::string& media) {
    Impl_->Proto.mutable_data()->set_media(TStringType{media});
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

TTableBuilder& TTableBuilder::SetStoreType(EStoreType type) {
    TableDescription_.SetStoreType(type);
    return *this;
}

TTableBuilder& TTableBuilder::AddNullableColumn(const std::string& name, const EPrimitiveType& type, const std::string& family) {
    auto columnType = TTypeBuilder()
        .BeginOptional()
            .Primitive(type)
        .EndOptional()
        .Build();

    TableDescription_.AddColumn(name, TProtoAccessor::GetProto(columnType), family, false);
    return *this;
}

TTableBuilder& TTableBuilder::AddNullableColumn(const std::string& name, const TDecimalType& type, const std::string& family) {
    auto columnType = TTypeBuilder()
        .BeginOptional()
            .Decimal(type)
        .EndOptional()
        .Build();
    TableDescription_.AddColumn(name, TProtoAccessor::GetProto(columnType), family, false);
    return *this;
}

TTableBuilder& TTableBuilder::AddNullableColumn(const std::string& name, const TPgType& type, const std::string& family) {
    auto columnType = TTypeBuilder()
        .Pg(type)
        .Build();

    TableDescription_.AddColumn(name, TProtoAccessor::GetProto(columnType), family, false);
    return *this;
}

TTableBuilder& TTableBuilder::AddNonNullableColumn(const std::string& name, const EPrimitiveType& type, const std::string& family) {
    auto columnType = TTypeBuilder()
        .Primitive(type)
        .Build();

    TableDescription_.AddColumn(name, TProtoAccessor::GetProto(columnType), family, true);
    return *this;
}

TTableBuilder& TTableBuilder::AddNonNullableColumn(const std::string& name, const TDecimalType& type, const std::string& family) {
    auto columnType = TTypeBuilder()
        .Decimal(type)
        .Build();

    TableDescription_.AddColumn(name, TProtoAccessor::GetProto(columnType), family, true);
    return *this;
}

TTableBuilder& TTableBuilder::AddNonNullableColumn(const std::string& name, const TPgType& type, const std::string& family) {
    auto columnType = TTypeBuilder()
        .Pg(type)
        .Build();

    TableDescription_.AddColumn(name, TProtoAccessor::GetProto(columnType), family, true);
    return *this;
}

TTableBuilder& TTableBuilder::SetPrimaryKeyColumns(const std::vector<std::string>& primaryKeyColumns) {
    TableDescription_.SetPrimaryKeyColumns(primaryKeyColumns);
    return *this;
}

TTableBuilder& TTableBuilder::SetPrimaryKeyColumn(const std::string& primaryKeyColumn) {
    TableDescription_.SetPrimaryKeyColumns(std::vector<std::string>{primaryKeyColumn});
    return *this;
}

TTableBuilder& TTableBuilder::AddSecondaryIndex(const std::string& indexName, EIndexType type, const std::vector<std::string>& indexColumns, const std::vector<std::string>& dataColumns) {
    TableDescription_.AddSecondaryIndex(indexName, type, indexColumns, dataColumns);
    return *this;
}

TTableBuilder& TTableBuilder::AddSecondaryIndex(const std::string& indexName, EIndexType type, const std::vector<std::string>& indexColumns) {
    TableDescription_.AddSecondaryIndex(indexName, type, indexColumns);
    return *this;
}

TTableBuilder& TTableBuilder::AddSecondaryIndex(const std::string& indexName, EIndexType type, const std::string& indexColumn) {
    TableDescription_.AddSecondaryIndex(indexName, type, std::vector<std::string>{indexColumn});
    return *this;
}

TTableBuilder& TTableBuilder::AddSyncSecondaryIndex(const std::string& indexName, const std::vector<std::string>& indexColumns, const std::vector<std::string>& dataColumns) {
    return AddSecondaryIndex(indexName, EIndexType::GlobalSync, indexColumns, dataColumns);
}

TTableBuilder& TTableBuilder::AddSyncSecondaryIndex(const std::string& indexName, const std::vector<std::string>& indexColumns) {
    return AddSecondaryIndex(indexName, EIndexType::GlobalSync, indexColumns);
}

TTableBuilder& TTableBuilder::AddSyncSecondaryIndex(const std::string& indexName, const std::string& indexColumn) {
    return AddSecondaryIndex(indexName, EIndexType::GlobalSync, indexColumn);
}

TTableBuilder& TTableBuilder::AddAsyncSecondaryIndex(const std::string& indexName, const std::vector<std::string>& indexColumns, const std::vector<std::string>& dataColumns) {
    return AddSecondaryIndex(indexName, EIndexType::GlobalAsync, indexColumns, dataColumns);
}

TTableBuilder& TTableBuilder::AddAsyncSecondaryIndex(const std::string& indexName, const std::vector<std::string>& indexColumns) {
    return AddSecondaryIndex(indexName, EIndexType::GlobalAsync, indexColumns);
}

TTableBuilder& TTableBuilder::AddAsyncSecondaryIndex(const std::string& indexName, const std::string& indexColumn) {
    return AddSecondaryIndex(indexName, EIndexType::GlobalAsync, indexColumn);
}

TTableBuilder& TTableBuilder::AddSecondaryIndex(const std::string& indexName, const std::vector<std::string>& indexColumns, const std::vector<std::string>& dataColumns) {
    return AddSyncSecondaryIndex(indexName, indexColumns, dataColumns);
}

TTableBuilder& TTableBuilder::AddSecondaryIndex(const std::string& indexName, const std::vector<std::string>& indexColumns) {
    return AddSyncSecondaryIndex(indexName, indexColumns);
}

TTableBuilder& TTableBuilder::AddUniqueSecondaryIndex(const std::string& indexName, const std::vector<std::string>& indexColumns, const std::vector<std::string>& dataColumns) {
    return AddSecondaryIndex(indexName, EIndexType::GlobalUnique, indexColumns, dataColumns);
}

TTableBuilder& TTableBuilder::AddUniqueSecondaryIndex(const std::string& indexName, const std::vector<std::string>& indexColumns) {
    return AddSecondaryIndex(indexName, EIndexType::GlobalUnique, indexColumns);
}

TTableBuilder& TTableBuilder::AddSecondaryIndex(const std::string& indexName, const std::string& indexColumn) {
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

TTableBuilder& TTableBuilder::SetTtlSettings(const std::string& columnName, const TDuration& expireAfter) {
    return SetTtlSettings(TTtlSettings(columnName, expireAfter));
}

TTableBuilder& TTableBuilder::SetTtlSettings(const std::string& columnName, EUnit columnUnit, const TDuration& expireAfter) {
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

TTableBuilder& TTableBuilder::AddAttribute(const std::string& key, const std::string& value) {
    TableDescription_.AddAttribute(key, value);
    return *this;
}

TTableBuilder& TTableBuilder::SetAttributes(const std::unordered_map<std::string, std::string>& attrs) {
    TableDescription_.SetAttributes(attrs);
    return *this;
}

TTableBuilder& TTableBuilder::SetAttributes(std::unordered_map<std::string, std::string>&& attrs) {
    TableDescription_.SetAttributes(std::move(attrs));
    return *this;
}

TTableBuilder& TTableBuilder::SetCompactionPolicy(const std::string& name) {
    TableDescription_.SetCompactionPolicy(name);
    return *this;
}

TTableBuilder& TTableBuilder::SetUniformPartitions(uint64_t partitionsCount) {
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

TTableBuilder& TTableBuilder::SetReadReplicasSettings(TReadReplicasSettings::EMode mode, uint64_t readReplicasCount) {
    TableDescription_.SetReadReplicasSettings(mode, readReplicasCount);
    return *this;
}

TTableDescription TTableBuilder::Build() {
    return TableDescription_;
}


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

int64_t TTableClient::GetActiveSessionCount() const {
    return Impl_->GetActiveSessionCount();
}

int64_t TTableClient::GetActiveSessionsLimit() const {
    return Impl_->GetActiveSessionsLimit();
}

int64_t TTableClient::GetCurrentPoolSize() const {
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

TAsyncStatus TTableClient::RetryOperation(TOperationFunc&& operation, const TRetryOperationSettings& settings) {
    TRetryContextAsync::TPtr ctx(new NRetry::Async::TRetryWithSession(*this, std::move(operation), settings));
    return ctx->Execute();
}

TAsyncStatus TTableClient::RetryOperation(TOperationWithoutSessionFunc&& operation, const TRetryOperationSettings& settings) {
    TRetryContextAsync::TPtr ctx(new NRetry::Async::TRetryWithoutSession(*this, std::move(operation), settings));
    return ctx->Execute();
}

TStatus TTableClient::RetryOperationSync(const TOperationWithoutSessionSyncFunc& operation, const TRetryOperationSettings& settings) {
    NRetry::Sync::TRetryWithoutSession ctx(*this, operation, settings);
    return ctx.Execute();
}

TStatus TTableClient::RetryOperationSync(const TOperationSyncFunc& operation, const TRetryOperationSettings& settings) {
    NRetry::Sync::TRetryWithSession ctx(*this, operation, settings);
    return ctx.Execute();
}

NThreading::TFuture<void> TTableClient::Stop() {
    return Impl_->Stop();
}

TAsyncBulkUpsertResult TTableClient::BulkUpsert(const std::string& table, TValue&& rows,
    const TBulkUpsertSettings& settings)
{
    return Impl_->BulkUpsert(table, std::move(rows), settings);
}

TAsyncBulkUpsertResult TTableClient::BulkUpsert(const std::string& table, EDataFormat format,
        const std::string& data, const std::string& schema, const TBulkUpsertSettings& settings)
{
    return Impl_->BulkUpsert(table, format, data, schema, settings);
}

TAsyncReadRowsResult TTableClient::ReadRows(const std::string& table, TValue&& rows, const std::vector<std::string>& columns,
    const TReadRowsSettings& settings)
{
    return Impl_->ReadRows(table, std::move(rows), columns, settings);
}

TAsyncScanQueryPartIterator TTableClient::StreamExecuteScanQuery(const std::string& query, const TParams& params,
    const TStreamExecScanQuerySettings& settings)
{
    return Impl_->StreamExecuteScanQuery(query, &params.GetProtoMap(), settings);
}

TAsyncScanQueryPartIterator TTableClient::StreamExecuteScanQuery(const std::string& query,
    const TStreamExecScanQuerySettings& settings)
{
    return Impl_->StreamExecuteScanQuery(query, nullptr, settings);
}

////////////////////////////////////////////////////////////////////////////////

static void ConvertCreateTableSettingsToProto(const TCreateTableSettings& settings, Ydb::Table::TableProfile* proto) {
    if (settings.PresetName_) {
        proto->set_preset_name(TStringType{settings.PresetName_.value()});
    }
    if (settings.ExecutionPolicy_) {
        proto->mutable_execution_policy()->set_preset_name(TStringType{settings.ExecutionPolicy_.value()});
    }
    if (settings.CompactionPolicy_) {
        proto->mutable_compaction_policy()->set_preset_name(TStringType{settings.CompactionPolicy_.value()});
    }
    if (settings.PartitioningPolicy_) {
        const auto& policy = settings.PartitioningPolicy_.value();
        if (policy.PresetName_) {
            proto->mutable_partitioning_policy()->set_preset_name(TStringType{policy.PresetName_.value()});
        }
        if (policy.AutoPartitioning_) {
            proto->mutable_partitioning_policy()->set_auto_partitioning(static_cast<Ydb::Table::PartitioningPolicy_AutoPartitioningPolicy>(policy.AutoPartitioning_.value()));
        }
        if (policy.UniformPartitions_) {
            proto->mutable_partitioning_policy()->set_uniform_partitions(policy.UniformPartitions_.value());
        }
        if (policy.ExplicitPartitions_) {
            auto* borders = proto->mutable_partitioning_policy()->mutable_explicit_partitions();
            for (const auto& splitPoint : policy.ExplicitPartitions_->SplitPoints_) {
                auto* border = borders->add_split_points();
                border->mutable_type()->CopyFrom(TProtoAccessor::GetProto(splitPoint.GetType()));
                border->mutable_value()->CopyFrom(TProtoAccessor::GetProto(splitPoint));
            }
        }
    }
    if (settings.StoragePolicy_) {
        const auto& policy = settings.StoragePolicy_.value();
        if (policy.PresetName_) {
            proto->mutable_storage_policy()->set_preset_name(TStringType{policy.PresetName_.value()});
        }
        if (policy.SysLog_) {
            proto->mutable_storage_policy()->mutable_syslog()->set_media(TStringType{policy.SysLog_.value()});
        }
        if (policy.Log_) {
            proto->mutable_storage_policy()->mutable_log()->set_media(TStringType{policy.Log_.value()});
        }
        if (policy.Data_) {
            proto->mutable_storage_policy()->mutable_data()->set_media(TStringType{policy.Data_.value()});
        }
        if (policy.External_) {
            proto->mutable_storage_policy()->mutable_external()->set_media(TStringType{policy.External_.value()});
        }
        for (const auto& familyPolicy : policy.ColumnFamilies_) {
            auto* familyProto = proto->mutable_storage_policy()->add_column_families();
            if (familyPolicy.Name_) {
                familyProto->set_name(TStringType{familyPolicy.Name_.value()});
            }
            if (familyPolicy.Data_) {
                familyProto->mutable_data()->set_media(TStringType{familyPolicy.Data_.value()});
            }
            if (familyPolicy.External_) {
                familyProto->mutable_external()->set_media(TStringType{familyPolicy.External_.value()});
            }
            if (familyPolicy.KeepInMemory_) {
                familyProto->set_keep_in_memory(
                    familyPolicy.KeepInMemory_.value()
                    ? Ydb::FeatureFlag_Status::FeatureFlag_Status_ENABLED
                    : Ydb::FeatureFlag_Status::FeatureFlag_Status_DISABLED
                );
            }
            if (familyPolicy.Compressed_) {
                familyProto->set_compression(familyPolicy.Compressed_.value()
                    ? Ydb::Table::ColumnFamilyPolicy::COMPRESSED
                    : Ydb::Table::ColumnFamilyPolicy::UNCOMPRESSED);
            }
        }
    }
    if (settings.ReplicationPolicy_) {
        const auto& policy = settings.ReplicationPolicy_.value();
        if (policy.PresetName_) {
            proto->mutable_replication_policy()->set_preset_name(TStringType{policy.PresetName_.value()});
        }
        if (policy.ReplicasCount_) {
            proto->mutable_replication_policy()->set_replicas_count(policy.ReplicasCount_.value());
        }
        if (policy.CreatePerAvailabilityZone_) {
            proto->mutable_replication_policy()->set_create_per_availability_zone(
                policy.CreatePerAvailabilityZone_.value()
                ? Ydb::FeatureFlag_Status::FeatureFlag_Status_ENABLED
                : Ydb::FeatureFlag_Status::FeatureFlag_Status_DISABLED
            );
        }
        if (policy.AllowPromotion_) {
            proto->mutable_replication_policy()->set_allow_promotion(
                policy.AllowPromotion_.value()
                ? Ydb::FeatureFlag_Status::FeatureFlag_Status_ENABLED
                : Ydb::FeatureFlag_Status::FeatureFlag_Status_DISABLED
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TSession::TSession(std::shared_ptr<TTableClient::TImpl> client, const std::string& sessionId, const std::string& endpointId, bool isOwnedBySessionPool)
    : Client_(client)
    , SessionImpl_(new TSession::TImpl(
            sessionId,
            endpointId,
            client->Settings_.UseQueryCache_,
            client->Settings_.QueryCacheSize_,
            isOwnedBySessionPool),
        TSession::TImpl::GetSmartDeleter(client))
{
    if (!endpointId.empty()) {
        Client_->LinkObjToEndpoint(SessionImpl_->GetEndpointKey(), SessionImpl_.get(), Client_.get());
    }
}

TSession::TSession(std::shared_ptr<TTableClient::TImpl> client, std::shared_ptr<TImpl> sessionid)
    : Client_(client)
    , SessionImpl_(sessionid)
{}

TFuture<TStatus> TSession::CreateTable(const std::string& path, TTableDescription&& tableDesc,
        const TCreateTableSettings& settings)
{
    auto request = MakeOperationRequest<Ydb::Table::CreateTableRequest>(settings);
    request.set_session_id(TStringType{SessionImpl_->GetId()});
    request.set_path(TStringType{path});

    tableDesc.SerializeTo(request);

    ConvertCreateTableSettingsToProto(settings, request.mutable_profile());

    return InjectSessionStatusInterception(
        SessionImpl_,
        Client_->CreateTable(std::move(request), settings),
        false,
        GetMinTimeToTouch(Client_->Settings_.SessionPoolSettings_));
}

TFuture<TStatus> TSession::DropTable(const std::string& path, const TDropTableSettings& settings) {
    return InjectSessionStatusInterception(
        SessionImpl_,
        Client_->DropTable(SessionImpl_->GetId(), path, settings),
        false,
        GetMinTimeToTouch(Client_->Settings_.SessionPoolSettings_));
}

static Ydb::Table::AlterTableRequest MakeAlterTableProtoRequest(
    const std::string& path, const TAlterTableSettings& settings, const std::string& sessionId)
{
    auto request = MakeOperationRequest<Ydb::Table::AlterTableRequest>(settings);
    request.set_session_id(TStringType{sessionId});
    request.set_path(TStringType{path});

    for (const auto& column : settings.AddColumns_) {
        auto& protoColumn = *request.add_add_columns();
        protoColumn.set_name(TStringType{column.Name});
        protoColumn.mutable_type()->CopyFrom(TProtoAccessor::GetProto(column.Type));
        protoColumn.set_family(TStringType{column.Family});
    }

    for (const auto& columnName : settings.DropColumns_) {
        request.add_drop_columns(TStringType{columnName});
    }

    for (const auto& alter : settings.AlterColumns_) {
        auto& protoAlter = *request.add_alter_columns();
        protoAlter.set_name(TStringType{alter.Name});
        protoAlter.set_family(TStringType{alter.Family});
    }

    for (const auto& addIndex : settings.AddIndexes_) {
        addIndex.SerializeTo(*request.add_add_indexes());
    }

    for (const auto& name : settings.DropIndexes_) {
        request.add_drop_indexes(TStringType{name});
    }

    for (const auto& rename : settings.RenameIndexes_) {
        SerializeTo(rename, *request.add_rename_indexes());
    }

    for (const auto& addChangefeed : settings.AddChangefeeds_) {
        addChangefeed.SerializeTo(*request.add_add_changefeeds());
    }

    for (const auto& name : settings.DropChangefeeds_) {
        request.add_drop_changefeeds(TStringType{name});
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

    if (!settings.SetCompactionPolicy_.empty()) {
        request.set_set_compaction_policy(TStringType{settings.SetCompactionPolicy_});
    }

    if (settings.AlterPartitioningSettings_) {
        request.mutable_alter_partitioning_settings()->CopyFrom(settings.AlterPartitioningSettings_->GetProto());
    }

    if (settings.SetKeyBloomFilter_.has_value()) {
        request.set_set_key_bloom_filter(
            settings.SetKeyBloomFilter_.value() ? Ydb::FeatureFlag::ENABLED : Ydb::FeatureFlag::DISABLED);
    }

    if (settings.SetReadReplicasSettings_.has_value()) {
        const auto& replSettings = settings.SetReadReplicasSettings_.value();
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

TAsyncStatus TSession::AlterTable(const std::string& path, const TAlterTableSettings& settings) {
    auto request = MakeAlterTableProtoRequest(path, settings, SessionImpl_->GetId());

    return InjectSessionStatusInterception(
        SessionImpl_,
        Client_->AlterTable(std::move(request), settings),
        false,
        GetMinTimeToTouch(Client_->Settings_.SessionPoolSettings_));
}

TAsyncOperation TSession::AlterTableLong(const std::string& path, const TAlterTableSettings& settings) {
    auto request = MakeAlterTableProtoRequest(path, settings, SessionImpl_->GetId());

    return InjectSessionStatusInterception(
        SessionImpl_,
        Client_->AlterTableLong(std::move(request), settings),
        false,
        GetMinTimeToTouch(Client_->Settings_.SessionPoolSettings_));
}

TAsyncStatus TSession::RenameTables(const std::vector<TRenameItem>& renameItems, const TRenameTablesSettings& settings) {
    auto request = MakeOperationRequest<Ydb::Table::RenameTablesRequest>(settings);
    request.set_session_id(TStringType{SessionImpl_->GetId()});

    for (const auto& item: renameItems) {
        auto add = request.add_tables();
        add->set_source_path(TStringType{item.SourcePath()});
        add->set_destination_path(TStringType{item.DestinationPath()});
        add->set_replace_destination(item.ReplaceDestination());
    }

    return InjectSessionStatusInterception(
        SessionImpl_,
        Client_->RenameTables(std::move(request), settings),
        false,
        GetMinTimeToTouch(Client_->Settings_.SessionPoolSettings_));
}

TAsyncStatus TSession::CopyTables(const std::vector<TCopyItem>& copyItems, const TCopyTablesSettings& settings) {
    auto request = MakeOperationRequest<Ydb::Table::CopyTablesRequest>(settings);
    request.set_session_id(TStringType{SessionImpl_->GetId()});

    for (const auto& item: copyItems) {
        auto add = request.add_tables();
        add->set_source_path(TStringType{item.SourcePath()});
        add->set_destination_path(TStringType{item.DestinationPath()});
        add->set_omit_indexes(item.OmitIndexes());
    }

    return InjectSessionStatusInterception(
        SessionImpl_,
        Client_->CopyTables(std::move(request), settings),
        false,
        GetMinTimeToTouch(Client_->Settings_.SessionPoolSettings_));
}

TFuture<TStatus> TSession::CopyTable(const std::string& src, const std::string& dst, const TCopyTableSettings& settings) {
    return InjectSessionStatusInterception(
        SessionImpl_,
        Client_->CopyTable(SessionImpl_->GetId(), src, dst, settings),
        false,
        GetMinTimeToTouch(Client_->Settings_.SessionPoolSettings_));
}

TAsyncDescribeTableResult TSession::DescribeTable(const std::string& path, const TDescribeTableSettings& settings) {
    return Client_->DescribeTable(SessionImpl_->GetId(), path, settings);
}

TAsyncDataQueryResult TSession::ExecuteDataQuery(const std::string& query, const TTxControl& txControl,
    const TExecDataQuerySettings& settings)
{
    return Client_->ExecuteDataQuery(*this, query, txControl, nullptr, settings);
}

TAsyncDataQueryResult TSession::ExecuteDataQuery(const std::string& query, const TTxControl& txControl,
    TParams&& params, const TExecDataQuerySettings& settings)
{
    auto paramsPtr = params.Empty() ? nullptr : params.GetProtoMapPtr();
    return Client_->ExecuteDataQuery(*this, query, txControl, paramsPtr, settings);
}

TAsyncDataQueryResult TSession::ExecuteDataQuery(const std::string& query, const TTxControl& txControl,
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
        using TProtoParamsType = const ::google::protobuf::Map<TStringType, Ydb::TypedValue>;
        return Client_->ExecuteDataQuery<TProtoParamsType&>(
            *this,
            query,
            txControl,
            params.GetProtoMap(),
            settings);
    }
}

TAsyncPrepareQueryResult TSession::PrepareDataQuery(const std::string& query, const TPrepareDataQuerySettings& settings) {
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

TAsyncStatus TSession::ExecuteSchemeQuery(const std::string& query, const TExecSchemeQuerySettings& settings) {
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

TAsyncExplainDataQueryResult TSession::ExplainDataQuery(const std::string& query,
    const TExplainDataQuerySettings& settings)
{
    return InjectSessionStatusInterception(
        SessionImpl_,
        Client_->ExplainDataQuery(*this, query, settings),
        true,
        GetMinTimeToTouch(Client_->Settings_.SessionPoolSettings_));
}

TAsyncTablePartIterator TSession::ReadTable(const std::string& path,
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

const std::string& TSession::GetId() const {
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

TTransaction::TTransaction(const TSession& session, const std::string& txId)
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

TDataQuery::TDataQuery(const TSession& session, const std::string& text, const std::string& id)
    : Impl_(new TImpl(session, text, session.Client_->Settings_.KeepDataQueryText_, id,
                      session.Client_->Settings_.AllowRequestMigration_))
{}

TDataQuery::TDataQuery(const TSession& session, const std::string& text, const std::string& id,
    const ::google::protobuf::Map<TStringType, Ydb::Type>& types)
    : Impl_(new TImpl(session, text, session.Client_->Settings_.KeepDataQueryText_, id,
                      session.Client_->Settings_.AllowRequestMigration_, types))
{}

const std::string& TDataQuery::GetId() const {
    return Impl_->GetId();
}

const std::optional<std::string>& TDataQuery::GetText() const {
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
        using TProtoParamsType = const ::google::protobuf::Map<TStringType, Ydb::TypedValue>;
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

TExplainQueryResult::TExplainQueryResult(TStatus&& status, std::string&& plan, std::string&& ast, std::string&& diagnostics)
    : TStatus(std::move(status))
    , Plan_(std::move(plan))
    , Ast_(std::move(ast))
    , Diagnostics_(std::move(diagnostics))
{}

const std::string& TExplainQueryResult::GetPlan() const {
    CheckStatusOk("TExplainQueryResult::GetPlan");
    return Plan_;
}

const std::string& TExplainQueryResult::GetAst() const {
    CheckStatusOk("TExplainQueryResult::GetAst");
    return Ast_;
}

const std::string& TExplainQueryResult::GetDiagnostics() const {
    CheckStatusOk("TExplainQueryResult::GetDiagnostics");
    return Diagnostics_;
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

TDataQueryResult::TDataQueryResult(TStatus&& status, std::vector<TResultSet>&& resultSets,
    const std::optional<TTransaction>& transaction, const std::optional<TDataQuery>& dataQuery, bool fromCache, const std::optional<TQueryStats> &queryStats)
    : TStatus(std::move(status))
    , Transaction_(transaction)
    , ResultSets_(std::move(resultSets))
    , DataQuery_(dataQuery)
    , FromCache_(fromCache)
    , QueryStats_(queryStats)
{}

const std::vector<TResultSet>& TDataQueryResult::GetResultSets() const {
    return ResultSets_;
}

std::vector<TResultSet> TDataQueryResult::ExtractResultSets() && {
    return std::move(ResultSets_);
}

TResultSet TDataQueryResult::GetResultSet(size_t resultIndex) const {
    if (resultIndex >= ResultSets_.size()) {
        RaiseError(std::string("Requested index out of range\n"));
    }

    return ResultSets_[resultIndex];
}

TResultSetParser TDataQueryResult::GetResultSetParser(size_t resultIndex) const {
    return TResultSetParser(GetResultSet(resultIndex));
}

std::optional<TTransaction> TDataQueryResult::GetTransaction() const {
    return Transaction_;
}

std::optional<TDataQuery> TDataQueryResult::GetQuery() const {
    return DataQuery_;
}

bool TDataQueryResult::IsQueryFromCache() const {
    return FromCache_;
}

const std::optional<TQueryStats>& TDataQueryResult::GetStats() const {
    return QueryStats_;
}

const std::string TDataQueryResult::GetQueryPlan() const {
    if (QueryStats_.has_value()) {
        return NYdb::TProtoAccessor::GetProto(*QueryStats_).query_plan();
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

TCommitTransactionResult::TCommitTransactionResult(TStatus&& status, const std::optional<TQueryStats>& queryStats)
    : TStatus(std::move(status))
    , QueryStats_(queryStats)
{}

const std::optional<TQueryStats>& TCommitTransactionResult::GetStats() const {
    return QueryStats_;
}

////////////////////////////////////////////////////////////////////////////////

TCopyItem::TCopyItem(const std::string& source, const std::string& destination)
    : Source_(source)
    , Destination_(destination)
    , OmitIndexes_(false) {
}

const std::string& TCopyItem::SourcePath() const {
    return Source_;
}

const std::string& TCopyItem::DestinationPath() const {
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

TRenameItem::TRenameItem(const std::string& source, const std::string& destination)
    : Source_(source)
    , Destination_(destination)
    , ReplaceDestination_(false) {
}

const std::string& TRenameItem::SourcePath() const {
    return Source_;
}

const std::string& TRenameItem::DestinationPath() const {
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

TIndexDescription::TIndexDescription(const std::string& name, EIndexType type,
        const std::vector<std::string>& indexColumns, const std::vector<std::string>& dataColumns)
    : IndexName_(name)
    , IndexType_(type)
    , IndexColumns_(indexColumns)
    , DataColumns_(dataColumns)
{}

TIndexDescription::TIndexDescription(const std::string& name, const std::vector<std::string>& indexColumns, const std::vector<std::string>& dataColumns)
    : TIndexDescription(name, EIndexType::GlobalSync, indexColumns, dataColumns)
{}

TIndexDescription::TIndexDescription(const Ydb::Table::TableIndex& tableIndex)
    : TIndexDescription(FromProto(tableIndex))
{}

TIndexDescription::TIndexDescription(const Ydb::Table::TableIndexDescription& tableIndexDesc)
    : TIndexDescription(FromProto(tableIndexDesc))
{}

const std::string& TIndexDescription::GetIndexName() const {
    return IndexName_;
}

EIndexType TIndexDescription::GetIndexType() const {
    return IndexType_;
}

const std::vector<std::string>& TIndexDescription::GetIndexColumns() const {
    return IndexColumns_;
}

const std::vector<std::string>& TIndexDescription::GetDataColumns() const {
    return DataColumns_;
}

uint64_t TIndexDescription::GetSizeBytes() const {
    return SizeBytes;
}

template <typename TProto>
TIndexDescription TIndexDescription::FromProto(const TProto& proto) {
    EIndexType type;
    std::vector<std::string> indexColumns;
    std::vector<std::string> dataColumns;

    indexColumns.assign(proto.index_columns().begin(), proto.index_columns().end());
    dataColumns.assign(proto.data_columns().begin(), proto.data_columns().end());

    switch (proto.type_case()) {
    case TProto::kGlobalIndex:
        type = EIndexType::GlobalSync;
        break;
    case TProto::kGlobalAsyncIndex:
        type = EIndexType::GlobalAsync;
        break;
    case TProto::kGlobalUniqueIndex:
        type = EIndexType::GlobalUnique;
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
    proto.set_name(TStringType{IndexName_});
    for (const auto& indexCol : IndexColumns_) {
        proto.add_index_columns(TStringType{indexCol});
    }

    *proto.mutable_data_columns() = {DataColumns_.begin(), DataColumns_.end()};

    switch (IndexType_) {
    case EIndexType::GlobalSync:
        *proto.mutable_global_index() = Ydb::Table::GlobalIndex();
        break;
    case EIndexType::GlobalAsync:
        *proto.mutable_global_async_index() = Ydb::Table::GlobalAsyncIndex();
        break;
    case EIndexType::GlobalUnique:
        *proto.mutable_global_unique_index() = Ydb::Table::GlobalUniqueIndex();
        break;
    case EIndexType::Unknown:
        break;
    }
}

std::string TIndexDescription::ToString() const {
    TString result;
    TStringOutput out(result);
    Out(out);
    return result;
}

void TIndexDescription::Out(IOutputStream& o) const {
    o << "{ name: \"" << IndexName_ << "\"";
    o << ", type: " << IndexType_ << "";
    o << ", index_columns: [" << JoinSeq(", ", IndexColumns_) << "]";

    if (!DataColumns_.empty()) {
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

TChangefeedDescription::TChangefeedDescription(const std::string& name, EChangefeedMode mode, EChangefeedFormat format)
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

TChangefeedDescription& TChangefeedDescription::AddAttribute(const std::string& key, const std::string& value) {
    Attributes_[key] = value;
    return *this;
}

TChangefeedDescription& TChangefeedDescription::SetAttributes(const std::unordered_map<std::string, std::string>& attrs) {
    Attributes_ = attrs;
    return *this;
}

TChangefeedDescription& TChangefeedDescription::SetAttributes(std::unordered_map<std::string, std::string>&& attrs) {
    Attributes_ = std::move(attrs);
    return *this;
}

TChangefeedDescription& TChangefeedDescription::WithAwsRegion(const std::string& value) {
    AwsRegion_ = value;
    return *this;
}

const std::string& TChangefeedDescription::GetName() const {
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

const std::unordered_map<std::string, std::string>& TChangefeedDescription::GetAttributes() const {
    return Attributes_;
}

const std::string& TChangefeedDescription::GetAwsRegion() const {
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
    case Ydb::Table::ChangefeedFormat::FORMAT_DEBEZIUM_JSON:
        format = EChangefeedFormat::DebeziumJson;
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
    proto.set_name(TStringType{Name_});
    proto.set_virtual_timestamps(VirtualTimestamps_);
    proto.set_initial_scan(InitialScan_);
    proto.set_aws_region(TStringType{AwsRegion_});

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
    case EChangefeedFormat::DebeziumJson:
        proto.set_format(Ydb::Table::ChangefeedFormat::FORMAT_DEBEZIUM_JSON);
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

std::string TChangefeedDescription::ToString() const {
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

    if (!AwsRegion_.empty()) {
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

TDateTypeColumnModeSettings::TDateTypeColumnModeSettings(const std::string& columnName, const TDuration& expireAfter)
    : ColumnName_(columnName)
    , ExpireAfter_(expireAfter)
{}

void TDateTypeColumnModeSettings::SerializeTo(Ydb::Table::DateTypeColumnModeSettings& proto) const {
    proto.set_column_name(TStringType{ColumnName_});
    proto.set_expire_after_seconds(ExpireAfter_.Seconds());
}

const std::string& TDateTypeColumnModeSettings::GetColumnName() const {
    return ColumnName_;
}

const TDuration& TDateTypeColumnModeSettings::GetExpireAfter() const {
    return ExpireAfter_;
}

TValueSinceUnixEpochModeSettings::TValueSinceUnixEpochModeSettings(const std::string& columnName, EUnit columnUnit, const TDuration& expireAfter)
    : ColumnName_(columnName)
    , ColumnUnit_(columnUnit)
    , ExpireAfter_(expireAfter)
{}

void TValueSinceUnixEpochModeSettings::SerializeTo(Ydb::Table::ValueSinceUnixEpochModeSettings& proto) const {
    proto.set_column_name(TStringType{ColumnName_});
    proto.set_column_unit(TProtoAccessor::GetProto(ColumnUnit_));
    proto.set_expire_after_seconds(ExpireAfter_.Seconds());
}

const std::string& TValueSinceUnixEpochModeSettings::GetColumnName() const {
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

std::string TValueSinceUnixEpochModeSettings::ToString(EUnit unit) {
    TString result;
    TStringOutput out(result);
    Out(out, unit);
    return result;
}

TValueSinceUnixEpochModeSettings::EUnit TValueSinceUnixEpochModeSettings::UnitFromString(const std::string& value) {
    const auto norm = NUtils::ToLower(value);

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

TTtlSettings::TTtlSettings(const std::string& columnName, const TDuration& expireAfter)
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

TTtlSettings::TTtlSettings(const std::string& columnName, EUnit columnUnit, const TDuration& expireAfter)
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

    const std::optional<TAlterTtlSettings>& GetAlterTtlSettings() const {
        return AlterTtlSettings_;
    }

private:
    std::optional<TAlterTtlSettings> AlterTtlSettings_;
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

TAlterTtlSettingsBuilder& TAlterTtlSettingsBuilder::Set(const std::string& columnName, const TDuration& expireAfter) {
    return Set(TTtlSettings(columnName, expireAfter));
}

TAlterTtlSettingsBuilder& TAlterTtlSettingsBuilder::Set(const std::string& columnName, EUnit columnUnit, const TDuration& expireAfter) {
    return Set(TTtlSettings(columnName, columnUnit, expireAfter));
}

TAlterTableSettings& TAlterTtlSettingsBuilder::EndAlterTtlSettings() {
    return Parent_.AlterTtlSettings(Impl_->GetAlterTtlSettings());
}

class TAlterTableSettings::TImpl {
public:
    TImpl() { }

    void SetAlterTtlSettings(const std::optional<TAlterTtlSettings>& value) {
        AlterTtlSettings_ = value;
    }

    const std::optional<TAlterTtlSettings>& GetAlterTtlSettings() const {
        return AlterTtlSettings_;
    }

private:
    std::optional<TAlterTtlSettings> AlterTtlSettings_;
};

TAlterTableSettings::TAlterTableSettings()
    : Impl_(std::make_shared<TImpl>())
{ }

TAlterTableSettings& TAlterTableSettings::AlterTtlSettings(const std::optional<TAlterTtlSettings>& value) {
    Impl_->SetAlterTtlSettings(value);
    return *this;
}

const std::optional<TAlterTtlSettings>& TAlterTableSettings::GetAlterTtlSettings() const {
    return Impl_->GetAlterTtlSettings();
}

////////////////////////////////////////////////////////////////////////////////

TReadReplicasSettings::TReadReplicasSettings(EMode mode, uint64_t readReplicasCount)
    : Mode_(mode)
    , ReadReplicasCount_(readReplicasCount)
{}

TReadReplicasSettings::EMode TReadReplicasSettings::GetMode() const {
    return Mode_;
}

uint64_t TReadReplicasSettings::GetReadReplicasCount() const {
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
