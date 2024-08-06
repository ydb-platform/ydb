#include "table.h"

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/scheme_helpers/helpers.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/table_helpers/helpers.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/make_request/make.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/retry/retry.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/retry/retry_async.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/retry/retry_sync.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_stats/stats.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>
#include <ydb/public/sdk/cpp/client/ydb_value/value.h>
#include <ydb/public/sdk/cpp/client/ydb_table/impl/client_session.h>
#include <ydb/public/sdk/cpp/client/ydb_table/impl/data_query.h>
#include <ydb/public/sdk/cpp/client/ydb_table/impl/request_migrator.h>
#include <ydb/public/sdk/cpp/client/ydb_table/impl/table_client.h>
#include <ydb/public/sdk/cpp/client/resources/ydb_resources.h>

#include <google/protobuf/util/time_util.h>

#include <library/cpp/cache/cache.h>

#include <util/generic/map.h>
#include <util/random/random.h>
#include <util/string/join.h>
#include <util/stream/output.h>

#include <unordered_map>

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

template <typename TProto>
TExplicitPartitions TExplicitPartitions::FromProto(const TProto& proto) {
    TExplicitPartitions out;
    for (const auto& splitPoint : proto.split_points()) {
        TValue value(TType(splitPoint.type()), splitPoint.value());
        out.AppendSplitPoints(value);
    }
    return out;
}

void TExplicitPartitions::SerializeTo(Ydb::Table::ExplicitPartitions& proto) const {
    for (const auto& splitPoint : SplitPoints_) {
        auto* boundary = proto.Addsplit_points();
        boundary->mutable_type()->CopyFrom(TProtoAccessor::GetProto(splitPoint.GetType()));
        boundary->mutable_value()->CopyFrom(TProtoAccessor::GetProto(splitPoint));
    }
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
                SetPartitionAtKeys(TExplicitPartitions::FromProto(request.partition_at_keys()));
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

    void AddColumn(const TString& name, const Ydb::Type& type, const TString& family, std::optional<bool> notNull) {
        Columns_.emplace_back(name, type, family, notNull);
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

    void AddVectorIndex(const TString& indexName, EIndexType type, const TVector<TString>& indexColumns, const TVectorIndexSettings& vectorIndexSettings) {
        Indexes_.emplace_back(TIndexDescription(indexName, type, indexColumns, {}, {}, vectorIndexSettings));
    }

    void AddVectorIndex(const TString& indexName, EIndexType type, const TVector<TString>& indexColumns, const TVector<TString>& dataColumns, const TVectorIndexSettings& vectorIndexSettings) {
        Indexes_.emplace_back(TIndexDescription(indexName, type, indexColumns, dataColumns, {}, vectorIndexSettings));
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

    void SetStoreType(EStoreType type) {
        StoreType_ = type;
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

    EStoreType GetStoreType() const {
        return StoreType_;
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

EStoreType TTableDescription::GetStoreType() const {
    return Impl_->GetStoreType();
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

void TTableDescription::AddColumn(const TString& name, const Ydb::Type& type, const TString& family, std::optional<bool> notNull) {
    Impl_->AddColumn(name, type, family, notNull);
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

void TTableDescription::AddUniqueSecondaryIndex(const TString& indexName, const TVector<TString>& indexColumns) {
    AddSecondaryIndex(indexName, EIndexType::GlobalUnique, indexColumns);
}

void TTableDescription::AddUniqueSecondaryIndex(const TString& indexName, const TVector<TString>& indexColumns, const TVector<TString>& dataColumns) {
    AddSecondaryIndex(indexName, EIndexType::GlobalUnique, indexColumns, dataColumns);
}

void TTableDescription::AddVectorKMeansTreeSecondaryIndex(const TString& indexName, const TVector<TString>& indexColumns, const TVectorIndexSettings& vectorIndexSettings) {
    Impl_->AddVectorIndex(indexName, EIndexType::GlobalVectorKMeansTree, indexColumns, vectorIndexSettings);
}

void TTableDescription::AddVectorKMeansTreeSecondaryIndex(const TString& indexName, const TVector<TString>& indexColumns, const TVector<TString>& dataColumns, const TVectorIndexSettings& vectorIndexSettings) {
    Impl_->AddVectorIndex(indexName, EIndexType::GlobalVectorKMeansTree, indexColumns, dataColumns, vectorIndexSettings);
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

void TTableDescription::SetStoreType(EStoreType type) {
    Impl_->SetStoreType(type);
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
        if (column.NotNull.has_value()) {
            protoColumn.set_not_null(column.NotNull.value());
        }
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

    if (Impl_->GetCompactionPolicy()) {
        request.set_compaction_policy(Impl_->GetCompactionPolicy());
    }

    if (const auto& uniformPartitions = Impl_->GetUniformPartitions()) {
        request.set_uniform_partitions(uniformPartitions.GetRef());
    }

    if (const auto& partitionAtKeys = Impl_->GetPartitionAtKeys()) {
        partitionAtKeys->SerializeTo(*request.mutable_partition_at_keys());
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

TTableBuilder& TTableBuilder::SetStoreType(EStoreType type) {
    TableDescription_.SetStoreType(type);
    return *this;
}

TTableBuilder& TTableBuilder::AddNullableColumn(const TString& name, const EPrimitiveType& type, const TString& family) {
    auto columnType = TTypeBuilder()
        .BeginOptional()
            .Primitive(type)
        .EndOptional()
        .Build();

    TableDescription_.AddColumn(name, TProtoAccessor::GetProto(columnType), family, false);
    return *this;
}

TTableBuilder& TTableBuilder::AddNullableColumn(const TString& name, const TDecimalType& type, const TString& family) {
    auto columnType = TTypeBuilder()
        .BeginOptional()
            .Decimal(type)
        .EndOptional()
        .Build();
    TableDescription_.AddColumn(name, TProtoAccessor::GetProto(columnType), family, false);
    return *this;
}

TTableBuilder& TTableBuilder::AddNullableColumn(const TString& name, const TPgType& type, const TString& family) {
    auto columnType = TTypeBuilder()
        .Pg(type)
        .Build();

    TableDescription_.AddColumn(name, TProtoAccessor::GetProto(columnType), family, false);
    return *this;
}

TTableBuilder& TTableBuilder::AddNonNullableColumn(const TString& name, const EPrimitiveType& type, const TString& family) {
    auto columnType = TTypeBuilder()
        .Primitive(type)
        .Build();

    TableDescription_.AddColumn(name, TProtoAccessor::GetProto(columnType), family, true);
    return *this;
}

TTableBuilder& TTableBuilder::AddNonNullableColumn(const TString& name, const TDecimalType& type, const TString& family) {
    auto columnType = TTypeBuilder()
        .Decimal(type)
        .Build();

    TableDescription_.AddColumn(name, TProtoAccessor::GetProto(columnType), family, true);
    return *this;
}

TTableBuilder& TTableBuilder::AddNonNullableColumn(const TString& name, const TPgType& type, const TString& family) {
    auto columnType = TTypeBuilder()
        .Pg(type)
        .Build();

    TableDescription_.AddColumn(name, TProtoAccessor::GetProto(columnType), family, true);
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

TTableBuilder& TTableBuilder::AddUniqueSecondaryIndex(const TString& indexName, const TVector<TString>& indexColumns, const TVector<TString>& dataColumns) {
    return AddSecondaryIndex(indexName, EIndexType::GlobalUnique, indexColumns, dataColumns);
}

TTableBuilder& TTableBuilder::AddUniqueSecondaryIndex(const TString& indexName, const TVector<TString>& indexColumns) {
    return AddSecondaryIndex(indexName, EIndexType::GlobalUnique, indexColumns);
}

TTableBuilder& TTableBuilder::AddVectorKMeansTreeSecondaryIndex(const TString& indexName, const TVector<TString>& indexColumns, const TVector<TString>& dataColumns, const TVectorIndexSettings& vectorIndexSettings) {
    TableDescription_.AddVectorKMeansTreeSecondaryIndex(indexName, indexColumns, dataColumns, vectorIndexSettings);
    return *this;
}

TTableBuilder& TTableBuilder::AddVectorKMeansTreeSecondaryIndex(const TString& indexName, const TVector<TString>& indexColumns, const TVectorIndexSettings& vectorIndexSettings) {
    TableDescription_.AddVectorKMeansTreeSecondaryIndex(indexName, indexColumns, vectorIndexSettings);
    return *this;
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

TAsyncReadRowsResult TTableClient::ReadRows(const TString& table, TValue&& rows, const TVector<TString>& columns,
    const TReadRowsSettings& settings)
{
    return Impl_->ReadRows(table, std::move(rows), columns, settings);
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

TSession::TSession(std::shared_ptr<TTableClient::TImpl> client, const TString& sessionId, const TString& endpointId, bool isOwnedBySessionPool)
    : Client_(client)
    , SessionImpl_(new TSession::TImpl(
            sessionId,
            endpointId,
            client->Settings_.UseQueryCache_,
            client->Settings_.QueryCacheSize_,
            isOwnedBySessionPool),
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

TExplainQueryResult::TExplainQueryResult(TStatus&& status, TString&& plan, TString&& ast, TString&& diagnostics)
    : TStatus(std::move(status))
    , Plan_(std::move(plan))
    , Ast_(std::move(ast))
    , Diagnostics_(std::move(diagnostics))
{}

const TString& TExplainQueryResult::GetPlan() const {
    CheckStatusOk("TExplainQueryResult::GetPlan");
    return Plan_;
}

const TString& TExplainQueryResult::GetAst() const {
    CheckStatusOk("TExplainQueryResult::GetAst");
    return Ast_;
}

const TString& TExplainQueryResult::GetDiagnostics() const {
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

TVector<TResultSet> TDataQueryResult::ExtractResultSets() && {
    return std::move(ResultSets_);
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

TIndexDescription::TIndexDescription(
    const TString& name,
    EIndexType type,
    const TVector<TString>& indexColumns,
    const TVector<TString>& dataColumns,
    const TVector<TGlobalIndexSettings>& globalIndexSettings,
    const std::optional<TVectorIndexSettings>& vectorIndexSettings
)   : IndexName_(name)
    , IndexType_(type)
    , IndexColumns_(indexColumns)
    , DataColumns_(dataColumns)
    , GlobalIndexSettings_(globalIndexSettings)
    , VectorIndexSettings_(vectorIndexSettings)
{}

TIndexDescription::TIndexDescription(
    const TString& name,
    const TVector<TString>& indexColumns,
    const TVector<TString>& dataColumns,
    const TVector<TGlobalIndexSettings>& globalIndexSettings
)   : TIndexDescription(name, EIndexType::GlobalSync, indexColumns, dataColumns, globalIndexSettings)
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

const std::optional<TVectorIndexSettings>& TIndexDescription::GetVectorIndexSettings() const {
    return VectorIndexSettings_;
}

ui64 TIndexDescription::GetSizeBytes() const {
    return SizeBytes;
}

template <typename TProto>
TGlobalIndexSettings TGlobalIndexSettings::FromProto(const TProto& proto) {
    auto partitionsFromProto = [](const auto& proto) -> TUniformOrExplicitPartitions {
        switch (proto.partitions_case()) {
        case TProto::kUniformPartitions:
            return proto.uniform_partitions();
        case TProto::kPartitionAtKeys:
            return TExplicitPartitions::FromProto(proto.partition_at_keys());
        default:
            return {};
        }
    };

    return {
        .PartitioningSettings = TPartitioningSettings(proto.partitioning_settings()),
        .Partitions = partitionsFromProto(proto)
    };
}

void TGlobalIndexSettings::SerializeTo(Ydb::Table::GlobalIndexSettings& settings) const {
    *settings.mutable_partitioning_settings() = PartitioningSettings.GetProto();

    auto variantVisitor = [&settings](auto&& partitions) {
        using T = std::decay_t<decltype(partitions)>;
        if constexpr (std::is_same_v<T, ui64>) {
            settings.set_uniform_partitions(partitions);
        } else if constexpr (std::is_same_v<T, TExplicitPartitions>) {
            partitions.SerializeTo(*settings.mutable_partition_at_keys());
        }
    };
    std::visit(std::move(variantVisitor), Partitions);
}

template <typename TProto>
TVectorIndexSettings TVectorIndexSettings::FromProto(const TProto& proto) {
    auto convertDistance = [] (auto distance) -> auto {
        switch (distance) {
        case Ydb::Table::VectorIndexSettings::DISTANCE_COSINE:
            return EDistance::Cosine;
        case Ydb::Table::VectorIndexSettings::DISTANCE_MANHATTAN:
            return EDistance::Manhattan;
        case Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN:
            return EDistance::Euclidean;
        default:
            return EDistance::Unknown;
        }
    };

    auto convertSimilarity = [] (auto similarity) -> auto {
        switch (similarity) {
        case Ydb::Table::VectorIndexSettings::SIMILARITY_COSINE:
            return ESimilarity::Cosine;
        case Ydb::Table::VectorIndexSettings::SIMILARITY_INNER_PRODUCT:
            return ESimilarity::InnerProduct;
        default:
            return ESimilarity::Unknown;
        }
    };

    auto convertVectorType = [] (auto vectorType) -> auto {
        switch (vectorType) {
        case Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT:
            return EVectorType::Float;
        case Ydb::Table::VectorIndexSettings::VECTOR_TYPE_UINT8:
            return EVectorType::Uint8;
        case Ydb::Table::VectorIndexSettings::VECTOR_TYPE_INT8:
            return EVectorType::Int8;
        case Ydb::Table::VectorIndexSettings::VECTOR_TYPE_BIT:
            return EVectorType::Bit;
        default:
            return EVectorType::Unknown;
        }
    }; 


    auto metricFromProto = [&](const auto& proto) -> TVectorIndexSettings::TMetric {
        switch (proto.metric_case()) {
        case TProto::kDistance:
            return convertDistance(proto.distance());
        case TProto::kSimilarity:
            return convertSimilarity(proto.similarity());
        default:
            return {};
        }
    };   

    return {
        .Metric = metricFromProto(proto),
        .VectorType = convertVectorType(proto.vector_type()),
        .VectorDimension = proto.vector_dimension()
    };
}

void TVectorIndexSettings::SerializeTo(Ydb::Table::VectorIndexSettings& settings) const {
    auto convertDistance = [] (auto distance) -> auto {
        switch (distance) {
        case EDistance::Cosine:
            return Ydb::Table::VectorIndexSettings::DISTANCE_COSINE;
        case EDistance::Manhattan:
            return Ydb::Table::VectorIndexSettings::DISTANCE_MANHATTAN;
        case EDistance::Euclidean:
            return Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN;
        case EDistance::Unknown:
            return Ydb::Table::VectorIndexSettings::DISTANCE_UNSPECIFIED;
        }
    };

    auto convertSimilarity = [] (auto similarity) -> auto {
        switch (similarity) {
        case ESimilarity::Cosine:
            return Ydb::Table::VectorIndexSettings::SIMILARITY_COSINE;
        case ESimilarity::InnerProduct:
            return Ydb::Table::VectorIndexSettings::SIMILARITY_INNER_PRODUCT;
        case ESimilarity::Unknown:
            return Ydb::Table::VectorIndexSettings::SIMILARITY_UNSPECIFIED;
        }
    };

    auto convertVectorType = [] (auto vectorType) -> auto {
        switch (vectorType) {
        case EVectorType::Float:
            return Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT;
        case EVectorType::Uint8:
            return Ydb::Table::VectorIndexSettings::VECTOR_TYPE_UINT8;
        case EVectorType::Int8:
            return Ydb::Table::VectorIndexSettings::VECTOR_TYPE_INT8;
        case EVectorType::Bit:
            return Ydb::Table::VectorIndexSettings::VECTOR_TYPE_BIT;
        case EVectorType::Unknown:
            return Ydb::Table::VectorIndexSettings::VECTOR_TYPE_UNSPECIFIED;
        }
    };
    
    
    if (const auto* distance = std::get_if<EDistance>(&Metric)) {
        settings.set_distance(convertDistance(*distance));
    } else if (const auto* similarity = std::get_if<ESimilarity>(&Metric)) {
        settings.set_similarity(convertSimilarity(*similarity));
    }

    settings.set_vector_type(convertVectorType(VectorType));
    settings.set_vector_dimension(VectorDimension);
}

void TVectorIndexSettings::Out(IOutputStream& o) const {
    o << *this;
}

template <typename TProto>
TIndexDescription TIndexDescription::FromProto(const TProto& proto) {
    EIndexType type;
    TVector<TString> indexColumns;
    TVector<TString> dataColumns;
    TVector<TGlobalIndexSettings> globalIndexSettings;
    std::optional<TVectorIndexSettings> vectorIndexSettings;

    indexColumns.assign(proto.index_columns().begin(), proto.index_columns().end());
    dataColumns.assign(proto.data_columns().begin(), proto.data_columns().end());

    switch (proto.type_case()) {
    case TProto::kGlobalIndex:
        type = EIndexType::GlobalSync;
        globalIndexSettings.emplace_back(TGlobalIndexSettings::FromProto(proto.global_index().settings()));
        break;
    case TProto::kGlobalAsyncIndex:
        type = EIndexType::GlobalAsync;
        globalIndexSettings.emplace_back(TGlobalIndexSettings::FromProto(proto.global_async_index().settings()));
        break;
    case TProto::kGlobalUniqueIndex:
        type = EIndexType::GlobalUnique;
        globalIndexSettings.emplace_back(TGlobalIndexSettings::FromProto(proto.global_unique_index().settings()));
        break;
    case TProto::kGlobalVectorKmeansTreeIndex: {
        type = EIndexType::GlobalVectorKMeansTree;
        const auto &vectorProto = proto.global_vector_kmeans_tree_index();
        globalIndexSettings.emplace_back(TGlobalIndexSettings::FromProto(vectorProto.level_table_settings()));
        globalIndexSettings.emplace_back(TGlobalIndexSettings::FromProto(vectorProto.posting_table_settings()));
        vectorIndexSettings = TVectorIndexSettings::FromProto(vectorProto.vector_settings());
        break;
    }
    default: // fallback to global sync
        type = EIndexType::GlobalSync;
        globalIndexSettings.resize(1);
        break;
    }

    auto result = TIndexDescription(proto.name(), type, indexColumns, dataColumns, globalIndexSettings, vectorIndexSettings);
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
    case EIndexType::GlobalSync: {
        auto& settings = *proto.mutable_global_index()->mutable_settings();
        if (GlobalIndexSettings_.size() == 1)
            GlobalIndexSettings_[0].SerializeTo(settings);
        break;
    }
    case EIndexType::GlobalAsync: {
        auto& settings = *proto.mutable_global_async_index()->mutable_settings();
        if (GlobalIndexSettings_.size() == 1)
            GlobalIndexSettings_[0].SerializeTo(settings);
        break;
    }
    case EIndexType::GlobalUnique: {
        auto& settings = *proto.mutable_global_unique_index()->mutable_settings();
        if (GlobalIndexSettings_.size() == 1)
            GlobalIndexSettings_[0].SerializeTo(settings);
        break;
    }
    case EIndexType::GlobalVectorKMeansTree: {
        auto* global_vector_kmeans_tree_index = proto.mutable_global_vector_kmeans_tree_index();
        auto& level_settings = *global_vector_kmeans_tree_index->mutable_level_table_settings();
        auto& posting_settings = *global_vector_kmeans_tree_index->mutable_posting_table_settings();
        auto& vector_settings = *global_vector_kmeans_tree_index->mutable_vector_settings();
        if (GlobalIndexSettings_.size() == 2) {
            GlobalIndexSettings_[0].SerializeTo(level_settings);
            GlobalIndexSettings_[1].SerializeTo(posting_settings);
        }
        if (VectorIndexSettings_) {
            VectorIndexSettings_->SerializeTo(vector_settings);
        }
        break;
    }
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

    if (VectorIndexSettings_) {
        o << ", vector_settings: " << *VectorIndexSettings_ << "";
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

TChangefeedDescription::TInitialScanProgress::TInitialScanProgress()
    : PartsTotal(0)
    , PartsCompleted(0)
{}

TChangefeedDescription::TInitialScanProgress::TInitialScanProgress(ui32 total, ui32 completed)
    : PartsTotal(total)
    , PartsCompleted(completed)
{}

TChangefeedDescription::TInitialScanProgress& TChangefeedDescription::TInitialScanProgress::operator+=(const TInitialScanProgress& other) {
    PartsTotal += other.PartsTotal;
    PartsCompleted += other.PartsCompleted;
    return *this;
}

ui32 TChangefeedDescription::TInitialScanProgress::GetPartsTotal() const {
    return PartsTotal;
}

ui32 TChangefeedDescription::TInitialScanProgress::GetPartsCompleted() const {
    return PartsCompleted;
}

float TChangefeedDescription::TInitialScanProgress::GetProgress() const {
    if (PartsTotal == 0) {
        return 0;
    }

    return 100 * float(PartsCompleted) / float(PartsTotal);
}

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

const std::optional<TChangefeedDescription::TInitialScanProgress>& TChangefeedDescription::GetInitialScanProgress() const {
    return InitialScanProgress_;
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

        if (proto.has_initial_scan_progress()) {
            ret.InitialScanProgress_ = std::make_optional<TInitialScanProgress>(
                proto.initial_scan_progress().parts_total(),
                proto.initial_scan_progress().parts_completed()
            );
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

    if (InitialScanProgress_) {
        o << ", initial_scan_progress: " << InitialScanProgress_->GetProgress() << "%";
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
