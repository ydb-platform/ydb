#pragma once

#include "table_enum.h"

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>
#include <ydb/public/sdk/cpp/client/ydb_retry/retry.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>
#include <ydb/public/sdk/cpp/client/ydb_table/query_stats/stats.h>
#include <ydb/public/sdk/cpp/client/ydb_types/operation/operation.h>

#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/variant.h>

namespace Ydb {
namespace Table {

class StorageSettings;
class ColumnFamily;
class CreateTableRequest;
class Changefeed;
class ChangefeedDescription;
class DescribeTableResult;
class ExplicitPartitions;
class GlobalIndexSettings;
class VectorIndexSettings;
class PartitioningSettings;
class DateTypeColumnModeSettings;
class TtlSettings;
class TableIndex;
class TableIndexDescription;
class ValueSinceUnixEpochModeSettings;

} // namespace Table
} // namespace Ydb

namespace NYdb {

namespace NRetry::Async {
template <typename TClient, typename TStatusType>
class TRetryContext;
} // namespace NRetry::Async

namespace NRetry::Sync {
template <typename TClient, typename TStatusType>
class TRetryContext;
} // namespace NRetry::Sync

namespace NScheme {
struct TPermissions;
} // namespace NScheme

namespace NTable {

////////////////////////////////////////////////////////////////////////////////

class TKeyBound {
public:
    static TKeyBound Inclusive(const TValue& value) {
        return TKeyBound(value, true);
    }

    static TKeyBound Exclusive(const TValue& value) {
        return TKeyBound(value, false);
    }

    bool IsInclusive() const {
        return Equal_;
    }

    const TValue& GetValue() const {
        return Value_;
    }
private:
    TKeyBound(const TValue& value, bool equal = false)
        : Value_(value)
        , Equal_(equal)
    {}
    TValue Value_;
    bool Equal_;
};

class TKeyRange {
public:
    TKeyRange(const TMaybe<TKeyBound>& from, const TMaybe<TKeyBound>& to)
        : From_(from)
        , To_(to) {}

    const TMaybe<TKeyBound>& From() const {
        return From_;
    }

    const TMaybe<TKeyBound>& To() const {
        return To_;
    }
private:
    TMaybe<TKeyBound> From_;
    TMaybe<TKeyBound> To_;
};

struct TTableColumn {
    TString Name;
    TType Type;
    TString Family;
    std::optional<bool> NotNull;

    TTableColumn() = default;

    TTableColumn(TString name, TType type, TString family = TString(), std::optional<bool> notNull = std::nullopt)
        : Name(std::move(name))
        , Type(std::move(type))
        , Family(std::move(family))
        , NotNull(std::move(notNull))
    { }

    // Conversion from TColumn for API compatibility
    TTableColumn(const TColumn& column)
        : Name(column.Name)
        , Type(column.Type)
    { }

    // Conversion from TColumn for API compatibility
    TTableColumn(TColumn&& column)
        : Name(std::move(column.Name))
        , Type(std::move(column.Type))
    { }

    // Conversion to TColumn for API compatibility
    operator TColumn() const {
        return TColumn(Name, Type);
    }
};

struct TAlterTableColumn {
    TString Name;
    TString Family;

    TAlterTableColumn() = default;

    explicit TAlterTableColumn(TString name)
        : Name(std::move(name))
    { }

    TAlterTableColumn(TString name, TString family)
        : Name(std::move(name))
        , Family(std::move(family))
    { }
};

////////////////////////////////////////////////////////////////////////////////

//! Represents table partitioning settings
class TPartitioningSettings {
public:
    TPartitioningSettings();
    explicit TPartitioningSettings(const Ydb::Table::PartitioningSettings& proto);

    const Ydb::Table::PartitioningSettings& GetProto() const;

    TMaybe<bool> GetPartitioningBySize() const;
    TMaybe<bool> GetPartitioningByLoad() const;
    ui64 GetPartitionSizeMb() const;
    ui64 GetMinPartitionsCount() const;
    ui64 GetMaxPartitionsCount() const;

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

struct TExplicitPartitions {
    using TSelf = TExplicitPartitions;

    FLUENT_SETTING_VECTOR(TValue, SplitPoints);
    
    template <typename TProto>
    static TExplicitPartitions FromProto(const TProto& proto);
    
    void SerializeTo(Ydb::Table::ExplicitPartitions& proto) const;
};

struct TGlobalIndexSettings {
    using TUniformOrExplicitPartitions = std::variant<std::monostate, ui64, TExplicitPartitions>;

    TPartitioningSettings PartitioningSettings;
    TUniformOrExplicitPartitions Partitions;

    template <typename TProto>
    static TGlobalIndexSettings FromProto(const TProto& proto);

    void SerializeTo(Ydb::Table::GlobalIndexSettings& proto) const;
};

struct TVectorIndexSettings {
public:
    enum class EDistance {
        Cosine,
        Manhattan,
        Euclidean,

        Unknown = std::numeric_limits<int>::max()
    };

    enum class ESimilarity {
        Cosine,
        InnerProduct,

        Unknown = std::numeric_limits<int>::max()
    };

    enum class EVectorType {
        Float,
        Uint8,
        Int8,
        Bit,

        Unknown = std::numeric_limits<int>::max()
    };
    using TMetric = std::variant<std::monostate, EDistance, ESimilarity>;

    TMetric Metric;
    EVectorType VectorType;
    ui32 VectorDimension;

    template <typename TProto>
    static TVectorIndexSettings FromProto(const TProto& proto);

    void SerializeTo(Ydb::Table::VectorIndexSettings& settings) const;

    void Out(IOutputStream &o) const;
};

//! Represents index description
class TIndexDescription {
    friend class NYdb::TProtoAccessor;

public:
    TIndexDescription(
        const TString& name,
        EIndexType type,
        const TVector<TString>& indexColumns,
        const TVector<TString>& dataColumns = {},
        const TVector<TGlobalIndexSettings>& globalIndexSettings = {},
        const std::optional<TVectorIndexSettings>& vectorIndexSettings = {}
    );

    TIndexDescription(
        const TString& name,
        const TVector<TString>& indexColumns,
        const TVector<TString>& dataColumns = {},
        const TVector<TGlobalIndexSettings>& globalIndexSettings = {}
    );

    const TString& GetIndexName() const;
    EIndexType GetIndexType() const;
    const TVector<TString>& GetIndexColumns() const;
    const TVector<TString>& GetDataColumns() const;
    const std::optional<TVectorIndexSettings>& GetVectorIndexSettings() const;
    ui64 GetSizeBytes() const;

    void SerializeTo(Ydb::Table::TableIndex& proto) const;
    TString ToString() const;
    void Out(IOutputStream& o) const;

private:
    explicit TIndexDescription(const Ydb::Table::TableIndex& tableIndex);
    explicit TIndexDescription(const Ydb::Table::TableIndexDescription& tableIndexDesc);

    template <typename TProto>
    static TIndexDescription FromProto(const TProto& proto);

private:
    TString IndexName_;
    EIndexType IndexType_;
    TVector<TString> IndexColumns_;
    TVector<TString> DataColumns_;
    TVector<TGlobalIndexSettings> GlobalIndexSettings_;
    std::optional<TVectorIndexSettings> VectorIndexSettings_;
    ui64 SizeBytes = 0;
};

struct TRenameIndex {
    TString SourceName_;
    TString DestinationName_;
    bool ReplaceDestination_ = false;
};

bool operator==(const TIndexDescription& lhs, const TIndexDescription& rhs);
bool operator!=(const TIndexDescription& lhs, const TIndexDescription& rhs);

class TBuildIndexOperation : public TOperation {
public:
    using TOperation::TOperation;
    TBuildIndexOperation(TStatus&& status, Ydb::Operations::Operation&& operation);

    struct TMetadata {
        EBuildIndexState State;
        float Progress;
        TString Path;
        TMaybe<TIndexDescription> Desctiption;
    };

    const TMetadata& Metadata() const;
private:
    TMetadata Metadata_;
};

////////////////////////////////////////////////////////////////////////////////

//! Represents changefeed description
class TChangefeedDescription {
    friend class NYdb::TProtoAccessor;

public:
    class TInitialScanProgress {
    public:
        TInitialScanProgress();
        explicit TInitialScanProgress(ui32 total, ui32 completed);

        TInitialScanProgress& operator+=(const TInitialScanProgress& other);

        ui32 GetPartsTotal() const;
        ui32 GetPartsCompleted() const;
        float GetProgress() const; // percentage

    private:
        ui32 PartsTotal;
        ui32 PartsCompleted;
    };

public:
    TChangefeedDescription(const TString& name, EChangefeedMode mode, EChangefeedFormat format);

    // Enable virtual timestamps
    TChangefeedDescription& WithVirtualTimestamps();
    // Enable resolved timestamps
    TChangefeedDescription& WithResolvedTimestamps(const TDuration& interval);
    // Customise retention period of underlying topic (24h by default).
    TChangefeedDescription& WithRetentionPeriod(const TDuration& value);
    // Initial scan will output the current state of the table first
    TChangefeedDescription& WithInitialScan();
    // Attributes
    TChangefeedDescription& AddAttribute(const TString& key, const TString& value);
    TChangefeedDescription& SetAttributes(const THashMap<TString, TString>& attrs);
    TChangefeedDescription& SetAttributes(THashMap<TString, TString>&& attrs);
    // Value that will be emitted in the `awsRegion` field of the record in DynamoDBStreamsJson format
    TChangefeedDescription& WithAwsRegion(const TString& value);

    const TString& GetName() const;
    EChangefeedMode GetMode() const;
    EChangefeedFormat GetFormat() const;
    EChangefeedState GetState() const;
    bool GetVirtualTimestamps() const;
    const std::optional<TDuration>& GetResolvedTimestamps() const;
    bool GetInitialScan() const;
    const THashMap<TString, TString>& GetAttributes() const;
    const TString& GetAwsRegion() const;
    const std::optional<TInitialScanProgress>& GetInitialScanProgress() const;

    void SerializeTo(Ydb::Table::Changefeed& proto) const;
    TString ToString() const;
    void Out(IOutputStream& o) const;

private:
    explicit TChangefeedDescription(const Ydb::Table::Changefeed& proto);
    explicit TChangefeedDescription(const Ydb::Table::ChangefeedDescription& proto);

    template <typename TProto>
    static TChangefeedDescription FromProto(const TProto& proto);

private:
    TString Name_;
    EChangefeedMode Mode_;
    EChangefeedFormat Format_;
    EChangefeedState State_ = EChangefeedState::Unknown;
    bool VirtualTimestamps_ = false;
    std::optional<TDuration> ResolvedTimestamps_;
    std::optional<TDuration> RetentionPeriod_;
    bool InitialScan_ = false;
    THashMap<TString, TString> Attributes_;
    TString AwsRegion_;
    std::optional<TInitialScanProgress> InitialScanProgress_;
};

bool operator==(const TChangefeedDescription& lhs, const TChangefeedDescription& rhs);
bool operator!=(const TChangefeedDescription& lhs, const TChangefeedDescription& rhs);

////////////////////////////////////////////////////////////////////////////////

struct TPartitionStats {
    ui64 Rows = 0;
    ui64 Size = 0;
};

class TDateTypeColumnModeSettings {
public:
    explicit TDateTypeColumnModeSettings(const TString& columnName, const TDuration& expireAfter);
    void SerializeTo(Ydb::Table::DateTypeColumnModeSettings& proto) const;

    const TString& GetColumnName() const;
    const TDuration& GetExpireAfter() const;

private:
    TString ColumnName_;
    TDuration ExpireAfter_;
};

class TValueSinceUnixEpochModeSettings {
public:
    enum class EUnit {
        Seconds,
        MilliSeconds,
        MicroSeconds,
        NanoSeconds,

        Unknown = std::numeric_limits<int>::max()
    };

public:
    explicit TValueSinceUnixEpochModeSettings(const TString& columnName, EUnit columnUnit, const TDuration& expireAfter);
    void SerializeTo(Ydb::Table::ValueSinceUnixEpochModeSettings& proto) const;

    const TString& GetColumnName() const;
    EUnit GetColumnUnit() const;
    const TDuration& GetExpireAfter() const;

    static void Out(IOutputStream& o, EUnit unit);
    static TString ToString(EUnit unit);
    static EUnit UnitFromString(const TString& value);

private:
    TString ColumnName_;
    EUnit ColumnUnit_;
    TDuration ExpireAfter_;
};

//! Represents ttl settings
class TTtlSettings {
public:
    using EUnit = TValueSinceUnixEpochModeSettings::EUnit;

    enum class EMode {
        DateTypeColumn = 0,
        ValueSinceUnixEpoch = 1,
    };

    explicit TTtlSettings(const TString& columnName, const TDuration& expireAfter);
    explicit TTtlSettings(const Ydb::Table::DateTypeColumnModeSettings& mode, ui32 runIntervalSeconds);
    const TDateTypeColumnModeSettings& GetDateTypeColumn() const;

    explicit TTtlSettings(const TString& columnName, EUnit columnUnit, const TDuration& expireAfter);
    explicit TTtlSettings(const Ydb::Table::ValueSinceUnixEpochModeSettings& mode, ui32 runIntervalSeconds);
    const TValueSinceUnixEpochModeSettings& GetValueSinceUnixEpoch() const;

    void SerializeTo(Ydb::Table::TtlSettings& proto) const;
    EMode GetMode() const;

    TTtlSettings& SetRunInterval(const TDuration& value);
    const TDuration& GetRunInterval() const;

private:
    std::variant<
        TDateTypeColumnModeSettings,
        TValueSinceUnixEpochModeSettings
    > Mode_;
    TDuration RunInterval_ = TDuration::Zero();
};

class TAlterTtlSettings {
    using EUnit = TValueSinceUnixEpochModeSettings::EUnit;

    TAlterTtlSettings()
        : Action_(true)
    {}

    template <typename... Args>
    explicit TAlterTtlSettings(Args&&... args)
        : Action_(TTtlSettings(std::forward<Args>(args)...))
    {}

public:
    enum class EAction {
        Drop = 0,
        Set = 1,
    };

    static TAlterTtlSettings Drop() {
        return TAlterTtlSettings();
    }

    template <typename... Args>
    static TAlterTtlSettings Set(Args&&... args) {
        return TAlterTtlSettings(std::forward<Args>(args)...);
    }

    EAction GetAction() const;
    const TTtlSettings& GetTtlSettings() const;

private:
    std::variant<
        bool, // EAction::Drop
        TTtlSettings // EAction::Set
    > Action_;
};

//! Represents table storage settings
class TStorageSettings {
public:
    TStorageSettings();
    explicit TStorageSettings(const Ydb::Table::StorageSettings& proto);

    const Ydb::Table::StorageSettings& GetProto() const;

    TMaybe<TString> GetTabletCommitLog0() const;
    TMaybe<TString> GetTabletCommitLog1() const;
    TMaybe<TString> GetExternal() const;
    TMaybe<bool> GetStoreExternalBlobs() const;

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

//! Represents column family description
class TColumnFamilyDescription {
public:
    explicit TColumnFamilyDescription(const Ydb::Table::ColumnFamily& desc);

    const Ydb::Table::ColumnFamily& GetProto() const;

    const TString& GetName() const;
    TMaybe<TString> GetData() const;
    TMaybe<EColumnFamilyCompression> GetCompression() const;
    TMaybe<bool> GetKeepInMemory() const;

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

//! Represents table read replicas settings
class TReadReplicasSettings {
public:
    enum class EMode {
        PerAz = 0,
        AnyAz = 1
    };

    TReadReplicasSettings(EMode mode, ui64 readReplicasCount);

    EMode GetMode() const;
    ui64 GetReadReplicasCount() const;

private:
    EMode Mode_;
    ui64 ReadReplicasCount_;
};

struct TExplicitPartitions;
struct TDescribeTableSettings;

enum class EStoreType {
    Row = 0,
    Column = 1
};

//! Represents table description
class TTableDescription {
    friend class TTableBuilder;
    friend class NYdb::TProtoAccessor;

    using EUnit = TValueSinceUnixEpochModeSettings::EUnit;

public:
    TTableDescription(Ydb::Table::DescribeTableResult&& desc, const TDescribeTableSettings& describeSettings);

    const TVector<TString>& GetPrimaryKeyColumns() const;
    // DEPRECATED: use GetTableColumns()
    TVector<TColumn> GetColumns() const;
    TVector<TTableColumn> GetTableColumns() const;
    TVector<TIndexDescription> GetIndexDescriptions() const;
    TVector<TChangefeedDescription> GetChangefeedDescriptions() const;
    TMaybe<TTtlSettings> GetTtlSettings() const;
    TMaybe<TString> GetTiering() const;
    EStoreType GetStoreType() const;

    // Deprecated. Use GetEntry() of TDescribeTableResult instead
    const TString& GetOwner() const;
    const TVector<NScheme::TPermissions>& GetPermissions() const;
    const TVector<NScheme::TPermissions>& GetEffectivePermissions() const;

    const TVector<TKeyRange>& GetKeyRanges() const;

    // Folow options related to table statistics
    // flag WithTableStatistics must be set

    // Number of partition
    ui64 GetPartitionsCount() const;
    // Approximate number of rows
    ui64 GetTableRows() const;
    // Approximate size of table (bytes)
    ui64 GetTableSize() const;
    // Timestamp of last modification
    TInstant GetModificationTime() const;
    // Timestamp of table creation
    TInstant GetCreationTime() const;

    // Returns partition statistics for table
    // flag WithTableStatistics and WithPartitionStatistics must be set
    const TVector<TPartitionStats>& GetPartitionStats() const;

    // Returns storage settings of the table
    const TStorageSettings& GetStorageSettings() const;

    // Returns column families of the table
    const TVector<TColumnFamilyDescription>& GetColumnFamilies() const;

    // Attributes
    const THashMap<TString, TString>& GetAttributes() const;

    // Returns partitioning settings of the table
    const TPartitioningSettings& GetPartitioningSettings() const;

    // Bloom filter by key
    TMaybe<bool> GetKeyBloomFilter() const;

    // Returns read replicas settings of the table
    TMaybe<TReadReplicasSettings> GetReadReplicasSettings() const;

    // Fills CreateTableRequest proto from this description
    void SerializeTo(Ydb::Table::CreateTableRequest& request) const;

private:
    TTableDescription();
    explicit TTableDescription(const Ydb::Table::CreateTableRequest& request);

    void AddColumn(const TString& name, const Ydb::Type& type, const TString& family, std::optional<bool> notNull);
    void SetPrimaryKeyColumns(const TVector<TString>& primaryKeyColumns);

    // common
    void AddSecondaryIndex(const TString& indexName, EIndexType type, const TVector<TString>& indexColumns);
    void AddSecondaryIndex(const TString& indexName, EIndexType type, const TVector<TString>& indexColumns, const TVector<TString>& dataColumns);
    // sync
    void AddSyncSecondaryIndex(const TString& indexName, const TVector<TString>& indexColumns);
    void AddSyncSecondaryIndex(const TString& indexName, const TVector<TString>& indexColumns, const TVector<TString>& dataColumns);
    // async
    void AddAsyncSecondaryIndex(const TString& indexName, const TVector<TString>& indexColumns);
    void AddAsyncSecondaryIndex(const TString& indexName, const TVector<TString>& indexColumns, const TVector<TString>& dataColumns);
    // unique
    void AddUniqueSecondaryIndex(const TString& indexName, const TVector<TString>& indexColumns);
    void AddUniqueSecondaryIndex(const TString& indexName, const TVector<TString>& indexColumns, const TVector<TString>& dataColumns);
    // vector KMeansTree
    void AddVectorKMeansTreeSecondaryIndex(const TString& indexName, const TVector<TString>& indexColumns, const TVectorIndexSettings& vectorIndexSettings);
    void AddVectorKMeansTreeSecondaryIndex(const TString& indexName, const TVector<TString>& indexColumns, const TVector<TString>& dataColumns, const TVectorIndexSettings& vectorIndexSettings);

    // default
    void AddSecondaryIndex(const TString& indexName, const TVector<TString>& indexColumns);
    void AddSecondaryIndex(const TString& indexName, const TVector<TString>& indexColumns, const TVector<TString>& dataColumns);

    void SetTtlSettings(TTtlSettings&& settings);
    void SetTtlSettings(const TTtlSettings& settings);

    void SetStorageSettings(const TStorageSettings& settings);
    void AddColumnFamily(const TColumnFamilyDescription& desc);
    void AddAttribute(const TString& key, const TString& value);
    void SetAttributes(const THashMap<TString, TString>& attrs);
    void SetAttributes(THashMap<TString, TString>&& attrs);
    void SetCompactionPolicy(const TString& name);
    void SetUniformPartitions(ui64 partitionsCount);
    void SetPartitionAtKeys(const TExplicitPartitions& keys);
    void SetPartitioningSettings(const TPartitioningSettings& settings);
    void SetKeyBloomFilter(bool enabled);
    void SetReadReplicasSettings(TReadReplicasSettings::EMode mode, ui64 readReplicasCount);
    void SetStoreType(EStoreType type);
    const Ydb::Table::DescribeTableResult& GetProto() const;

    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

class TStorageSettingsBuilder {
public:
    TStorageSettingsBuilder();
    ~TStorageSettingsBuilder();

    TStorageSettingsBuilder& SetTabletCommitLog0(const TString& media);
    TStorageSettingsBuilder& SetTabletCommitLog1(const TString& media);
    TStorageSettingsBuilder& SetExternal(const TString& media);
    TStorageSettingsBuilder& SetStoreExternalBlobs(bool enabled);

    TStorageSettings Build() const;

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

class TPartitioningSettingsBuilder {
public:
    TPartitioningSettingsBuilder();
    ~TPartitioningSettingsBuilder();

    TPartitioningSettingsBuilder& SetPartitioningBySize(bool enabled);
    TPartitioningSettingsBuilder& SetPartitioningByLoad(bool enabled);
    TPartitioningSettingsBuilder& SetPartitionSizeMb(ui64 sizeMb);
    TPartitioningSettingsBuilder& SetMinPartitionsCount(ui64 count);
    TPartitioningSettingsBuilder& SetMaxPartitionsCount(ui64 count);

    TPartitioningSettings Build() const;

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

class TColumnFamilyBuilder {
public:
    explicit TColumnFamilyBuilder(const TString& name);
    ~TColumnFamilyBuilder();

    TColumnFamilyBuilder& SetData(const TString& media);
    TColumnFamilyBuilder& SetCompression(EColumnFamilyCompression compression);

    TColumnFamilyDescription Build() const;

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

class TTableBuilder;

class TTableStorageSettingsBuilder {
public:
    explicit TTableStorageSettingsBuilder(TTableBuilder& parent)
        : Parent_(parent)
    { }

    TTableStorageSettingsBuilder& SetTabletCommitLog0(const TString& media) {
        Builder_.SetTabletCommitLog0(media);
        return *this;
    }

    TTableStorageSettingsBuilder& SetTabletCommitLog1(const TString& media) {
        Builder_.SetTabletCommitLog1(media);
        return *this;
    }

    TTableStorageSettingsBuilder& SetExternal(const TString& media) {
        Builder_.SetExternal(media);
        return *this;
    }

    TTableStorageSettingsBuilder& SetStoreExternalBlobs(bool enabled) {
        Builder_.SetStoreExternalBlobs(enabled);
        return *this;
    }

    TTableBuilder& EndStorageSettings();

private:
    TTableBuilder& Parent_;
    TStorageSettingsBuilder Builder_;
};

class TTableColumnFamilyBuilder {
public:
    TTableColumnFamilyBuilder(TTableBuilder& parent, const TString& name)
        : Parent_(parent)
        , Builder_(name)
    { }

    TTableColumnFamilyBuilder& SetData(const TString& media) {
        Builder_.SetData(media);
        return *this;
    }

    TTableColumnFamilyBuilder& SetCompression(EColumnFamilyCompression compression) {
        Builder_.SetCompression(compression);
        return *this;
    }

    TTableBuilder& EndColumnFamily();

private:
    TTableBuilder& Parent_;
    TColumnFamilyBuilder Builder_;
};

class TTablePartitioningSettingsBuilder {
public:
    explicit TTablePartitioningSettingsBuilder(TTableBuilder& parent)
        : Parent_(parent)
    { }

    TTablePartitioningSettingsBuilder& SetPartitioningBySize(bool enabled) {
        Builder_.SetPartitioningBySize(enabled);
        return *this;
    }

    TTablePartitioningSettingsBuilder& SetPartitioningByLoad(bool enabled) {
        Builder_.SetPartitioningByLoad(enabled);
        return *this;
    }

    TTablePartitioningSettingsBuilder& SetPartitionSizeMb(ui64 sizeMb) {
        Builder_.SetPartitionSizeMb(sizeMb);
        return *this;
    }

    TTablePartitioningSettingsBuilder& SetMinPartitionsCount(ui64 count) {
        Builder_.SetMinPartitionsCount(count);
        return *this;
    }

    TTablePartitioningSettingsBuilder& SetMaxPartitionsCount(ui64 count) {
        Builder_.SetMaxPartitionsCount(count);
        return *this;
    }

    TTableBuilder& EndPartitioningSettings();

private:
    TTableBuilder& Parent_;
    TPartitioningSettingsBuilder Builder_;
};

////////////////////////////////////////////////////////////////////////////////

class TTableBuilder {
    using EUnit = TValueSinceUnixEpochModeSettings::EUnit;

public:
    TTableBuilder() = default;

    TTableBuilder& SetStoreType(EStoreType type);

    TTableBuilder& AddNullableColumn(const TString& name, const EPrimitiveType& type, const TString& family = TString());
    TTableBuilder& AddNullableColumn(const TString& name, const TDecimalType& type, const TString& family = TString());
    TTableBuilder& AddNullableColumn(const TString& name, const TPgType& type, const TString& family = TString());
    TTableBuilder& AddNonNullableColumn(const TString& name, const EPrimitiveType& type, const TString& family = TString());
    TTableBuilder& AddNonNullableColumn(const TString& name, const TDecimalType& type, const TString& family = TString());
    TTableBuilder& AddNonNullableColumn(const TString& name, const TPgType& type, const TString& family = TString());
    TTableBuilder& SetPrimaryKeyColumns(const TVector<TString>& primaryKeyColumns);
    TTableBuilder& SetPrimaryKeyColumn(const TString& primaryKeyColumn);

    // common
    TTableBuilder& AddSecondaryIndex(const TString& indexName, EIndexType type, const TVector<TString>& indexColumns, const TVector<TString>& dataColumns);
    TTableBuilder& AddSecondaryIndex(const TString& indexName, EIndexType type, const TVector<TString>& indexColumns);
    TTableBuilder& AddSecondaryIndex(const TString& indexName, EIndexType type, const TString& indexColumn);

    // sync
    TTableBuilder& AddSyncSecondaryIndex(const TString& indexName, const TVector<TString>& indexColumns, const TVector<TString>& dataColumns);
    TTableBuilder& AddSyncSecondaryIndex(const TString& indexName, const TVector<TString>& indexColumns);
    TTableBuilder& AddSyncSecondaryIndex(const TString& indexName, const TString& indexColumn);

    // async
    TTableBuilder& AddAsyncSecondaryIndex(const TString& indexName, const TVector<TString>& indexColumns, const TVector<TString>& dataColumns);
    TTableBuilder& AddAsyncSecondaryIndex(const TString& indexName, const TVector<TString>& indexColumns);
    TTableBuilder& AddAsyncSecondaryIndex(const TString& indexName, const TString& indexColumn);

    // unique
    TTableBuilder& AddUniqueSecondaryIndex(const TString& indexName, const TVector<TString>& indexColumns);
    TTableBuilder& AddUniqueSecondaryIndex(const TString& indexName, const TVector<TString>& indexColumns, const TVector<TString>& dataColumns);

    // vector KMeansTree
    TTableBuilder& AddVectorKMeansTreeSecondaryIndex(const TString& indexName, const TVector<TString>& indexColumns, const TVectorIndexSettings& vectorIndexSettings);
    TTableBuilder& AddVectorKMeansTreeSecondaryIndex(const TString& indexName, const TVector<TString>& indexColumns, const TVector<TString>& dataColumns, const TVectorIndexSettings& vectorIndexSettings);

    // default
    TTableBuilder& AddSecondaryIndex(const TString& indexName, const TVector<TString>& indexColumns, const TVector<TString>& dataColumns);
    TTableBuilder& AddSecondaryIndex(const TString& indexName, const TVector<TString>& indexColumns);
    TTableBuilder& AddSecondaryIndex(const TString& indexName, const TString& indexColumn);

    TTableBuilder& SetTtlSettings(TTtlSettings&& settings);
    TTableBuilder& SetTtlSettings(const TTtlSettings& settings);
    TTableBuilder& SetTtlSettings(const TString& columnName, const TDuration& expireAfter = TDuration::Zero());
    TTableBuilder& SetTtlSettings(const TString& columnName, EUnit columnUnit, const TDuration& expireAfter = TDuration::Zero());

    TTableBuilder& SetStorageSettings(const TStorageSettings& settings);

    TTableBuilder& AddColumnFamily(const TColumnFamilyDescription& desc);

    TTableBuilder& AddAttribute(const TString& key, const TString& value);
    TTableBuilder& SetAttributes(const THashMap<TString, TString>& attrs);
    TTableBuilder& SetAttributes(THashMap<TString, TString>&& attrs);

    TTableBuilder& SetCompactionPolicy(const TString& name);

    // UniformPartitions and PartitionAtKeys are mutually exclusive
    TTableBuilder& SetUniformPartitions(ui64 partitionsCount);
    TTableBuilder& SetPartitionAtKeys(const TExplicitPartitions& keys);

    TTableBuilder& SetPartitioningSettings(const TPartitioningSettings& settings);

    TTableBuilder& SetKeyBloomFilter(bool enabled);

    TTableBuilder& SetReadReplicasSettings(TReadReplicasSettings::EMode mode, ui64 readReplicasCount);

    TTableStorageSettingsBuilder BeginStorageSettings() {
        return TTableStorageSettingsBuilder(*this);
    }

    TTableColumnFamilyBuilder BeginColumnFamily(const TString& name) {
        return TTableColumnFamilyBuilder(*this, name);
    }

    TTablePartitioningSettingsBuilder BeginPartitioningSettings() {
        return TTablePartitioningSettingsBuilder(*this);
    }

    TTableDescription Build();

private:
    TTableDescription TableDescription_;
};

inline TTableBuilder& TTableStorageSettingsBuilder::EndStorageSettings() {
    return Parent_.SetStorageSettings(Builder_.Build());
}

inline TTableBuilder& TTableColumnFamilyBuilder::EndColumnFamily() {
    return Parent_.AddColumnFamily(Builder_.Build());
}

inline TTableBuilder& TTablePartitioningSettingsBuilder::EndPartitioningSettings() {
    return Parent_.SetPartitioningSettings(Builder_.Build());
}

////////////////////////////////////////////////////////////////////////////////

class TCopyItem {
public:
    TCopyItem(const TString& source, const TString& destination);

    const TString& SourcePath() const;
    const TString& DestinationPath() const;

    TCopyItem& SetOmitIndexes();
    bool OmitIndexes() const;
private:
    TString Source_;
    TString Destination_;
    bool OmitIndexes_;
};

////////////////////////////////////////////////////////////////////////////////

class TRenameItem {
public:
    TRenameItem(const TString& source, const TString& destination);

    const TString& SourcePath() const;
    const TString& DestinationPath() const;

    TRenameItem& SetReplaceDestination();
    bool ReplaceDestination() const;
private:
    TString Source_;
    TString Destination_;
    bool ReplaceDestination_;
};

////////////////////////////////////////////////////////////////////////////////

class TCreateSessionResult;
class TDataQueryResult;
class TTablePartIterator;
class TPrepareQueryResult;
class TExplainQueryResult;
class TDescribeTableResult;
class TBeginTransactionResult;
class TCommitTransactionResult;
class TKeepAliveResult;
class TBulkUpsertResult;
class TReadRowsResult;
class TScanQueryPartIterator;

using TAsyncCreateSessionResult = NThreading::TFuture<TCreateSessionResult>;
using TAsyncDataQueryResult = NThreading::TFuture<TDataQueryResult>;
using TAsyncPrepareQueryResult = NThreading::TFuture<TPrepareQueryResult>;
using TAsyncExplainDataQueryResult = NThreading::TFuture<TExplainQueryResult>;
using TAsyncDescribeTableResult = NThreading::TFuture<TDescribeTableResult>;
using TAsyncBeginTransactionResult = NThreading::TFuture<TBeginTransactionResult>;
using TAsyncCommitTransactionResult = NThreading::TFuture<TCommitTransactionResult>;
using TAsyncTablePartIterator = NThreading::TFuture<TTablePartIterator>;
using TAsyncKeepAliveResult = NThreading::TFuture<TKeepAliveResult>;
using TAsyncBulkUpsertResult = NThreading::TFuture<TBulkUpsertResult>;
using TAsyncReadRowsResult = NThreading::TFuture<TReadRowsResult>;
using TAsyncScanQueryPartIterator = NThreading::TFuture<TScanQueryPartIterator>;

////////////////////////////////////////////////////////////////////////////////

struct TCreateSessionSettings : public TOperationRequestSettings<TCreateSessionSettings> {};

using TBackoffSettings = NYdb::NRetry::TBackoffSettings;
using TRetryOperationSettings = NYdb::NRetry::TRetryOperationSettings;

struct TSessionPoolSettings {
    using TSelf = TSessionPoolSettings;

    // Max number of sessions client can get from session pool
    FLUENT_SETTING_DEFAULT(ui32, MaxActiveSessions, 50);

    // Max number of attempt to create session inside session pool
    // to handle OVERLOADED error
    FLUENT_SETTING_DEFAULT(ui32, RetryLimit, 5);

    // Max time session to be in idle state in session pool before
    // keep alive start to touch it
    FLUENT_SETTING_DEFAULT(TDuration, KeepAliveIdleThreshold, TDuration::Minutes(5));

    // Max time session to be in idle state before closing
    FLUENT_SETTING_DEFAULT(TDuration, CloseIdleThreshold, TDuration::Minutes(1));

    // Min number of session in session pool.
    // Sessions will not be closed by CloseIdleThreshold if the number of sessions less then this limit.
    FLUENT_SETTING_DEFAULT(ui32, MinPoolSize, 10);
};

struct TClientSettings : public TCommonClientSettingsBase<TClientSettings> {
    using TSelf = TClientSettings;
    using TSessionPoolSettings = TSessionPoolSettings;

    // Enable client query cache. Client query cache is used to map query text to
    // prepared query id for ExecuteDataQuery calls on client side.
    // Starting from YDB 20-4, server query cache is enabled by default, which
    // make use of client cache unnecessary. Use of server cache is preferred
    // as it doesn't require client-server synchronization and can recompile
    // query on demand without client interaction.
    // The recommended value is False.
    FLUENT_SETTING_DEFAULT(bool, UseQueryCache, false);
    FLUENT_SETTING_DEFAULT(ui32, QueryCacheSize, 1000);
    FLUENT_SETTING_DEFAULT(bool, KeepDataQueryText, true);

    // Min allowed session variation coefficient (%) to start session balancing.
    // Variation coefficient is a ratio of the standard deviation sigma to the mean
    // Example:
    //   - 3 hosts with [90, 100, 110] sessions per host. Cv will be 10%
    //   - add new host ([90, 100, 110, 0] sessions per host). Cv will be 77%
    // Balancing is will be performed if calculated cv greater than MinSessionCV
    // Zero - disable this feature
    FLUENT_SETTING_DEFAULT(ui32, MinSessionCV, 20);

    // Allow migrate requests between session during session balancing
    FLUENT_SETTING_DEFAULT(bool, AllowRequestMigration, true);

    // Settings of session pool
    FLUENT_SETTING(TSessionPoolSettings, SessionPoolSettings);
};

struct TBulkUpsertSettings : public TOperationRequestSettings<TBulkUpsertSettings> {
    // Format setting proto serialized into string. If not set format defaults are used.
    // I.e. it's Ydb.Table.CsvSettings for CSV.
    FLUENT_SETTING_DEFAULT(TString, FormatSettings, "");
};

struct TReadRowsSettings : public TOperationRequestSettings<TReadRowsSettings> {
};

struct TStreamExecScanQuerySettings : public TRequestSettings<TStreamExecScanQuerySettings> {
    // Return query plan without actual query execution
    FLUENT_SETTING_DEFAULT(bool, Explain, false);

    // Collect runtime statistics with a given detalization mode
    FLUENT_SETTING_DEFAULT(ECollectQueryStatsMode, CollectQueryStats, ECollectQueryStatsMode::None);

    // Collect full query compilation diagnostics
    FLUENT_SETTING_DEFAULT(bool, CollectFullDiagnostics, false);
};

class TSession;
class TSessionPool;

enum class EDataFormat {
    ApacheArrow = 1,
    CSV = 2,
};

class TTableClient {
    friend class TSession;
    friend class TTransaction;
    friend class TSessionPool;
    friend class NRetry::Sync::TRetryContext<TTableClient, TStatus>;
    friend class NRetry::Async::TRetryContext<TTableClient, TAsyncStatus>;

public:
    using TOperationFunc = std::function<TAsyncStatus(TSession session)>;
    using TOperationSyncFunc = std::function<TStatus(TSession session)>;
    using TOperationWithoutSessionFunc = std::function<TAsyncStatus(TTableClient& tableClient)>;
    using TOperationWithoutSessionSyncFunc = std::function<TStatus(TTableClient& tableClient)>;
    using TSettings = TClientSettings;
    using TSession = TSession;
    using TCreateSessionSettings = TCreateSessionSettings;
    using TAsyncCreateSessionResult = TAsyncCreateSessionResult;

public:
    TTableClient(const TDriver& driver, const TClientSettings& settings = TClientSettings());

    //! Creates new session
    TAsyncCreateSessionResult CreateSession(const TCreateSessionSettings& settings = TCreateSessionSettings());

    //! Returns session from session pool,
    //! if all sessions are occupied will be generated session with CLIENT_RESOURCE_EXHAUSTED status.
    TAsyncCreateSessionResult GetSession(const TCreateSessionSettings& settings = TCreateSessionSettings());

    //! Returns number of active sessions given via session pool
    i64 GetActiveSessionCount() const;

    //! Returns the maximum number of sessions in session pool
    i64 GetActiveSessionsLimit() const;

    //! Returns the size of session pool
    i64 GetCurrentPoolSize() const;

    //! Returns new table builder
    TTableBuilder GetTableBuilder();
    //! Returns new params builder
    TParamsBuilder GetParamsBuilder() const;
    //! Returns new type builder
    TTypeBuilder GetTypeBuilder();

    TAsyncStatus RetryOperation(TOperationFunc&& operation,
        const TRetryOperationSettings& settings = TRetryOperationSettings());

    template<typename TResult>
    TAsyncStatus RetryOperation(std::function<NThreading::TFuture<TResult>(TSession session)>&& operation,
        const TRetryOperationSettings& settings = TRetryOperationSettings());

    template<typename TResult>
    TAsyncStatus RetryOperation(const std::function<NThreading::TFuture<TResult>(TSession session)>& operation,
        const TRetryOperationSettings& settings = TRetryOperationSettings());

    TStatus RetryOperationSync(const TOperationSyncFunc& operation,
        const TRetryOperationSettings& settings = TRetryOperationSettings());

    TAsyncStatus RetryOperation(TOperationWithoutSessionFunc&& operation,
        const TRetryOperationSettings& settings = TRetryOperationSettings());

    template<typename TResult>
    TAsyncStatus RetryOperation(std::function<NThreading::TFuture<TResult>(TTableClient& tableClient)>&& operation,
        const TRetryOperationSettings& settings = TRetryOperationSettings());

    template<typename TResult>
    TAsyncStatus RetryOperation(const std::function<NThreading::TFuture<TResult>(TTableClient& tableClient)>& operation,
        const TRetryOperationSettings& settings = TRetryOperationSettings());

    TStatus RetryOperationSync(const TOperationWithoutSessionSyncFunc& operation,
        const TRetryOperationSettings& settings = TRetryOperationSettings());

    //! Stop all client internal routines, drain session pools
    //! Sessions returned to the session pool after this call will be closed
    //! Using the client after call this method causes UB
    NThreading::TFuture<void> Stop();

    //! Non-transactional fast bulk write.
    //! Interanlly it uses an implicit session and thus doesn't need a session to be passed.
    //! "rows" parameter must be a list of structs where each stuct represents one row.
    //! It must contain all key columns but not necessarily all non-key columns.
    //! Similar to UPSERT statement only values of specified columns will be updated.
    TAsyncBulkUpsertResult BulkUpsert(const TString& table, TValue&& rows,
        const TBulkUpsertSettings& settings = TBulkUpsertSettings());
    TAsyncBulkUpsertResult BulkUpsert(const TString& table, EDataFormat format,
        const TString& data, const TString& schema = {}, const TBulkUpsertSettings& settings = TBulkUpsertSettings());

    TAsyncReadRowsResult ReadRows(const TString& table, TValue&& keys, const TVector<TString>& columns = {},
        const TReadRowsSettings& settings = TReadRowsSettings());

    TAsyncScanQueryPartIterator StreamExecuteScanQuery(const TString& query,
        const TStreamExecScanQuerySettings& settings = TStreamExecScanQuerySettings());

    TAsyncScanQueryPartIterator StreamExecuteScanQuery(const TString& query, const TParams& params,
        const TStreamExecScanQuerySettings& settings = TStreamExecScanQuerySettings());

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

class TTransaction;

struct TTxOnlineSettings {
    using TSelf = TTxOnlineSettings;

    TTxOnlineSettings() {}

    FLUENT_SETTING_DEFAULT(bool, AllowInconsistentReads, false);
};

class TTxSettings {
    friend class TTableClient;

public:
    using TSelf = TTxSettings;

    TTxSettings()
        : Mode_(TS_SERIALIZABLE_RW) {}

    static TTxSettings SerializableRW() {
        return TTxSettings(TS_SERIALIZABLE_RW);
    }

    static TTxSettings OnlineRO(const TTxOnlineSettings& settings = TTxOnlineSettings()) {
        return TTxSettings(TS_ONLINE_RO).OnlineSettings(settings);
    }

    static TTxSettings StaleRO() {
        return TTxSettings(TS_STALE_RO);
    }

    static TTxSettings SnapshotRO() {
        return TTxSettings(TS_SNAPSHOT_RO);
    }

    void Out(IOutputStream& out) const {
        switch (Mode_) {
        case TS_SERIALIZABLE_RW:
            out << "SerializableRW";
            break;
        case TS_ONLINE_RO:
            out << "OnlineRO";
            break;
        case TS_STALE_RO:
            out << "StaleRO";
            break;
        case TS_SNAPSHOT_RO:
            out << "SnapshotRO";
            break;
        default:
            out << "Unknown";
            break;
        }
    }

private:
    enum ETransactionMode {
        TS_SERIALIZABLE_RW,
        TS_ONLINE_RO,
        TS_STALE_RO,
        TS_SNAPSHOT_RO
    };

    FLUENT_SETTING(TTxOnlineSettings, OnlineSettings);

private:
    TTxSettings(ETransactionMode mode)
        : Mode_(mode) {}

    ETransactionMode Mode_;
};

class TTxControl {
    friend class TTableClient;

public:
    using TSelf = TTxControl;

    static TTxControl Tx(const TTransaction& tx) {
        return TTxControl(tx);
    }

    static TTxControl BeginTx(const TTxSettings& settings = TTxSettings()) {
        return TTxControl(settings);
    }

    FLUENT_SETTING_FLAG(CommitTx);

private:
    TTxControl(const TTransaction& tx);
    TTxControl(const TTxSettings& begin);

private:
    TMaybe<TString> TxId_;
    TTxSettings BeginTx_;
};

enum class EAutoPartitioningPolicy {
    Disabled = 1,
    AutoSplit = 2,
    AutoSplitMerge = 3
};

////////////////////////////////////////////////////////////////////////////////

struct TColumnFamilyPolicy {
    using TSelf = TColumnFamilyPolicy;

    FLUENT_SETTING_OPTIONAL(TString, Name);

    FLUENT_SETTING_OPTIONAL(TString, Data);

    FLUENT_SETTING_OPTIONAL(TString, External);

    FLUENT_SETTING_OPTIONAL(bool, KeepInMemory);

    FLUENT_SETTING_OPTIONAL(bool, Compressed);
};

struct TStoragePolicy {
    using TSelf = TStoragePolicy;

    FLUENT_SETTING_OPTIONAL(TString, PresetName);

    FLUENT_SETTING_OPTIONAL(TString, SysLog);

    FLUENT_SETTING_OPTIONAL(TString, Log);

    FLUENT_SETTING_OPTIONAL(TString, Data);

    FLUENT_SETTING_OPTIONAL(TString, External);

    FLUENT_SETTING_VECTOR(TColumnFamilyPolicy, ColumnFamilies);
};

struct TPartitioningPolicy {
    using TSelf = TPartitioningPolicy;

    FLUENT_SETTING_OPTIONAL(TString, PresetName);

    FLUENT_SETTING_OPTIONAL(EAutoPartitioningPolicy, AutoPartitioning);

    FLUENT_SETTING_OPTIONAL(ui64, UniformPartitions);

    FLUENT_SETTING_OPTIONAL(TExplicitPartitions, ExplicitPartitions);
};

struct TReplicationPolicy {
    using TSelf = TReplicationPolicy;

    FLUENT_SETTING_OPTIONAL(TString, PresetName);

    FLUENT_SETTING_OPTIONAL(ui32, ReplicasCount);

    FLUENT_SETTING_OPTIONAL(bool, CreatePerAvailabilityZone);

    FLUENT_SETTING_OPTIONAL(bool, AllowPromotion);
};

////////////////////////////////////////////////////////////////////////////////

struct TCreateTableSettings : public TOperationRequestSettings<TCreateTableSettings> {
    using TSelf = TCreateTableSettings;

    FLUENT_SETTING_OPTIONAL(TString, PresetName);

    FLUENT_SETTING_OPTIONAL(TString, ExecutionPolicy);

    FLUENT_SETTING_OPTIONAL(TString, CompactionPolicy);

    FLUENT_SETTING_OPTIONAL(TPartitioningPolicy, PartitioningPolicy);

    FLUENT_SETTING_OPTIONAL(TStoragePolicy, StoragePolicy);

    FLUENT_SETTING_OPTIONAL(TReplicationPolicy, ReplicationPolicy);
};

////////////////////////////////////////////////////////////////////////////////

struct TDropTableSettings : public TOperationRequestSettings<TDropTableSettings> {
    using TOperationRequestSettings<TDropTableSettings>::TOperationRequestSettings;
};

////////////////////////////////////////////////////////////////////////////////

struct TAlterTableSettings;

class TAlterStorageSettingsBuilder {
public:
    explicit TAlterStorageSettingsBuilder(TAlterTableSettings& parent)
        : Parent_(parent)
    { }

    TAlterStorageSettingsBuilder& SetTabletCommitLog0(const TString& media) {
        Builder_.SetTabletCommitLog0(media);
        return *this;
    }

    TAlterStorageSettingsBuilder& SetTabletCommitLog1(const TString& media) {
        Builder_.SetTabletCommitLog1(media);
        return *this;
    }

    TAlterStorageSettingsBuilder& SetExternal(const TString& media) {
        Builder_.SetExternal(media);
        return *this;
    }

    TAlterStorageSettingsBuilder& SetStoreExternalBlobs(bool enabled) {
        Builder_.SetStoreExternalBlobs(enabled);
        return *this;
    }

    TAlterTableSettings& EndAlterStorageSettings();

private:
    TAlterTableSettings& Parent_;
    TStorageSettingsBuilder Builder_;
};

class TAlterColumnFamilyBuilder {
public:
    TAlterColumnFamilyBuilder(TAlterTableSettings& parent, const TString& name)
        : Parent_(parent)
        , Builder_(name)
    { }

    TAlterColumnFamilyBuilder& SetData(const TString& media) {
        Builder_.SetData(media);
        return *this;
    }

    TAlterColumnFamilyBuilder& SetCompression(EColumnFamilyCompression compression) {
        Builder_.SetCompression(compression);
        return *this;
    }

    TAlterTableSettings& EndAddColumnFamily();
    TAlterTableSettings& EndAlterColumnFamily();

private:
    TAlterTableSettings& Parent_;
    TColumnFamilyBuilder Builder_;
};

class TAlterTtlSettingsBuilder {
    using EUnit = TValueSinceUnixEpochModeSettings::EUnit;

public:
    TAlterTtlSettingsBuilder(TAlterTableSettings& parent);

    TAlterTtlSettingsBuilder& Drop();
    TAlterTtlSettingsBuilder& Set(TTtlSettings&& settings);
    TAlterTtlSettingsBuilder& Set(const TTtlSettings& settings);
    TAlterTtlSettingsBuilder& Set(const TString& columnName, const TDuration& expireAfter = TDuration::Zero());
    TAlterTtlSettingsBuilder& Set(const TString& columnName, EUnit columnUnit, const TDuration& expireAfter = TDuration::Zero());

    TAlterTableSettings& EndAlterTtlSettings();

private:
    TAlterTableSettings& Parent_;

    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

class TAlterAttributesBuilder {
public:
    TAlterAttributesBuilder(TAlterTableSettings& parent)
        : Parent_(parent)
    { }

    TAlterAttributesBuilder& Alter(const TString& key, const TString& value) {
        AlterAttributes_[key] = value;
        return *this;
    }

    TAlterAttributesBuilder& Add(const TString& key, const TString& value) {
        return Alter(key, value);
    }

    TAlterAttributesBuilder& Drop(const TString& key) {
        return Alter(key, "");
    }

    TAlterTableSettings& EndAlterAttributes();

private:
    TAlterTableSettings& Parent_;
    THashMap<TString, TString> AlterAttributes_;
};

class TAlterPartitioningSettingsBuilder {
public:
    explicit TAlterPartitioningSettingsBuilder(TAlterTableSettings& parent)
        : Parent_(parent)
    { }

    TAlterPartitioningSettingsBuilder& SetPartitioningBySize(bool enabled) {
        Builder_.SetPartitioningBySize(enabled);
        return *this;
    }

    TAlterPartitioningSettingsBuilder& SetPartitioningByLoad(bool enabled) {
        Builder_.SetPartitioningByLoad(enabled);
        return *this;
    }

    TAlterPartitioningSettingsBuilder& SetPartitionSizeMb(ui64 sizeMb) {
        Builder_.SetPartitionSizeMb(sizeMb);
        return *this;
    }

    TAlterPartitioningSettingsBuilder& SetMinPartitionsCount(ui64 count) {
        Builder_.SetMinPartitionsCount(count);
        return *this;
    }

    TAlterPartitioningSettingsBuilder& SetMaxPartitionsCount(ui64 count) {
        Builder_.SetMaxPartitionsCount(count);
        return *this;
    }

    TAlterTableSettings& EndAlterPartitioningSettings();

private:
    TAlterTableSettings& Parent_;
    TPartitioningSettingsBuilder Builder_;
};

struct TAlterTableSettings : public TOperationRequestSettings<TAlterTableSettings> {
    using TSelf = TAlterTableSettings;
    using TAlterAttributes = THashMap<TString, TString>;

    TAlterTableSettings();

    FLUENT_SETTING_VECTOR(TTableColumn, AddColumns);

    FLUENT_SETTING_VECTOR(TString, DropColumns);

    FLUENT_SETTING_VECTOR(TAlterTableColumn, AlterColumns);

    FLUENT_SETTING_VECTOR(TIndexDescription, AddIndexes);
    FLUENT_SETTING_VECTOR(TString, DropIndexes);
    FLUENT_SETTING_VECTOR(TRenameIndex, RenameIndexes);

    FLUENT_SETTING_VECTOR(TChangefeedDescription, AddChangefeeds);
    FLUENT_SETTING_VECTOR(TString, DropChangefeeds);

    TSelf& AlterColumnFamily(TString name, TString family) {
        AlterColumns_.emplace_back(std::move(name), std::move(family));
        return *this;
    }

    FLUENT_SETTING_OPTIONAL(TStorageSettings, AlterStorageSettings);

    FLUENT_SETTING_VECTOR(TColumnFamilyDescription, AddColumnFamilies);
    FLUENT_SETTING_VECTOR(TColumnFamilyDescription, AlterColumnFamilies);

    // workaround for MSVC
    TSelf& AlterTtlSettings(const TMaybe<TAlterTtlSettings>& value);
    const TMaybe<TAlterTtlSettings>& GetAlterTtlSettings() const;

    FLUENT_SETTING(TAlterAttributes, AlterAttributes);

    FLUENT_SETTING(TString, SetCompactionPolicy);

    FLUENT_SETTING_OPTIONAL(TPartitioningSettings, AlterPartitioningSettings);

    FLUENT_SETTING_OPTIONAL(bool, SetKeyBloomFilter);

    FLUENT_SETTING_OPTIONAL(TReadReplicasSettings, SetReadReplicasSettings);
    TSelf& SetReadReplicasSettings(TReadReplicasSettings::EMode mode, ui64 readReplicasCount) {
        SetReadReplicasSettings_ = TReadReplicasSettings(mode, readReplicasCount);
        return *this;
    }

    TAlterStorageSettingsBuilder BeginAlterStorageSettings() {
        return TAlterStorageSettingsBuilder(*this);
    }

    TAlterColumnFamilyBuilder BeginAddColumnFamily(const TString& name) {
        return TAlterColumnFamilyBuilder(*this, name);
    }

    TAlterColumnFamilyBuilder BeginAlterColumnFamily(const TString& name) {
        return TAlterColumnFamilyBuilder(*this, name);
    }

    TAlterTtlSettingsBuilder BeginAlterTtlSettings() {
        return TAlterTtlSettingsBuilder(*this);
    }

    TAlterAttributesBuilder BeginAlterAttributes() {
        return TAlterAttributesBuilder(*this);
    }

    TAlterPartitioningSettingsBuilder BeginAlterPartitioningSettings() {
        return TAlterPartitioningSettingsBuilder(*this);
    }

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

inline TAlterTableSettings& TAlterStorageSettingsBuilder::EndAlterStorageSettings() {
    return Parent_.AlterStorageSettings(Builder_.Build());
}

inline TAlterTableSettings& TAlterColumnFamilyBuilder::EndAddColumnFamily() {
    return Parent_.AppendAddColumnFamilies(Builder_.Build());
}

inline TAlterTableSettings& TAlterColumnFamilyBuilder::EndAlterColumnFamily() {
    return Parent_.AppendAlterColumnFamilies(Builder_.Build());
}

inline TAlterTableSettings& TAlterAttributesBuilder::EndAlterAttributes() {
    return Parent_.AlterAttributes(AlterAttributes_);
}

inline TAlterTableSettings& TAlterPartitioningSettingsBuilder::EndAlterPartitioningSettings() {
    return Parent_.AlterPartitioningSettings(Builder_.Build());
}

////////////////////////////////////////////////////////////////////////////////

struct TCopyTableSettings : public TOperationRequestSettings<TCopyTableSettings> {};

struct TCopyTablesSettings : public TOperationRequestSettings<TCopyTablesSettings> {};

struct TRenameTablesSettings : public TOperationRequestSettings<TRenameTablesSettings> {};

struct TDescribeTableSettings : public TOperationRequestSettings<TDescribeTableSettings> {
    FLUENT_SETTING_DEFAULT(bool, WithKeyShardBoundary, false);
    FLUENT_SETTING_DEFAULT(bool, WithTableStatistics, false);
    FLUENT_SETTING_DEFAULT(bool, WithPartitionStatistics, false);
};

struct TExplainDataQuerySettings : public TOperationRequestSettings<TExplainDataQuerySettings> {
    FLUENT_SETTING_DEFAULT(bool, WithCollectFullDiagnostics, false);
};

struct TPrepareDataQuerySettings : public TOperationRequestSettings<TPrepareDataQuerySettings> {};

struct TExecDataQuerySettings : public TOperationRequestSettings<TExecDataQuerySettings> {
    FLUENT_SETTING_OPTIONAL(bool, KeepInQueryCache);

    FLUENT_SETTING_OPTIONAL(ECollectQueryStatsMode, CollectQueryStats);
};

struct TExecSchemeQuerySettings : public TOperationRequestSettings<TExecSchemeQuerySettings> {};

struct TBeginTxSettings : public TOperationRequestSettings<TBeginTxSettings> {};

struct TCommitTxSettings : public TOperationRequestSettings<TCommitTxSettings> {
    FLUENT_SETTING_OPTIONAL(ECollectQueryStatsMode, CollectQueryStats);
};

struct TRollbackTxSettings : public TOperationRequestSettings<TRollbackTxSettings> {};

struct TCloseSessionSettings : public TOperationRequestSettings<TCloseSessionSettings> {};

struct TKeepAliveSettings : public TOperationRequestSettings<TKeepAliveSettings> {};

struct TReadTableSettings : public TRequestSettings<TReadTableSettings> {

    using TSelf = TReadTableSettings;

    FLUENT_SETTING_OPTIONAL(TKeyBound, From);

    FLUENT_SETTING_OPTIONAL(TKeyBound, To);

    FLUENT_SETTING_VECTOR(TString, Columns);

    FLUENT_SETTING_FLAG(Ordered);

    FLUENT_SETTING_OPTIONAL(ui64, RowLimit);

    FLUENT_SETTING_OPTIONAL(bool, UseSnapshot);

    FLUENT_SETTING_OPTIONAL(ui64, BatchLimitBytes);

    FLUENT_SETTING_OPTIONAL(ui64, BatchLimitRows);

    FLUENT_SETTING_OPTIONAL(bool, ReturnNotNullAsOptional);
};

//! Represents all session operations
//! Session is transparent logic representation of connection
class TSession {
    friend class TTableClient;
    friend class TDataQuery;
    friend class TTransaction;
    friend class TSessionPool;

public:
    //! The following methods perform corresponding calls.
    //! Results are NThreading::TFuture<T> where T is corresponding result.
    TAsyncStatus CreateTable(const TString& path, TTableDescription&& tableDesc,
        const TCreateTableSettings& settings = TCreateTableSettings());

    TAsyncStatus DropTable(const TString& path, const TDropTableSettings& settings = TDropTableSettings());

    TAsyncStatus AlterTable(const TString& path, const TAlterTableSettings& settings = TAlterTableSettings());

    // Same as AlterTable but may return operation in case of long running
    TAsyncOperation AlterTableLong(const TString& path, const TAlterTableSettings& settings = TAlterTableSettings());

    TAsyncStatus CopyTable(const TString& src, const TString& dst,
        const TCopyTableSettings& settings = TCopyTableSettings());

    TAsyncStatus CopyTables(const TVector<TCopyItem>& copyItems,
        const TCopyTablesSettings& settings = TCopyTablesSettings());

    TAsyncStatus RenameTables(const TVector<TRenameItem>& renameItems,
        const TRenameTablesSettings& settings = TRenameTablesSettings());

    TAsyncDescribeTableResult DescribeTable(const TString& path,
        const TDescribeTableSettings& settings = TDescribeTableSettings());

    TAsyncBeginTransactionResult BeginTransaction(const TTxSettings& txSettings = TTxSettings(),
        const TBeginTxSettings& settings = TBeginTxSettings());

    TAsyncExplainDataQueryResult ExplainDataQuery(const TString& query,
        const TExplainDataQuerySettings& settings = TExplainDataQuerySettings());

    TAsyncPrepareQueryResult PrepareDataQuery(const TString& query,
        const TPrepareDataQuerySettings& settings = TPrepareDataQuerySettings());

    TAsyncDataQueryResult ExecuteDataQuery(const TString& query, const TTxControl& txControl,
        const TExecDataQuerySettings& settings = TExecDataQuerySettings());

    TAsyncDataQueryResult ExecuteDataQuery(const TString& query, const TTxControl& txControl,
        const TParams& params, const TExecDataQuerySettings& settings = TExecDataQuerySettings());

    TAsyncDataQueryResult ExecuteDataQuery(const TString& query, const TTxControl& txControl,
        TParams&& params, const TExecDataQuerySettings& settings = TExecDataQuerySettings());

    TAsyncStatus ExecuteSchemeQuery(const TString& query,
        const TExecSchemeQuerySettings& settings = TExecSchemeQuerySettings());

    TAsyncTablePartIterator ReadTable(const TString& path,
        const TReadTableSettings& settings = TReadTableSettings());

    TAsyncStatus Close(const TCloseSessionSettings& settings = TCloseSessionSettings());

    TAsyncKeepAliveResult KeepAlive(const TKeepAliveSettings& settings = TKeepAliveSettings());

    void InvalidateQueryCache();

    //! Returns new table builder
    TTableBuilder GetTableBuilder();
    //! Returns new params builder
    TParamsBuilder GetParamsBuilder();
    //! Returns new type builder
    TTypeBuilder GetTypeBuilder();
    //! Returns session id
    const TString& GetId() const;

    class TImpl;
private:
    TSession(std::shared_ptr<TTableClient::TImpl> client, const TString& sessionId, const TString& endpointId, bool isOwnedBySessionPool);
    TSession(std::shared_ptr<TTableClient::TImpl> client, std::shared_ptr<TSession::TImpl> SessionImpl_);

    std::shared_ptr<TTableClient::TImpl> Client_;
    std::shared_ptr<TSession::TImpl> SessionImpl_;
};

////////////////////////////////////////////////////////////////////////////////

template<typename TResult>
TAsyncStatus TTableClient::RetryOperation(
    std::function<NThreading::TFuture<TResult>(TSession session)>&& operation,
    const TRetryOperationSettings& settings)
{
    return RetryOperation([operation = std::move(operation)] (TSession session) {
        return operation(session).Apply([] (const NThreading::TFuture<TResult>& result) {
            return NThreading::MakeFuture<TStatus>(result.GetValue());
        });
    }, settings);
}

template<typename TResult>
TAsyncStatus TTableClient::RetryOperation(
    const std::function<NThreading::TFuture<TResult>(TSession session)>& operation,
    const TRetryOperationSettings& settings)
{
    return RetryOperation([operation] (TSession session) {
        return operation(session).Apply([] (const NThreading::TFuture<TResult>& result) {
            return NThreading::MakeFuture<TStatus>(result.GetValue());
        });
    }, settings);
}

template<typename TResult>
TAsyncStatus TTableClient::RetryOperation(
    std::function<NThreading::TFuture<TResult>(TTableClient& tableClient)>&& operation,
    const TRetryOperationSettings& settings)
{
    return RetryOperation([operation = std::move(operation)] (TTableClient& tableClient) {
        return operation(tableClient).Apply([] (const NThreading::TFuture<TResult>& result) {
            return NThreading::MakeFuture<TStatus>(result.GetValue());
        });
    }, settings);
}

template<typename TResult>
TAsyncStatus TTableClient::RetryOperation(
    const std::function<NThreading::TFuture<TResult>(TTableClient& tableClient)>& operation,
    const TRetryOperationSettings& settings)
{
    return RetryOperation([operation] (TTableClient& tableClient) {
        return operation(tableClient).Apply([] (const NThreading::TFuture<TResult>& result) {
            return NThreading::MakeFuture<TStatus>(result.GetValue());
        });
    }, settings);
}

////////////////////////////////////////////////////////////////////////////////

//! Represents data transaction
class TTransaction {
    friend class TTableClient;
public:
    const TString& GetId() const {
        return TxId_;
    }

    bool IsActive() const {
        return !TxId_.empty();
    }

    TAsyncCommitTransactionResult Commit(const TCommitTxSettings& settings = TCommitTxSettings());
    TAsyncStatus Rollback(const TRollbackTxSettings& settings = TRollbackTxSettings());

    TSession GetSession() const {
        return Session_;
    }

private:
    TTransaction(const TSession& session, const TString& txId);

    TSession Session_;
    TString TxId_;
};

////////////////////////////////////////////////////////////////////////////////

//! Represents query identificator (e.g. used for prepared query)
class TDataQuery {
    friend class TTableClient;
    friend class TSession;

public:
    const TString& GetId() const;
    const TMaybe<TString>& GetText() const;
    TParamsBuilder GetParamsBuilder() const;

    TAsyncDataQueryResult Execute(const TTxControl& txControl,
        const TExecDataQuerySettings& settings = TExecDataQuerySettings());

    TAsyncDataQueryResult Execute(const TTxControl& txControl, const TParams& params,
        const TExecDataQuerySettings& settings = TExecDataQuerySettings());

    TAsyncDataQueryResult Execute(const TTxControl& txControl, TParams&& params,
        const TExecDataQuerySettings& settings = TExecDataQuerySettings());

private:
    TDataQuery(const TSession& session, const TString& text, const TString& id);
    TDataQuery(const TSession& session, const TString& text, const TString& id,
        const ::google::protobuf::Map<TString, Ydb::Type>& types);

    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

//! Represents result of PrepareDataQuery call.
//! If result is successful TDataQuery should be used for next
//! ExecuteDataQuery calls
class TPrepareQueryResult : public TStatus {
public:
    TPrepareQueryResult(TStatus&& status, const TDataQuery& query, bool fromCache);

    TDataQuery GetQuery() const;
    bool IsQueryFromCache() const;

private:
    TDataQuery PreparedQuery_;
    bool FromCache_;
};

//! Represents result of ExplainDataQuery call
class TExplainQueryResult : public TStatus {
public:
    TExplainQueryResult(TStatus&& status, TString&& plan, TString&& ast, TString&& diagnostics);

    const TString& GetPlan() const;
    const TString& GetAst() const;
    const TString& GetDiagnostics() const;

private:
    TString Plan_;
    TString Ast_;
    TString Diagnostics_;
};

//! Represents result of DescribeTable call
class TDescribeTableResult : public NScheme::TDescribePathResult {
public:
    TDescribeTableResult(TStatus&& status, Ydb::Table::DescribeTableResult&& desc,
        const TDescribeTableSettings& describeSettings);

    TTableDescription GetTableDescription() const;

private:
    TTableDescription TableDescription_;
};

class TDataQueryResult : public TStatus {
public:
    TDataQueryResult(TStatus&& status, TVector<TResultSet>&& resultSets, const TMaybe<TTransaction>& transaction,
        const TMaybe<TDataQuery>& dataQuery, bool fromCache, const TMaybe<TQueryStats>& queryStats);

    const TVector<TResultSet>& GetResultSets() const;
    TVector<TResultSet> ExtractResultSets() &&;
    TResultSet GetResultSet(size_t resultIndex) const;

    TResultSetParser GetResultSetParser(size_t resultIndex) const;

    TMaybe<TTransaction> GetTransaction() const;

    TMaybe<TDataQuery> GetQuery() const;
    bool IsQueryFromCache() const;

    const TMaybe<TQueryStats>& GetStats() const;

    const TString GetQueryPlan() const;

private:
    TMaybe<TTransaction> Transaction_;
    TVector<TResultSet> ResultSets_;
    TMaybe<TDataQuery> DataQuery_;
    bool FromCache_;
    TMaybe<TQueryStats> QueryStats_;
};

class TReadTableSnapshot {
public:
    TReadTableSnapshot(ui64 step, ui64 txId)
        : Step_(step)
        , TxId_(txId)
    {}

    ui64 GetStep() const { return Step_; }
    ui64 GetTxId() const { return TxId_; }

private:
    ui64 Step_;
    ui64 TxId_;
};

template<typename TPart>
class TSimpleStreamPart : public TStreamPartStatus {
public:
    const TPart& GetPart() const { return Part_; }

    TPart ExtractPart() { return std::move(Part_); }

    TSimpleStreamPart(TPart&& part, TStatus&& status,
            std::optional<TReadTableSnapshot> snapshot = std::nullopt)
        : TStreamPartStatus(std::move(status))
        , Part_(std::move(part))
        , Snapshot_(std::move(snapshot))
    {}

    const std::optional<TReadTableSnapshot>& GetSnapshot() { return Snapshot_; }

private:
    TPart Part_;
    std::optional<TReadTableSnapshot> Snapshot_;
};

template<typename TPart>
using TAsyncSimpleStreamPart = NThreading::TFuture<TSimpleStreamPart<TPart>>;

class TTablePartIterator : public TStatus {
    friend class TSession;
public:
    TAsyncSimpleStreamPart<TResultSet> ReadNext();
    class TReaderImpl;
private:
    TTablePartIterator(
        std::shared_ptr<TReaderImpl> impl,
        TPlainStatus&& status
    );
    std::shared_ptr<TReaderImpl> ReaderImpl_;
};

using TReadTableResultPart = TSimpleStreamPart<TResultSet>;

class TScanQueryPart : public TStreamPartStatus {
public:
    bool HasResultSet() const { return ResultSet_.Defined(); }
    const TResultSet& GetResultSet() const { return *ResultSet_; }
    TResultSet ExtractResultSet() { return std::move(*ResultSet_); }

    bool HasQueryStats() const { return QueryStats_.Defined(); }
    const TQueryStats& GetQueryStats() const { return *QueryStats_; }
    TQueryStats ExtractQueryStats() { return std::move(*QueryStats_); }

    bool HasDiagnostics() const { return Diagnostics_.Defined(); }
    const TString& GetDiagnostics() const { return *Diagnostics_; }
    TString&& ExtractDiagnostics() { return std::move(*Diagnostics_); }

    TScanQueryPart(TStatus&& status)
        : TStreamPartStatus(std::move(status))
    {}

    TScanQueryPart(TStatus&& status, const TMaybe<TQueryStats>& queryStats, const TMaybe<TString>& diagnostics)
        : TStreamPartStatus(std::move(status))
        , QueryStats_(queryStats)
        , Diagnostics_(diagnostics)
    {}

    TScanQueryPart(TStatus&& status, TResultSet&& resultSet, const TMaybe<TQueryStats>& queryStats, const TMaybe<TString>& diagnostics)
        : TStreamPartStatus(std::move(status))
        , ResultSet_(std::move(resultSet))
        , QueryStats_(queryStats)
        , Diagnostics_(diagnostics)
    {}

private:
    TMaybe<TResultSet> ResultSet_;
    TMaybe<TQueryStats> QueryStats_;
    TMaybe<TString> Diagnostics_;
};

using TAsyncScanQueryPart = NThreading::TFuture<TScanQueryPart>;

class TScanQueryPartIterator : public TStatus {
    friend class TTableClient;
public:
    TAsyncScanQueryPart ReadNext();
    class TReaderImpl;
private:
    TScanQueryPartIterator(
        std::shared_ptr<TReaderImpl> impl,
        TPlainStatus&& status
    );
    std::shared_ptr<TReaderImpl> ReaderImpl_;
};

class TBeginTransactionResult : public TStatus {
public:
    TBeginTransactionResult(TStatus&& status, TTransaction transaction);

    const TTransaction& GetTransaction() const;

private:
    TTransaction Transaction_;
};

class TCommitTransactionResult : public TStatus {
public:
    TCommitTransactionResult(TStatus&& status, const TMaybe<TQueryStats>& queryStats);

    const TMaybe<TQueryStats>& GetStats() const;

private:
    TMaybe<TQueryStats> QueryStats_;
};

class TCreateSessionResult: public TStatus {
    friend class TSession::TImpl;
public:
    TCreateSessionResult(TStatus&& status, TSession&& session);
    TSession GetSession() const;

private:
    TSession Session_;
};

enum class ESessionStatus {
    Unspecified = 0,
    Ready = 1,
    Busy = 2
};

class TKeepAliveResult : public TStatus {
public:
    TKeepAliveResult(TStatus&& status, ESessionStatus sessionStatus);
    ESessionStatus GetSessionStatus() const;
private:
    ESessionStatus SessionStatus;
};

class TBulkUpsertResult : public TStatus {
public:
    explicit TBulkUpsertResult(TStatus&& status);
};

class TReadRowsResult : public TStatus {
    TResultSet ResultSet;

  public:
    explicit TReadRowsResult(TStatus&& status, TResultSet&& resultSet);

    TResultSet GetResultSet() {
        return std::move(ResultSet);
    }
};

} // namespace NTable
} // namespace NYdb
