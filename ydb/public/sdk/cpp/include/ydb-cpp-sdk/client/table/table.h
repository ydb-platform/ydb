#pragma once

#include "fwd.h"

#include "table_enum.h"

#include <ydb-cpp-sdk/client/driver/driver.h>
#include <ydb-cpp-sdk/client/params/params.h>
#include <ydb-cpp-sdk/client/result/result.h>
#include <ydb-cpp-sdk/client/retry/retry.h>
#include <ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb-cpp-sdk/client/table/query_stats/stats.h>
#include <ydb-cpp-sdk/client/types/operation/operation.h>

#include <variant>

namespace Ydb {
namespace Table {

class StorageSettings;
class ColumnFamily;
class CreateTableRequest;
class Changefeed;
class ChangefeedDescription;
class DescribeExternalDataSourceResult;
class DescribeExternalTableResult;
class DescribeTableResult;
class ExplicitPartitions;
class GlobalIndexSettings;
class VectorIndexSettings;
class KMeansTreeSettings;
class PartitioningSettings;
class DateTypeColumnModeSettings;
class TtlSettings;
class TtlTier;
class TableIndex;
class TableIndexDescription;
class ValueSinceUnixEpochModeSettings;
class EvictionToExternalStorageSettings;

} // namespace Table
} // namespace Ydb

namespace NYdb::inline Dev {

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
    TKeyRange(const std::optional<TKeyBound>& from, const std::optional<TKeyBound>& to)
        : From_(from)
        , To_(to) {}

    const std::optional<TKeyBound>& From() const {
        return From_;
    }

    const std::optional<TKeyBound>& To() const {
        return To_;
    }
private:
    std::optional<TKeyBound> From_;
    std::optional<TKeyBound> To_;
};

struct TSequenceDescription {
    struct TSetVal {
        int64_t NextValue;
        bool NextUsed;
    };
    std::optional<TSetVal> SetVal;
};

struct TTableColumn {
    std::string Name;
    TType Type;
    std::string Family;
    std::optional<bool> NotNull;
    std::optional<TSequenceDescription> SequenceDescription;

    TTableColumn() = default;

    TTableColumn(std::string name,
                 TType type,
                 std::string family = std::string(),
                 std::optional<bool> notNull = std::nullopt,
                 std::optional<TSequenceDescription> sequenceDescription = std::nullopt)
        : Name(std::move(name))
        , Type(std::move(type))
        , Family(std::move(family))
        , NotNull(std::move(notNull))
        , SequenceDescription(std::move(sequenceDescription))
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
    std::string Name;
    std::string Family;

    TAlterTableColumn() = default;

    explicit TAlterTableColumn(std::string name)
        : Name(std::move(name))
    { }

    TAlterTableColumn(std::string name, std::string family)
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

    std::optional<bool> GetPartitioningBySize() const;
    std::optional<bool> GetPartitioningByLoad() const;
    uint64_t GetPartitionSizeMb() const;
    uint64_t GetMinPartitionsCount() const;
    uint64_t GetMaxPartitionsCount() const;

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

struct TExplicitPartitions {
    using TSelf = TExplicitPartitions;

    FLUENT_SETTING_VECTOR(TValue, SplitPoints);

    static TExplicitPartitions FromProto(const Ydb::Table::ExplicitPartitions& proto);

    void SerializeTo(Ydb::Table::ExplicitPartitions& proto) const;
};

struct TGlobalIndexSettings {
    using TUniformOrExplicitPartitions = std::variant<std::monostate, uint64_t, TExplicitPartitions>;

    TPartitioningSettings PartitioningSettings;
    TUniformOrExplicitPartitions Partitions;

    static TGlobalIndexSettings FromProto(const Ydb::Table::GlobalIndexSettings& proto);

    void SerializeTo(Ydb::Table::GlobalIndexSettings& proto) const;
};

struct TVectorIndexSettings {
public:
    enum class EMetric {
        Unspecified = 0,
        InnerProduct,
        CosineSimilarity,
        CosineDistance,
        Manhattan,
        Euclidean,
    };

    enum class EVectorType {
        Unspecified = 0,
        Float,
        Uint8,
        Int8,
        Bit,
    };

    EMetric Metric = EMetric::Unspecified;
    EVectorType VectorType = EVectorType::Unspecified;
    uint32_t VectorDimension = 0;

    static TVectorIndexSettings FromProto(const Ydb::Table::VectorIndexSettings& proto);

    void SerializeTo(Ydb::Table::VectorIndexSettings& settings) const;

    void Out(IOutputStream &o) const;
};

struct TKMeansTreeSettings {
public:
    enum class EMetric {
        Unspecified = 0,
        InnerProduct,
        CosineSimilarity,
        CosineDistance,
        Manhattan,
        Euclidean,
    };

    enum class EVectorType {
        Unspecified = 0,
        Float,
        Uint8,
        Int8,
        Bit,
    };

    TVectorIndexSettings Settings;
    uint32_t Clusters = 0;
    uint32_t Levels = 0;

    static TKMeansTreeSettings FromProto(const Ydb::Table::KMeansTreeSettings& proto);

    void SerializeTo(Ydb::Table::KMeansTreeSettings& settings) const;

    void Out(IOutputStream &o) const;
};

//! Represents index description
class TIndexDescription {
    friend class NYdb::TProtoAccessor;

public:
    TIndexDescription(
        const std::string& name,
        EIndexType type,
        const std::vector<std::string>& indexColumns,
        const std::vector<std::string>& dataColumns = {},
        const std::vector<TGlobalIndexSettings>& globalIndexSettings = {},
        const std::variant<std::monostate, TKMeansTreeSettings>& specializedIndexSettings = {}
    );

    TIndexDescription(
        const std::string& name,
        const std::vector<std::string>& indexColumns,
        const std::vector<std::string>& dataColumns = {},
        const std::vector<TGlobalIndexSettings>& globalIndexSettings = {}
    );

    const std::string& GetIndexName() const;
    EIndexType GetIndexType() const;
    const std::vector<std::string>& GetIndexColumns() const;
    const std::vector<std::string>& GetDataColumns() const;
    const std::variant<std::monostate, TKMeansTreeSettings>& GetIndexSettings() const;
    uint64_t GetSizeBytes() const;

    void SerializeTo(Ydb::Table::TableIndex& proto) const;
    std::string ToString() const;
    void Out(IOutputStream& o) const;

private:
    explicit TIndexDescription(const Ydb::Table::TableIndex& tableIndex);
    explicit TIndexDescription(const Ydb::Table::TableIndexDescription& tableIndexDesc);

    template <typename TProto>
    static TIndexDescription FromProto(const TProto& proto);

private:
    std::string IndexName_;
    EIndexType IndexType_;
    std::vector<std::string> IndexColumns_;
    std::vector<std::string> DataColumns_;
    std::vector<TGlobalIndexSettings> GlobalIndexSettings_;
    std::variant<std::monostate, TKMeansTreeSettings> SpecializedIndexSettings_;
    uint64_t SizeBytes_ = 0;
};

struct TRenameIndex {
    std::string SourceName_;
    std::string DestinationName_;
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
        std::string Path;
        std::optional<TIndexDescription> Desctiption;
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
        explicit TInitialScanProgress(uint32_t total, uint32_t completed);

        TInitialScanProgress& operator+=(const TInitialScanProgress& other);

        uint32_t GetPartsTotal() const;
        uint32_t GetPartsCompleted() const;
        float GetProgress() const; // percentage

    private:
        uint32_t PartsTotal;
        uint32_t PartsCompleted;
    };

public:
    TChangefeedDescription(const std::string& name, EChangefeedMode mode, EChangefeedFormat format);

    // Enable virtual timestamps
    TChangefeedDescription& WithVirtualTimestamps();
    // Enable resolved timestamps
    TChangefeedDescription& WithResolvedTimestamps(const TDuration& interval);
    // Customise retention period of underlying topic (24h by default).
    TChangefeedDescription& WithRetentionPeriod(const TDuration& value);
    // Initial scan will output the current state of the table first
    TChangefeedDescription& WithInitialScan();
    // Attributes
    TChangefeedDescription& AddAttribute(const std::string& key, const std::string& value);
    TChangefeedDescription& SetAttributes(const std::unordered_map<std::string, std::string>& attrs);
    TChangefeedDescription& SetAttributes(std::unordered_map<std::string, std::string>&& attrs);
    // Value that will be emitted in the `awsRegion` field of the record in DynamoDBStreamsJson format
    TChangefeedDescription& WithAwsRegion(const std::string& value);

    const std::string& GetName() const;
    EChangefeedMode GetMode() const;
    EChangefeedFormat GetFormat() const;
    EChangefeedState GetState() const;
    bool GetVirtualTimestamps() const;
    const std::optional<TDuration>& GetResolvedTimestamps() const;
    bool GetInitialScan() const;
    const std::unordered_map<std::string, std::string>& GetAttributes() const;
    const std::string& GetAwsRegion() const;
    const std::optional<TInitialScanProgress>& GetInitialScanProgress() const;

    void SerializeTo(Ydb::Table::Changefeed& proto) const;
    void SerializeTo(Ydb::Table::ChangefeedDescription& proto) const;
    std::string ToString() const;
    void Out(IOutputStream& o) const;

    template <typename TProto>
    void SerializeCommonFields(TProto& proto) const;

private:
    explicit TChangefeedDescription(const Ydb::Table::Changefeed& proto);
    explicit TChangefeedDescription(const Ydb::Table::ChangefeedDescription& proto);

    template <typename TProto>
    static TChangefeedDescription FromProto(const TProto& proto);

private:
    std::string Name_;
    EChangefeedMode Mode_;
    EChangefeedFormat Format_;
    EChangefeedState State_ = EChangefeedState::Unknown;
    bool VirtualTimestamps_ = false;
    std::optional<TDuration> ResolvedTimestamps_;
    std::optional<TDuration> RetentionPeriod_;
    bool InitialScan_ = false;
    std::unordered_map<std::string, std::string> Attributes_;
    std::string AwsRegion_;
    std::optional<TInitialScanProgress> InitialScanProgress_;
};

bool operator==(const TChangefeedDescription& lhs, const TChangefeedDescription& rhs);
bool operator!=(const TChangefeedDescription& lhs, const TChangefeedDescription& rhs);

////////////////////////////////////////////////////////////////////////////////

struct TPartitionStats {
    uint64_t Rows = 0;
    uint64_t Size = 0;
    uint32_t LeaderNodeId = 0;
};

class TDateTypeColumnModeSettings {
public:
    explicit TDateTypeColumnModeSettings(const std::string& columnName, const TDuration& applyAfter);
    void SerializeTo(Ydb::Table::DateTypeColumnModeSettings& proto) const;

    const std::string& GetColumnName() const;
    const TDuration& GetExpireAfter() const;

private:
    std::string ColumnName_;
    TDuration ApplyAfter_;
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
    explicit TValueSinceUnixEpochModeSettings(const std::string& columnName, EUnit columnUnit, const TDuration& applyAfter);
    void SerializeTo(Ydb::Table::ValueSinceUnixEpochModeSettings& proto) const;

    const std::string& GetColumnName() const;
    EUnit GetColumnUnit() const;
    const TDuration& GetExpireAfter() const;

    static void Out(IOutputStream& o, EUnit unit);
    static std::string ToString(EUnit unit);
    static EUnit UnitFromString(const std::string& value);

private:
    std::string ColumnName_;
    EUnit ColumnUnit_;
    TDuration ApplyAfter_;
};

class TTtlDeleteAction {};
class TTtlEvictToExternalStorageAction {
public:
    TTtlEvictToExternalStorageAction(const std::string& storageName);
    void SerializeTo(Ydb::Table::EvictionToExternalStorageSettings& proto) const;

    std::string GetStorage() const;

private:
    std::string Storage_;
};

class TTtlTierSettings {
public:
    using TExpression = std::variant<
        TDateTypeColumnModeSettings,
        TValueSinceUnixEpochModeSettings
    >;

    using TAction = std::variant<
        TTtlDeleteAction,
        TTtlEvictToExternalStorageAction
    >;

public:
    explicit TTtlTierSettings(const TExpression& expression, const TAction& action);

    static std::optional<TTtlTierSettings> FromProto(const Ydb::Table::TtlTier& tier);
    void SerializeTo(Ydb::Table::TtlTier& proto) const;

    const TExpression& GetExpression() const;
    const TAction& GetAction() const;

private:
    TExpression Expression_;
    TAction Action_;
};

//! Represents ttl settings
class TTtlSettings {
private:
    using TMode = std::variant<
        TDateTypeColumnModeSettings,
        TValueSinceUnixEpochModeSettings
    >;

public:
    using EUnit = TValueSinceUnixEpochModeSettings::EUnit;

    enum class EMode {
        DateTypeColumn = 0,
        ValueSinceUnixEpoch = 1,
    };

    explicit TTtlSettings(const std::vector<TTtlTierSettings>& tiers);

    explicit TTtlSettings(const std::string& columnName, const TDuration& expireAfter);
    const TDateTypeColumnModeSettings& GetDateTypeColumn() const;
    explicit TTtlSettings(const Ydb::Table::DateTypeColumnModeSettings& mode, uint32_t runIntervalSeconds);

    explicit TTtlSettings(const std::string& columnName, EUnit columnUnit, const TDuration& expireAfter);
    const TValueSinceUnixEpochModeSettings& GetValueSinceUnixEpoch() const;
    explicit TTtlSettings(const Ydb::Table::ValueSinceUnixEpochModeSettings& mode, uint32_t runIntervalSeconds);

    static std::optional<TTtlSettings> FromProto(const Ydb::Table::TtlSettings& proto);
    void SerializeTo(Ydb::Table::TtlSettings& proto) const;
    EMode GetMode() const;

    TTtlSettings& SetRunInterval(const TDuration& value);
    const TDuration& GetRunInterval() const;

    const std::vector<TTtlTierSettings>& GetTiers() const;

private:
    std::vector<TTtlTierSettings> Tiers_;
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

    std::optional<std::string> GetTabletCommitLog0() const;
    std::optional<std::string> GetTabletCommitLog1() const;
    std::optional<std::string> GetExternal() const;
    std::optional<bool> GetStoreExternalBlobs() const;

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

//! Represents column family description
class TColumnFamilyDescription {
public:
    explicit TColumnFamilyDescription(const Ydb::Table::ColumnFamily& desc);

    const Ydb::Table::ColumnFamily& GetProto() const;

    const std::string& GetName() const;
    std::optional<std::string> GetData() const;
    std::optional<EColumnFamilyCompression> GetCompression() const;
    std::optional<bool> GetKeepInMemory() const;

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

    TReadReplicasSettings(EMode mode, uint64_t readReplicasCount);

    EMode GetMode() const;
    uint64_t GetReadReplicasCount() const;

private:
    EMode Mode_;
    uint64_t ReadReplicasCount_;
};

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

    const std::vector<std::string>& GetPrimaryKeyColumns() const;
    // DEPRECATED: use GetTableColumns()
    std::vector<TColumn> GetColumns() const;
    std::vector<TTableColumn> GetTableColumns() const;
    std::vector<TIndexDescription> GetIndexDescriptions() const;
    std::vector<TChangefeedDescription> GetChangefeedDescriptions() const;
    std::optional<TTtlSettings> GetTtlSettings() const;
    // Deprecated. Use GetTtlSettings() instead
    std::optional<std::string> GetTiering() const;
    EStoreType GetStoreType() const;

    // Deprecated. Use GetEntry() of TDescribeTableResult instead
    const std::string& GetOwner() const;
    const std::vector<NScheme::TPermissions>& GetPermissions() const;
    const std::vector<NScheme::TPermissions>& GetEffectivePermissions() const;

    const std::vector<TKeyRange>& GetKeyRanges() const;

    // Folow options related to table statistics
    // flag WithTableStatistics must be set

    // Number of partition
    uint64_t GetPartitionsCount() const;
    // Approximate number of rows
    uint64_t GetTableRows() const;
    // Approximate size of table (bytes)
    uint64_t GetTableSize() const;
    // Timestamp of last modification
    TInstant GetModificationTime() const;
    // Timestamp of table creation
    TInstant GetCreationTime() const;

    // Returns partition statistics for table
    // flag WithTableStatistics and WithPartitionStatistics must be set
    const std::vector<TPartitionStats>& GetPartitionStats() const;

    // Returns storage settings of the table
    const TStorageSettings& GetStorageSettings() const;

    // Returns column families of the table
    const std::vector<TColumnFamilyDescription>& GetColumnFamilies() const;

    // Attributes
    const std::unordered_map<std::string, std::string>& GetAttributes() const;

    // Returns partitioning settings of the table
    const TPartitioningSettings& GetPartitioningSettings() const;

    // Bloom filter by key
    std::optional<bool> GetKeyBloomFilter() const;

    // Returns read replicas settings of the table
    std::optional<TReadReplicasSettings> GetReadReplicasSettings() const;

    // Fills CreateTableRequest proto from this description
    void SerializeTo(Ydb::Table::CreateTableRequest& request) const;

private:
    TTableDescription();
    explicit TTableDescription(const Ydb::Table::CreateTableRequest& request);

    void AddColumn(const std::string& name, const Ydb::Type& type, const std::string& family, std::optional<bool> notNull, std::optional<TSequenceDescription> sequenceDescription);
    void SetPrimaryKeyColumns(const std::vector<std::string>& primaryKeyColumns);

    // common
    void AddSecondaryIndex(const std::string& indexName, EIndexType type, const std::vector<std::string>& indexColumns);
    void AddSecondaryIndex(const std::string& indexName, EIndexType type, const std::vector<std::string>& indexColumns, const std::vector<std::string>& dataColumns);
    void AddSecondaryIndex(const TIndexDescription& indexDescription);
    // sync
    void AddSyncSecondaryIndex(const std::string& indexName, const std::vector<std::string>& indexColumns);
    void AddSyncSecondaryIndex(const std::string& indexName, const std::vector<std::string>& indexColumns, const std::vector<std::string>& dataColumns);
    // async
    void AddAsyncSecondaryIndex(const std::string& indexName, const std::vector<std::string>& indexColumns);
    void AddAsyncSecondaryIndex(const std::string& indexName, const std::vector<std::string>& indexColumns, const std::vector<std::string>& dataColumns);
    // unique
    void AddUniqueSecondaryIndex(const std::string& indexName, const std::vector<std::string>& indexColumns);
    void AddUniqueSecondaryIndex(const std::string& indexName, const std::vector<std::string>& indexColumns, const std::vector<std::string>& dataColumns);
    // vector KMeansTree
    void AddVectorKMeansTreeIndex(const std::string& indexName, const std::vector<std::string>& indexColumns, const TKMeansTreeSettings& indexSettings);
    void AddVectorKMeansTreeIndex(const std::string& indexName, const std::vector<std::string>& indexColumns, const std::vector<std::string>& dataColumns, const TKMeansTreeSettings& indexSettings);

    // default
    void AddSecondaryIndex(const std::string& indexName, const std::vector<std::string>& indexColumns);
    void AddSecondaryIndex(const std::string& indexName, const std::vector<std::string>& indexColumns, const std::vector<std::string>& dataColumns);

    void SetTtlSettings(TTtlSettings&& settings);
    void SetTtlSettings(const TTtlSettings& settings);

    void SetStorageSettings(const TStorageSettings& settings);
    void AddColumnFamily(const TColumnFamilyDescription& desc);
    void AddAttribute(const std::string& key, const std::string& value);
    void SetAttributes(const std::unordered_map<std::string, std::string>& attrs);
    void SetAttributes(std::unordered_map<std::string, std::string>&& attrs);
    void SetCompactionPolicy(const std::string& name);
    void SetUniformPartitions(uint64_t partitionsCount);
    void SetPartitionAtKeys(const TExplicitPartitions& keys);
    void SetPartitioningSettings(const TPartitioningSettings& settings);
    void SetKeyBloomFilter(bool enabled);
    void SetReadReplicasSettings(TReadReplicasSettings::EMode mode, uint64_t readReplicasCount);
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

    TStorageSettingsBuilder& SetTabletCommitLog0(const std::string& media);
    TStorageSettingsBuilder& SetTabletCommitLog1(const std::string& media);
    TStorageSettingsBuilder& SetExternal(const std::string& media);
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
    TPartitioningSettingsBuilder& SetPartitionSizeMb(uint64_t sizeMb);
    TPartitioningSettingsBuilder& SetMinPartitionsCount(uint64_t count);
    TPartitioningSettingsBuilder& SetMaxPartitionsCount(uint64_t count);

    TPartitioningSettings Build() const;

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

class TColumnFamilyBuilder {
public:
    explicit TColumnFamilyBuilder(const std::string& name);
    ~TColumnFamilyBuilder();

    TColumnFamilyBuilder& SetData(const std::string& media);
    TColumnFamilyBuilder& SetCompression(EColumnFamilyCompression compression);
    TColumnFamilyBuilder& SetKeepInMemory(bool enabled);

    TColumnFamilyDescription Build() const;

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

class TTableStorageSettingsBuilder {
public:
    explicit TTableStorageSettingsBuilder(TTableBuilder& parent)
        : Parent_(parent)
    { }

    TTableStorageSettingsBuilder& SetTabletCommitLog0(const std::string& media) {
        Builder_.SetTabletCommitLog0(media);
        return *this;
    }

    TTableStorageSettingsBuilder& SetTabletCommitLog1(const std::string& media) {
        Builder_.SetTabletCommitLog1(media);
        return *this;
    }

    TTableStorageSettingsBuilder& SetExternal(const std::string& media) {
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
    TTableColumnFamilyBuilder(TTableBuilder& parent, const std::string& name)
        : Parent_(parent)
        , Builder_(name)
    { }

    TTableColumnFamilyBuilder& SetData(const std::string& media) {
        Builder_.SetData(media);
        return *this;
    }

    TTableColumnFamilyBuilder& SetCompression(EColumnFamilyCompression compression) {
        Builder_.SetCompression(compression);
        return *this;
    }

    TTableColumnFamilyBuilder& SetKeepInMemory(bool enabled) {
        Builder_.SetKeepInMemory(enabled);
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

    TTablePartitioningSettingsBuilder& SetPartitionSizeMb(uint64_t sizeMb) {
        Builder_.SetPartitionSizeMb(sizeMb);
        return *this;
    }

    TTablePartitioningSettingsBuilder& SetMinPartitionsCount(uint64_t count) {
        Builder_.SetMinPartitionsCount(count);
        return *this;
    }

    TTablePartitioningSettingsBuilder& SetMaxPartitionsCount(uint64_t count) {
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

    TTableBuilder& AddNullableColumn(const std::string& name, const EPrimitiveType& type, const std::string& family = std::string());
    TTableBuilder& AddNullableColumn(const std::string& name, const TDecimalType& type, const std::string& family = std::string());
    TTableBuilder& AddNullableColumn(const std::string& name, const TPgType& type, const std::string& family = std::string());
    TTableBuilder& AddNonNullableColumn(const std::string& name, const EPrimitiveType& type, const std::string& family = std::string());
    TTableBuilder& AddNonNullableColumn(const std::string& name, const TDecimalType& type, const std::string& family = std::string());
    TTableBuilder& AddNonNullableColumn(const std::string& name, const TPgType& type, const std::string& family = std::string());
    TTableBuilder& SetPrimaryKeyColumns(const std::vector<std::string>& primaryKeyColumns);
    TTableBuilder& SetPrimaryKeyColumn(const std::string& primaryKeyColumn);
    TTableBuilder& AddSerialColumn(const std::string& name, const EPrimitiveType& type, TSequenceDescription sequenceDescription, const std::string& family = std::string());

    // common
    TTableBuilder& AddSecondaryIndex(const TIndexDescription& indexDescription);
    TTableBuilder& AddSecondaryIndex(const std::string& indexName, EIndexType type, const std::vector<std::string>& indexColumns, const std::vector<std::string>& dataColumns);
    TTableBuilder& AddSecondaryIndex(const std::string& indexName, EIndexType type, const std::vector<std::string>& indexColumns);
    TTableBuilder& AddSecondaryIndex(const std::string& indexName, EIndexType type, const std::string& indexColumn);

    // sync
    TTableBuilder& AddSyncSecondaryIndex(const std::string& indexName, const std::vector<std::string>& indexColumns, const std::vector<std::string>& dataColumns);
    TTableBuilder& AddSyncSecondaryIndex(const std::string& indexName, const std::vector<std::string>& indexColumns);
    TTableBuilder& AddSyncSecondaryIndex(const std::string& indexName, const std::string& indexColumn);

    // async
    TTableBuilder& AddAsyncSecondaryIndex(const std::string& indexName, const std::vector<std::string>& indexColumns, const std::vector<std::string>& dataColumns);
    TTableBuilder& AddAsyncSecondaryIndex(const std::string& indexName, const std::vector<std::string>& indexColumns);
    TTableBuilder& AddAsyncSecondaryIndex(const std::string& indexName, const std::string& indexColumn);

    // unique
    TTableBuilder& AddUniqueSecondaryIndex(const std::string& indexName, const std::vector<std::string>& indexColumns);
    TTableBuilder& AddUniqueSecondaryIndex(const std::string& indexName, const std::vector<std::string>& indexColumns, const std::vector<std::string>& dataColumns);

    // vector KMeansTree
    TTableBuilder& AddVectorKMeansTreeIndex(const std::string& indexName, const std::vector<std::string>& indexColumns, const TKMeansTreeSettings& indexSettings);
    TTableBuilder& AddVectorKMeansTreeIndex(const std::string& indexName, const std::vector<std::string>& indexColumns, const std::vector<std::string>& dataColumns, const TKMeansTreeSettings& indexSettings);

    // default
    TTableBuilder& AddSecondaryIndex(const std::string& indexName, const std::vector<std::string>& indexColumns, const std::vector<std::string>& dataColumns);
    TTableBuilder& AddSecondaryIndex(const std::string& indexName, const std::vector<std::string>& indexColumns);
    TTableBuilder& AddSecondaryIndex(const std::string& indexName, const std::string& indexColumn);

    TTableBuilder& SetTtlSettings(TTtlSettings&& settings);
    TTableBuilder& SetTtlSettings(const TTtlSettings& settings);
    TTableBuilder& SetTtlSettings(const std::string& columnName, const TDuration& expireAfter = TDuration::Zero());
    TTableBuilder& SetTtlSettings(const std::string& columnName, EUnit columnUnit, const TDuration& expireAfter = TDuration::Zero());

    TTableBuilder& SetStorageSettings(const TStorageSettings& settings);

    TTableBuilder& AddColumnFamily(const TColumnFamilyDescription& desc);

    TTableBuilder& AddAttribute(const std::string& key, const std::string& value);
    TTableBuilder& SetAttributes(const std::unordered_map<std::string, std::string>& attrs);
    TTableBuilder& SetAttributes(std::unordered_map<std::string, std::string>&& attrs);

    TTableBuilder& SetCompactionPolicy(const std::string& name);

    // UniformPartitions and PartitionAtKeys are mutually exclusive
    TTableBuilder& SetUniformPartitions(uint64_t partitionsCount);
    TTableBuilder& SetPartitionAtKeys(const TExplicitPartitions& keys);

    TTableBuilder& SetPartitioningSettings(const TPartitioningSettings& settings);

    TTableBuilder& SetKeyBloomFilter(bool enabled);

    TTableBuilder& SetReadReplicasSettings(TReadReplicasSettings::EMode mode, uint64_t readReplicasCount);

    TTableStorageSettingsBuilder BeginStorageSettings() {
        return TTableStorageSettingsBuilder(*this);
    }

    TTableColumnFamilyBuilder BeginColumnFamily(const std::string& name) {
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
    TCopyItem(const std::string& source, const std::string& destination);

    const std::string& SourcePath() const;
    const std::string& DestinationPath() const;

    TCopyItem& SetOmitIndexes();
    bool OmitIndexes() const;

    void Out(IOutputStream& out) const;

private:
    std::string Source_;
    std::string Destination_;
    bool OmitIndexes_;
};

////////////////////////////////////////////////////////////////////////////////

class TRenameItem {
public:
    TRenameItem(const std::string& source, const std::string& destination);

    const std::string& SourcePath() const;
    const std::string& DestinationPath() const;

    TRenameItem& SetReplaceDestination();
    bool ReplaceDestination() const;
private:
    std::string Source_;
    std::string Destination_;
    bool ReplaceDestination_;
};

////////////////////////////////////////////////////////////////////////////////

class TDescribeExternalDataSourceResult;
class TDescribeExternalTableResult;

using TAsyncCreateSessionResult = NThreading::TFuture<TCreateSessionResult>;
using TAsyncDataQueryResult = NThreading::TFuture<TDataQueryResult>;
using TAsyncPrepareQueryResult = NThreading::TFuture<TPrepareQueryResult>;
using TAsyncExplainDataQueryResult = NThreading::TFuture<TExplainQueryResult>;
using TAsyncDescribeTableResult = NThreading::TFuture<TDescribeTableResult>;
using TAsyncDescribeExternalDataSourceResult = NThreading::TFuture<TDescribeExternalDataSourceResult>;
using TAsyncDescribeExternalTableResult = NThreading::TFuture<TDescribeExternalTableResult>;
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
    FLUENT_SETTING_DEFAULT(uint32_t, MaxActiveSessions, 50);

    // Max number of attempt to create session inside session pool
    // to handle OVERLOADED error
    FLUENT_SETTING_DEFAULT(uint32_t, RetryLimit, 5);

    // Max time session to be in idle state in session pool before
    // keep alive start to touch it
    FLUENT_SETTING_DEFAULT(TDuration, KeepAliveIdleThreshold, TDuration::Minutes(5));

    // Max time session to be in idle state before closing
    FLUENT_SETTING_DEFAULT(TDuration, CloseIdleThreshold, TDuration::Minutes(1));

    // Min number of session in session pool.
    // Sessions will not be closed by CloseIdleThreshold if the number of sessions less then this limit.
    FLUENT_SETTING_DEFAULT(uint32_t, MinPoolSize, 10);
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
    FLUENT_SETTING_DEFAULT(uint32_t, QueryCacheSize, 1000);
    FLUENT_SETTING_DEFAULT(bool, KeepDataQueryText, true);

    // Min allowed session variation coefficient (%) to start session balancing.
    // Variation coefficient is a ratio of the standard deviation sigma to the mean
    // Example:
    //   - 3 hosts with [90, 100, 110] sessions per host. Cv will be 10%
    //   - add new host ([90, 100, 110, 0] sessions per host). Cv will be 77%
    // Balancing is will be performed if calculated cv greater than MinSessionCV
    // Zero - disable this feature
    FLUENT_SETTING_DEFAULT(uint32_t, MinSessionCV, 20);

    // Allow migrate requests between session during session balancing
    FLUENT_SETTING_DEFAULT(bool, AllowRequestMigration, true);

    // Settings of session pool
    FLUENT_SETTING(TSessionPoolSettings, SessionPoolSettings);
};

struct TBulkUpsertSettings : public TOperationRequestSettings<TBulkUpsertSettings> {
    // Format setting proto serialized into string. If not set format defaults are used.
    // I.e. it's Ydb.Table.CsvSettings for CSV.
    FLUENT_SETTING_DEFAULT(std::string, FormatSettings, "");
};

struct TReadRowsSettings : public TOperationRequestSettings<TReadRowsSettings> {
};

struct TStreamExecScanQuerySettings : public TRequestSettings<TStreamExecScanQuerySettings> {
    // Return query plan without actual query execution
    FLUENT_SETTING_DEFAULT(bool, Explain, false);

    // Collect runtime statistics with a given detalization mode
    FLUENT_SETTING_DEFAULT(ECollectQueryStatsMode, CollectQueryStats, ECollectQueryStatsMode::None);

    // Deprecated. Use CollectQueryStats >= ECollectQueryStatsMode::Full to get QueryMeta in QueryStats
    // Collect full query compilation diagnostics
    FLUENT_SETTING_DEFAULT(bool, CollectFullDiagnostics, false);
};

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
    int64_t GetActiveSessionCount() const;

    //! Returns the maximum number of sessions in session pool
    int64_t GetActiveSessionsLimit() const;

    //! Returns the size of session pool
    int64_t GetCurrentPoolSize() const;

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
    TAsyncBulkUpsertResult BulkUpsert(const std::string& table, TValue&& rows,
        const TBulkUpsertSettings& settings = TBulkUpsertSettings());
    TAsyncBulkUpsertResult BulkUpsert(const std::string& table, EDataFormat format,
        const std::string& data, const std::string& schema = {}, const TBulkUpsertSettings& settings = TBulkUpsertSettings());

    TAsyncReadRowsResult ReadRows(const std::string& table, TValue&& keys, const std::vector<std::string>& columns = {},
        const TReadRowsSettings& settings = TReadRowsSettings());

    TAsyncScanQueryPartIterator StreamExecuteScanQuery(const std::string& query,
        const TStreamExecScanQuerySettings& settings = TStreamExecScanQuerySettings());

    TAsyncScanQueryPartIterator StreamExecuteScanQuery(const std::string& query, const TParams& params,
        const TStreamExecScanQuerySettings& settings = TStreamExecScanQuerySettings());

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

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

    static TTxSettings SnapshotRW() {
        return TTxSettings(TS_SNAPSHOT_RW);
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
        case TS_SNAPSHOT_RW:
            out << "SnapshotRW";
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
        TS_SNAPSHOT_RO,
        TS_SNAPSHOT_RW,
    };

    FLUENT_SETTING(TTxOnlineSettings, OnlineSettings);

private:
    TTxSettings(ETransactionMode mode)
        : Mode_(mode) {}

    ETransactionMode Mode_;
};

enum class EAutoPartitioningPolicy {
    Disabled = 1,
    AutoSplit = 2,
    AutoSplitMerge = 3
};

////////////////////////////////////////////////////////////////////////////////

struct TColumnFamilyPolicy {
    using TSelf = TColumnFamilyPolicy;

    FLUENT_SETTING_OPTIONAL(std::string, Name);

    FLUENT_SETTING_OPTIONAL(std::string, Data);

    FLUENT_SETTING_OPTIONAL(std::string, External);

    FLUENT_SETTING_OPTIONAL(bool, KeepInMemory);

    FLUENT_SETTING_OPTIONAL(bool, Compressed);
};

struct TStoragePolicy {
    using TSelf = TStoragePolicy;

    FLUENT_SETTING_OPTIONAL(std::string, PresetName);

    FLUENT_SETTING_OPTIONAL(std::string, SysLog);

    FLUENT_SETTING_OPTIONAL(std::string, Log);

    FLUENT_SETTING_OPTIONAL(std::string, Data);

    FLUENT_SETTING_OPTIONAL(std::string, External);

    FLUENT_SETTING_VECTOR(TColumnFamilyPolicy, ColumnFamilies);
};

struct TPartitioningPolicy {
    using TSelf = TPartitioningPolicy;

    FLUENT_SETTING_OPTIONAL(std::string, PresetName);

    FLUENT_SETTING_OPTIONAL(EAutoPartitioningPolicy, AutoPartitioning);

    FLUENT_SETTING_OPTIONAL(uint64_t, UniformPartitions);

    FLUENT_SETTING_OPTIONAL(TExplicitPartitions, ExplicitPartitions);
};

struct TReplicationPolicy {
    using TSelf = TReplicationPolicy;

    FLUENT_SETTING_OPTIONAL(std::string, PresetName);

    FLUENT_SETTING_OPTIONAL(uint32_t, ReplicasCount);

    FLUENT_SETTING_OPTIONAL(bool, CreatePerAvailabilityZone);

    FLUENT_SETTING_OPTIONAL(bool, AllowPromotion);
};

////////////////////////////////////////////////////////////////////////////////

struct TCreateTableSettings : public TOperationRequestSettings<TCreateTableSettings> {
    using TSelf = TCreateTableSettings;

    FLUENT_SETTING_OPTIONAL(std::string, PresetName);

    FLUENT_SETTING_OPTIONAL(std::string, ExecutionPolicy);

    FLUENT_SETTING_OPTIONAL(std::string, CompactionPolicy);

    FLUENT_SETTING_OPTIONAL(TPartitioningPolicy, PartitioningPolicy);

    FLUENT_SETTING_OPTIONAL(TStoragePolicy, StoragePolicy);

    FLUENT_SETTING_OPTIONAL(TReplicationPolicy, ReplicationPolicy);
};

////////////////////////////////////////////////////////////////////////////////

struct TDropTableSettings : public TOperationRequestSettings<TDropTableSettings> {
    using TOperationRequestSettings<TDropTableSettings>::TOperationRequestSettings;
};

////////////////////////////////////////////////////////////////////////////////

class TAlterStorageSettingsBuilder {
public:
    explicit TAlterStorageSettingsBuilder(TAlterTableSettings& parent)
        : Parent_(parent)
    { }

    TAlterStorageSettingsBuilder& SetTabletCommitLog0(const std::string& media) {
        Builder_.SetTabletCommitLog0(media);
        return *this;
    }

    TAlterStorageSettingsBuilder& SetTabletCommitLog1(const std::string& media) {
        Builder_.SetTabletCommitLog1(media);
        return *this;
    }

    TAlterStorageSettingsBuilder& SetExternal(const std::string& media) {
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
    TAlterColumnFamilyBuilder(TAlterTableSettings& parent, const std::string& name)
        : Parent_(parent)
        , Builder_(name)
    { }

    TAlterColumnFamilyBuilder& SetData(const std::string& media) {
        Builder_.SetData(media);
        return *this;
    }

    TAlterColumnFamilyBuilder& SetCompression(EColumnFamilyCompression compression) {
        Builder_.SetCompression(compression);
        return *this;
    }

    TAlterColumnFamilyBuilder& SetKeepInMemory(bool enabled) {
        Builder_.SetKeepInMemory(enabled);
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
    TAlterTtlSettingsBuilder& Set(const std::string& columnName, const TDuration& expireAfter = TDuration::Zero());
    TAlterTtlSettingsBuilder& Set(const std::string& columnName, EUnit columnUnit, const TDuration& expireAfter = TDuration::Zero());

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

    TAlterAttributesBuilder& Alter(const std::string& key, const std::string& value) {
        AlterAttributes_[key] = value;
        return *this;
    }

    TAlterAttributesBuilder& Add(const std::string& key, const std::string& value) {
        return Alter(key, value);
    }

    TAlterAttributesBuilder& Drop(const std::string& key) {
        return Alter(key, "");
    }

    TAlterTableSettings& EndAlterAttributes();

private:
    TAlterTableSettings& Parent_;
    std::unordered_map<std::string, std::string> AlterAttributes_;
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

    TAlterPartitioningSettingsBuilder& SetPartitionSizeMb(uint64_t sizeMb) {
        Builder_.SetPartitionSizeMb(sizeMb);
        return *this;
    }

    TAlterPartitioningSettingsBuilder& SetMinPartitionsCount(uint64_t count) {
        Builder_.SetMinPartitionsCount(count);
        return *this;
    }

    TAlterPartitioningSettingsBuilder& SetMaxPartitionsCount(uint64_t count) {
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
    using TAlterAttributes = std::unordered_map<std::string, std::string>;

    TAlterTableSettings();

    FLUENT_SETTING_VECTOR(TTableColumn, AddColumns);

    FLUENT_SETTING_VECTOR(std::string, DropColumns);

    FLUENT_SETTING_VECTOR(TAlterTableColumn, AlterColumns);

    FLUENT_SETTING_VECTOR(TIndexDescription, AddIndexes);
    FLUENT_SETTING_VECTOR(std::string, DropIndexes);
    FLUENT_SETTING_VECTOR(TRenameIndex, RenameIndexes);

    FLUENT_SETTING_VECTOR(TChangefeedDescription, AddChangefeeds);
    FLUENT_SETTING_VECTOR(std::string, DropChangefeeds);

    TSelf& AlterColumnFamily(std::string name, std::string family) {
        AlterColumns_.emplace_back(std::move(name), std::move(family));
        return *this;
    }

    FLUENT_SETTING_OPTIONAL(TStorageSettings, AlterStorageSettings);

    FLUENT_SETTING_VECTOR(TColumnFamilyDescription, AddColumnFamilies);
    FLUENT_SETTING_VECTOR(TColumnFamilyDescription, AlterColumnFamilies);

    // workaround for MSVC
    TSelf& AlterTtlSettings(const std::optional<TAlterTtlSettings>& value);
    const std::optional<TAlterTtlSettings>& GetAlterTtlSettings() const;

    FLUENT_SETTING(TAlterAttributes, AlterAttributes);

    FLUENT_SETTING(std::string, SetCompactionPolicy);

    FLUENT_SETTING_OPTIONAL(TPartitioningSettings, AlterPartitioningSettings);

    FLUENT_SETTING_OPTIONAL(bool, SetKeyBloomFilter);

    FLUENT_SETTING_OPTIONAL(TReadReplicasSettings, SetReadReplicasSettings);
    TSelf& SetReadReplicasSettings(TReadReplicasSettings::EMode mode, uint64_t readReplicasCount) {
        SetReadReplicasSettings_ = TReadReplicasSettings(mode, readReplicasCount);
        return *this;
    }

    TAlterStorageSettingsBuilder BeginAlterStorageSettings() {
        return TAlterStorageSettingsBuilder(*this);
    }

    TAlterColumnFamilyBuilder BeginAddColumnFamily(const std::string& name) {
        return TAlterColumnFamilyBuilder(*this, name);
    }

    TAlterColumnFamilyBuilder BeginAlterColumnFamily(const std::string& name) {
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
    FLUENT_SETTING_DEFAULT(bool, WithSetVal, false);
    FLUENT_SETTING_DEFAULT(bool, WithShardNodesInfo, false);
};

struct TDescribeExternalDataSourceSettings : public TOperationRequestSettings<TDescribeExternalDataSourceSettings> {};

struct TDescribeExternalTableSettings : public TOperationRequestSettings<TDescribeExternalTableSettings> {};

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

    FLUENT_SETTING_VECTOR(std::string, Columns);

    FLUENT_SETTING_FLAG(Ordered);

    FLUENT_SETTING_OPTIONAL(uint64_t, RowLimit);

    FLUENT_SETTING_OPTIONAL(bool, UseSnapshot);

    FLUENT_SETTING_OPTIONAL(uint64_t, BatchLimitBytes);

    FLUENT_SETTING_OPTIONAL(uint64_t, BatchLimitRows);

    FLUENT_SETTING_OPTIONAL(bool, ReturnNotNullAsOptional);
};

using TPrecommitTransactionCallback = std::function<TAsyncStatus ()>;

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
    TAsyncStatus CreateTable(const std::string& path, TTableDescription&& tableDesc,
        const TCreateTableSettings& settings = TCreateTableSettings());

    TAsyncStatus DropTable(const std::string& path, const TDropTableSettings& settings = TDropTableSettings());

    TAsyncStatus AlterTable(const std::string& path, const TAlterTableSettings& settings = TAlterTableSettings());

    // Same as AlterTable but may return operation in case of long running
    TAsyncOperation AlterTableLong(const std::string& path, const TAlterTableSettings& settings = TAlterTableSettings());

    TAsyncStatus CopyTable(const std::string& src, const std::string& dst,
        const TCopyTableSettings& settings = TCopyTableSettings());

    TAsyncStatus CopyTables(const std::vector<TCopyItem>& copyItems,
        const TCopyTablesSettings& settings = TCopyTablesSettings());

    TAsyncStatus RenameTables(const std::vector<TRenameItem>& renameItems,
        const TRenameTablesSettings& settings = TRenameTablesSettings());

    TAsyncDescribeTableResult DescribeTable(const std::string& path,
        const TDescribeTableSettings& settings = TDescribeTableSettings());

    TAsyncDescribeExternalDataSourceResult DescribeExternalDataSource(const std::string& path,
        const TDescribeExternalDataSourceSettings& settings = {});

    TAsyncDescribeExternalTableResult DescribeExternalTable(const std::string& path,
        const TDescribeExternalTableSettings& settings = {});

    TAsyncBeginTransactionResult BeginTransaction(const TTxSettings& txSettings = TTxSettings(),
        const TBeginTxSettings& settings = TBeginTxSettings());

    TAsyncExplainDataQueryResult ExplainDataQuery(const std::string& query,
        const TExplainDataQuerySettings& settings = TExplainDataQuerySettings());

    TAsyncPrepareQueryResult PrepareDataQuery(const std::string& query,
        const TPrepareDataQuerySettings& settings = TPrepareDataQuerySettings());

    TAsyncDataQueryResult ExecuteDataQuery(const std::string& query, const TTxControl& txControl,
        const TExecDataQuerySettings& settings = TExecDataQuerySettings());

    TAsyncDataQueryResult ExecuteDataQuery(const std::string& query, const TTxControl& txControl,
        const TParams& params, const TExecDataQuerySettings& settings = TExecDataQuerySettings());

    TAsyncDataQueryResult ExecuteDataQuery(const std::string& query, const TTxControl& txControl,
        TParams&& params, const TExecDataQuerySettings& settings = TExecDataQuerySettings());

    TAsyncStatus ExecuteSchemeQuery(const std::string& query,
        const TExecSchemeQuerySettings& settings = TExecSchemeQuerySettings());

    TAsyncTablePartIterator ReadTable(const std::string& path,
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
    const std::string& GetId() const;

    class TImpl;
private:
    TSession(std::shared_ptr<TTableClient::TImpl> client, const std::string& sessionId, const std::string& endpointId, bool isOwnedBySessionPool);
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
    const std::string& GetId() const;
    bool IsActive() const;

    TAsyncCommitTransactionResult Commit(const TCommitTxSettings& settings = TCommitTxSettings());
    TAsyncStatus Rollback(const TRollbackTxSettings& settings = TRollbackTxSettings());

    TSession GetSession() const;
    void AddPrecommitCallback(TPrecommitTransactionCallback cb);

private:
    TTransaction(const TSession& session, const std::string& txId);

    TAsyncStatus Precommit() const;

    class TImpl;

    std::shared_ptr<TImpl> TransactionImpl_;
};

////////////////////////////////////////////////////////////////////////////////

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
    std::optional<TTransaction> Tx_;
    TTxSettings BeginTx_;
};

//! Represents query identificator (e.g. used for prepared query)
class TDataQuery {
    friend class TTableClient;
    friend class TSession;

public:
    const std::string& GetId() const;
    const std::optional<std::string>& GetText() const;
    TParamsBuilder GetParamsBuilder() const;

    TAsyncDataQueryResult Execute(const TTxControl& txControl,
        const TExecDataQuerySettings& settings = TExecDataQuerySettings());

    TAsyncDataQueryResult Execute(const TTxControl& txControl, const TParams& params,
        const TExecDataQuerySettings& settings = TExecDataQuerySettings());

    TAsyncDataQueryResult Execute(const TTxControl& txControl, TParams&& params,
        const TExecDataQuerySettings& settings = TExecDataQuerySettings());

private:
    TDataQuery(const TSession& session, const std::string& text, const std::string& id);
    TDataQuery(const TSession& session, const std::string& text, const std::string& id,
        const ::google::protobuf::Map<TStringType, Ydb::Type>& types);

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
    TExplainQueryResult(TStatus&& status, std::string&& plan, std::string&& ast, std::string&& diagnostics);

    const std::string& GetPlan() const;
    const std::string& GetAst() const;
    const std::string& GetDiagnostics() const;

private:
    std::string Plan_;
    std::string Ast_;
    std::string Diagnostics_;
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
    TDataQueryResult(TStatus&& status, std::vector<TResultSet>&& resultSets, const std::optional<TTransaction>& transaction,
        const std::optional<TDataQuery>& dataQuery, bool fromCache, const std::optional<TQueryStats>& queryStats);

    const std::vector<TResultSet>& GetResultSets() const;
    std::vector<TResultSet> ExtractResultSets() &&;
    TResultSet GetResultSet(size_t resultIndex) const;

    TResultSetParser GetResultSetParser(size_t resultIndex) const;

    std::optional<TTransaction> GetTransaction() const;

    std::optional<TDataQuery> GetQuery() const;
    bool IsQueryFromCache() const;

    const std::optional<TQueryStats>& GetStats() const;

    const std::string GetQueryPlan() const;

private:
    std::optional<TTransaction> Transaction_;
    std::vector<TResultSet> ResultSets_;
    std::optional<TDataQuery> DataQuery_;
    bool FromCache_;
    std::optional<TQueryStats> QueryStats_;
};

class TReadTableSnapshot {
public:
    TReadTableSnapshot(uint64_t step, uint64_t txId)
        : Step_(step)
        , TxId_(txId)
    {}

    uint64_t GetStep() const { return Step_; }
    uint64_t GetTxId() const { return TxId_; }

private:
    uint64_t Step_;
    uint64_t TxId_;
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
    bool HasResultSet() const { return ResultSet_.has_value(); }
    const TResultSet& GetResultSet() const { return *ResultSet_; }
    TResultSet ExtractResultSet() { return std::move(*ResultSet_); }

    bool HasQueryStats() const { return QueryStats_.has_value(); }
    const TQueryStats& GetQueryStats() const { return *QueryStats_; }
    TQueryStats ExtractQueryStats() { return std::move(*QueryStats_); }

    // Deprecated. Use GetMeta() of TQueryStats
    bool HasDiagnostics() const { return Diagnostics_.has_value(); }
    const std::string& GetDiagnostics() const { return *Diagnostics_; }
    std::string&& ExtractDiagnostics() { return std::move(*Diagnostics_); }

    TScanQueryPart(TStatus&& status)
        : TStreamPartStatus(std::move(status))
    {}

    TScanQueryPart(TStatus&& status, const std::optional<TQueryStats>& queryStats, const std::optional<std::string>& diagnostics)
        : TStreamPartStatus(std::move(status))
        , QueryStats_(queryStats)
        , Diagnostics_(diagnostics)
    {}

    TScanQueryPart(TStatus&& status, TResultSet&& resultSet, const std::optional<TQueryStats>& queryStats, const std::optional<std::string>& diagnostics)
        : TStreamPartStatus(std::move(status))
        , ResultSet_(std::move(resultSet))
        , QueryStats_(queryStats)
        , Diagnostics_(diagnostics)
    {}

private:
    std::optional<TResultSet> ResultSet_;
    std::optional<TQueryStats> QueryStats_;
    std::optional<std::string> Diagnostics_;
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
    TCommitTransactionResult(TStatus&& status, const std::optional<TQueryStats>& queryStats);

    const std::optional<TQueryStats>& GetStats() const;

private:
    std::optional<TQueryStats> QueryStats_;
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

class TExternalDataSourceDescription {
public:
    TExternalDataSourceDescription(Ydb::Table::DescribeExternalDataSourceResult&& description);

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;

    friend class NYdb::TProtoAccessor;
    const Ydb::Table::DescribeExternalDataSourceResult& GetProto() const;
};

//! Represents the result of a DescribeExternalDataSource call.
class TDescribeExternalDataSourceResult : public NScheme::TDescribePathResult {
public:
    TDescribeExternalDataSourceResult(
        TStatus&& status,
        Ydb::Table::DescribeExternalDataSourceResult&& description
    );

    TExternalDataSourceDescription GetExternalDataSourceDescription() const;

private:
    TExternalDataSourceDescription ExternalDataSourceDescription_;
};

class TExternalTableDescription {
public:
    TExternalTableDescription(Ydb::Table::DescribeExternalTableResult&& description);

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;

    friend class NYdb::TProtoAccessor;
    const Ydb::Table::DescribeExternalTableResult& GetProto() const;
};

//! Represents the result of a DescribeExternalTable call.
class TDescribeExternalTableResult : public NScheme::TDescribePathResult {
public:
    TDescribeExternalTableResult(
        TStatus&& status,
        Ydb::Table::DescribeExternalTableResult&& description
    );

    TExternalTableDescription GetExternalTableDescription() const;

private:
    TExternalTableDescription ExternalTableDescription_;
};

} // namespace NTable
} // namespace NYdb
