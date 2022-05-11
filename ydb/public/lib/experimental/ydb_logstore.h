#pragma once

#include <ydb/public/sdk/cpp/client/ydb_result/result.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

namespace Ydb::LogStore {

class Schema;
class Compression;
class CreateLogStoreRequest;
class CreateLogTableRequest;
class DescribeLogStoreResult;
class DescribeLogTableResult;

}

namespace NYdb::NLogStore {

using NTable::TTtlSettings;
using NTable::TAlterTtlSettings;

enum class EColumnCompression {
    None,
    LZ4,
    ZSTD
};

struct TCompression {
    EColumnCompression Codec = EColumnCompression::LZ4;
    TMaybe<int> Level;

    void SerializeTo(Ydb::LogStore::Compression& compression) const;
};

struct TTierConfig {
    TCompression Compression;
};

struct TTier {
    TTtlSettings Eviction;
};

struct TCreateLogStoreSettings : public TOperationRequestSettings<TCreateLogStoreSettings> {
    using TSelf = TCreateLogStoreSettings;
};

struct TDropLogStoreSettings : public TOperationRequestSettings<TDropLogStoreSettings> {
    using TSelf = TDropLogStoreSettings;
};

struct TDescribeLogStoreSettings : public TOperationRequestSettings<TDescribeLogStoreSettings> {
    using TSelf = TDescribeLogStoreSettings;
};

struct TAlterLogStoreSettings : public TOperationRequestSettings<TAlterLogStoreSettings> {
    using TSelf = TAlterLogStoreSettings;
};

struct TCreateLogTableSettings : public TOperationRequestSettings<TCreateLogTableSettings> {
    using TSelf = TCreateLogTableSettings;
};

struct TDropLogTableSettings : public TOperationRequestSettings<TDropLogTableSettings> {
    using TSelf = TDropLogTableSettings;
};

struct TDescribeLogTableSettings : public TOperationRequestSettings<TDescribeLogTableSettings> {
    using TSelf = TDescribeLogTableSettings;
};

struct TAlterLogTableSettings : public TOperationRequestSettings<TAlterLogTableSettings> {
    using TSelf = TAlterLogTableSettings;

    TSelf& AlterTtlSettings(const TMaybe<TAlterTtlSettings>& value);
    const TMaybe<TAlterTtlSettings>& GetAlterTtlSettings() const;
private:
    TMaybe<TAlterTtlSettings> AlterTtlSettings_;
};

TType MakeColumnType(EPrimitiveType primitiveType);

class TSchema {
public:
    TSchema(const TVector<TColumn>& columns = {}, const TVector<TString> primaryKeyColumns = {},
            const TCompression& defaultCompression = {})
        : Columns(columns)
        , PrimaryKeyColumns(primaryKeyColumns)
        , DefaultCompression(defaultCompression)
    {}

    explicit TSchema(const Ydb::LogStore::Schema& schema);

    void SerializeTo(Ydb::LogStore::Schema& schema) const;

    TVector<TColumn> GetColumns() const {
        return Columns;
    }
    const TVector<TString>& GetPrimaryKeyColumns() const {
        return PrimaryKeyColumns;
    }
    const TCompression GetDefaultCompression() const {
        return DefaultCompression;
    }

private:
    TVector<TColumn> Columns;
    TVector<TString> PrimaryKeyColumns;
    TCompression DefaultCompression;
};

class TLogStoreDescription {
public:
    TLogStoreDescription(ui32 columnShardCount, const THashMap<TString, TSchema>& schemaPresets,
                         const THashMap<TString, TTierConfig>& tierConfigs = {});
    TLogStoreDescription(Ydb::LogStore::DescribeLogStoreResult&& desc, const TDescribeLogStoreSettings& describeSettings);
    void SerializeTo(Ydb::LogStore::CreateLogStoreRequest& request) const;
    const THashMap<TString, TSchema>& GetSchemaPresets() const {
        return SchemaPresets;
    }
    ui32 GetColumnShardCount() const {
        return ColumnShardCount;
    }
    const TString& GetOwner() const {
        return Owner;
    }
    const TVector<NScheme::TPermissions>& GetPermissions() const {
        return Permissions;
    }
    const TVector<NScheme::TPermissions>& GetEffectivePermissions() const {
        return EffectivePermissions;
    }
    const THashMap<TString, TTierConfig>& GetTierConfigs() const {
        return TierConfigs;
    }

private:
    ui32 ColumnShardCount;
    THashMap<TString, TSchema> SchemaPresets;
    TString Owner;
    TVector<NScheme::TPermissions> Permissions;
    TVector<NScheme::TPermissions> EffectivePermissions;
    THashMap<TString, TTierConfig> TierConfigs;
};

class TLogTableDescription {
public:
    TLogTableDescription(const TString& schemaPresetName, const TVector<TString>& shardingColumns,
        ui32 columnShardCount, const TMaybe<TTtlSettings>& ttlSettings = {});
    TLogTableDescription(const TSchema& schema, const TVector<TString>& shardingColumns,
        ui32 columnShardCount, const TMaybe<TTtlSettings>& ttlSettings = {});
    TLogTableDescription(const TString& schemaPresetName, const TVector<TString>& shardingColumns,
        ui32 columnShardCount, const THashMap<TString, TTier>& tiers);
    TLogTableDescription(Ydb::LogStore::DescribeLogTableResult&& desc, const TDescribeLogTableSettings& describeSettings);
    void SerializeTo(Ydb::LogStore::CreateLogTableRequest& request) const;
    const TSchema& GetSchema() const {
        return Schema;
    }
    const TVector<TString>& GetShardingColumns() const {
        return ShardingColumns;
    }
    ui32 GetColumnShardCount() const {
        return ColumnShardCount;
    }
    const TMaybe<TTtlSettings>& GetTtlSettings() const {
        return TtlSettings;
    }

    const TString& GetOwner() const {
        return Owner;
    }
    const TVector<NScheme::TPermissions>& GetPermissions() const {
        return Permissions;
    }
    const TVector<NScheme::TPermissions>& GetEffectivePermissions() const {
        return EffectivePermissions;
    }

private:
    const TString SchemaPresetName;
    const TSchema Schema;
    const TVector<TString> ShardingColumns;
    const ui32 ColumnShardCount;
    const TMaybe<TTtlSettings> TtlSettings;
    THashMap<TString, TTier> Tiers;
    TString Owner;
    TVector<NScheme::TPermissions> Permissions;
    TVector<NScheme::TPermissions> EffectivePermissions;
};

//! Represents result of DescribeLogStore call
class TDescribeLogStoreResult : public TStatus {
public:
    TDescribeLogStoreResult(TStatus&& status, Ydb::LogStore::DescribeLogStoreResult&& desc,
        const TDescribeLogStoreSettings& describeSettings);

    const TLogStoreDescription& GetDescription() const {
        return LogStoreDescription_;
    }

private:
    TLogStoreDescription LogStoreDescription_;
};

//! Represents result of DescribeLogTable call
class TDescribeLogTableResult : public TStatus {
public:
    TDescribeLogTableResult(TStatus&& status, Ydb::LogStore::DescribeLogTableResult&& desc,
        const TDescribeLogTableSettings& describeSettings);

    TLogTableDescription GetDescription() const {
        return LogTableDescription_;
    }

private:
    TLogTableDescription LogTableDescription_;
};

using TAsyncDescribeLogStoreResult = NThreading::TFuture<TDescribeLogStoreResult>;
using TAsyncDescribeLogTableResult = NThreading::TFuture<TDescribeLogTableResult>;

class TLogStoreClient {
    class TImpl;

public:
    TLogStoreClient(const TDriver& driver, const TCommonClientSettings& settings = TCommonClientSettings());

    TAsyncStatus CreateLogStore(const TString& path, TLogStoreDescription&& tableDesc,
        const TCreateLogStoreSettings& settings = TCreateLogStoreSettings());

    TAsyncDescribeLogStoreResult DescribeLogStore(const TString& path,
        const TDescribeLogStoreSettings& settings = TDescribeLogStoreSettings());

    TAsyncStatus DropLogStore(const TString& path, const TDropLogStoreSettings& settings = TDropLogStoreSettings());

    TAsyncStatus AlterLogStore(const TString& path, const TAlterLogStoreSettings& settings = TAlterLogStoreSettings());

    TAsyncStatus CreateLogTable(const TString& path, TLogTableDescription&& tableDesc,
        const TCreateLogTableSettings& settings = TCreateLogTableSettings());

    TAsyncDescribeLogTableResult DescribeLogTable(const TString& path,
        const TDescribeLogTableSettings& settings = TDescribeLogTableSettings());

    TAsyncStatus DropLogTable(const TString& path, const TDropLogTableSettings& settings = TDropLogTableSettings());

    TAsyncStatus AlterLogTable(const TString& path, const TAlterLogTableSettings& settings = TAlterLogTableSettings());

private:
    std::shared_ptr<TImpl> Impl_;
};

}
