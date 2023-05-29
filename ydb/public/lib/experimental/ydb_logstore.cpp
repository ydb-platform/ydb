#include "ydb_logstore.h"

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/make_request/make.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/scheme_helpers/helpers.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/api/grpc/draft/ydb_logstore_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>
#include <ydb/public/sdk/cpp/client/ydb_common_client/impl/client.h>

#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

namespace NYdb {
namespace NLogStore {

TMaybe<TTtlSettings> TtlSettingsFromProto(const Ydb::Table::TtlSettings& proto) {
    switch (proto.mode_case()) {
    case Ydb::Table::TtlSettings::kDateTypeColumn:
        return TTtlSettings(
            proto.date_type_column(),
            proto.run_interval_seconds()
        );

    case Ydb::Table::TtlSettings::kValueSinceUnixEpoch:
        return TTtlSettings(
            proto.value_since_unix_epoch(),
            proto.run_interval_seconds()
        );

    default:
        break;
    }
    return {};
}

static TCompression CompressionFromProto(const Ydb::LogStore::Compression& compression) {
    TCompression out;
    switch (compression.compression_codec()) {
        case Ydb::LogStore::Compression::CODEC_PLAIN:
            out.Codec = EColumnCompression::None;
            break;
        case Ydb::LogStore::Compression::CODEC_LZ4:
            out.Codec = EColumnCompression::LZ4;
            break;
        case Ydb::LogStore::Compression::CODEC_ZSTD:
            out.Codec = EColumnCompression::ZSTD;
            break;
        default:
            break;
    }
    if (compression.compression_level()) {
        out.Level = compression.compression_level();
    }
    return out;
}

TType MakeColumnType(EPrimitiveType primitiveType, bool notNull) {
    if (notNull) {
        return TTypeBuilder().Primitive(primitiveType).Build();
    }
    return TTypeBuilder().BeginOptional().Primitive(primitiveType).EndOptional().Build();
}

void TCompression::SerializeTo(Ydb::LogStore::Compression& compression) const {
    switch (Codec) {
        case EColumnCompression::None:
            compression.set_compression_codec(Ydb::LogStore::Compression::CODEC_PLAIN);
            break;
        case EColumnCompression::LZ4:
            compression.set_compression_codec(Ydb::LogStore::Compression::CODEC_LZ4);
            break;
        case EColumnCompression::ZSTD:
            compression.set_compression_codec(Ydb::LogStore::Compression::CODEC_ZSTD);
            break;
    }
    if (Level) {
        compression.set_compression_level(*Level);
    }
}

TSchema::TSchema(const Ydb::LogStore::Schema& schema)
    : Columns()
    , PrimaryKeyColumns(schema.primary_key().begin(), schema.primary_key().end())
{
    Columns.reserve(schema.columns().size());
    for (const auto& col : schema.columns()) {
        TColumn c(col.name(), TType(col.type()));
        Columns.emplace_back(std::move(c));
    }

    if (schema.has_default_compression()) {
        DefaultCompression = CompressionFromProto(schema.default_compression());
    }
}

void TSchema::SerializeTo(Ydb::LogStore::Schema& schema) const {
    for (const auto& c : Columns) {
        auto& col = *schema.add_columns();
        col.set_name(c.Name);
        col.mutable_type()->CopyFrom(TProtoAccessor::GetProto(c.Type));
    }
    for (const auto& pkc : PrimaryKeyColumns) {
        schema.add_primary_key(pkc);
    }
    DefaultCompression.SerializeTo(*schema.mutable_default_compression());
}

TLogStoreDescription::TLogStoreDescription(ui32 shardsCount, const THashMap<TString, TSchema>& schemaPresets)
    : ShardsCount(shardsCount)
    , SchemaPresets(schemaPresets)
{}

TLogStoreDescription::TLogStoreDescription(Ydb::LogStore::DescribeLogStoreResult&& desc,
                                           const TDescribeLogStoreSettings& describeSettings)
    : ShardsCount(desc.shards_count())
    , SchemaPresets()
    , Owner(desc.self().owner())
{
    Y_UNUSED(describeSettings);
    for (const auto& sp : desc.schema_presets()) {
        SchemaPresets[sp.name()] = TSchema(sp.schema());
    }
    PermissionToSchemeEntry(desc.self().permissions(), &Permissions);
    PermissionToSchemeEntry(desc.self().effective_permissions(), &EffectivePermissions);
}

void TLogStoreDescription::SerializeTo(Ydb::LogStore::CreateLogStoreRequest& request) const {
    for (const auto& [presetName, presetSchema] : SchemaPresets) {
        auto& pb = *request.add_schema_presets();
        pb.set_name(presetName);
        presetSchema.SerializeTo(*pb.mutable_schema());
    }
    request.set_shards_count(ShardsCount);
}

TDescribeLogStoreResult::TDescribeLogStoreResult(TStatus&& status, Ydb::LogStore::DescribeLogStoreResult&& desc,
    const TDescribeLogStoreSettings& describeSettings)
    : TStatus(std::move(status))
    , LogStoreDescription_(std::move(desc), describeSettings)
{}


TLogTableSharding::TLogTableSharding(const Ydb::LogStore::DescribeLogTableResult& desc)
    : Type(EShardingHashType::HASH_TYPE_UNSPECIFIED)
    , Columns(desc.sharding_columns().begin(), desc.sharding_columns().end())
    , ShardsCount(desc.shards_count())
    , ActiveShardsCount(desc.active_shards_count())
{
    if (desc.sharding_type() == Ydb::LogStore::ShardingHashType::HASH_TYPE_MODULO_N) {
        Type = EShardingHashType::HASH_TYPE_MODULO_N;
    } else if (desc.sharding_type() == Ydb::LogStore::ShardingHashType::HASH_TYPE_LOGS_SPECIAL) {
        Type = EShardingHashType::HASH_TYPE_LOGS_SPECIAL;
    }
}

TLogTableDescription::TLogTableDescription(const TString& schemaPresetName, const TLogTableSharding& sharding)
    : SchemaPresetName(schemaPresetName)
    , Sharding(sharding)
{}

TLogTableDescription::TLogTableDescription(const TSchema& schema, const TLogTableSharding& sharding)
    : Schema(schema)
    , Sharding(sharding)
{}

TLogTableDescription::TLogTableDescription(Ydb::LogStore::DescribeLogTableResult&& desc,
                                           const TDescribeLogTableSettings& describeSettings)
    : Schema(desc.schema())
    , Sharding(desc)
    , TtlSettings(TtlSettingsFromProto(desc.ttl_settings()))
    , Owner(desc.self().owner())
{
    Y_UNUSED(describeSettings);
    PermissionToSchemeEntry(desc.self().permissions(), &Permissions);
    PermissionToSchemeEntry(desc.self().effective_permissions(), &EffectivePermissions);
}

void TLogTableDescription::SerializeTo(Ydb::LogStore::CreateLogTableRequest& request) const {
    if (!Schema.GetColumns().empty()) {
        Schema.SerializeTo(*request.mutable_schema());
    }
    request.set_schema_preset_name(SchemaPresetName);
    request.set_shards_count(Sharding.ShardsCount);
    if (Sharding.ActiveShardsCount) {
        request.set_active_shards_count(Sharding.ActiveShardsCount);
    }
    for (const auto& sc : Sharding.Columns) {
        request.add_sharding_columns(sc);
    }
    Ydb::LogStore::ShardingHashType shardingType = (Sharding.Type == NYdb::NLogStore::HASH_TYPE_MODULO_N) ?
        Ydb::LogStore::ShardingHashType::HASH_TYPE_MODULO_N :
        Ydb::LogStore::ShardingHashType::HASH_TYPE_LOGS_SPECIAL;
    request.set_sharding_type(shardingType);
    if (Sharding.ActiveShardsCount) {
        request.set_active_shards_count(Sharding.ActiveShardsCount);
    }

    if (TtlSettings) {
        TtlSettings->SerializeTo(*request.mutable_ttl_settings());
    } else if (TieringSettings) {
        TieringSettings->SerializeTo(*request.mutable_tiering_settings());
    }
}

TDescribeLogTableResult::TDescribeLogTableResult(TStatus&& status, Ydb::LogStore::DescribeLogTableResult&& desc,
    const TDescribeLogTableSettings& describeSettings)
    : TStatus(std::move(status))
    , LogTableDescription_(std::move(desc), describeSettings)
{}

class TLogStoreClient::TImpl: public TClientImplCommon<TLogStoreClient::TImpl> {
public:
    TImpl(std::shared_ptr<TGRpcConnectionsImpl>&& connections, const TCommonClientSettings& settings)
        : TClientImplCommon(std::move(connections), settings)
    {}

    TAsyncStatus CreateLogStore(const TString& path, TLogStoreDescription&& storeDesc,
        const TCreateLogStoreSettings& settings)
    {
        auto request = MakeOperationRequest<Ydb::LogStore::CreateLogStoreRequest>(settings);
        storeDesc.SerializeTo(request);
        request.set_path(path);
        return RunSimple<
            Ydb::LogStore::V1::LogStoreService,
            Ydb::LogStore::CreateLogStoreRequest,
            Ydb::LogStore::CreateLogStoreResponse>(
            std::move(request),
            &Ydb::LogStore::V1::LogStoreService::Stub::AsyncCreateLogStore,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncDescribeLogStoreResult DescribeLogStore(const TString& path, const TDescribeLogStoreSettings& settings) {
        auto request = MakeOperationRequest<Ydb::LogStore::DescribeLogStoreRequest>(settings);
        request.set_path(path);

        auto promise = NThreading::NewPromise<TDescribeLogStoreResult>();

        auto extractor = [promise, settings]
            (google::protobuf::Any* any, TPlainStatus status) mutable {
                Ydb::LogStore::DescribeLogStoreResult result;
                if (any) {
                    any->UnpackTo(&result);
                }
                TDescribeLogStoreResult describeLogStoreResult(TStatus(std::move(status)),
                    std::move(result), settings);
                promise.SetValue(std::move(describeLogStoreResult));
            };

        Connections_->RunDeferred<
            Ydb::LogStore::V1::LogStoreService,
            Ydb::LogStore::DescribeLogStoreRequest,
            Ydb::LogStore::DescribeLogStoreResponse>(
            std::move(request),
            extractor,
            &Ydb::LogStore::V1::LogStoreService::Stub::AsyncDescribeLogStore,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }

    TAsyncStatus DropLogStore(const TString& path, const TDropLogStoreSettings& settings) {
        auto request = MakeOperationRequest<Ydb::LogStore::DropLogStoreRequest>(settings);
        request.set_path(path);
        return RunSimple<
            Ydb::LogStore::V1::LogStoreService,
            Ydb::LogStore::DropLogStoreRequest,
            Ydb::LogStore::DropLogStoreResponse>(
            std::move(request),
            &Ydb::LogStore::V1::LogStoreService::Stub::AsyncDropLogStore,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncStatus AlterLogStore(const TString& path, const TAlterLogStoreSettings& settings) {
        auto request = MakeOperationRequest<Ydb::LogStore::AlterLogStoreRequest>(settings);
        request.set_path(path);

        // TODO

        return RunSimple<
            Ydb::LogStore::V1::LogStoreService,
            Ydb::LogStore::AlterLogStoreRequest,
            Ydb::LogStore::AlterLogStoreResponse>(
            std::move(request),
            &Ydb::LogStore::V1::LogStoreService::Stub::AsyncAlterLogStore,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncStatus CreateLogTable(const TString& path, TLogTableDescription&& tableDesc,
        const TCreateLogTableSettings& settings)
    {
        auto request = MakeOperationRequest<Ydb::LogStore::CreateLogTableRequest>(settings);
        tableDesc.SerializeTo(request);
        request.set_path(path);
        return RunSimple<
            Ydb::LogStore::V1::LogStoreService,
            Ydb::LogStore::CreateLogTableRequest,
            Ydb::LogStore::CreateLogTableResponse>(
            std::move(request),
            &Ydb::LogStore::V1::LogStoreService::Stub::AsyncCreateLogTable,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncDescribeLogTableResult DescribeLogTable(const TString& path, const TDescribeLogTableSettings& settings) {
        auto request = MakeOperationRequest<Ydb::LogStore::DescribeLogTableRequest>(settings);
        request.set_path(path);

        auto promise = NThreading::NewPromise<TDescribeLogTableResult>();

        auto extractor = [promise, settings]
            (google::protobuf::Any* any, TPlainStatus status) mutable {
                Ydb::LogStore::DescribeLogTableResult result;
                if (any) {
                    any->UnpackTo(&result);
                }
                TDescribeLogTableResult describeLogTableResult(TStatus(std::move(status)),
                    std::move(result), settings);
                promise.SetValue(std::move(describeLogTableResult));
            };

        Connections_->RunDeferred<
            Ydb::LogStore::V1::LogStoreService,
            Ydb::LogStore::DescribeLogTableRequest,
            Ydb::LogStore::DescribeLogTableResponse>(
            std::move(request),
            extractor,
            &Ydb::LogStore::V1::LogStoreService::Stub::AsyncDescribeLogTable,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }

    TAsyncStatus DropLogTable(const TString& path, const TDropLogTableSettings& settings) {
        auto request = MakeOperationRequest<Ydb::LogStore::DropLogTableRequest>(settings);
        request.set_path(path);
        return RunSimple<
            Ydb::LogStore::V1::LogStoreService,
            Ydb::LogStore::DropLogTableRequest,
            Ydb::LogStore::DropLogTableResponse>(
            std::move(request),
            &Ydb::LogStore::V1::LogStoreService::Stub::AsyncDropLogTable,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncStatus AlterLogTable(const TString& path, const TAlterLogTableSettings& settings) {
        auto request = MakeOperationRequest<Ydb::LogStore::AlterLogTableRequest>(settings);
        request.set_path(path);
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
        return RunSimple<
            Ydb::LogStore::V1::LogStoreService,
            Ydb::LogStore::AlterLogTableRequest,
            Ydb::LogStore::AlterLogTableResponse>(
            std::move(request),
            &Ydb::LogStore::V1::LogStoreService::Stub::AsyncAlterLogTable,
            TRpcRequestSettings::Make(settings));
    }
};

TLogStoreClient::TLogStoreClient(const TDriver& driver, const TCommonClientSettings& settings)
    : Impl_(new TImpl(CreateInternalInterface(driver), settings))
{}

TAsyncStatus TLogStoreClient::CreateLogStore(const TString& path, TLogStoreDescription&& storeDesc,
        const TCreateLogStoreSettings& settings)
{
    return Impl_->CreateLogStore(path, std::move(storeDesc), settings);
}

TAsyncDescribeLogStoreResult TLogStoreClient::DescribeLogStore(const TString& path, const TDescribeLogStoreSettings& settings)
{
    return Impl_->DescribeLogStore(path, settings);
}

TAsyncStatus TLogStoreClient::AlterLogStore(const TString& path, const TAlterLogStoreSettings& settings)
{
    return Impl_->AlterLogStore(path, settings);
}

TAsyncStatus TLogStoreClient::DropLogStore(const TString& path, const TDropLogStoreSettings& settings)
{
    return Impl_->DropLogStore(path, settings);
}

TAsyncStatus TLogStoreClient::CreateLogTable(const TString& path, TLogTableDescription&& storeDesc,
        const TCreateLogTableSettings& settings)
{
    return Impl_->CreateLogTable(path, std::move(storeDesc), settings);
}

TAsyncDescribeLogTableResult TLogStoreClient::DescribeLogTable(const TString& path, const TDescribeLogTableSettings& settings)
{
    return Impl_->DescribeLogTable(path, settings);
}

TAsyncStatus TLogStoreClient::DropLogTable(const TString& path, const TDropLogTableSettings& settings)
{
    return Impl_->DropLogTable(path, settings);
}

TAsyncStatus TLogStoreClient::AlterLogTable(const TString& path, const TAlterLogTableSettings& settings)
{
    return Impl_->AlterLogTable(path, settings);
}

TAlterLogTableSettings& TAlterLogTableSettings::AlterTtlSettings(const TMaybe<TAlterTtlSettings>& value) {
    AlterTtlSettings_ = value;
    return *this;
}

const TMaybe<TAlterTtlSettings>& TAlterLogTableSettings::GetAlterTtlSettings() const {
    return AlterTtlSettings_;
}

}}
