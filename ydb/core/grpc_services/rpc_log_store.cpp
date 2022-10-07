#include "service_logstore.h"
#include "rpc_common.h"
#include "rpc_scheme_base.h"

#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/core/ydb_convert/table_settings.h>
#include <ydb/core/scheme/scheme_type_id.h>
#include <ydb/library/mkql_proto/mkql_proto.h>

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/public/api/grpc/draft/ydb_logstore_v1.pb.h>

namespace NKikimr::NGRpcService {

using namespace NActors;
using namespace Ydb;

using TEvCreateLogStoreRequest =
    TGrpcRequestOperationCall<Ydb::LogStore::CreateLogStoreRequest, Ydb::LogStore::CreateLogStoreResponse>;
using TEvDescribeLogStoreRequest =
    TGrpcRequestOperationCall<Ydb::LogStore::DescribeLogStoreRequest, Ydb::LogStore::DescribeLogStoreResponse>;
using TEvDropLogStoreRequest =
    TGrpcRequestOperationCall<Ydb::LogStore::DropLogStoreRequest, Ydb::LogStore::DropLogStoreResponse>;
using TEvAlterLogStoreRequest =
    TGrpcRequestOperationCall<Ydb::LogStore::AlterLogStoreRequest, Ydb::LogStore::AlterLogStoreResponse>;
using TEvCreateLogTableRequest =
    TGrpcRequestOperationCall<Ydb::LogStore::CreateLogTableRequest, Ydb::LogStore::CreateLogTableResponse>;
using TEvDescribeLogTableRequest =
    TGrpcRequestOperationCall<Ydb::LogStore::DescribeLogTableRequest, Ydb::LogStore::DescribeLogTableResponse>;
using TEvDropLogTableRequest =
    TGrpcRequestOperationCall<Ydb::LogStore::DropLogTableRequest, Ydb::LogStore::DropLogTableResponse>;
using TEvAlterLogTableRequest =
    TGrpcRequestOperationCall<Ydb::LogStore::AlterLogTableRequest, Ydb::LogStore::AlterLogTableResponse>;

bool ConvertCompressionFromPublicToInternal(const Ydb::LogStore::Compression& from,
                                            NKikimrSchemeOp::TCompressionOptions& to, TString& error)
{
    switch (from.compression_codec()) {
        case Ydb::LogStore::Compression::CODEC_PLAIN:
            //to.SetCompressionCodec(NKikimrSchemeOp::ColumnCodecPlain);
            error = "LogStores with no compression are disabled.";
            return false;
        case Ydb::LogStore::Compression::CODEC_LZ4:
            to.SetCompressionCodec(NKikimrSchemeOp::ColumnCodecLZ4);
            break;
        case Ydb::LogStore::Compression::CODEC_ZSTD:
            to.SetCompressionCodec(NKikimrSchemeOp::ColumnCodecZSTD);
            break;
        default:
            break;
    }
    if (from.compression_level()) {
        to.SetCompressionLevel(from.compression_level());
    }
    return true;
}

void ConvertCompressionFromInternalToPublic(const NKikimrSchemeOp::TCompressionOptions& from,
                                            Ydb::LogStore::Compression& to)
{
    to.set_compression_codec(Ydb::LogStore::Compression::CODEC_LZ4); // LZ4 if not set
    switch (from.GetCompressionCodec()) {
        case NKikimrSchemeOp::ColumnCodecPlain:
            to.set_compression_codec(Ydb::LogStore::Compression::CODEC_PLAIN);
            break;
        case NKikimrSchemeOp::ColumnCodecLZ4:
            to.set_compression_codec(Ydb::LogStore::Compression::CODEC_LZ4);
            break;
        case NKikimrSchemeOp::ColumnCodecZSTD:
            to.set_compression_codec(Ydb::LogStore::Compression::CODEC_ZSTD);
            break;
        default:
            break;
    }
    to.set_compression_level(from.GetCompressionLevel());
}

static bool IsAllowedFirstPkField(ui32 typeId) {
    switch (typeId) {
        //case NYql::NProto::Bool
        case NYql::NProto::Uint8: // Byte
        case NYql::NProto::Int32:
        case NYql::NProto::Uint32:
        case NYql::NProto::Int64:
        case NYql::NProto::Uint64:
        //case NYql::NProto::Float:
        //case NYql::NProto::Double:
        case NYql::NProto::String:
        case NYql::NProto::Utf8:
        //case NYql::NProto::Yson:
        //case NYql::NProto::Json:
        //case NYql::NProto::JsonDocument:
        case NYql::NProto::Date:
        case NYql::NProto::Datetime:
        case NYql::NProto::Timestamp:
        //case NYql::NProto::Interval:
        //case NYql::NProto::Decimal:
        //case NYql::NProto::DyNumber:
            return true;
        default:
            break;
    }
    return false;
}

bool ConvertSchemaFromPublicToInternal(const Ydb::LogStore::Schema& from, NKikimrSchemeOp::TColumnTableSchema& to,
    Ydb::StatusIds::StatusCode& status, TString& error)
{
    status = Ydb::StatusIds::SCHEME_ERROR;

    to.MutableKeyColumnNames()->CopyFrom(from.primary_key());
    THashSet<TString> key;
    for (auto& column : from.primary_key()) {
        key.insert(column);
    }
    if (key.empty()) {
        error = "no columns in primary key";
        return false;
    }
    TString firstKeyColumn = from.primary_key()[0];

    for (const auto& column : from.columns()) {
        auto* col = to.AddColumns();
        col->SetName(column.name());
        NScheme::TTypeInfo typeInfo;
        if (!ExtractColumnTypeInfo(typeInfo, column.type(), status, error)) {
            return false;
        }
        auto typeName = NScheme::TypeName(typeInfo);
        col->SetType(typeName);

        key.erase(column.name());
        if (column.name() == firstKeyColumn && !IsAllowedFirstPkField(typeInfo.GetTypeId())) {
            error = "not supported first PK column type for LogStore";
            return false;
        }
    }
    if (!key.empty()) {
        error = "unknown cloumn in primary key";
        return false;
    }

    if (from.has_default_compression()) {
        auto& from_compression = from.default_compression();
        auto* to_compression = to.MutableDefaultCompression();
        if (!ConvertCompressionFromPublicToInternal(from_compression, *to_compression, error)) {
            return false;
        }
    }

    to.SetEngine(NKikimrSchemeOp::COLUMN_ENGINE_REPLACING_TIMESERIES);
    status = {};
    return true;
}

bool ConvertSchemaFromInternalToPublic(const NKikimrSchemeOp::TColumnTableSchema& from, Ydb::LogStore::Schema& to,
    Ydb::StatusIds::StatusCode& status, TString& error)
{
    if (from.GetEngine() != NKikimrSchemeOp::COLUMN_ENGINE_REPLACING_TIMESERIES) {
        status = Ydb::StatusIds::INTERNAL_ERROR;
        error = TStringBuilder() << "Unexpected table engine: " << NKikimrSchemeOp::EColumnTableEngine_Name(from.GetEngine());
        return false;
    }
    to.mutable_primary_key()->CopyFrom(from.GetKeyColumnNames());
    for (const auto& column : from.GetColumns()) {
        auto* col = to.add_columns();
        col->set_name(column.GetName());
        ui32 typeId = column.GetTypeId();
        auto& item = *col->mutable_type()->mutable_optional_type()->mutable_item();
        if (typeId == NYql::NProto::TypeIds::Decimal) {
            auto typeParams = item.mutable_decimal_type();
            typeParams->set_precision(22);
            typeParams->set_scale(9);
        } else {
            try {
                NMiniKQL::ExportPrimitiveTypeToProto(typeId, item);
            } catch (...) {
                status = Ydb::StatusIds::INTERNAL_ERROR;
                error = TStringBuilder() << "Unexpected type for column '" << column.GetName() << "': " << column.GetType();
                return false;
            }
        }
    }

    if (from.HasDefaultCompression()) {
        ConvertCompressionFromInternalToPublic(from.GetDefaultCompression(), *to.mutable_default_compression());
    }
    return true;
}


class TCreateLogStoreRPC : public TRpcSchemeRequestActor<TCreateLogStoreRPC, TEvCreateLogStoreRequest> {
    using TBase = TRpcSchemeRequestActor<TCreateLogStoreRPC, TEvCreateLogStoreRequest>;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ;
    }

    explicit TCreateLogStoreRPC(IRequestOpCtx* request)
        : TBase(request)
    {}

    void Bootstrap(const TActorContext &ctx) {
        TBase::Bootstrap(ctx);

        SendProposeRequest(ctx);
        Become(&TCreateLogStoreRPC::StateWork);
    }

private:
    void SendProposeRequest(const TActorContext &ctx) {
        const auto req = GetProtoRequest();
        std::pair<TString, TString> destinationPathPair;
        try {
            destinationPathPair = SplitPath(req->path());
        } catch (const std::exception& ex) {
            Request_->RaiseIssue(NYql::ExceptionToIssue(ex));
            return Reply(StatusIds::BAD_REQUEST, "Invalid path: " + req->path(), NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
        }

        const auto& workingDir = destinationPathPair.first;
        const auto& name = destinationPathPair.second;

        Ydb::StatusIds::StatusCode status;
        TString error;

        std::unique_ptr<TEvTxUserProxy::TEvProposeTransaction> proposeRequest = CreateProposeTransaction();
        NKikimrTxUserProxy::TEvProposeTransaction& record = proposeRequest->Record;
        NKikimrSchemeOp::TModifyScheme* modifyScheme = record.MutableTransaction()->MutableModifyScheme();
        modifyScheme->SetWorkingDir(workingDir);
        modifyScheme->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreateColumnStore);
        auto create = modifyScheme->MutableCreateColumnStore();
        create->SetName(name);
        create->SetColumnShardCount(req->shards_count());
        for (const auto& schemaPreset : req->schema_presets()) {
            auto* toSchemaPreset = create->AddSchemaPresets();
            toSchemaPreset->SetName(schemaPreset.name());
            if (!ConvertSchemaFromPublicToInternal(schemaPreset.schema(), *toSchemaPreset->MutableSchema(), status, error)) {
                LOG_DEBUG(ctx, NKikimrServices::GRPC_SERVER, "LogStore schema error: %s", error.c_str());
                return Reply(status, error, NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
            }
            for (const auto& tier : req->tiers()) {
                auto* toTier = toSchemaPreset->MutableSchema()->AddStorageTiers();
                toTier->SetName(tier.name());
                if (!ConvertCompressionFromPublicToInternal(tier.compression(), *toTier->MutableCompression(), error)) {
                    LOG_DEBUG(ctx, NKikimrServices::GRPC_SERVER, "LogStore schema error: %s", error.c_str());
                    return Reply(status, error, NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
                }
            }
        }
        ctx.Send(MakeTxProxyID(), proposeRequest.release());
    }
};

class TDescribeLogStoreRPC : public TRpcSchemeRequestActor<TDescribeLogStoreRPC, TEvDescribeLogStoreRequest> {
    using TBase = TRpcSchemeRequestActor<TDescribeLogStoreRPC, TEvDescribeLogStoreRequest>;

public:
    TDescribeLogStoreRPC(IRequestOpCtx* request)
        : TBase(request) {}

    void Bootstrap(const TActorContext &ctx) {
        TBase::Bootstrap(ctx);

        SendProposeRequest(ctx);
        Become(&TDescribeLogStoreRPC::StateWork);
    }

private:
    void StateWork(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            default: TBase::StateWork(ev, ctx);
        }
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->GetRecord();
        const auto status = record.GetStatus();
        if (record.HasReason()) {
            auto issue = NYql::TIssue(record.GetReason());
            Request_->RaiseIssue(issue);
        }
        Ydb::LogStore::DescribeLogStoreResult describeLogStoreResult;
        switch (status) {
            case NKikimrScheme::StatusSuccess: {
                const auto& pathDescription = record.GetPathDescription();
                Ydb::Scheme::Entry* selfEntry = describeLogStoreResult.mutable_self();
                selfEntry->set_name(pathDescription.GetSelf().GetName());
                selfEntry->set_type(static_cast<Ydb::Scheme::Entry::Type>(pathDescription.GetSelf().GetPathType()));
                if (pathDescription.GetSelf().GetPathType() != NKikimrSchemeOp::EPathTypeColumnStore) {
                    return Reply(Ydb::StatusIds::BAD_REQUEST, "Path is not LogStore", NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
                }
                ConvertDirectoryEntry(pathDescription.GetSelf(), selfEntry, true);
                const auto& storeDescription = pathDescription.GetColumnStoreDescription();
                describeLogStoreResult.set_shards_count(storeDescription.GetColumnShardCount());

                bool firstPreset = true;
                for (const auto& schemaPreset : storeDescription.GetSchemaPresets()) {
                    auto* toSchemaPreset = describeLogStoreResult.add_schema_presets();
                    toSchemaPreset->set_name(schemaPreset.GetName());
                    Ydb::StatusIds::StatusCode status;
                    TString error;
                    if (!ConvertSchemaFromInternalToPublic(schemaPreset.GetSchema(), *toSchemaPreset->mutable_schema(), status, error)) {
                        LOG_DEBUG(ctx, NKikimrServices::GRPC_SERVER, "LogStore schema error: %s", error.c_str());
                        return Reply(status, error, NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
                    }
                    if (firstPreset) {
                        // Preset's tiers are the same. We take first ones.
                        firstPreset = false;
                        for (auto& tier : schemaPreset.GetSchema().GetStorageTiers()) {
                            auto* to = describeLogStoreResult.add_tiers();
                            to->set_name(tier.GetName());
                            ConvertCompressionFromInternalToPublic(tier.GetCompression(), *to->mutable_compression());
                        }
                    }
                }
                return ReplyWithResult(Ydb::StatusIds::SUCCESS, describeLogStoreResult, ctx);
            }

            case NKikimrScheme::StatusPathDoesNotExist:
            case NKikimrScheme::StatusSchemeError: {
                return Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);
            }

            case NKikimrScheme::StatusAccessDenied: {
                return Reply(Ydb::StatusIds::UNAUTHORIZED, ctx);
            }

            case NKikimrScheme::StatusNotAvailable: {
                return Reply(Ydb::StatusIds::UNAVAILABLE, ctx);
            }

            default: {
                return Reply(Ydb::StatusIds::GENERIC_ERROR, ctx);
            }
        }
    }

    void SendProposeRequest(const TActorContext &ctx) {
        const auto req = GetProtoRequest();

        std::unique_ptr<TEvTxUserProxy::TEvNavigate> navigateRequest(new TEvTxUserProxy::TEvNavigate());
        SetAuthToken(navigateRequest, *Request_);
        SetDatabase(navigateRequest.get(), *Request_);
        NKikimrSchemeOp::TDescribePath* record = navigateRequest->Record.MutableDescribePath();
        record->SetPath(req->path());

        ctx.Send(MakeTxProxyID(), navigateRequest.release());
    }
};

template <class TEv, NKikimrSchemeOp::EOperationType EOpType>
class TDropLogRPC : public TRpcSchemeRequestActor<TDropLogRPC<TEv, EOpType>, TEv> {
    using TSelf = TDropLogRPC<TEv, EOpType>;
    using TBase = TRpcSchemeRequestActor<TSelf, TEv>;

public:
    TDropLogRPC(IRequestOpCtx* msg)
        : TBase(msg) {}

    void Bootstrap(const TActorContext &ctx) {
        TBase::Bootstrap(ctx);

        SendProposeRequest(ctx);
        this->Become(&TSelf::StateWork);
    }

private:
    void SendProposeRequest(const TActorContext &ctx) {
        const auto req = this->GetProtoRequest();
        std::pair<TString, TString> pathPair;
        try {
            pathPair = SplitPath(req->path());
        } catch (const std::exception& ex) {
            this->Request_->RaiseIssue(NYql::ExceptionToIssue(ex));
            return ReplyWithResult(StatusIds::BAD_REQUEST, ctx);
        }

        const auto& workingDir = pathPair.first;
        const auto& name = pathPair.second;

        std::unique_ptr<TEvTxUserProxy::TEvProposeTransaction> proposeRequest = this->CreateProposeTransaction();
        NKikimrTxUserProxy::TEvProposeTransaction& record = proposeRequest->Record;
        NKikimrSchemeOp::TModifyScheme* modifyScheme = record.MutableTransaction()->MutableModifyScheme();
        modifyScheme->SetWorkingDir(workingDir);
        modifyScheme->SetOperationType(EOpType);
        auto drop = modifyScheme->MutableDrop();
        drop->SetName(name);
        ctx.Send(MakeTxProxyID(), proposeRequest.release());
    }

    void ReplyWithResult(StatusIds::StatusCode status, const TActorContext &ctx) {
        this->Request_->ReplyWithYdbStatus(status);
        this->Die(ctx);
    }
};

class TAlterLogStoreRPC : public TRpcSchemeRequestActor<TAlterLogStoreRPC, TEvAlterLogStoreRequest> {
    using TBase = TRpcSchemeRequestActor<TAlterLogStoreRPC, TEvAlterLogStoreRequest>;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ;
    }

    explicit TAlterLogStoreRPC(IRequestOpCtx* request)
        : TBase(request)
    {}

    void Bootstrap(const TActorContext &ctx) {
        TBase::Bootstrap(ctx);

        SendProposeRequest(ctx);
        Become(&TAlterLogStoreRPC::StateWork);
    }

private:
    void SendProposeRequest(const TActorContext &ctx) {
        return Reply(StatusIds::UNSUPPORTED, "Alter LogStore is not implemented",
                     NKikimrIssues::TIssuesIds::UNEXPECTED, ctx);
    }
};

class TCreateLogTableRPC : public TRpcSchemeRequestActor<TCreateLogTableRPC, TEvCreateLogTableRequest> {
    using TBase = TRpcSchemeRequestActor<TCreateLogTableRPC, TEvCreateLogTableRequest>;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ;
    }

    explicit TCreateLogTableRPC(IRequestOpCtx* request)
        : TBase(request)
    {}

    void Bootstrap(const TActorContext &ctx) {
        TBase::Bootstrap(ctx);

        SendProposeRequest(ctx);
        Become(&TCreateLogTableRPC::StateWork);
    }

private:
    void SendProposeRequest(const TActorContext &ctx) {
        const auto req = GetProtoRequest();
        std::pair<TString, TString> destinationPathPair;
        try {
            destinationPathPair = SplitPath(req->path());
        } catch (const std::exception& ex) {
            Request_->RaiseIssue(NYql::ExceptionToIssue(ex));
            return Reply(StatusIds::BAD_REQUEST, "Invalid path: " + req->path(), NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
        }

        const auto& workingDir = destinationPathPair.first;
        const auto& name = destinationPathPair.second;

        Ydb::StatusIds::StatusCode status;
        TString error;

        std::unique_ptr<TEvTxUserProxy::TEvProposeTransaction> proposeRequest = CreateProposeTransaction();
        NKikimrTxUserProxy::TEvProposeTransaction& record = proposeRequest->Record;
        NKikimrSchemeOp::TModifyScheme* modifyScheme = record.MutableTransaction()->MutableModifyScheme();
        modifyScheme->SetWorkingDir(workingDir);
        modifyScheme->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreateColumnTable);
        auto create = modifyScheme->MutableCreateColumnTable();
        create->SetName(name);
        if (!req->schema_preset_name().empty()) {
            create->SetSchemaPresetName(req->schema_preset_name());
        }
        if (req->has_schema()) {
            if (!ConvertSchemaFromPublicToInternal(req->schema(), *create->MutableSchema(), status, error)) {
                LOG_DEBUG(ctx, NKikimrServices::GRPC_SERVER, "LogTable schema error: %s", error.c_str());
                return Reply(status, error, NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
            }
        }

        if (req->has_ttl_settings()) {
            if (!FillTtlSettings(*create->MutableTtlSettings()->MutableEnabled(), req->ttl_settings(), status, error)) {
                return Reply(status, error, NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
            }
        }

        create->SetColumnShardCount(req->shards_count());
        auto* sharding = create->MutableSharding()->MutableHashSharding();
        if (req->sharding_type() == Ydb::LogStore::ShardingHashType::HASH_TYPE_MODULO_N) {
            sharding->SetFunction(NKikimrSchemeOp::TColumnTableSharding::THashSharding::HASH_FUNCTION_MODULO_N);
        } else {
            sharding->SetFunction(NKikimrSchemeOp::TColumnTableSharding::THashSharding::HASH_FUNCTION_CLOUD_LOGS);
            if (req->active_shards_count()) {
                sharding->SetActiveShardsCount(req->active_shards_count());
            }
        }
        sharding->MutableColumns()->CopyFrom(req->sharding_columns());
        ctx.Send(MakeTxProxyID(), proposeRequest.release());
    }
};

class TDescribeLogTableRPC : public TRpcSchemeRequestActor<TDescribeLogTableRPC, TEvDescribeLogTableRequest> {
    using TBase = TRpcSchemeRequestActor<TDescribeLogTableRPC, TEvDescribeLogTableRequest>;

public:
    TDescribeLogTableRPC(IRequestOpCtx* request)
        : TBase(request) {}

    void Bootstrap(const TActorContext &ctx) {
        TBase::Bootstrap(ctx);

        SendProposeRequest(ctx);
        Become(&TDescribeLogTableRPC::StateWork);
    }

private:
    void StateWork(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            default: TBase::StateWork(ev, ctx);
        }
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->GetRecord();
        const auto status = record.GetStatus();
        if (record.HasReason()) {
            auto issue = NYql::TIssue(record.GetReason());
            Request_->RaiseIssue(issue);
        }
        Ydb::LogStore::DescribeLogTableResult describeLogTableResult;
        switch (status) {
            case NKikimrScheme::StatusSuccess: {
                const auto& pathDescription = record.GetPathDescription();
                Ydb::Scheme::Entry* selfEntry = describeLogTableResult.mutable_self();
                selfEntry->set_name(pathDescription.GetSelf().GetName());
                selfEntry->set_type(static_cast<Ydb::Scheme::Entry::Type>(pathDescription.GetSelf().GetPathType()));
                if (pathDescription.GetSelf().GetPathType() != NKikimrSchemeOp::EPathTypeColumnTable) {
                    return Reply(Ydb::StatusIds::BAD_REQUEST, "Path is not LogTable", NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
                }
                ConvertDirectoryEntry(pathDescription.GetSelf(), selfEntry, true);
                const auto& tableDescription = pathDescription.GetColumnTableDescription();
                describeLogTableResult.set_shards_count(tableDescription.GetColumnShardCount());
                Ydb::StatusIds::StatusCode status;
                TString error;
                if (!ConvertSchemaFromInternalToPublic(tableDescription.GetSchema(), *describeLogTableResult.mutable_schema(), status, error)) {
                    LOG_DEBUG(ctx, NKikimrServices::GRPC_SERVER, "LogTable schema error: %s", error.c_str());
                    return Reply(status, error, NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
                }
                if (tableDescription.HasSchemaPresetName()) {
                    describeLogTableResult.set_schema_preset_name(tableDescription.GetSchemaPresetName());
                }

                if (tableDescription.HasTtlSettings() && tableDescription.GetTtlSettings().HasEnabled()) {
                    const auto& inTTL = tableDescription.GetTtlSettings().GetEnabled();

                    switch (inTTL.GetColumnUnit()) {
                    case NKikimrSchemeOp::TTTLSettings::UNIT_AUTO: {
                        auto& outTTL = *describeLogTableResult.mutable_ttl_settings()->mutable_date_type_column();
                        outTTL.set_column_name(inTTL.GetColumnName());
                        outTTL.set_expire_after_seconds(inTTL.GetExpireAfterSeconds());
                        break;
                    }

                    case NKikimrSchemeOp::TTTLSettings::UNIT_SECONDS:
                    case NKikimrSchemeOp::TTTLSettings::UNIT_MILLISECONDS:
                    case NKikimrSchemeOp::TTTLSettings::UNIT_MICROSECONDS:
                    case NKikimrSchemeOp::TTTLSettings::UNIT_NANOSECONDS: {
                        auto& outTTL = *describeLogTableResult.mutable_ttl_settings()->mutable_value_since_unix_epoch();
                        outTTL.set_column_name(inTTL.GetColumnName());
                        outTTL.set_column_unit(static_cast<Ydb::Table::ValueSinceUnixEpochModeSettings::Unit>(inTTL.GetColumnUnit()));
                        outTTL.set_expire_after_seconds(inTTL.GetExpireAfterSeconds());
                        break;
                    }

                    default:
                        break;
                    }
                }

                return ReplyWithResult(Ydb::StatusIds::SUCCESS, describeLogTableResult, ctx);
            }

            case NKikimrScheme::StatusPathDoesNotExist:
            case NKikimrScheme::StatusSchemeError: {
                return Reply(Ydb::StatusIds::SCHEME_ERROR, ctx);
            }

            case NKikimrScheme::StatusAccessDenied: {
                return Reply(Ydb::StatusIds::UNAUTHORIZED, ctx);
            }

            case NKikimrScheme::StatusNotAvailable: {
                return Reply(Ydb::StatusIds::UNAVAILABLE, ctx);
            }

            default: {
                return Reply(Ydb::StatusIds::GENERIC_ERROR, ctx);
            }
        }
    }

    void SendProposeRequest(const TActorContext &ctx) {
        const auto req = GetProtoRequest();

        std::unique_ptr<TEvTxUserProxy::TEvNavigate> navigateRequest(new TEvTxUserProxy::TEvNavigate());
        SetAuthToken(navigateRequest, *Request_);
        SetDatabase(navigateRequest.get(), *Request_);
        NKikimrSchemeOp::TDescribePath* record = navigateRequest->Record.MutableDescribePath();
        record->SetPath(req->path());

        ctx.Send(MakeTxProxyID(), navigateRequest.release());
    }
};

class TAlterLogTableRPC : public TRpcSchemeRequestActor<TAlterLogTableRPC, TEvAlterLogTableRequest> {
    using TBase = TRpcSchemeRequestActor<TAlterLogTableRPC, TEvAlterLogTableRequest>;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ;
    }

    explicit TAlterLogTableRPC(IRequestOpCtx* request)
        : TBase(request)
    {}

    void Bootstrap(const TActorContext &ctx) {
        TBase::Bootstrap(ctx);

        SendProposeRequest(ctx);
        Become(&TAlterLogTableRPC::StateWork);
    }

private:
    void SendProposeRequest(const TActorContext &ctx) {
        const auto req = GetProtoRequest();
        std::pair<TString, TString> destinationPathPair;
        try {
            destinationPathPair = SplitPath(req->path());
        } catch (const std::exception& ex) {
            Request_->RaiseIssue(NYql::ExceptionToIssue(ex));
            return Reply(StatusIds::BAD_REQUEST, "Invalid path: " + req->path(), NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
        }

        const auto& workingDir = destinationPathPair.first;
        const auto& name = destinationPathPair.second;

        std::unique_ptr<TEvTxUserProxy::TEvProposeTransaction> proposeRequest = CreateProposeTransaction();
        NKikimrTxUserProxy::TEvProposeTransaction& record = proposeRequest->Record;
        NKikimrSchemeOp::TModifyScheme* modifyScheme = record.MutableTransaction()->MutableModifyScheme();
        modifyScheme->SetWorkingDir(workingDir);
        modifyScheme->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpAlterColumnTable);
        auto alter = modifyScheme->MutableAlterColumnTable();
        alter->SetName(name);

        Ydb::StatusIds::StatusCode status;
        TString error;
        if (req->has_set_ttl_settings()) {
            if (!FillTtlSettings(*alter->MutableAlterTtlSettings()->MutableEnabled(), req->set_ttl_settings(), status, error)) {
                return Reply(status, error, NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
            }
        } else if (req->has_drop_ttl_settings()) {
            alter->MutableAlterTtlSettings()->MutableDisabled();
        }

        ctx.Send(MakeTxProxyID(), proposeRequest.release());
    }
};


using TDropLogStoreRPC = TDropLogRPC<TEvDropLogStoreRequest, NKikimrSchemeOp::EOperationType::ESchemeOpDropColumnStore>;
using TDropLogTableRPC = TDropLogRPC<TEvDropLogTableRequest, NKikimrSchemeOp::EOperationType::ESchemeOpDropColumnTable>;

void DoCreateLogStoreRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TCreateLogStoreRPC(p.release()));
}

void DoDescribeLogStoreRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TDescribeLogStoreRPC(p.release()));
}

void DoDropLogStoreRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TDropLogStoreRPC(p.release()));
}

void DoAlterLogStoreRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TAlterLogStoreRPC(p.release()));
}


void DoCreateLogTableRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TCreateLogTableRPC(p.release()));
}

void DoDescribeLogTableRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TDescribeLogTableRPC(p.release()));
}

void DoDropLogTableRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TDropLogTableRPC(p.release()));
}

void DoAlterLogTableRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TAlterLogTableRPC(p.release()));
}

}
