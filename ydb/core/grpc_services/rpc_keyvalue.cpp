#include "service_keyvalue.h"

#include <ydb/public/api/protos/ydb_keyvalue.pb.h>

#include <ydb/core/base/path.h>
#include <ydb/core/grpc_services/rpc_scheme_base.h>
#include <ydb/core/grpc_services/rpc_common/rpc_common.h>
#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/mind/local.h>
#include <ydb/core/protos/local.pb.h>


namespace NKikimr::NGRpcService {

using namespace NActors;
using namespace Ydb;

using TEvCreateVolumeKeyValueRequest =
    TGrpcRequestOperationCall<Ydb::KeyValue::CreateVolumeRequest,
        Ydb::KeyValue::CreateVolumeResponse>;
using TEvDropVolumeKeyValueRequest =
    TGrpcRequestOperationCall<Ydb::KeyValue::DropVolumeRequest,
        Ydb::KeyValue::DropVolumeResponse>;
using TEvAlterVolumeKeyValueRequest =
    TGrpcRequestOperationCall<Ydb::KeyValue::AlterVolumeRequest,
        Ydb::KeyValue::AlterVolumeResponse>;
using TEvDescribeVolumeKeyValueRequest =
    TGrpcRequestOperationCall<Ydb::KeyValue::DescribeVolumeRequest,
        Ydb::KeyValue::DescribeVolumeResponse>;
using TEvListLocalPartitionsKeyValueRequest =
    TGrpcRequestOperationCall<Ydb::KeyValue::ListLocalPartitionsRequest,
        Ydb::KeyValue::ListLocalPartitionsResponse>;

using TEvAcquireLockKeyValueRequest =
    TGrpcRequestOperationCall<Ydb::KeyValue::AcquireLockRequest,
        Ydb::KeyValue::AcquireLockResponse>;
using TEvExecuteTransactionKeyValueRequest =
    TGrpcRequestOperationCall<Ydb::KeyValue::ExecuteTransactionRequest,
        Ydb::KeyValue::ExecuteTransactionResponse>;
using TEvReadKeyValueRequest =
    TGrpcRequestOperationCall<Ydb::KeyValue::ReadRequest,
        Ydb::KeyValue::ReadResponse>;
using TEvReadRangeKeyValueRequest =
    TGrpcRequestOperationCall<Ydb::KeyValue::ReadRangeRequest,
        Ydb::KeyValue::ReadRangeResponse>;
using TEvListRangeKeyValueRequest =
    TGrpcRequestOperationCall<Ydb::KeyValue::ListRangeRequest,
        Ydb::KeyValue::ListRangeResponse>;
using TEvGetStorageChannelStatusKeyValueRequest =
    TGrpcRequestOperationCall<Ydb::KeyValue::GetStorageChannelStatusRequest,
        Ydb::KeyValue::GetStorageChannelStatusResponse>;

} // namespace NKikimr::NGRpcService


namespace NKikimr::NGRpcService {

using namespace NActors;
using namespace Ydb;

#define COPY_PRIMITIVE_FIELD(name) \
    to->set_ ## name(static_cast<decltype(to->name())>(from.name())) \
// COPY_PRIMITIVE_FIELD

#define COPY_PRIMITIVE_OPTIONAL_FIELD(name) \
    if (from.has_ ## name()) { \
        to->set_ ## name(static_cast<decltype(to->name())>(from.name())); \
    } \
// COPY_PRIMITIVE_FIELD

namespace {

void CopyProtobuf(const Ydb::KeyValue::AcquireLockRequest &/*from*/,
        NKikimrKeyValue::AcquireLockRequest */*to*/)
{
}

void CopyProtobuf(const NKikimrKeyValue::AcquireLockResult &from,
        Ydb::KeyValue::AcquireLockResult *to)
{
    COPY_PRIMITIVE_FIELD(lock_generation);
    COPY_PRIMITIVE_FIELD(node_id);
}


void CopyProtobuf(const Ydb::KeyValue::ExecuteTransactionRequest::Command::Rename &from,
        NKikimrKeyValue::ExecuteTransactionRequest::Command::Rename *to)
{
    COPY_PRIMITIVE_FIELD(old_key);
    COPY_PRIMITIVE_FIELD(new_key);
}

void CopyProtobuf(const Ydb::KeyValue::ExecuteTransactionRequest::Command::Concat &from,
        NKikimrKeyValue::ExecuteTransactionRequest::Command::Concat *to)
{
    *to->mutable_input_keys() = from.input_keys();
    COPY_PRIMITIVE_FIELD(output_key);
    COPY_PRIMITIVE_FIELD(keep_inputs);
}

void CopyProtobuf(const Ydb::KeyValue::KeyRange &from, NKikimrKeyValue::KVRange *to) {
#define CHECK_AND_SET(name) \
    if (from.has_ ## name()) { \
        COPY_PRIMITIVE_FIELD(name); \
    } \
// CHECK_AND_SET

    CHECK_AND_SET(from_key_inclusive)
    CHECK_AND_SET(from_key_exclusive)
    CHECK_AND_SET(to_key_inclusive)
    CHECK_AND_SET(to_key_exclusive)

#undef CHECK_AND_SET
}

void CopyProtobuf(const Ydb::KeyValue::ExecuteTransactionRequest::Command::CopyRange &from,
        NKikimrKeyValue::ExecuteTransactionRequest::Command::CopyRange *to)
{
    CopyProtobuf(from.range(), to->mutable_range());
    COPY_PRIMITIVE_FIELD(prefix_to_remove);
    COPY_PRIMITIVE_FIELD(prefix_to_add);
}

template <typename TProtoFrom, typename TProtoTo>
void CopyPriority(TProtoFrom &&from, TProtoTo *to) {
    switch(from.priority()) {
    case Ydb::KeyValue::Priorities::PRIORITY_REALTIME:
        to->set_priority(NKikimrKeyValue::Priorities::PRIORITY_REALTIME);
        break;
    case Ydb::KeyValue::Priorities::PRIORITY_BACKGROUND:
        to->set_priority(NKikimrKeyValue::Priorities::PRIORITY_BACKGROUND);
        break;
    default:
        to->set_priority(NKikimrKeyValue::Priorities::PRIORITY_UNSPECIFIED);
        break;
    }
}

void CopyProtobuf(const Ydb::KeyValue::ExecuteTransactionRequest::Command::Write &from,
        NKikimrKeyValue::ExecuteTransactionRequest::Command::Write *to)
{
    COPY_PRIMITIVE_FIELD(key);
    COPY_PRIMITIVE_FIELD(value);
    COPY_PRIMITIVE_FIELD(storage_channel);
    CopyPriority(from, to);
    switch(from.tactic()) {
    case Ydb::KeyValue::ExecuteTransactionRequest::Command::Write::TACTIC_MAX_THROUGHPUT:
        to->set_tactic(NKikimrKeyValue::ExecuteTransactionRequest::Command::Write::TACTIC_MAX_THROUGHPUT);
        break;
    case Ydb::KeyValue::ExecuteTransactionRequest::Command::Write::TACTIC_MIN_LATENCY:
        to->set_tactic(NKikimrKeyValue::ExecuteTransactionRequest::Command::Write::TACTIC_MIN_LATENCY);
        break;
    default:
        to->set_tactic(NKikimrKeyValue::ExecuteTransactionRequest::Command::Write::TACTIC_UNSPECIFIED);
        break;
    }
}

void CopyProtobuf(const Ydb::KeyValue::ExecuteTransactionRequest::Command::DeleteRange &from,
        NKikimrKeyValue::ExecuteTransactionRequest::Command::DeleteRange *to)
{
    CopyProtobuf(from.range(), to->mutable_range());
}

void CopyProtobuf(const Ydb::KeyValue::ExecuteTransactionRequest::Command &from,
        NKikimrKeyValue::ExecuteTransactionRequest::Command *to)
{
#define CHECK_AND_COPY(name) \
    if (from.has_ ## name()) { \
        CopyProtobuf(from.name(), to->mutable_ ## name()); \
    } \
// CHECK_AND_COPY

    CHECK_AND_COPY(rename)
    CHECK_AND_COPY(concat)
    CHECK_AND_COPY(copy_range)
    CHECK_AND_COPY(write)
    CHECK_AND_COPY(delete_range)

#undef CHECK_AND_COPY
}

void CopyProtobuf(const Ydb::KeyValue::ExecuteTransactionRequest &from,
        NKikimrKeyValue::ExecuteTransactionRequest *to)
{
    COPY_PRIMITIVE_OPTIONAL_FIELD(lock_generation);
    for (auto &cmd : from.commands()) {
        CopyProtobuf(cmd, to->add_commands());
    }
}

void CopyProtobuf(const NKikimrKeyValue::StorageChannel &from, Ydb::KeyValue::StorageChannelInfo *to) {
    COPY_PRIMITIVE_FIELD(storage_channel);
    COPY_PRIMITIVE_FIELD(status_flag);
}

void CopyProtobuf(const NKikimrKeyValue::ExecuteTransactionResult &from,
        Ydb::KeyValue::ExecuteTransactionResult *to)
{
    COPY_PRIMITIVE_FIELD(node_id);
    for (auto &channel : from.storage_channel()) {
        CopyProtobuf(channel, to->add_storage_channel_info());
    }
}

void CopyProtobuf(const Ydb::KeyValue::ReadRequest &from, NKikimrKeyValue::ReadRequest *to) {
    COPY_PRIMITIVE_OPTIONAL_FIELD(lock_generation);
    COPY_PRIMITIVE_FIELD(key);
    COPY_PRIMITIVE_FIELD(offset);
    COPY_PRIMITIVE_FIELD(size);
    CopyPriority(from, to);
    COPY_PRIMITIVE_FIELD(limit_bytes);
}

void CopyProtobuf(const NKikimrKeyValue::ReadResult &from, Ydb::KeyValue::ReadResult *to) {
    COPY_PRIMITIVE_FIELD(requested_key);
    COPY_PRIMITIVE_FIELD(requested_offset);
    COPY_PRIMITIVE_FIELD(requested_size);
    COPY_PRIMITIVE_FIELD(value);
    COPY_PRIMITIVE_FIELD(node_id);
    switch (from.status()) {
        case NKikimrKeyValue::Statuses::RSTATUS_OVERRUN:
            to->set_is_overrun(true);
            break;
        default:
            break;
    }
}

void CopyProtobuf(const Ydb::KeyValue::ReadRangeRequest &from, NKikimrKeyValue::ReadRangeRequest *to) {
    COPY_PRIMITIVE_OPTIONAL_FIELD(lock_generation);
    CopyProtobuf(from.range(), to->mutable_range());
    to->set_include_data(true);
    COPY_PRIMITIVE_FIELD(limit_bytes);
    CopyPriority(from, to);
}

void CopyProtobuf(const Ydb::KeyValue::ListRangeRequest &from, NKikimrKeyValue::ReadRangeRequest *to) {
    COPY_PRIMITIVE_OPTIONAL_FIELD(lock_generation);
    CopyProtobuf(from.range(), to->mutable_range());
    to->set_include_data(false);
    COPY_PRIMITIVE_FIELD(limit_bytes);
}

void CopyProtobuf(const NKikimrKeyValue::ReadRangeResult::KeyValuePair &from,
        Ydb::KeyValue::ReadRangeResult::KeyValuePair *to)
{
    COPY_PRIMITIVE_FIELD(key);
    COPY_PRIMITIVE_FIELD(value);
    COPY_PRIMITIVE_FIELD(creation_unix_time);
    COPY_PRIMITIVE_FIELD(storage_channel);
}

void CopyProtobuf(const NKikimrKeyValue::ReadRangeResult &from,
        Ydb::KeyValue::ReadRangeResult *to)
{
    for (auto &pair : from.pair()) {
        CopyProtobuf(pair, to->add_pair());
    }
    if (from.status() == NKikimrKeyValue::Statuses::RSTATUS_OVERRUN) {
        to->set_is_overrun(true);
    }
    COPY_PRIMITIVE_FIELD(node_id);
}

void CopyProtobuf(const NKikimrKeyValue::ReadRangeResult::KeyValuePair &from,
        Ydb::KeyValue::ListRangeResult::KeyInfo *to)
{
    COPY_PRIMITIVE_FIELD(key);
    COPY_PRIMITIVE_FIELD(value_size);
    COPY_PRIMITIVE_FIELD(creation_unix_time);
    COPY_PRIMITIVE_FIELD(storage_channel);
}

void CopyProtobuf(const NKikimrKeyValue::ReadRangeResult &from,
        Ydb::KeyValue::ListRangeResult *to)
{
    for (auto &pair : from.pair()) {
        CopyProtobuf(pair, to->add_key());
    }
    if (from.status() == NKikimrKeyValue::Statuses::RSTATUS_OVERRUN) {
        to->set_is_overrun(true);
    }
    COPY_PRIMITIVE_FIELD(node_id);
}

void CopyProtobuf(const Ydb::KeyValue::GetStorageChannelStatusRequest &from,
        NKikimrKeyValue::GetStorageChannelStatusRequest *to)
{
    COPY_PRIMITIVE_OPTIONAL_FIELD(lock_generation);
    *to->mutable_storage_channel() = from.storage_channel();
}


void CopyProtobuf(const NKikimrKeyValue::GetStorageChannelStatusResult &from,
        Ydb::KeyValue::GetStorageChannelStatusResult *to)
{
    for (auto &channel : from.storage_channel()) {
        CopyProtobuf(channel, to->add_storage_channel_info());
    }
    COPY_PRIMITIVE_FIELD(node_id);
}


Ydb::StatusIds::StatusCode PullStatus(const NKikimrKeyValue::AcquireLockResult &) {
    return Ydb::StatusIds::SUCCESS;
}

template <typename TResult>
Ydb::StatusIds::StatusCode PullStatus(const TResult &result) {
    switch (result.status()) {
    case NKikimrKeyValue::Statuses::RSTATUS_OK:
    case NKikimrKeyValue::Statuses::RSTATUS_OVERRUN:
        return Ydb::StatusIds::SUCCESS;
    case NKikimrKeyValue::Statuses::RSTATUS_ERROR:
        return Ydb::StatusIds::GENERIC_ERROR;
    case NKikimrKeyValue::Statuses::RSTATUS_TIMEOUT:
        return Ydb::StatusIds::TIMEOUT;
    case NKikimrKeyValue::Statuses::RSTATUS_NOT_FOUND:
        return Ydb::StatusIds::NOT_FOUND;
    case NKikimrKeyValue::Statuses::RSTATUS_WRONG_LOCK_GENERATION:
        return Ydb::StatusIds::PRECONDITION_FAILED;
    default:
        return Ydb::StatusIds::INTERNAL_ERROR;
    }
}

namespace {
    void AssignPoolKinds(auto &storageConfig, auto *internalStorageConfig) {
        ui32 size = storageConfig.channel_size();

        for (ui32 channelIdx = 0; channelIdx < size; ++channelIdx) {
            internalStorageConfig->AddChannel()->SetPreferredPoolKind(storageConfig.channel(channelIdx).media());
        }
    }
}


class TCreateVolumeRequest : public TRpcSchemeRequestActor<TCreateVolumeRequest, TEvCreateVolumeKeyValueRequest> {
public:
    using TBase = TRpcSchemeRequestActor<TCreateVolumeRequest, TEvCreateVolumeKeyValueRequest>;
    using TBase::TBase;

    void Bootstrap(const TActorContext& ctx) {
        TBase::Bootstrap(ctx);
        Become(&TCreateVolumeRequest::StateFunc);
        SendProposeRequest(ctx);
    }

    void SendProposeRequest(const TActorContext &ctx) {
        const auto req = this->GetProtoRequest();

        std::pair<TString, TString> pathPair;
        try {
            pathPair = SplitPath(Request_->GetDatabaseName(), req->path());
        } catch (const std::exception& ex) {
            Request_->RaiseIssue(NYql::ExceptionToIssue(ex));
            return Reply(StatusIds::BAD_REQUEST, ctx);
        }
        const auto& workingDir = pathPair.first;
        const auto& name = pathPair.second;

        std::unique_ptr<TEvTxUserProxy::TEvProposeTransaction> proposeRequest = this->CreateProposeTransaction();
        NKikimrTxUserProxy::TEvProposeTransaction& record = proposeRequest->Record;
        NKikimrSchemeOp::TModifyScheme* modifyScheme = record.MutableTransaction()->MutableModifyScheme();
        modifyScheme->SetWorkingDir(workingDir);
        NKikimrSchemeOp::TCreateSolomonVolume* tableDesc = nullptr;

        modifyScheme->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreateSolomonVolume);
        tableDesc = modifyScheme->MutableCreateSolomonVolume();
        tableDesc->SetName(name);
        tableDesc->SetPartitionCount(req->partition_count());

        if (GetProtoRequest()->has_storage_config()) {
            auto &storageConfig = GetProtoRequest()->storage_config();
            auto *internalStorageConfig = tableDesc->MutableStorageConfig();
            AssignPoolKinds(storageConfig, internalStorageConfig);
        } else {
            tableDesc->SetChannelProfileId(GetProtoRequest()->partition_count());
        }

        ctx.Send(MakeTxProxyID(), proposeRequest.release());
    }

    STFUNC(StateFunc) {
        return TBase::StateWork(ev);
    }
};


class TDropVolumeRequest : public TRpcSchemeRequestActor<TDropVolumeRequest, TEvDropVolumeKeyValueRequest> {
public:
    using TBase = TRpcSchemeRequestActor<TDropVolumeRequest, TEvDropVolumeKeyValueRequest>;
    using TBase::TBase;

    void Bootstrap(const TActorContext& ctx) {
        TBase::Bootstrap(ctx);
        Become(&TDropVolumeRequest::StateFunc);
        SendProposeRequest(ctx);
    }

    void SendProposeRequest(const TActorContext &ctx) {
        const auto req = this->GetProtoRequest();

        std::pair<TString, TString> pathPair;
        try {
            pathPair = SplitPath(req->path());
        } catch (const std::exception& ex) {
            Request_->RaiseIssue(NYql::ExceptionToIssue(ex));
            return Reply(StatusIds::BAD_REQUEST, ctx);
        }
        const auto& workingDir = pathPair.first;
        const auto& name = pathPair.second;

        std::unique_ptr<TEvTxUserProxy::TEvProposeTransaction> proposeRequest = this->CreateProposeTransaction();
        NKikimrTxUserProxy::TEvProposeTransaction& record = proposeRequest->Record;
        NKikimrSchemeOp::TModifyScheme* modifyScheme = record.MutableTransaction()->MutableModifyScheme();
        modifyScheme->SetWorkingDir(workingDir);
        NKikimrSchemeOp::TDrop* drop = nullptr;

        modifyScheme->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpDropSolomonVolume);
        drop = modifyScheme->MutableDrop();
        drop->SetName(name);

        ctx.Send(MakeTxProxyID(), proposeRequest.release());
    }

    STFUNC(StateFunc) {
        return TBase::StateWork(ev);
    }
};

class TAlterVolumeRequest : public TRpcSchemeRequestActor<TAlterVolumeRequest, TEvAlterVolumeKeyValueRequest> {
public:
    using TBase = TRpcSchemeRequestActor<TAlterVolumeRequest, TEvAlterVolumeKeyValueRequest>;
    using TBase::TBase;

    void Bootstrap(const TActorContext& ctx) {
        TBase::Bootstrap(ctx);
        Become(&TAlterVolumeRequest::StateFunc);
        SendProposeRequest(ctx);
    }

    void SendProposeRequest(const TActorContext &ctx) {
        const auto req = this->GetProtoRequest();

        std::pair<TString, TString> pathPair;
        try {
            pathPair = SplitPath(req->path());
        } catch (const std::exception& ex) {
            Request_->RaiseIssue(NYql::ExceptionToIssue(ex));
            return Reply(StatusIds::BAD_REQUEST, ctx);
        }
        const auto& workingDir = pathPair.first;
        const auto& name = pathPair.second;

        std::unique_ptr<TEvTxUserProxy::TEvProposeTransaction> proposeRequest = this->CreateProposeTransaction();
        NKikimrTxUserProxy::TEvProposeTransaction& record = proposeRequest->Record;
        NKikimrSchemeOp::TModifyScheme* modifyScheme = record.MutableTransaction()->MutableModifyScheme();
        modifyScheme->SetWorkingDir(workingDir);
        NKikimrSchemeOp::TAlterSolomonVolume* tableDesc = nullptr;

        modifyScheme->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpAlterSolomonVolume);
        tableDesc = modifyScheme->MutableAlterSolomonVolume();
        tableDesc->SetName(name);
        tableDesc->SetPartitionCount(req->alter_partition_count());

        if (GetProtoRequest()->has_storage_config()) {
            tableDesc->SetUpdateChannelsBinding(true);
            auto &storageConfig = GetProtoRequest()->storage_config();
            auto *internalStorageConfig = tableDesc->MutableStorageConfig();
            AssignPoolKinds(storageConfig, internalStorageConfig);
        } else {
            tableDesc->SetUpdateChannelsBinding(false);
            tableDesc->SetChannelProfileId(0);
        }

        ctx.Send(MakeTxProxyID(), proposeRequest.release());
    }

    STFUNC(StateFunc) {
        return TBase::StateWork(ev);
    }
};

template <typename TDerived>
class TBaseKeyValueRequest {
protected:
    void OnBootstrap() {
        auto self = static_cast<TDerived*>(this);
        Ydb::StatusIds::StatusCode status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
        NYql::TIssues issues;
        if (!self->ValidateRequest(status, issues)) {
            self->Reply(status, issues, self->ActorContext());
            return;
        }
        if (const auto& userToken = self->Request_->GetSerializedToken()) {
            UserToken = new NACLib::TUserToken(userToken);
        }
        SendNavigateRequest();
    }

    void SendNavigateRequest() {
        auto self = static_cast<TDerived*>(this);
        auto &rec = *self->GetProtoRequest();
        auto req = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        auto& entry = req->ResultSet.emplace_back();
        entry.Path = ::NKikimr::SplitPath(rec.path());
        entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByPath;
        entry.ShowPrivatePath = true;
        entry.SyncVersion = false;
        req->UserToken = UserToken;
        req->DatabaseName = self->Request_->GetDatabaseName().GetOrElse("");
        auto ev = new TEvTxProxySchemeCache::TEvNavigateKeySet(req.Release());
        self->Send(MakeSchemeCacheID(), ev, 0, 0, self->Span_.GetTraceId());
    }

    bool OnNavigateKeySetResult(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr &ev, ui32 access) {
        auto self = static_cast<TDerived*>(this);
        TEvTxProxySchemeCache::TEvNavigateKeySetResult* res = ev->Get();
        NSchemeCache::TSchemeCacheNavigate *request = res->Request.Get();

        auto ctx = self->ActorContext();

        if (res->Request->ResultSet.size() != 1) {
            self->Reply(StatusIds::INTERNAL_ERROR, "Received an incorrect answer from SchemeCache.", NKikimrIssues::TIssuesIds::UNEXPECTED, ctx);
            return false;
        }

        switch (request->ResultSet[0].Status) {
        case NSchemeCache::TSchemeCacheNavigate::EStatus::Ok:
            break;
        case NSchemeCache::TSchemeCacheNavigate::EStatus::AccessDenied:
            self->Reply(StatusIds::UNAUTHORIZED, "Access denied.", NKikimrIssues::TIssuesIds::ACCESS_DENIED, ctx);
            return false;
        case NSchemeCache::TSchemeCacheNavigate::EStatus::RootUnknown:
        case NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown:
            self->Reply(StatusIds::SCHEME_ERROR, "Path isn't exist.", NKikimrIssues::TIssuesIds::PATH_NOT_EXIST, ctx);
            return false;
        case NSchemeCache::TSchemeCacheNavigate::EStatus::LookupError:
        case NSchemeCache::TSchemeCacheNavigate::EStatus::RedirectLookupError:
            self->Reply(StatusIds::UNAVAILABLE, "Database resolve failed with no certain result.", NKikimrIssues::TIssuesIds::RESOLVE_LOOKUP_ERROR, ctx);
            return false;
        default:
            self->Reply(StatusIds::UNAVAILABLE, "Resolve error", NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR, ctx);
            return false;
        }

        if (!self->CheckAccess(CanonizePath(res->Request->ResultSet[0].Path), res->Request->ResultSet[0].SecurityObject, access)) {
            return false;
        }
        if (!request->ResultSet[0].SolomonVolumeInfo) {
            self->Reply(StatusIds::SCHEME_ERROR, "Table isn't keyvalue.", NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
            return false;
        }

        return true;
    }

    bool CheckAccess(const TString& path, TIntrusivePtr<TSecurityObject> securityObject, ui32 access) {
        auto self = static_cast<TDerived*>(this);
        if (!UserToken || !securityObject) {
            return true;
        }

        if (securityObject->CheckAccess(access, *UserToken)) {
            return true;
        }

        self->Reply(Ydb::StatusIds::UNAUTHORIZED,
            TStringBuilder() << "Access denied"
                << ": for# " << UserToken->GetUserSID()
                << ", path# " << path
                << ", access# " << NACLib::AccessRightsToString(access),
            NKikimrIssues::TIssuesIds::ACCESS_DENIED,
            self->ActorContext());
        return false;
    }

private:
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
};

class TDescribeVolumeRequest
        : public TRpcOperationRequestActor<TDescribeVolumeRequest, TEvDescribeVolumeKeyValueRequest>
        , public TBaseKeyValueRequest<TDescribeVolumeRequest>
{
public:
    using TBase = TRpcOperationRequestActor<TDescribeVolumeRequest, TEvDescribeVolumeKeyValueRequest>;
    using TBase::TBase;

    friend class TBaseKeyValueRequest<TDescribeVolumeRequest>;

    void Bootstrap(const TActorContext& ctx) {
        TBase::Bootstrap(ctx);
        OnBootstrap();
        Become(&TDescribeVolumeRequest::StateFunc);
    }


protected:
    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
        default:
            return TBase::StateFuncBase(ev);
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr &ev) {
        TEvTxProxySchemeCache::TEvNavigateKeySetResult* res = ev->Get();
        NSchemeCache::TSchemeCacheNavigate *request = res->Request.Get();

        if (!OnNavigateKeySetResult(ev, NACLib::DescribeSchema)) {
            return;
        }

        const NKikimrSchemeOp::TSolomonVolumeDescription &desc = request->ResultSet[0].SolomonVolumeInfo->Description;
        Ydb::KeyValue::DescribeVolumeResult result;
        result.set_path(this->GetProtoRequest()->path());
        result.set_partition_count(desc.PartitionsSize());
        this->ReplyWithResult(Ydb::StatusIds::SUCCESS, result, TActivationContext::AsActorContext());
    }

    bool ValidateRequest(Ydb::StatusIds::StatusCode& /*status*/, NYql::TIssues& /*issues*/) {
        return true;
    }

private:
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
};


class TListLocalPartitionsRequest
        : public TRpcOperationRequestActor<TListLocalPartitionsRequest, TEvListLocalPartitionsKeyValueRequest>
        , public TBaseKeyValueRequest<TListLocalPartitionsRequest>
{
public:
    using TBase = TRpcOperationRequestActor<TListLocalPartitionsRequest, TEvListLocalPartitionsKeyValueRequest>;
    using TBase::TBase;

    friend class TBaseKeyValueRequest<TListLocalPartitionsRequest>;

    void Bootstrap(const TActorContext& ctx) {
        TBase::Bootstrap(ctx);
        OnBootstrap();
        Become(&TListLocalPartitionsRequest::StateFunc);
    }

protected:
    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvLocal::TEvEnumerateTabletsResult, Handle);
        default:
            return TBase::StateFuncBase(ev);
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr &ev) {
        TEvTxProxySchemeCache::TEvNavigateKeySetResult* res = ev->Get();
        NSchemeCache::TSchemeCacheNavigate *request = res->Request.Get();

        if (!OnNavigateKeySetResult(ev, NACLib::DescribeSchema)) {
            return;
        }

        const NKikimrSchemeOp::TSolomonVolumeDescription &desc = request->ResultSet[0].SolomonVolumeInfo->Description;
        for (const NKikimrSchemeOp::TSolomonVolumeDescription::TPartition &partition : desc.GetPartitions()) {
            TabletIdToPartitionId[partition.GetTabletId()] = partition.GetPartitionId();
        }

        if (TabletIdToPartitionId.empty()) {
            Ydb::KeyValue::ListLocalPartitionsResult result;
            result.set_path(this->GetProtoRequest()->path());
            result.set_node_id(SelfId().NodeId());
            this->ReplyWithResult(Ydb::StatusIds::SUCCESS, result, TActivationContext::AsActorContext());
            return;
        }

        SendRequest();
    }

    TActorId MakeLocalRegistrarID() {
        auto &ctx = TActivationContext::AsActorContext();
        auto &domainsInfo = AppData(ctx)->DomainsInfo;
        if (domainsInfo->GetDomain()->DomainUid != 1) { // TODO: WAT?
            return {};
        }
        auto &rec = *this->GetProtoRequest();
        ui32 nodeId = rec.node_id() ? rec.node_id() : ctx.SelfID.NodeId();
        ui64 hiveId = domainsInfo->GetHive();
        return ::NKikimr::MakeLocalRegistrarID(nodeId, hiveId);
    }

    TEvLocal::TEvEnumerateTablets* MakeRequest() {
        return new TEvLocal::TEvEnumerateTablets(TTabletTypes::KeyValue);
    }

    void SendRequest() {
        Send(MakeLocalRegistrarID(), MakeRequest(), IEventHandle::FlagTrackDelivery, 0);
    }

    void Handle(TEvLocal::TEvEnumerateTabletsResult::TPtr &ev) {
        const NKikimrLocal::TEvEnumerateTabletsResult &record = ev->Get()->Record;
        if (!record.HasStatus() || record.GetStatus() != NKikimrProto::OK) {
            this->Reply(StatusIds::INTERNAL_ERROR, "Received an incorrect answer from Local.", NKikimrIssues::TIssuesIds::UNEXPECTED, this->ActorContext());
            return;
        }

        Ydb::KeyValue::ListLocalPartitionsResult result;
        result.set_path(this->GetProtoRequest()->path());
        result.set_node_id(SelfId().NodeId());
        for (auto &item : record.GetTabletInfo()) {
            if (!item.HasTabletId()) {
                continue;
            }
            auto it = TabletIdToPartitionId.find(item.GetTabletId());
            if (it != TabletIdToPartitionId.end()) {
                result.add_partition_ids(it->second);
            }
        }
        this->ReplyWithResult(Ydb::StatusIds::SUCCESS, result, TActivationContext::AsActorContext());
    }

    bool ValidateRequest(Ydb::StatusIds::StatusCode& /*status*/, NYql::TIssues& /*issues*/) {
        return true;
    }

private:
    THashMap<ui64, ui64> TabletIdToPartitionId;
};


template <typename TDerived, typename TRequest, typename TResultRecord, typename TKVRequest>
class TKeyValueRequestGrpc
    : public TRpcOperationRequestActor<TDerived, TRequest>
    , public TBaseKeyValueRequest<TKeyValueRequestGrpc<TDerived, TRequest, TResultRecord, TKVRequest>>
{
public:
    using TBase = TRpcOperationRequestActor<TDerived, TRequest>;
    using TBase::TBase;

    template<typename T, typename = void>
    struct THasMsg: std::false_type
    {};
    template<typename T>
    struct THasMsg<T, std::enable_if_t<std::is_same<decltype(std::declval<T>().msg()), void>::value>>: std::true_type
    {};
    template<typename T>
    static constexpr bool HasMsgV = THasMsg<T>::value;

    friend class TBaseKeyValueRequest<TKeyValueRequestGrpc<TDerived, TRequest, TResultRecord, TKVRequest>>;

    void Bootstrap(const TActorContext& ctx) {
        TBase::Bootstrap(ctx);
        this->OnBootstrap();
        this->Become(&TKeyValueRequestGrpc::StateFunc);
    }


protected:
    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            hFunc(TKVRequest::TResponse, Handle);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
        default:
            return TBase::StateFuncBase(ev);
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr &ev) {
        TEvTxProxySchemeCache::TEvNavigateKeySetResult* res = ev->Get();
        NSchemeCache::TSchemeCacheNavigate *request = res->Request.Get();

        if (!this->OnNavigateKeySetResult(ev, static_cast<TDerived*>(this)->GetRequiredAccessRights())) {
            return;
        }

        auto &rec = *this->GetProtoRequest();
        const NKikimrSchemeOp::TSolomonVolumeDescription &desc = request->ResultSet[0].SolomonVolumeInfo->Description;

        if (rec.partition_id() >= desc.PartitionsSize()) {
            this->Reply(StatusIds::SCHEME_ERROR, "The partition wasn't found. Partition ID was larger or equal partition count.", NKikimrIssues::TIssuesIds::DEFAULT_ERROR, this->ActorContext());
            return;
        }

        ui64 partitionId = rec.partition_id();
        if (const auto &partition = desc.GetPartitions(rec.partition_id()); partition.GetPartitionId() == partitionId) {
            KVTabletId = partition.GetTabletId();
        } else {
            Y_DEBUG_ABORT_UNLESS(false);
            for (const NKikimrSchemeOp::TSolomonVolumeDescription::TPartition &partition : desc.GetPartitions()) {
                if (partition.GetPartitionId() == partitionId)  {
                    KVTabletId = partition.GetTabletId();
                    break;
                }
            }
        }

        if (!KVTabletId) {
            this->Reply(StatusIds::INTERNAL_ERROR, "Partition wasn't found.", NKikimrIssues::TIssuesIds::DEFAULT_ERROR, this->ActorContext());
            return;
        }

        CreatePipe();
        SendRequest();
    }

    void SendRequest() {
        std::unique_ptr<TKVRequest> req = std::make_unique<TKVRequest>();
        auto &rec = *this->GetProtoRequest();
        CopyProtobuf(rec, &req->Record);
        req->Record.set_tablet_id(KVTabletId);
        NTabletPipe::SendData(this->SelfId(), KVPipeClient, req.release(), 0, TBase::Span_.GetTraceId());
    }

    void Handle(typename TKVRequest::TResponse::TPtr &ev) {
        auto status = PullStatus(ev->Get()->Record);
        if constexpr (HasMsgV<decltype(ev->Get()->Record)>) {
            if (status != Ydb::StatusIds::SUCCESS) {
                this->Reply(status, ev->Get()->Record.msg(), NKikimrIssues::TIssuesIds::DEFAULT_ERROR, this->ActorContext());
            }
        }
        TResultRecord result;
        CopyProtobuf(ev->Get()->Record, &result);
        this->ReplyWithResult(status, result, TActivationContext::AsActorContext());
    }

    NTabletPipe::TClientConfig GetPipeConfig() {
        NTabletPipe::TClientConfig cfg;
        cfg.RetryPolicy = {
            .RetryLimitCount = 3u
        };
        return cfg;
    }

    void CreatePipe() {
        KVPipeClient = this->Register(NTabletPipe::CreateClient(this->SelfId(), KVTabletId, GetPipeConfig()));
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            this->Reply(StatusIds::UNAVAILABLE, "Failed to connect to coordination node.", NKikimrIssues::TIssuesIds::SHARD_NOT_AVAILABLE, this->ActorContext());
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr&) {
        this->Reply(StatusIds::UNAVAILABLE, "Connection to coordination node was lost.", NKikimrIssues::TIssuesIds::SHARD_NOT_AVAILABLE, this->ActorContext());
    }

    virtual bool ValidateRequest(Ydb::StatusIds::StatusCode& status, NYql::TIssues& issues) = 0;

    void PassAway() override {
        if (KVPipeClient) {
            NTabletPipe::CloseClient(this->SelfId(), KVPipeClient);
            KVPipeClient = {};
        }
        TBase::PassAway();
    }

protected:
    ui64 KVTabletId = 0;
    TActorId KVPipeClient;
};

class TAcquireLockRequest
    : public TKeyValueRequestGrpc<TAcquireLockRequest, TEvAcquireLockKeyValueRequest,
            Ydb::KeyValue::AcquireLockResult, TEvKeyValue::TEvAcquireLock>
{
public:
    using TBase = TKeyValueRequestGrpc<TAcquireLockRequest, TEvAcquireLockKeyValueRequest,
            Ydb::KeyValue::AcquireLockResult, TEvKeyValue::TEvAcquireLock>;
    using TBase::TBase;

    bool ValidateRequest(Ydb::StatusIds::StatusCode& /*status*/, NYql::TIssues& /*issues*/) override {
        return true;
    }
    NACLib::EAccessRights GetRequiredAccessRights() const {
        return NACLib::UpdateRow;
    }
};


class TExecuteTransactionRequest
    : public TKeyValueRequestGrpc<TExecuteTransactionRequest, TEvExecuteTransactionKeyValueRequest,
            Ydb::KeyValue::ExecuteTransactionResult, TEvKeyValue::TEvExecuteTransaction> {
public:
    using TBase = TKeyValueRequestGrpc<TExecuteTransactionRequest, TEvExecuteTransactionKeyValueRequest,
            Ydb::KeyValue::ExecuteTransactionResult, TEvKeyValue::TEvExecuteTransaction>;
    using TBase::TBase;

    bool ValidateRequest(Ydb::StatusIds::StatusCode& /*status*/, NYql::TIssues& /*issues*/) override {
        return true;
    }

    NACLib::EAccessRights GetRequiredAccessRights() const {
        ui32 accessRights = 0;
        auto &rec = *this->GetProtoRequest();
        for (auto &command : rec.commands()) {
            if (command.has_delete_range()) {
                accessRights |= NACLib::EraseRow;
            }
            if (command.has_rename()) {
                accessRights |= NACLib::UpdateRow | NACLib::EraseRow;
            }
            if (command.has_copy_range()) {
                accessRights |= NACLib::UpdateRow;
            }
            if (command.has_concat() && !command.concat().keep_inputs()) {
                accessRights |= NACLib::UpdateRow | NACLib::EraseRow;
            }
            if (command.has_concat() && command.concat().keep_inputs()) {
                accessRights |= NACLib::UpdateRow;
            }
            if (command.has_write()) {
                accessRights |= NACLib::UpdateRow;
            }
        }
        return static_cast<NACLib::EAccessRights>(accessRights);
    }
};

class TReadRequest
    : public TKeyValueRequestGrpc<TReadRequest, TEvReadKeyValueRequest,
            Ydb::KeyValue::ReadResult, TEvKeyValue::TEvRead> {
public:
    using TBase = TKeyValueRequestGrpc<TReadRequest, TEvReadKeyValueRequest,
            Ydb::KeyValue::ReadResult, TEvKeyValue::TEvRead>;
    using TBase::TBase;
    using TBase::Handle;
    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
        default:
            return TBase::StateFunc(ev);
        }
    }
    bool ValidateRequest(Ydb::StatusIds::StatusCode& /*status*/, NYql::TIssues& /*issues*/) override {
        return true;
    }
    NACLib::EAccessRights GetRequiredAccessRights() const {
        return NACLib::SelectRow;
    }
};

class TReadRangeRequest
    : public TKeyValueRequestGrpc<TReadRangeRequest, TEvReadRangeKeyValueRequest,
            Ydb::KeyValue::ReadRangeResult, TEvKeyValue::TEvReadRange> {
public:
    using TBase = TKeyValueRequestGrpc<TReadRangeRequest, TEvReadRangeKeyValueRequest,
            Ydb::KeyValue::ReadRangeResult, TEvKeyValue::TEvReadRange>;
    using TBase::TBase;
    using TBase::Handle;
    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
        default:
            return TBase::StateFunc(ev);
        }
    }
    bool ValidateRequest(Ydb::StatusIds::StatusCode& /*status*/, NYql::TIssues& /*issues*/) override {
        return true;
    }
    NACLib::EAccessRights GetRequiredAccessRights() const {
        return NACLib::SelectRow;
    }
};

class TListRangeRequest
    : public TKeyValueRequestGrpc<TListRangeRequest, TEvListRangeKeyValueRequest,
            Ydb::KeyValue::ListRangeResult, TEvKeyValue::TEvReadRange> {
public:
    using TBase = TKeyValueRequestGrpc<TListRangeRequest, TEvListRangeKeyValueRequest,
            Ydb::KeyValue::ListRangeResult, TEvKeyValue::TEvReadRange>;
    using TBase::TBase;
    using TBase::Handle;
    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
        default:
            return TBase::StateFunc(ev);
        }
    }
    bool ValidateRequest(Ydb::StatusIds::StatusCode& /*status*/, NYql::TIssues& /*issues*/) override {
        return true;
    }
    NACLib::EAccessRights GetRequiredAccessRights() const {
        return NACLib::SelectRow;
    }
};

class TGetStorageChannelStatusRequest
    : public TKeyValueRequestGrpc<TGetStorageChannelStatusRequest, TEvGetStorageChannelStatusKeyValueRequest,
            Ydb::KeyValue::GetStorageChannelStatusResult, TEvKeyValue::TEvGetStorageChannelStatus> {
public:
    using TBase = TKeyValueRequestGrpc<TGetStorageChannelStatusRequest, TEvGetStorageChannelStatusKeyValueRequest,
            Ydb::KeyValue::GetStorageChannelStatusResult, TEvKeyValue::TEvGetStorageChannelStatus>;
    using TBase::TBase;
    using TBase::Handle;
    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
        default:
            return TBase::StateFunc(ev);
        }
    }
    bool ValidateRequest(Ydb::StatusIds::StatusCode& /*status*/, NYql::TIssues& /*issues*/) override {
        return true;
    }
    NACLib::EAccessRights GetRequiredAccessRights() const {
        return NACLib::DescribeSchema;
    }
};

}


void DoCreateVolumeKeyValue(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TCreateVolumeRequest(p.release()));
}

void DoDropVolumeKeyValue(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TDropVolumeRequest(p.release()));
}

void DoAlterVolumeKeyValue(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TAlterVolumeRequest(p.release()));
}

void DoDescribeVolumeKeyValue(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TDescribeVolumeRequest(p.release()));
}

void DoListLocalPartitionsKeyValue(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TListLocalPartitionsRequest(p.release()));
}

void DoAcquireLockKeyValue(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TAcquireLockRequest(p.release()));
}

void DoExecuteTransactionKeyValue(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TExecuteTransactionRequest(p.release()));
}

void DoReadKeyValue(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TReadRequest(p.release()));
}

void DoReadRangeKeyValue(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TReadRangeRequest(p.release()));
}

void DoListRangeKeyValue(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TListRangeRequest(p.release()));
}

void DoGetStorageChannelStatusKeyValue(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TGetStorageChannelStatusRequest(p.release()));
}

} // namespace NKikimr::NGRpcService
