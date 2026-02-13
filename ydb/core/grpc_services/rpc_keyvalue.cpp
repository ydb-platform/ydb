#include "service_keyvalue.h"

#include <ydb/public/api/protos/ydb_keyvalue.pb.h>

#include <ydb/core/base/path.h>
#include <ydb/core/grpc_services/rpc_scheme_base.h>
#include <ydb/core/grpc_services/rpc_common/rpc_common.h>
#include <ydb/core/grpc_services/rpc_request_base.h>
#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
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
using TEvAcquireLockKeyValueV2Request =
    TGrpcRequestNoOperationCall<Ydb::KeyValue::AcquireLockRequest,
        Ydb::KeyValue::AcquireLockResult>;
using TEvExecuteTransactionKeyValueRequest =
    TGrpcRequestOperationCall<Ydb::KeyValue::ExecuteTransactionRequest,
        Ydb::KeyValue::ExecuteTransactionResponse>;
using TEvExecuteTransactionKeyValueV2Request =
    TGrpcRequestNoOperationCall<Ydb::KeyValue::ExecuteTransactionRequest,
        Ydb::KeyValue::ExecuteTransactionResult>;
using TEvReadKeyValueRequest =
    TGrpcRequestOperationCall<Ydb::KeyValue::ReadRequest,
        Ydb::KeyValue::ReadResponse>;
using TEvReadKeyValueV2Request =
    TGrpcRequestNoOperationCall<Ydb::KeyValue::ReadRequest,
        Ydb::KeyValue::ReadResult>;
using TEvReadRangeKeyValueRequest =
    TGrpcRequestOperationCall<Ydb::KeyValue::ReadRangeRequest,
        Ydb::KeyValue::ReadRangeResponse>;
using TEvReadRangeKeyValueV2Request =
    TGrpcRequestNoOperationCall<Ydb::KeyValue::ReadRangeRequest,
        Ydb::KeyValue::ReadRangeResult>;
using TEvListRangeKeyValueRequest =
    TGrpcRequestOperationCall<Ydb::KeyValue::ListRangeRequest,
        Ydb::KeyValue::ListRangeResponse>;
using TEvListRangeKeyValueV2Request =
    TGrpcRequestNoOperationCall<Ydb::KeyValue::ListRangeRequest,
        Ydb::KeyValue::ListRangeResult>;
using TEvGetStorageChannelStatusKeyValueRequest =
    TGrpcRequestOperationCall<Ydb::KeyValue::GetStorageChannelStatusRequest,
        Ydb::KeyValue::GetStorageChannelStatusResponse>;
using TEvGetStorageChannelStatusKeyValueV2Request =
    TGrpcRequestNoOperationCall<Ydb::KeyValue::GetStorageChannelStatusRequest,
        Ydb::KeyValue::GetStorageChannelStatusResult>;

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

constexpr bool UsePayloadForExecuteTransactionWrite = false;

void AppendVarint(TString& out, ui64 value) {
    while (value >= 0x80) {
        out.push_back(static_cast<char>((value & 0x7F) | 0x80));
        value >>= 7;
    }
    out.push_back(static_cast<char>(value));
}

TString MakeLengthDelimitedFieldHeader(ui32 fieldNumber, ui64 size) {
    TString out;
    out.reserve(16);
    const ui64 fieldTag = (static_cast<ui64>(fieldNumber) << 3) | 2ull;
    AppendVarint(out, fieldTag);
    AppendVarint(out, size);
    return out;
}

bool TryReplyReadResultWithCustomSerialization(
        const TEvKeyValue::TEvReadResponse& from,
        Ydb::StatusIds::StatusCode status,
        bool useCustomSerialization,
        IRequestNoOpCtx* request)
{
    if (status != Ydb::StatusIds::SUCCESS || !from.IsPayload() || !request) {
        return false;
    }
    if (!useCustomSerialization) {
        return false;
    }

    Ydb::KeyValue::ReadResult result;
    result.set_requested_key(from.Record.requested_key());
    result.set_requested_offset(from.Record.requested_offset());
    result.set_requested_size(from.Record.requested_size());
    result.set_node_id(from.Record.node_id());
    if (from.Record.status() == NKikimrKeyValue::Statuses::RSTATUS_OVERRUN) {
        result.set_is_overrun(true);
    }
    result.set_status(status);

    TString serializedResult;
    Y_PROTOBUF_SUPPRESS_NODISCARD result.SerializeToString(&serializedResult);

    TRope payload = from.GetBuffer();
    if (payload.GetSize()) {
        TString valueFieldHeader = MakeLengthDelimitedFieldHeader(4, payload.GetSize()); // bytes value = 4;

        TRope response(std::move(serializedResult));
        response.Insert(response.End(), TRope(std::move(valueFieldHeader)));
        response.Insert(response.End(), std::move(payload));
        request->SendSerializedResult(std::move(response), status);
    } else {
        request->SendSerializedResult(TRope(std::move(serializedResult)), status);
    }

    return true;
}

bool TryReplyReadRangeResultWithCustomSerialization(
        const TEvKeyValue::TEvReadRangeResponse& from,
        Ydb::StatusIds::StatusCode status,
        bool useCustomSerialization,
        IRequestNoOpCtx* request)
{
    if (status != Ydb::StatusIds::SUCCESS || !request) {
        return false;
    }
    if (!useCustomSerialization) {
        return false;
    }

    bool hasPayload = true;
    for (int idx = 0; idx < from.Record.pair_size(); ++idx) {
        if (!from.IsPayload(idx)) {
            hasPayload = false;
            break;
        }
    }
    if (!hasPayload) {
        return false;
    }

    Ydb::KeyValue::ReadRangeResult result;
    if (from.Record.status() == NKikimrKeyValue::Statuses::RSTATUS_OVERRUN) {
        result.set_is_overrun(true);
    }
    result.set_node_id(from.Record.node_id());
    result.set_status(status);

    TString serializedResult;
    Y_PROTOBUF_SUPPRESS_NODISCARD result.SerializeToString(&serializedResult);
    TRope response(std::move(serializedResult));

    for (int idx = 0; idx < from.Record.pair_size(); ++idx) {
        Ydb::KeyValue::ReadRangeResult::KeyValuePair pair;
        const auto& fromPair = from.Record.pair(idx);
        pair.set_key(fromPair.key());
        pair.set_creation_unix_time(fromPair.creation_unix_time());
        pair.set_storage_channel(fromPair.storage_channel());

        TString serializedPair;
        Y_PROTOBUF_SUPPRESS_NODISCARD pair.SerializeToString(&serializedPair);

        TRope payload = from.GetBuffer(idx);
        ui64 pairSize = serializedPair.size();
        TString valueFieldHeader;
        if (payload.GetSize()) {
            valueFieldHeader = MakeLengthDelimitedFieldHeader(2, payload.GetSize()); // bytes value = 2;
            pairSize += valueFieldHeader.size() + payload.GetSize();
        }

        TString pairFieldHeader = MakeLengthDelimitedFieldHeader(1, pairSize); // repeated KeyValuePair pair = 1;
        response.Insert(response.End(), TRope(std::move(pairFieldHeader)));
        response.Insert(response.End(), TRope(std::move(serializedPair)));
        if (payload.GetSize()) {
            response.Insert(response.End(), TRope(std::move(valueFieldHeader)));
            response.Insert(response.End(), std::move(payload));
        }
    }

    request->SendSerializedResult(std::move(response), status);
    return true;
}

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

void CopyProtobuf(const Ydb::KeyValue::ExecuteTransactionRequest::Command::Write &from,
        NKikimrKeyValue::ExecuteTransactionRequest::Command::Write *to,
        TEvKeyValue::TEvExecuteTransaction *event, bool usePayload)
{
    COPY_PRIMITIVE_FIELD(key);
    if (usePayload) {
        ui32 payloadId = event->AddPayload(TRope(from.value()));
        to->set_payload_id(payloadId);
    } else {
        COPY_PRIMITIVE_FIELD(value);
    }
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

void CopyProtobuf(const Ydb::KeyValue::ExecuteTransactionRequest::Command &from,
        NKikimrKeyValue::ExecuteTransactionRequest::Command *to,
        TEvKeyValue::TEvExecuteTransaction *event, bool usePayload)
{
    if (from.has_rename()) {
        CopyProtobuf(from.rename(), to->mutable_rename());
    }
    if (from.has_concat()) {
        CopyProtobuf(from.concat(), to->mutable_concat());
    }
    if (from.has_copy_range()) {
        CopyProtobuf(from.copy_range(), to->mutable_copy_range());
    }
    if (from.has_write()) {
        CopyProtobuf(from.write(), to->mutable_write(), event, usePayload);
    }
    if (from.has_delete_range()) {
        CopyProtobuf(from.delete_range(), to->mutable_delete_range());
    }
}

void CopyProtobuf(const Ydb::KeyValue::ExecuteTransactionRequest &from,
        NKikimrKeyValue::ExecuteTransactionRequest *to)
{
    COPY_PRIMITIVE_OPTIONAL_FIELD(lock_generation);
    for (auto &cmd : from.commands()) {
        CopyProtobuf(cmd, to->add_commands());
    }
}

void CopyProtobuf(const Ydb::KeyValue::ExecuteTransactionRequest &from,
        TEvKeyValue::TEvExecuteTransaction *to, bool usePayload)
{
    if (!usePayload) {
        CopyProtobuf(from, &to->Record);
        return;
    }

    if (from.has_lock_generation()) {
        to->Record.set_lock_generation(from.lock_generation());
    }
    for (const auto &cmd : from.commands()) {
        CopyProtobuf(cmd, to->Record.add_commands(), to, true);
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

void CopyProtobuf(const NKikimrKeyValue::ReadRangeResult &from,
        Ydb::KeyValue::ReadRangeResult *to);

void CopyReadResultFromEvent(const TEvKeyValue::TEvReadResponse& from, Ydb::KeyValue::ReadResult* to) {
    CopyProtobuf(from.Record, to);
    if (from.IsPayload()) {
        TRope value = from.GetBuffer();
        const TContiguousSpan span = value.GetContiguousSpan();
        to->set_value(span.data(), span.size());
    }
}

void CopyReadRangeResultFromEvent(const TEvKeyValue::TEvReadRangeResponse& from, Ydb::KeyValue::ReadRangeResult *to) {
    CopyProtobuf(from.Record, to);
    for (int idx = 0; idx < to->pair_size(); ++idx) {
        if (from.IsPayload(idx)) {
            TRope value = from.GetBuffer(idx);
            const TContiguousSpan span = value.GetContiguousSpan();
            to->mutable_pair(idx)->set_value(span.data(), span.size());
        }
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


template <typename TDerived>
class TBaseKeyValueRequest {
protected:
    void OnBootstrap() {
        auto self = static_cast<TDerived*>(this);
        Ydb::StatusIds::StatusCode status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
        if (!self->ValidateRequest(status)) {
            self->Reply(status);
            return;
        }
        UserToken = self->GetToken();
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
        req->DatabaseName = self->GetDatabaseName();
        auto ev = new TEvTxProxySchemeCache::TEvNavigateKeySet(req.Release());
        self->Send(MakeSchemeCacheID(), ev, 0, 0, self->GetTraceId());
    }

    bool OnNavigateKeySetResult(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr &ev, ui32 access) {
        auto self = static_cast<TDerived*>(this);
        TEvTxProxySchemeCache::TEvNavigateKeySetResult* res = ev->Get();
        NSchemeCache::TSchemeCacheNavigate *request = res->Request.Get();

        if (res->Request->ResultSet.size() != 1) {
            self->Reply(StatusIds::INTERNAL_ERROR, "Received an incorrect answer from SchemeCache.", NKikimrIssues::TIssuesIds::UNEXPECTED);
            return false;
        }

        switch (request->ResultSet[0].Status) {
        case NSchemeCache::TSchemeCacheNavigate::EStatus::Ok:
            break;
        case NSchemeCache::TSchemeCacheNavigate::EStatus::AccessDenied:
            self->Reply(StatusIds::UNAUTHORIZED, "Access denied.", NKikimrIssues::TIssuesIds::ACCESS_DENIED);
            return false;
        case NSchemeCache::TSchemeCacheNavigate::EStatus::RootUnknown:
        case NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown:
            self->Reply(StatusIds::SCHEME_ERROR, "Path isn't exist.", NKikimrIssues::TIssuesIds::PATH_NOT_EXIST);
            return false;
        case NSchemeCache::TSchemeCacheNavigate::EStatus::LookupError:
        case NSchemeCache::TSchemeCacheNavigate::EStatus::RedirectLookupError:
            self->Reply(StatusIds::UNAVAILABLE, "Database resolve failed with no certain result.", NKikimrIssues::TIssuesIds::RESOLVE_LOOKUP_ERROR);
            return false;
        default:
            self->Reply(StatusIds::UNAVAILABLE, "Resolve error", NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR);
            return false;
        }

        if (!self->InternalCheckAccess(CanonizePath(res->Request->ResultSet[0].Path), res->Request->ResultSet[0].SecurityObject, access)) {
            return false;
        }
        if (!request->ResultSet[0].SolomonVolumeInfo) {
            self->Reply(StatusIds::SCHEME_ERROR, "Table isn't keyvalue.", NKikimrIssues::TIssuesIds::DEFAULT_ERROR);
            return false;
        }

        return true;
    }

    bool InternalCheckAccess(const TString& path, TIntrusivePtr<TSecurityObject> securityObject, ui32 access) {
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
            NKikimrIssues::TIssuesIds::ACCESS_DENIED);
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
        auto *storageConfig = result.mutable_storage_config();
        for (auto &channel : desc.GetBoundChannels()) {
            auto *channelBind = storageConfig->add_channel();
            channelBind->set_media(channel.GetStoragePoolKind());
        }
        this->ReplyWithResult(Ydb::StatusIds::SUCCESS, result, TActivationContext::AsActorContext());
    }

    bool ValidateRequest(Ydb::StatusIds::StatusCode& /*status*/) {
        return true;
    }

    TIntrusiveConstPtr<NACLib::TUserToken> GetToken() {
        if (const auto& userToken = this->Request_->GetSerializedToken()) {
            return new NACLib::TUserToken(userToken);
        }
        return nullptr;
    }

    TString GetDatabaseName() {
        return this->Request_->GetDatabaseName().GetOrElse("");
    }

    NWilson::TTraceId GetTraceId() {
        return this->Span_.GetTraceId();
    }
};


class TAlterVolumeRequest
    : public TRpcSchemeRequestActor<TAlterVolumeRequest, TEvAlterVolumeKeyValueRequest>
    , public TBaseKeyValueRequest<TAlterVolumeRequest>
{
public:
    using TBase = TRpcSchemeRequestActor<TAlterVolumeRequest, TEvAlterVolumeKeyValueRequest>;
    using TBase::TBase;
    using TBaseKeyValueRequest::TBaseKeyValueRequest;
    friend class TBaseKeyValueRequest<TAlterVolumeRequest>;

    void Bootstrap(const TActorContext& ctx) {
        TBase::Bootstrap(ctx);
        Become(&TAlterVolumeRequest::StateWork);
        if (GetProtoRequest()->has_storage_config()) {
            StorageConfig = GetProtoRequest()->storage_config();
            SendProposeRequest(ctx);
        } else {
            OnBootstrap();
        }
    }

    bool ValidateRequest(Ydb::StatusIds::StatusCode& /*status*/) {
        return true;
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
        if (req->alter_partition_count()) {
            tableDesc->SetPartitionCount(req->alter_partition_count());
        }

        if (GetProtoRequest()->has_storage_config()) {
            tableDesc->SetUpdateChannelsBinding(true);
        }
        auto *internalStorageConfig = tableDesc->MutableStorageConfig();
        AssignPoolKinds(StorageConfig, internalStorageConfig);

        ctx.Send(MakeTxProxyID(), proposeRequest.release());
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
        default:
            return TBase::StateWork(ev);
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr &ev) {
        TEvTxProxySchemeCache::TEvNavigateKeySetResult* res = ev->Get();
        NSchemeCache::TSchemeCacheNavigate *request = res->Request.Get();

        if (!OnNavigateKeySetResult(ev, NACLib::DescribeSchema)) {
            return;
        }

        const NKikimrSchemeOp::TSolomonVolumeDescription &desc = request->ResultSet[0].SolomonVolumeInfo->Description;
        for (auto &channel : desc.GetBoundChannels()) {
            auto *channelBind = StorageConfig.add_channel();
            channelBind->set_media(channel.GetStoragePoolKind());
        }
        SendProposeRequest(TActivationContext::AsActorContext());
    }

    TIntrusiveConstPtr<NACLib::TUserToken> GetToken() {
        if (const auto& userToken = this->Request_->GetSerializedToken()) {
            return new NACLib::TUserToken(userToken);
        }
        return nullptr;
    }

    TString GetDatabaseName() {
        return this->Request_->GetDatabaseName().GetOrElse("");
    }

    NWilson::TTraceId GetTraceId() {
        return this->Span_.GetTraceId();
    }

private:
    Ydb::KeyValue::StorageConfig StorageConfig;
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

    bool ValidateRequest(Ydb::StatusIds::StatusCode& /*status*/) {
        return true;
    }

    TIntrusiveConstPtr<NACLib::TUserToken> GetToken() {
        if (const auto& userToken = this->Request_->GetSerializedToken()) {
            return new NACLib::TUserToken(userToken);
        }
        return nullptr;
    }

    TString GetDatabaseName() {
        return this->Request_->GetDatabaseName().GetOrElse("");
    }

    NWilson::TTraceId GetTraceId() {
        return this->Span_.GetTraceId();
    }

private:
    THashMap<ui64, ui64> TabletIdToPartitionId;
};


template <typename TDerived, typename TRequest, typename TResultRecord, typename TKVRequest, bool IsOperational>
class TKeyValueRequestGrpc
    : public std::conditional_t<IsOperational,
        TRpcOperationRequestActor<TDerived, TRequest>,
        TRpcRequestActor<TDerived, TRequest>
    >
    , public TBaseKeyValueRequest<TDerived>
{
public:
    using TBase = std::conditional_t<IsOperational,
        TRpcOperationRequestActor<TDerived, TRequest>,
        TRpcRequestActor<TDerived, TRequest>
    >;
    using TBase::TBase;
    using TBase::Reply;

    TKeyValueRequestGrpc(std::conditional_t<IsOperational, IRequestOpCtx, IRequestNoOpCtx>* request,
            const TKeyValueRequestSettings& settings)
        : TBase(request)
        , RequestSettings(settings)
    {
    }

    template<typename T, typename = void>
    struct THasMsg: std::false_type
    {};
    template<typename T>
    struct THasMsg<T, std::enable_if_t<std::is_same<decltype(std::declval<T>().msg()), void>::value>>: std::true_type
    {};
    template<typename T>
    static constexpr bool HasMsgV = THasMsg<T>::value;

    friend class TBaseKeyValueRequest<TDerived>;

    void Bootstrap(const TActorContext& ctx) {
        if constexpr (IsOperational) {
            TBase::Bootstrap(ctx);
        }
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
            if constexpr (IsOperational) {
                return TBase::StateFuncBase(ev);
            } else {
                this->Reply(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Unexpected event received in TKeyValueRequestGrpc::StateWork: " << ev->GetTypeRewrite());
            }
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
            this->Reply(StatusIds::SCHEME_ERROR, "The partition wasn't found. Partition ID was larger or equal partition count.", NKikimrIssues::TIssuesIds::DEFAULT_ERROR);
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
            this->Reply(StatusIds::INTERNAL_ERROR, "Partition wasn't found.", NKikimrIssues::TIssuesIds::DEFAULT_ERROR);
            return;
        }

        CreatePipe();
        SendRequest();
    }

    void SendRequest() {
        std::unique_ptr<TKVRequest> req = std::make_unique<TKVRequest>();
        auto &rec = *this->GetProtoRequest();
        if constexpr (std::is_same_v<TKVRequest, TEvKeyValue::TEvExecuteTransaction>) {
            CopyProtobuf(rec, req.get(), UsePayloadForExecuteTransactionWrite);
        } else {
            CopyProtobuf(rec, &req->Record);
        }
        req->Record.set_tablet_id(KVTabletId);
        NTabletPipe::SendData(this->SelfId(), KVPipeClient, req.release(), 0, GetTraceId());
    }

    void Handle(typename TKVRequest::TResponse::TPtr &ev) {
        auto status = PullStatus(ev->Get()->Record);
        if constexpr (HasMsgV<decltype(ev->Get()->Record)>) {
            if (status != Ydb::StatusIds::SUCCESS) {
                this->Reply(status, ev->Get()->Record.msg(), NKikimrIssues::TIssuesIds::DEFAULT_ERROR);
            }
        }
        if constexpr (IsOperational) {
            TResultRecord result;
            if constexpr (std::is_same_v<TKVRequest, TEvKeyValue::TEvRead>
                    && std::is_same_v<TResultRecord, Ydb::KeyValue::ReadResult>) {
                CopyReadResultFromEvent(*ev->Get(), &result);
            } else if constexpr (std::is_same_v<TKVRequest, TEvKeyValue::TEvReadRange>
                    && std::is_same_v<TResultRecord, Ydb::KeyValue::ReadRangeResult>) {
                CopyReadRangeResultFromEvent(*ev->Get(), &result);
            } else {
                CopyProtobuf(ev->Get()->Record, &result);
            }
            this->ReplyWithResult(status, result, TActivationContext::AsActorContext());
        } else {      
            if constexpr (!IsOperational
                    && std::is_same_v<TKVRequest, TEvKeyValue::TEvRead>
                    && std::is_same_v<TResultRecord, Ydb::KeyValue::ReadResult>) {
                if (TryReplyReadResultWithCustomSerialization(*ev->Get(), status,
                        RequestSettings.UseCustomSerialization, this->Request.Get())) {
                    PassAway();
                    return;
                }
            }
            if constexpr (!IsOperational
                    && std::is_same_v<TKVRequest, TEvKeyValue::TEvReadRange>
                    && std::is_same_v<TResultRecord, Ydb::KeyValue::ReadRangeResult>) {
                if (TryReplyReadRangeResultWithCustomSerialization(*ev->Get(), status,
                        RequestSettings.UseCustomSerialization, this->Request.Get())) {
                    PassAway();
                    return;
                }
            }

            TResultRecord result;//google::protobuf::Arena::CreateMessage<TResultRecord>(this->Request->GetArena());
            if constexpr (std::is_same_v<TKVRequest, TEvKeyValue::TEvRead>
                    && std::is_same_v<TResultRecord, Ydb::KeyValue::ReadResult>) {
                CopyReadResultFromEvent(*ev->Get(), &result);
            } else if constexpr (std::is_same_v<TKVRequest, TEvKeyValue::TEvReadRange>
                    && std::is_same_v<TResultRecord, Ydb::KeyValue::ReadRangeResult>) {
                CopyReadRangeResultFromEvent(*ev->Get(), &result);
            } else {
                CopyProtobuf(ev->Get()->Record, &result);
            }
            result.set_status(status);
            this->Request->Reply(&result, status);
            PassAway();
        }
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
            this->Reply(StatusIds::UNAVAILABLE, "Failed to connect to partition.", NKikimrIssues::TIssuesIds::SHARD_NOT_AVAILABLE);
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr&) {
        this->Reply(StatusIds::UNAVAILABLE, "Connection to partition was lost.", NKikimrIssues::TIssuesIds::SHARD_NOT_AVAILABLE);
    }

    void PassAway() override {
        if (KVPipeClient) {
            NTabletPipe::CloseClient(this->SelfId(), KVPipeClient);
            KVPipeClient = {};
        }
        TBase::PassAway();
    }

    TIntrusiveConstPtr<NACLib::TUserToken> GetToken() {
        if constexpr (IsOperational) {
            if (const auto& userToken = this->Request_->GetSerializedToken()) {
                return new NACLib::TUserToken(userToken);
            }
        } else {
            if (this->TBase::UserToken) {
                return new NACLib::TUserToken(*this->TBase::UserToken);
            }
        }
        return nullptr;
    }

    TString GetDatabaseName() {
        if constexpr (IsOperational) {
            return this->Request_->GetDatabaseName().GetOrElse("");
        } else {
            return this->TBase::GetDatabaseName();
        }
    }

    NWilson::TTraceId GetTraceId() {
        if constexpr (IsOperational) {
            return this->Span_.GetTraceId();
        } else {
            return NWilson::TTraceId();
        }
    }

protected:
    ui64 KVTabletId = 0;
    TActorId KVPipeClient;
    TKeyValueRequestSettings RequestSettings;
};

template <bool IsOperational>
class TAcquireLockRequest
    : public TKeyValueRequestGrpc<TAcquireLockRequest<IsOperational>,
            std::conditional_t<IsOperational, TEvAcquireLockKeyValueRequest, TEvAcquireLockKeyValueV2Request>,
            Ydb::KeyValue::AcquireLockResult, TEvKeyValue::TEvAcquireLock, IsOperational>
{
public:
    using TBase = TKeyValueRequestGrpc<TAcquireLockRequest,
            std::conditional_t<IsOperational, TEvAcquireLockKeyValueRequest, TEvAcquireLockKeyValueV2Request>,
            Ydb::KeyValue::AcquireLockResult, TEvKeyValue::TEvAcquireLock, IsOperational>;
    using TBase::TBase;

    bool ValidateRequest(Ydb::StatusIds::StatusCode& /*status*/) {
        return true;
    }
    NACLib::EAccessRights GetRequiredAccessRights() const {
        return NACLib::UpdateRow;
    }
};

template <bool IsOperational>
class TExecuteTransactionRequest
    : public TKeyValueRequestGrpc<TExecuteTransactionRequest<IsOperational>,
            std::conditional_t<IsOperational, TEvExecuteTransactionKeyValueRequest, TEvExecuteTransactionKeyValueV2Request>,
            Ydb::KeyValue::ExecuteTransactionResult, TEvKeyValue::TEvExecuteTransaction, IsOperational> {
public:
    using TBase = TKeyValueRequestGrpc<TExecuteTransactionRequest,
            std::conditional_t<IsOperational, TEvExecuteTransactionKeyValueRequest, TEvExecuteTransactionKeyValueV2Request>,
            Ydb::KeyValue::ExecuteTransactionResult, TEvKeyValue::TEvExecuteTransaction, IsOperational>;
    using TBase::TBase;

    bool ValidateRequest(Ydb::StatusIds::StatusCode& /*status*/) {
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

template <bool IsOperational>
class TReadRequest
    : public TKeyValueRequestGrpc<TReadRequest<IsOperational>, 
            std::conditional_t<IsOperational, TEvReadKeyValueRequest, TEvReadKeyValueV2Request>,
            Ydb::KeyValue::ReadResult, TEvKeyValue::TEvRead, IsOperational> {
public:
    using TBase = TKeyValueRequestGrpc<TReadRequest,
            std::conditional_t<IsOperational, TEvReadKeyValueRequest, TEvReadKeyValueV2Request>,
            Ydb::KeyValue::ReadResult, TEvKeyValue::TEvRead, IsOperational>;
    using TBase::TBase;
    using TBase::Handle;
    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
        default:
            return TBase::StateFunc(ev);
        }
    }

    bool ValidateRequest(Ydb::StatusIds::StatusCode& /*status*/) {
        return true;
    }
    NACLib::EAccessRights GetRequiredAccessRights() const {
        return NACLib::SelectRow;
    }
};

template <bool IsOperational>
class TReadRangeRequest
    : public TKeyValueRequestGrpc<TReadRangeRequest<IsOperational>,
            std::conditional_t<IsOperational, TEvReadRangeKeyValueRequest, TEvReadRangeKeyValueV2Request>,
            Ydb::KeyValue::ReadRangeResult, TEvKeyValue::TEvReadRange, IsOperational> {
public:
    using TBase = TKeyValueRequestGrpc<TReadRangeRequest,
            std::conditional_t<IsOperational, TEvReadRangeKeyValueRequest, TEvReadRangeKeyValueV2Request>,
            Ydb::KeyValue::ReadRangeResult, TEvKeyValue::TEvReadRange, IsOperational>;
    using TBase::TBase;
    using TBase::Handle;
    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
        default:
            return TBase::StateFunc(ev);
        }
    }
    bool ValidateRequest(Ydb::StatusIds::StatusCode& /*status*/) {
        return true;
    }
    NACLib::EAccessRights GetRequiredAccessRights() const {
        return NACLib::SelectRow;
    }
};

template <bool IsOperational>
class TListRangeRequest
    : public TKeyValueRequestGrpc<TListRangeRequest<IsOperational>,
            std::conditional_t<IsOperational, TEvListRangeKeyValueRequest, TEvListRangeKeyValueV2Request>,
            Ydb::KeyValue::ListRangeResult, TEvKeyValue::TEvReadRange, IsOperational> {
public:
    using TBase = TKeyValueRequestGrpc<TListRangeRequest,
            std::conditional_t<IsOperational, TEvListRangeKeyValueRequest, TEvListRangeKeyValueV2Request>,
            Ydb::KeyValue::ListRangeResult, TEvKeyValue::TEvReadRange, IsOperational>;
    using TBase::TBase;
    using TBase::Handle;
    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
        default:
            return TBase::StateFunc(ev);
        }
    }
    bool ValidateRequest(Ydb::StatusIds::StatusCode& /*status*/) {
        return true;
    }
    NACLib::EAccessRights GetRequiredAccessRights() const {
        return NACLib::SelectRow;
    }
};

template <bool IsOperational>
class TGetStorageChannelStatusRequest
    : public TKeyValueRequestGrpc<TGetStorageChannelStatusRequest<IsOperational>,
            std::conditional_t<IsOperational, TEvGetStorageChannelStatusKeyValueRequest, TEvGetStorageChannelStatusKeyValueV2Request>,
            Ydb::KeyValue::GetStorageChannelStatusResult, TEvKeyValue::TEvGetStorageChannelStatus, IsOperational> {
public:
    using TBase = TKeyValueRequestGrpc<TGetStorageChannelStatusRequest,
            std::conditional_t<IsOperational, TEvGetStorageChannelStatusKeyValueRequest, TEvGetStorageChannelStatusKeyValueV2Request>,
            Ydb::KeyValue::GetStorageChannelStatusResult, TEvKeyValue::TEvGetStorageChannelStatus, IsOperational>;
    using TBase::TBase;
    using TBase::Handle;
    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
        default:
            return TBase::StateFunc(ev);
        }
    }
    bool ValidateRequest(Ydb::StatusIds::StatusCode& /*status*/) {
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
    TActivationContext::AsActorContext().Register(new TAcquireLockRequest<true>(p.release()));
}

void DoAcquireLockKeyValueV2(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TAcquireLockRequest<false>(p.release()));
}

void DoExecuteTransactionKeyValue(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TExecuteTransactionRequest<true>(p.release()));
}

void DoExecuteTransactionKeyValueV2(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TExecuteTransactionRequest<false>(p.release()));
}

void DoReadKeyValue(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility) {
    DoReadKeyValue(std::move(p), facility, {});
}

void DoReadKeyValueV2(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& facility) {
    DoReadKeyValueV2(std::move(p), facility, {});
}

void DoReadRangeKeyValue(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& facility) {
    DoReadRangeKeyValue(std::move(p), facility, {});
}

void DoReadRangeKeyValueV2(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& facility) {
    DoReadRangeKeyValueV2(std::move(p), facility, {});
}

void DoReadKeyValue(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&, const TKeyValueRequestSettings& settings) {
    TActivationContext::AsActorContext().Register(new TReadRequest<true>(p.release(), settings));
}

void DoReadKeyValueV2(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider&, const TKeyValueRequestSettings& settings) {
    TActivationContext::AsActorContext().Register(new TReadRequest<false>(p.release(), settings));
}

void DoReadRangeKeyValue(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&, const TKeyValueRequestSettings& settings) {
    TActivationContext::AsActorContext().Register(new TReadRangeRequest<true>(p.release(), settings));
}

void DoReadRangeKeyValueV2(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider&, const TKeyValueRequestSettings& settings) {
    TActivationContext::AsActorContext().Register(new TReadRangeRequest<false>(p.release(), settings));
}

void DoListRangeKeyValue(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TListRangeRequest<true>(p.release()));
}

void DoListRangeKeyValueV2(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TListRangeRequest<false>(p.release()));
}

void DoGetStorageChannelStatusKeyValue(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TGetStorageChannelStatusRequest<true>(p.release()));
}

void DoGetStorageChannelStatusKeyValueV2(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider&) {
    TActivationContext::AsActorContext().Register(new TGetStorageChannelStatusRequest<false>(p.release()));
}

} // namespace NKikimr::NGRpcService
