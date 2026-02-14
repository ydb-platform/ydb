#include "keyvalue_client_v1.h"

#include <ydb/public/api/protos/ydb_keyvalue.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <util/string/builder.h>
#include <util/system/env.h>

#include <grpcpp/create_channel.h>

#include <chrono>
#include <thread>

namespace NKvVolumeStress {

namespace {

using namespace std::chrono_literals;

constexpr ui32 GrpcRetryCount = 10;
constexpr auto GrpcRetrySleep = 100ms;
constexpr auto GrpcDeadline = 10s;
constexpr const char* YdbAuthHeader = "x-ydb-auth-ticket";

TString StatusToString(Ydb::StatusIds::StatusCode status) {
    return Ydb::StatusIds::StatusCode_Name(status);
}

const TString& AuthTicket() {
    static const TString token = [] {
        const TString fromEnv = GetEnv("YDB_AUTH_TICKET", "");
        return fromEnv;
    }();

    return token;
}

void AdjustCtx(grpc::ClientContext& ctx) {
    auto token = AuthTicket();
    if (token.size()) {
        ctx.AddMetadata(YdbAuthHeader, token);
    }
}

} // namespace

TKeyValueClientV1::TKeyValueClientV1(const TString& hostPort)
    : Channel_(grpc::CreateChannel(hostPort, grpc::InsecureChannelCredentials()))
    , Stub_(Ydb::KeyValue::V1::KeyValueService::NewStub(Channel_))
{
}

bool TKeyValueClientV1::CreateVolume(const TString& path, ui32 partitionCount, const TVector<TString>& channels, TString* error) {
    Ydb::KeyValue::CreateVolumeRequest request;
    request.set_path(path);
    request.set_partition_count(partitionCount);
    for (const TString& channelMedia : channels) {
        request.mutable_storage_config()->add_channel()->set_media(channelMedia);
    }

    Ydb::KeyValue::CreateVolumeResponse response;
    if (!CallWithRetry(
            [&](grpc::ClientContext* ctx) { return Stub_->CreateVolume(ctx, request, &response); },
            [&] { return response.operation().status(); },
            error))
    {
        return false;
    }

    if (response.operation().status() != Ydb::StatusIds::SUCCESS) {
        auto builder = TStringBuilder() << "CreateVolume for path " << path << " returned " << StatusToString(response.operation().status()) 
            << " partition_count: " << partitionCount
            << " media: ";
        for (const TString& channelMedia : channels) {
            builder << channelMedia << ' ';
        }

        *error = builder;
        return false;
    }

    return true;
}

bool TKeyValueClientV1::DropVolume(const TString& path, TString* error) {
    Ydb::KeyValue::DropVolumeRequest request;
    request.set_path(path);

    Ydb::KeyValue::DropVolumeResponse response;
    if (!CallWithRetry(
            [&](grpc::ClientContext* ctx) { return Stub_->DropVolume(ctx, request, &response); },
            [&] { return response.operation().status(); },
            error))
    {
        return false;
    }

    if (response.operation().status() != Ydb::StatusIds::SUCCESS) {
        *error = TStringBuilder() << "DropVolume returned " << StatusToString(response.operation().status());
        return false;
    }

    return true;
}

bool TKeyValueClientV1::Write(
    const TString& path,
    ui32 partitionId,
    const TVector<std::pair<TString, TString>>& kvPairs,
    ui32 channel,
    TString* error)
{
    Ydb::KeyValue::ExecuteTransactionRequest request;
    request.set_path(path);
    request.set_partition_id(partitionId);

    for (const auto& [key, value] : kvPairs) {
        auto* write = request.add_commands()->mutable_write();
        write->set_key(key);
        write->set_value(value);
        write->set_storage_channel(channel);
    }

    Ydb::KeyValue::ExecuteTransactionResponse response;
    if (!CallWithRetry(
            [&](grpc::ClientContext* ctx) { return Stub_->ExecuteTransaction(ctx, request, &response); },
            [&] { return response.operation().status(); },
            error))
    {
        return false;
    }

    if (response.operation().status() != Ydb::StatusIds::SUCCESS) {
        *error = TStringBuilder() << "Write returned " << StatusToString(response.operation().status());
        return false;
    }

    return true;
}

bool TKeyValueClientV1::DeleteKey(const TString& path, ui32 partitionId, const TString& key, TString* error) {
    Ydb::KeyValue::ExecuteTransactionRequest request;
    request.set_path(path);
    request.set_partition_id(partitionId);

    auto* deleteRange = request.add_commands()->mutable_delete_range();
    deleteRange->mutable_range()->set_from_key_inclusive(key);
    deleteRange->mutable_range()->set_to_key_inclusive(key);

    Ydb::KeyValue::ExecuteTransactionResponse response;
    if (!CallWithRetry(
            [&](grpc::ClientContext* ctx) { return Stub_->ExecuteTransaction(ctx, request, &response); },
            [&] { return response.operation().status(); },
            error))
    {
        return false;
    }

    if (response.operation().status() != Ydb::StatusIds::SUCCESS) {
        *error = TStringBuilder() << "Delete returned " << StatusToString(response.operation().status());
        return false;
    }

    return true;
}

bool TKeyValueClientV1::Read(
    const TString& path,
    ui32 partitionId,
    const TString& key,
    ui32 offset,
    ui32 size,
    TString* value,
    TString* error)
{
    Ydb::KeyValue::ReadRequest request;
    request.set_path(path);
    request.set_partition_id(partitionId);
    request.set_key(key);
    request.set_offset(offset);
    request.set_size(size);

    Ydb::KeyValue::ReadResponse response;
    if (!CallWithRetry(
            [&](grpc::ClientContext* ctx) { return Stub_->Read(ctx, request, &response); },
            [&] { return response.operation().status(); },
            error))
    {
        return false;
    }

    if (response.operation().status() != Ydb::StatusIds::SUCCESS) {
        *error = TStringBuilder() << "Read returned " << StatusToString(response.operation().status());
        return false;
    }

    Ydb::KeyValue::ReadResult readResult;
    if (!response.operation().result().UnpackTo(&readResult)) {
        *error = "Read result unpack failed";
        return false;
    }

    *value = readResult.value();
    return true;
}

bool TKeyValueClientV1::CallWithRetry(
    const std::function<grpc::Status(grpc::ClientContext*)>& call,
    const std::function<Ydb::StatusIds::StatusCode()>& getOperationStatus,
    TString* error)
{
    for (ui32 attempt = 1; attempt <= GrpcRetryCount; ++attempt) {
        grpc::ClientContext context;
        AdjustCtx(context);
        context.set_deadline(std::chrono::system_clock::now() + GrpcDeadline);

        grpc::Status grpcStatus = call(&context);
        if (!grpcStatus.ok()) {
            if (attempt == GrpcRetryCount) {
                *error = TStringBuilder()
                    << "gRPC call failed: code=" << static_cast<int>(grpcStatus.error_code())
                    << " message='" << grpcStatus.error_message() << "'";
                return false;
            }

            std::this_thread::sleep_for(GrpcRetrySleep);
            continue;
        }

        const Ydb::StatusIds::StatusCode operationStatus = getOperationStatus();
        if (operationStatus == Ydb::StatusIds::UNAVAILABLE) {
            if (attempt == GrpcRetryCount) {
                *error = TStringBuilder()
                    << "Operation failed with status=" << StatusToString(operationStatus);
                return false;
            }

            std::this_thread::sleep_for(GrpcRetrySleep);
            continue;
        }

        return true;
    }

    *error = "Retry loop failed unexpectedly";
    return false;
}

} // namespace NKvVolumeStress
