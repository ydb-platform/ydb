#include "keyvalue_client_v1.h"

#include <ydb/public/api/protos/ydb_keyvalue.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <util/string/builder.h>
#include <util/system/env.h>

#include <grpc/impl/grpc_types.h>
#include <grpcpp/create_channel.h>

#include <chrono>
#include <stdexcept>
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

TString BuildGrpcError(bool cqOk, const grpc::Status& grpcStatus) {
    if (!cqOk) {
        return "gRPC completion queue returned !ok";
    }

    return TStringBuilder()
        << "gRPC call failed: code=" << static_cast<int>(grpcStatus.error_code())
        << " message='" << grpcStatus.error_message() << "'";
}

template <typename TResponse, typename TStartCall, typename TGetOperationStatus, typename TDone>
struct TAsyncRetryState {
    std::shared_ptr<TGrpcAsyncExecutor> Executor;
    TStartCall StartCall;
    TGetOperationStatus GetOperationStatus;
    TDone Done;
    ui32 Attempt = 1;
};

template <typename TResponse, typename TStartCall, typename TGetOperationStatus, typename TDone>
void RunAsyncRetry(const std::shared_ptr<TAsyncRetryState<TResponse, TStartCall, TGetOperationStatus, TDone>>& state) {
    auto context = std::make_unique<grpc::ClientContext>();
    AdjustCtx(*context);
    context->set_deadline(std::chrono::system_clock::now() + GrpcDeadline);

    state->Executor->template UnaryAsync<TResponse>(
        [state](grpc::ClientContext* asyncCtx, grpc::CompletionQueue* completionQueue) mutable {
            return state->StartCall(asyncCtx, completionQueue);
        },
        std::move(context),
        [state](bool ok, grpc::Status grpcStatus, const TResponse& response) mutable {
            if (!ok || !grpcStatus.ok()) {
                if (state->Attempt >= GrpcRetryCount) {
                    state->Done(false, nullptr, BuildGrpcError(ok, grpcStatus));
                    return;
                }

                ++state->Attempt;
                RunAsyncRetry<TResponse>(state);
                return;
            }

            const Ydb::StatusIds::StatusCode operationStatus = state->GetOperationStatus(response);
            if (operationStatus == Ydb::StatusIds::UNAVAILABLE) {
                if (state->Attempt >= GrpcRetryCount) {
                    state->Done(
                        false,
                        nullptr,
                        TStringBuilder() << "Operation failed with status=" << StatusToString(operationStatus));
                    return;
                }

                ++state->Attempt;
                RunAsyncRetry<TResponse>(state);
                return;
            }

            state->Done(true, &response, TString{});
        });
}

template <typename TResponse, typename TStartCall, typename TGetOperationStatus, typename TDone>
void CallWithRetryAsync(
    const std::shared_ptr<TGrpcAsyncExecutor>& executor,
    TStartCall startCall,
    TGetOperationStatus getOperationStatus,
    TDone done)
{
    auto state = std::make_shared<TAsyncRetryState<TResponse, TStartCall, TGetOperationStatus, TDone>>(TAsyncRetryState<TResponse, TStartCall, TGetOperationStatus, TDone>{
        .Executor = executor,
        .StartCall = std::move(startCall),
        .GetOperationStatus = std::move(getOperationStatus),
        .Done = std::move(done),
        .Attempt = 1,
    });
    RunAsyncRetry<TResponse>(state);
}

} // namespace

TKeyValueClientV1::TKeyValueClientV1(const TString& hostPort, std::shared_ptr<TGrpcAsyncExecutor> executor)
    : Executor_(std::move(executor))
    , Channel_([&hostPort] {
        grpc::ChannelArguments args;
        args.SetInt(GRPC_ARG_USE_LOCAL_SUBCHANNEL_POOL, 1);
        return grpc::CreateCustomChannel(hostPort, grpc::InsecureChannelCredentials(), args);
    }())
    , Stub_(Ydb::KeyValue::V1::KeyValueService::NewStub(Channel_))
{
    if (!Executor_) {
        throw std::runtime_error("TKeyValueClientV1: async executor is null");
    }
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
            [&](grpc::ClientContext* ctx) {
                return Executor_->Unary<Ydb::KeyValue::CreateVolumeResponse>(
                    [&](grpc::ClientContext* asyncCtx, grpc::CompletionQueue* completionQueue) {
                        return Stub_->AsyncCreateVolume(asyncCtx, request, completionQueue);
                    },
                    ctx,
                    &response);
            },
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
            [&](grpc::ClientContext* ctx) {
                return Executor_->Unary<Ydb::KeyValue::DropVolumeResponse>(
                    [&](grpc::ClientContext* asyncCtx, grpc::CompletionQueue* completionQueue) {
                        return Stub_->AsyncDropVolume(asyncCtx, request, completionQueue);
                    },
                    ctx,
                    &response);
            },
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
            [&](grpc::ClientContext* ctx) {
                return Executor_->Unary<Ydb::KeyValue::ExecuteTransactionResponse>(
                    [&](grpc::ClientContext* asyncCtx, grpc::CompletionQueue* completionQueue) {
                        return Stub_->AsyncExecuteTransaction(asyncCtx, request, completionQueue);
                    },
                    ctx,
                    &response);
            },
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
            [&](grpc::ClientContext* ctx) {
                return Executor_->Unary<Ydb::KeyValue::ExecuteTransactionResponse>(
                    [&](grpc::ClientContext* asyncCtx, grpc::CompletionQueue* completionQueue) {
                        return Stub_->AsyncExecuteTransaction(asyncCtx, request, completionQueue);
                    },
                    ctx,
                    &response);
            },
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
            [&](grpc::ClientContext* ctx) {
                return Executor_->Unary<Ydb::KeyValue::ReadResponse>(
                    [&](grpc::ClientContext* asyncCtx, grpc::CompletionQueue* completionQueue) {
                        return Stub_->AsyncRead(asyncCtx, request, completionQueue);
                    },
                    ctx,
                    &response);
            },
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

void TKeyValueClientV1::WriteAsync(
    const TString& path,
    ui32 partitionId,
    const TVector<std::pair<TString, TString>>& kvPairs,
    ui32 channel,
    TStatusCallback done)
{
    auto request = std::make_shared<Ydb::KeyValue::ExecuteTransactionRequest>();
    request->set_path(path);
    request->set_partition_id(partitionId);

    for (const auto& [key, value] : kvPairs) {
        auto* write = request->add_commands()->mutable_write();
        write->set_key(key);
        write->set_value(value);
        write->set_storage_channel(channel);
    }

    CallWithRetryAsync<Ydb::KeyValue::ExecuteTransactionResponse>(
        Executor_,
        [this, request](grpc::ClientContext* asyncCtx, grpc::CompletionQueue* completionQueue) {
            return Stub_->AsyncExecuteTransaction(asyncCtx, *request, completionQueue);
        },
        [](const Ydb::KeyValue::ExecuteTransactionResponse& response) {
            return response.operation().status();
        },
        [done = std::move(done)](bool ok, const Ydb::KeyValue::ExecuteTransactionResponse* response, TString error) mutable {
            if (!ok || !response) {
                done(false, std::move(error));
                return;
            }

            if (response->operation().status() != Ydb::StatusIds::SUCCESS) {
                done(
                    false,
                    TStringBuilder() << "Write returned " << StatusToString(response->operation().status()));
                return;
            }

            done(true, TString{});
        });
}

void TKeyValueClientV1::DeleteKeyAsync(
    const TString& path,
    ui32 partitionId,
    const TString& key,
    TStatusCallback done)
{
    auto request = std::make_shared<Ydb::KeyValue::ExecuteTransactionRequest>();
    request->set_path(path);
    request->set_partition_id(partitionId);

    auto* deleteRange = request->add_commands()->mutable_delete_range();
    deleteRange->mutable_range()->set_from_key_inclusive(key);
    deleteRange->mutable_range()->set_to_key_inclusive(key);

    CallWithRetryAsync<Ydb::KeyValue::ExecuteTransactionResponse>(
        Executor_,
        [this, request](grpc::ClientContext* asyncCtx, grpc::CompletionQueue* completionQueue) {
            return Stub_->AsyncExecuteTransaction(asyncCtx, *request, completionQueue);
        },
        [](const Ydb::KeyValue::ExecuteTransactionResponse& response) {
            return response.operation().status();
        },
        [done = std::move(done)](bool ok, const Ydb::KeyValue::ExecuteTransactionResponse* response, TString error) mutable {
            if (!ok || !response) {
                done(false, std::move(error));
                return;
            }

            if (response->operation().status() != Ydb::StatusIds::SUCCESS) {
                done(
                    false,
                    TStringBuilder() << "Delete returned " << StatusToString(response->operation().status()));
                return;
            }

            done(true, TString{});
        });
}

void TKeyValueClientV1::ReadAsync(
    const TString& path,
    ui32 partitionId,
    const TString& key,
    ui32 offset,
    ui32 size,
    TReadCallback done)
{
    auto request = std::make_shared<Ydb::KeyValue::ReadRequest>();
    request->set_path(path);
    request->set_partition_id(partitionId);
    request->set_key(key);
    request->set_offset(offset);
    request->set_size(size);

    CallWithRetryAsync<Ydb::KeyValue::ReadResponse>(
        Executor_,
        [this, request](grpc::ClientContext* asyncCtx, grpc::CompletionQueue* completionQueue) {
            return Stub_->AsyncRead(asyncCtx, *request, completionQueue);
        },
        [](const Ydb::KeyValue::ReadResponse& response) {
            return response.operation().status();
        },
        [done = std::move(done)](bool ok, const Ydb::KeyValue::ReadResponse* response, TString error) mutable {
            if (!ok || !response) {
                done(false, TString{}, std::move(error));
                return;
            }

            if (response->operation().status() != Ydb::StatusIds::SUCCESS) {
                done(
                    false,
                    TString{},
                    TStringBuilder() << "Read returned " << StatusToString(response->operation().status()));
                return;
            }

            Ydb::KeyValue::ReadResult readResult;
            if (!response->operation().result().UnpackTo(&readResult)) {
                done(false, TString{}, "Read result unpack failed");
                return;
            }

            done(true, readResult.value(), TString{});
        });
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
