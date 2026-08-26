#include <ydb/public/api/grpc/ydb_keyvalue_v1.grpc.pb.h>
#include <ydb/public/api/protos/ydb_issue_message.pb.h>
#include <ydb/public/api/protos/ydb_keyvalue.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <library/cpp/getopt/last_getopt.h>

#include <util/generic/size_literals.h>
#include <util/stream/output.h>
#include <util/string/builder.h>
#include <util/string/printf.h>
#include <util/string/split.h>
#include <util/system/env.h>
#include <util/stream/file.h>
#include <util/string/strip.h>
#include <util/system/fstat.h>

#include <google/protobuf/text_format.h>

#include <grpcpp/create_channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/security/credentials.h>

#include <grpcpp/support/channel_arguments.h>
#include <grpc/impl/channel_arg_names.h>

#include <chrono>
#include <cinttypes>
#include <csignal>
#include <memory>
#include <thread>
#include <atomic>
#include <mutex>
#include <vector>
#include <algorithm>
#include <random>
#include <tuple>


using namespace std::chrono_literals;

static bool VerboseMode = false;
static TString AuthToken;

namespace {

constexpr auto GrpcDeadline = 30s;
constexpr ui64 GrpcMaxMessageSize = 64_MB;
constexpr ui64 FileTransferChunkSize = 32_MB;
constexpr ui64 MinValueSizeForRateLimit = 64_KB;
constexpr const char* YdbAuthHeader = "x-ydb-auth-ticket";
constexpr const char* YdbDatabaseHeader = "x-ydb-database";
// W3C trace context, https://w3c.github.io/trace-context/#header-name
constexpr const char* OtelTraceHeader = "traceparent";

TString GetAuthToken() {
    return AuthToken;
}

TString MakeVolumePath(const TString& database, const TString& path) {
    if (database.empty() || path.StartsWith(database)) {
        return path;
    }
    if (database.back() == '/') {
        return database + path;
    }
    return database + "/" + path;
}

struct TTraceContext {
    TString Traceparent;
    TString TraceId;
};

// Generates a traceparent header: "00-<32 hex trace id>-<16 hex span id>-01".
// Only the id fields are random; the version and sampled flag are fixed.
// The server picks the tracing level from the matching external_throttling rule in
// tracing_config, so the sampled flag here is informational only.
TTraceContext MakeTraceparent() {
    static thread_local std::mt19937_64 rng(std::random_device{}());
    const ui64 hi = rng() | 1;
    const ui64 lo = rng();
    const ui64 spanId = rng() | 1;
    TTraceContext ctx;
    ctx.TraceId = Sprintf("%016" PRIx64 "%016" PRIx64, hi, lo);
    ctx.Traceparent = Sprintf("00-%s-%016" PRIx64 "-01", ctx.TraceId.c_str(), spanId);
    return ctx;
}

void AdjustContext(grpc::ClientContext& ctx, const TString& database, const TString& traceparent = {}) {
    auto token = GetAuthToken();
    if (!token.empty()) {
        ctx.AddMetadata(YdbAuthHeader, token);
    }
    if (!database.empty()) {
        ctx.AddMetadata(YdbDatabaseHeader, database);
    }
    if (!traceparent.empty()) {
        ctx.AddMetadata(OtelTraceHeader, traceparent);
    }
    ctx.set_deadline(std::chrono::system_clock::now() + GrpcDeadline);
}

TString StatusToString(Ydb::StatusIds::StatusCode status) {
    return Ydb::StatusIds::StatusCode_Name(status);
}

bool IsSecureEndpoint(const TString& endpoint) {
    return endpoint.StartsWith("grpcs://");
}

TString ParseHostPort(const TString& endpoint) {
    TString hostPort = endpoint;
    const TString scheme = "://";
    const size_t pos = hostPort.find(scheme);
    if (pos != TString::npos) {
        hostPort = hostPort.substr(pos + scheme.size());
    }
    return hostPort;
}

std::shared_ptr<grpc::ChannelCredentials> MakeChannelCredentials(bool useTls) {
    if (useTls) {
        return grpc::SslCredentials({});
    }
    return grpc::InsecureChannelCredentials();
}

std::shared_ptr<grpc::Channel> MakeGrpcChannel(const TString& endpoint, bool useTls, ui32 channelId = 0) {
    grpc::ChannelArguments args;
    args.SetMaxReceiveMessageSize(GrpcMaxMessageSize);
    args.SetMaxSendMessageSize(GrpcMaxMessageSize);
    // Default gRPC shares one TCP connection across all Channel objects to the same
    // target. Each load worker must get its own connection.
    args.SetInt(GRPC_ARG_USE_LOCAL_SUBCHANNEL_POOL, 1);
    args.SetInt(GRPC_ARG_CHANNEL_ID, static_cast<int>(channelId));
    return grpc::CreateCustomChannel(endpoint, MakeChannelCredentials(useTls), args);
}

std::atomic<bool>* LoadStopFlag = nullptr;

void HandleLoadSignal(int) {
    if (LoadStopFlag) {
        LoadStopFlag->store(true, std::memory_order_relaxed);
    }
}

class TByteRateLimiter {
public:
    explicit TByteRateLimiter(ui64 bytesPerSec)
        : BytesPerSec(bytesPerSec)
        , Start(std::chrono::steady_clock::now())
    {
    }

    void Acquire(ui64 bytes) {
        if (BytesPerSec == 0) {
            return;
        }
        while (true) {
            ui64 already = Consumed.load(std::memory_order_relaxed);
            const double elapsed = std::chrono::duration<double>(
                std::chrono::steady_clock::now() - Start).count();
            const ui64 budget = static_cast<ui64>(elapsed * BytesPerSec);
            if (already <= budget) {
                if (Consumed.compare_exchange_weak(already, already + bytes, std::memory_order_relaxed)) {
                    return;
                }
                continue;
            }
            const double waitSec = static_cast<double>(already - budget) / BytesPerSec;
            auto wait = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::duration<double>(waitSec));
            if (wait < 1ms) {
                wait = 1ms;
            }
            std::this_thread::sleep_for(wait);
        }
    }

private:
    const ui64 BytesPerSec;
    const std::chrono::steady_clock::time_point Start;
    std::atomic<ui64> Consumed{0};
};

double ToMiBps(ui64 bytes, double seconds) {
    if (seconds <= 0) {
        return 0;
    }
    return static_cast<double>(bytes) / (1.0 * 1_MB) / seconds;
}

int DescribeVolume(const TString& endpoint, const TString& database, const TString& path, bool useTls) {
    auto channel = grpc::CreateChannel(endpoint, MakeChannelCredentials(useTls));
    auto stub = Ydb::KeyValue::V1::KeyValueService::NewStub(channel);

    Ydb::KeyValue::DescribeVolumeRequest request;
    request.set_path(path);

    if (VerboseMode) {
        TString requestText;
        google::protobuf::TextFormat::PrintToString(request, &requestText);
        Cerr << "=== Request ===" << Endl;
        Cerr << requestText << Endl;
    }

    Ydb::KeyValue::DescribeVolumeResponse response;
    grpc::ClientContext context;
    AdjustContext(context, database);

    grpc::Status status = stub->DescribeVolume(&context, request, &response);

    if (VerboseMode) {
        TString responseText;
        google::protobuf::TextFormat::PrintToString(response, &responseText);
        Cerr << "=== Response ===" << Endl;
        Cerr << responseText << Endl;
    }

    if (!status.ok()) {
        Cerr << "gRPC call failed: " << status.error_message() << Endl;
        return 1;
    }

    if (response.operation().status() != Ydb::StatusIds::SUCCESS) {
        Cerr << "DescribeVolume failed with status: " << StatusToString(response.operation().status()) << Endl;
        if (response.operation().issues_size() > 0) {
            Cerr << "Issues:" << Endl;
            for (const auto& issue : response.operation().issues()) {
                Cerr << "  " << issue.message() << Endl;
            }
        }
        return 1;
    }

    Ydb::KeyValue::DescribeVolumeResult result;
    if (!response.operation().result().UnpackTo(&result)) {
        Cerr << "Failed to unpack DescribeVolumeResult" << Endl;
        return 1;
    }

    Cout << "Volume path: " << result.path() << Endl;
    Cout << "Partition count: " << result.partition_count() << Endl;

    if (result.has_storage_config() && result.storage_config().channel_size() > 0) {
        Cout << "Storage channels: " << result.storage_config().channel_size() << Endl;
        for (int i = 0; i < result.storage_config().channel_size(); ++i) {
            const auto& channel = result.storage_config().channel(i);
            TString mediaStr = channel.media();
            Cout << "  Channel " << i << ": ";
            if (mediaStr.empty()) {
                Cout << "<default storage pool>";
            } else {
                Cout << "media=" << mediaStr;
            }
            Cout << Endl;
        }
    } else {
        Cout << "Storage channels: <not configured>" << Endl;
    }

    return 0;
}

int CreateVolume(const TString& endpoint, const TString& database, const TString& path, ui32 partitionCount, const TVector<TString>& channelMedia, bool useTls) {
    auto channel = grpc::CreateChannel(endpoint, MakeChannelCredentials(useTls));
    auto stub = Ydb::KeyValue::V1::KeyValueService::NewStub(channel);

    Ydb::KeyValue::CreateVolumeRequest request;
    request.set_path(path);
    request.set_partition_count(partitionCount);

    if (!channelMedia.empty()) {
        auto* storageConfig = request.mutable_storage_config();
        for (const auto& media : channelMedia) {
            storageConfig->add_channel()->set_media(media);
        }
    }

    if (VerboseMode) {
        TString requestText;
        google::protobuf::TextFormat::PrintToString(request, &requestText);
        Cerr << "=== Request ===" << Endl;
        Cerr << requestText << Endl;
    }

    Ydb::KeyValue::CreateVolumeResponse response;
    grpc::ClientContext context;
    AdjustContext(context, database);

    grpc::Status status = stub->CreateVolume(&context, request, &response);

    if (VerboseMode) {
        TString responseText;
        google::protobuf::TextFormat::PrintToString(response, &responseText);
        Cerr << "=== Response ===" << Endl;
        Cerr << responseText << Endl;
    }

    if (!status.ok()) {
        Cerr << "gRPC call failed: " << status.error_message() << Endl;
        return 1;
    }

    if (response.operation().status() != Ydb::StatusIds::SUCCESS) {
        Cerr << "CreateVolume failed with status: " << StatusToString(response.operation().status()) << Endl;
        if (response.operation().issues_size() > 0) {
            Cerr << "Issues:" << Endl;
            for (const auto& issue : response.operation().issues()) {
                Cerr << "  " << issue.message() << Endl;
            }
        }
        return 1;
    }

    Cout << "Volume created successfully" << Endl;
    return 0;
}

int RemoveVolume(const TString& endpoint, const TString& database, const TString& path, bool useTls) {
    auto channel = grpc::CreateChannel(endpoint, MakeChannelCredentials(useTls));
    auto stub = Ydb::KeyValue::V1::KeyValueService::NewStub(channel);

    Ydb::KeyValue::DropVolumeRequest request;
    request.set_path(path);

    if (VerboseMode) {
        TString requestText;
        google::protobuf::TextFormat::PrintToString(request, &requestText);
        Cerr << "=== Request ===" << Endl;
        Cerr << requestText << Endl;
    }

    Ydb::KeyValue::DropVolumeResponse response;
    grpc::ClientContext context;
    AdjustContext(context, database);

    grpc::Status status = stub->DropVolume(&context, request, &response);

    if (VerboseMode) {
        TString responseText;
        google::protobuf::TextFormat::PrintToString(response, &responseText);
        Cerr << "=== Response ===" << Endl;
        Cerr << responseText << Endl;
    }

    if (!status.ok()) {
        Cerr << "gRPC call failed: " << status.error_message() << Endl;
        return 1;
    }

    if (response.operation().status() != Ydb::StatusIds::SUCCESS) {
        Cerr << "DropVolume failed with status: " << StatusToString(response.operation().status()) << Endl;
        if (response.operation().issues_size() > 0) {
            Cerr << "Issues:" << Endl;
            for (const auto& issue : response.operation().issues()) {
                Cerr << "  " << issue.message() << Endl;
            }
        }
        return 1;
    }

    Cout << "Volume removed successfully" << Endl;
    return 0;
}

int AlterVolume(const TString& endpoint, const TString& database, const TString& path, ui32 partitionCount, const TVector<TString>& channelMedia, bool useTls) {
    auto channel = grpc::CreateChannel(endpoint, MakeChannelCredentials(useTls));
    auto stub = Ydb::KeyValue::V1::KeyValueService::NewStub(channel);

    Ydb::KeyValue::AlterVolumeRequest request;
    request.set_path(path);
    
    if (partitionCount > 0) {
        request.set_alter_partition_count(partitionCount);
    }

    if (!channelMedia.empty()) {
        auto* storageConfig = request.mutable_storage_config();
        for (const auto& media : channelMedia) {
            storageConfig->add_channel()->set_media(media);
        }
    }

    if (VerboseMode) {
        TString requestText;
        google::protobuf::TextFormat::PrintToString(request, &requestText);
        Cerr << "=== Request ===" << Endl;
        Cerr << requestText << Endl;
    }

    Ydb::KeyValue::AlterVolumeResponse response;
    grpc::ClientContext context;
    AdjustContext(context, database);

    grpc::Status status = stub->AlterVolume(&context, request, &response);

    if (VerboseMode) {
        TString responseText;
        google::protobuf::TextFormat::PrintToString(response, &responseText);
        Cerr << "=== Response ===" << Endl;
        Cerr << responseText << Endl;
    }

    if (!status.ok()) {
        Cerr << "gRPC call failed: " << status.error_message() << Endl;
        return 1;
    }

    if (response.operation().status() != Ydb::StatusIds::SUCCESS) {
        Cerr << "AlterVolume failed with status: " << StatusToString(response.operation().status()) << Endl;
        if (response.operation().issues_size() > 0) {
            Cerr << "Issues:" << Endl;
            for (const auto& issue : response.operation().issues()) {
                Cerr << "  " << issue.message() << Endl;
            }
        }
        return 1;
    }

    Cout << "Volume altered successfully" << Endl;
    return 0;
}

int ReadValue(const TString& endpoint, const TString& database, const TString& path, ui64 partitionId, const TString& key, ui64 offset, ui64 size, bool useTls) {
    auto channel = grpc::CreateChannel(endpoint, MakeChannelCredentials(useTls));
    auto stub = Ydb::KeyValue::V1::KeyValueService::NewStub(channel);

    Ydb::KeyValue::ReadRequest request;
    request.set_path(path);
    request.set_partition_id(partitionId);
    request.set_key(key);
    request.set_offset(offset);
    request.set_size(size);

    if (VerboseMode) {
        TString requestText;
        google::protobuf::TextFormat::PrintToString(request, &requestText);
        Cerr << "=== Request ===" << Endl;
        Cerr << requestText << Endl;
    }

    Ydb::KeyValue::ReadResponse response;
    grpc::ClientContext context;
    AdjustContext(context, database);

    grpc::Status status = stub->Read(&context, request, &response);

    if (VerboseMode) {
        TString responseText;
        google::protobuf::TextFormat::PrintToString(response, &responseText);
        Cerr << "=== Response ===" << Endl;
        Cerr << responseText << Endl;
    }

    if (!status.ok()) {
        Cerr << "gRPC call failed: " << status.error_message() << Endl;
        return 1;
    }

    if (response.operation().status() != Ydb::StatusIds::SUCCESS) {
        Cerr << "Read failed with status: " << StatusToString(response.operation().status()) << Endl;
        if (response.operation().issues_size() > 0) {
            Cerr << "Issues:" << Endl;
            for (const auto& issue : response.operation().issues()) {
                Cerr << "  " << issue.message() << Endl;
            }
        }
        return 1;
    }

    Ydb::KeyValue::ReadResult result;
    if (!response.operation().result().UnpackTo(&result)) {
        Cerr << "Failed to unpack ReadResult" << Endl;
        return 1;
    }

    Cout << "Key: " << result.requested_key() << Endl;
    Cout << "Offset: " << result.requested_offset() << Endl;
    Cout << "Size: " << result.requested_size() << Endl;
    Cout << "Value size: " << result.value().size() << Endl;
    if (result.is_overrun()) {
        Cout << "Warning: Result was truncated (is_overrun=true)" << Endl;
    }
    Cout << "Value: " << result.value() << Endl;

    return 0;
}

int WriteValue(const TString& endpoint, const TString& database, const TString& path, ui64 partitionId, const TString& key, const TString& value, ui32 storageChannel, bool useTls) {
    auto channel = grpc::CreateChannel(endpoint, MakeChannelCredentials(useTls));
    auto stub = Ydb::KeyValue::V1::KeyValueService::NewStub(channel);

    Ydb::KeyValue::ExecuteTransactionRequest request;
    request.set_path(path);
    request.set_partition_id(partitionId);

    auto* command = request.add_commands();
    auto* write = command->mutable_write();
    write->set_key(key);
    write->set_value(value);
    if (storageChannel > 0) {
        write->set_storage_channel(storageChannel);
    }

    if (VerboseMode) {
        TString requestText;
        google::protobuf::TextFormat::PrintToString(request, &requestText);
        Cerr << "=== Request ===" << Endl;
        Cerr << requestText << Endl;
    }

    Ydb::KeyValue::ExecuteTransactionResponse response;
    grpc::ClientContext context;
    AdjustContext(context, database);

    grpc::Status status = stub->ExecuteTransaction(&context, request, &response);

    if (VerboseMode) {
        TString responseText;
        google::protobuf::TextFormat::PrintToString(response, &responseText);
        Cerr << "=== Response ===" << Endl;
        Cerr << responseText << Endl;
    }

    if (!status.ok()) {
        Cerr << "gRPC call failed: " << status.error_message() << Endl;
        return 1;
    }

    if (response.operation().status() != Ydb::StatusIds::SUCCESS) {
        Cerr << "Write failed with status: " << StatusToString(response.operation().status()) << Endl;
        if (response.operation().issues_size() > 0) {
            Cerr << "Issues:" << Endl;
            for (const auto& issue : response.operation().issues()) {
                Cerr << "  " << issue.message() << Endl;
            }
        }
        return 1;
    }

    Ydb::KeyValue::ExecuteTransactionResult result;
    if (!response.operation().result().UnpackTo(&result)) {
        Cerr << "Failed to unpack ExecuteTransactionResult" << Endl;
        return 1;
    }

    Cout << "Value written successfully" << Endl;
    if (result.storage_channel_info_size() > 0) {
        Cout << "Storage channel info:" << Endl;
        for (const auto& info : result.storage_channel_info()) {
            Cout << "  Channel " << info.storage_channel() << ": ";
            switch (info.status_flag()) {
                case Ydb::KeyValue::StorageChannelInfo::STATUS_FLAG_UNSPECIFIED:
                    Cout << "UNSPECIFIED";
                    break;
                case Ydb::KeyValue::StorageChannelInfo::STATUS_FLAG_GREEN:
                    Cout << "GREEN";
                    break;
                case Ydb::KeyValue::StorageChannelInfo::STATUS_FLAG_YELLOW_STOP:
                    Cout << "YELLOW_STOP";
                    break;
                case Ydb::KeyValue::StorageChannelInfo::STATUS_FLAG_ORANGE_OUT_SPACE:
                    Cout << "ORANGE_OUT_SPACE";
                    break;
                default:
                    Cout << "UNKNOWN";
                    break;
            }
            Cout << Endl;
        }
    }

    return 0;
}

TString MakePartKey(const TString& key, ui64 partIndex) {
    return TStringBuilder() << key << "_part_" << partIndex;
}

int ExecuteTransaction(
    Ydb::KeyValue::V1::KeyValueService::Stub& stub,
    const TString& database,
    Ydb::KeyValue::ExecuteTransactionRequest request,
    const char* operationName)
{
    if (VerboseMode) {
        TString requestText;
        google::protobuf::TextFormat::PrintToString(request, &requestText);
        Cerr << "=== Request ===" << Endl;
        Cerr << requestText << Endl;
    }

    Ydb::KeyValue::ExecuteTransactionResponse response;
    grpc::ClientContext context;
    AdjustContext(context, database);

    grpc::Status status = stub.ExecuteTransaction(&context, request, &response);

    if (VerboseMode) {
        TString responseText;
        google::protobuf::TextFormat::PrintToString(response, &responseText);
        Cerr << "=== Response ===" << Endl;
        Cerr << responseText << Endl;
    }

    if (!status.ok()) {
        Cerr << "gRPC call failed: " << status.error_message() << Endl;
        return 1;
    }

    if (response.operation().status() != Ydb::StatusIds::SUCCESS) {
        Cerr << operationName << " failed with status: " << StatusToString(response.operation().status()) << Endl;
        if (response.operation().issues_size() > 0) {
            Cerr << "Issues:" << Endl;
            for (const auto& issue : response.operation().issues()) {
                Cerr << "  " << issue.message() << Endl;
            }
        }
        return 1;
    }

    Ydb::KeyValue::ExecuteTransactionResult result;
    if (!response.operation().result().UnpackTo(&result)) {
        Cerr << "Failed to unpack ExecuteTransactionResult" << Endl;
        return 1;
    }

    return 0;
}

bool ReadValueChunk(
    Ydb::KeyValue::V1::KeyValueService::Stub& stub,
    const TString& database,
    const TString& path,
    ui64 partitionId,
    const TString& key,
    ui64 offset,
    ui64 limitBytes,
    Ydb::KeyValue::ReadResult& result)
{
    Ydb::KeyValue::ReadRequest request;
    request.set_path(path);
    request.set_partition_id(partitionId);
    request.set_key(key);
    request.set_offset(offset);
    request.set_size(0);
    request.set_limit_bytes(limitBytes);

    if (VerboseMode) {
        TString requestText;
        google::protobuf::TextFormat::PrintToString(request, &requestText);
        Cerr << "=== Request ===" << Endl;
        Cerr << requestText << Endl;
    }

    Ydb::KeyValue::ReadResponse response;
    grpc::ClientContext context;
    AdjustContext(context, database);

    grpc::Status status = stub.Read(&context, request, &response);

    if (VerboseMode) {
        TString responseText;
        google::protobuf::TextFormat::PrintToString(response, &responseText);
        Cerr << "=== Response ===" << Endl;
        Cerr << responseText << Endl;
    }

    if (!status.ok()) {
        Cerr << "gRPC call failed: " << status.error_message() << Endl;
        return false;
    }

    if (response.operation().status() != Ydb::StatusIds::SUCCESS) {
        Cerr << "Read failed with status: " << StatusToString(response.operation().status()) << Endl;
        if (response.operation().issues_size() > 0) {
            Cerr << "Issues:" << Endl;
            for (const auto& issue : response.operation().issues()) {
                Cerr << "  " << issue.message() << Endl;
            }
        }
        return false;
    }

    if (!response.operation().result().UnpackTo(&result)) {
        Cerr << "Failed to unpack ReadResult" << Endl;
        return false;
    }

    return true;
}

void CleanupPartKeys(
    Ydb::KeyValue::V1::KeyValueService::Stub& stub,
    const TString& database,
    const TString& path,
    ui64 partitionId,
    const TVector<TString>& partKeys)
{
    for (const auto& partKey : partKeys) {
        Ydb::KeyValue::ExecuteTransactionRequest request;
        request.set_path(path);
        request.set_partition_id(partitionId);

        auto* deleteRange = request.add_commands()->mutable_delete_range();
        auto* range = deleteRange->mutable_range();
        range->set_from_key_inclusive(partKey);
        range->set_to_key_inclusive(partKey);

        if (ExecuteTransaction(stub, database, std::move(request), "DeletePart") != 0) {
            Cerr << "Failed to cleanup part key: " << partKey << Endl;
        }
    }
}

int UploadFile(const TString& endpoint, const TString& database, const TString& path, ui64 partitionId, const TString& key, const TString& filePath, ui32 storageChannel, bool useTls) {
    const i64 fileSize = GetFileLength(filePath);
    if (fileSize < 0) {
        Cerr << "Failed to get file size: " << filePath << Endl;
        return 1;
    }

    Cout << "Uploading file: " << filePath << " (" << fileSize << " bytes)" << Endl;

    auto channel = MakeGrpcChannel(endpoint, useTls);
    auto stub = Ydb::KeyValue::V1::KeyValueService::NewStub(channel);

    if (static_cast<ui64>(fileSize) <= FileTransferChunkSize) {
        TFileInput fileInput(filePath);
        const TString value = fileInput.ReadAll();

        Ydb::KeyValue::ExecuteTransactionRequest request;
        request.set_path(path);
        request.set_partition_id(partitionId);

        auto* write = request.add_commands()->mutable_write();
        write->set_key(key);
        write->set_value(value);
        if (storageChannel > 0) {
            write->set_storage_channel(storageChannel);
        }

        if (ExecuteTransaction(*stub, database, std::move(request), "Write") != 0) {
            return 1;
        }
    } else {
        TVector<TString> partKeys;
        partKeys.reserve((fileSize + FileTransferChunkSize - 1) / FileTransferChunkSize);

        TFileInput fileInput(filePath);
        TVector<char> buffer(FileTransferChunkSize);
        ui64 partIndex = 0;

        while (true) {
            const size_t readBytes = fileInput.Read(buffer.data(), buffer.size());
            if (readBytes == 0) {
                break;
            }

            const TString partKey = MakePartKey(key, partIndex);
            partKeys.push_back(partKey);

            Ydb::KeyValue::ExecuteTransactionRequest request;
            request.set_path(path);
            request.set_partition_id(partitionId);

            auto* write = request.add_commands()->mutable_write();
            write->set_key(partKey);
            write->set_value(TStringBuf(buffer.data(), readBytes));
            if (storageChannel > 0) {
                write->set_storage_channel(storageChannel);
            }

            Cout << "Uploading part " << partIndex << ": key=" << partKey << " (" << readBytes << " bytes)" << Endl;
            if (ExecuteTransaction(*stub, database, std::move(request), "Write") != 0) {
                CleanupPartKeys(*stub, database, path, partitionId, partKeys);
                return 1;
            }

            ++partIndex;
        }

        Ydb::KeyValue::ExecuteTransactionRequest request;
        request.set_path(path);
        request.set_partition_id(partitionId);

        auto* concat = request.add_commands()->mutable_concat();
        for (const auto& partKey : partKeys) {
            concat->add_input_keys(partKey);
        }
        concat->set_output_key(key);
        concat->set_keep_inputs(false);

        Cout << "Concatenating " << partKeys.size() << " part(s) into key=" << key << Endl;
        if (ExecuteTransaction(*stub, database, std::move(request), "Concat") != 0) {
            CleanupPartKeys(*stub, database, path, partitionId, partKeys);
            return 1;
        }
    }

    Cout << "File uploaded successfully: " << filePath << " -> key=" << key << " (" << fileSize << " bytes)" << Endl;
    return 0;
}

int DownloadFile(const TString& endpoint, const TString& database, const TString& path, ui64 partitionId, const TString& key, const TString& filePath, bool useTls) {
    auto channel = MakeGrpcChannel(endpoint, useTls);
    auto stub = Ydb::KeyValue::V1::KeyValueService::NewStub(channel);

    ui64 offset = 0;
    ui64 totalBytes = 0;
    std::unique_ptr<TFileOutput> out;

    while (true) {
        Ydb::KeyValue::ReadResult result;
        if (!ReadValueChunk(*stub, database, path, partitionId, key, offset, FileTransferChunkSize, result)) {
            return 1;
        }

        if (result.value().empty()) {
            if (offset == 0) {
                TFileOutput emptyOut(filePath);
                emptyOut.Finish();
                Cout << "File downloaded successfully: key=" << key << " -> " << filePath << " (0 bytes)" << Endl;
                return 0;
            }
            break;
        }

        if (!out) {
            out = std::make_unique<TFileOutput>(filePath);
        }

        out->Write(result.value().data(), result.value().size());
        totalBytes += result.value().size();
        offset += result.value().size();

        if (!result.is_overrun()) {
            break;
        }
    }

    if (out) {
        out->Finish();
    }

    Cout << "File downloaded successfully: key=" << key << " -> " << filePath << " (" << totalBytes << " bytes)" << Endl;
    return 0;
}

void AppendIssue(TStringBuilder& out, const Ydb::Issue::IssueMessage& issue, bool& first) {
    if (!first) {
        out << "; ";
    }
    first = false;
    out << issue.message();
    for (const auto& nested : issue.issues()) {
        AppendIssue(out, nested, first);
    }
}

TString FormatOperationIssues(const Ydb::Operations::Operation& operation) {
    TStringBuilder issues;
    bool first = true;
    for (const auto& issue : operation.issues()) {
        AppendIssue(issues, issue, first);
    }
    return issues;
}

bool DoWriteValue(Ydb::KeyValue::V1::KeyValueService::Stub& stub, const TString& database, const TString& path, ui64 partitionId, const TString& key, const TString& value, ui32 storageChannel, TString* errorOut = nullptr, const TString& traceparent = {}) {
    Ydb::KeyValue::ExecuteTransactionRequest request;
    request.set_path(path);
    request.set_partition_id(partitionId);

    auto* command = request.add_commands();
    auto* write = command->mutable_write();
    write->set_key(key);
    write->set_value(value);
    if (storageChannel > 0) {
        write->set_storage_channel(storageChannel);
    }

    Ydb::KeyValue::ExecuteTransactionResponse response;
    grpc::ClientContext context;
    AdjustContext(context, database, traceparent);

    grpc::Status status = stub.ExecuteTransaction(&context, request, &response);
    if (!status.ok()) {
        if (errorOut) {
            *errorOut = TStringBuilder() << "gRPC error: " << status.error_message()
                << " (code=" << static_cast<int>(status.error_code()) << ")";
        }
        return false;
    }
    if (response.operation().status() != Ydb::StatusIds::SUCCESS) {
        if (errorOut) {
            *errorOut = TStringBuilder() << "status=" << StatusToString(response.operation().status())
                << " issues=[" << FormatOperationIssues(response.operation()) << "]";
        }
        return false;
    }
    return true;
}

bool DoWriteBatch(Ydb::KeyValue::V1::KeyValueService::Stub& stub, const TString& database, const TString& path, ui64 partitionId, const TVector<TString>& keys, const TString& value, ui32 storageChannel, TString* errorOut = nullptr, const TString& traceparent = {}) {
    Ydb::KeyValue::ExecuteTransactionRequest request;
    request.set_path(path);
    request.set_partition_id(partitionId);

    for (const auto& key : keys) {
        auto* command = request.add_commands();
        auto* write = command->mutable_write();
        write->set_key(key);
        write->set_value(value);
        if (storageChannel > 0) {
            write->set_storage_channel(storageChannel);
        }
    }

    Ydb::KeyValue::ExecuteTransactionResponse response;
    grpc::ClientContext context;
    AdjustContext(context, database, traceparent);

    grpc::Status status = stub.ExecuteTransaction(&context, request, &response);
    if (!status.ok()) {
        if (errorOut) {
            *errorOut = TStringBuilder() << "gRPC error: " << status.error_message()
                << " (code=" << static_cast<int>(status.error_code()) << ")";
        }
        return false;
    }
    if (response.operation().status() != Ydb::StatusIds::SUCCESS) {
        if (errorOut) {
            *errorOut = TStringBuilder() << "status=" << StatusToString(response.operation().status())
                << " issues=[" << FormatOperationIssues(response.operation()) << "]";
        }
        return false;
    }
    return true;
}

bool DoReadValue(Ydb::KeyValue::V1::KeyValueService::Stub& stub, const TString& database, const TString& path, ui64 partitionId, const TString& key, TString* errorOut = nullptr, const TString& traceparent = {}) {
    Ydb::KeyValue::ReadRequest request;
    request.set_path(path);
    request.set_partition_id(partitionId);
    request.set_key(key);
    request.set_offset(0);
    request.set_size(0);

    Ydb::KeyValue::ReadResponse response;
    grpc::ClientContext context;
    AdjustContext(context, database, traceparent);

    grpc::Status status = stub.Read(&context, request, &response);
    if (!status.ok()) {
        if (errorOut) {
            *errorOut = TStringBuilder() << "gRPC error: " << status.error_message()
                << " (code=" << static_cast<int>(status.error_code()) << ")";
        }
        return false;
    }
    if (response.operation().status() != Ydb::StatusIds::SUCCESS) {
        if (errorOut) {
            *errorOut = TStringBuilder() << "status=" << StatusToString(response.operation().status())
                << " issues=[" << FormatOperationIssues(response.operation()) << "]";
        }
        return false;
    }
    return true;
}

ui64 Percentile(TVector<ui64>& values, double q) {
    if (values.empty()) {
        return 0;
    }
    std::sort(values.begin(), values.end());
    const size_t index = Min(values.size() - 1, static_cast<size_t>(q * (values.size() - 1)));
    return values[index];
}

struct TOptions {
    TString Command;
    TString Endpoint;
    TString Database;
    TString Path;
    TString TokenFile;
    ui32 PartitionCount = 0;
    ui64 PartitionId = 0;
    bool PartitionIdSpecified = false;
    TVector<TString> ChannelMedia;
    TString Key;
    TString Value;
    TString FilePath;
    ui64 ReadOffset = 0;
    ui64 ReadSize = 0;
    ui32 StorageChannel = 0;
    TVector<ui32> S3Channels;
    ui32 LocalChannel = 0;
    ui32 S3Threads = 3;
    ui32 LocalThreads = 1;
    TString S3ErrorFile = "s3_errors.log";
    ui32 Seconds = 10;
    ui32 Threads = 1;
    ui32 ReadThreads = 0;
    ui32 ValueSize = 1024;
    ui32 ReadPercent = 100;
    ui32 DeletePercent = 0;
    ui32 ReportPeriod = 1;
    ui64 WriteRateMibps = 0;
    ui64 KeyCount = 0;
    ui32 BatchSize = 1;
    ui32 TraceEveryN = 0;
    bool Verbose = false;
};

int LoadVolume(const TOptions& options, const TVector<TString>& endpoints, const TString& path, bool useTls) {
    if (options.PartitionCount == 0) {
        Cerr << "--partition-count must be specified for load command" << Endl;
        return 1;
    }
    if (options.Threads == 0) {
        Cerr << "--threads must be greater than zero" << Endl;
        return 1;
    }
    const ui32 reportPeriod = options.ReportPeriod == 0 ? 1 : options.ReportPeriod;
    if (options.ReadPercent > 100) {
        Cerr << "--read-percent must be in range [0, 100]" << Endl;
        return 1;
    }

    const bool dedicatedReaders = options.ReadThreads > 0;
    const ui32 inlineReadPercent = dedicatedReaders ? 0 : options.ReadPercent;
    const ui64 targetWriteBytesPerSec = options.WriteRateMibps * 1_MB;

    if (options.WriteRateMibps > 0 && options.ValueSize < MinValueSizeForRateLimit) {
        Cerr << "WARNING: --value-size is small for a high write rate; "
             << "consider --value-size " << 1_MB << " (1 MiB) to reach 4 GiB/s" << Endl;
    }

    std::atomic<bool> stop{false};
    std::atomic<ui64> totalWriteOps{0};
    std::atomic<ui64> totalWriteBytes{0};
    std::atomic<ui64> totalReadOps{0};
    std::atomic<ui64> totalReadBytes{0};
    std::atomic<ui64> totalErrors{0};
    std::atomic<ui64> windowWriteOps{0};
    std::atomic<ui64> windowWriteBytes{0};
    std::atomic<ui64> windowReadOps{0};
    std::atomic<ui64> windowReadBytes{0};
    std::atomic<ui64> windowErrors{0};
    std::atomic<ui64> windowWriteErrors{0};
    std::atomic<ui64> windowReadErrors{0};

    std::mutex latencyMutex;
    TVector<ui64> windowWriteLatenciesMs;
    TVector<ui64> windowReadLatenciesMs;

    std::mutex errorMutex;
    std::atomic<ui64> loggedErrors{0};
    constexpr ui64 MaxLoggedErrors = 50;

    // every N-th RPC carries a traceparent header, so the server samples
    // exactly that request; the trace id is logged together with the observed latency
    std::atomic<ui64> writeTraceCounter{0};
    std::atomic<ui64> readTraceCounter{0};

    std::unique_ptr<std::atomic<ui64>[]> writtenPerThread(new std::atomic<ui64>[options.Threads]);
    for (ui32 i = 0; i < options.Threads; ++i) {
        writtenPerThread[i].store(0, std::memory_order_relaxed);
    }

    const auto started = std::chrono::steady_clock::now();
    const auto deadline = options.Seconds == 0
        ? std::chrono::steady_clock::time_point::max()
        : started + std::chrono::seconds(options.Seconds);
    const TString value(options.ValueSize, 'x');
    const ui64 runId = static_cast<ui64>(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
    TByteRateLimiter writeLimiter(targetWriteBytesPerSec);

    LoadStopFlag = &stop;
    const auto prevInt = std::signal(SIGINT, HandleLoadSignal);
    const auto prevTerm = std::signal(SIGTERM, HandleLoadSignal);

    auto stillRunning = [&] {
        return !stop.load(std::memory_order_relaxed) && std::chrono::steady_clock::now() < deadline;
    };

    auto logError = [&](const char* op, ui64 partitionId, const TString& key, ui64 latencyMs, const TString& error) {
        const ui64 n = loggedErrors.fetch_add(1, std::memory_order_relaxed);
        if (n >= MaxLoggedErrors) {
            return;
        }
        std::lock_guard<std::mutex> guard(errorMutex);
        Cerr << "ERROR\t" << op
             << "\tchannel=" << options.StorageChannel
             << "\tpartition=" << partitionId
             << "\tkey=" << key
             << "\tlatency_ms=" << latencyMs
             << "\t" << error
             << Endl;
        Cerr.Flush();
    };

    auto makeTraceparent = [&](std::atomic<ui64>& counter) -> TTraceContext {
        if (options.TraceEveryN == 0) {
            return {};
        }
        if (counter.fetch_add(1, std::memory_order_relaxed) % options.TraceEveryN != 0) {
            return {};
        }
        return MakeTraceparent();
    };

    auto logTrace = [&](const char* op, ui64 partitionId, ui64 latencyMs, bool ok, const TString& traceId) {
        std::lock_guard<std::mutex> guard(errorMutex);
        Cerr << "TRACE\t" << op
             << "\tchannel=" << options.StorageChannel
             << "\tpartition=" << partitionId
             << "\tlatency_ms=" << latencyMs
             << "\tok=" << (ok ? 1 : 0)
             << "\ttrace_id=" << traceId
             << Endl;
        Cerr.Flush();
    };

    auto record = [&](bool ok, ui64 latencyMs, bool isWrite) {
        if (isWrite) {
            totalWriteOps.fetch_add(1, std::memory_order_relaxed);
            windowWriteOps.fetch_add(1, std::memory_order_relaxed);
            if (ok) {
                totalWriteBytes.fetch_add(options.ValueSize, std::memory_order_relaxed);
                windowWriteBytes.fetch_add(options.ValueSize, std::memory_order_relaxed);
            } else {
                windowWriteErrors.fetch_add(1, std::memory_order_relaxed);
            }
        } else {
            totalReadOps.fetch_add(1, std::memory_order_relaxed);
            windowReadOps.fetch_add(1, std::memory_order_relaxed);
            if (ok) {
                totalReadBytes.fetch_add(options.ValueSize, std::memory_order_relaxed);
                windowReadBytes.fetch_add(options.ValueSize, std::memory_order_relaxed);
            } else {
                windowReadErrors.fetch_add(1, std::memory_order_relaxed);
            }
        }
        if (!ok) {
            totalErrors.fetch_add(1, std::memory_order_relaxed);
            windowErrors.fetch_add(1, std::memory_order_relaxed);
        }
        {
            std::lock_guard<std::mutex> guard(latencyMutex);
            if (isWrite) {
                windowWriteLatenciesMs.push_back(latencyMs);
            } else {
                windowReadLatenciesMs.push_back(latencyMs);
            }
        }
    };

    Cout << "Load started:" << Endl;
    Cout << "  endpoints: " << endpoints.size() << Endl;
    for (const auto& ep : endpoints) {
        Cout << "    " << ep << Endl;
    }
    Cout << "  storage channel: " << options.StorageChannel << Endl;
    Cout << "  write threads: " << options.Threads << Endl;
    Cout << "  read threads: " << options.ReadThreads
         << (dedicatedReaders ? " (dedicated)" : " (inline via --read-percent)") << Endl;
    if (!dedicatedReaders) {
        Cout << "  read-percent: " << inlineReadPercent << Endl;
    }
    Cout << "  batch-size: " << options.BatchSize << " keys per RPC" << Endl;
    Cout << "  value-size: " << options.ValueSize << " bytes" << Endl;
    Cout << "  tracing: ";
    if (options.TraceEveryN == 0) {
        Cout << "off";
    } else {
        Cout << "every " << options.TraceEveryN << "-th RPC (traceparent header, see TRACE lines in stderr)";
    }
    Cout << Endl;
    Cout << "  write-rate: ";
    if (options.WriteRateMibps == 0) {
        Cout << "unlimited";
    } else {
        Cout << options.WriteRateMibps << " MiB/s";
    }
    Cout << Endl;
    Cout << "  key-count per write thread: ";
    if (options.KeyCount == 0) {
        Cout << "unique (no overwrite)";
    } else {
        Cout << options.KeyCount << " (overwrite working set)";
    }
    Cout << Endl;
    Cout << "  duration: ";
    if (options.Seconds == 0) {
        Cout << "until SIGINT/SIGTERM";
    } else {
        Cout << options.Seconds << " seconds";
    }
    Cout << Endl;
    Cout << Endl;

    TVector<std::thread> workers;
    workers.reserve(options.Threads + options.ReadThreads);

    for (ui32 t = 0; t < options.Threads; ++t) {
        workers.emplace_back([&, t] {
            const auto& ep = endpoints[t % endpoints.size()];
            auto channel = MakeGrpcChannel(ep, useTls, t + 1);
            auto stub = Ydb::KeyValue::V1::KeyValueService::NewStub(channel);
            std::mt19937 rng(static_cast<unsigned>(runId + t));
            std::uniform_int_distribution<int> percent(1, 100);

            while (stillRunning()) {
                const ui64 batchBytes = static_cast<ui64>(options.ValueSize) * options.BatchSize;
                writeLimiter.Acquire(batchBytes);

                const ui64 written = writtenPerThread[t].load(std::memory_order_relaxed);
                const ui64 baseKeyIndex = (options.KeyCount > 0) ? (written % options.KeyCount) : written;
                const ui64 partitionId = (t + baseKeyIndex) % options.PartitionCount;

                TVector<TString> keys;
                keys.reserve(options.BatchSize);
                for (ui32 b = 0; b < options.BatchSize; ++b) {
                    const ui64 ki = (options.KeyCount > 0) ? ((written + b) % options.KeyCount) : (written + b);
                    keys.push_back(TStringBuilder() << "load_" << runId << "_" << t << "_" << ki);
                }

                TString error;
                const TTraceContext trace = makeTraceparent(writeTraceCounter);
                auto begin = std::chrono::steady_clock::now();
                bool ok = (options.BatchSize == 1)
                    ? DoWriteValue(*stub, options.Database, path, partitionId, keys[0], value, options.StorageChannel, &error, trace.Traceparent)
                    : DoWriteBatch(*stub, options.Database, path, partitionId, keys, value, options.StorageChannel, &error, trace.Traceparent);
                auto end = std::chrono::steady_clock::now();
                const ui64 latencyMs = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();

                for (ui32 b = 0; b < options.BatchSize; ++b) {
                    record(ok, latencyMs, true);
                }
                if (ok) {
                    writtenPerThread[t].fetch_add(options.BatchSize, std::memory_order_relaxed);
                } else {
                    logError("write", partitionId, keys[0], latencyMs, error);
                }
                if (!trace.TraceId.empty()) {
                    logTrace("write", partitionId, latencyMs, ok, trace.TraceId);
                }

                if (ok && inlineReadPercent > 0 && static_cast<ui32>(percent(rng)) <= inlineReadPercent) {
                    begin = std::chrono::steady_clock::now();
                    ok = DoReadValue(*stub, options.Database, path, partitionId, keys[0]);
                    end = std::chrono::steady_clock::now();
                    record(ok, std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count(), false);
                }
            }
        });
    }

    for (ui32 t = 0; t < options.ReadThreads; ++t) {
        workers.emplace_back([&, t] {
            const auto& ep = endpoints[(options.Threads + t) % endpoints.size()];
            auto channel = MakeGrpcChannel(ep, useTls, options.Threads + t + 1);
            auto stub = Ydb::KeyValue::V1::KeyValueService::NewStub(channel);
            std::mt19937 rng(static_cast<unsigned>(runId + 100000 + t));
            std::uniform_int_distribution<ui32> threadDist(0, options.Threads - 1);

            while (stillRunning()) {
                const ui32 writer = threadDist(rng);
                const ui64 written = writtenPerThread[writer].load(std::memory_order_relaxed);
                if (written == 0) {
                    std::this_thread::sleep_for(1ms);
                    continue;
                }
                const ui64 usable = (options.KeyCount > 0) ? Min(written, options.KeyCount) : written;
                std::uniform_int_distribution<ui64> keyDist(0, usable - 1);
                const ui64 keyIndex = keyDist(rng);
                const ui64 partitionId = (writer + keyIndex) % options.PartitionCount;
                const TString key = TStringBuilder() << "load_" << runId << "_" << writer << "_" << keyIndex;

                TString error;
                const TTraceContext trace = makeTraceparent(readTraceCounter);
                auto begin = std::chrono::steady_clock::now();
                const bool ok = DoReadValue(*stub, options.Database, path, partitionId, key, &error, trace.Traceparent);
                auto end = std::chrono::steady_clock::now();
                const ui64 latencyMs = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();
                record(ok, latencyMs, false);
                if (!ok) {
                    logError("read", partitionId, key, latencyMs, error);
                }
                if (!trace.TraceId.empty()) {
                    logTrace("read", partitionId, latencyMs, ok, trace.TraceId);
                }
            }
        });
    }

    Cout << "Window\tWOps\tWMiB/s\tWErr\tROps\tRMiB/s\tRErr\tWp50\tWp95\tWp99\tRp50\tRp95\tRp99" << Endl;

    ui32 window = 0;
    while (stillRunning()) {
        std::this_thread::sleep_for(std::chrono::seconds(reportPeriod));
        if (!stillRunning()) {
            break;
        }
        ++window;

        const ui64 writeOps = windowWriteOps.exchange(0, std::memory_order_relaxed);
        const ui64 writeBytes = windowWriteBytes.exchange(0, std::memory_order_relaxed);
        const ui64 writeErrors = windowWriteErrors.exchange(0, std::memory_order_relaxed);
        const ui64 readOps = windowReadOps.exchange(0, std::memory_order_relaxed);
        const ui64 readBytes = windowReadBytes.exchange(0, std::memory_order_relaxed);
        const ui64 readErrors = windowReadErrors.exchange(0, std::memory_order_relaxed);
        windowErrors.exchange(0, std::memory_order_relaxed);

        TVector<ui64> writeLatencies;
        TVector<ui64> readLatencies;
        {
            std::lock_guard<std::mutex> guard(latencyMutex);
            writeLatencies.swap(windowWriteLatenciesMs);
            readLatencies.swap(windowReadLatenciesMs);
        }

        auto stats = [](TVector<ui64>& latencies) {
            ui64 p50 = 0;
            ui64 p95 = 0;
            ui64 p99 = 0;
            if (!latencies.empty()) {
                p50 = Percentile(latencies, 0.50);
                p95 = Percentile(latencies, 0.95);
                p99 = Percentile(latencies, 0.99);
            }
            return std::tuple<ui64, ui64, ui64>{p50, p95, p99};
        };
        const auto [wp50, wp95, wp99] = stats(writeLatencies);
        const auto [rp50, rp95, rp99] = stats(readLatencies);

        Cout << window
             << "\t" << writeOps
             << "\t" << ToMiBps(writeBytes, reportPeriod)
             << "\t" << writeErrors
             << "\t" << readOps
             << "\t" << ToMiBps(readBytes, reportPeriod)
             << "\t" << readErrors
             << "\t" << wp50
             << "\t" << wp95
             << "\t" << wp99
             << "\t" << rp50
             << "\t" << rp95
             << "\t" << rp99
             << Endl;
        Cout.Flush();
    }

    stop.store(true, std::memory_order_relaxed);
    for (auto& worker : workers) {
        worker.join();
    }

    std::signal(SIGINT, prevInt);
    std::signal(SIGTERM, prevTerm);
    LoadStopFlag = nullptr;

    const auto finished = std::chrono::steady_clock::now();
    const double elapsed = std::chrono::duration<double>(finished - started).count();

    Cout << Endl;
    Cout << "Total\tSec\tWOps\tWMiB/s\tROps\tRMiB/s\tErrors" << Endl;
    Cout << static_cast<ui64>(elapsed)
         << "\t" << elapsed
         << "\t" << totalWriteOps.load(std::memory_order_relaxed)
         << "\t" << ToMiBps(totalWriteBytes.load(std::memory_order_relaxed), elapsed)
         << "\t" << totalReadOps.load(std::memory_order_relaxed)
         << "\t" << ToMiBps(totalReadBytes.load(std::memory_order_relaxed), elapsed)
         << "\t" << totalErrors.load(std::memory_order_relaxed)
         << Endl;
    Cout.Flush();

    return totalErrors.load(std::memory_order_relaxed) == 0 ? 0 : 2;
}

int LoadVolumeChannels(const TOptions& options, const TString& endpoint, const TString& path, bool useTls) {
    if (options.PartitionCount == 0) {
        Cerr << "--partition-count must be specified for load-channels command" << Endl;
        return 1;
    }
    if (options.S3Threads == 0 && options.LocalThreads == 0) {
        Cerr << "at least one of --s3-threads or --local-threads must be greater than zero" << Endl;
        return 1;
    }
    if (options.S3Threads > 0 && options.S3Channels.empty()) {
        Cerr << "--s3-channel must be specified when --s3-threads > 0" << Endl;
        return 1;
    }
    if (options.Seconds == 0) {
        Cerr << "--seconds must be greater than zero" << Endl;
        return 1;
    }
    const ui32 reportPeriod = options.ReportPeriod == 0 ? 1 : options.ReportPeriod;
    if (options.ReadPercent > 100) {
        Cerr << "--read-percent must be in range [0, 100]" << Endl;
        return 1;
    }

    const ui32 totalThreads = options.S3Threads + options.LocalThreads;

    Cout << "Multi-channel load started:" << Endl;
    Cout << "  S3 channels:";
    if (options.S3Channels.empty()) {
        Cout << " <none>";
    } else {
        for (ui32 channel : options.S3Channels) {
            Cout << " " << channel;
        }
    }
    Cout << ", threads: " << options.S3Threads << Endl;
    Cout << "  Local channel: " << options.LocalChannel << ", threads: " << options.LocalThreads << Endl;
    Cout << "  Total threads: " << totalThreads << Endl;
    Cout << "  S3 error file: " << options.S3ErrorFile << Endl;
    Cout << Endl;

    std::atomic<bool> stop{false};
    std::atomic<ui64> totalOps{0};
    std::atomic<ui64> totalErrors{0};
    std::atomic<ui64> s3Ops{0};
    std::atomic<ui64> s3Errors{0};
    std::atomic<ui64> localOps{0};
    std::atomic<ui64> localErrors{0};
    std::atomic<ui64> windowOps{0};
    std::atomic<ui64> windowErrors{0};
    std::atomic<ui64> windowS3Ops{0};
    std::atomic<ui64> windowS3Errors{0};
    std::atomic<ui64> windowLocalOps{0};
    std::atomic<ui64> windowLocalErrors{0};

    std::mutex latencyMutex;
    TVector<ui64> windowLatenciesMs;

    std::mutex s3ErrorMutex;
    TFileOutput s3ErrorOut(options.S3ErrorFile);
    s3ErrorOut << "ts_ms\top\tchannel\tpartition\tkey\tlatency_ms\terror" << Endl;

    const auto started = std::chrono::steady_clock::now();
    const auto deadline = started + std::chrono::seconds(options.Seconds);
    const TString value(options.ValueSize, 'x');
    const ui64 runId = static_cast<ui64>(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());

    auto logS3Error = [&](const char* op, ui32 channel, ui64 partitionId, const TString& key, ui64 latencyMs, const TString& error) {
        const ui64 tsMs = static_cast<ui64>(std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
        std::lock_guard<std::mutex> guard(s3ErrorMutex);
        s3ErrorOut << tsMs
                   << "\t" << op
                   << "\t" << channel
                   << "\t" << partitionId
                   << "\t" << key
                   << "\t" << latencyMs
                   << "\t" << error
                   << Endl;
        s3ErrorOut.Flush();
    };

    auto record = [&](bool ok, ui64 latencyMs, bool isS3) {
        totalOps.fetch_add(1, std::memory_order_relaxed);
        windowOps.fetch_add(1, std::memory_order_relaxed);

        auto& typeOps = isS3 ? s3Ops : localOps;
        auto& typeWindowOps = isS3 ? windowS3Ops : windowLocalOps;
        std::atomic<ui64>& typeErrors = isS3 ? s3Errors : localErrors;
        std::atomic<ui64>& typeWindowErrors = isS3 ? windowS3Errors : windowLocalErrors;

        typeOps.fetch_add(1, std::memory_order_relaxed);
        typeWindowOps.fetch_add(1, std::memory_order_relaxed);
        if (!ok) {
            typeErrors.fetch_add(1, std::memory_order_relaxed);
            typeWindowErrors.fetch_add(1, std::memory_order_relaxed);
            totalErrors.fetch_add(1, std::memory_order_relaxed);
            windowErrors.fetch_add(1, std::memory_order_relaxed);
        }
        {
            std::lock_guard<std::mutex> guard(latencyMutex);
            windowLatenciesMs.push_back(latencyMs);
        }
    };

    struct TWorkerConfig {
        ui32 ThreadIndex = 0;
        ui32 StorageChannel = 0;
        TString KeyPrefix;
        bool IsS3 = false;
    };

    TVector<TWorkerConfig> workerConfigs;
    workerConfigs.reserve(totalThreads);

    for (ui32 t = 0; t < options.S3Threads; ++t) {
        TWorkerConfig config;
        config.ThreadIndex = t;
        config.StorageChannel = options.S3Channels[t % options.S3Channels.size()];
        config.KeyPrefix = TStringBuilder() << "s3_load_" << runId << "_" << t;
        config.IsS3 = true;
        workerConfigs.push_back(std::move(config));
    }

    for (ui32 t = 0; t < options.LocalThreads; ++t) {
        TWorkerConfig config;
        config.ThreadIndex = options.S3Threads + t;
        config.StorageChannel = options.LocalChannel;
        config.KeyPrefix = TStringBuilder() << "local_load_" << runId << "_" << t;
        config.IsS3 = false;
        workerConfigs.push_back(std::move(config));
    }

    TVector<std::thread> workers;
    workers.reserve(totalThreads);

    for (const auto& config : workerConfigs) {
        workers.emplace_back([&, config] {
            auto channel = grpc::CreateChannel(endpoint, MakeChannelCredentials(useTls));
            auto stub = Ydb::KeyValue::V1::KeyValueService::NewStub(channel);
            std::mt19937 rng(static_cast<unsigned>(runId + config.ThreadIndex));
            std::uniform_int_distribution<int> percent(1, 100);

            ui64 i = 0;
            while (!stop.load(std::memory_order_relaxed) && std::chrono::steady_clock::now() < deadline) {
                const ui64 partitionId = (config.ThreadIndex + i) % options.PartitionCount;
                const TString key = TStringBuilder() << config.KeyPrefix << "_" << i;

                TString error;
                auto begin = std::chrono::steady_clock::now();
                bool ok = DoWriteValue(*stub, options.Database, path, partitionId, key, value, config.StorageChannel, config.IsS3 ? &error : nullptr);
                auto end = std::chrono::steady_clock::now();
                const ui64 latencyMs = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();
                record(ok, latencyMs, config.IsS3);
                if (!ok && config.IsS3) {
                    logS3Error("write", config.StorageChannel, partitionId, key, latencyMs, error);
                }

                if (ok && options.ReadPercent > 0 && static_cast<ui32>(percent(rng)) <= options.ReadPercent) {
                    error.clear();
                    begin = std::chrono::steady_clock::now();
                    ok = DoReadValue(*stub, options.Database, path, partitionId, key, config.IsS3 ? &error : nullptr);
                    end = std::chrono::steady_clock::now();
                    const ui64 readLatencyMs = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();
                    record(ok, readLatencyMs, config.IsS3);
                    if (!ok && config.IsS3) {
                        logS3Error("read", config.StorageChannel, partitionId, key, readLatencyMs, error);
                    }
                }

                ++i;
            }
        });
    }

    Cout << "Window\tOps\tOps/Sec\tErrors\tS3Ops\tS3Err\tLocalOps\tLocalErr\tp50(ms)\tp95(ms)\tp99(ms)\tpMax(ms)" << Endl;

    ui32 window = 0;
    while (std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::seconds(reportPeriod));
        ++window;

        const ui64 ops = windowOps.exchange(0, std::memory_order_relaxed);
        const ui64 errors = windowErrors.exchange(0, std::memory_order_relaxed);
        const ui64 s3WindowOps = windowS3Ops.exchange(0, std::memory_order_relaxed);
        const ui64 s3WindowErrors = windowS3Errors.exchange(0, std::memory_order_relaxed);
        const ui64 localWindowOps = windowLocalOps.exchange(0, std::memory_order_relaxed);
        const ui64 localWindowErrors = windowLocalErrors.exchange(0, std::memory_order_relaxed);

        TVector<ui64> latencies;
        {
            std::lock_guard<std::mutex> guard(latencyMutex);
            latencies.swap(windowLatenciesMs);
        }

        ui64 p50 = 0;
        ui64 p95 = 0;
        ui64 p99 = 0;
        ui64 pMax = 0;
        if (!latencies.empty()) {
            p50 = Percentile(latencies, 0.50);
            p95 = Percentile(latencies, 0.95);
            p99 = Percentile(latencies, 0.99);
            pMax = *std::max_element(latencies.begin(), latencies.end());
        }

        Cout << window
             << "\t" << ops
             << "\t" << (static_cast<double>(ops) / reportPeriod)
             << "\t" << errors
             << "\t" << s3WindowOps
             << "\t" << s3WindowErrors
             << "\t" << localWindowOps
             << "\t" << localWindowErrors
             << "\t" << p50
             << "\t" << p95
             << "\t" << p99
             << "\t" << pMax
             << Endl;
    }

    stop.store(true, std::memory_order_relaxed);
    for (auto& worker : workers) {
        worker.join();
    }

    const auto finished = std::chrono::steady_clock::now();
    const double elapsed = std::chrono::duration<double>(finished - started).count();

    Cout << Endl;
    Cout << "Total\tOps\tOps/Sec\tErrors\tS3Ops\tS3Err\tLocalOps\tLocalErr" << Endl;
    Cout << static_cast<ui64>(elapsed)
         << "\t" << totalOps.load(std::memory_order_relaxed)
         << "\t" << (static_cast<double>(totalOps.load(std::memory_order_relaxed)) / elapsed)
         << "\t" << totalErrors.load(std::memory_order_relaxed)
         << "\t" << s3Ops.load(std::memory_order_relaxed)
         << "\t" << s3Errors.load(std::memory_order_relaxed)
         << "\t" << localOps.load(std::memory_order_relaxed)
         << "\t" << localErrors.load(std::memory_order_relaxed)
         << Endl;

    if (s3Errors.load(std::memory_order_relaxed) > 0) {
        Cout << "S3 errors written to: " << options.S3ErrorFile << Endl;
    }

    return totalErrors.load(std::memory_order_relaxed) == 0 ? 0 : 2;
}

TOptions ParseOptions(int argc, char** argv) {
    TOptions options;

    NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
    opts.SetTitle("KeyValue volume management tool");
    opts.SetFreeArgsNum(1);
    opts.SetFreeArgTitle(0, "COMMAND", "Command to execute: create, describe, alter, remove, read, write, upload, download, load or load-channels");

    opts.AddLongOption('e', "endpoint", "YDB endpoint (e.g., grpc://localhost:2135)")
        .Required()
        .StoreResult(&options.Endpoint);

    opts.AddLongOption('d', "database", "Database path")
        .StoreResult(&options.Database)
        .DefaultValue("");

    opts.AddLongOption('p', "path", "Volume path (relative to database)")
        .Required()
        .StoreResult(&options.Path);

    opts.AddLongOption("partition-count", "Partition count (for create command) or new partition count (for alter command)")
        .StoreResult(&options.PartitionCount)
        .DefaultValue("0");

    opts.AddLongOption("partition-id", "Partition ID (for read/write commands)")
        .StoreResult(&options.PartitionId)
        .DefaultValue("0")
        .Handler0([&options] { options.PartitionIdSpecified = true; });

    opts.AddLongOption("channel-media", "Storage channel media types (for create/alter commands, can be specified multiple times)")
        .AppendTo(&options.ChannelMedia);

    opts.AddLongOption("key", "Key to read or write")
        .StoreResult(&options.Key);

    opts.AddLongOption("value", "Value to write")
        .StoreResult(&options.Value);

    opts.AddLongOption("file", "Path to local file (source for upload, destination for download)")
        .StoreResult(&options.FilePath);

    opts.AddLongOption("read-offset", "Offset in bytes for read operation (default: 0)")
        .StoreResult(&options.ReadOffset)
        .DefaultValue("0");

    opts.AddLongOption("read-size", "Size in bytes for read operation (0 means read to end)")
        .StoreResult(&options.ReadSize)
        .DefaultValue("0");

    opts.AddLongOption("storage-channel", "Storage channel for write/load operation (default: 0)")
        .StoreResult(&options.StorageChannel)
        .DefaultValue("0");

    opts.AddLongOption("s3-channel", "S3 storage channel ID (for load-channels command, can be specified multiple times)")
        .AppendTo(&options.S3Channels);

    opts.AddLongOption("local-channel", "Local storage channel ID (for load-channels command, default: 0)")
        .StoreResult(&options.LocalChannel)
        .DefaultValue("0");

    opts.AddLongOption("s3-threads", "Number of worker threads writing to S3 channels (for load-channels command, default: 3)")
        .StoreResult(&options.S3Threads)
        .DefaultValue("3");

    opts.AddLongOption("local-threads", "Number of worker threads writing to local channel (for load-channels command, default: 1)")
        .StoreResult(&options.LocalThreads)
        .DefaultValue("1");

    opts.AddLongOption("s3-error-file", "File to write S3 channel errors (for load-channels command, default: s3_errors.log)")
        .StoreResult(&options.S3ErrorFile)
        .DefaultValue("s3_errors.log");

    opts.AddLongOption("seconds", "Load duration in seconds (for load/load-channels commands, 0 = until SIGINT/SIGTERM)")
        .StoreResult(&options.Seconds)
        .DefaultValue("10");

    opts.AddLongOption("threads", "Number of write worker threads (for load command)")
        .StoreResult(&options.Threads)
        .DefaultValue("1");

    opts.AddLongOption("read-threads", "Number of dedicated reader threads (for load command, default: 0). When > 0, writers only write")
        .StoreResult(&options.ReadThreads)
        .DefaultValue("0");

    opts.AddLongOption("write-rate-mibps", "Target write rate in MiB/s (for load command, 0 = unlimited). Use 4096 for 4 GiB/s")
        .StoreResult(&options.WriteRateMibps)
        .DefaultValue("0");

    opts.AddLongOption("key-count", "Distinct keys per write thread (for load command, 0 = unique keys). Reuse/overwrite when > 0")
        .StoreResult(&options.KeyCount)
        .DefaultValue("0");

    opts.AddLongOption("batch-size", "Number of keys to write per single ExecuteTransaction RPC (for load command, default: 1)")
        .StoreResult(&options.BatchSize)
        .DefaultValue("1");

    opts.AddLongOption("value-size", "Value size in bytes (for load/load-channels commands)")
        .StoreResult(&options.ValueSize)
        .DefaultValue("1024");

    opts.AddLongOption("read-percent", "Percent of successful writes followed by read, 0..100 (for load/load-channels; ignored if --read-threads > 0)")
        .StoreResult(&options.ReadPercent)
        .DefaultValue("100");

    opts.AddLongOption("delete-percent", "Percent of successful write/read cycles followed by delete, 0..100 (for load command)")
        .StoreResult(&options.DeletePercent)
        .DefaultValue("0");

    opts.AddLongOption("trace-every-n", "Send a traceparent header on every N-th RPC and log its trace id with the measured latency (for load command, 0 = off). Requires an external_throttling rule for KeyValue.ExecuteTransaction / KeyValue.Read in the cluster tracing_config")
        .StoreResult(&options.TraceEveryN)
        .DefaultValue("0");

    opts.AddLongOption("report-period", "Report period in seconds (for load/load-channels commands)")
        .StoreResult(&options.ReportPeriod)
        .DefaultValue("1");

    opts.AddLongOption('v', "verbose", "Verbose mode: print request and response protobufs")
        .NoArgument()
        .SetFlag(&options.Verbose);

    opts.AddLongOption("token-file", "Path to file containing auth token")
        .StoreResult(&options.TokenFile);

    opts.SetFreeArgsMax(1);

    NLastGetopt::TOptsParseResult parseResult(&opts, argc, argv);

    if (parseResult.GetFreeArgs().size() != 1) {
        throw std::runtime_error("exactly one command must be specified");
    }

    options.Command = parseResult.GetFreeArgs()[0];

    if (options.Command != "create" && options.Command != "describe" && options.Command != "alter" &&
        options.Command != "remove" && options.Command != "read" && options.Command != "write" &&
        options.Command != "upload" && options.Command != "download" &&
        options.Command != "load" && options.Command != "load-channels") {
        throw std::runtime_error("command must be 'create', 'describe', 'alter', 'remove', 'read', 'write', 'upload', 'download', 'load' or 'load-channels'");
    }

    if (options.Command == "create" && options.PartitionCount == 0) {
        throw std::runtime_error("for create command, --partition-count must be specified");
    }

    if (options.Command == "alter" && options.PartitionCount == 0 && options.ChannelMedia.empty()) {
        throw std::runtime_error("for alter command, at least one of --partition-count or --channel-media must be specified");
    }

    if (options.Command == "read" && !options.PartitionIdSpecified) {
        throw std::runtime_error("for read command, --partition-id must be specified");
    }

    if (options.Command == "read" && options.Key.empty()) {
        throw std::runtime_error("for read command, --key must be specified");
    }

    if (options.Command == "write" && !options.PartitionIdSpecified) {
        throw std::runtime_error("for write command, --partition-id must be specified");
    }

    if (options.Command == "write" && options.Key.empty()) {
        throw std::runtime_error("for write command, --key must be specified");
    }

    if (options.Command == "write" && options.Value.empty()) {
        throw std::runtime_error("for write command, --value must be specified");
    }

    if (options.Command == "upload" && !options.PartitionIdSpecified) {
        throw std::runtime_error("for upload command, --partition-id must be specified");
    }

    if (options.Command == "upload" && options.Key.empty()) {
        throw std::runtime_error("for upload command, --key must be specified");
    }

    if (options.Command == "upload" && options.FilePath.empty()) {
        throw std::runtime_error("for upload command, --file must be specified");
    }

    if (options.Command == "download" && !options.PartitionIdSpecified) {
        throw std::runtime_error("for download command, --partition-id must be specified");
    }

    if (options.Command == "download" && options.Key.empty()) {
        throw std::runtime_error("for download command, --key must be specified");
    }

    if (options.Command == "download" && options.FilePath.empty()) {
        throw std::runtime_error("for download command, --file must be specified");
    }

    if (options.Command == "load" && options.PartitionCount == 0) {
        throw std::runtime_error("for load command, --partition-count must be specified");
    }

    if (options.Command == "load" && options.DeletePercent > 100) {
        throw std::runtime_error("for load command, --delete-percent must be in range [0, 100]");
    }

    if (options.Command == "load-channels" && options.PartitionCount == 0) {
        throw std::runtime_error("for load-channels command, --partition-count must be specified");
    }

    if (options.Command == "load-channels" && options.S3Threads > 0 && options.S3Channels.empty()) {
        throw std::runtime_error("for load-channels command, --s3-channel must be specified when --s3-threads > 0");
    }

    if (options.Command == "load-channels" && options.S3Threads == 0 && options.LocalThreads == 0) {
        throw std::runtime_error("for load-channels command, at least one of --s3-threads or --local-threads must be greater than zero");
    }

    return options;
}

} // namespace

int main(int argc, char** argv) {
    try {
        TOptions options = ParseOptions(argc, argv);

        // Load auth token from file or environment variable
        if (options.TokenFile) {
            TAutoPtr<TMappedFileInput> fileInput(new TMappedFileInput(options.TokenFile));
            AuthToken = Strip(fileInput->ReadAll());
        } else {
            AuthToken = Strip(GetEnv("YDB_TOKEN"));
        }

        VerboseMode = options.Verbose;

        const bool useTls = IsSecureEndpoint(options.Endpoint);
        const TString hostPort = ParseHostPort(options.Endpoint);
        const TString volumePath = MakeVolumePath(options.Database, options.Path);

        TVector<TString> loadEndpoints;
        StringSplitter(options.Endpoint).Split(',').SkipEmpty().Collect(&loadEndpoints);
        std::transform(loadEndpoints.begin(), loadEndpoints.end(), loadEndpoints.begin(), ParseHostPort);
        if (loadEndpoints.empty()) {
            loadEndpoints.push_back(hostPort);
        }

        if (VerboseMode) {
            Cerr << "Endpoint: " << options.Endpoint << Endl;
            Cerr << "Host:Port: " << hostPort << Endl;
            Cerr << "TLS: " << (useTls ? "enabled" : "disabled") << Endl;
            Cerr << "Database: " << options.Database << Endl;
            Cerr << "Path: " << options.Path << Endl;
            Cerr << "Volume path: " << volumePath << Endl;
            Cerr << Endl;
        }

        if (options.Command == "create") {
            return CreateVolume(hostPort, options.Database, volumePath, options.PartitionCount, options.ChannelMedia, useTls);
        } else if (options.Command == "describe") {
            return DescribeVolume(hostPort, options.Database, volumePath, useTls);
        } else if (options.Command == "alter") {
            return AlterVolume(hostPort, options.Database, volumePath, options.PartitionCount, options.ChannelMedia, useTls);
        } else if (options.Command == "remove") {
            return RemoveVolume(hostPort, options.Database, volumePath, useTls);
        } else if (options.Command == "read") {
            return ReadValue(hostPort, options.Database, volumePath, options.PartitionId, options.Key, options.ReadOffset, options.ReadSize, useTls);
        } else if (options.Command == "write") {
            return WriteValue(hostPort, options.Database, volumePath, options.PartitionId, options.Key, options.Value, options.StorageChannel, useTls);
        } else if (options.Command == "upload") {
            return UploadFile(hostPort, options.Database, volumePath, options.PartitionId, options.Key, options.FilePath, options.StorageChannel, useTls);
        } else if (options.Command == "download") {
            return DownloadFile(hostPort, options.Database, volumePath, options.PartitionId, options.Key, options.FilePath, useTls);
        } else if (options.Command == "load") {
            return LoadVolume(options, loadEndpoints, volumePath, useTls);
        } else if (options.Command == "load-channels") {
            return LoadVolumeChannels(options, hostPort, volumePath, useTls);
        }

        Cerr << "Unknown command: " << options.Command << Endl;
        return 1;
    } catch (const std::exception& e) {
        Cerr << "Error: " << e.what() << Endl;
        return 1;
    }
}
