#include <ydb/public/api/grpc/ydb_keyvalue_v1.grpc.pb.h>
#include <ydb/public/api/protos/ydb_keyvalue.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <library/cpp/getopt/last_getopt.h>

#include <util/stream/output.h>
#include <util/string/builder.h>
#include <util/system/env.h>
#include <util/stream/file.h>
#include <util/string/strip.h>

#include <google/protobuf/text_format.h>

#include <grpcpp/create_channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/security/credentials.h>

#include <chrono>
#include <memory>
#include <thread>
#include <atomic>
#include <mutex>
#include <vector>
#include <algorithm>
#include <random>


using namespace std::chrono_literals;

static bool VerboseMode = false;
static TString AuthToken;

namespace {

constexpr auto GrpcDeadline = 30s;
constexpr const char* YdbAuthHeader = "x-ydb-auth-ticket";
constexpr const char* YdbDatabaseHeader = "x-ydb-database";

TString GetAuthToken() {
    return AuthToken;
}

void AdjustContext(grpc::ClientContext& ctx, const TString& database) {
    auto token = GetAuthToken();
    if (!token.empty()) {
        ctx.AddMetadata(YdbAuthHeader, token);
    }
    if (!database.empty()) {
        ctx.AddMetadata(YdbDatabaseHeader, database);
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

TString MakeVolumePath(const TString& database, const TString& path) {
    if (database.empty() || path.StartsWith(database)) {
        return path;
    }
    if (database.back() == '/') {
        return database + path;
    }
    return database + "/" + path;
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


bool DoWriteValue(Ydb::KeyValue::V1::KeyValueService::Stub& stub, const TString& database, const TString& path, ui64 partitionId, const TString& key, const TString& value, ui32 storageChannel) {
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
    AdjustContext(context, database);

    grpc::Status status = stub.ExecuteTransaction(&context, request, &response);
    return status.ok() && response.operation().status() == Ydb::StatusIds::SUCCESS;
}

bool DoReadValue(Ydb::KeyValue::V1::KeyValueService::Stub& stub, const TString& database, const TString& path, ui64 partitionId, const TString& key) {
    Ydb::KeyValue::ReadRequest request;
    request.set_path(path);
    request.set_partition_id(partitionId);
    request.set_key(key);
    request.set_offset(0);
    request.set_size(0);

    Ydb::KeyValue::ReadResponse response;
    grpc::ClientContext context;
    AdjustContext(context, database);

    grpc::Status status = stub.Read(&context, request, &response);
    return status.ok() && response.operation().status() == Ydb::StatusIds::SUCCESS;
}

bool DoDeleteValue(Ydb::KeyValue::V1::KeyValueService::Stub& stub, const TString& database, const TString& path, ui64 partitionId, const TString& key) {
    Ydb::KeyValue::ExecuteTransactionRequest request;
    request.set_path(path);
    request.set_partition_id(partitionId);

    auto* command = request.add_commands();
    auto* deleteRange = command->mutable_delete_range();
    auto* range = deleteRange->mutable_range();
    range->set_from_key_inclusive(key);
    range->set_to_key_inclusive(key);

    Ydb::KeyValue::ExecuteTransactionResponse response;
    grpc::ClientContext context;
    AdjustContext(context, database);

    grpc::Status status = stub.ExecuteTransaction(&context, request, &response);
    return status.ok() && response.operation().status() == Ydb::StatusIds::SUCCESS;
}

ui64 Percentile(TVector<ui64>& values, double q) {
    if (values.empty()) {
        return 0;
    }
    std::sort(values.begin(), values.end());
    const size_t index = Min(values.size() - 1, static_cast<size_t>(q * (values.size() - 1)));
    return values[index];
}

int LoadVolume(
    const TString& endpoint,
    const TString& database,
    const TString& path,
    ui32 partitionCount,
    ui32 storageChannel,
    ui32 seconds,
    ui32 threads,
    ui32 valueSize,
    ui32 readPercent,
    ui32 deletePercent,
    ui32 reportPeriod,
    bool useTls)
{
    if (partitionCount == 0) {
        Cerr << "--partition-count must be specified for load command" << Endl;
        return 1;
    }
    if (threads == 0) {
        Cerr << "--threads must be greater than zero" << Endl;
        return 1;
    }
    if (seconds == 0) {
        Cerr << "--seconds must be greater than zero" << Endl;
        return 1;
    }
    if (reportPeriod == 0) {
        reportPeriod = 1;
    }
    if (readPercent > 100) {
        Cerr << "--read-percent must be in range [0, 100]" << Endl;
        return 1;
    }
    if (deletePercent > 100) {
        Cerr << "--delete-percent must be in range [0, 100]" << Endl;
        return 1;
    }

    std::atomic<bool> stop{false};
    std::atomic<ui64> totalOps{0};
    std::atomic<ui64> totalErrors{0};
    std::atomic<ui64> windowOps{0};
    std::atomic<ui64> windowErrors{0};

    std::mutex latencyMutex;
    TVector<ui64> windowLatenciesMs;

    const auto started = std::chrono::steady_clock::now();
    const auto deadline = started + std::chrono::seconds(seconds);
    const TString value(valueSize, 'x');
    const ui64 runId = static_cast<ui64>(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());

    auto record = [&](bool ok, ui64 latencyMs) {
        totalOps.fetch_add(1, std::memory_order_relaxed);
        windowOps.fetch_add(1, std::memory_order_relaxed);
        if (!ok) {
            totalErrors.fetch_add(1, std::memory_order_relaxed);
            windowErrors.fetch_add(1, std::memory_order_relaxed);
        }
        {
            std::lock_guard<std::mutex> guard(latencyMutex);
            windowLatenciesMs.push_back(latencyMs);
        }
    };

    TVector<std::thread> workers;
    workers.reserve(threads);

    for (ui32 t = 0; t < threads; ++t) {
        workers.emplace_back([&, t] {
            auto channel = grpc::CreateChannel(endpoint, MakeChannelCredentials(useTls));
            auto stub = Ydb::KeyValue::V1::KeyValueService::NewStub(channel);
            std::mt19937 rng(static_cast<unsigned>(runId + t));
            std::uniform_int_distribution<int> percent(1, 100);

            ui64 i = 0;
            while (!stop.load(std::memory_order_relaxed) && std::chrono::steady_clock::now() < deadline) {
                const ui64 partitionId = (t + i) % partitionCount;
                const TString key = TStringBuilder() << "load_" << runId << "_" << t << "_" << i;

                auto begin = std::chrono::steady_clock::now();
                bool ok = DoWriteValue(*stub, database, path, partitionId, key, value, storageChannel);
                auto end = std::chrono::steady_clock::now();
                record(ok, std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count());

                if (ok && readPercent > 0 && static_cast<ui32>(percent(rng)) <= readPercent) {
                    begin = std::chrono::steady_clock::now();
                    ok = DoReadValue(*stub, database, path, partitionId, key);
                    end = std::chrono::steady_clock::now();
                    record(ok, std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count());
                }

                if (ok && deletePercent > 0 && static_cast<ui32>(percent(rng)) <= deletePercent) {
                    begin = std::chrono::steady_clock::now();
                    ok = DoDeleteValue(*stub, database, path, partitionId, key);
                    end = std::chrono::steady_clock::now();
                    record(ok, std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count());
                }

                ++i;
            }
        });
    }

    Cout << "Window\tOps\tOps/Sec\tErrors\tp50(ms)\tp95(ms)\tp99(ms)\tpMax(ms)" << Endl;

    ui32 window = 0;
    while (std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::seconds(reportPeriod));
        ++window;

        const ui64 ops = windowOps.exchange(0, std::memory_order_relaxed);
        const ui64 errors = windowErrors.exchange(0, std::memory_order_relaxed);

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
    Cout << "Total\tOps\tOps/Sec\tErrors" << Endl;
    Cout << static_cast<ui64>(elapsed)
         << "\t" << totalOps.load(std::memory_order_relaxed)
         << "\t" << (static_cast<double>(totalOps.load(std::memory_order_relaxed)) / elapsed)
         << "\t" << totalErrors.load(std::memory_order_relaxed)
         << Endl;

    return totalErrors.load(std::memory_order_relaxed) == 0 ? 0 : 2;
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
    ui64 ReadOffset = 0;
    ui64 ReadSize = 0;
    ui32 StorageChannel = 0;
    ui32 Seconds = 10;
    ui32 Threads = 1;
    ui32 ValueSize = 1024;
    ui32 ReadPercent = 100;
    ui32 DeletePercent = 0;
    ui32 ReportPeriod = 1;
    bool Verbose = false;
};

TOptions ParseOptions(int argc, char** argv) {
    TOptions options;

    NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
    opts.SetTitle("KeyValue volume management tool");
    opts.SetFreeArgsNum(1);
    opts.SetFreeArgTitle(0, "COMMAND", "Command to execute: create, describe, alter, remove, read, write or load");

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

    opts.AddLongOption("read-offset", "Offset in bytes for read operation (default: 0)")
        .StoreResult(&options.ReadOffset)
        .DefaultValue("0");

    opts.AddLongOption("read-size", "Size in bytes for read operation (0 means read to end)")
        .StoreResult(&options.ReadSize)
        .DefaultValue("0");

    opts.AddLongOption("storage-channel", "Storage channel for write/load operation (default: 0)")
        .StoreResult(&options.StorageChannel)
        .DefaultValue("0");

    opts.AddLongOption("seconds", "Load duration in seconds (for load command)")
        .StoreResult(&options.Seconds)
        .DefaultValue("10");

    opts.AddLongOption("threads", "Number of worker threads (for load command)")
        .StoreResult(&options.Threads)
        .DefaultValue("1");

    opts.AddLongOption("value-size", "Value size in bytes (for load command)")
        .StoreResult(&options.ValueSize)
        .DefaultValue("1024");

    opts.AddLongOption("read-percent", "Percent of successful writes followed by read, 0..100 (for load command)")
        .StoreResult(&options.ReadPercent)
        .DefaultValue("100");

    opts.AddLongOption("delete-percent", "Percent of successful write/read cycles followed by delete, 0..100 (for load command)")
        .StoreResult(&options.DeletePercent)
        .DefaultValue("0");

    opts.AddLongOption("report-period", "Report period in seconds (for load command)")
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
        options.Command != "remove" && options.Command != "read" && options.Command != "write" && options.Command != "load") {
        throw std::runtime_error("command must be 'create', 'describe', 'alter', 'remove', 'read', 'write' or 'load'");
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

    if (options.Command == "load" && options.PartitionCount == 0) {
        throw std::runtime_error("for load command, --partition-count must be specified");
    }

    return options;
}

} // namespace

int main(int argc, char** argv) {
    try {
        TOptions options = ParseOptions(argc, argv);

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
        } else if (options.Command == "load") {
            return LoadVolume(hostPort, options.Database, volumePath, options.PartitionCount, options.StorageChannel, options.Seconds, options.Threads, options.ValueSize, options.ReadPercent, options.DeletePercent, options.ReportPeriod, useTls);
        }

        Cerr << "Unknown command: " << options.Command << Endl;
        return 1;
    } catch (const std::exception& e) {
        Cerr << "Error: " << e.what() << Endl;
        return 1;
    }
}
