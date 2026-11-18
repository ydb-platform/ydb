#pragma once

#include "otel_any_value_builder.h"

#include <ydb/library/actors/struct_log/log_sink.h>

#include <opentelemetry/proto/collector/logs/v1/logs_service.grpc.pb.h>
#include <opentelemetry/proto/collector/logs/v1/logs_service.pb.h>
#include <opentelemetry/proto/common/v1/common.pb.h>
#include <opentelemetry/proto/logs/v1/logs.pb.h>

#include <grpcpp/alarm.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/client_context.h>
#include <grpcpp/support/channel_arguments.h>
#include <grpcpp/support/status.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <type_traits>
#include <utility>
#include <vector>

namespace NActors::NStructuredLog {

namespace otel_logs_v1 = ::opentelemetry::proto::logs::v1;
namespace otel_common_v1 = ::opentelemetry::proto::common::v1;
namespace otel_collector_logs_v1 = ::opentelemetry::proto::collector::logs::v1;

/// Sends structured YDB log records to an OpenTelemetry Collector (OTLP logs / gRPC).
class TOtelLogSink : public ILogSink, public std::enable_shared_from_this<TOtelLogSink> {
public:
    struct TOtelSettings {
        TString CollectorUri;
        TString ScopeName = "ydb_structured_log";
        TDuration RpcTimeout = TDuration::Seconds(5);

        ui64 MaxFlushBatchSize = 1000;
        std::optional<ui64> AutoFlushBatchSize;
        std::optional<TDuration> AutoFlushInterval;
        ui16 MaxActiveRequests = 10;
        ui64 MaxQueuedMessages = 10;
        TDuration FirstRetryBackoff = TDuration::MilliSeconds(100);
        TDuration LastRetryBackoff = TDuration::MilliSeconds(5000);

        grpc::ChannelArguments ChannelArgs;

        TOtelSettings();
    };

    explicit TOtelLogSink(TOtelSettings settings);

    ~TOtelLogSink() override;

    bool Write(const TLogMessage& message) override;
    void Stop() override;

private:
    template <typename T>
    static TOtelAnyValue CreateAnyValue(T&& value) {
        using TType = std::decay_t<T>;
        return TOtelAnyValueBuilder<TType>::Build(std::forward<T>(value));
    }

    template <typename TValue>
    static void AddAttr(
        google::protobuf::RepeatedPtrField<otel_common_v1::KeyValue>* attrs,
        const TString& key,
        TValue&& val) {
        auto* kv = attrs->Add();
        kv->set_key(key);
        *kv->mutable_value() = CreateAnyValue(std::forward<TValue>(val));
    }
    static otel_logs_v1::SeverityNumber GetOtelSeverity(NActors::NLog::EPrio priority);
    static const char* GetOtelSeverityText(NActors::NLog::EPrio priority);

    void FillLogRecord(otel_logs_v1::LogRecord* record, const TLogMessage& message);

    void FillLogRequest(
        const std::vector<TLogMessage>& messages,
        otel_collector_logs_v1::ExportLogsServiceRequest& request,
        size_t startIndex,
        size_t finishIndex);

    // Active requests
    struct TRequest {
        ui64 Id{0};
        enum class TState {
            Sent,
            Pending,
        };
        TState State{TState::Sent};
        grpc::ClientContext Context;
        otel_collector_logs_v1::ExportLogsServiceRequest Request;
        otel_collector_logs_v1::ExportLogsServiceResponse Response;
        std::unique_ptr<grpc::Alarm> RetryAlarm;
        std::optional<TDuration> LastRetryBackoff;
    };
    using TRequestSPtr = std::shared_ptr<TRequest>;

    std::unordered_map<ui64, TRequestSPtr> ActiveRequests;
    std::mutex ActiveRequestsMutex;
    ui64 NextRequestId{1};
    bool AutoFlushOnCompleteRequest{false};

    // Messages queue
    struct TMessagesSpinLock {
        std::atomic<bool> Locked{false};

        void Lock() {
            bool expected = false;
            while (!Locked.compare_exchange_weak(
                expected, true, std::memory_order_acquire, std::memory_order_relaxed)) {
                expected = false;
            }
        }

        void Unlock() { Locked.store(false, std::memory_order_release); }

        struct TGuard {
            TMessagesSpinLock& SpinLock;
            explicit TGuard(TMessagesSpinLock& spinLock) : SpinLock(spinLock) { SpinLock.Lock(); }
            ~TGuard() { SpinLock.Unlock(); }
        };
    };

    std::vector<TLogMessage> Messages;
    TMessagesSpinLock MessagesSpinLock;

    void Flush();

    bool IsAlive() const;
    void SendLogRequest(TRequestSPtr request);
    enum class TRetryReason {
        PartialSuccess,
        DeadlineExceeded,
        Unavailable,
        Error
    };
    void ScheduleRetryRequest(TRequestSPtr request, TRetryReason reason);
    void RetryRequest(TRequestSPtr request, bool alarmExpired);
    void RemoveActiveRequest(ui64 requestId);
    void ScheduleFlush();
    void ProcessLogResponse(TRequestSPtr request, const grpc::Status& status);

    const TOtelSettings Settings;

    std::shared_ptr<grpc::Channel> Channel;
    std::unique_ptr<otel_collector_logs_v1::LogsService::Stub> Stub;
    std::atomic<grpc::Alarm*> FlushAlarm{nullptr};
    std::atomic<bool> Stopped{false};
};

}  // namespace NActors::NStructuredLog
