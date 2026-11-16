#include "otel_log_sink.h"

#include <algorithm>
#include <chrono>
#include <iostream>
#include <sstream>
#include <thread>

namespace NActors::NStructuredLog {

otel_logs_v1::SeverityNumber TOtelLogSink::GetOtelSeverity(NActors::NLog::EPrio priority) {
    switch (priority) {
        case NActors::NLog::EPrio::Emerg:
            return otel_logs_v1::SEVERITY_NUMBER_FATAL;
        case NActors::NLog::EPrio::Alert:
        case NActors::NLog::EPrio::Crit:
            return otel_logs_v1::SEVERITY_NUMBER_ERROR;
        case NActors::NLog::EPrio::Error:
            return otel_logs_v1::SEVERITY_NUMBER_ERROR;
        case NActors::NLog::EPrio::Warn:
            return otel_logs_v1::SEVERITY_NUMBER_WARN;
        case NActors::NLog::EPrio::Notice:
        case NActors::NLog::EPrio::Info:
            return otel_logs_v1::SEVERITY_NUMBER_INFO;
        case NActors::NLog::EPrio::Debug:
            return otel_logs_v1::SEVERITY_NUMBER_DEBUG;
        case NActors::NLog::EPrio::Trace:
            return otel_logs_v1::SEVERITY_NUMBER_TRACE;
    }
    return otel_logs_v1::SEVERITY_NUMBER_UNSPECIFIED;
}

const char* TOtelLogSink::GetOtelSeverityText(NActors::NLog::EPrio priority) {
    switch (priority) {
        case NActors::NLog::EPrio::Emerg:
            return "EMERG";
        case NActors::NLog::EPrio::Alert:
            return "ALERT";
        case NActors::NLog::EPrio::Crit:
            return "CRIT";
        case NActors::NLog::EPrio::Error:
            return "ERROR";
        case NActors::NLog::EPrio::Warn:
            return "WARN";
        case NActors::NLog::EPrio::Notice:
            return "NOTICE";
        case NActors::NLog::EPrio::Info:
            return "INFO";
        case NActors::NLog::EPrio::Debug:
            return "DEBUG";
        case NActors::NLog::EPrio::Trace:
            return "TRACE";
    }
    return "UNSPECIFIED";
}

void TOtelLogSink::FillLogRecord(otel_logs_v1::LogRecord* record, const TLogMessage& message) {
    const uint64_t timeNs = message.Time.NanoSeconds();
    record->set_time_unix_nano(timeNs);
    record->set_observed_time_unix_nano(timeNs);
    record->set_severity_number(GetOtelSeverity(message.Priority));
    record->set_severity_text(GetOtelSeverityText(message.Priority));
    *record->mutable_body() = CreateAnyValue(message.TextMessage);

    auto* attrs = record->mutable_attributes();
    if (message.FileName) {
        TStringStream location;
        location << message.FileName << ":" << message.LineNumber;
        AddAttr(attrs, "location", location.Str());
    }

    AddAttr(attrs, "nodeId", message.NodeId);
    AddAttr(attrs, "component", message.Component);

    message.StructuredMessage.ForEachTyped([&](const std::vector<TKeyName>& key, const auto& value) {

        TStringStream attrName;
        bool first = true;
        for (const auto& part : key) {
            if (!first) {
                attrName << '.';
            }
            first = false;
            attrName << part.ToString();
        }

        AddAttr(attrs, attrName.Str(), value);
    });
}

void TOtelLogSink::FillLogRequest(
    const std::vector<TLogMessage>& messages,
    otel_collector_logs_v1::ExportLogsServiceRequest& request,
    size_t startIndex,
    size_t finishIndex) {
    otel_logs_v1::ResourceLogs* resource_logs = request.add_resource_logs();

    otel_logs_v1::ScopeLogs* scope_logs = resource_logs->add_scope_logs();
    scope_logs->mutable_scope()->set_name(Settings.ScopeName);

    for(size_t i = startIndex; i < finishIndex; i++) {
        FillLogRecord(scope_logs->add_log_records(), messages[i]);
    }
}

TOtelLogSink::TOtelSettings::TOtelSettings() {
    ChannelArgs.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 60'000);
    ChannelArgs.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 5'000);
    ChannelArgs.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS, 100);
    ChannelArgs.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, 100);
}

TOtelLogSink::TOtelLogSink(TOtelSettings settings)
    : Settings(std::move(settings)) {
    Channel = grpc::CreateCustomChannel(
        Settings.CollectorUri.c_str(),
        grpc::InsecureChannelCredentials(),
        Settings.ChannelArgs);
    Stub = otel_collector_logs_v1::LogsService::NewStub(Channel);
}

TOtelLogSink::~TOtelLogSink() = default;

bool TOtelLogSink::IsAlive() const {
    return !Stopped.load() && Stub != nullptr;
}

void TOtelLogSink::SendLogRequest(TRequestSPtr request) {
    if (!Stub) {
        return;
    }

    request->State = TRequest::TState::Sent;
    request->Context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(Settings.RpcTimeout.MilliSeconds()));

    const auto self = shared_from_this();
    Stub->async()->Export(
        &request->Context,
        &request->Request,
        &request->Response,
        [self, request](const grpc::Status& status) {
            self->ProcessLogResponse(request, status);
        });
}

void TOtelLogSink::RetryRequest(TRequestSPtr request, bool alarmExpired) {
    request->RetryAlarm.reset();

    if (!alarmExpired) {
        return;
    }

    request->Context.~ClientContext();
    new (&request->Context) grpc::ClientContext();
    request->Response.Clear();
    SendLogRequest(request);
}

void TOtelLogSink::ScheduleRetryRequest(TRequestSPtr request, TRetryReason reason) {
    (void)reason;

    request->State = TRequest::TState::Pending;

    if (request->RetryAlarm) {
        request->RetryAlarm->Cancel();
        request->RetryAlarm.reset();
    }

    if (!request->LastRetryBackoff) {
        request->LastRetryBackoff = Settings.FirstRetryBackoff;
    } else {
        request->LastRetryBackoff = TDuration::MilliSeconds(
            std::min(request->LastRetryBackoff.value().MilliSeconds() * 2, Settings.LastRetryBackoff.MilliSeconds()));
    }

    const auto deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(request->LastRetryBackoff.value().MilliSeconds());
    const auto self = shared_from_this();
    request->RetryAlarm = std::make_unique<grpc::Alarm>();
    request->RetryAlarm->Set(deadline, [self, request](bool expired) {
        self->RetryRequest(request, expired);
    });
}

void TOtelLogSink::ScheduleFlush() {
    if (!Settings.AutoFlushInterval.has_value()) {
        return;
    }

    const auto self = shared_from_this();

    auto* alarm = new grpc::Alarm();
    grpc::Alarm* expected = nullptr;
    if (!FlushAlarm.compare_exchange_strong(expected, alarm)) {
        delete alarm;
        return;
    }
    const auto t = std::chrono::system_clock::now() + std::chrono::milliseconds(Settings.AutoFlushInterval.value().MilliSeconds());
    alarm->Set(t, [self](bool expired) {
        grpc::Alarm* toDelete = self->FlushAlarm.exchange(nullptr);
        if (toDelete != nullptr) {
            delete toDelete;
        }

        if (!expired) {
            return;
        }
        self->Flush();
    });
}

void TOtelLogSink::RemoveActiveRequest(ui64 requestId) {
    bool shouldAutoFlush;
    {
        std::lock_guard<std::mutex> lock(ActiveRequestsMutex);
        ActiveRequests.erase(requestId);
        shouldAutoFlush = AutoFlushOnCompleteRequest;
    }

    if (shouldAutoFlush) {
        AutoFlushOnCompleteRequest = false;
        Flush();
    }
}

void TOtelLogSink::ProcessLogResponse(TRequestSPtr request, const grpc::Status& status) {
    if (status.ok()) {
        if (request->Response.has_partial_success() &&
            request->Response.partial_success().rejected_log_records() != 0) {
            ScheduleRetryRequest(request, TRetryReason::PartialSuccess);
        } else {
            RemoveActiveRequest(request->Id);
        }
    } else if (status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED) {
        ScheduleRetryRequest(request, TRetryReason::DeadlineExceeded);
    } else if (status.error_code() == grpc::StatusCode::CANCELLED) {
        RemoveActiveRequest(request->Id);
    } else if (status.error_code() == grpc::StatusCode::UNAVAILABLE) {
        ScheduleRetryRequest(request, TRetryReason::Unavailable);
    } else {
        ScheduleRetryRequest(request, TRetryReason::Error);
    }
}

bool TOtelLogSink::Write(const TLogMessage& message) {
    if (!IsAlive()) {
        return false;
    }
    bool shouldFlush = false;
    {
        TMessagesSpinLock::TGuard lock(MessagesSpinLock);
        if (Messages.size() >= Settings.MaxQueuedMessages) {
            return false;
        }
        Messages.push_back(message);
        if (Settings.AutoFlushBatchSize.has_value()) {
            shouldFlush = (Messages.size() >= Settings.AutoFlushBatchSize.value());
        }
    }

    if (shouldFlush) {
        Flush();
    } else if (Settings.AutoFlushInterval.has_value()) {
        ScheduleFlush();
    }
    return true;
}

void TOtelLogSink::Flush() {
    std::vector<TLogMessage> batch;
    {
        TMessagesSpinLock::TGuard lock(MessagesSpinLock);
        batch = std::move(Messages);
    }

    if (batch.empty()) {
        return;
    }

    size_t offset = 0;
    while (offset < batch.size()) {
        // Calc next chunk size
        const size_t rest = batch.size() - offset;
        const size_t count = std::min(rest, static_cast<size_t>(Settings.MaxFlushBatchSize));

        TRequestSPtr logRequest;
        {
            std::lock_guard<std::mutex> lock(ActiveRequestsMutex);
            if (ActiveRequests.size() >= Settings.MaxActiveRequests) {
                // Restore messages from batch to the beginning of the queue
                TMessagesSpinLock::TGuard messagesLock(MessagesSpinLock);
                Messages.insert(
                    Messages.begin(),
                    std::make_move_iterator(batch.begin() + static_cast<std::ptrdiff_t>(offset)),
                    std::make_move_iterator(batch.end()));
                AutoFlushOnCompleteRequest = true;
                break;
            } else {
                logRequest = std::make_shared<TRequest>();
                logRequest->Id = NextRequestId++;
                ActiveRequests[logRequest->Id] = logRequest;
            }
        }

        FillLogRequest(batch, logRequest->Request, offset, offset + count);
        SendLogRequest(logRequest);

        offset += count;
    }

    if (Settings.AutoFlushInterval.has_value()) {
        ScheduleFlush();
    }
}

void TOtelLogSink::Stop() {
    if (!IsAlive()) {
        return ;
    }
    Stopped = true;

    std::vector<TRequestSPtr> active;
    {
        std::lock_guard<std::mutex> lock(ActiveRequestsMutex);
        active.reserve(ActiveRequests.size());
        for (auto& [id, req] : ActiveRequests) {
            active.push_back(req);
        }
    }

    for (const TRequestSPtr& request : active) {
        if (request->RetryAlarm) {
            request->RetryAlarm->Cancel();
            request->RetryAlarm.reset();
        }
    }

    {
        grpc::Alarm* alarm = FlushAlarm.exchange(nullptr);
        if (alarm != nullptr) {
            alarm->Cancel();
            delete alarm;
        }
    }

    Stub.reset();
    Channel.reset();
}

}  // namespace NActors::NStructuredLog
