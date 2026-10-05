#include "yql_pq_message_stream_client.h"

#include <ydb/public/sdk/cpp/adapters/issue/issue.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/errors.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

#include <exception>
#include <limits>
#include <unordered_map>

namespace NYql {

namespace {

using namespace NYdb;
using namespace NYdb::NTopic;

NFq::EMessageStreamStatus ToStreamStatus(EStatus status) {
    switch (status) {
        case EStatus::SUCCESS:
            return NFq::EMessageStreamStatus::Success;
        case EStatus::NOT_FOUND:
            return NFq::EMessageStreamStatus::NotFound;
        case EStatus::UNAUTHORIZED:
            return NFq::EMessageStreamStatus::Unauthorized;
        case EStatus::BAD_REQUEST:
            return NFq::EMessageStreamStatus::InvalidArgument;
        case EStatus::SCHEME_ERROR:
            return NFq::EMessageStreamStatus::SchemeError;
        case EStatus::PRECONDITION_FAILED:
            return NFq::EMessageStreamStatus::PreconditionFailed;
        case EStatus::UNAVAILABLE:
            return NFq::EMessageStreamStatus::Unavailable;
        case EStatus::TIMEOUT:
            return NFq::EMessageStreamStatus::Timeout;
        case EStatus::OVERLOADED:
            return NFq::EMessageStreamStatus::Overloaded;
        case EStatus::ABORTED:
            return NFq::EMessageStreamStatus::Aborted;
        case EStatus::CANCELLED:
            return NFq::EMessageStreamStatus::Cancelled;
        case EStatus::UNSUPPORTED:
            return NFq::EMessageStreamStatus::Unsupported;
        case EStatus::UNDETERMINED:
            return NFq::EMessageStreamStatus::Undetermined;
        case EStatus::EXTERNAL_ERROR:
            return NFq::EMessageStreamStatus::External;
        case EStatus::BAD_SESSION:
            return NFq::EMessageStreamStatus::BadSession;
        case EStatus::GENERIC_ERROR:
            return NFq::EMessageStreamStatus::GenericError;
        case EStatus::ALREADY_EXISTS:
            return NFq::EMessageStreamStatus::AlreadyExists;
        case EStatus::SESSION_EXPIRED:
            return NFq::EMessageStreamStatus::SessionExpired;
        case EStatus::SESSION_BUSY:
            return NFq::EMessageStreamStatus::SessionBusy;
        case EStatus::INTERNAL_ERROR:
            return NFq::EMessageStreamStatus::InternalError;
        default:
            // Client transport statuses are not Ydb::StatusIds values.
            return NFq::EMessageStreamStatus::Unknown;
    }
}

template <class TValue>
NFq::TMessageStreamResult<TValue> MakeResult(const TStatus& status, TValue value = {}) {
    using TResult = NFq::TMessageStreamResult<TValue>;
    auto issues = NYdb::NAdapters::ToYqlIssues(status.GetIssues());
    return status.IsSuccess()
        ? TResult::Success(std::move(value), std::move(issues))
        : TResult::Failure(ToStreamStatus(status.GetStatus()), std::move(issues));
}

TReadSessionSettings ToSdkReadSettingsImpl(const TString& stream, const NFq::TMessageStreamReadSessionSettings& settings) {
    settings.Validate();
    if (settings.OffsetResetPolicy != NFq::EMessageStreamOffsetResetPolicy::Earliest) {
        ythrow NFq::TMessageStreamException(NFq::EMessageStreamStatus::Unsupported)
            << "YDB SDK adapter supports only Earliest offset reset policy";
    }
    TTopicReadSettings topic;
    topic.Path(stream);
    for (const auto partitionId : settings.PartitionIds) {
        topic.AppendPartitionIds(partitionId.Value);
    }

    TReadSessionSettings sdk;
    sdk.AppendTopics(std::move(topic));
    if (!settings.Consumer) {
        sdk.WithoutConsumer();
    } else {
        sdk.ConsumerName(*settings.Consumer);
    }
    if (settings.ReadFromWriteTime) {
        sdk.ReadFromTimestamp(*settings.ReadFromWriteTime);
    }
    if (settings.MaxMemoryUsageBytes) {
        sdk.MaxMemoryUsageBytes(settings.MaxMemoryUsageBytes);
    }
    if (settings.TraceId) {
        sdk.TraceId(settings.TraceId);
    }
    if (settings.Retry) {
        const auto& retry = *settings.Retry;
        const bool retryAuthenticationErrors = retry.RetryAuthenticationErrors;
        sdk.RetryPolicy(NYdb::NTopic::IRetryPolicy::GetExponentialBackoffPolicy(
            retry.MinDelay,
            retry.MinLongRetryDelay,
            retry.MaxDelay,
            retry.MaxRetries,
            retry.MaxTime,
            retry.ScaleFactor,
            [retryAuthenticationErrors](EStatus status) {
                if (retryAuthenticationErrors && status == EStatus::CLIENT_UNAUTHENTICATED) {
                    return ERetryErrorClass::LongRetry;
                }
                return GetRetryErrorClass(status);
            }));
    }
    sdk.AutoPartitioningSupport(settings.AutoPartitioningSupport);
    return sdk;
}

} // anonymous namespace

std::optional<Ydb::StatusIds::StatusCode> ToYdbStatus(NFq::EMessageStreamStatus status) {
    using NFq::EMessageStreamStatus;

    switch (status) {
        case EMessageStreamStatus::Success:
            return Ydb::StatusIds::SUCCESS;
        case EMessageStreamStatus::NotFound:
            return Ydb::StatusIds::NOT_FOUND;
        case EMessageStreamStatus::Unauthorized:
            return Ydb::StatusIds::UNAUTHORIZED;
        case EMessageStreamStatus::InvalidArgument:
            return Ydb::StatusIds::BAD_REQUEST;
        case EMessageStreamStatus::Unavailable:
            return Ydb::StatusIds::UNAVAILABLE;
        case EMessageStreamStatus::InternalError:
            return Ydb::StatusIds::INTERNAL_ERROR;
        case EMessageStreamStatus::SchemeError:
            return Ydb::StatusIds::SCHEME_ERROR;
        case EMessageStreamStatus::PreconditionFailed:
            return Ydb::StatusIds::PRECONDITION_FAILED;
        case EMessageStreamStatus::Aborted:
            return Ydb::StatusIds::ABORTED;
        case EMessageStreamStatus::Overloaded:
            return Ydb::StatusIds::OVERLOADED;
        case EMessageStreamStatus::Timeout:
            return Ydb::StatusIds::TIMEOUT;
        case EMessageStreamStatus::Cancelled:
            return Ydb::StatusIds::CANCELLED;
        case EMessageStreamStatus::Unsupported:
            return Ydb::StatusIds::UNSUPPORTED;
        case EMessageStreamStatus::Undetermined:
            return Ydb::StatusIds::UNDETERMINED;
        case EMessageStreamStatus::External:
            return Ydb::StatusIds::EXTERNAL_ERROR;
        case EMessageStreamStatus::BadSession:
            return Ydb::StatusIds::BAD_SESSION;
        case EMessageStreamStatus::GenericError:
            return Ydb::StatusIds::GENERIC_ERROR;
        case EMessageStreamStatus::AlreadyExists:
            return Ydb::StatusIds::ALREADY_EXISTS;
        case EMessageStreamStatus::SessionExpired:
            return Ydb::StatusIds::SESSION_EXPIRED;
        case EMessageStreamStatus::SessionBusy:
            return Ydb::StatusIds::SESSION_BUSY;
        case EMessageStreamStatus::Unknown:
            return std::nullopt;
    }
}

NYdb::NTopic::TReadSessionSettings ToSdkReadSettings(const TString& stream, const NFq::TMessageStreamReadSessionSettings& settings) {
    return ToSdkReadSettingsImpl(stream, settings);
}

NFq::TMessageStreamResult<NFq::TMessageStreamDescription> ToMessageStream(const NYdb::NTopic::TDescribeTopicResult& result) {
    NFq::TMessageStreamDescription description;
    if (result.IsSuccess()) {
        for (const auto& partition : result.GetTopicDescription().GetPartitions()) {
            description.Partitions.push_back({.PartitionId = {partition.GetPartitionId()}, .Active = partition.GetActive()});
        }
        description.Consumers.emplace();
        for (const auto& consumer : result.GetTopicDescription().GetConsumers()) {
            description.Consumers->push_back({.Name = TString(consumer.GetConsumerName())});
        }
    }
    return MakeResult(result, std::move(description));
}

NFq::TMessageStreamResult<NFq::TMessageStreamConsumerDescription> ToMessageStream(const NYdb::NTopic::TDescribeConsumerResult& result) {
    NFq::TMessageStreamConsumerDescription description;
    if (result.IsSuccess()) {
        for (const auto& partition : result.GetConsumerDescription().GetPartitions()) {
            NFq::TMessageStreamConsumerPartition item;
            item.PartitionId = {partition.GetPartitionId()};
            if (const auto& stats = partition.GetPartitionStats()) {
                item.StartOffset = stats->GetStartOffset();
                item.EndOffset = stats->GetEndOffset();
                item.LastWriteTime = stats->GetLastWriteTime();
            }
            if (const auto& location = partition.GetPartitionLocation()) {
                item.Generation = location->GetGeneration();
            }
            if (const auto& stats = partition.GetPartitionConsumerStats()) {
                item.CommittedOffset = stats->GetCommittedOffset();
            }
            description.Partitions.push_back(std::move(item));
        }
    }
    return MakeResult(result, std::move(description));
}

NFq::TMessageStreamResult<NFq::TMessageStreamPartitionDescription> ToMessageStream(const NYdb::NTopic::TDescribePartitionResult& result) {
    NFq::TMessageStreamPartitionDescription description;
    if (result.IsSuccess()) {
        const auto& partition = result.GetPartitionDescription().GetPartition();
        description.PartitionId = {partition.GetPartitionId()};
        if (const auto& stats = partition.GetPartitionStats()) {
            description.StartOffset = stats->GetStartOffset();
            description.EndOffset = stats->GetEndOffset();
        }
    }
    return MakeResult(result, std::move(description));
}

NFq::TMessageStreamResult<NFq::TMessageStreamConsumerPosition> ToMessageStreamConsumerPosition(const NYdb::TStatus& status, NFq::TMessageStreamPartitionId partitionId, ui64 offset) {
    return MakeResult(status, NFq::TMessageStreamConsumerPosition{.PartitionId = partitionId, .NextOffset = offset});
}

NYdb::TStatus ToSdkStatus(NFq::EMessageStreamStatus status, const NYql::TIssues& issues) {
    const auto ydbStatus = ToYdbStatus(status).value_or(Ydb::StatusIds::GENERIC_ERROR);
    return NYdb::TStatus(static_cast<NYdb::EStatus>(static_cast<size_t>(ydbStatus)), NYdb::NAdapters::ToSdkIssues(issues));
}

namespace {

using namespace NYdb;
using namespace NYdb::NTopic;

class TYdbPartitionControl final : public NFq::IMessageStreamPartitionControl {
public:
    explicit TYdbPartitionControl(TPartitionSession::TPtr session)
        : Session(std::move(session))
    {}

    NFq::TMessageStreamPartitionId GetPartitionId() const override {
        return {Session->GetPartitionId()};
    }

    ui64 GetPartitionSessionId() const {
        return Session->GetPartitionSessionId();
    }

    void ConfirmStart(std::optional<ui64> startOffset, std::optional<ui64> maxOffset) override {
        if (Closed) {
            return;
        }
        Y_ENSURE(StartEvent, "Start partition event is not pending");
        StartEvent->Confirm(startOffset, std::nullopt, maxOffset);
        StartEvent.reset();
    }

    void ConfirmStop() override {
        if (Closed) {
            return;
        }
        Y_ENSURE(StopEvent, "Stop partition event is not pending");
        StopEvent->Confirm();
        Invalidate();
    }

    void ConfirmExhausted() override {
        if (Closed) {
            return;
        }
        Y_ENSURE(EndEvent, "End partition event is not pending");
        EndEvent->Confirm();
        EndEvent.reset();
    }

    void RequestStatus() override {
        if (!Closed) {
            Session->RequestStatus();
        }
    }

    bool AcknowledgeRange(ui64 startOffset, ui64 endOffset) override {
        if (startOffset >= endOffset) {
            ythrow NFq::TMessageStreamException(NFq::EMessageStreamStatus::InvalidArgument)
                << "Empty or reversed acknowledgement range";
        }
        if (Closed) {
            return false;
        }
        static_cast<TPartitionSessionControl*>(Session.Get())->Commit(startOffset, endOffset);
        return true;
    }

    void Invalidate() {
        Closed = true;
        StartEvent.reset();
        StopEvent.reset();
        EndEvent.reset();
    }

    void SetStartEvent(TReadSessionEvent::TStartPartitionSessionEvent event) {
        StartEvent = std::move(event);
    }

    void SetStopEvent(TReadSessionEvent::TStopPartitionSessionEvent event) {
        StopEvent = std::move(event);
    }

    void SetEndEvent(TReadSessionEvent::TEndPartitionSessionEvent event) {
        EndEvent = std::move(event);
    }

    const TPartitionSession::TPtr& GetSession() const {
        return Session;
    }

private:
    bool Closed = false;
    TPartitionSession::TPtr Session;
    std::optional<TReadSessionEvent::TStartPartitionSessionEvent> StartEvent;
    std::optional<TReadSessionEvent::TStopPartitionSessionEvent> StopEvent;
    std::optional<TReadSessionEvent::TEndPartitionSessionEvent> EndEvent;
};

class TYdbMessageStreamReadSession final : public NFq::IMessageStreamReadSession {
public:
    explicit TYdbMessageStreamReadSession(std::shared_ptr<IReadSession> session)
        : Session(std::move(session))
    {}

    ~TYdbMessageStreamReadSession() override {
        try {
            Close();
        } catch (...) {
            // Explicit Close reports shutdown errors; destruction cannot throw.
        }
    }

    NThreading::TFuture<void> WaitEvent() override {
        if (Closed) {
            return NThreading::MakeFuture();
        }
        if (!Readiness.Initialized() || Readiness.IsReady()) {
            Readiness = NThreading::NewPromise();
            Session->WaitEvent().Subscribe([promise = Readiness](const NThreading::TFuture<void>& future) mutable {
                try {
                    future.GetValue();
                    promise.TrySetValue();
                } catch (...) {
                    promise.TrySetException(std::current_exception());
                }
            });
        }
        return Readiness.GetFuture();
    }

    std::vector<NFq::TMessageStreamReadEvent> GetEvents(const NFq::TMessageStreamGetEventsSettings& settings) override {
        const size_t limit = settings.MaxEventsCount.value_or(std::numeric_limits<size_t>::max());
        std::vector<NFq::TMessageStreamReadEvent> result;
        if (Closed || limit == 0 || settings.MaxByteSize == 0) {
            return result;
        }
        result.reserve(std::min(limit, size_t{16}));

        // Retry only batches consisting entirely of filtered acknowledgements.
        // Returning the first useful batch preserves the SDK byte budget and avoids
        // repeatedly reading a terminal event that the SDK leaves in its queue.
        bool block = settings.Block;
        while (true) {
            auto events = Session->GetEvents(block, limit, settings.MaxByteSize);
            block = false;
            // A queued event may exceed the remaining byte budget. Permit one
            // indivisible event so the caller can make progress with a soft limit.
            // Probe only once without blocking: readiness can race with delivery.
            if (events.empty() && settings.MaxByteSize != std::numeric_limits<size_t>::max()
                && WaitEvent().IsReady()) {
                events = Session->GetEvents(false, 1, std::numeric_limits<size_t>::max());
            }
            if (events.empty()) {
                break;
            }
            for (auto& event : events) {
                if (auto converted = ConvertEvent(std::move(event))) {
                    result.push_back(std::move(*converted));
                    if (Closed) {
                        break;
                    }
                }
            }
            if (!result.empty()) {
                break;
            }
        }
        return result;
    }

    NThreading::TFuture<void> Close() override {
        if (!CloseRequested) {
            CloseRequested = true;
            auto promise = NThreading::NewPromise<void>();
            CloseFuture = promise.GetFuture();
            MarkClosed();
            try {
                // Abort locally; this intentionally does not wait for commit delivery.
                Session->Close(TDuration::Zero());
                promise.SetValue();
            } catch (...) {
                promise.SetException(std::current_exception());
            }
        }
        return CloseFuture;
    }

    TString GetSessionId() const override {
        return TString(Session->GetSessionId());
    }

private:
    std::shared_ptr<TYdbPartitionControl> ControlFor(const TPartitionSession::TPtr& session) {
        const auto it = Controls.find(session->GetPartitionSessionId());
        if (it != Controls.end()) {
            return it->second;
        }
        auto control = std::make_shared<TYdbPartitionControl>(session);
        Controls.emplace(session->GetPartitionSessionId(), control);
        return control;
    }

    void Activate(const std::shared_ptr<TYdbPartitionControl>& control) {
        const auto partitionId = control->GetPartitionId().Value;
        if (const auto active = ActivePartitions.find(partitionId); active != ActivePartitions.end()) {
            if (active->second != control->GetPartitionSessionId()) {
                if (const auto old = Controls.find(active->second); old != Controls.end()) {
                    old->second->Invalidate();
                }
            }
        }
        ActivePartitions[partitionId] = control->GetPartitionSessionId();
    }

    // Drop the registry entry. Events and in-flight commits keep their own shared_ptr.
    void Release(const TPartitionSession::TPtr& session) {
        const ui64 sessionId = session->GetPartitionSessionId();
        if (const auto it = Controls.find(sessionId); it != Controls.end()) {
            it->second->Invalidate();
            Controls.erase(it);
        }
        const auto active = ActivePartitions.find(session->GetPartitionId());
        if (active != ActivePartitions.end() && active->second == sessionId) {
            ActivePartitions.erase(active);
        }
    }

    void DropPartitionRegistry() {
        for (const auto& [_, control] : Controls) {
            control->Invalidate();
        }
        Controls.clear();
        ActivePartitions.clear();
    }

    void MarkClosed() {
        if (!Closed) {
            Closed = true;
            DropPartitionRegistry();
            if (Readiness.Initialized()) {
                Readiness.TrySetValue();
            }
        }
    }

    std::optional<NFq::TMessageStreamReadEvent> ConvertEvent(TReadSessionEvent::TEvent event) {
        return std::visit([&](auto&& concrete) -> std::optional<NFq::TMessageStreamReadEvent> {
            using TEvent = std::decay_t<decltype(concrete)>;
            if constexpr (std::is_same_v<TEvent, TReadSessionEvent::TDataReceivedEvent>) {
                auto control = ControlFor(concrete.GetPartitionSession());
                NFq::TMessageStreamDataEvent converted;
                converted.PartitionControl = control;
                converted.Records.reserve(concrete.GetMessages().size());
                for (const auto& message : concrete.GetMessages()) {
                    NFq::TMessageStreamRecord record;
                    record.Id.PartitionId = control->GetPartitionId();
                    record.Id.Offset = message.GetOffset();
                    record.CreateTime = message.GetCreateTime();
                    record.WriteTime = message.GetWriteTime();
                    record.MessageGroupId = TString(message.GetMessageGroupId());
                    record.SeqNo = message.GetSeqNo();
                    if (const auto& meta = message.GetMessageMeta()) {
                        record.Attributes.reserve(meta->Fields.size());
                        for (const auto& [key, value] : meta->Fields) {
                            record.Attributes.push_back({.Name = TString(key), .Value = TString(value)});
                        }
                    }
                    try {
                        const auto& bytes = message.GetData();
                        record.Data.emplace(bytes.data(), bytes.size());
                    } catch (const std::exception& ex) {
                        record.DecompressionError = ex.what();
                    }
                    converted.Records.push_back(std::move(record));
                }
                return NFq::TMessageStreamReadEvent{std::move(converted)};
            } else if constexpr (std::is_same_v<TEvent, TReadSessionEvent::TStartPartitionSessionEvent>) {
                auto control = ControlFor(concrete.GetPartitionSession());
                Activate(control);
                const ui64 committed = concrete.GetCommittedOffset();
                const ui64 end = concrete.GetEndOffset();
                control->SetStartEvent(std::move(concrete));
                return NFq::TMessageStreamReadEvent{NFq::TMessageStreamPartitionStartRequestedEvent{
                    .PartitionControl = std::move(control),
                    .CommittedOffset = committed,
                    .EndOffset = end,
                }};
            } else if constexpr (std::is_same_v<TEvent, TReadSessionEvent::TStopPartitionSessionEvent>) {
                auto control = ControlFor(concrete.GetPartitionSession());
                control->SetStopEvent(std::move(concrete));
                return NFq::TMessageStreamReadEvent{NFq::TMessageStreamPartitionStopRequestedEvent{.PartitionControl = std::move(control)}};
            } else if constexpr (std::is_same_v<TEvent, TReadSessionEvent::TEndPartitionSessionEvent>) {
                auto control = ControlFor(concrete.GetPartitionSession());
                control->SetEndEvent(std::move(concrete));
                return NFq::TMessageStreamReadEvent{NFq::TMessageStreamPartitionExhaustedEvent{.PartitionControl = std::move(control)}};
            } else if constexpr (std::is_same_v<TEvent, TReadSessionEvent::TPartitionSessionStatusEvent>) {
                NFq::TMessageStreamPartitionStatusEvent status;
                status.PartitionControl = ControlFor(concrete.GetPartitionSession());
                status.CommittedOffset = concrete.GetCommittedOffset();
                status.ReadOffset = concrete.GetReadOffset();
                status.EndOffset = concrete.GetEndOffset();
                status.WriteTimeHighWatermark = concrete.GetWriteTimeHighWatermark();
                return NFq::TMessageStreamReadEvent{std::move(status)};
            } else if constexpr (std::is_same_v<TEvent, TReadSessionEvent::TPartitionSessionClosedEvent>) {
                auto control = ControlFor(concrete.GetPartitionSession());
                Release(concrete.GetPartitionSession());
                return NFq::TMessageStreamReadEvent{NFq::TMessageStreamPartitionClosedEvent{.PartitionControl = std::move(control)}};
            } else if constexpr (std::is_same_v<TEvent, TSessionClosedEvent>) {
                MarkClosed();
                return NFq::TMessageStreamReadEvent{NFq::TMessageStreamSessionClosedEvent{
                    .Status = ToStreamStatus(concrete.GetStatus()),
                    .Issues = NYdb::NAdapters::ToYqlIssues(concrete.GetIssues()),
                }};
            } else if constexpr (std::is_same_v<TEvent, TReadSessionEvent::TCommitOffsetAcknowledgementEvent>) {
                // The SDK queue has already given up this event. Callers that treat
                // a ready WaitEvent as a non-empty GetEvents must tolerate the gap.
                return std::nullopt;
            } else {
                return std::nullopt;
            }
        }, std::move(event));
    }

    bool Closed = false;
    bool CloseRequested = false;
    NThreading::TFuture<void> CloseFuture;
    NThreading::TPromise<void> Readiness;
    std::shared_ptr<IReadSession> Session;
    // Live partition sessions of this read session, keyed by partition session id.
    std::unordered_map<ui64, std::shared_ptr<TYdbPartitionControl>> Controls;
    // Partition id to the partition session id assigned by the latest start event.
    std::unordered_map<ui64, ui64> ActivePartitions;
};

class TPqMessageStreamClient final : public NFq::IMessageStreamClient {
public:
    explicit TPqMessageStreamClient(const TString& stream, TTopicClient client)
        : Stream(stream)
        , Client(std::move(client))
    {
        if (Stream.empty()) {
            ythrow NFq::TMessageStreamException(NFq::EMessageStreamStatus::InvalidArgument) << "Stream name must be nonempty";
        }
    }

    const TString& GetStream() const override {
        return Stream;
    }

    NThreading::TFuture<NFq::TMessageStreamResult<NFq::TMessageStreamDescription>> DescribeStream() override {
        return Client.DescribeTopic(Stream, {}).Apply([](const auto& future) {
            return ToMessageStream(future.GetValue());
        });
    }

    NThreading::TFuture<NFq::TMessageStreamResult<NFq::TMessageStreamConsumerDescription>> DescribeConsumer(
        const TString& consumer, const NFq::TMessageStreamDescribeConsumerSettings& settings) override
    {
        return Client.DescribeConsumer(Stream, consumer, TDescribeConsumerSettings()
            .IncludeStats(settings.IncludeStats)
            .IncludeLocation(settings.IncludeGeneration)).Apply([](const auto& future) {
            return ToMessageStream(future.GetValue());
        });
    }

    NThreading::TFuture<NFq::TMessageStreamResult<NFq::TMessageStreamPartitionDescription>> DescribePartition(NFq::TMessageStreamPartitionId partitionId) override {
        return Client.DescribePartition(Stream, partitionId.Value, TDescribePartitionSettings().IncludeStats(true)).Apply([](const auto& future) {
            return ToMessageStream(future.GetValue());
        });
    }

    std::shared_ptr<NFq::IMessageStreamReadSession> CreateReadSession(const NFq::TMessageStreamReadSessionSettings& settings) override {
        return std::make_shared<TYdbMessageStreamReadSession>(Client.CreateReadSession(ToSdkReadSettings(Stream, settings)));
    }

    NThreading::TFuture<NFq::TMessageStreamResult<NFq::TMessageStreamConsumerPosition>> CommitPosition(
        NFq::TMessageStreamPartitionId partitionId, const TString& consumer, ui64 offset) override
    {
        return Client.CommitOffset(Stream, partitionId.Value, consumer, offset).Apply([partitionId, offset](const auto& future) {
            return ToMessageStreamConsumerPosition(future.GetValue(), partitionId, offset);
        });
    }

private:
    const TString Stream;
    TTopicClient Client;
};

} // anonymous namespace

std::shared_ptr<NFq::IMessageStreamClient> CreateMessageStreamClient(const TString& stream, NYdb::NTopic::TTopicClient client) {
    return std::make_shared<TPqMessageStreamClient>(stream, std::move(client));
}

std::shared_ptr<NFq::IMessageStreamReadSession> WrapYdbReadSession(std::shared_ptr<NYdb::NTopic::IReadSession> session) {
    return std::make_shared<TYdbMessageStreamReadSession>(std::move(session));
}

} // namespace NYql
