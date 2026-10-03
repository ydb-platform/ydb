#include "yql_pq_message_stream_client.h"

#include <ydb/public/sdk/cpp/adapters/issue/issue.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/errors.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

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
    NFq::TMessageStreamResult<TValue> result;
    result.Status = ToStreamStatus(status.GetStatus());
    result.Issues = NYdb::NAdapters::ToYqlIssues(status.GetIssues());
    result.Value = std::move(value);
    return result;
}

TReadSessionSettings ToSdkReadSettingsImpl(const NFq::TMessageStreamReadSettings& settings) {
    TTopicReadSettings topic;
    topic.Path(settings.Stream);
    for (const ui64 partitionId : settings.PartitionIds) {
        topic.AppendPartitionIds(partitionId);
    }
    if (settings.PartitionId) {
        topic.AppendPartitionIds(*settings.PartitionId);
    }

    TReadSessionSettings sdk;
    sdk.AppendTopics(std::move(topic));
    if (settings.WithoutConsumer) {
        sdk.WithoutConsumer();
    } else if (settings.Consumer) {
        sdk.ConsumerName(settings.Consumer);
    }
    if (settings.StartTime) {
        sdk.ReadFromTimestamp(*settings.StartTime);
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

NYdb::NTopic::TReadSessionSettings ToSdkReadSettings(const NFq::TMessageStreamReadSettings& settings) {
    return ToSdkReadSettingsImpl(settings);
}

NFq::TMessageStreamResult<NFq::TMessageStreamTopicDescription> ToMessageStream(const NYdb::NTopic::TDescribeTopicResult& result) {
    NFq::TMessageStreamTopicDescription description;
    if (result.IsSuccess()) {
        description.PartitionsCount = result.GetTopicDescription().GetTotalPartitionsCount();
        for (const auto& consumer : result.GetTopicDescription().GetConsumers()) {
            description.Consumers.push_back({.Name = TString(consumer.GetConsumerName())});
        }
    }
    return MakeResult(result, std::move(description));
}

NFq::TMessageStreamResult<NFq::TMessageStreamConsumerDescription> ToMessageStream(const NYdb::NTopic::TDescribeConsumerResult& result) {
    NFq::TMessageStreamConsumerDescription description;
    if (result.IsSuccess()) {
        for (const auto& partition : result.GetConsumerDescription().GetPartitions()) {
            NFq::TMessageStreamConsumerPartition item;
            item.PartitionId = partition.GetPartitionId();
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
        description.PartitionId = partition.GetPartitionId();
        if (const auto& stats = partition.GetPartitionStats()) {
            description.StartOffset = stats->GetStartOffset();
            description.EndOffset = stats->GetEndOffset();
        }
    }
    return MakeResult(result, std::move(description));
}

NFq::TMessageStreamResult<NFq::TMessageStreamOffset> ToMessageStreamOffset(const NYdb::TStatus& status, ui64 partitionId, ui64 offset) {
    return MakeResult(status, NFq::TMessageStreamOffset{.PartitionId = partitionId, .Offset = offset});
}

NYdb::TStatus ToSdkStatus(NFq::EMessageStreamStatus status, const NYql::TIssues& issues) {
    const auto ydbStatus = NFq::ToYdbStatus(status).value_or(Ydb::StatusIds::GENERIC_ERROR);
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

    ui64 GetPartitionId() const override {
        return Session->GetPartitionId();
    }

    ui64 GetPartitionSessionId() const {
        return Session->GetPartitionSessionId();
    }

    void ConfirmStart(std::optional<ui64> startOffset, std::optional<ui64> maxOffset) override {
        Y_ENSURE(StartEvent, "Start partition event is not pending");
        StartEvent->Confirm(startOffset, std::nullopt, maxOffset);
        StartEvent.reset();
    }

    void ConfirmStop() override {
        Y_ENSURE(StopEvent, "Stop partition event is not pending");
        StopEvent->Confirm();
        StopEvent.reset();
    }

    void RequestStatus() override {
        Session->RequestStatus();
    }

    void Commit(ui64 startOffset, ui64 endOffset) override {
        static_cast<TPartitionSessionControl*>(Session.Get())->Commit(startOffset, endOffset);
    }

    void SetStartEvent(TReadSessionEvent::TStartPartitionSessionEvent event) {
        StartEvent = std::move(event);
    }

    void SetStopEvent(TReadSessionEvent::TStopPartitionSessionEvent event) {
        StopEvent = std::move(event);
    }

    const TPartitionSession::TPtr& GetSession() const {
        return Session;
    }

private:
    TPartitionSession::TPtr Session;
    std::optional<TReadSessionEvent::TStartPartitionSessionEvent> StartEvent;
    std::optional<TReadSessionEvent::TStopPartitionSessionEvent> StopEvent;
};

class TYdbMessageStreamReadSession final : public NFq::IMessageStreamReadSession {
public:
    explicit TYdbMessageStreamReadSession(std::shared_ptr<IReadSession> session)
        : Session(std::move(session))
    {}

    NThreading::TFuture<void> WaitEvent() override {
        return Session->WaitEvent();
    }

    std::vector<NFq::TMessageStreamReadEvent> GetEvents(const NFq::TMessageStreamReadEventSettings& settings) override {
        const size_t limit = settings.MaxEventsCount.value_or(std::numeric_limits<size_t>::max());
        std::vector<NFq::TMessageStreamReadEvent> result;
        if (limit == 0) {
            return result;
        }
        result.reserve(std::min(limit, size_t{16}));

        // Commit acknowledgements are not part of the common event API. Skip them and
        // keep pulling while the SDK queue still has events. Stop when the queue is
        // empty or the next event does not fit MaxByteSize: that call returns nothing
        // and leaves WaitEvent ready.
        bool block = settings.Block;
        while (result.size() < limit) {
            auto events = Session->GetEvents(block, limit - result.size(), settings.MaxByteSize);
            block = false;
            if (events.empty()) {
                break;
            }
            const size_t convertedBefore = result.size();
            for (auto& event : events) {
                if (auto converted = ConvertEvent(std::move(event))) {
                    result.push_back(std::move(*converted));
                }
            }
            if (result.size() == convertedBefore && !Session->WaitEvent().IsReady()) {
                break;
            }
        }
        return result;
    }

    NThreading::TFuture<void> CommitOffset(const NFq::TMessageStreamOffset& offset) override {
        auto partition = Partition(offset.PartitionId);
        Y_ENSURE(partition, "Unknown partition " << offset.PartitionId);
        partition->Commit(offset.Offset, offset.Offset + 1);
        return NThreading::MakeFuture();
    }

    NThreading::TFuture<void> Close() override {
        DropPartitionRegistry();
        Session->Close(TDuration::Zero());
        return NThreading::MakeFuture();
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
        ActivePartitions[control->GetPartitionId()] = control->GetPartitionSessionId();
    }

    // Drop the registry entry. Events and in-flight commits keep their own shared_ptr.
    void Release(const TPartitionSession::TPtr& session) {
        const ui64 sessionId = session->GetPartitionSessionId();
        Controls.erase(sessionId);
        const auto active = ActivePartitions.find(session->GetPartitionId());
        if (active != ActivePartitions.end() && active->second == sessionId) {
            ActivePartitions.erase(active);
        }
    }

    void DropPartitionRegistry() {
        Controls.clear();
        ActivePartitions.clear();
    }

    std::shared_ptr<TYdbPartitionControl> Partition(ui64 partitionId) const {
        const auto active = ActivePartitions.find(partitionId);
        if (active == ActivePartitions.end()) {
            return nullptr;
        }
        const auto control = Controls.find(active->second);
        if (control == Controls.end()) {
            return nullptr;
        }
        return control->second;
    }

    std::optional<NFq::TMessageStreamReadEvent> ConvertEvent(TReadSessionEvent::TEvent event) {
        return std::visit([&](auto&& concrete) -> std::optional<NFq::TMessageStreamReadEvent> {
            using TEvent = std::decay_t<decltype(concrete)>;
            if constexpr (std::is_same_v<TEvent, TReadSessionEvent::TDataReceivedEvent>) {
                auto control = ControlFor(concrete.GetPartitionSession());
                NFq::TMessageStreamDataEvent converted;
                converted.Partition = control;
                converted.Messages.reserve(concrete.GetMessages().size());
                for (const auto& message : concrete.GetMessages()) {
                    NFq::TMessageStreamMessage item;
                    item.Offset.PartitionId = control->GetPartitionId();
                    item.Offset.Offset = message.GetOffset();
                    item.CreateTime = message.GetCreateTime();
                    item.WriteTime = message.GetWriteTime();
                    item.MessageGroupId = TString(message.GetMessageGroupId());
                    item.SeqNo = message.GetSeqNo();
                    if (const auto& meta = message.GetMessageMeta()) {
                        item.Attributes.reserve(meta->Fields.size());
                        for (const auto& [key, value] : meta->Fields) {
                            item.Attributes.emplace_back(key, value);
                        }
                    }
                    try {
                        const auto& bytes = message.GetData();
                        item.Data.assign(bytes.data(), bytes.size());
                    } catch (const std::exception& ex) {
                        item.DecompressionError = ex.what();
                    }
                    converted.Messages.push_back(std::move(item));
                }
                return NFq::TMessageStreamReadEvent{std::move(converted)};
            } else if constexpr (std::is_same_v<TEvent, TReadSessionEvent::TStartPartitionSessionEvent>) {
                auto control = ControlFor(concrete.GetPartitionSession());
                Activate(control);
                const ui64 committed = concrete.GetCommittedOffset();
                const ui64 end = concrete.GetEndOffset();
                control->SetStartEvent(std::move(concrete));
                return NFq::TMessageStreamReadEvent{NFq::TMessageStreamPartitionStartedEvent{
                    .Partition = std::move(control),
                    .CommittedOffset = committed,
                    .EndOffset = end,
                }};
            } else if constexpr (std::is_same_v<TEvent, TReadSessionEvent::TStopPartitionSessionEvent>) {
                auto control = ControlFor(concrete.GetPartitionSession());
                control->SetStopEvent(std::move(concrete));
                return NFq::TMessageStreamReadEvent{NFq::TMessageStreamPartitionStoppedEvent{.Partition = std::move(control)}};
            } else if constexpr (std::is_same_v<TEvent, TReadSessionEvent::TEndPartitionSessionEvent>) {
                return NFq::TMessageStreamReadEvent{NFq::TMessageStreamPartitionEndedEvent{.Partition = ControlFor(concrete.GetPartitionSession())}};
            } else if constexpr (std::is_same_v<TEvent, TReadSessionEvent::TPartitionSessionStatusEvent>) {
                NFq::TMessageStreamPartitionStatusEvent status;
                status.Partition = ControlFor(concrete.GetPartitionSession());
                status.CommittedOffset = concrete.GetCommittedOffset();
                status.ReadOffset = concrete.GetReadOffset();
                status.EndOffset = concrete.GetEndOffset();
                status.WriteTimeHighWatermark = concrete.GetWriteTimeHighWatermark();
                return NFq::TMessageStreamReadEvent{std::move(status)};
            } else if constexpr (std::is_same_v<TEvent, TReadSessionEvent::TPartitionSessionClosedEvent>) {
                auto control = ControlFor(concrete.GetPartitionSession());
                Release(concrete.GetPartitionSession());
                return NFq::TMessageStreamReadEvent{NFq::TMessageStreamPartitionClosedEvent{.Partition = std::move(control)}};
            } else if constexpr (std::is_same_v<TEvent, TSessionClosedEvent>) {
                DropPartitionRegistry();
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

    std::shared_ptr<IReadSession> Session;
    // Live partition sessions of this read session, keyed by partition session id.
    std::unordered_map<ui64, std::shared_ptr<TYdbPartitionControl>> Controls;
    // Partition id to the partition session id assigned by the latest start event.
    std::unordered_map<ui64, ui64> ActivePartitions;
};

class TPqMessageStreamClient final : public NFq::IMessageStreamClient {
public:
    explicit TPqMessageStreamClient(TTopicClient client)
        : Client(std::move(client))
    {}

    NThreading::TFuture<NFq::TMessageStreamResult<NFq::TMessageStreamTopicDescription>> DescribeStream(const TString& stream) override {
        return Client.DescribeTopic(stream, {}).Apply([](const auto& future) {
            return ToMessageStream(future.GetValue());
        });
    }

    NThreading::TFuture<NFq::TMessageStreamResult<NFq::TMessageStreamConsumerDescription>> DescribeConsumer(
        const TString& stream, const TString& consumer, const NFq::TMessageStreamDescribeConsumerSettings& settings) override
    {
        return Client.DescribeConsumer(stream, consumer, TDescribeConsumerSettings()
            .IncludeStats(settings.IncludeStats)
            .IncludeLocation(settings.IncludeLocation)).Apply([](const auto& future) {
            return ToMessageStream(future.GetValue());
        });
    }

    NThreading::TFuture<NFq::TMessageStreamResult<NFq::TMessageStreamPartitionDescription>> DescribePartition(const TString& stream, ui64 partitionId) override {
        return Client.DescribePartition(stream, partitionId, TDescribePartitionSettings().IncludeStats(true)).Apply([](const auto& future) {
            return ToMessageStream(future.GetValue());
        });
    }

    std::shared_ptr<NFq::IMessageStreamReadSession> CreateReadSession(const NFq::TMessageStreamReadSettings& settings) override {
        return std::make_shared<TYdbMessageStreamReadSession>(Client.CreateReadSession(ToSdkReadSettings(settings)));
    }

    NThreading::TFuture<NFq::TMessageStreamResult<NFq::TMessageStreamOffset>> CommitOffset(
        const TString& stream, ui64 partitionId, const TString& consumer, ui64 offset) override
    {
        return Client.CommitOffset(stream, partitionId, consumer, offset).Apply([partitionId, offset](const auto& future) {
            return ToMessageStreamOffset(future.GetValue(), partitionId, offset);
        });
    }

private:
    TTopicClient Client;
};

} // anonymous namespace

std::shared_ptr<NFq::IMessageStreamClient> CreateMessageStreamClient(NYdb::NTopic::TTopicClient client) {
    return std::make_shared<TPqMessageStreamClient>(std::move(client));
}

std::shared_ptr<NFq::IMessageStreamReadSession> WrapYdbReadSession(std::shared_ptr<NYdb::NTopic::IReadSession> session) {
    return std::make_shared<TYdbMessageStreamReadSession>(std::move(session));
}

} // namespace NYql
