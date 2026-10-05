#include "yql_qyt_message_stream_client.h"
#include "yql_qyt_blocking_queue.h"

#include <atomic>
#include <functional>
#include <future>

#include <library/cpp/threading/future/async.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/queue_client.h>
#include <yt/yt/client/api/transaction.h>
#include <yt/yt/client/transaction_client/helpers.h>
#include <yt/yt/client/queue_client/consumer_client.h>
#include <yt/yt/client/queue_client/queue_rowset.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/actions/future.h>
#include <library/cpp/yt/logging/logger.h>

#include <util/generic/yexception.h>
#include <util/string/builder.h>

#include <util/datetime/base.h>

#include <map>
#include <mutex>
#include <thread>

#include "yql_qyt_read_session.h"
namespace NYql {
namespace {
using namespace NFq;

using NYT::NApi::IClientPtr;
using NYT::NQueueClient::IQueueRowsetPtr;
using NYT::NQueueClient::TQueueRowBatchReadOptions;
using NYT::NTableClient::TUnversionedRow;
using NYT::NTableClient::TUnversionedValue;

////////////////////////////////////////////////////////////////////////////////

// Returns true if the exception represents a fatal (non-retryable) error.
// Fatal errors include: permission denied, not found, invalid path, etc.
// Transient errors (network, timeout, rate limit) should be retried.
bool IsFatalError(const std::exception& ex) {
    const TString msg = ex.what();
    const TString lowerMsg = to_lower(msg);
    static const std::vector<TString> FatalKeywords = {
        "permission denied",
        "not found",
        "no such",
        "invalid path",
        "invalid argument",
        "already exists",
        "access denied",
        "unauthorized",
        "forbidden",
    };
    for (const auto& keyword : FatalKeywords) {
        if (lowerMsg.Contains(keyword)) {
            return true;
        }
    }
    return false;
}

///////////////////////////////////////////////////////////////////////////////

using TCommitOffset = std::function<NThreading::TFuture<void>(ui64)>;

struct TQytPartitionSession final : public IMessageStreamPartitionControl {
    TQytPartitionSession(ui64 partitionId, TCommitOffset commit, ui64 defaultOffset, ui64 minimumOffset)
        : Id{partitionId}
        , CommitOffset(std::move(commit))
        , NextAcknowledged(defaultOffset)
        , MinimumOffset(minimumOffset)
    {}

    NFq::TMessageStreamPartitionId GetPartitionId() const override { return Id; }

    void ConfirmStart(std::optional<ui64> startOffset, std::optional<ui64> maxOffset) override {
        if (!Active || Confirmed) {
            return;
        }
        Confirmed = true;
        std::lock_guard guard(Mutex);
        NextAcknowledged = std::max(startOffset.value_or(NextAcknowledged), MinimumOffset);
        Start.TrySetValue(TStartOffsets{NextAcknowledged, maxOffset});
    }

    void ConfirmStop() override { Active = false; }
    void ConfirmExhausted() override {}
    void RequestStatus() override {
        if (Active && ReportStatus) {
            ReportStatus();
        }
    }

    bool AcknowledgeRange(ui64 startOffset, ui64 endOffset) override {
        if (!Active) {
            return false;
        }
        if (startOffset >= endOffset) {
            ythrow TMessageStreamException(EMessageStreamStatus::InvalidArgument) << "Invalid acknowledgement range";
        }
        if (!Confirmed) {
            ythrow TMessageStreamException(EMessageStreamStatus::PreconditionFailed) << "Partition is not confirmed";
        }
        std::lock_guard guard(Mutex);
        auto& end = Acknowledged[startOffset];
        end = std::max(end, endOffset);
        ui64 next = NextAcknowledged;
        for (auto it = Acknowledged.begin(); it != Acknowledged.end() && it->first <= next;) {
            next = std::max(next, it->second);
            it = Acknowledged.erase(it);
        }
        if (next > NextAcknowledged) {
            NextAcknowledged = next;
            PendingCommit = next;
        }
        return true;
    }

    // Called only by the polling worker, never on the actor thread.
    void FlushAcknowledgements() {
        const auto next = PendingCommit.exchange(UnknownOffset);
        if (next == UnknownOffset || !Active) {
            return;
        }
        try {
            CommitOffset(next).GetValueSync();
            CommittedOffset = next;
        } catch (const std::exception& ex) {
            if (Active.exchange(false)) {
                ReportError(ex.what());
            }
        }
    }

    void SkipRemovedRange(ui64 begin, ui64 end) {
        if (begin < end) {
            AcknowledgeRange(begin, end);
        }
    }

    std::optional<ui64> GetCommittedOffset() const {
        const auto value = CommittedOffset.load();
        return value == UnknownOffset ? std::nullopt : std::optional<ui64>(value);
    }

    struct TStartOffsets {
        std::optional<ui64> Start;
        std::optional<ui64> End;
    };

    const NFq::TMessageStreamPartitionId Id;
    const TCommitOffset CommitOffset;
    std::atomic<bool> Active{true};
    bool Confirmed = false;
    ui64 NextAcknowledged;
    const ui64 MinimumOffset;
    static constexpr ui64 UnknownOffset = std::numeric_limits<ui64>::max();
    std::atomic<ui64> CommittedOffset{UnknownOffset};
    std::atomic<ui64> PendingCommit{UnknownOffset};
    std::mutex Mutex;
    std::map<ui64, ui64> Acknowledged;
    std::function<void()> ReportStatus;
    std::function<void(const TString&)> ReportError;
    NThreading::TPromise<TStartOffsets> Start = NThreading::NewPromise<TStartOffsets>();
};

////////////////////////////////////////////////////////////////////////////////

// MessageStream read session backed by the YTsaurus queue pull API.
//
// A background thread polls pull_queue_consumer starting from the current
// offset, converts each queue row into a message stream data event and pushes
// it into a bounded blocking queue drained by WaitEvent()/GetEvents().


class TQytReadSession final : public IMessageStreamReadSession, public std::enable_shared_from_this<TQytReadSession> {
    using TEQueue = TBlockingEQueue<TMessageStreamReadEvent>;
    using TMessage = TMessageStreamRecord;

public:
    TQytReadSession(
        IClientPtr client,
        NYT::NYPath::TRichYPath queuePath,
        NYT::NYPath::TRichYPath consumerPath,
        std::shared_ptr<TQytPartitionSession> session,
        int partitionIndex,
        i64 startOffset,
        TString dataColumn,
        TQueueRowBatchReadOptions readOptions,
        TDuration pollPeriod,
        bool tableMode,
        i64 endOffset,
        size_t maxMemoryBytes,
        bool requireWriteTime,
        std::shared_ptr<TEQueue> events)
        : Client(std::move(client))
        , QueuePath(std::move(queuePath))
        , ConsumerPath(std::move(consumerPath))
        , Session(std::move(session))
        , PartitionIndex(partitionIndex)
        , Offset(startOffset)
        , DataColumn(std::move(dataColumn))
        , ReadOptions(readOptions)
        , PollPeriod(pollPeriod)
        , TableMode(tableMode)
        , EndOffset(endOffset)
        , RequireWriteTime(requireWriteTime)
        , EventsQ(std::move(events))

    {
        Y_UNUSED(maxMemoryBytes);
        Pool.Start(1);
        Session->ReportStatus = [this]() {
            EventsQ->PushControl(TMessageStreamPartitionStatusEvent{Session, Session->GetCommittedOffset(),
                static_cast<ui64>(Offset.load()), TableMode ? std::optional<ui64>(EndOffset) : std::nullopt, {}});
        };
        Session->ReportError = [this](const TString& error) {
            EventsQ->PushControl(TMessageStreamSessionClosedEvent{EMessageStreamStatus::InternalError, {TIssue(error)}});
            EventsQ->Stop();
        };
    }

    void StartPolling() {
        std::thread([self = shared_from_this()]() {
            self->RunPolling();
        }).detach();
    }

    void RunPolling() {
        auto completion = Finished;
        {
            try {
                PollLoop();
            } catch (const TMessageStreamException& ex) {
                EventsQ->Push(TMessageStreamSessionClosedEvent{ex.GetStatus(), {TIssue(ex.what())}}, 0);
            } catch (const std::exception& ex) {
                EventsQ->Push(TMessageStreamSessionClosedEvent{EMessageStreamStatus::InternalError, {TIssue(ex.what())}}, 0);
            }
        }
        completion.TrySetValue();
    }

    ~TQytReadSession() override {
        try {
            Cleanup();
        } catch (...) {
        }
    }

    NThreading::TFuture<void> WaitEvent() final {
        return NThreading::Async([this]() {
            EventsQ->BlockUntilEvent();
            return NThreading::MakeFuture();
        }, Pool);
    }

    std::vector<TMessageStreamReadEvent> GetEvents(const TMessageStreamGetEventsSettings& settings) final {
        std::vector<TMessageStreamReadEvent> result;
        if (TerminalDelivered) {
            return result;
        }
        size_t bytes = 0;
        const size_t limit = settings.MaxEventsCount.value_or(std::numeric_limits<size_t>::max());
        while (result.size() < limit && bytes < settings.MaxByteSize) {
            auto event = EventsQ->Pop(settings.Block && result.empty());
            if (!event) {
                break;
            }
            if (const auto* data = std::get_if<TMessageStreamDataEvent>(&*event)) {
                for (const auto& message : data->Records) {
                    bytes += message.Data ? message.Data->size() : 0;
                }
            }
            const bool terminal = std::holds_alternative<TMessageStreamSessionClosedEvent>(*event);
            result.push_back(std::move(*event));
            if (terminal) {
                TerminalDelivered = true;
                Session->Active = false;
                EventsQ->Stop();
                break;
            }
        }
        return result;
    }

    NThreading::TFuture<void> Close() final {
        Cleanup();
        return Finished.GetFuture();
    }

    TString GetSessionId() const final {
        return ToString(Session->GetPartitionId().Value);
    }

private:
    TMessage MakeMessage(const std::optional<TString>& data, i64 offset, std::optional<TInstant> writeTime) {
        TMessage message;
        message.Data = data;
        message.Id = {Session->GetPartitionId(), static_cast<ui64>(offset)};
        message.WriteTime = writeTime;
        return message;
    }

    // Extracts the payload of a single queue row as a string. If a DataColumn is
    // configured it returns that column's value.
    // Supports string-like value types: String, Any, Composite (binary data is
    // stored as String in YT queues).
    std::optional<TString> ExtractRowData(const NYT::NTableClient::TNameTablePtr& nameTable, TUnversionedRow row) {
        Y_ENSURE(!DataColumn.empty(), "DataColumn must be configured for QYT topic read session");

        std::optional<int> dataId = nameTable->FindId(DataColumn);
        Y_ENSURE(dataId.has_value(), TStringBuilder() << "Data column '" << DataColumn << "' not found in row name table");

        TStringBuilder data;
        for (const auto& value : row) {
            if (value.Id != static_cast<ui16>(*dataId)) {
                continue;
            }
            switch (value.Type) {
                case NYT::NTableClient::EValueType::String:
                case NYT::NTableClient::EValueType::Any:
                case NYT::NTableClient::EValueType::Composite:
                    data << TStringBuf(value.Data.String, value.Length);
                    break;
                case NYT::NTableClient::EValueType::Null:
                    return std::nullopt;
                default:
                    // Non-string-like types (Int64, Uint64, Double, Boolean) are not
                    // supported as data columns for topic messages.
                    throw std::runtime_error(TStringBuilder()
                        << "Unsupported value type '" << static_cast<int>(value.Type)
                        << "' for data column '" << DataColumn
                        << "'. Only string-like types (String, Any, Composite) are supported.");
            }
        }
        return TString(data);
    }

    void PollLoop() {
        const TDuration MinBackoff = TDuration::MilliSeconds(100);
        const TDuration MaxBackoff = TDuration::Seconds(30);
        TDuration backoff = MinBackoff;

        // Announce assignment and snapshot bounds before delivering records.
        EventsQ->Push(TMessageStreamPartitionStartRequestedEvent{
            Session, static_cast<ui64>(Offset.load()), static_cast<ui64>(EndOffset)}, 0);
        auto startFuture = Session->Start.GetFuture();
        while (!startFuture.Wait(PollPeriod)) {
            if (EventsQ->IsStopped()) {
                return;
            }
        }
        const auto& start = startFuture.GetValueSync();
        if (start.Start) {
            Offset = *start.Start;
        }
        std::optional<ui64> readEnd;
        if (TableMode) {
            readEnd = EndOffset;
        }
        if (start.End && *start.End < std::numeric_limits<ui64>::max()) {
            const ui64 exclusiveEnd = *start.End + 1;
            readEnd = readEnd ? std::min(*readEnd, exclusiveEnd) : exclusiveEnd;
        }

        while (!EventsQ->IsStopped() && Session->Active) {
            Session->FlushAcknowledgements();
            if (readEnd && static_cast<ui64>(Offset.load()) >= *readEnd) {
                SleepInterruptibly(PollPeriod);
                continue;
            }
            IQueueRowsetPtr rowset;
            try {

                auto future = Client->PullQueueConsumer(
                    ConsumerPath,
                    QueuePath,
                    Offset,
                    PartitionIndex,
                    ReadOptions);
                                while (!future.BlockingWait(TDuration::MilliSeconds(10))) {
                    if (EventsQ->IsStopped()) {
                        future.Cancel(NYT::TError("QYT read session closed"));
                        return;
                    }
                }
                if (EventsQ->IsStopped()) {
                    return;
                }
                rowset = future.BlockingGet().ValueOrThrow().Rowset;

                // Reset backoff on success
                backoff = MinBackoff;
            } catch (const std::exception& ex) {

                if (EventsQ->IsStopped()) {
                    break;
                }
                // Fatal errors should not be retried — terminate the session.
                if (IsFatalError(ex)) {
                    // Fatal error in PollLoop — terminate the session.
                    EventsQ->Push(TMessageStreamSessionClosedEvent{
                        EMessageStreamStatus::InvalidArgument,
                        {TIssue(TString("Fatal error: ") + ex.what())}}, 0);
                    break;
                }
                // Transient error in PollLoop — retry with backoff.
                // Retry with exponential backoff on transient errors
                SleepInterruptibly(backoff);
                backoff = std::min(backoff * 2, MaxBackoff);
                if (EventsQ->IsStopped()) {
                    break;
                }
                continue;
            }

            const auto rows = rowset->GetRows();
            if (rows.empty()) {
                // No more messages in the queue at the current offset.
                //
                // In table mode (batch read, StopAtCurrentEndOffsets), the DQ read
                // actor terminates by comparing the consumed offset against the end
                // offset carried by the PartitionStartRequested — it must NOT
                // receive a SessionClosed, which it always treats as an error
                // (BAD_REQUEST) regardless of status. Once we have delivered all rows
                // up to the end offset, simply stop polling and let the actor finish.
                if (TableMode) {
                    if (Offset.load() >= EndOffset) {
                        // All rows up to the snapshot end offset have been delivered.

                        break;
                    }
                    // The queue tail has not yet caught up to the end offset we
                    // captured at start (rows may still be flushing). Back off and
                    // retry rather than closing the session.

                    SleepInterruptibly(PollPeriod);
                    continue;
                }

                SleepInterruptibly(PollPeriod);
                continue;
            }

            const auto& nameTable = rowset->GetNameTable();
            i64 rowOffset = rowset->GetStartOffset();
            Session->SkipRemovedRange(Offset.load(), rowOffset);

            std::vector<TMessage> msgs;
            msgs.reserve(rows.size());
            size_t batchSize = 0;
            for (auto row : rows) {
                if (readEnd && static_cast<ui64>(rowOffset) >= *readEnd) {
                    break;
                }
                auto data = ExtractRowData(nameTable, row);
                std::optional<TInstant> writeTime;
                const auto timestampId = nameTable->FindId("$timestamp");
                for (const auto& value : row) {
                    if (timestampId && value.Id == *timestampId && value.Type == NYT::NTableClient::EValueType::Uint64) {
                        writeTime = NYT::NTransactionClient::TimestampToInstant(NYT::NTransactionClient::TTimestamp(value.Data.Uint64)).first;
                    }
                }
                if (RequireWriteTime && !writeTime) {
                    ythrow TMessageStreamException(EMessageStreamStatus::Unsupported)
                        << "QYT requires the $timestamp column for reads with write-time metadata";
                }
                batchSize += data ? data->size() : 0;
                msgs.emplace_back(MakeMessage(data, rowOffset, writeTime));
                ++rowOffset;
            }

            Offset = readEnd
                ? std::min<i64>(rowset->GetFinishOffset(), *readEnd)
                : rowset->GetFinishOffset();

            EventsQ->Push(TMessageStreamDataEvent{Session, std::move(msgs)}, batchSize);
        }

    }

    void SleepInterruptibly(TDuration duration) {
        const auto until = TInstant::Now() + duration;
        while (!EventsQ->IsStopped() && TInstant::Now() < until) {
            Sleep(std::min(TDuration::MilliSeconds(10), until - TInstant::Now()));
        }
    }

    void Cleanup() {
        Session->Active = false;
        TerminalDelivered = true;
        EventsQ->Stop();
        Pool.Stop();

        while (EventsQ->Pop(false)) {}
    }

    const IClientPtr Client;
    const NYT::NYPath::TRichYPath QueuePath;
    const NYT::NYPath::TRichYPath ConsumerPath;
    const std::shared_ptr<TQytPartitionSession> Session;
    const int PartitionIndex;
    std::atomic<i64> Offset;
    const TString DataColumn;
    const TQueueRowBatchReadOptions ReadOptions;
    const TDuration PollPeriod;
    const bool TableMode;
    const i64 EndOffset;
    const bool RequireWriteTime;
    bool TerminalDelivered = false;
    const std::shared_ptr<TEQueue> EventsQ;
    TThreadPool Pool;
    NThreading::TPromise<void> Finished = NThreading::NewPromise();
};

////////////////////////////////////////////////////////////////////////////////

// All partitions share one bounded queue, so MaxMemoryUsageBytes is a session
// limit. Each assignment keeps its own offsets and confirmation/commit control.
class TQytMultiReadSession final : public IMessageStreamReadSession {
public:
    explicit TQytMultiReadSession(std::vector<std::shared_ptr<IMessageStreamReadSession>> sessions)
        : Sessions(std::move(sessions))
    {}
    ~TQytMultiReadSession() override { Close(); }

    NThreading::TFuture<void> WaitEvent() override { return Sessions.front()->WaitEvent(); }
    std::vector<TMessageStreamReadEvent> GetEvents(const TMessageStreamGetEventsSettings& settings) override {
        if (Closed) {
            return {};
        }
        auto events = Sessions.front()->GetEvents(settings);
        for (const auto& event : events) {
            if (std::holds_alternative<TMessageStreamSessionClosedEvent>(event)) {
                Close();
                break;
            }
        }
        return events;
    }
    NThreading::TFuture<void> Close() override {
        if (!Closed) {
            Closed = true;
            TVector<NThreading::TFuture<void>> futures;
            for (const auto& session : Sessions) {
                futures.push_back(session->Close());
            }
            ClosedFuture = NThreading::WaitAll(futures);
        }
        return ClosedFuture;
    }
    TString GetSessionId() const override { return Sessions.front()->GetSessionId(); }
private:
    const std::vector<std::shared_ptr<IMessageStreamReadSession>> Sessions;
    bool Closed = false;
    NThreading::TFuture<void> ClosedFuture;
};


}
std::shared_ptr<NFq::IMessageStreamReadSession> CreateQytPartitionReadSession(
    const TQytMessageStreamClientSettings& config, const NFq::TMessageStreamReadSessionSettings& settings,
    const NYT::NYPath::TRichYPath& queue, const NYT::NYPath::TRichYPath& consumer,
    int partition, ui64 start, ui64 minimum, ui64 end,
    std::function<NThreading::TFuture<void>(ui64)> commit, std::shared_ptr<TQytEventQueue> events) {
    auto control = std::make_shared<TQytPartitionSession>(partition, std::move(commit), start, minimum);
    NYT::NQueueClient::TQueueRowBatchReadOptions options;
    options.MaxRowCount = config.MaxRowCount;
    options.MaxDataWeight = config.MaxDataWeight;
    auto session = std::make_shared<TQytReadSession>(config.Client, queue, consumer,
        control, partition, start, config.DataColumn, options, TDuration::MilliSeconds(config.PollPeriodMs),
        !settings.AutoPartitioningSupport, end, settings.MaxMemoryUsageBytes,
        settings.RequireWriteTime || settings.ReadFromWriteTime.has_value(), std::move(events));
    session->StartPolling();
    return session;
}
std::shared_ptr<NFq::IMessageStreamReadSession> CreateQytMultiReadSession(
    std::vector<std::shared_ptr<NFq::IMessageStreamReadSession>> sessions) {
    return std::make_shared<TQytMultiReadSession>(std::move(sessions));
}
}
