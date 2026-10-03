#pragma once

#include <library/cpp/threading/future/core/future.h>

#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <yql/essentials/public/issue/yql_issue.h>

#include <util/generic/string.h>
#include <util/datetime/base.h>
#include <util/system/types.h>

#include <limits>
#include <memory>
#include <optional>
#include <variant>
#include <vector>

namespace NFq {

// Categories match the server status codes callers already expose.
// Statuses that are not server codes stay Unknown: control plane reports them as
// EXTERNAL_ERROR, and the row dispatcher reports them as GENERIC_ERROR.
enum class EMessageStreamStatus {
    Success,
    NotFound,
    Unauthorized,
    InvalidArgument,
    Unavailable,
    InternalError,
    Unknown,
    SchemeError,
    PreconditionFailed,
    Aborted,
    Overloaded,
    Timeout,
    Cancelled,
    Unsupported,
    Undetermined,
    External,
    BadSession,
    GenericError,
    AlreadyExists,
    SessionExpired,
    SessionBusy,
};

inline std::optional<Ydb::StatusIds::StatusCode> ToYdbStatus(EMessageStreamStatus status) {
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

struct TMessageStreamOffset {
    ui64 PartitionId = 0;
    // Offset of this message within its partition.
    ui64 Offset = 0;
};

struct TMessageStreamMessage {
    TString Data;
    std::optional<TString> Key;
    TMessageStreamOffset Offset;
    TInstant CreateTime;
    TInstant WriteTime;
    TString MessageGroupId;
    ui64 SeqNo = 0;
    std::vector<std::pair<TString, TString>> Attributes;
    std::optional<TString> DecompressionError;
};

struct TMessageStreamRetrySettings {
    TDuration MinDelay = TDuration::MilliSeconds(500);
    TDuration MinLongRetryDelay = TDuration::Seconds(5);
    TDuration MaxDelay = TDuration::Seconds(20);
    ui32 MaxRetries = 100;
    TDuration MaxTime = TDuration::Seconds(60);
    double ScaleFactor = 2.0;
    bool RetryAuthenticationErrors = false;
};

struct TMessageStreamReadSettings {
    TString Stream;
    TString Consumer;
    std::optional<ui64> PartitionId;
    std::vector<ui64> PartitionIds;
    std::optional<TInstant> StartTime;
    bool WithoutConsumer = false;
    bool AutoPartitioningSupport = true;
    ui64 MaxMemoryUsageBytes = 0;
    TString TraceId;
    std::optional<TMessageStreamRetrySettings> Retry;
};

class IMessageStreamPartitionControl {
public:
    virtual ~IMessageStreamPartitionControl() = default;

    virtual ui64 GetPartitionId() const = 0;
    virtual void ConfirmStart(std::optional<ui64> startOffset, std::optional<ui64> maxOffset) = 0;
    virtual void ConfirmStop() = 0;
    virtual void RequestStatus() = 0;
    virtual void Commit(ui64 startOffset, ui64 endOffset) = 0;
};

struct TMessageStreamDataEvent {
    std::shared_ptr<IMessageStreamPartitionControl> Partition;
    std::vector<TMessageStreamMessage> Messages;
};

struct TMessageStreamPartitionStartedEvent {
    std::shared_ptr<IMessageStreamPartitionControl> Partition;
    ui64 CommittedOffset = 0;
    ui64 EndOffset = 0;
};

struct TMessageStreamPartitionStoppedEvent {
    std::shared_ptr<IMessageStreamPartitionControl> Partition;
};

struct TMessageStreamPartitionEndedEvent {
    std::shared_ptr<IMessageStreamPartitionControl> Partition;
};

struct TMessageStreamPartitionStatusEvent {
    std::shared_ptr<IMessageStreamPartitionControl> Partition;
    ui64 CommittedOffset = 0;
    ui64 ReadOffset = 0;
    ui64 EndOffset = 0;
    std::optional<TInstant> WriteTimeHighWatermark;
};

struct TMessageStreamPartitionClosedEvent {
    std::shared_ptr<IMessageStreamPartitionControl> Partition;
};

struct TMessageStreamSessionClosedEvent {
    EMessageStreamStatus Status = EMessageStreamStatus::Unknown;
    NYql::TIssues Issues;
};

using TMessageStreamReadEvent = std::variant<
    TMessageStreamDataEvent,
    TMessageStreamPartitionStartedEvent,
    TMessageStreamPartitionStoppedEvent,
    TMessageStreamPartitionEndedEvent,
    TMessageStreamPartitionStatusEvent,
    TMessageStreamPartitionClosedEvent,
    TMessageStreamSessionClosedEvent>;

struct TMessageStreamReadEventSettings {
    bool Block = false;
    std::optional<size_t> MaxEventsCount;
    size_t MaxByteSize = std::numeric_limits<size_t>::max();
};

class IMessageStreamReadSession {
public:
    virtual ~IMessageStreamReadSession() = default;

    virtual NThreading::TFuture<void> WaitEvent() = 0;
    virtual std::vector<TMessageStreamReadEvent> GetEvents(const TMessageStreamReadEventSettings& settings) = 0;
    virtual NThreading::TFuture<void> CommitOffset(const TMessageStreamOffset& offset) = 0;
    virtual NThreading::TFuture<void> Close() = 0;
    virtual TString GetSessionId() const = 0;
};

} // namespace NFq
