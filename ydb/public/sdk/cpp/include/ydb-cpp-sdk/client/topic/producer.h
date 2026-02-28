#pragma once

#include "write_session.h"

#include <memory>
#include <utility>

namespace NYdb::NTopic {

struct TProducerSettings : public TWriteSessionSettings {
    using TSelf = TProducerSettings;

    enum class EPartitionChooserStrategy {
        Bound,
        Hash,
    };

    TProducerSettings() = default;
    TProducerSettings(const TProducerSettings&) = default;
    TProducerSettings(TProducerSettings&&) = default;

    TProducerSettings& operator=(const TProducerSettings&) = default;
    TProducerSettings& operator=(TProducerSettings&&) = default;

    //! Session lifetime.
    FLUENT_SETTING_DEFAULT(TDuration, SubSessionIdleTimeout, TDuration::Seconds(30));

    //! Partition chooser strategy.
    FLUENT_SETTING_DEFAULT(EPartitionChooserStrategy, PartitionChooserStrategy, EPartitionChooserStrategy::Bound);

    //! Hasher function.
    FLUENT_SETTING_DEFAULT(std::function<std::string(const std::string_view key)>, PartitioningKeyHasher, DefaultPartitioningKeyHasher);

    //! Default partitioning key hasher.
    //! Uses MurmurHash.
    static std::string DefaultPartitioningKeyHasher(const std::string_view key);

    //! ProducerId prefix to use.
    //! ProducerId is generated as ProducerIdPrefix + partition id.
    FLUENT_SETTING(std::string, ProducerIdPrefix);

    //! SessionID to use.
    FLUENT_SETTING_DEFAULT(std::string, SessionId, "");

    //! Maximum block time for write. If set, write will block for up to MaxBlockMs when the buffer is overloaded.
    FLUENT_SETTING_DEFAULT(TDuration, MaxBlock, TDuration::Zero());

    //! Key producer function.
    FLUENT_SETTING_OPTIONAL(std::function<std::string(const TWriteMessage& message)>, KeyProducer);

private:
    using TWriteSessionSettings::ProducerId;
};

//! Write status.
//! If write was successfully added to buffer, returns Queued.
//! If write was not successful because of closed session, returns Closed.
//! If write was not successful because of timeout, returns Timeout. Usually it means that the producer's buffer is overloaded. You can try to increase MaxBlock setting or try to write later.
//! If write was not successful because of error, returns Error.
enum class EWriteStatus {
    Queued = 0,
    Timeout = 1,
    Error = 2,
};

//! Flush status.
//! If flush was successful, returns Success. This status means that all messages in buffer were persistently written to the server.
//! If flush was not successful because of closed session, returns Closed.
//! If flush was not successful because of timeout, returns Timeout.
enum class EFlushStatus {
    Success = 0,
    ProducerClosed = 1,
};

//! Result of write operation.
//! If write was successful, returns Queued. This status means that write was successfully added to buffer.
//! If write was not successful due to overloaded buffer, returns Overloaded.
//! If write was not successful because of closed session, returns Closed.
//! If write was not successful because of timeout, returns Timeout. Usually it means that the producer's buffer is overloaded. You can try to increase MaxBlock setting or try to write later.
//! If write was not successful because of error, returns Error.
struct TWriteResult {
    //! Status of write operation.
    EWriteStatus Status;
    //! Error message.
    //! Value is empty if the write was successful.
    std::optional<std::string> ErrorMessage = std::nullopt;
    //! Description why session was closed.
    //! Value is std::nullopt if the session is not closed.
    std::optional<TCloseDescription> ClosedDescription;

    bool IsSuccess() const {
        return Status == EWriteStatus::Queued;
    }

    bool IsTimeout() const {
        return Status == EWriteStatus::Timeout;
    }

    bool IsError() const {
        return Status == EWriteStatus::Error;
    }
};

//! Result of flush operation.
//! If flush was successful, returns Success. This status means that all messages in buffer were persistently written to the server.
//! If flush was not successful because of closed session, returns Closed.
//! If flush was not successful because of timeout, returns Timeout.
struct TFlushResult {
    //! Status of flush operation.
    EFlushStatus Status;
    //! Last written sequence number.
    std::uint64_t LastWrittenSeqNo;
    //! Description why session was closed.
    //! Value is std::nullopt if the session is not closed.
    std::optional<TCloseDescription> ClosedDescription;

    bool IsSuccess() const {
        return Status == EFlushStatus::Success;
    }

    bool IsClosed() const {
        return Status == EFlushStatus::ProducerClosed;
    }
};

//! Statistics of write operations.
struct TWriteStats {
    //! Last written sequence number. If messages do not have sequence numbers, returns std::nullopt.
    std::optional<std::uint64_t> LastWrittenSeqNo;
    //! Number of messages written.
    std::uint64_t MessagesWritten;
};

//! Producer is an abstraction that can write messages to the topic.
//! It has three versions of Write method:
//! - Write without key and partition (partition is chosen by key generated by KeyProducer function in ProducerSettings or by ProducerIdPrefix interpreted as key)
//! - Write with key (partition is chosen based on the key)
//! - Write with partition
//! EXPERIMENTAL SDK, DO NOT USE IN PRODUCTION.
class IProducer {
public:
    //! Write single message to partition.
    //! Returns write result.
    //! If write was successful, returns Queued.
    //! If write was not successful due to overloaded buffer, returns Overloaded.
    //! If write was not successful because of closed session, returns ProducerClosed.
    //! DO NOT IGNORE THE RETURN VALUE.
    [[nodiscard]] virtual TWriteResult Write(TWriteMessage&& message) = 0;

    //! Flush all messages to the server.
    //! Returns future that is set when flush is complete.
    //! If flush was successful, returns TFlushResult with Status Success and LastWrittenSeqNo set to the last written sequence number.
    //! If flush was not successful because of closed session, returns TFlushResult with Status ProducerClosed and ClosedDescription set to the description why session was closed.
    //! If flush was not successful because of timeout, returns TFlushResult with Status Timeout.
    [[nodiscard]] virtual NThreading::TFuture<TFlushResult> Flush() = 0;

    //! Close the producer.
    //! Returns close result.
    //! If close was successful, returns Success. This status means that all messages in buffer were persistently written to the server.
    //! If close was not successful because of timeout, returns Timeout.
    //! If close was not successful because of error, returns Error.
    //! DO NOT IGNORE THE RETURN VALUE.
    [[nodiscard]] virtual TCloseResult Close(TDuration closeTimeout = TDuration::Max()) = 0;

    //! Get statistics of write operations.
    //! Returns statistics of write operations.
    virtual TWriteStats GetWriteStats() = 0;

    virtual ~IProducer() = default;
};

//! Typed producer — inherits from IProducer; adds only Write(T&&).
//! Type T is implicitly converted to TWriteMessage (e.g. via TWriteMessage's template constructor).
template<typename T>
class TTypedProducer {
public:
    explicit TTypedProducer(std::shared_ptr<IProducer> impl)
        : Impl_(impl)
    {}

    //! Write single message. T converts to TWriteMessage, then IProducer::Write is called.
    [[nodiscard]] TWriteResult Write(T&& message) {
        return Impl_->Write(TWriteMessage(std::forward<T>(message)));
    }

    [[nodiscard]] NThreading::TFuture<TFlushResult> Flush() {
        return Impl_->Flush();
    }

    [[nodiscard]] TCloseResult Close(TDuration closeTimeout = TDuration::Max()) {
        return Impl_->Close(closeTimeout);
    }

    TWriteStats GetWriteStats() {
        return Impl_->GetWriteStats();
    }

private:
    std::shared_ptr<IProducer> Impl_;
};

} // namespace NYdb::NTopic