#pragma once

#include "write_session.h"
#include "control_plane.h"

namespace NYdb::NTopic {

//! Write status.
//! If write was successfully added ti buffer, returns QUEUED.
//! If write was not successful due to overloaded buffer, returns OVERLOADED.
//! If write was not successful because of closed session, returns CLOSED.
enum class EWriteStatus : uint8_t {
    QUEUED = 0,
    OVERLOADED = 1,
    CLOSED = 2,
    TIMEOUT = 3,
    ERROR = 4,
};

//! Flush status.
//! If flush was successful, returns SUCCESS.
//! If flush was not successful because of closed session, returns CLOSED.
//! If flush was not successful because of timeout, returns TIMEOUT.
enum class EFlushStatus : uint8_t {
    SUCCESS = 0,
    CLOSED = 1,
    TIMEOUT = 2,
};

//! Result of write operation.
//! If write was successful, returns SUCCESS.
//! If write was not successful due to overloaded buffer, returns OVERLOADED.
//! If write was not successful because of closed session, returns CLOSED.
//! If write was not successful because of timeout, returns TIMEOUT.
//! If write was not successful because of error, returns ERROR.
struct TWriteResult {
    //! Status of write operation.
    EWriteStatus Status;
    //! Error message.
    //! Value is empty if the write was successful.
    std::optional<std::string> ErrorMessage = std::nullopt;

    bool IsSuccess() const {
        return Status == EWriteStatus::QUEUED;
    }

    bool IsOverloaded() const {
        return Status == EWriteStatus::OVERLOADED;
    }

    bool IsClosed() const {
        return Status == EWriteStatus::CLOSED;
    }

    bool IsTimeout() const {
        return Status == EWriteStatus::TIMEOUT;
    }

    bool IsError() const {
        return Status == EWriteStatus::ERROR;
    }
};

//! Result of flush operation.
//! If flush was successful, returns SUCCESS.
//! If flush was not successful because of closed session, returns CLOSED.
//! If flush was not successful because of timeout, returns TIMEOUT.
struct TFlushResult {
    //! Status of flush operation.
    EFlushStatus Status;
    //! Last written sequence number.
    std::uint64_t LastWrittenSeqNo;
    //! Description why session was closed.
    //! Value is std::nullopt if the session is not closed.
    std::optional<TCloseDescription> ClosedDescription;

    bool IsSuccess() const {
        return Status == EFlushStatus::SUCCESS;
    }

    bool IsClosed() const {
        return Status == EFlushStatus::CLOSED;
    }

    bool IsTimeout() const {
        return Status == EFlushStatus::TIMEOUT;
    }
};

//! Statistics of write operations.
struct TWriteStats {
    //! Last written sequence number. If messages do not have sequence numbers, returns std::nullopt.
    std::optional<std::uint64_t> LastWrittenSeqNo;
    //! Number of messages written.
    std::uint64_t MessagesWritten;
};

//! Result of write operation.

//! Producer is an abstraction that can write messages to the topic.
//! It has three versions of Write method:
//! - Write without key and partition (partition is chosen randomly by uniform distribution)
//! - Write with key (partition is chosen based on the key)
//! - Write with partition
//! EXPERIMENTAL SDK, DO NOT USE IN PRODUCTION.
class IProducer {
public:
    //! Write single message to partition.
    //! Returns write result.
    //! If write was successful, returns QUEUED.
    //! If write was not successful due to overloaded buffer, returns OVERLOADED.
    //! If write was not successful because of closed session, returns CLOSED.
    //! DO NOT IGNORE THE RETURN VALUE.
    [[nodiscard]] virtual TWriteResult Write(TWriteMessage&& message) = 0;

    //! Flush all messages to the server.
    //! Returns future that is set when flush is complete.
    //! If flush was successful, returns TFlushResult with Status SUCCESS and LastWrittenSeqNo set to the last written sequence number.
    //! If flush was not successful because of closed session, returns TFlushResult with Status CLOSED and ClosedDescription set to the description why session was closed.
    //! If flush was not successful because of timeout, returns TFlushResult with Status TIMEOUT.
    [[nodiscard]] virtual NThreading::TFuture<TFlushResult> Flush() = 0;

    //! Close the producer.
    //! Returns close result.
    //! If close was successful, returns SUCCESS.
    //! If close was not successful because of timeout, returns TIMEOUT.
    //! If close was not successful because of error, returns ERROR.
    //! DO NOT IGNORE THE RETURN VALUE.
    [[nodiscard]] virtual TCloseResult Close(TDuration closeTimeout = TDuration::Max()) = 0;

    //! Get statistics of write operations.
    //! Returns statistics of write operations.
    virtual TWriteStats GetWriteStats() = 0;

    virtual ~IProducer() = default;
};

//! Typed producer.
//! Typed producer is a producer that can write messages of a specific type.
//! It has the same interface as IProducer, but the messages are typed.
//! It is more efficient than IProducer because it does not need to serialize and deserialize messages.
//! Derived class must override Write(TWriteMessage&&) from IProducer.
template<typename T>
class ITypedProducer : public IProducer {
public:
    //! Write single message. Forwards to IProducer::Write (virtual call to derived class).
    [[nodiscard]] TWriteResult Write(T&& message) {
        return this->Write(std::move(message));
    }
};

} // namespace NYdb::NTopic