#pragma once

#include "write_session.h"
#include "control_plane.h"

namespace NYdb::NTopic {

//! Write result.
//! If write was successfully added ti buffer, returns QUEUED.
//! If write was not successful due to overloaded buffer, returns OVERLOADED.
//! If write was not successful because of closed session, returns CLOSED.
enum class EWriteResult : uint8_t {
    QUEUED = 0,
    OVERLOADED = 1,
    CLOSED = 2,
    TIMEOUT = 3,
};

//! Result of flush operation.
//! If flush was successful, returns SUCCESS.
//! If flush was not successful because of closed session, returns CLOSED.
//! If flush was not successful because of timeout, returns TIMEOUT.
enum class EFlushResult : uint8_t {
    SUCCESS = 0,
    CLOSED = 1,
    TIMEOUT = 2,
};

//! Description why session was closed.
struct TCloseDescription : public TSessionClosedEvent {};

//! Result of flush operation.
//! If flush was successful, returns SUCCESS.
//! If flush was not successful because of closed session, returns CLOSED.
//! If flush was not successful because of timeout, returns TIMEOUT.
struct TFlushResult {
    //! Status of flush operation.
    EFlushResult Status;
    //! Last written sequence number.
    std::uint64_t LastWrittenSeqNo;
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
    [[nodiscard]] virtual EWriteResult Write(TWriteMessage&& message) = 0;

    //! Explain why session was closed.
    //! Returns session closed event if session was closed.
    //! Returns std::nullopt if session is not closed.
    virtual std::optional<TCloseDescription> ExplainClosed() = 0;

    //! Flush all messages to the server.
    //! Returns future that is set when flush is complete.
    //! If flush was successful, returns SUCCESS.
    //! If flush was not successful because of closed session, returns CLOSED.
    [[nodiscard]] virtual NThreading::TFuture<EFlushResult> Flush() = 0;

    //! Close the producer.
    //! Returns close result.
    //! If close was successful, returns SUCCESS.
    //! If close was not successful because of timeout, returns TIMEOUT.
    //! If close was not successful because of error, returns ERROR.
    //! DO NOT IGNORE THE RETURN VALUE.
    [[nodiscard]] virtual ECloseResult Close(TDuration closeTimeout = TDuration::Max()) = 0;

    //! Get statistics of write operations.
    //! Returns statistics of write operations.
    virtual TWriteStats GetWriteStats() = 0;

    virtual ~IProducer() = default;
};

//! Typed producer.
//! Typed producer is a producer that can write messages of a specific type.
//! It has the same interface as IProducer, but the messages are typed.
//! It is more efficient than IProducer because it does not need to serialize and deserialize messages.
template<typename T>
class ITypedProducer : public IProducer {
public:
    //! Write single message.
    //! Returns write result.
    [[nodiscard]] EWriteResult Write(TWriteMessage&& message) override {
        return this->Write(std::move(message));
    }
};

} // namespace NYdb::NTopic