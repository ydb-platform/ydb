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

struct TCloseDescription : public TSessionClosedEvent {};

//! Producer is an abstraction that can write messages to the topic.
//! It has two versions of Write method:
//! - Write without key (partition is chosen randomly)
//! - Write with key (partition is chosen based on the key)
//! EXPERIMENTAL SDK, DO NOT USE IN PRODUCTION.
class IProducer {
public:
    //! Write single message.
    //! Returns write result.
    //! If write was successful, returns QUEUED.
    //! If write was not successful due to overloaded buffer, returns OVERLOADED.
    //! If write was not successful because of closed session, returns CLOSED.
    //! DO NOT IGNORE THE RETURN VALUE.
    [[nodiscard]] virtual EWriteResult Write(TWriteMessage&& message,
        TTransactionBase* tx = nullptr) = 0;

    //! Write single message.
    //! key - message key.
    //! Returns write result.
    //! If write was successful, returns QUEUED.
    //! If write was not successful due to overloaded buffer, returns OVERLOADED.
    //! If write was not successful because of closed session, returns CLOSED.
    //! DO NOT IGNORE THE RETURN VALUE.
    [[nodiscard]] virtual EWriteResult Write(const std::string& key, TWriteMessage&& message,
        TTransactionBase* tx = nullptr) = 0;

    //! Write single message to partition.
    //! partition - partition ID.
    //! Returns write result.
    //! If write was successful, returns QUEUED.
    //! If write was not successful due to overloaded buffer, returns OVERLOADED.
    //! If write was not successful because of closed session, returns CLOSED.
    //! DO NOT IGNORE THE RETURN VALUE.
    [[nodiscard]] virtual EWriteResult Write(std::uint32_t partition, TWriteMessage&& message,
        TTransactionBase* tx = nullptr) = 0;

    //! Explain why session was closed.
    //! Returns session closed event if session was closed.
    //! Returns std::nullopt if session is not closed.
    virtual std::optional<TCloseDescription> ExplainClosed() = 0;

    //! Flush all messages to the server.
    //! Returns future that is set when flush is complete.
    //! If flush was successful, returns SUCCESS.
    //! If flush was not successful because of closed session, returns CLOSED.
    virtual NThreading::TFuture<EFlushResult> Flush() = 0;

    //! Flush all messages to the server and wait result.
    //! Returns flush result.
    [[nodiscard]] virtual EFlushResult FlushAndWait(TDuration timeout = TDuration::Max()) = 0;

    //! Close the producer.
    //! Returns close result.
    //! If close was successful, returns SUCCESS.
    //! If close was not successful because of timeout, returns TIMEOUT.
    //! If close was not successful because of error, returns ERROR.
    //! DO NOT IGNORE THE RETURN VALUE.
    [[nodiscard]] virtual ECloseResult Close(TDuration closeTimeout = TDuration::Max()) = 0;

    virtual ~IProducer() = default;
};

} // namespace NYdb::NTopic