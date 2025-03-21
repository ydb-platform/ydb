#pragma once

#include "aliases.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/write_events.h>

#include <util/datetime/base.h>

namespace NYdb::inline Dev::NPersQueue {
    
struct TWriteStat : public TThrRefBase {
    TDuration WriteTime;
    TDuration TotalTimeInPartitionQueue;
    TDuration PartitionQuotedTime;
    TDuration TopicQuotedTime;
    using TPtr = TIntrusivePtr<TWriteStat>;
};

using NTopic::TContinuationToken;
using NTopic::TContinuationTokenIssuer;

//! Events for write session.
struct TWriteSessionEvent {

    //! Event with acknowledge for written messages.
    struct TWriteAck {
        //! Write result.
        enum EEventState {
            EES_WRITTEN, //! Successfully written.
            EES_ALREADY_WRITTEN, //! Skipped on SeqNo deduplication.
            EES_DISCARDED //! In case of destruction of writer or retry policy discarded future retries in this writer.
        };
        //! Details of successfully written message.
        struct TWrittenMessageDetails {
            ui64 Offset;
            ui64 PartitionId;
        };
        //! Same SeqNo as provided on write.
        ui64 SeqNo;
        EEventState State;
        //! Filled only for EES_WRITTEN. Empty for ALREADY and DISCARDED.
        std::optional<TWrittenMessageDetails> Details;
        //! Write stats from server. See TWriteStat. nullptr for DISCARDED event.
        TWriteStat::TPtr Stat;

    };

    struct TAcksEvent {
        //! Acks could be batched from several WriteBatch/Write requests.
        //! Acks for messages from one WriteBatch request could be emitted as several TAcksEvents -
        //! they are provided to client as soon as possible.
        std::vector<TWriteAck> Acks;

        std::string DebugString() const;

    };

    //! Indicates that a writer is ready to accept new message(s).
    //! Continuation token should be kept and then used in write methods.
    struct TReadyToAcceptEvent {
        mutable TContinuationToken ContinuationToken;

        TReadyToAcceptEvent() = delete;
        TReadyToAcceptEvent(TContinuationToken&& t) : ContinuationToken(std::move(t)) {
        }
        TReadyToAcceptEvent(TReadyToAcceptEvent&&) = default;
        TReadyToAcceptEvent(const TReadyToAcceptEvent& other) : ContinuationToken(std::move(other.ContinuationToken)) {
        }
        TReadyToAcceptEvent& operator=(TReadyToAcceptEvent&&) = default;
        TReadyToAcceptEvent& operator=(const TReadyToAcceptEvent& other) {
            ContinuationToken = std::move(other.ContinuationToken);
            return *this;
        }

        std::string DebugString() const;
    };

    using TEvent = std::variant<TAcksEvent, TReadyToAcceptEvent, TSessionClosedEvent>;
};

//! Event debug string.
std::string DebugString(const TWriteSessionEvent::TEvent& event);

}  // namespace NYdb::NPersQueue
