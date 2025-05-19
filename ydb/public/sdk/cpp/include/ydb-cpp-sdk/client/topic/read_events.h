#pragma once

#include "codecs.h"
#include "control_plane.h"
#include "events_common.h"

#include <util/datetime/base.h>


namespace NYdb::inline Dev::NTopic {

//! Partition session.
struct TPartitionSession: public TThrRefBase, public TPrintable<TPartitionSession> {
    using TPtr = TIntrusivePtr<TPartitionSession>;

public:
    //! Request partition session status.
    //! Result will come to TPartitionSessionStatusEvent.
    virtual void RequestStatus() = 0;

    //!
    //! Properties.
    //!

    //! Unique identifier of partition session.
    //! It is unique within one read session.
    uint64_t GetPartitionSessionId() const {
        return PartitionSessionId;
    }

    //! Topic path.
    const std::string& GetTopicPath() const {
        return TopicPath;
    }

    //! Read session id.
    const std::string& GetReadSessionId() const {
        return ReadSessionId;
    }

    //! Partition id.
    uint64_t GetPartitionId() const {
        return PartitionId;
    }

protected:

    uint64_t PartitionSessionId;
    std::string TopicPath;
    std::string ReadSessionId;
    uint64_t PartitionId;
    std::optional<TPartitionLocation> Location;
    /*TDirectReadId*/ std::int64_t NextDirectReadId = 1;
    std::optional</*TDirectReadId*/ std::int64_t> LastDirectReadId;
};

template<>
void TPrintable<TPartitionSession>::DebugString(TStringBuilder& res, bool) const;

//! Events for read session.
struct TReadSessionEvent {
    class TPartitionSessionAccessor {
    public:
        TPartitionSessionAccessor(TPartitionSession::TPtr partitionSession);

        virtual ~TPartitionSessionAccessor() = default;

        virtual const TPartitionSession::TPtr& GetPartitionSession() const;

    protected:
        TPartitionSession::TPtr PartitionSession;
    };

    //! Event with new data.
    //! Contains batch of messages from single partition session.
    struct TDataReceivedEvent : public TPartitionSessionAccessor, public TPrintable<TDataReceivedEvent> {
        struct TMessageInformation {
            TMessageInformation(uint64_t offset,
                                std::string producerId,
                                uint64_t seqNo,
                                TInstant createTime,
                                TInstant writeTime,
                                TWriteSessionMeta::TPtr meta,
                                TMessageMeta::TPtr messageMeta,
                                uint64_t uncompressedSize,
                                std::string messageGroupId);
            uint64_t Offset;
            std::string ProducerId;
            uint64_t SeqNo;
            TInstant CreateTime;
            TInstant WriteTime;
            TWriteSessionMeta::TPtr Meta;
            TMessageMeta::TPtr MessageMeta;
            uint64_t UncompressedSize;
            std::string MessageGroupId;
        };

        class TMessageBase : public TPrintable<TMessageBase> {
        public:
            TMessageBase(const std::string& data, TMessageInformation info);

            virtual ~TMessageBase() = default;

            virtual const std::string& GetData() const;

            virtual void Commit() = 0;

            //! Message offset.
            uint64_t GetOffset() const;

            //! Producer id.
            const std::string& GetProducerId() const;

            //! Message group id.
            const std::string& GetMessageGroupId() const;

            //! Sequence number.
            uint64_t GetSeqNo() const;

            //! Message creation timestamp.
            TInstant GetCreateTime() const;

            //! Message write timestamp.
            TInstant GetWriteTime() const;

            //! Metainfo.
            const TWriteSessionMeta::TPtr& GetMeta() const;

            //! Message level meta info.
            const TMessageMeta::TPtr& GetMessageMeta() const;

        protected:
            std::string Data;
            TMessageInformation Information;
        };

        //! Single message.
        struct TMessage: public TMessageBase, public TPartitionSessionAccessor, public TPrintable<TMessage> {
            using TPrintable<TMessage>::DebugString;

            TMessage(const std::string& data, std::exception_ptr decompressionException, TMessageInformation information,
                     TPartitionSession::TPtr partitionSession);

            //! User data.
            //! Throws decompressor exception if decompression failed.
            const std::string& GetData() const override;

            //! Commits single message.
            void Commit() override;

            bool HasException() const;

        private:
            std::exception_ptr DecompressionException;
        };

        struct TCompressedMessage: public TMessageBase,
                                   public TPartitionSessionAccessor,
                                   public TPrintable<TCompressedMessage> {
            using TPrintable<TCompressedMessage>::DebugString;

            TCompressedMessage(ECodec codec, const std::string& data, TMessageInformation information,
                               TPartitionSession::TPtr partitionSession);

            virtual ~TCompressedMessage() {
            }

            //! Message codec
            ECodec GetCodec() const;

            //! Uncompressed size.
            uint64_t GetUncompressedSize() const;

            //! Commits all offsets in compressed message.
            void Commit() override;

        private:
            ECodec Codec;
        };

    public:
        TDataReceivedEvent(std::vector<TMessage> messages, std::vector<TCompressedMessage> compressedMessages,
                           TPartitionSession::TPtr partitionSession);

        bool HasCompressedMessages() const {
            return !CompressedMessages.empty();
        }

        size_t GetMessagesCount() const {
            return Messages.size() + CompressedMessages.size();
        }

        //! Get messages.
        std::vector<TMessage>& GetMessages() {
            CheckMessagesFilled(false);
            return Messages;
        }

        const std::vector<TMessage>& GetMessages() const {
            CheckMessagesFilled(false);
            return Messages;
        }

        //! Get compressed messages.
        std::vector<TCompressedMessage>& GetCompressedMessages() {
            CheckMessagesFilled(true);
            return CompressedMessages;
        }

        const std::vector<TCompressedMessage>& GetCompressedMessages() const {
            CheckMessagesFilled(true);
            return CompressedMessages;
        }

        void SetReadInTransaction() {
            ReadInTransaction = true;
        }

        //! Commits all messages in batch.
        void Commit();

    private:
        void CheckMessagesFilled(bool compressed) const {
            Y_ABORT_UNLESS(!Messages.empty() || !CompressedMessages.empty());
            if (compressed && CompressedMessages.empty()) {
                ythrow yexception() << "cannot get compressed messages, parameter decompress=true for read session";
            }
            if (!compressed && Messages.empty()) {
                ythrow yexception() << "cannot get decompressed messages, parameter decompress=false for read session";
            }
        }

    private:
        std::vector<TMessage> Messages;
        std::vector<TCompressedMessage> CompressedMessages;
        std::vector<std::pair<uint64_t, uint64_t>> OffsetRanges;
        bool ReadInTransaction = false;
    };

    //! Acknowledgement for commit request.
    struct TCommitOffsetAcknowledgementEvent: public TPartitionSessionAccessor,
                                              public TPrintable<TCommitOffsetAcknowledgementEvent> {
        TCommitOffsetAcknowledgementEvent(TPartitionSession::TPtr partitionSession, uint64_t committedOffset);

        //! Committed offset.
        //! This means that from now the first available
        //! message offset in current partition
        //! for current consumer is this offset.
        //! All messages before are committed and futher never be available.
        uint64_t GetCommittedOffset() const {
            return CommittedOffset;
        }

    private:
        uint64_t CommittedOffset;
    };

    //! Server command for creating and starting partition session.
    struct TStartPartitionSessionEvent: public TPartitionSessionAccessor,
                                        public TPrintable<TStartPartitionSessionEvent> {
        explicit TStartPartitionSessionEvent(TPartitionSession::TPtr, uint64_t committedOffset, uint64_t endOffset);

        //! Current committed offset in partition session.
        uint64_t GetCommittedOffset() const {
            return CommittedOffset;
        }

        //! Offset of first not existing message in partition session.
        uint64_t GetEndOffset() const {
            return EndOffset;
        }

        //! Confirm partition session creation.
        //! This signals that user is ready to receive data from this partition session.
        //! If maybe is empty then no rewinding
        void Confirm(std::optional<uint64_t> readOffset = std::nullopt, std::optional<uint64_t> commitOffset = std::nullopt);

    private:
        uint64_t CommittedOffset;
        uint64_t EndOffset;
    };

    //! Server command for stopping and destroying partition session.
    //! Server can destroy partition session gracefully
    //! for rebalancing among all topic clients.
    struct TStopPartitionSessionEvent: public TPartitionSessionAccessor, public TPrintable<TStopPartitionSessionEvent> {
        TStopPartitionSessionEvent(TPartitionSession::TPtr partitionSession, uint64_t committedOffset);

        //! Last offset of the partition session that was committed.
        uint64_t GetCommittedOffset() const {
            return CommittedOffset;
        }

        //! Confirm partition session destruction.
        //! Confirm has no effect if TPartitionSessionClosedEvent for same partition session with is received.
        void Confirm();

    private:
        uint64_t CommittedOffset;
    };

    //! Server command for ending partition session.
    //! This is a hint that all messages from the partition have been read and will no longer appear, and that the client must commit offsets.
    struct TEndPartitionSessionEvent: public TPartitionSessionAccessor, public TPrintable<TEndPartitionSessionEvent> {
        TEndPartitionSessionEvent(TPartitionSession::TPtr partitionSession, std::vector<uint32_t>&& adjacentPartitionIds, std::vector<uint32_t>&& childPartitionIds);

        //! A list of the partition IDs that also participated in the partition's merge.
        const std::vector<uint32_t>& GetAdjacentPartitionIds() const {
            return AdjacentPartitionIds;
        }

        //! A list of partition IDs that were obtained as a result of merging or splitting this partition.
        const std::vector<uint32_t>& GetChildPartitionIds() const {
            return ChildPartitionIds;
        }

        //! Confirm partition session destruction.
        //! Confirm has no effect if TPartitionSessionClosedEvent for same partition session with is received.
        void Confirm();

    private:
        std::vector<uint32_t> AdjacentPartitionIds;
        std::vector<uint32_t> ChildPartitionIds;
    };

    //! Status for partition session requested via TPartitionSession::RequestStatus()
    struct TPartitionSessionStatusEvent: public TPartitionSessionAccessor,
                                         public TPrintable<TPartitionSessionStatusEvent> {
        TPartitionSessionStatusEvent(TPartitionSession::TPtr partitionSession, uint64_t committedOffset, uint64_t readOffset,
                                     uint64_t endOffset, TInstant writeTimeHighWatermark);

        //! Committed offset.
        uint64_t GetCommittedOffset() const {
            return CommittedOffset;
        }

        //! Offset of next message (that is not yet read by session).
        uint64_t GetReadOffset() const {
            return ReadOffset;
        }

        //! Offset of first not existing message in partition.
        uint64_t GetEndOffset() const {
            return EndOffset;
        }

        //! Write time high watermark.
        //! Write timestamp of next message written to this partition will be no less than this.
        TInstant GetWriteTimeHighWatermark() const {
            return WriteTimeHighWatermark;
        }

    private:
        uint64_t CommittedOffset = 0;
        uint64_t ReadOffset = 0;
        uint64_t EndOffset = 0;
        TInstant WriteTimeHighWatermark;
    };

    //! Event that signals user about
    //! partition session death.
    //! This could be after graceful stop of partition session
    //! or when connection with partition was lost.
    struct TPartitionSessionClosedEvent: public TPartitionSessionAccessor,
                                         public TPrintable<TPartitionSessionClosedEvent> {
        enum class EReason {
            StopConfirmedByUser,
            Lost,
            ConnectionLost,
        };

    public:
        TPartitionSessionClosedEvent(TPartitionSession::TPtr partitionSession, EReason reason);

        EReason GetReason() const {
            return Reason;
        }

    private:
        EReason Reason;
    };

    using TEvent = std::variant<TDataReceivedEvent,
                                TCommitOffsetAcknowledgementEvent,
                                TStartPartitionSessionEvent,
                                TStopPartitionSessionEvent,
                                TEndPartitionSessionEvent,
                                TPartitionSessionStatusEvent,
                                TPartitionSessionClosedEvent,
                                TSessionClosedEvent>;
};

//! Set of offsets to commit.
//! Class that could store offsets in order to commit them later.
//! This class is not thread safe.
class TDeferredCommit {
public:
    //! Add message to set.
    void Add(const TReadSessionEvent::TDataReceivedEvent::TMessage& message);

    //! Add all messages from dataReceivedEvent to set.
    void Add(const TReadSessionEvent::TDataReceivedEvent& dataReceivedEvent);

    //! Add offsets range to set.
    void Add(const TPartitionSession::TPtr& partitionSession, uint64_t startOffset, uint64_t endOffset);

    //! Add offset to set.
    void Add(const TPartitionSession::TPtr& partitionSession, uint64_t offset);

    //! Commit all added offsets.
    void Commit();

    TDeferredCommit();
    TDeferredCommit(const TDeferredCommit&) = delete;
    TDeferredCommit(TDeferredCommit&&);
    TDeferredCommit& operator=(const TDeferredCommit&) = delete;
    TDeferredCommit& operator=(TDeferredCommit&&);

    ~TDeferredCommit();

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl;
};

//! Events debug strings.
template<>
void TPrintable<TReadSessionEvent::TDataReceivedEvent::TMessageBase>::DebugString(TStringBuilder& ret, bool printData) const;
template<>
void TPrintable<TReadSessionEvent::TDataReceivedEvent::TMessage>::DebugString(TStringBuilder& ret, bool printData) const;
template<>
void TPrintable<TReadSessionEvent::TDataReceivedEvent::TCompressedMessage>::DebugString(TStringBuilder& ret, bool printData) const;
template<>
void TPrintable<TReadSessionEvent::TDataReceivedEvent>::DebugString(TStringBuilder& ret, bool printData) const;
template<>
void TPrintable<TReadSessionEvent::TCommitOffsetAcknowledgementEvent>::DebugString(TStringBuilder& ret, bool printData) const;
template<>
void TPrintable<TReadSessionEvent::TStartPartitionSessionEvent>::DebugString(TStringBuilder& ret, bool printData) const;
template<>
void TPrintable<TReadSessionEvent::TStopPartitionSessionEvent>::DebugString(TStringBuilder& ret, bool printData) const;
template<>
void TPrintable<TReadSessionEvent::TPartitionSessionStatusEvent>::DebugString(TStringBuilder& ret, bool printData) const;
template<>
void TPrintable<TReadSessionEvent::TPartitionSessionClosedEvent>::DebugString(TStringBuilder& ret, bool printData) const;
template<>
void TPrintable<TReadSessionEvent::TEndPartitionSessionEvent>::DebugString(TStringBuilder& ret, bool printData) const;
template<>
void TPrintable<TSessionClosedEvent>::DebugString(TStringBuilder& ret, bool printData) const;

std::string DebugString(const TReadSessionEvent::TEvent& event);

}
