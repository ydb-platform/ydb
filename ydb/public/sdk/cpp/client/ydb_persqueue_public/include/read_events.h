#pragma once

#include "aliases.h"

#include <util/datetime/base.h>

namespace NYdb::NPersQueue {

//! Partition stream.
struct TPartitionStream : public TThrRefBase {
    using TPtr = TIntrusivePtr<TPartitionStream>;


public:

    //! Temporary stop receiving data from this partition stream.
    // virtual void StopReading() = 0; // Not implemented yet.

    //! Resume receiving data from this partition stream after StopReading() call.
    // virtual void ResumeReading() = 0; // Not implemented yet.

    //! Request partition stream status.
    //! Result will come to TPartitionStreamStatusEvent.
    virtual void RequestStatus() = 0;

    //!
    //! Properties.
    //!

    //! Unique identifier of partition stream inside session.
    //! It is unique inside one read session.
    ui64 GetPartitionStreamId() const {
        return PartitionStreamId;
    }

    //! Topic path.
    const TString& GetTopicPath() const {
        return TopicPath;
    }

    //! Cluster name.
    const TString& GetCluster() const {
        return Cluster;
    }

    //! Partition group id.
    ui64 GetPartitionGroupId() const {
        return PartitionGroupId;
    }

    //! Partition id.
    ui64 GetPartitionId() const {
        return PartitionId;
    }

protected:
    ui64 PartitionStreamId;
    TString TopicPath;
    TString Cluster;
    ui64 PartitionGroupId;
    ui64 PartitionId;
};


//! Events for read session.
struct TReadSessionEvent {

    //! Event with new data.
    //! Contains batch of messages from single partition stream.
    struct TDataReceivedEvent {

        struct TMessageInformation {
            TMessageInformation(ui64 offset,
                                TString messageGroupId,
                                ui64 seqNo,
                                TInstant createTime,
                                TInstant writeTime,
                                TString ip,
                                TWriteSessionMeta::TPtr meta,
                                ui64 uncompressedSize);
            ui64 Offset;
            TString MessageGroupId;
            ui64 SeqNo;
            TInstant CreateTime;
            TInstant WriteTime;
            TString Ip;
            TWriteSessionMeta::TPtr Meta;
            ui64 UncompressedSize;
        };

        class IMessage {
        public:
            virtual const TString& GetData() const;

            //! Partition stream. Same as in batch.
            const TPartitionStream::TPtr& GetPartitionStream() const;

            const TString& GetPartitionKey() const;

            const TString GetExplicitHash() const;

            virtual void Commit() = 0;

            TString DebugString(bool printData = false) const;
            virtual void DebugString(TStringBuilder& ret, bool printData = false) const = 0;

            IMessage(const TString& data,
                     TPartitionStream::TPtr partitionStream,
                     const TString& partitionKey,
                     const TString& explicitHash);

            virtual ~IMessage() = default;
        protected:
            TString Data;

            TPartitionStream::TPtr PartitionStream;
            TString PartitionKey;
            TString ExplicitHash;
        };

        //! Single message.
        struct TMessage : public IMessage {
            //! User data.
            //! Throws decompressor exception if decompression failed.
            const TString& GetData() const override;

            bool HasException() const;

            //! Message offset.
            ui64 GetOffset() const;

            //! Message group id.
            const TString& GetMessageGroupId() const;

            //! Sequence number.
            ui64 GetSeqNo() const;

            //! Message creation timestamp.
            TInstant GetCreateTime() const;

            //! Message write timestamp.
            TInstant GetWriteTime() const;

            //! Ip address of message source host.
            const TString& GetIp() const;

            //! Metainfo.
            const TWriteSessionMeta::TPtr& GetMeta() const;

            TMessage(const TString& data,
                     std::exception_ptr decompressionException,
                     const TMessageInformation& information,
                     TPartitionStream::TPtr partitionStream,
                     const TString& partitionKey,
                     const TString& explicitHash);

            //! Commits single message.
            void Commit() override;

            using IMessage::DebugString;
            void DebugString(TStringBuilder& ret, bool printData = false) const override;

        private:
            std::exception_ptr DecompressionException;
            TMessageInformation Information;
        };

        struct TCompressedMessage : public IMessage {
            //! Messages count in compressed data
            ui64 GetBlocksCount() const;

            //! Message codec
            ECodec GetCodec() const;

            //! Message offset.
            ui64 GetOffset(ui64 index) const;

            //! Message group id.
            const TString& GetMessageGroupId(ui64 index) const;

            //! Sequence number.
            ui64 GetSeqNo(ui64 index) const;

            //! Message creation timestamp.
            TInstant GetCreateTime(ui64 index) const;

            //! Message write timestamp.
            TInstant GetWriteTime(ui64 index) const;

            //! Ip address of message source host.
            const TString& GetIp(ui64 index) const;

            //! Metainfo.
            const TWriteSessionMeta::TPtr& GetMeta(ui64 index) const;

            //! Uncompressed block size.
            ui64 GetUncompressedSize(ui64 index) const;

            virtual ~TCompressedMessage() {}
            TCompressedMessage(ECodec codec,
                               const TString& data,
                               const TVector<TMessageInformation>& information,
                               TPartitionStream::TPtr partitionStream,
                               const TString& partitionKey,
                               const TString& explicitHash);

            //! Commits all offsets in compressed message.
            void Commit() override;

            using IMessage::DebugString;
            void DebugString(TStringBuilder& ret, bool printData = false) const override;

        private:
            ECodec Codec;
            TVector<TMessageInformation> Information;
        };

        //! Partition stream.
        const TPartitionStream::TPtr& GetPartitionStream() const {
            return PartitionStream;
        }

        bool IsCompressedMessages() const {
            return !CompressedMessages.empty();
        }

        size_t GetMessagesCount() const {
            return Messages.size() + CompressedMessages.size();
        }

        //! Get messages.
        TVector<TMessage>& GetMessages() {
            CheckMessagesFilled(false);
            return Messages;
        }

        const TVector<TMessage>& GetMessages() const {
            CheckMessagesFilled(false);
            return Messages;
        }

        //! Get compressed messages.
        TVector<TCompressedMessage>& GetCompressedMessages() {
            CheckMessagesFilled(true);
            return CompressedMessages;
        }

        const TVector<TCompressedMessage>& GetCompressedMessages() const {
            CheckMessagesFilled(true);
            return CompressedMessages;
        }

        //! Commits all messages in batch.
        void Commit();

        TString DebugString(bool printData = false) const;

        TDataReceivedEvent(TVector<TMessage> messages,
                           TVector<TCompressedMessage> compressedMessages,
                           TPartitionStream::TPtr partitionStream);

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
        TVector<TMessage> Messages;
        TVector<TCompressedMessage> CompressedMessages;
        TPartitionStream::TPtr PartitionStream;
        std::vector<std::pair<ui64, ui64>> OffsetRanges;
    };

    //! Acknowledgement for commit request.
    struct TCommitAcknowledgementEvent {
        //! Partition stream.
        const TPartitionStream::TPtr& GetPartitionStream() const {
            return PartitionStream;
        }

        //! Committed offset.
        //! This means that from now the first available
        //! message offset in current partition
        //! for current consumer is this offset.
        //! All messages before are committed and futher never be available.
        ui64 GetCommittedOffset() const {
            return CommittedOffset;
        }

        TString DebugString() const;

        TCommitAcknowledgementEvent(TPartitionStream::TPtr partitionStream, ui64 committedOffset);

    private:
        TPartitionStream::TPtr PartitionStream;
        ui64 CommittedOffset;
    };

    //! Server request for creating partition stream.
    struct TCreatePartitionStreamEvent {
        TCreatePartitionStreamEvent(TPartitionStream::TPtr, ui64 committedOffset, ui64 endOffset);

        const TPartitionStream::TPtr& GetPartitionStream() const {
            return PartitionStream;
        }

        //! Current committed offset in partition stream.
        ui64 GetCommittedOffset() const {
            return CommittedOffset;
        }

        //! Offset of first not existing message in partition stream.
        ui64 GetEndOffset() const {
            return EndOffset;
        }

        //! Confirm partition stream creation.
        //! This signals that user is ready to receive data from this partition stream.
        //! If maybe is empty then no rewinding
        void Confirm(TMaybe<ui64> readOffset = Nothing(), TMaybe<ui64> commitOffset = Nothing());

        TString DebugString() const;

    private:
        TPartitionStream::TPtr PartitionStream;
        ui64 CommittedOffset;
        ui64 EndOffset;
    };

    //! Server request for destroying partition stream.
    //! Server can destroy partition stream gracefully
    //! for rebalancing among all topic clients.
    struct TDestroyPartitionStreamEvent {
        const TPartitionStream::TPtr& GetPartitionStream() const {
            return PartitionStream;
        }

        //! Last offset of the partition stream that was committed.
        ui64 GetCommittedOffset() const {
            return CommittedOffset;
        }

        //! Confirm partition stream destruction.
        //! Confirm has no effect if TPartitionStreamClosedEvent for same partition stream with is received.
        void Confirm();

        TString DebugString() const;

        TDestroyPartitionStreamEvent(TPartitionStream::TPtr partitionStream, ui64 committedOffset);

    private:
        TPartitionStream::TPtr PartitionStream;
        ui64 CommittedOffset;
    };

    //! Status for partition stream requested via TPartitionStream::RequestStatus()
    struct TPartitionStreamStatusEvent {
        const TPartitionStream::TPtr& GetPartitionStream() const {
            return PartitionStream;
        }

        //! Committed offset.
        ui64 GetCommittedOffset() const {
            return CommittedOffset;
        }

        //! Offset of next message (that is not yet read by session).
        ui64 GetReadOffset() const {
            return ReadOffset;
        }

        //! Offset of first not existing message in partition.
        ui64 GetEndOffset() const {
            return EndOffset;
        }

        //! Write watermark.
        //! The last written timestamp of message in this partition stream.
        TInstant GetWriteWatermark() const {
            return WriteWatermark;
        }

        TString DebugString() const;

        TPartitionStreamStatusEvent(TPartitionStream::TPtr partitionStream, ui64 committedOffset, ui64 readOffset, ui64 endOffset, TInstant writeWatermark);

    private:
        TPartitionStream::TPtr PartitionStream;
        ui64 CommittedOffset = 0;
        ui64 ReadOffset = 0;
        ui64 EndOffset = 0;
        TInstant WriteWatermark;
    };

    //! Event that signals user about
    //! partition stream death.
    //! This could be after graceful destruction of
    //! partition stream or when connection with partition was lost.
    struct TPartitionStreamClosedEvent {
        enum class EReason {
            DestroyConfirmedByUser,
            Lost,
            ConnectionLost,
        };

        const TPartitionStream::TPtr& GetPartitionStream() const {
            return PartitionStream;
        }

        EReason GetReason() const {
            return Reason;
        }

        TString DebugString() const;

        TPartitionStreamClosedEvent(TPartitionStream::TPtr partitionStream, EReason reason);

    private:
        TPartitionStream::TPtr PartitionStream;
        EReason Reason;
    };

    using TEvent = std::variant<TDataReceivedEvent,
                                TCommitAcknowledgementEvent,
                                TCreatePartitionStreamEvent,
                                TDestroyPartitionStreamEvent,
                                TPartitionStreamStatusEvent,
                                TPartitionStreamClosedEvent,
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
    void Add(const TPartitionStream::TPtr& partitionStream, ui64 startOffset, ui64 endOffset);

    //! Add offset to set.
    void Add(const TPartitionStream::TPtr& partitionStream, ui64 offset);

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
    THolder<TImpl> Impl;
};

//! Event debug string.
TString DebugString(const TReadSessionEvent::TEvent& event);

}  // namespace NYdb::NPersQueue
