#pragma once

#include "topic_metadata_fields.h"
#include "ydb/public/lib/ydb_cli/commands/ydb_command.h"
#include <util/stream/null.h>
#include <ydb/public/lib/ydb_cli/common/format.h>
#include <ydb/public/lib/ydb_cli/common/interruptable.h>
#include <ydb/public/lib/ydb_cli/common/pretty_table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

namespace NYdb::NConsoleClient {
#define GETTER(TYPE, NAME) \
    TYPE NAME() const {    \
        return NAME##_;    \
    }

    class TTopicReaderSettings {
    public:
        using TPartitionReadOffsetMap = std::unordered_map<ui64, ui64>;

        TTopicReaderSettings(
            TMaybe<i64> limit,
            bool commit,
            bool wait,
            TPartitionReadOffsetMap partitionReadOffset,
            EMessagingFormat format,
            TVector<ETopicMetadataField> metadataFields,
            ETransformBody transform,
            TDuration idleTimeout);

        TTopicReaderSettings();
        TTopicReaderSettings(const TTopicReaderSettings&) = default;
        TTopicReaderSettings(TTopicReaderSettings&&) = default;

        GETTER(TVector<ETopicMetadataField>, MetadataFields);
        GETTER(bool, Commit);
        GETTER(TMaybe<i64>, Limit);
        GETTER(bool, Wait);
        GETTER(TPartitionReadOffsetMap, PartitionReadOffset);
        GETTER(EMessagingFormat, MessagingFormat);
        GETTER(ETransformBody, Transform);
        GETTER(TDuration, IdleTimeout);
        // TODO(shmel1k@): add batching settings.

    private:
        TVector<ETopicMetadataField> MetadataFields_;
        TMaybe<TDuration> FlushDuration_;
        TMaybe<int> FlushSize_;
        TMaybe<int> FlushMessagesCount_;
        TDuration IdleTimeout_;

        EMessagingFormat MessagingFormat_ = EMessagingFormat::SingleMessage;
        ETransformBody Transform_ = ETransformBody::None;
        TMaybe<i64> Limit_ = Nothing();
        TPartitionReadOffsetMap PartitionReadOffset_;
        bool Commit_ = false;
        bool Wait_ = false;
    };

    class TTopicReaderTests;

    // TODO(shmel1k@): think about interruption here.
    class TTopicReader: public TInterruptableCommand {
        using TReceivedMessage = NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage;

    public:
        TTopicReader(std::shared_ptr<NTopic::IReadSession>, TTopicReaderSettings);

        void Close(IOutputStream& output, TDuration closeTimeout = TDuration::Max());
        void Init();
        int Run(IOutputStream&);

        void HandleReceivedMessage(const TReceivedMessage& message, IOutputStream& output);

        int HandleStartPartitionSessionEvent(NTopic::TReadSessionEvent::TStartPartitionSessionEvent*);
        int HandlePartitionSessionStatusEvent(NTopic::TReadSessionEvent::TPartitionSessionStatusEvent*);
        int HandleStopPartitionSessionEvent(NTopic::TReadSessionEvent::TStopPartitionSessionEvent*);
        int HandlePartitionSessionClosedEvent(NTopic::TReadSessionEvent::TPartitionSessionClosedEvent*);
        int HandleEndPartitionSessionEvent(NTopic::TReadSessionEvent::TEndPartitionSessionEvent*);
        int HandleDataReceivedEvent(NTopic::TReadSessionEvent::TDataReceivedEvent*, IOutputStream&);
        int HandleCommitOffsetAcknowledgementEvent(NTopic::TReadSessionEvent::TCommitOffsetAcknowledgementEvent*);
        int HandleEvent(NTopic::TReadSessionEvent::TEvent&, IOutputStream&);

    private:
        void PrintMessagesInPrettyFormat(IOutputStream& output) const;
        void PrintMessagesInJsonArrayFormat(IOutputStream& output) const;
        void PrintMessagesInCsvFormat(IOutputStream& output, char delimiter) const;
        void PrintMessageAsJson(const TReceivedMessage& message, IOutputStream& output) const;
        void PrintCsvHeader(IOutputStream& output, char delimiter);
        void PrintMessageAsCsvRow(const TReceivedMessage& message, IOutputStream& output, char delimiter) const;
        void PrintCsvFieldValue(const ETopicMetadataField& f, TReceivedMessage const& message, IOutputStream& output, char delimiter) const;
        TString GetFieldWithEscaping(const TString& body, char delimiter) const;

        enum EReadingStatus {
            NoPartitionTaken = 0,
            PartitionWithoutData = 1,
            PartitionWithData = 2,
        };

        struct TPartitionSessionInfo {
            NTopic::TPartitionSession::TPtr PartitionSession;
            EReadingStatus ReadingStatus;
            std::optional<ui64> LastReadOffset;
        };

        bool HasSession(ui64 sessionId) const;
        std::optional<uint64_t> GetNextReadOffset(ui64 partitionId) const;

    private:
        std::shared_ptr<NTopic::IReadSession> ReadSession_;
        const TTopicReaderSettings ReaderParams_;

        i64 MessagesLeft_ = 1; // Messages left to read. -1 means 'unlimited'
        bool HasFirstMessage_ = false;
        TInstant LastMessageReceivedTs_;

        std::unique_ptr<TPrettyTable> OutputTable_;
        TVector<TReceivedMessage> ReceivedMessages_;

        ui32 PartitionsBeingRead_ = 0;
        bool CsvHeaderPrinted_ = false;
        bool FirstPartitionSessionCreated = false;
        std::optional<TInstant> AllPartitionsAreFullyReadTime;

        friend class TTopicReaderTests;

        THashMap<ui64, TPartitionSessionInfo> ActivePartitionSessions_;
        TTopicReaderSettings::TPartitionReadOffsetMap PartitionReadOffset_;
    };
} // namespace NYdb::NConsoleClient
