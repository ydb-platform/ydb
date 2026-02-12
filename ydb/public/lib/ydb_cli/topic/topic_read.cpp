#include "topic_metadata_fields.h"
#include "topic_read.h"
#include "topic_write.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_common.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <util/generic/set.h>

namespace NYdb::NConsoleClient {
    namespace {
        constexpr i64 MessagesLimitUnlimited = -1;
        constexpr i64 MessagesLimitNonStreamingFormatDefault = 10;

        bool IsStreamingFormat(EMessagingFormat format) {
            return format == EMessagingFormat::NewlineDelimited ||
                   format == EMessagingFormat::Concatenated ||
                   format == EMessagingFormat::JsonStreamConcat ||
                   format == EMessagingFormat::Csv ||
                   format == EMessagingFormat::Tsv;
        }
    }

    TTopicReaderSettings::TTopicReaderSettings() {
    }

    TTopicReaderSettings::TTopicReaderSettings(
        TMaybe<i64> limit,
        bool commit,
        bool wait,
        TPartitionReadOffsetMap partitionReadOffset,
        EMessagingFormat format,
        TVector<ETopicMetadataField> metadataFields,
        ETransformBody transform,
        TDuration idleTimeout)
        : MetadataFields_(metadataFields)
        , IdleTimeout_(idleTimeout)
        , MessagingFormat_(format)
        , Transform_(transform)
        , Limit_(limit)
        , PartitionReadOffset_(std::move(partitionReadOffset))
        , Commit_(commit)
        , Wait_(wait) {
    }

    TTopicReader::TTopicReader(
        std::shared_ptr<NTopic::IReadSession> readSession,
        TTopicReaderSettings params)
        : ReadSession_(readSession)
        , ReaderParams_(params)
        , PartitionReadOffset_(ReaderParams_.PartitionReadOffset()) {
    }

    void TTopicReader::Init() {
        TVector<TString> tableFields;
        for (const auto& f : ReaderParams_.MetadataFields()) {
            tableFields.emplace_back(TStringBuilder() << f);
        }
        TPrettyTable table(tableFields);
        OutputTable_ = std::make_unique<TPrettyTable>(table);

        if (!ReaderParams_.Limit().Defined()) {
            if (IsStreamingFormat(ReaderParams_.MessagingFormat())) {
                MessagesLeft_ = MessagesLimitUnlimited;
            } else {
                MessagesLeft_ = MessagesLimitNonStreamingFormatDefault;
            }
            return;
        }

        i64 limit = *(ReaderParams_.Limit());
        if (IsStreamingFormat(ReaderParams_.MessagingFormat()) && limit == 0) {
            limit = -1;
        }
        MessagesLeft_ = limit;
    }

    namespace {
        const std::string FormatBody(const std::string& body, ETransformBody transform) {
            if (transform == ETransformBody::Base64) {
                return Base64Encode(body);
            }

            return body;
        }

        using TReceivedMessage = NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage;

        void AddMetadataFieldToRow(TPrettyTable::TRow& row, const TReceivedMessage& message, ETransformBody transform, ETopicMetadataField f, size_t idx) {
            switch (f) {
                case ETopicMetadataField::Body:
                    row.Column(idx, FormatBody(message.GetData(), transform));
                    break;
                case ETopicMetadataField::CreateTime:
                    row.Column(idx, message.GetCreateTime());
                    break;
                case ETopicMetadataField::ProducerID:
                    row.Column(idx, message.GetProducerId());
                    break;
                case ETopicMetadataField::Offset:
                    row.Column(idx, message.GetOffset());
                    break;
                case ETopicMetadataField::WriteTime:
                    row.Column(idx, message.GetWriteTime()); // improve for pretty
                    break;
                case ETopicMetadataField::SeqNo:
                    row.Column(idx, message.GetSeqNo());
                    break;
                case ETopicMetadataField::MessageMeta:
                    {
                        NJson::TJsonValue json;
                        for (auto const& [k, v] : message.GetMessageMeta()->Fields) {
                            json[k] = v;
                        }
                        row.Column(idx, json);
                    }
                    break;
                case ETopicMetadataField::SessionMeta:
                    {
                        NJson::TJsonValue json;
                        for (auto const& [k, v] : message.GetMeta()->Fields) {
                            json[k] = v;
                        }
                        row.Column(idx, json);
                    }
                    break;
                case ETopicMetadataField::PartitionID:
                    row.Column(idx, message.GetPartitionSession()->GetPartitionId());
                    break;
                default:
                    break;
            }
        }
    } // namespace

    void TTopicReader::PrintMessagesInPrettyFormat(IOutputStream& output) const {
        for (const auto& message : ReceivedMessages_) {
            TPrettyTable::TRow& row = OutputTable_->AddRow();
            for (size_t i = 0; i < ReaderParams_.MetadataFields().size(); ++i) {
                ETopicMetadataField f = ReaderParams_.MetadataFields()[i];
                AddMetadataFieldToRow(row, message, ReaderParams_.Transform(), f, i);
            }
        }

        OutputTable_->Print(output);
    }

    void TTopicReader::PrintMessageAsJson(const TReceivedMessage& message, IOutputStream& output) const {
        NJson::TJsonWriter writer(&output, false);
        writer.OpenMap();

        for (const auto& f : ReaderParams_.MetadataFields()) {
            switch (f) {
                case ETopicMetadataField::Body:
                    writer.Write("body", TString(FormatBody(message.GetData(), ReaderParams_.Transform())));
                    break;
                case ETopicMetadataField::CreateTime:
                    writer.Write("create_time", message.GetCreateTime().ToString());
                    break;
                case ETopicMetadataField::ProducerID:
                    writer.Write("producer_id", TString(message.GetProducerId()));
                    break;
                case ETopicMetadataField::Offset:
                    writer.Write("offset", message.GetOffset());
                    break;
                case ETopicMetadataField::WriteTime:
                    writer.Write("write_time", message.GetWriteTime().ToString());
                    break;
                case ETopicMetadataField::SeqNo:
                    writer.Write("seq_no", message.GetSeqNo());
                    break;
                case ETopicMetadataField::MessageMeta:
                    {
                        writer.WriteKey("message_meta");
                        writer.OpenMap();
                        for (const auto& [k, v] : message.GetMessageMeta()->Fields) {
                            writer.Write(k, v);
                        }
                        writer.CloseMap();
                    }
                    break;
                case ETopicMetadataField::SessionMeta:
                    {
                        writer.WriteKey("session_meta");
                        writer.OpenMap();
                        for (const auto& [k, v] : message.GetMeta()->Fields) {
                            writer.Write(k, v);
                        }
                        writer.CloseMap();
                    }
                    break;
                case ETopicMetadataField::PartitionID:
                    writer.Write("partition_id", message.GetPartitionSession()->GetPartitionId());
                    break;
                default:
                    break;
            }
        }

        writer.CloseMap();
    }

    void TTopicReader::PrintMessagesInJsonArrayFormat(IOutputStream& output) const {
        output << "[";
        bool first = true;
        for (const auto& message : ReceivedMessages_) {
            if (!first) {
                output << ",";
            }
            first = false;
            PrintMessageAsJson(message, output);
        }
        output << "]" << Endl;
    }

    TString TTopicReader::GetFieldWithEscaping(const TString& body, char delimiter) const {
        if (body.Contains(delimiter) || body.Contains('"') || body.Contains('\n')) {
            TString escaped;
            escaped.reserve(body.size() + 2);
            escaped += '"';
            for (char c : body) {
                if (c == '"') {
                    escaped += "\"\"";
                } else {
                    escaped += c;
                }
            }
            escaped += '"';
            return escaped;
        } else {
            return body;
        }
    }

    void TTopicReader::PrintCsvFieldValue(const ETopicMetadataField& f, TReceivedMessage const& message, IOutputStream& output, char delimiter) const {
        switch (f) {
            case ETopicMetadataField::Body:
                {
                    TString body(FormatBody(message.GetData(), ReaderParams_.Transform()));
                    output << GetFieldWithEscaping(body, delimiter);
                }
                break;
            case ETopicMetadataField::CreateTime:
                output << message.GetCreateTime();
                break;
            case ETopicMetadataField::ProducerID:
                output << GetFieldWithEscaping(TString(message.GetProducerId()), delimiter);
                break;
            case ETopicMetadataField::Offset:
                output << message.GetOffset();
                break;
            case ETopicMetadataField::WriteTime:
                output << message.GetWriteTime();
                break;
            case ETopicMetadataField::SeqNo:
                output << message.GetSeqNo();
                break;
            case ETopicMetadataField::MessageMeta:
                {
                    NJson::TJsonValue json;
                    for (const auto& [k, v] : message.GetMessageMeta()->Fields) {
                        json[k] = v;
                    }
                    output << GetFieldWithEscaping(json.GetStringRobust(), delimiter);
                }
                break;
            case ETopicMetadataField::SessionMeta:
                {
                    NJson::TJsonValue json;
                    for (const auto& [k, v] : message.GetMeta()->Fields) {
                        json[k] = v;
                    }
                    output << GetFieldWithEscaping(json.GetStringRobust(), delimiter);
                }
                break;
            case ETopicMetadataField::PartitionID:
                output << message.GetPartitionSession()->GetPartitionId();
                break;
            default:
                break;
        }
    }

    void TTopicReader::PrintMessagesInCsvFormat(IOutputStream& output, char delimiter) const {
        // Print header
        bool firstCol = true;
        for (const auto& f : ReaderParams_.MetadataFields()) {
            if (!firstCol) {
                output << delimiter;
            }
            firstCol = false;
            output << f;
        }
        output << "\n";

        // Print rows
        for (const auto& message : ReceivedMessages_) {
            firstCol = true;
            for (const auto& f : ReaderParams_.MetadataFields()) {
                if (!firstCol) {
                    output << delimiter;
                }
                firstCol = false;
                PrintCsvFieldValue(f, message, output, delimiter);
            }
            output << "\n";
        }
    }

    void TTopicReader::PrintCsvHeader(IOutputStream& output, char delimiter) {
        bool firstCol = true;
        for (const auto& f : ReaderParams_.MetadataFields()) {
            if (!firstCol) {
                output << delimiter;
            }
            firstCol = false;
            output << f;
        }
        output << "\n";
        CsvHeaderPrinted_ = true;
    }

    void TTopicReader::PrintMessageAsCsvRow(const TReceivedMessage& message, IOutputStream& output, char delimiter) const {
        bool firstCol = true;
        for (const auto& f : ReaderParams_.MetadataFields()) {
            if (!firstCol) {
                output << delimiter;
            }
            firstCol = false;

            PrintCsvFieldValue(f, message, output, delimiter);
        }
        output << "\n";
    }

    void TTopicReader::Close(IOutputStream& output, TDuration closeTimeout) {
        EMessagingFormat format = ReaderParams_.MessagingFormat();
        if (format == EMessagingFormat::Pretty) {
            PrintMessagesInPrettyFormat(output);
        } else if (format == EMessagingFormat::JsonArray) {
            PrintMessagesInJsonArrayFormat(output);
        }
        // CSV and TSV are streaming formats - rows already output in HandleReceivedMessage
        output.Flush();
        bool success = ReadSession_->Close(closeTimeout);
        if (!success) {
            throw yexception() << "Failed to close read session\n";
        }
    }

    void TTopicReader::HandleReceivedMessage(const TReceivedMessage& message, IOutputStream& output) {
        EMessagingFormat format = ReaderParams_.MessagingFormat();
        if (format == EMessagingFormat::SingleMessage || format == EMessagingFormat::Concatenated) {
            output << FormatBody(message.GetData(), ReaderParams_.Transform());
            output.Flush();
            return;
        }
        if (format == EMessagingFormat::NewlineDelimited) {
            output << FormatBody(message.GetData(), ReaderParams_.Transform());
            output << "\n";
            output.Flush();
            return;
        }
        if (format == EMessagingFormat::JsonStreamConcat) {
            PrintMessageAsJson(message, output);
            output << "\n";
            output.Flush();
            return;
        }
        if (format == EMessagingFormat::Csv) {
            if (!CsvHeaderPrinted_) {
                PrintCsvHeader(output, ',');
            }
            PrintMessageAsCsvRow(message, output, ',');
            output.Flush();
            return;
        }
        if (format == EMessagingFormat::Tsv) {
            if (!CsvHeaderPrinted_) {
                PrintCsvHeader(output, '\t');
            }
            PrintMessageAsCsvRow(message, output, '\t');
            output.Flush();
            return;
        }
        // For batch formats (Pretty, JsonArray), accumulate messages
        ReceivedMessages_.push_back(message);
    }

    int TTopicReader::HandleDataReceivedEvent(NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent* event, IOutputStream& output) {
        ui64 sessionId = event->GetPartitionSession()->GetPartitionSessionId();
        if (!HasSession(sessionId)) {
            return EXIT_SUCCESS;
        }

        if (ActivePartitionSessions_[sessionId].ReadingStatus == EReadingStatus::PartitionWithoutData) {
            ActivePartitionSessions_[sessionId].ReadingStatus = EReadingStatus::PartitionWithData;
            ++PartitionsBeingRead_;
        }

        HasFirstMessage_ = true;

        NTopic::TDeferredCommit defCommit;
        for (const auto& message : event->GetMessages()) {
            HandleReceivedMessage(message, output);
            if (ReaderParams_.Commit()) {
                defCommit.Add(message);
            }

            if (!PartitionReadOffset_.empty()) {
                if (ui64* nextOffset = MapFindPtr(PartitionReadOffset_, event->GetPartitionSession()->GetPartitionId())) {
                    *nextOffset = message.GetOffset() + 1; // memorize next offset for the case of session soft restart
                }
            }

            ActivePartitionSessions_[sessionId].LastReadOffset = message.GetOffset();

            if (MessagesLeft_ == MessagesLimitUnlimited) {
                continue;
            }

            --MessagesLeft_;
            if (MessagesLeft_ == 0) {
                break;
            }
        }

        if (ReaderParams_.Commit()) {
            defCommit.Commit();
        }
        LastMessageReceivedTs_ = Now();

        event->GetPartitionSession()->RequestStatus();
        return EXIT_SUCCESS;
    }

    bool TTopicReader::HasSession(ui64 sessionId) const {
        auto f = ActivePartitionSessions_.find(sessionId);
        return !(f == ActivePartitionSessions_.end());
    }

    int TTopicReader::HandleStartPartitionSessionEvent(NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent* event) {
        const std::optional<uint64_t> readOffset = GetNextReadOffset(event->GetPartitionSession()->GetPartitionId());
        event->Confirm(readOffset);
        FirstPartitionSessionCreated = true;

        EReadingStatus readingStatus = EReadingStatus::PartitionWithData;
        if (event->GetCommittedOffset() == event->GetEndOffset() || (readOffset.has_value() && readOffset.value() >= event->GetEndOffset())) {
            readingStatus = EReadingStatus::PartitionWithoutData;
            if (PartitionsBeingRead_ == 0) {
                AllPartitionsAreFullyReadTime = TInstant::Now();
            }
        } else {
            ++PartitionsBeingRead_;
        }

        ActivePartitionSessions_.insert({event->GetPartitionSession()->GetPartitionSessionId(), {event->GetPartitionSession(), readingStatus, event->GetEndOffset()}});
        return EXIT_SUCCESS;
    }

    int TTopicReader::HandleCommitOffsetAcknowledgementEvent(NTopic::TReadSessionEvent::TCommitOffsetAcknowledgementEvent* event) {
        Y_UNUSED(event);

        return EXIT_SUCCESS;
    }

    std::optional<uint64_t> TTopicReader::GetNextReadOffset(ui64 partitionId) const {
        if (!PartitionReadOffset_.empty()) {
            if (const ui64* offset = MapFindPtr(PartitionReadOffset_, partitionId)) {
                return *offset;
            }
        }
        return std::nullopt;
    }

    int TTopicReader::HandlePartitionSessionStatusEvent(NTopic::TReadSessionEvent::TPartitionSessionStatusEvent* event) {
        ui64 sessionId = event->GetPartitionSession()->GetPartitionSessionId();
        if (!HasSession(sessionId)) {
            return EXIT_SUCCESS;
        }

        auto status = ActivePartitionSessions_.find(sessionId);
        EReadingStatus currentPartitionStatus = status->second.ReadingStatus;
        const std::optional<uint64_t> readOffset = GetNextReadOffset(event->GetPartitionSession()->GetPartitionId());
        if (event->GetEndOffset() == event->GetCommittedOffset() ||
           (event->GetEndOffset() == *ActivePartitionSessions_[sessionId].LastReadOffset + 1) ||
           (readOffset.has_value() && readOffset.value() >= event->GetEndOffset())) {
            if (currentPartitionStatus == EReadingStatus::PartitionWithData) {
                --PartitionsBeingRead_;
            }
            ActivePartitionSessions_[sessionId].ReadingStatus = EReadingStatus::PartitionWithoutData;
        } else {
            if (currentPartitionStatus == EReadingStatus::PartitionWithoutData || currentPartitionStatus == EReadingStatus::NoPartitionTaken) {
                ++PartitionsBeingRead_;
            }
            ActivePartitionSessions_[sessionId].ReadingStatus = EReadingStatus::PartitionWithData;
        }
        if (PartitionsBeingRead_ == 0) {
            AllPartitionsAreFullyReadTime = TInstant::Now();
        }

        return EXIT_SUCCESS;
    }

    int TTopicReader::HandleStopPartitionSessionEvent(NTopic::TReadSessionEvent::TStopPartitionSessionEvent* event) {
        if (!HasSession(event->GetPartitionSession()->GetPartitionSessionId())) {
            return EXIT_SUCCESS;
        }

        event->Confirm();

        auto f = ActivePartitionSessions_.find(event->GetPartitionSession()->GetPartitionSessionId());
        if (f->second.ReadingStatus == EReadingStatus::PartitionWithData) {
            --PartitionsBeingRead_;
        }
        ActivePartitionSessions_.erase(event->GetPartitionSession()->GetPartitionSessionId());
        if (PartitionsBeingRead_ == 0) {
            AllPartitionsAreFullyReadTime = TInstant::Now();
        }
        return EXIT_SUCCESS;
    }

    int TTopicReader::HandlePartitionSessionClosedEvent(NTopic::TReadSessionEvent::TPartitionSessionClosedEvent *event) {
        if (!HasSession(event->GetPartitionSession()->GetPartitionSessionId())) {
            return EXIT_SUCCESS;
        }

        if (ActivePartitionSessions_[event->GetPartitionSession()->GetPartitionSessionId()].ReadingStatus == EReadingStatus::PartitionWithData) {
            --PartitionsBeingRead_;
        }
        ActivePartitionSessions_.erase(event->GetPartitionSession()->GetPartitionSessionId());
        if (PartitionsBeingRead_ == 0) {
            AllPartitionsAreFullyReadTime = TInstant::Now();
        }
        return EXIT_SUCCESS;
    }

    int TTopicReader::HandleEndPartitionSessionEvent(NTopic::TReadSessionEvent::TEndPartitionSessionEvent *event) {
        if (!HasSession(event->GetPartitionSession()->GetPartitionSessionId())) {
            return EXIT_SUCCESS;
        }

        event->Confirm();

        return EXIT_SUCCESS;
    }

    int TTopicReader::HandleEvent(NYdb::NTopic::TReadSessionEvent::TEvent& event, IOutputStream& output) {
        if (auto* dataEvent = std::get_if<NTopic::TReadSessionEvent::TDataReceivedEvent>(&event)) {
            return HandleDataReceivedEvent(dataEvent, output);
        } else if (auto* createPartitionStreamEvent = std::get_if<NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(&event)) {
            return HandleStartPartitionSessionEvent(createPartitionStreamEvent);
        } else if (auto* commitEvent = std::get_if<NTopic::TReadSessionEvent::TCommitOffsetAcknowledgementEvent>(&event)) {
            return HandleCommitOffsetAcknowledgementEvent(commitEvent);
        } else if (auto* partitionStatusEvent = std::get_if<NTopic::TReadSessionEvent::TPartitionSessionStatusEvent>(&event)) {
            return HandlePartitionSessionStatusEvent(partitionStatusEvent);
        } else if (auto* stopPartitionSessionEvent = std::get_if<NTopic::TReadSessionEvent::TStopPartitionSessionEvent>(&event)) {
            return HandleStopPartitionSessionEvent(stopPartitionSessionEvent);
        } else if (auto* partitionSessionClosedEvent = std::get_if<NTopic::TReadSessionEvent::TPartitionSessionClosedEvent>(&event)) {
            return HandlePartitionSessionClosedEvent(partitionSessionClosedEvent);
        } else if (auto* endPartitionSessionEvent = std::get_if<NTopic::TReadSessionEvent::TEndPartitionSessionEvent>(&event)) {
            return HandleEndPartitionSessionEvent(endPartitionSessionEvent);
        } else if (auto* sessionClosedEvent = std::get_if<NTopic::TSessionClosedEvent>(&event)) {
            NStatusHelpers::ThrowOnErrorOrPrintIssues(*sessionClosedEvent);
            return 1;
        }
        return 0;
    }

    int TTopicReader::Run(IOutputStream& output) {
        LastMessageReceivedTs_ = TInstant::Now();

        bool waitForever = (ReaderParams_.Wait()) && IsStreamingFormat(ReaderParams_.MessagingFormat());
        if (ReaderParams_.Wait() && !IsStreamingFormat(ReaderParams_.MessagingFormat())) {
            Cerr << "Option --wait is ignored because messaging format is not streaming." << Endl;
        }

        TInstant runStartTime = TInstant::Now();
        TDuration waitForPartitionSessionStart = Max(TDuration::Seconds(30), ReaderParams_.IdleTimeout());
        while ((MessagesLeft_ > 0 || MessagesLeft_ == -1) && !IsInterrupted()) {
            TInstant messageReceiveDeadline = LastMessageReceivedTs_ + TDuration::MilliSeconds(100);
            NThreading::TFuture<void> future = ReadSession_->WaitEvent();
            future.Wait(messageReceiveDeadline);

            if (!FirstPartitionSessionCreated && (TInstant::Now() - runStartTime > waitForPartitionSessionStart)) {
                Cerr << "There isn't any successfully initialized partition session after 30s.";
                return EXIT_FAILURE;
            }
            if (!PartitionsBeingRead_ && AllPartitionsAreFullyReadTime.has_value() && (TInstant::Now() - *AllPartitionsAreFullyReadTime > ReaderParams_.IdleTimeout())) {
                return EXIT_SUCCESS;
            }

            if (future.HasValue()) {
                // TODO(shmel1k@): throttling?
                // TODO(shmel1k@): think about limiting size of events
                std::vector<NTopic::TReadSessionEvent::TEvent> events = ReadSession_->GetEvents(true);
                for (auto& event : events) {
                    if (int status = HandleEvent(event, output); status) {
                        return status;
                    }
                }
                continue;
            }

            if (waitForever) {
                LastMessageReceivedTs_ = TInstant::Now();
                continue;
            }
        }
        return EXIT_SUCCESS;
    }
} // namespace NYdb::NConsoleClient
