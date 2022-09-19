#include "topic_write.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_common.h>

#include <library/cpp/string_utils/base64/base64.h>
#include <openssl/sha.h>
#include <util/generic/overloaded.h>
#include <util/stream/tokenizer.h>
#include <util/string/hex.h>

#include <signal.h>

namespace NYdb::NConsoleClient {
    namespace {
        constexpr TDuration DefaultMessagesWaitTimeout = TDuration::Seconds(1);
    }

    TTopicWriterParams::TTopicWriterParams() {
    }

    TTopicWriterParams::TTopicWriterParams(EMessagingFormat inputFormat, TMaybe<TString> delimiter, ui64 messageSizeLimit,
                                           TMaybe<TDuration> batchDuration, TMaybe<ui64> batchSize, TMaybe<ui64> batchMessagesCount,
                                           ETransformBody transform)
        : MessagingFormat_(inputFormat)
        , BatchDuration_(batchDuration)
        , BatchSize_(batchSize)
        , BatchMessagesCount_(batchMessagesCount)
        , Transform_(transform)
        , MessageSizeLimit_(messageSizeLimit) {
        if (inputFormat == EMessagingFormat::NewlineDelimited || inputFormat == EMessagingFormat::Concatenated) {
            Delimiter_ = TMaybe<char>('\n');
        }
        if (delimiter.Defined()) {
            // TODO(shmel1k@): remove when string delimiter is supported.
            // TODO(shmel1k@): think about better way.
            if (delimiter == "\\n") {
                Delimiter_ = TMaybe<char>('\n');
                return;
            }
            if (delimiter == "\\t") {
                Delimiter_ = TMaybe<char>('\t');
                return;
            }
            if (delimiter == "\r") {
                Delimiter_ = TMaybe<char>('\r');
                return;
            }
            if (delimiter == "\0") {
                Delimiter_ = TMaybe<char>('\0');
                return;
            }
            if (delimiter == "") {
                Delimiter_ = Nothing();
                return;
            }

            Y_ENSURE(delimiter->Size() == 1, "Invalid delimiter size, should be <= 1");
            Delimiter_ = TMaybe<char>(delimiter->at(0));
        }
    }

    TTopicWriter::TTopicWriter() {
    }

    TTopicWriter::TTopicWriter(std::shared_ptr<NYdb::NTopic::IWriteSession> writeSession,
                               TTopicWriterParams params)
        : WriteSession_(writeSession)
        , WriterParams_(params) {
    }

    int TTopicWriter::Init() {
        TInstant endPreparationTime = Now() + DefaultMessagesWaitTimeout;
        NThreading::TFuture<ui64> initSeqNo = WriteSession_->GetInitSeqNo();

        while (Now() < endPreparationTime) {
            // TODO(shmel1k@): handle situation if seqNo already exists but with exception.
            if (!initSeqNo.HasValue() && !initSeqNo.Wait(TDuration::Seconds(1))) {
                // TODO(shmel1k@): change logs
                Cerr << "no init seqno yet" << Endl;
                continue;
            }
            break;
        }

        if (!initSeqNo.HasValue()) {
            // TODO(shmel1k@): logging
            if (initSeqNo.HasException()) {
                // NOTE(shmel1k@): SessionClosedEvent is stored in EventsQueue, so we can try to get it.
                auto event = WriteSession_->GetEvent(true);
                if (event.Defined()) {
                    return HandleEvent(*event);
                }
                initSeqNo.TryRethrow();
            }
            return EXIT_FAILURE;
        }

        CurrentSeqNo_ = initSeqNo.GetValue() + 1;
        return EXIT_SUCCESS;
    }

    int TTopicWriter::HandleAcksEvent(const NTopic::TWriteSessionEvent::TAcksEvent* event) {
        Y_UNUSED(event);
        return EXIT_SUCCESS;
    }

    int TTopicWriter::HandleReadyToAcceptEvent(NTopic::TWriteSessionEvent::TReadyToAcceptEvent* event) {
        ContinuationToken_ = std::move(event->ContinuationToken);
        return EXIT_SUCCESS;
    }

    int TTopicWriter::HandleSessionClosedEvent(const NTopic::TSessionClosedEvent* event) {
        ThrowOnError(*event);
        return EXIT_FAILURE;
    }

    int TTopicWriter::HandleEvent(NTopic::TWriteSessionEvent::TEvent& event) {
        if (auto* acksEvent = std::get_if<NTopic::TWriteSessionEvent::TAcksEvent>(&event)) {
            return HandleAcksEvent(acksEvent);
        } else if (auto* readyToAcceptEvent = std::get_if<NTopic::TWriteSessionEvent::TReadyToAcceptEvent>(&event)) {
            return HandleReadyToAcceptEvent(readyToAcceptEvent);
        } else if (auto* sessionClosedEvent = std::get_if<NTopic::TSessionClosedEvent>(&event)) {
            return HandleSessionClosedEvent(sessionClosedEvent);
        }

        return EXIT_SUCCESS;
    }

    namespace {
        TString TransformBody(const TString& body, ETransformBody transform) {
            if (transform == ETransformBody::None) {
                return body;
            }

            return Base64Decode(body);
        }
    }

    TTopicWriter::TSendMessageData TTopicWriter::EnterMessage(IInputStream& input) {
        // TODO(shmel1k@): add interruption here.
        // TODO(shmel1k@): add JSONStreamReader & etc interfaces.
        // TODO(shmel1k@): add stream parsing here & improve performance.
        if (!WriterParams_.Delimiter().Defined()) {
            TString body = input.ReadAll();
            return TSendMessageData{
                .Data = TransformBody(body, WriterParams_.Transform()),
                .NeedSend = true,
                .ContinueSending = false,
            };
        }

        TString buffer;
        char delimiter = *(WriterParams_.Delimiter());
        size_t read = input.ReadTo(buffer, delimiter);
        if (read == 0) {
            return TSendMessageData{
                .Data = "",
                .NeedSend = false,
                .ContinueSending = false,
            };
        }
        return TSendMessageData{
            .Data = TransformBody(buffer, WriterParams_.Transform()),
            .NeedSend = true,
            .ContinueSending = true,
        };
    }

    int TTopicWriter::Run(IInputStream& input) {
        // TODO(shmel1k@): add notificator about failures.
        SetInterruptHandlers();
        bool continueSending = true;
        while (continueSending) {
            while (!ContinuationToken_.Defined()) {
                TMaybe<NTopic::TWriteSessionEvent::TEvent> event = WriteSession_->GetEvent(true);
                if (event.Empty()) {
                    continue;
                }
                if (int status = HandleEvent(*event); status) {
                    return status;
                }
            }
            TTopicWriter::TSendMessageData message = EnterMessage(input);
            continueSending = message.ContinueSending;
            if (!message.NeedSend) {
                continue;
            }

            WriteSession_->Write(std::move(*ContinuationToken_), std::move(message.Data), CurrentSeqNo_++);
            ContinuationToken_ = Nothing();
        }

        return EXIT_SUCCESS;
    }

    bool TTopicWriter::Close(TDuration closeTimeout) {
        if (WriteSession_->Close(closeTimeout)) {
            return true;
        }
        TVector<NTopic::TWriteSessionEvent::TEvent> events = WriteSession_->GetEvents(true);
        if (events.empty()) {
            return false;
        }
        for (auto& evt : events) {
            if (HandleEvent(evt)) {
                return false;
            }
        }
        return true;
    }

    void TTopicWriter::OnTerminate(int) {
        exit(EXIT_FAILURE);
    }

    void TTopicWriter::SetInterruptHandlers() {
        signal(SIGINT, &TTopicWriter::OnTerminate);
        signal(SIGTERM, &TTopicWriter::OnTerminate);
    }
} // namespace NYdb::NConsoleClient
