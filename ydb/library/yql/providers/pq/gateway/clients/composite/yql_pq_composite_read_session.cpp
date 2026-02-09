#include "yql_pq_composite_read_session.h"

#include <ydb/library/yverify_stream/yverify_stream.h>

#include <util/generic/guid.h>

#include <queue>

namespace NYql {

namespace {

using namespace NYdb::NTopic;

class TCompositeTopicReadSession final : public IReadSession, public ICompositeTopicReadSessionControl {
    struct TTopicEventSizeVisitor {
        template <typename TEv>
        void operator()(TEv& ev) {
            Size = sizeof(ev);
        }

        void operator()(TReadSessionEvent::TDataReceivedEvent& ev) {
            Size = sizeof(ev);

            auto messagesCount = ev.GetMessagesCount();
            if (ev.HasCompressedMessages()) {
                const auto& compressedMessages = ev.GetCompressedMessages();
                messagesCount -= compressedMessages.size();

                for (const auto& compressedMessage : compressedMessages) {
                    Size += sizeof(compressedMessage);
                    Size += compressedMessage.GetProducerId().size() + compressedMessage.GetMessageGroupId().size();
                    Size += compressedMessage.GetData().size();
                }
            }

            if (messagesCount) {
                for (const auto& message : ev.GetMessages()) {
                    Size += sizeof(message);
                    Size += message.GetProducerId().size() + message.GetMessageGroupId().size();
                    if (message.HasException()) {
                        Size += message.GetBrokenData().size();
                    } else {
                        Size += message.GetData().size();
                    }
                }
            }
        }

        static ui64 GetEventSize(const TReadSessionEvent::TEvent& event) {
            TTopicEventSizeVisitor visitor;
            std::visit(visitor, event);
            return visitor.Size;
        }

    private:
        ui64 Size = 0;
    };

    class TReadSession {
    public:
        TReadSession(TCompositeTopicReadSession* self, ui64 idx, const std::shared_ptr<IReadSession>& readSession)
            : Self(self)
            , Idx(idx)
            , ReadSession(readSession)
        {}

        NThreading::TFuture<void> WaitEvent() const {
            return ReadSession->WaitEvent();
        }

        bool Close(TDuration timeout) const {
            return ReadSession->Close(timeout);
        }

        bool CheckEvent() const {
            return LastEvent && Self->CanProcessEvent(Idx, *LastEvent);
        }

        bool IsSuspended() const {
            return LastEvent && !Self->CanProcessEvent(Idx, *LastEvent);
        }

        bool ReadEvent(const TReadSessionGetEventSettings& settings) {
            if (!LastEvent) {
                LastEvent = ReadSession->GetEvent(settings);
            }
            return CheckEvent();
        }

        TReadSessionEvent::TEvent ExtractEvent() {
            Y_VALIDATE(LastEvent, "Unexpected extract event call");
            auto event = std::move(*LastEvent);
            LastEvent = std::nullopt;
            return event;
        }

    private:
        TCompositeTopicReadSession* Self;
        ui64 Idx = 0;
        std::shared_ptr<IReadSession> ReadSession;
        std::optional<TReadSessionEvent::TEvent> LastEvent;
    };

public:
    TCompositeTopicReadSession(const TCompositeTopicReadSessionSettings& settings, const std::vector<std::shared_ptr<IReadSession>>& readSessions)
        : SessionId(CreateGuidAsString())
        , MaxPartitionReadSkew(settings.MaxPartitionReadSkew)
        , UpdatePromise(NThreading::NewPromise<void>())
        , PartitionsReadTime(readSessions.size())
    {
        ReadSessions.reserve(readSessions.size());
        for (ui64 i = 0; i < readSessions.size(); ++i) {
            ReadSessions.emplace_back(this, i, readSessions[i]);
        }
    }

    // IReadSession

    NThreading::TFuture<void> WaitEvent() final {
        if (ReadyReadSessionIdx) {
            return NThreading::MakeFuture<void>();
        }

        std::vector<NThreading::TFuture<void>> futures;
        futures.reserve(ReadSessions.size());
        for (ui64 i = 0; i < ReadSessions.size(); ++i) {
            auto& readSession = ReadSessions[i];
            if (readSession.IsSuspended()) {
                continue;
            }

            futures.emplace_back(readSession.WaitEvent());
            if (futures.back().IsReady()) {
                ReadyReadSessionIdx = i;
                return NThreading::MakeFuture<void>();
            }
        }

        if (!AdvanceTimePromise) {
            AdvanceTimePromise = NThreading::NewPromise<void>();
        }

        futures.emplace_back(AdvanceTimePromise->GetFuture());
        return NThreading::WaitAny(futures);
    }

    std::vector<TReadSessionEvent::TEvent> GetEvents(bool block, std::optional<size_t> maxEventsCount, size_t maxByteSize) final {
        return GetEvents(TReadSessionGetEventSettings()
            .Block(block)
            .MaxByteSize(maxByteSize)
            .MaxEventsCount(maxEventsCount)
        );
    }

    std::vector<TReadSessionEvent::TEvent> GetEvents(const TReadSessionGetEventSettings& settings) final {
        auto getEventSettings = TReadSessionGetEventSettings(settings)
            .MaxEventsCount(1);

        ui64 usedSize = 0;
        std::vector<TReadSessionEvent::TEvent> result;
        while (auto event = GetEvent(getEventSettings)) {
            result.emplace_back(std::move(*event));
            usedSize += TTopicEventSizeVisitor::GetEventSize(result.back());

            if ((settings.MaxEventsCount_ && result.size() >= *settings.MaxEventsCount_) || usedSize >= settings.MaxByteSize_) {
                break;
            }

            getEventSettings.Block(false);
        }

        return result;
    }

    std::optional<TReadSessionEvent::TEvent> GetEvent(bool block, size_t maxByteSize) final {
        return GetEvent(TReadSessionGetEventSettings()
            .Block(block)
            .MaxByteSize(maxByteSize)
            .MaxEventsCount(1)
        );
    }

    std::optional<TReadSessionEvent::TEvent> GetEvent(const TReadSessionGetEventSettings& settings) final {
        auto readSettings = settings;
        RefreshReadyReadSessionsIdx(readSettings);

        if (!ReadyReadSessionIdx) {
            return std::nullopt;
        }

        const auto i = *ReadyReadSessionIdx;
        ReadyReadSessionIdx.reset();

        auto& readSession = ReadSessions[i];
        auto event = readSession.ExtractEvent();

        if (readSession.ReadEvent(readSettings.Block(false))) {
            ReadyReadSessionIdx = i;
            SequentialEventsRead++;
        } else {
            SequentialEventsRead = 0;
        }

        return event;
    }

    bool Close(TDuration timeout) final {
        bool success = true;
        TStringBuilder errors;
        for (const auto& readSession : ReadSessions) {
            try {
                success = readSession.Close(timeout) && success;
            } catch (const std::exception& e) {
                errors << e.what() << "; " << Endl;
            }
        }

        if (errors) {
            throw yexception() << "Failed to close composite read session: " << errors;
        }

        return success;
    }

    TReaderCounters::TPtr GetCounters() const final {
        return nullptr;
    }

    std::string GetSessionId() const final {
        return SessionId;
    }

    // ICompositeTopicReadSessionControl

    void AdvanceTime(TInstant readTime) final {
        const auto prevTime = ExternalReadTime;
        ExternalReadTime = readTime;

        if (AdvanceTimePromise && prevTime < readTime) {
            const auto it = PartitionsReadTimeSet.upper_bound({prevTime + MaxPartitionReadSkew, Max<ui64>()});
            if (it != PartitionsReadTimeSet.end() && it->first <= readTime + MaxPartitionReadSkew) {
                // There is new not suspended partition, we should refresh WaitEvent feature
                AdvanceTimePromise->SetValue();
                AdvanceTimePromise.reset();
            }
        }

        if (ReadyReadSessionIdx && !ReadSessions[*ReadyReadSessionIdx].CheckEvent()) {
            ReadyReadSessionIdx.reset();
        }
    }

    TInstant GetReadTime() const final {
        if (PartitionsReadTimeSet.empty()) {
            return TInstant::Zero();
        }

        return PartitionsReadTimeSet.begin()->first;
    }

    NThreading::TFuture<void> SubscribeOnUpdate() final {
        if (UpdatePromise.IsReady()) {
            UpdatePromise = NThreading::NewPromise<void>();
        }
        return UpdatePromise.GetFuture();
    }

private:
    static TInstant GetEventWriteTime(const TReadSessionEvent::TDataReceivedEvent& event) {
        TInstant result;

        auto messagesCount = event.GetMessagesCount();
        if (event.HasCompressedMessages()) {
            const auto& compressedMessages = event.GetCompressedMessages();
            messagesCount -= compressedMessages.size();

            for (const auto& compressedMessage : compressedMessages) {
                result = std::max(result, compressedMessage.GetWriteTime());
            }
        }

        if (messagesCount) {
            for (const auto& message : event.GetMessages()) {
                result = std::max(result, message.GetWriteTime());
            }
        }

        return result;
    }

    void RefreshReadyReadSessionsIdx(TReadSessionGetEventSettings& settings) {
        if (SequentialEventsRead >= ReadSessions.size()) {
            // Switch to another partition
            SequentialEventsRead = 0;
            ReadyReadSessionIdx.reset();
        }

        if (ReadyReadSessionIdx) {
            return;
        }

        for (ui64 i = 0; i < ReadSessions.size(); ++i) {
            if (++LastReadyReadSessionIdx >= ReadSessions.size()) {
                LastReadyReadSessionIdx = 0;
            }

            if (ReadSessions[LastReadyReadSessionIdx].ReadEvent(settings)) {
                ReadyReadSessionIdx = LastReadyReadSessionIdx;
                settings.Block(false);
                break;
            }
        }
    }

    bool CanProcessEvent(ui64 idx, const TReadSessionEvent::TEvent& event) {
        if (!std::holds_alternative<TReadSessionEvent::TDataReceivedEvent>(event)) {
            return true;
        }

        const auto eventTime = GetEventWriteTime(std::get<TReadSessionEvent::TDataReceivedEvent>(event));

        // Refresh minimal read time for local partition
        if (const auto prevTime = PartitionsReadTime[idx]; eventTime > prevTime) {
            const auto lastReadTime = GetReadTime();
            PartitionsReadTimeSet.erase({prevTime, idx});
            PartitionsReadTimeSet.emplace(eventTime, idx);
            PartitionsReadTime[idx] = eventTime;

            if (!UpdatePromise.IsReady() && lastReadTime != GetReadTime()) {
                UpdatePromise.SetValue();
            }
        }

        return eventTime <= ExternalReadTime + MaxPartitionReadSkew;
    }

    const TString SessionId;
    const TDuration MaxPartitionReadSkew;
    NThreading::TPromise<void> UpdatePromise;
    std::optional<NThreading::TPromise<void>> AdvanceTimePromise;

    TInstant ExternalReadTime; // Minimal read time from external partitions
    std::vector<TInstant> PartitionsReadTime;
    std::set<std::pair<TInstant, ui64>> PartitionsReadTimeSet;

    std::vector<TReadSession> ReadSessions;
    std::optional<ui64> ReadyReadSessionIdx; // Session with at least one ready event
    ui64 LastReadyReadSessionIdx = 0;
    ui64 SequentialEventsRead = 0;
};

} // anonymous namespace

std::pair<std::shared_ptr<IReadSession>, ICompositeTopicReadSessionControl::TPtr> CreateCompositeTopicReadSession(const TCompositeTopicReadSessionSettings& settings, const std::vector<std::shared_ptr<IReadSession>>& readSessions) {
    auto compositeReadSession = std::make_shared<TCompositeTopicReadSession>(settings, readSessions);
    return {compositeReadSession, compositeReadSession};
}

} // namespace NYql
