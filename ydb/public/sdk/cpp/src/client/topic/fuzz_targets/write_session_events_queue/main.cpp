#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/write_events.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/impl/write_session_impl.h>
#include <ydb/public/sdk/cpp/src/client/topic/impl/write_session_impl.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <variant>
#include <vector>

namespace {

class TTokenIssuer : protected NYdb::NTopic::TContinuationTokenIssuer {
public:
    static NYdb::NTopic::TContinuationToken Make() {
        return IssueContinuationToken();
    }
};

template <class TWriteAck>
std::vector<TWriteAck> MakeAcks(FuzzedDataProvider& fdp) {
    std::vector<TWriteAck> result;
    const size_t count = fdp.ConsumeIntegralInRange<size_t>(0, 8);
    result.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        TWriteAck ack;
        ack.SeqNo = fdp.ConsumeIntegral<ui64>();
        ack.State = static_cast<typename TWriteAck::EEventState>(
            fdp.ConsumeIntegralInRange<int>(0, 2));
        if (fdp.ConsumeBool()) {
            typename TWriteAck::TWrittenMessageDetails details;
            details.Offset = fdp.ConsumeIntegral<ui64>();
            details.PartitionId = fdp.ConsumeIntegral<ui64>();
            ack.Details = details;
        }
        result.push_back(std::move(ack));
    }
    return result;
}

template <class TWriteSessionEvent>
void TouchWriteEvent(typename TWriteSessionEvent::TEvent& event) {
    if (auto* acks = std::get_if<typename TWriteSessionEvent::TAcksEvent>(&event)) {
        for (const auto& ack : acks->Acks) {
            (void)ack.SeqNo;
            (void)ack.State;
            if (ack.Details) {
                (void)ack.Details->Offset;
                (void)ack.Details->PartitionId;
            }
        }
    } else if (auto* ready = std::get_if<typename TWriteSessionEvent::TReadyToAcceptEvent>(&event)) {
        auto token = std::move(ready->ContinuationToken);
        (void)token;
    } else if (auto* closed = std::get_if<NYdb::NTopic::TSessionClosedEvent>(&event)) {
        (void)closed->GetStatus();
    }
}

template <class TQueue, class TSettings, class TWriteSessionEvent>
void FuzzWriteQueue(FuzzedDataProvider& fdp) {
    TSettings settings;
    TQueue queue(settings);

    const size_t operations = fdp.ConsumeIntegralInRange<size_t>(0, 32);
    for (size_t i = 0; i < operations; ++i) {
        switch (fdp.ConsumeIntegralInRange<int>(0, 2)) {
            case 0: {
                queue.PushEvent(typename TWriteSessionEvent::TReadyToAcceptEvent(TTokenIssuer::Make()));
                break;
            }
            case 1: {
                typename TWriteSessionEvent::TAcksEvent event;
                event.Acks = MakeAcks<typename TWriteSessionEvent::TWriteAck>(fdp);
                queue.PushEvent(std::move(event));
                break;
            }
            default: {
                if (fdp.ConsumeBool()) {
                    auto event = queue.GetEvent(false);
                    if (event) {
                        TouchWriteEvent<TWriteSessionEvent>(*event);
                    }
                } else {
                    auto events = queue.GetEvents(
                        false,
                        fdp.ConsumeIntegralInRange<size_t>(0, 8));
                    for (auto& event : events) {
                        TouchWriteEvent<TWriteSessionEvent>(event);
                    }
                }
                break;
            }
        }
    }

    while (auto event = queue.GetEvent(false)) {
        TouchWriteEvent<TWriteSessionEvent>(*event);
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    FuzzedDataProvider fdp(data, size);

    FuzzWriteQueue<
        NYdb::NTopic::TWriteSessionEventsQueue,
        NYdb::NTopic::TWriteSessionSettings,
        NYdb::NTopic::TWriteSessionEvent>(fdp);
    FuzzWriteQueue<
        NYdb::NPersQueue::TWriteSessionEventsQueue,
        NYdb::NPersQueue::TWriteSessionSettings,
        NYdb::NPersQueue::TWriteSessionEvent>(fdp);

    return 0;
}
