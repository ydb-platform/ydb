// Micro-benchmarks for the session v2 wire format: TEventSerializer (framing, per-channel scheduling,
// checksumming, copy-vs-alias decisions) and TEventDeserializer (chunk reassembly). Neither needs an actor
// system nor a socket, so these numbers are stable and isolate the data-plane code from scheduling noise.

#include <library/cpp/testing/gbenchmark/benchmark.h>

#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/interconnect/v2_event_serializer.h>
#include <ydb/library/actors/protos/unittests.pb.h>

#include <util/generic/string.h>

using namespace NActors;

namespace {

    struct TEvBench: TEventPB<TEvBench, TMessageWithPayload, EventSpaceBegin(TEvents::ES_PRIVATE)> {};

    const TActorId Sender(1, 2, 3, 4);
    const TActorId Recipient(2, 3, 4, 5);

    // Two ways an event carries its bytes, which the serializer treats very differently: an inline string
    // inside the protobuf must be copied into the output scratch buffer, whereas a rope payload is large
    // enough to be aliased (referenced in place) and never touched by memcpy.
    enum EPayload {
        InlineInProto = 0,
        SeparateRope = 1,
    };

    std::unique_ptr<IEventHandle> MakeEvent(size_t payloadSize, EPayload payload, ui16 channel, ui64 cookie) {
        auto ev = std::make_unique<TEvBench>();
        ev->Record.SetMeta("bench");
        if (payload == SeparateRope) {
            ev->AddPayload(TRope(TString(payloadSize, 'x')));
        } else {
            ev->Record.AddSomeData(TString(payloadSize, 'x'));
        }
        return std::make_unique<IEventHandle>(Recipient, Sender, ev.release(),
            IEventHandle::MakeFlags(channel, 0), cookie);
    }

    // Serialized form of the event, reusable across benchmark iterations: constructing a handle around it is
    // a refcount bump, so the measured time is the serializer's and not the protobuf's. This is also exactly
    // what the engine sees when TInterconnectSettings::V2::EnablePreserializeEvents is on.
    TIntrusivePtr<TEventSerializedData> MakeSerializedData(size_t payloadSize, EPayload payload) {
        auto ev = MakeEvent(payloadSize, payload, 0, 0);
        return ev->ReleaseChainBuffer();
    }

    std::unique_ptr<IEventHandle> WrapSerializedData(const TIntrusivePtr<TEventSerializedData>& data,
            ui16 channel, ui64 cookie) {
        return std::make_unique<IEventHandle>(TEvBench::EventType, IEventHandle::MakeFlags(channel, 0),
            Recipient, Sender, data, cookie);
    }

    // Drives TEventSerializer the way the engine's shard worker does: push, drain into a scratch buffer plus
    // a span list, then acknowledge the produced bytes so the event's memory is released.
    class TSerializeDriver {
    public:
        explicit TSerializeDriver(bool checksumming)
            : Serializer(checksumming)
        {}

        size_t Push(std::unique_ptr<IEventHandle> ev) {
            Serializer.Push(std::move(ev));

            size_t produced = 0;
            for (;;) {
                if (Scratch.size() < MinScratchSize) {
                    Scratch = TRcBuf::Uninitialized(ScratchSize);
                }
                const size_t bytes = Serializer.ProduceOutputStream(Scratch, &Spans);
                if (!bytes) {
                    break;
                }
                produced += bytes;
            }

            Spans.clear();
            Serializer.CommitProducedBytes(produced);
            return produced;
        }

        TEventSerializer& Get() {
            return Serializer;
        }

    private:
        // Mirrors the engine's adaptive serialize window at its upper end; a chunk header plus a length is
        // the smallest amount of scratch that can still make progress.
        static constexpr size_t ScratchSize = 256 << 10;
        static constexpr size_t MinScratchSize = 1024;

        TEventSerializer Serializer;
        TRcBuf Scratch;
        std::vector<TContiguousSpan> Spans;
    };

    struct TDroppingProcessor: TEventDeserializer::IEventProcessor {
        size_t Count = 0;

        void PushEvent(std::unique_ptr<IEventHandle>) override {
            ++Count;
        }

        void Process(NActorsInterconnect::TSystemPayloadV2&) override {}
    };

    // Concatenates the serialized form of `count` events into one contiguous buffer, so the deserializer can
    // be fed the same stream over and over.
    TRcBuf MakeStream(size_t payloadSize, EPayload payload, bool checksumming, ui16 numChannels, size_t count) {
        TEventSerializer serializer(checksumming);
        for (size_t i = 0; i < count; ++i) {
            serializer.Push(MakeEvent(payloadSize, payload, i % numChannels, i));
        }

        TString stream;
        std::vector<TRcBuf> buffers;
        std::vector<TContiguousSpan> spans;
        for (;;) {
            if (buffers.empty() || buffers.back().size() < 1024) {
                buffers.push_back(TRcBuf::Uninitialized(256 << 10));
            }
            if (!serializer.ProduceOutputStream(buffers.back(), &spans)) {
                break;
            }
        }
        for (const TContiguousSpan& span : spans) {
            stream.append(span.data(), span.size());
        }
        return TRcBuf(std::move(stream));
    }

    void ApplyArgs(benchmark::Benchmark* bench) {
        bench->ArgNames({"payload", "checksum", "rope"})
            ->ArgsProduct({{64, 1024, 4096, 65536, 1 << 20}, {0, 1}, {InlineInProto, SeparateRope}})
            ->Unit(benchmark::kNanosecond);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////

    // Serialization of an already-serialized event: pure framing, checksum and copy-vs-alias cost.
    void SerializePreserialized(benchmark::State& state) {
        const size_t payloadSize = state.range(0);
        const auto data = MakeSerializedData(payloadSize, EPayload(state.range(2)));

        TSerializeDriver driver(state.range(1));
        ui64 cookie = 0;
        size_t bytes = 0;
        for (auto _ : state) {
            bytes += driver.Push(WrapSerializedData(data, 0, cookie++));
        }

        state.SetBytesProcessed(bytes);
        state.counters["copied"] = benchmark::Counter(driver.Get().GetBytesCopied(), benchmark::Counter::kIsRate);
        state.counters["aliased"] = benchmark::Counter(driver.Get().GetBytesAliased(), benchmark::Counter::kIsRate);
    }

    // Serialization starting from a live protobuf event, i.e. what the engine pays when preserialization is
    // off: the protobuf serializer runs on the shard worker thread as part of this call.
    void SerializeFromProto(benchmark::State& state) {
        const size_t payloadSize = state.range(0);
        const auto payload = EPayload(state.range(2));

        TSerializeDriver driver(state.range(1));
        ui64 cookie = 0;
        size_t bytes = 0;
        for (auto _ : state) {
            bytes += driver.Push(MakeEvent(payloadSize, payload, 0, cookie++));
        }

        state.SetBytesProcessed(bytes);
    }

    // Round-robin across 8 active channels, which exercises the per-channel quota heap and forces the
    // serializer to switch streams mid-flight instead of draining one queue.
    void SerializeMultiChannel(benchmark::State& state) {
        const size_t payloadSize = state.range(0);
        const auto data = MakeSerializedData(payloadSize, EPayload(state.range(2)));

        TSerializeDriver driver(state.range(1));
        ui64 cookie = 0;
        size_t bytes = 0;
        for (auto _ : state) {
            bytes += driver.Push(WrapSerializedData(data, cookie % 8, cookie));
            ++cookie;
        }

        state.SetBytesProcessed(bytes);
    }

    // Reassembly of a whole stream: chunk header parsing, per-channel accumulation, event handle
    // construction. The stream is pushed as one buffer, which is the loopback case; TCP segmentation would
    // add more partial-chunk work.
    void Deserialize(benchmark::State& state) {
        constexpr size_t EventsPerStream = 16;
        const TRcBuf stream = MakeStream(state.range(0), EPayload(state.range(2)), state.range(1),
            /*numChannels=*/1, EventsPerStream);

        TEventDeserializer deserializer{TScopeId{}};
        TDroppingProcessor processor;
        size_t bytes = 0;
        for (auto _ : state) {
            deserializer.Push(stream, &processor, TActorId());
            bytes += stream.size();
        }

        state.SetBytesProcessed(bytes);
        state.SetItemsProcessed(processor.Count);
    }

    BENCHMARK(SerializePreserialized)->Apply(ApplyArgs);
    BENCHMARK(SerializeFromProto)->Apply(ApplyArgs);
    BENCHMARK(SerializeMultiChannel)->Apply(ApplyArgs);
    BENCHMARK(Deserialize)->Apply(ApplyArgs);

} // namespace
