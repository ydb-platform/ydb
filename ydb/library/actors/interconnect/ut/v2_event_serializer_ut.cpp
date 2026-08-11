#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/actors/interconnect/v2_event_serializer.h>
#include <ydb/library/actors/protos/unittests.pb.h>

#include <contrib/restricted/abseil-cpp-tstring/y_absl/strings/cord.h>
#include <contrib/restricted/abseil-cpp-tstring/y_absl/strings/cord_test_helpers.h>

#include <util/string/cast.h>

using namespace NActors;

struct TEvPrivate {
    enum {
        EvTest = EventSpaceBegin(TEvents::ES_PRIVATE),
    };

    struct TEvProto : public TEventPB<TEvProto, TMessageWithPayload, EvTest> {};
};

struct TEventProcessor : TEventDeserializer::IEventProcessor {
    std::deque<std::unique_ptr<IEventHandle>> Events;

    void PushEvent(std::unique_ptr<IEventHandle> event) override {
        Events.push_back(std::move(event));
    }

    void Process(NActorsInterconnect::TSystemPayloadV2&) override {}
};

// deterministic payload for the event with the given index, so that the receiver can validate consistency
TString MakeSomeData(ui64 index, size_t len) {
    return TString(len, static_cast<char>('A' + index % 26));
}

TString MakePayloadData(ui64 index, size_t len) {
    return TString(len, static_cast<char>('a' + index % 26));
}

std::unique_ptr<IEventHandle> MakeIndexedEvent(ui16 channel, ui64 index, size_t dataLen, bool withPayload) {
    auto ev = std::make_unique<TEvPrivate::TEvProto>();
    ev->Record.SetMeta(ToString(index));
    ev->Record.AddSomeData(MakeSomeData(index, dataLen));
    if (withPayload) {
        ev->AddPayload(TRope(MakePayloadData(index, dataLen)));
    }
    TActorId sender(1, 2, 3, static_cast<ui32>(index));
    TActorId recipient(2, 3, 4, 5);
    // the cookie carries the event index so we can restore delivery order on the receiving side
    return std::make_unique<IEventHandle>(recipient, sender, ev.release(),
        IEventHandle::MakeFlags(channel, 0), index /*cookie*/);
}

void CheckIndexedEvent(IEventHandle& outEv, ui16 channel, size_t dataLen, bool withPayload) {
    const ui64 index = outEv.Cookie;
    UNIT_ASSERT_VALUES_EQUAL(outEv.GetChannel(), channel);
    UNIT_ASSERT_VALUES_EQUAL(outEv.Type, static_cast<ui32>(TEvPrivate::EvTest));
    UNIT_ASSERT(outEv.Sender == TActorId(1, 2, 3, static_cast<ui32>(index)));
    UNIT_ASSERT(outEv.Recipient == TActorId(2, 3, 4, 5));
    auto& out = *outEv.Get<TEvPrivate::TEvProto>();
    UNIT_ASSERT(out.Record.HasMeta());
    UNIT_ASSERT_VALUES_EQUAL(out.Record.GetMeta(), ToString(index));
    UNIT_ASSERT_VALUES_EQUAL(out.Record.SomeDataSize(), 1);
    UNIT_ASSERT_VALUES_EQUAL(out.Record.GetSomeData(0), MakeSomeData(index, dataLen));
    UNIT_ASSERT_VALUES_EQUAL(out.GetPayloadCount(), withPayload ? 1 : 0);
    if (withPayload) {
        UNIT_ASSERT_VALUES_EQUAL(out.GetPayload(0).ConvertToString(), MakePayloadData(index, dataLen));
    }
}

std::vector<TContiguousSpan> Serialize(TEventSerializer& ser, std::vector<TRcBuf>& bufs) {
    std::vector<TContiguousSpan> spans;

    for (;;) {
        if (bufs.empty() || bufs.back().size() < 1024) {
            bufs.push_back(TRcBuf::Uninitialized(65536));
        }
        const size_t produced = ser.ProduceOutputStream(bufs.back(), &spans);
        if (!produced) {
            break;
        }
    }

    return spans;
}

void CheckSerializeThenDeserialize(bool withPayload, bool buffer, ui32 metaLength) {
    auto ev = std::make_unique<TEvPrivate::TEvProto>();
    ev->Record.SetMeta(TString(metaLength, 'X'));
    ev->Record.AddSomeData("world");
    TActorId sender(1, 2, 3, 4);
    TActorId recipient(2, 3, 4, 5);
    ui64 cookie = 12387134;
    if (withPayload) {
        ev->AddPayload(TRope(TString("hello, world")));
    }
    auto h = std::make_unique<IEventHandle>(recipient, sender, ev.release(), 0, cookie);
    if (buffer) {
        h = std::make_unique<IEventHandle>(h->Type, h->Flags, h->Recipient, h->Sender, h->ReleaseChainBuffer(),
            h->Cookie, nullptr, std::move(h->TraceId));
    }

    TEventSerializer ser(true);
    ser.Push(std::move(h));

    std::vector<TRcBuf> bufs;
    std::vector<TContiguousSpan> spans = Serialize(ser, bufs);

    TEventDeserializer deser(TScopeId{});
    TEventProcessor processor;
    for (TContiguousSpan span : spans) {
        UNIT_ASSERT(processor.Events.empty());
        deser.Push(TRcBuf::Copy(span), &processor, {});
    }
    UNIT_ASSERT(processor.Events.size() == 1);
    auto& outEv = *processor.Events.front();
    UNIT_ASSERT(outEv.Sender == sender);
    UNIT_ASSERT(outEv.Recipient == recipient);
    UNIT_ASSERT(outEv.Cookie == cookie);
    UNIT_ASSERT(outEv.Type == TEvPrivate::EvTest);
    auto& out = *outEv.Get<TEvPrivate::TEvProto>();
    UNIT_ASSERT(out.Record.HasMeta());
    UNIT_ASSERT_VALUES_EQUAL(out.Record.GetMeta(), TString(metaLength, 'X'));
    UNIT_ASSERT_VALUES_EQUAL(out.Record.SomeDataSize(), 1);
    UNIT_ASSERT_VALUES_EQUAL(out.Record.GetSomeData(0), "world");
    UNIT_ASSERT_VALUES_EQUAL(out.GetPayloadCount(), withPayload ? 1 : 0);
    if (withPayload) {
        UNIT_ASSERT_VALUES_EQUAL(out.GetPayload(0).ConvertToString(), "hello, world");
    }
}

Y_UNIT_TEST_SUITE(EventSerializerV2) {

    Y_UNIT_TEST(CheckSerializeThenDeserializeProtoWithoutPayload) {
        CheckSerializeThenDeserialize(false, false, 10);
    }

    Y_UNIT_TEST(CheckSerializeThenDeserializeProtoWithPayload) {
        CheckSerializeThenDeserialize(true, false, 10);
    }

    Y_UNIT_TEST(CheckSerializeThenDeserializeBufferWithoutPayload) {
        CheckSerializeThenDeserialize(false, true, 10);
    }

    Y_UNIT_TEST(CheckSerializeThenDeserializeBufferWithPayload) {
        CheckSerializeThenDeserialize(true, true, 10);
    }

    Y_UNIT_TEST(CheckSerializeThenDeserializeProtoWithoutPayloadLong) {
        CheckSerializeThenDeserialize(false, false, 1000);
    }

    Y_UNIT_TEST(CheckSerializeThenDeserializeProtoWithPayloadLong) {
        CheckSerializeThenDeserialize(true, false, 1000);
    }

    Y_UNIT_TEST(CheckSerializeThenDeserializeBufferWithoutPayloadLong) {
        CheckSerializeThenDeserialize(false, true, 1000);
    }

    Y_UNIT_TEST(CheckSerializeThenDeserializeBufferWithPayloadLong) {
        CheckSerializeThenDeserialize(true, true, 1000);
    }

    // Feed the serialized stream to the deserializer in tiny fragments (1..3 bytes), so chunk headers and
    // bodies straddle buffer boundaries exactly as they do with real TCP segmentation (which loopback,
    // delivering big buffers, never triggers). Every event must still round-trip intact and in order.
    Y_UNIT_TEST(DeserializeFromFragmentedStream) {
        constexpr size_t numEvents = 300;
        constexpr ui16 numChannels = 4;

        auto channelOf = [](ui64 i) -> ui16 { return i % numChannels; };
        auto dataLenOf = [](ui64 i) -> size_t { return (i % 11) * 37 + 1; };
        auto withPayloadOf = [](ui64 i) -> bool { return i % 2; };

        TEventSerializer ser(false);
        for (ui64 i = 0; i < numEvents; ++i) {
            ser.Push(MakeIndexedEvent(channelOf(i), i, dataLenOf(i), withPayloadOf(i)));
        }

        std::vector<TRcBuf> bufs;
        std::vector<TContiguousSpan> spans = Serialize(ser, bufs);

        TString stream;
        for (const TContiguousSpan& s : spans) {
            stream.append(s.data(), s.size());
        }

        TEventDeserializer deser(TScopeId{});
        TEventProcessor processor;
        std::vector<std::pair<ui16, ui64>> delivered;

        size_t pos = 0;
        while (pos < stream.size()) {
            const size_t frag = Min<size_t>(1 + (pos % 3), stream.size() - pos);
            deser.Push(TRcBuf::Copy(TContiguousSpan(stream.data() + pos, frag)), &processor, TActorId());
            pos += frag;
            while (!processor.Events.empty()) {
                auto ev = std::move(processor.Events.front());
                processor.Events.pop_front();
                const ui64 index = ev->Cookie;
                CheckIndexedEvent(*ev, channelOf(index), dataLenOf(index), withPayloadOf(index));
                delivered.push_back({ev->GetChannel(), index});
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(delivered.size(), numEvents);
        std::array<ui64, numChannels> nextPerChannel{};
        for (const auto& [channel, index] : delivered) {
            UNIT_ASSERT_VALUES_EQUAL(channelOf(index), channel);
            UNIT_ASSERT_VALUES_EQUAL(index, nextPerChannel[channel] * numChannels + channel);
            ++nextPerChannel[channel];
        }
    }

    // A large event on one channel interleaves with many small events on another, so their serialized bytes
    // end up scattered across many pipelined batches. Acknowledging (committing) the batches one at a time
    // must never release an event whose bytes still live in a not-yet-committed batch -- otherwise the
    // aliased payload memory is freed while a later batch still references it. That is a use-after-free that
    // only manifests once the network actually sends those later bytes (loopback sends inline, so it hides
    // it). We surface it by reading every not-yet-committed batch's spans after each commit -- under ASAN a
    // premature release fails here; it also corrupts the round-trip which we verify at the end.
    Y_UNIT_TEST(CommitDoesNotReleaseEventsWithBytesStillInFlight) {
        auto channelOf = [](ui64 i) -> ui16 { return i == 0 ? 1 : 2; };
        auto dataLenOf = [](ui64 i) -> size_t { return i == 0 ? 60000 : 1000; };

        TEventSerializer ser(false);
        constexpr ui64 numEvents = 41;
        ser.Push(MakeIndexedEvent(channelOf(0), 0, dataLenOf(0), /*withPayload=*/true));
        for (ui64 i = 1; i < numEvents; ++i) {
            ser.Push(MakeIndexedEvent(channelOf(i), i, dataLenOf(i), /*withPayload=*/true));
        }

        struct TBatch {
            TRcBuf Scratch;
            std::vector<TContiguousSpan> Spans;
            size_t Bytes;
        };
        std::vector<TBatch> batches;
        for (;;) {
            TRcBuf scratch = TRcBuf::Uninitialized(4096); // small scratch -> many pipelined batches
            std::vector<TContiguousSpan> spans;
            size_t total = 0;
            for (;;) {
                const size_t produced = ser.ProduceOutputStream(scratch, &spans);
                total += produced;
                if (produced == 0 || scratch.size() < sizeof(TEventSerializer::TChunkHeader) + sizeof(ui32)) {
                    break;
                }
            }
            if (total == 0) {
                break;
            }
            batches.push_back({std::move(scratch), std::move(spans), total});
        }

        // gather the full stream up front (all spans still reference live memory here) for a round-trip check
        TString stream;
        for (const TBatch& b : batches) {
            for (const TContiguousSpan& s : b.Spans) {
                stream.append(s.data(), s.size());
            }
        }

        // commit the batches one at a time; after each commit, touch every span that has not been committed
        // yet -- these must all still point to live memory
        volatile ui64 acc = 0;
        for (size_t i = 0; i < batches.size(); ++i) {
            ser.CommitProducedBytes(batches[i].Bytes);
            for (size_t j = i + 1; j < batches.size(); ++j) {
                for (const TContiguousSpan& s : batches[j].Spans) {
                    for (size_t k = 0; k < s.size(); ++k) {
                        acc += static_cast<ui8>(s.data()[k]);
                    }
                }
            }
        }
        Y_UNUSED(acc);

        // the stream captured before any commit must still round-trip to the original events
        TEventDeserializer deser(TScopeId{});
        TEventProcessor processor;
        deser.Push(TRcBuf::Copy(TContiguousSpan(stream.data(), stream.size())), &processor, TActorId());
        UNIT_ASSERT_VALUES_EQUAL(processor.Events.size(), numEvents);
        while (!processor.Events.empty()) {
            auto ev = std::move(processor.Events.front());
            processor.Events.pop_front();
            const ui64 index = ev->Cookie;
            CheckIndexedEvent(*ev, channelOf(index), dataLenOf(index), /*withPayload=*/true);
        }
    }

    // Push a lot of events into one channel and only a few into another one, then serialize the whole thing into a
    // stream and deserialize it back. Verifies that:
    //   * every event is delivered exactly once, in FIFO order within each channel;
    //   * every event survives the round-trip unchanged (consistency);
    //   * the flooded channel does not starve the other one -- its few events are delivered early instead of being
    //     stuck behind the whole flood (equal bandwidth distribution).
    Y_UNIT_TEST(FairBandwidthDistributionAcrossChannels) {
        constexpr ui16 floodChannel = 3;
        constexpr ui16 otherChannel = 7;
        constexpr size_t numFlood = 1000;
        constexpr size_t numOther = 30;
        constexpr size_t floodDataLen = 512;
        constexpr size_t otherDataLen = 8;

        TEventSerializer ser(true);
        for (ui64 i = 0; i < numFlood; ++i) {
            ser.Push(MakeIndexedEvent(floodChannel, i, floodDataLen, true /*withPayload*/));
        }
        for (ui64 i = 0; i < numOther; ++i) {
            ser.Push(MakeIndexedEvent(otherChannel, i, otherDataLen, false /*withPayload*/));
        }

        std::vector<TRcBuf> bufs;
        std::vector<TContiguousSpan> spans = Serialize(ser, bufs);

        // feed the produced stream to the deserializer chunk by chunk and record the exact delivery order
        TEventDeserializer deser(TScopeId{});
        TEventProcessor processor;

        struct TDelivered {
            ui16 Channel;
            ui64 Cookie;
        };
        std::vector<TDelivered> delivered;

        for (TContiguousSpan span : spans) {
            deser.Push(TRcBuf::Copy(span), &processor, {});
            while (!processor.Events.empty()) {
                auto ev = std::move(processor.Events.front());
                processor.Events.pop_front();
                const ui16 channel = ev->GetChannel();
                const bool withPayload = channel == floodChannel;
                const size_t dataLen = channel == floodChannel ? floodDataLen : otherDataLen;
                CheckIndexedEvent(*ev, channel, dataLen, withPayload);
                delivered.push_back({channel, ev->Cookie});
            }
        }

        // every event must be delivered exactly once, preserving FIFO order within each channel
        UNIT_ASSERT_VALUES_EQUAL(delivered.size(), numFlood + numOther);
        ui64 nextFlood = 0;
        ui64 nextOther = 0;
        size_t lastOtherPos = 0;
        size_t floodBeforeLastOther = 0;
        for (size_t pos = 0; pos < delivered.size(); ++pos) {
            const TDelivered& d = delivered[pos];
            if (d.Channel == floodChannel) {
                UNIT_ASSERT_VALUES_EQUAL(d.Cookie, nextFlood++);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(d.Channel, otherChannel);
                UNIT_ASSERT_VALUES_EQUAL(d.Cookie, nextOther++);
                lastOtherPos = pos;
                floodBeforeLastOther = nextFlood;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(nextFlood, numFlood);
        UNIT_ASSERT_VALUES_EQUAL(nextOther, numOther);

        // fairness: by the time the last small-channel event is delivered, only a small fraction of the flood must have
        // been delivered -- otherwise the flood would be blocking the other channel's receivers
        Cerr << "lastOtherPos# " << lastOtherPos << " floodBeforeLastOther# " << floodBeforeLastOther << Endl;
        UNIT_ASSERT_C(floodBeforeLastOther < numFlood / 4,
            "the flooded channel blocks the other one: floodBeforeLastOther# " << floodBeforeLastOther);
    }

    // Mock event whose body is emitted solely via WriteCord. The Cord is temporary — built inside
    // SerializeToArcadiaStream from MakeCordFromExternal and destroyed when that returns — so only
    // WriteCord's GetCords() (drained into RefcountItems::Cords) keeps the aliased backing alive.
    // Keeping a Cord member on the event would hide a missing RefcountItems::Cords push, because the
    // Event itself is retained until CommitProducedBytes.
    //
    // The external releaser poisons the backing with 0xAB before freeing it, so a premature Cord
    // release is visible in still-aliased output spans without relying on ASAN or allocator churn.
    struct TCordEvent: public IEventBase {
        static constexpr char Poison = char(0xAB);

        char Fill = 'X';
        size_t Size = 0;
        bool Fragmented = false;

        TCordEvent(char fill, size_t size, bool fragmented = false)
            : Fill(fill)
            , Size(size)
            , Fragmented(fragmented)
        {}

        static y_absl::Cord MakePoisoningCord(char fill, size_t size) {
            auto* heap = new TString(size, fill);
            return y_absl::MakeCordFromExternal(
                y_absl::string_view(heap->data(), heap->size()),
                [heap](y_absl::string_view) {
                    if (!heap->empty()) {
                        memset(heap->Detach(), static_cast<unsigned char>(Poison), heap->size());
                    }
                    delete heap;
                });
        }

        y_absl::Cord MakeTemporaryCord() const {
            if (Fragmented) {
                const size_t part = Size / 3;
                y_absl::Cord cord = MakePoisoningCord(Fill, part);
                cord.Append(MakePoisoningCord(static_cast<char>(Fill + 1), part));
                cord.Append(MakePoisoningCord(static_cast<char>(Fill + 2), Size - 2 * part));
                return cord;
            }
            return MakePoisoningCord(Fill, Size);
        }

        bool SerializeToArcadiaStream(TChunkSerializer* chunker) const override {
            y_absl::Cord tmp = MakeTemporaryCord();
            return chunker->WriteCord(tmp);
        }
        bool IsSerializable() const override {
            return true;
        }
        TString ToStringHeader() const override {
            return TString();
        }
        ui32 Type() const override {
            return TEvPrivate::EvTest + 100;
        }
        ui32 CalculateSerializedSize() const override {
            return Size;
        }
    };

    std::unique_ptr<IEventHandle> MakeCordEventHandle(ui16 channel, ui64 cookie, char fill, size_t size,
            bool fragmented = false)
    {
        auto* ev = new TCordEvent(fill, size, fragmented);
        TActorId sender(1, 2, 3, 4);
        TActorId recipient(2, 3, 4, 5);
        return std::make_unique<IEventHandle>(recipient, sender, ev,
            IEventHandle::MakeFlags(channel, 0), cookie);
    }

    Y_UNIT_TEST(WriteCordBytesAppearInOutputStream) {
        constexpr size_t cordSize = 4096 + 27;
        TEventSerializer ser(true);
        ser.Push(MakeCordEventHandle(/*channel=*/1, /*cookie=*/7, /*fill=*/'Z', /*size=*/cordSize));

        std::vector<TRcBuf> bufs;
        std::vector<TContiguousSpan> spans = Serialize(ser, bufs);

        TString stream;
        for (const TContiguousSpan& s : spans) {
            stream.append(s.data(), s.size());
        }

        // Event body is raw Cord bytes interleaved with chunk headers, so long runs may be split —
        // check a short run and that enough Z bytes made it into the stream.
        UNIT_ASSERT(stream.Contains(TString(64, 'Z')));
        size_t zCount = 0;
        for (char c : stream) {
            if (c == 'Z') {
                ++zCount;
            }
        }
        UNIT_ASSERT_GE(zCount, cordSize);
        UNIT_ASSERT_GE(stream.size(), cordSize);
    }

    // Same UAF pattern as CommitDoesNotReleaseEventsWithBytesStillInFlight, but the aliased memory comes
    // from a temporary WriteCord Cord / RefcountItems::Cords — not from an Event-owned Cord member.
    Y_UNIT_TEST(CommitDoesNotReleaseCordBytesStillInFlight) {
        TEventSerializer ser(true);

        // One large fragmented Cord-backed event plus many smaller ones so serialization pipelines
        // across batches. External releasers poison with 0xAB when the Cord is dropped.
        constexpr size_t largeSize = 60000;
        ser.Push(MakeCordEventHandle(/*channel=*/1, /*cookie=*/0, /*fill=*/'A', largeSize, /*fragmented=*/true));

        constexpr ui64 numSmall = 40;
        for (ui64 i = 1; i <= numSmall; ++i) {
            ser.Push(MakeCordEventHandle(/*channel=*/2, i, static_cast<char>('a' + (i % 26)), 1000));
        }

        struct TBatch {
            TRcBuf Scratch;
            std::vector<TContiguousSpan> Spans;
            size_t Bytes;
        };
        // One ProduceOutputStream call per batch: Cord aliasing barely consumes scratch, so draining
        // a large scratch in an inner loop would collapse everything into a single batch.
        std::vector<TBatch> batches;
        for (;;) {
            TRcBuf scratch = TRcBuf::Uninitialized(512);
            std::vector<TContiguousSpan> spans;
            const size_t produced = ser.ProduceOutputStream(scratch, &spans);
            if (!produced) {
                break;
            }
            batches.push_back({std::move(scratch), std::move(spans), produced});
        }

        TString stream;
        for (const TBatch& b : batches) {
            for (const TContiguousSpan& s : b.Spans) {
                stream.append(s.data(), s.size());
            }
        }
        // Fragmented large event is Fill/'A', Fill+1/'B', Fill+2/'C' thirds. If RefcountItems::Cords
        // was skipped, the external releaser already poisoned those aliases with 0xAB.
        UNIT_ASSERT(stream.Contains(TString(64, 'A')));
        UNIT_ASSERT(stream.Contains(TString(64, 'B')));
        UNIT_ASSERT(stream.Contains(TString(64, 'C')));
        UNIT_ASSERT(!stream.Contains(TString(64, TCordEvent::Poison)));

        volatile ui64 acc = 0;
        for (size_t i = 0; i < batches.size(); ++i) {
            ser.CommitProducedBytes(batches[i].Bytes);
            for (size_t j = i + 1; j < batches.size(); ++j) {
                for (const TContiguousSpan& s : batches[j].Spans) {
                    for (size_t k = 0; k < s.size(); ++k) {
                        acc += static_cast<ui8>(s.data()[k]);
                    }
                }
            }
            // After committing earlier batches, in-flight Cord aliases must still be unpoisoned.
            TString rest;
            for (size_t j = i + 1; j < batches.size(); ++j) {
                for (const TContiguousSpan& s : batches[j].Spans) {
                    rest.append(s.data(), s.size());
                }
            }
            if (!rest.empty()) {
                UNIT_ASSERT_C(!rest.Contains(TString(64, TCordEvent::Poison)),
                    "Cord backing was released while bytes still in flight (missing RefcountItems::Cords?)");
            }
        }
        Y_UNUSED(acc);
        UNIT_ASSERT_GT(batches.size(), 1);
    }

}
