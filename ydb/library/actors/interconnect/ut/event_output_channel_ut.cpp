#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/actors/interconnect/interconnect_channel.h>
#include <ydb/library/actors/interconnect/interconnect_counters.h>
#include <ydb/library/actors/interconnect/outgoing_stream.h>
#include <ydb/library/actors/interconnect/ut/protos/interconnect_test.pb.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <optional>

using namespace NActors;

namespace {

struct TEvTestSerialization : public TEventPB<TEvTestSerialization, NInterconnectTest::TEvTestSerialization, 123> {};

struct TXdcPushData {
    EXdcCommand Command = EXdcCommand::PUSH_DATA;
    ui16 Size = 0;
    std::optional<ui32> Checksum;
};

struct TSerializedEvent {
    TVector<TXdcPushData> PushData;
    TString MainStreamData;
    TString XdcPayloadData;
    ui32 DescriptorFlags = 0;
    bool HasDescriptor = false;
};

TString ProduceString(NInterconnect::TOutgoingStream& stream) {
    TVector<TConstIoVec> data;
    stream.ProduceIoVec(data, 1024, Max<size_t>());

    TString result;
    for (const auto& [ptr, len] : data) {
        result.append(static_cast<const char*>(ptr), len);
    }
    return result;
}

void SkipDeclareSection(const char** ptr, const char* end) {
    const ui64 headroom = NInterconnect::NDetail::DeserializeNumber(ptr, end);
    const ui64 size = NInterconnect::NDetail::DeserializeNumber(ptr, end);
    const ui64 tailroom = NInterconnect::NDetail::DeserializeNumber(ptr, end);
    const ui64 alignment = NInterconnect::NDetail::DeserializeNumber(ptr, end);

    UNIT_ASSERT_VALUES_UNEQUAL(headroom, Max<ui64>());
    UNIT_ASSERT_VALUES_UNEQUAL(size, Max<ui64>());
    UNIT_ASSERT_VALUES_UNEQUAL(tailroom, Max<ui64>());
    UNIT_ASSERT_VALUES_UNEQUAL(alignment, Max<ui64>());
}

TVector<TXdcPushData> ExtractPushDataCommands(const TString& packet, ui32* descriptorFlags, bool* hasDescriptor) {
    UNIT_ASSERT_C(packet.size() >= sizeof(TTcpPacketHeader_v2), "packet is too short");

    TVector<TXdcPushData> result;
    const char* ptr = packet.data() + sizeof(TTcpPacketHeader_v2);
    const char* end = packet.data() + packet.size();

    while (ptr < end) {
        UNIT_ASSERT_C(static_cast<size_t>(end - ptr) >= sizeof(TChannelPart), "missing channel part header");

        TChannelPart part;
        memcpy(&part, ptr, sizeof(part));
        ptr += sizeof(part);

        UNIT_ASSERT_C(static_cast<size_t>(end - ptr) >= part.Size, "invalid channel part payload size");
        const char* partEnd = ptr + part.Size;

        if (!part.IsXdc()) {
            if (part.IsLastPart()) {
                UNIT_ASSERT_VALUES_EQUAL(part.Size, sizeof(TEventDescr2));
                TEventDescr2 descr;
                memcpy(&descr, ptr, sizeof(descr));
                *descriptorFlags = descr.Flags;
                *hasDescriptor = true;
            }
            ptr = partEnd;
            continue;
        }

        while (ptr < partEnd) {
            const auto cmd = static_cast<EXdcCommand>(*ptr++);
            switch (cmd) {
                case EXdcCommand::DECLARE_SECTION:
                case EXdcCommand::DECLARE_SECTION_INLINE:
                case EXdcCommand::DECLARE_SECTION_RDMA:
                    SkipDeclareSection(&ptr, partEnd);
                    break;

                case EXdcCommand::PUSH_DATA: {
                    constexpr size_t cmdLen = sizeof(ui16) + sizeof(ui32);
                    UNIT_ASSERT_C(static_cast<size_t>(partEnd - ptr) >= cmdLen, "invalid PUSH_DATA command");

                    const ui16 size = ReadUnaligned<ui16>(ptr);
                    ptr += sizeof(ui16);
                    const ui32 checksum = ReadUnaligned<ui32>(ptr);
                    ptr += sizeof(ui32);

                    result.push_back(TXdcPushData{
                        .Command = cmd,
                        .Size = size,
                        .Checksum = checksum,
                    });
                    break;
                }

                case EXdcCommand::PUSH_DATA_NO_CHECKSUMS: {
                    UNIT_ASSERT_C(static_cast<size_t>(partEnd - ptr) >= sizeof(ui16),
                        "invalid PUSH_DATA_NO_CHECKSUMS command");

                    const ui16 size = ReadUnaligned<ui16>(ptr);
                    ptr += sizeof(ui16);

                    result.push_back(TXdcPushData{
                        .Command = cmd,
                        .Size = size,
                        .Checksum = std::nullopt,
                    });
                    break;
                }

                case EXdcCommand::RDMA_READ:
                case EXdcCommand::RDMA_READ_NO_CHECKSUMS: {
                    UNIT_ASSERT_C(static_cast<size_t>(partEnd - ptr) >= sizeof(ui16), "invalid RDMA_READ command");
                    const ui16 credsSerializedSize = ReadUnaligned<ui16>(ptr);
                    ptr += sizeof(ui16);
                    UNIT_ASSERT_C(static_cast<size_t>(partEnd - ptr) >= credsSerializedSize + sizeof(ui32),
                        "invalid RDMA_READ payload");
                    ptr += credsSerializedSize + sizeof(ui32);
                    break;
                }

                default:
                    UNIT_FAIL("unknown XDC command");
            }
        }
    }

    return result;
}

TSerializedEvent SerializeEvent(bool useXxhash, bool allowDisablingPayloadChecksums, ui32 eventFlags) {
    auto common = MakeIntrusive<TInterconnectProxyCommon>();
    common->MonCounters = MakeIntrusive<NMonitoring::TDynamicCounters>();

    std::shared_ptr<IInterconnectMetrics> metrics = CreateInterconnectCounters(common);
    metrics->SetPeerInfo("peer", "1", "peer");

    auto releaseCallback = [](THolder<IEventBase>) {};
    TEventHolderPool pool(common, releaseCallback);

    TSessionParams params;
    params.UseExternalDataChannel = true;
    params.UseXxhash = useXxhash;
    params.AllowDisablingPayloadChecksums = allowDisablingPayloadChecksums;

    TEventOutputChannel channel(1, 1, 64 << 20, metrics, params, nullptr);

    auto* ev = new TEvTestSerialization;
    ev->Record.SetBlobID(1);
    ev->Record.SetBuffer("metadata");
    ev->AddPayload(TRope(TString(5000, 'x')));
    UNIT_ASSERT(ev->AllowExternalDataChannel());

    auto evHandle = MakeHolder<IEventHandle>(
        TActorId(),
        TActorId(),
        ev,
        eventFlags);

    channel.Push(*evHandle, pool, TInstant::Zero());

    NInterconnect::TOutgoingStream mainStream;
    NInterconnect::TOutgoingStream xdcStream;
    TTcpPacketOutTask task(params, mainStream, xdcStream);

    UNIT_ASSERT(channel.FeedBuf(task, 1));
    task.Finish(1, 0);

    TSerializedEvent result;
    result.MainStreamData = ProduceString(mainStream);
    result.XdcPayloadData = ProduceString(xdcStream);
    result.PushData = ExtractPushDataCommands(result.MainStreamData, &result.DescriptorFlags, &result.HasDescriptor);
    return result;
}

ui32 CalculateExpectedChecksum(const TString& payload, bool useXxhash) {
    if (useXxhash) {
        return XXH3_64bits(payload.data(), payload.size());
    }
    return Crc32cExtendMSanCompatible(0, payload.data(), payload.size());
}

ui32 CalculateExpectedMainChannelChecksum(TString packet, bool useXxhash) {
    UNIT_ASSERT_C(packet.size() >= sizeof(TTcpPacketHeader_v2), "packet is too short");

    TTcpPacketHeader_v2 header;
    memcpy(&header, packet.data(), sizeof(header));
    header.Checksum = 0;
    memcpy(&packet[0], &header, sizeof(header));

    return CalculateExpectedChecksum(packet, useXxhash);
}

void AssertMainChannelChecksum(const TSerializedEvent& event, bool useXxhash) {
    UNIT_ASSERT_C(event.MainStreamData.size() >= sizeof(TTcpPacketHeader_v2), "packet is too short");

    TTcpPacketHeader_v2 header;
    memcpy(&header, event.MainStreamData.data(), sizeof(header));

    UNIT_ASSERT_VALUES_EQUAL(header.PayloadLength, event.MainStreamData.size() - sizeof(TTcpPacketHeader_v2));
    UNIT_ASSERT_VALUES_UNEQUAL(header.Checksum, 0u);
    UNIT_ASSERT_VALUES_EQUAL(header.Checksum, CalculateExpectedMainChannelChecksum(event.MainStreamData, useXxhash));
}

void AssertSinglePushData(const TSerializedEvent& event, ui32 expectedFlags, bool useXxhash) {
    AssertMainChannelChecksum(event, useXxhash);
    UNIT_ASSERT(event.HasDescriptor);
    UNIT_ASSERT_VALUES_EQUAL(event.PushData.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(event.PushData.front().Size, event.XdcPayloadData.size());
    UNIT_ASSERT_VALUES_EQUAL(event.DescriptorFlags & IEventHandle::FlagDisablePayloadChecksums, expectedFlags);
}

void AssertDisabledPayloadChecksums(bool useXxhash) {
    const TSerializedEvent event = SerializeEvent(
        useXxhash,
        true /*allow disabling checksums*/,
        IEventHandle::FlagDisablePayloadChecksums);

    AssertSinglePushData(event, IEventHandle::FlagDisablePayloadChecksums, useXxhash);

    const TXdcPushData& pushData = event.PushData.front();
    UNIT_ASSERT(pushData.Command == EXdcCommand::PUSH_DATA_NO_CHECKSUMS);
    UNIT_ASSERT(!pushData.Checksum);
}

void AssertChecksumsWhenEventFlagIsNotSet(bool useXxhash) {
    const TSerializedEvent event = SerializeEvent(
        useXxhash,
        true /*allow disabling checksums*/,
        0);

    AssertSinglePushData(event, 0, useXxhash);

    const TXdcPushData& pushData = event.PushData.front();
    UNIT_ASSERT(pushData.Command == EXdcCommand::PUSH_DATA);
    UNIT_ASSERT(pushData.Checksum);
    UNIT_ASSERT_VALUES_EQUAL(*pushData.Checksum, CalculateExpectedChecksum(event.XdcPayloadData, useXxhash));
}

void AssertChecksumsWhenDisablingIsNotNegotiated(bool useXxhash) {
    const TSerializedEvent event = SerializeEvent(
        useXxhash,
        false /*allow disabling checksums*/,
        IEventHandle::FlagDisablePayloadChecksums);

    AssertSinglePushData(event, IEventHandle::FlagDisablePayloadChecksums, useXxhash);

    const TXdcPushData& pushData = event.PushData.front();
    UNIT_ASSERT(pushData.Command == EXdcCommand::PUSH_DATA);
    UNIT_ASSERT(pushData.Checksum);
    UNIT_ASSERT_VALUES_EQUAL(*pushData.Checksum, CalculateExpectedChecksum(event.XdcPayloadData, useXxhash));
}

} // namespace

Y_UNIT_TEST_SUITE(EventOutputChannel) {
    Y_UNIT_TEST(DisabledPayloadChecksums) {
        AssertDisabledPayloadChecksums(false);
        AssertDisabledPayloadChecksums(true);
    }

    Y_UNIT_TEST(EnabledPayloadChecksums) {
        AssertChecksumsWhenEventFlagIsNotSet(false);
        AssertChecksumsWhenEventFlagIsNotSet(true);
    }

    Y_UNIT_TEST(DisablingChecksumsNotNegotiated) {
        AssertChecksumsWhenDisablingIsNotNegotiated(false);
        AssertChecksumsWhenDisablingIsNotNegotiated(true);
    }
}
