#include "rdma_xdc_test_common.h"

#include <algorithm>
#include <cstring>

namespace {

    class TExhaustRdmaMemPoolActor : public TActorBootstrapped<TExhaustRdmaMemPoolActor> {
    public:
        TExhaustRdmaMemPoolActor(std::shared_ptr<NInterconnect::NRdma::IMemPool> pool,
                NThreading::TPromise<size_t> exhausted, NThreading::TPromise<void> released)
            : Pool(std::move(pool))
            , Exhausted(std::move(exhausted))
            , Released(std::move(released))
        {}

        void Bootstrap() {
            while (auto buffer = Pool->AllocRcBuf(5000, NInterconnect::NRdma::IMemPool::EMPTY)) {
                Buffers.emplace_back(std::move(*buffer));
            }
            Exhausted.SetValue(Buffers.size());
            Become(&TThis::StateFunc);
        }

    private:
        void Release() {
            Buffers.clear();
            Released.SetValue();
            PassAway();
        }

        STRICT_STFUNC(StateFunc,
            cFunc(TEvents::TSystem::PoisonPill, Release)
        )

    private:
        const std::shared_ptr<NInterconnect::NRdma::IMemPool> Pool;
        NThreading::TPromise<size_t> Exhausted;
        NThreading::TPromise<void> Released;
        std::vector<TRcBuf> Buffers;
    };

}

TEST_F(XdcRdmaTest, SerializeToRope) {
    auto common = MakeIntrusive<TInterconnectProxyCommon>();
    common->MonCounters = MakeIntrusive<NMonitoring::TDynamicCounters>();
    std::shared_ptr<IInterconnectMetrics> ctr = CreateInterconnectCounters(common);
    ctr->SetPeerInfo("peer", "1", "peer");
    auto callback = [](THolder<IEventBase>) {};
    TEventHolderPool pool(common, callback);
    TSessionParams p;
    p.UseExternalDataChannel = true;
    TEventOutputChannel channel(1, 1, 64 << 20, ctr, p, nullptr);

    auto ev = MakeTestEvent(123);
    auto evHandle = MakeHolder<IEventHandle>(TActorId(), TActorId(), ev);

    TInstant t = TInstant::Zero();

    channel.Push(*evHandle, pool, t);

    NInterconnect::TOutgoingStream main, xdc;
    TTcpPacketOutTask task(p, main, xdc);

    ASSERT_TRUE(channel.FeedBuf(task, 0));

    TVector<TConstIoVec> mainData, xdcData;
    main.ProduceIoVec(mainData, 100, 10000);
    xdc.ProduceIoVec(xdcData, 100, 10000);

    ui32 totalXdcSize = 0;
    for (const auto& [_, len] : xdcData) {
        totalXdcSize += len;
    }

    auto mempool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, {});
    TRdmaAllocatorWithFallback allocator(mempool);
    auto serializedRope = ev->SerializeToRope(&allocator);

    ASSERT_TRUE(serializedRope.has_value());
    auto rope = serializedRope->ConvertToString();
    // 6 1 -120 39 88x5000 8 123 18 11 104 101 108 108 111 32 119 111 114 108 100

    UNIT_ASSERT_VALUES_EQUAL(totalXdcSize, rope.size());
    ui32 index = 0;
    for (const auto& [ptr, len] : xdcData) {
        for (size_t i = 0; i < len; ++i) {
            TStringStream msg;
            msg << "Index: " << index << " " << (i32)(((char*)ptr)[i]) << " != " << (i32)rope[index];
            UNIT_ASSERT_EQUAL_C((i32)(((char*)ptr)[i]), (i32)rope[index], msg.Data());
            ++index;
        }
    }

    auto serializationInfo = ev->CreateSerializationInfo();
    auto parsedEventHandle = std::make_unique<IEventHandle>(
        TActorId(),
        ev->Type(),
        ~IEventHandle::FlagExtendedFormat,
        TActorId(),
        TActorId(),
        MakeIntrusive<TEventSerializedData>(std::move(*serializedRope), std::move(serializationInfo)),
        0,
        TScopeId(),
        NWilson::TTraceId()
    );
    auto parsedEvent = parsedEventHandle->Get<TEvTestSerialization>();
    UNIT_ASSERT(parsedEvent);
    UNIT_ASSERT_VALUES_EQUAL(parsedEvent->Record.GetBlobID(), 123u);
    UNIT_ASSERT_VALUES_EQUAL(parsedEvent->Record.GetBuffer(), "hello world");
    UNIT_ASSERT_VALUES_EQUAL(parsedEvent->GetPayload().size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(parsedEvent->GetPayload()[0].GetSize(), 5000u);
    UNIT_ASSERT_VALUES_EQUAL(parsedEvent->GetPayload()[0].ConvertToString(), TString(5000, 'X'));
}

namespace {
    struct TXdcCommandCounters {
        ui32 DeclareSection = 0;
        ui32 DeclareSectionInline = 0;
        ui32 DeclareSectionRdma = 0;
        ui32 PushData = 0;
        ui32 RdmaReadWithChecksums = 0;
        ui32 RdmaReadWithoutChecksums = 0;
        ui32 RdmaRead() const { return RdmaReadWithChecksums + RdmaReadWithoutChecksums; }
    };

    static TString CollectStreamData(NInterconnect::TOutgoingStream& stream) {
        TVector<TConstIoVec> data;
        stream.ProduceIoVec(data, 256, Max<size_t>());
        TString packet;
        for (const auto& [ptr, len] : data) {
            packet.append(static_cast<const char*>(ptr), len);
        }
        return packet;
    }

    static void AccumulateXdcCommandCounters(const TString& packet, TXdcCommandCounters& counters) {
        UNIT_ASSERT_C(packet.size() >= sizeof(TTcpPacketHeader_v2), "packet too short");

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
                ptr = partEnd;
                continue;
            }

            while (ptr < partEnd) {
                const EXdcCommand cmd = static_cast<EXdcCommand>(*ptr++);
                switch (cmd) {
                    case EXdcCommand::DECLARE_SECTION:
                    case EXdcCommand::DECLARE_SECTION_INLINE:
                    case EXdcCommand::DECLARE_SECTION_RDMA: {
                        for (ui32 i = 0; i < 4; ++i) {
                            const ui64 value = NInterconnect::NDetail::DeserializeNumber(&ptr, partEnd);
                            UNIT_ASSERT_VALUES_UNEQUAL(value, Max<ui64>());
                        }
                        if (cmd == EXdcCommand::DECLARE_SECTION) {
                            ++counters.DeclareSection;
                        } else if (cmd == EXdcCommand::DECLARE_SECTION_INLINE) {
                            ++counters.DeclareSectionInline;
                        } else {
                            ++counters.DeclareSectionRdma;
                        }
                        break;
                    }
                    case EXdcCommand::PUSH_DATA: 
                    case EXdcCommand::PUSH_DATA_NO_CHECKSUMS: {
                        const size_t cmdLen = sizeof(ui16) + (cmd == EXdcCommand::PUSH_DATA ? sizeof(ui32) : 0);
                        UNIT_ASSERT_C(static_cast<size_t>(partEnd - ptr) >= cmdLen, "invalid PUSH_DATA");
                        ptr += cmdLen;
                        ++counters.PushData;
                        break;
                    }
                    case EXdcCommand::RDMA_READ: 
                    case EXdcCommand::RDMA_READ_NO_CHECKSUMS: {
                        UNIT_ASSERT_C(static_cast<size_t>(partEnd - ptr) >= sizeof(ui16), "invalid RDMA_READ");
                        const ui16 credsSerializedSize = ReadUnaligned<ui16>(ptr);
                        ptr += sizeof(ui16);
                        UNIT_ASSERT_C(static_cast<size_t>(partEnd - ptr) >= credsSerializedSize + sizeof(ui32),
                            "invalid RDMA_READ payload");
                        ptr += credsSerializedSize;
                        const ui32 checksum = ReadUnaligned<ui32>(ptr);
                        ptr += sizeof(ui32);
                        if (cmd == EXdcCommand::RDMA_READ) {
                            ++counters.RdmaReadWithChecksums;
                            UNIT_ASSERT_C(checksum, "expected checksum");
                        } else {
                            ++counters.RdmaReadWithoutChecksums;
                            UNIT_ASSERT_C(!checksum, "unexpected checksum");
                        }
                        break;
                    }
                }
            }
        }
    }
}

TEST_F(XdcRdmaTest, ShuffleRdmaUsesIteratorOffsetInsideChunk) {
    auto common = MakeIntrusive<TInterconnectProxyCommon>();
    common->MonCounters = MakeIntrusive<NMonitoring::TDynamicCounters>();
    std::shared_ptr<IInterconnectMetrics> ctr = CreateInterconnectCounters(common);
    ctr->SetPeerInfo("peer", "1", "peer");
    auto callback = [](THolder<IEventBase>) {};
    TEventHolderPool pool(common, callback);

    const auto memPool = NInterconnect::NRdma::CreateDummyMemPool();

    TSessionParams p;
    p.UseExternalDataChannel = true;
    p.UseXdcShuffle = true;
    p.UseRdmaRead = true;
    TEventOutputChannel channel(1, 1, 64 << 20, ctr, p, memPool);

    constexpr size_t prefixSize = 17;
    constexpr size_t rdmaSize = 100;
    constexpr size_t totalSize = prefixSize + rdmaSize;

    auto rcBuf = memPool->AllocRcBuf(totalSize, 0).value();
    for (size_t i = 0; i < totalSize; ++i) {
        rcBuf.GetDataMut()[i] = static_cast<char>(i);
    }

    const auto memReg = NInterconnect::NRdma::TryExtractFromRcBuf(rcBuf);
    UNIT_ASSERT(!memReg.Empty());
    const ui64 expectedAddr = reinterpret_cast<ui64>(memReg.GetAddr()) + prefixSize;

    TEventSerializationInfo info;
    info.Sections.push_back(TEventSectionInfo{0, prefixSize, 0, 0, false, false});
    info.Sections.push_back(TEventSectionInfo{0, rdmaSize, 0, 0, false, true});
    auto serialized = MakeIntrusive<TEventSerializedData>(TRope(std::move(rcBuf)), std::move(info));

    auto evHandle = MakeHolder<IEventHandle>(
        TEvTestSerialization::EventType,
        0,
        TActorId(),
        TActorId(),
        serialized,
        0
    );

    channel.Push(*evHandle, pool, TInstant::Zero());

    NInterconnect::TOutgoingStream main;
    NInterconnect::TOutgoingStream xdc;
    TTcpPacketOutTask task(p, main, xdc);
    UNIT_ASSERT(channel.FeedBuf(task, 0, 0));
    task.Finish(1, 0);

    TVector<TConstIoVec> mainData;
    main.ProduceIoVec(mainData, 100, 10000);

    TString packet;
    for (const auto& [ptr, len] : mainData) {
        packet.append(static_cast<const char*>(ptr), len);
    }
    UNIT_ASSERT(packet.size() >= sizeof(TTcpPacketHeader_v2));

    bool foundRdmaRead = false;
    ui64 rdmaReadAddr = 0;
    ui64 rdmaReadSize = 0;

    const char* ptr = packet.data() + sizeof(TTcpPacketHeader_v2);
    const char* end = packet.data() + packet.size();
    while (ptr < end) {
        UNIT_ASSERT_C(static_cast<size_t>(end - ptr) >= sizeof(TChannelPart), "missing channel part header");
        TChannelPart part;
        memcpy(&part, ptr, sizeof(part));
        ptr += sizeof(part);
        UNIT_ASSERT_C(static_cast<size_t>(end - ptr) >= part.Size, "invalid channel part payload size");

        const char* partEnd = ptr + part.Size;
        if (part.IsXdc()) {
            while (ptr < partEnd) {
                const EXdcCommand cmd = static_cast<EXdcCommand>(*ptr++);
                switch (cmd) {
                    case EXdcCommand::DECLARE_SECTION:
                    case EXdcCommand::DECLARE_SECTION_INLINE:
                    case EXdcCommand::DECLARE_SECTION_RDMA: {
                        for (ui32 i = 0; i < 4; ++i) {
                            const ui64 value = NInterconnect::NDetail::DeserializeNumber(&ptr, partEnd);
                            UNIT_ASSERT_VALUES_UNEQUAL(value, Max<ui64>());
                        }
                        break;
                    }
                    case EXdcCommand::PUSH_DATA:
                    case EXdcCommand::PUSH_DATA_NO_CHECKSUMS: {
                        constexpr size_t cmdLen = sizeof(ui16) + sizeof(ui32);
                        UNIT_ASSERT_C(static_cast<size_t>(partEnd - ptr) >= cmdLen, "invalid PUSH_DATA");
                        ptr += cmdLen;
                        break;
                    }
                    case EXdcCommand::RDMA_READ: 
                    case EXdcCommand::RDMA_READ_NO_CHECKSUMS: {
                        UNIT_ASSERT_C(static_cast<size_t>(partEnd - ptr) >= sizeof(ui16), "invalid RDMA_READ");
                        const ui16 credsSerializedSize = ReadUnaligned<ui16>(ptr);
                        ptr += sizeof(ui16);
                        UNIT_ASSERT_C(static_cast<size_t>(partEnd - ptr) >= credsSerializedSize + sizeof(ui32),
                            "invalid RDMA_READ payload");
                        NActorsInterconnect::TRdmaCreds creds;
                        UNIT_ASSERT(creds.ParseFromArray(ptr, credsSerializedSize));
                        ptr += credsSerializedSize;
                        ptr += sizeof(ui32); // checksum
                        UNIT_ASSERT_VALUES_EQUAL(creds.CredsSize(), 1u);
                        rdmaReadAddr = creds.GetCreds(0).GetAddress();
                        rdmaReadSize = creds.GetCreds(0).GetSize();
                        foundRdmaRead = true;
                        break;
                    }
                }
            }
        } else {
            ptr = partEnd;
        }
    }

    UNIT_ASSERT(foundRdmaRead);
    UNIT_ASSERT_VALUES_EQUAL(rdmaReadSize, rdmaSize);
    UNIT_ASSERT_VALUES_EQUAL(rdmaReadAddr, expectedAddr);
}

struct TRdmaPayloadChecksumTestParams {
    bool AllowDisablingPayloadChecksums = false;
    bool DisablePayloadChecksumsFlag = false;
};

class XdcRdmaPayloadChecksumTest
    : public XdcRdmaTest
    , public ::testing::WithParamInterface<TRdmaPayloadChecksumTestParams>
{};

TEST_P(XdcRdmaPayloadChecksumTest, RdmaPayloadChecksums) {
    const TRdmaPayloadChecksumTestParams params = GetParam();
    const bool disablePayloadChecksums = params.AllowDisablingPayloadChecksums && params.DisablePayloadChecksumsFlag;

    auto common = MakeIntrusive<TInterconnectProxyCommon>();
    common->MonCounters = MakeIntrusive<NMonitoring::TDynamicCounters>();

    std::shared_ptr<IInterconnectMetrics> ctr = CreateInterconnectCounters(common);
    ctr->SetPeerInfo("peer", "1", "peer");

    auto callback = [](THolder<IEventBase>) {};
    TEventHolderPool pool(common, callback);

    const auto memPool = NInterconnect::NRdma::CreateDummyMemPool();

    TSessionParams p;
    p.UseExternalDataChannel = true;
    p.UseXdcShuffle = true;
    p.UseRdmaRead = true;
    p.ChecksumRdmaEvent = true;
    p.AllowDisablingPayloadChecksums = params.AllowDisablingPayloadChecksums;
    TEventOutputChannel channel(1, 1, 64 << 20, ctr, p, memPool);

    constexpr size_t payloadSize = 257;
    auto rcBuf = memPool->AllocRcBuf(payloadSize, 0).value();
    for (size_t i = 0; i < payloadSize; ++i) {
        rcBuf.GetDataMut()[i] = static_cast<char>(i);
    }
    const ui32 checksumIfCalculated = XXH3_64bits(rcBuf.GetData(), payloadSize);
    UNIT_ASSERT_VALUES_UNEQUAL(checksumIfCalculated, 0u);

    TEventSerializationInfo info;
    info.Sections.push_back(TEventSectionInfo{0, payloadSize, 0, 0, false, true /*IsRdmaCapable*/});
    auto serialized = MakeIntrusive<TEventSerializedData>(TRope(std::move(rcBuf)), std::move(info));

    auto evHandle = MakeHolder<IEventHandle>(
        TEvTestSerialization::EventType,
        params.DisablePayloadChecksumsFlag ? IEventHandle::FlagDisablePayloadChecksums : 0,
        TActorId(),
        TActorId(),
        serialized,
        0
    );
    channel.Push(*evHandle, pool, TInstant::Zero());

    NInterconnect::TOutgoingStream main;
    NInterconnect::TOutgoingStream xdc;
    TTcpPacketOutTask task(p, main, xdc);
    UNIT_ASSERT(channel.FeedBuf(task, 1, 0));
    task.Finish(1, 0);

    TXdcCommandCounters counters;
    AccumulateXdcCommandCounters(CollectStreamData(main), counters);

    UNIT_ASSERT_VALUES_EQUAL(counters.DeclareSectionRdma, 1u);
    UNIT_ASSERT_VALUES_EQUAL(counters.RdmaRead(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(counters.RdmaReadWithChecksums, disablePayloadChecksums ? 0u : 1u);
    UNIT_ASSERT_VALUES_EQUAL(counters.RdmaReadWithoutChecksums, disablePayloadChecksums ? 1u : 0u);
}

INSTANTIATE_TEST_SUITE_P(
    DisablePayloadChecksums,
    XdcRdmaPayloadChecksumTest,
    ::testing::Values(
        TRdmaPayloadChecksumTestParams{false, false},
        TRdmaPayloadChecksumTestParams{false, true},
        TRdmaPayloadChecksumTestParams{true, false},
        TRdmaPayloadChecksumTestParams{true, true}
    ),
    [](const testing::TestParamInfo<TRdmaPayloadChecksumTestParams>& info) {
        return std::string(info.param.AllowDisablingPayloadChecksums ? "AllowDisabling" : "DisablingNotAllowed")
            + "_" + (info.param.DisablePayloadChecksumsFlag ? "FlagSet" : "FlagNotSet");
    }
);

TEST_F(XdcRdmaTest, ShuffleRdmaFallsBackToPushDataWhenDeviceIndexIsInvalid) {
    auto common = MakeIntrusive<TInterconnectProxyCommon>();
    common->MonCounters = MakeIntrusive<NMonitoring::TDynamicCounters>();
    std::shared_ptr<IInterconnectMetrics> ctr = CreateInterconnectCounters(common);
    ctr->SetPeerInfo("peer", "1", "peer");
    auto callback = [](THolder<IEventBase>) {};
    TEventHolderPool pool(common, callback);

    const auto memPool = NInterconnect::NRdma::CreateDummyMemPool();
    TSessionParams p;
    p.UseExternalDataChannel = true;
    p.UseXdcShuffle = true;
    p.UseRdmaRead = true;
    TEventOutputChannel channel(1, 1, 64 << 20, ctr, p, memPool);

    constexpr size_t payloadSize = 128;
    auto rcBuf = memPool->AllocRcBuf(payloadSize, 0).value();
    memset(rcBuf.GetDataMut(), 0xAB, payloadSize);

    TEventSerializationInfo info;
    info.Sections.push_back(TEventSectionInfo{0, payloadSize, 0, 0, false, true});
    auto serialized = MakeIntrusive<TEventSerializedData>(TRope(std::move(rcBuf)), std::move(info));

    auto evHandle = MakeHolder<IEventHandle>(
        TEvTestSerialization::EventType,
        0,
        TActorId(),
        TActorId(),
        serialized,
        0
    );
    channel.Push(*evHandle, pool, TInstant::Zero());

    NInterconnect::TOutgoingStream main;
    NInterconnect::TOutgoingStream xdc;
    TTcpPacketOutTask task(p, main, xdc);
    UNIT_ASSERT(channel.FeedBuf(task, 1, -1));
    task.Finish(1, 0);

    TXdcCommandCounters counters;
    AccumulateXdcCommandCounters(CollectStreamData(main), counters);

    UNIT_ASSERT(counters.DeclareSection > 0u);
    UNIT_ASSERT_VALUES_EQUAL(counters.DeclareSectionRdma, 0u);
    UNIT_ASSERT(counters.PushData > 0u);
    UNIT_ASSERT_VALUES_EQUAL(counters.RdmaRead(), 0u);
}

TEST_F(XdcRdmaTest, ShuffleRdmaFallsBackToPushDataWhenSerializeToRopeFails) {
    auto common = MakeIntrusive<TInterconnectProxyCommon>();
    common->MonCounters = MakeIntrusive<NMonitoring::TDynamicCounters>();
    std::shared_ptr<IInterconnectMetrics> ctr = CreateInterconnectCounters(common);
    ctr->SetPeerInfo("peer", "1", "peer");
    auto callback = [](THolder<IEventBase>) {};
    TEventHolderPool pool(common, callback);

    const auto memPool = NInterconnect::NRdma::CreateDummyMemPool();
    TSessionParams p;
    p.UseExternalDataChannel = true;
    p.UseXdcShuffle = true;
    p.UseRdmaRead = true;
    TEventOutputChannel channel(1, 1, 64 << 20, ctr, p, memPool);

    auto* ev = new TEvSerializeToRopeFailure();
    auto evHandle = MakeHolder<IEventHandle>(TActorId(), TActorId(), ev);
    channel.Push(*evHandle, pool, TInstant::Zero());

    TXdcCommandCounters counters;
    bool done = false;
    for (ui64 serial = 1; serial <= 4 && !done; ++serial) {
        NInterconnect::TOutgoingStream main;
        NInterconnect::TOutgoingStream xdc;
        TTcpPacketOutTask task(p, main, xdc);
        done = channel.FeedBuf(task, serial, 0);
        task.Finish(serial, 0);
        AccumulateXdcCommandCounters(CollectStreamData(main), counters);
    }

    UNIT_ASSERT(done);
    UNIT_ASSERT_C(ev->SerializeToRopeCallCount > 0u, "SerializeToRope should be attempted for RDMA-capable sections");
    UNIT_ASSERT(counters.DeclareSection > 0u);
    UNIT_ASSERT_VALUES_EQUAL(counters.DeclareSectionRdma, 0u);
    UNIT_ASSERT(counters.PushData > 0u);
    UNIT_ASSERT_VALUES_EQUAL(counters.RdmaRead(), 0u);
}

#ifndef NDEBUG
TEST_F(XdcRdmaTest, ShuffleRdmaFallsBackToPushDataWhenChunkIsNotRdmaRegistered) {
    auto common = MakeIntrusive<TInterconnectProxyCommon>();
    common->MonCounters = MakeIntrusive<NMonitoring::TDynamicCounters>();
    std::shared_ptr<IInterconnectMetrics> ctr = CreateInterconnectCounters(common);
    ctr->SetPeerInfo("peer", "1", "peer");
    auto callback = [](THolder<IEventBase>) {};
    TEventHolderPool pool(common, callback);

    const auto memPool = NInterconnect::NRdma::CreateDummyMemPool();
    TSessionParams p;
    p.UseExternalDataChannel = true;
    p.UseXdcShuffle = true;
    p.UseRdmaRead = true;
    TEventOutputChannel channel(1, 1, 64 << 20, ctr, p, memPool);

    constexpr size_t payloadSize = 128;
    auto nonRdmaBuf = TRcBuf::Uninitialized(payloadSize);
    memset(nonRdmaBuf.GetDataMut(), 0xCD, payloadSize);

    TEventSerializationInfo info;
    info.Sections.push_back(TEventSectionInfo{0, payloadSize, 0, 0, false, true});
    auto serialized = MakeIntrusive<TEventSerializedData>(TRope(std::move(nonRdmaBuf)), std::move(info));

    auto evHandle = MakeHolder<IEventHandle>(
        TEvTestSerialization::EventType,
        0,
        TActorId(),
        TActorId(),
        serialized,
        0
    );
    channel.Push(*evHandle, pool, TInstant::Zero());

    TXdcCommandCounters counters;
    bool done = false;
    for (ui64 serial = 1; serial <= 4 && !done; ++serial) {
        NInterconnect::TOutgoingStream main;
        NInterconnect::TOutgoingStream xdc;
        TTcpPacketOutTask task(p, main, xdc);
        done = channel.FeedBuf(task, serial, 0);
        task.Finish(serial, 0);
        AccumulateXdcCommandCounters(CollectStreamData(main), counters);
    }

    UNIT_ASSERT(done);
    UNIT_ASSERT(counters.DeclareSection > 0u);
    UNIT_ASSERT_VALUES_EQUAL(counters.DeclareSectionRdma, 0u);
    UNIT_ASSERT(counters.PushData > 0u);
    UNIT_ASSERT_VALUES_EQUAL(counters.RdmaRead(), 0u);
}

TEST_F(XdcRdmaTest, ShuffleRdmaFallsBackToPushDataWhenRdmaPartContainsMixedChunks) {
    auto common = MakeIntrusive<TInterconnectProxyCommon>();
    common->MonCounters = MakeIntrusive<NMonitoring::TDynamicCounters>();
    std::shared_ptr<IInterconnectMetrics> ctr = CreateInterconnectCounters(common);
    ctr->SetPeerInfo("peer", "1", "peer");
    auto callback = [](THolder<IEventBase>) {};
    TEventHolderPool pool(common, callback);

    const auto memPool = NInterconnect::NRdma::CreateDummyMemPool();
    TSessionParams p;
    p.UseExternalDataChannel = true;
    p.UseXdcShuffle = true;
    p.UseRdmaRead = true;
    TEventOutputChannel channel(1, 1, 64 << 20, ctr, p, memPool);

    constexpr size_t rdmaChunkSize = 64;
    constexpr size_t nonRdmaChunkSize = 48;
    constexpr size_t payloadSize = rdmaChunkSize + nonRdmaChunkSize;

    auto rdmaBuf = memPool->AllocRcBuf(rdmaChunkSize, 0).value();
    memset(rdmaBuf.GetDataMut(), 0xE1, rdmaChunkSize);
    auto nonRdmaBuf = TRcBuf::Uninitialized(nonRdmaChunkSize);
    memset(nonRdmaBuf.GetDataMut(), 0xE2, nonRdmaChunkSize);

    TRope payload(std::move(rdmaBuf));
    payload.Insert(payload.End(), TRope(std::move(nonRdmaBuf)));

    TEventSerializationInfo info;
    info.Sections.push_back(TEventSectionInfo{0, payloadSize, 0, 0, false, true});
    auto serialized = MakeIntrusive<TEventSerializedData>(std::move(payload), std::move(info));

    auto evHandle = MakeHolder<IEventHandle>(
        TEvTestSerialization::EventType,
        0,
        TActorId(),
        TActorId(),
        serialized,
        0
    );
    channel.Push(*evHandle, pool, TInstant::Zero());

    TXdcCommandCounters counters;
    bool done = false;
    for (ui64 serial = 1; serial <= 4 && !done; ++serial) {
        NInterconnect::TOutgoingStream main;
        NInterconnect::TOutgoingStream xdc;
        TTcpPacketOutTask task(p, main, xdc);
        done = channel.FeedBuf(task, serial, 0);
        task.Finish(serial, 0);
        AccumulateXdcCommandCounters(CollectStreamData(main), counters);
    }

    UNIT_ASSERT(done);
    UNIT_ASSERT(counters.DeclareSection > 0u);
    UNIT_ASSERT_VALUES_EQUAL(counters.DeclareSectionRdma, 0u);
    UNIT_ASSERT(counters.PushData > 0u);
    UNIT_ASSERT_VALUES_EQUAL(counters.RdmaRead(), 0u);
}
#endif

TEST_P(XdcRdmaTransportTest, SendRdma) {
    const auto params = GetParam();
    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, GetRdmaCqModeFlags(params),
        TTestICCluster::TCheckerFactory(), TDuration::Seconds(2), TNode::DefaultInflight(), GetRdmaSettingsCustomizer(params));
    auto memPool = NInterconnect::NRdma::CreateDummyMemPool();
    std::unique_ptr<IEventBase> ev(MakeTestEvent(123, memPool.get()));

    auto receiverPtr = new TReceiveActor([](TEvTestSerialization::TPtr ev) {
        Cerr << "Blob ID: " << ev->Get()->Record.GetBlobID() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBlobID(), 123u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBuffer(), "hello world");
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload().size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].GetSize(), 5000u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].ConvertToString(), TString(5000, 'X'));
    });
    const TActorId receiver = cluster.RegisterActor(receiverPtr, 1);

    Sleep(TDuration::MilliSeconds(1000));

    auto senderPtr = new TSendActor(receiver, std::move(ev));
    cluster.RegisterActor(senderPtr, 2);
    UNIT_ASSERT(receiverPtr->WaitForReceive(1, 20));
}

TEST_P(XdcRdmaTransportTest, SendRdmaEmptyProtoRecordWithPayload) {
    const auto params = GetParam();
    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, GetRdmaCqModeFlags(params),
        TTestICCluster::TCheckerFactory(), TDuration::Seconds(2), TNode::DefaultInflight(), GetRdmaSettingsCustomizer(params));
    auto memPool = NInterconnect::NRdma::CreateDummyMemPool();

    auto buf = memPool->AllocRcBuf(5000, 0).value();
    std::fill(buf.GetDataMut(), buf.GetDataMut() + 5000, 'X');

    std::unique_ptr<TEvTestSerialization> ev = std::make_unique<TEvTestSerialization>();
    ev->AddPayload(TRope(std::move(buf)));
    UNIT_ASSERT_VALUES_EQUAL(ev->Record.ByteSize(), 0);
    UNIT_ASSERT(ev->AllowExternalDataChannel());

    auto receiverPtr = new TReceiveActor([](TEvTestSerialization::TPtr ev) {
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.ByteSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload().size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].GetSize(), 5000u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].ConvertToString(), TString(5000, 'X'));
    });
    const TActorId receiver = cluster.RegisterActor(receiverPtr, 1);

    Sleep(TDuration::MilliSeconds(1000));

    auto senderPtr = new TSendActor(receiver, std::move(ev));
    cluster.RegisterActor(senderPtr, 2);
    UNIT_ASSERT(receiverPtr->WaitForReceive(1, 20));

    TString lastRdmaStatus;
    UNIT_ASSERT_C(WaitForRdmaChecksumStatus(cluster, 2, 1, GetExpectedRdmaStatus(params), 20, lastRdmaStatus),
        "last RDMA status: " << FormatLastRdmaStatus(lastRdmaStatus));
}

TEST_P(XdcRdmaTransportTest, SendRdmaWithShuffledPayload) {
    const auto params = GetParam();
    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, GetRdmaCqModeFlags(params),
        TTestICCluster::TCheckerFactory(), TDuration::Seconds(2), TNode::DefaultInflight(), GetRdmaSettingsCustomizer(params));
    auto memPool = NInterconnect::NRdma::CreateDummyMemPool();
    auto ev = std::make_unique<TEvTestSerialization>();
    ev->Record.SetBlobID(123);
    ev->Record.SetBuffer("hello world");
    for (ui32 i = 0; i < 10; ++i) {
        if (i % 2 == 0) {
            TRope tmp(TString(5000, 'X'));
            ev->AddPayload(std::move(tmp));
        } else {
            auto buf = memPool->AllocRcBuf(5000, 0).value();
            std::fill(buf.GetDataMut(), buf.GetDataMut() + 5000, 'Y');
            ev->AddPayload(TRope(std::move(buf)));
        }
    }

    auto receiverPtr = new TReceiveActor([](TEvTestSerialization::TPtr ev) {
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBlobID(), 123u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBuffer(), "hello world");
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload().size(), 10u);
        for (ui32 i = 0; i < 10; ++i) {
            if (i % 2 == 0) {
                UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[i].GetSize(), 5000u);
                UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[i].ConvertToString(), TString(5000, 'X'));
            } else {
                UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[i].GetSize(), 5000u);
                UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[i].ConvertToString(), TString(5000, 'Y'));
            }
        }
    });
    const TActorId receiver = cluster.RegisterActor(receiverPtr, 1);

    Sleep(TDuration::MilliSeconds(1000));

    auto senderPtr = new TSendActor(receiver, std::move(ev));
    cluster.RegisterActor(senderPtr, 2);
    UNIT_ASSERT(receiverPtr->WaitForReceive(1, 20));
}

TEST_P(XdcRdmaTransportTest, SendRdmaWithShuffledPayloadLongRope) {
    const auto params = GetParam();
    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, GetRdmaCqModeFlags(params),
        TTestICCluster::TCheckerFactory(), TDuration::Seconds(2), TNode::DefaultInflight(), GetRdmaSettingsCustomizer(params));
    auto memPool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, {});
    auto ev = std::make_unique<TEvTestSerialization>();
    ev->Record.SetBlobID(123);
    ev->Record.SetBuffer("hello world");
    for (ui32 i = 0; i < 20; ++i) {
        if (i % 2 == 0) {
            TRope tmp;
            for (ui32 j = 0; j < 100; j++) {
                tmp.Insert(tmp.End(), TRope(ToString(i * 100 + j)));
            }
            ev->AddPayload(std::move(tmp));
        } else {
            TRope tmp;
            for (ui32 j = 0; j < 100; j++) {
                auto s = ToString(i * 100 + j);
                auto buf = memPool->AllocRcBuf(s.size(), 0).value();
                std::memcpy(buf.GetDataMut(), s.data(), buf.Size());
                tmp.Insert(tmp.End(), TRope(buf));
            }
            ev->AddPayload(TRope(std::move(tmp)));
        }
    }

    auto receiverPtr = new TReceiveActor([](TEvTestSerialization::TPtr ev) {
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBlobID(), 123u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBuffer(), "hello world");
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload().size(), 20u);
        for (ui32 i = 0; i < 20; ++i) {
            TString expected;
            for (ui32 j = 0; j < 100; ++j) {
                expected += ToString(i * 100 + j);
            }
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[i].GetSize(), expected.size());
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[i].ConvertToString(), expected);
        }
    });
    const TActorId receiver = cluster.RegisterActor(receiverPtr, 1);

    Sleep(TDuration::MilliSeconds(1000));

    auto senderPtr = new TSendActor(receiver, std::move(ev));
    cluster.RegisterActor(senderPtr, 2);
    UNIT_ASSERT(receiverPtr->WaitForReceive(1, 20));
}


// Right now the case of mixed (rdma/regular memory) in one rope is transfeting via TCP.
// This test just checks data is stil correct in this case.
TEST_P(XdcRdmaTransportTest, SendRdmaWithShuffledPayloadLongMixedRope) {
    const auto params = GetParam();
    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, GetRdmaCqModeFlags(params),
        TTestICCluster::TCheckerFactory(), TDuration::Seconds(2), TNode::DefaultInflight(), GetRdmaSettingsCustomizer(params));
    auto memPool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, {});
    auto ev = std::make_unique<TEvTestSerialization>();
    ev->Record.SetBlobID(123);
    ev->Record.SetBuffer("hello world");
    TRope tmp;
    for (ui32 i = 0; i < 2000; ++i) {
        if (i % 2 == 0) {
            tmp.Insert(tmp.End(), TRope(ToString(i)));
        } else {
            auto s = ToString(i);
            auto buf = memPool->AllocRcBuf(s.size(), 0).value();
            std::memcpy(buf.GetDataMut(), s.data(), buf.Size());
            tmp.Insert(tmp.End(), TRope(buf));
        }
    }
    ev->AddPayload(TRope(std::move(tmp)));

    auto receiverPtr = new TReceiveActor([](TEvTestSerialization::TPtr ev) {
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBlobID(), 123u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBuffer(), "hello world");
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload().size(), 1u);

        TString expected;
        for (ui32 i = 0; i < 2000; ++i) {
            expected += ToString(i);
        }
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].GetSize(), expected.size());
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].ConvertToString(), expected);
    });
    const TActorId receiver = cluster.RegisterActor(receiverPtr, 1);

    Sleep(TDuration::MilliSeconds(1000));

    auto senderPtr = new TSendActor(receiver, std::move(ev));
    cluster.RegisterActor(senderPtr, 2);
    UNIT_ASSERT(receiverPtr->WaitForReceive(1, 20));
}



TEST_P(XdcRdmaTransportTest, SendRdmaWithRegionOffset) {
    const auto params = GetParam();
    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, GetRdmaCqModeFlags(params),
        TTestICCluster::TCheckerFactory(), TDuration::Seconds(2), TNode::DefaultInflight(), GetRdmaSettingsCustomizer(params));
    auto memPool = NInterconnect::NRdma::CreateDummyMemPool();
    std::unique_ptr<IEventBase> ev(MakeTestEvent(123, memPool.get(), false, true));

    auto receiverPtr = new TReceiveActor([](TEvTestSerialization::TPtr ev) {
        Cerr << "Blob ID: " << ev->Get()->Record.GetBlobID() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBlobID(), 123u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBuffer(), "hello world");
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload().size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].GetSize(), 5999u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].ConvertToString(), TString(5000, 'X') + TString(999, 'Z'));
    });
    const TActorId receiver = cluster.RegisterActor(receiverPtr, 1);

    Sleep(TDuration::MilliSeconds(1000));

    auto senderPtr = new TSendActor(receiver, std::move(ev));
    cluster.RegisterActor(senderPtr, 2);
    UNIT_ASSERT(receiverPtr->WaitForReceive(1, 20));
}

TEST_P(XdcRdmaTransportTest, SendRdmaWithGlueWithRegionOffset) {
    const auto params = GetParam();
    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, GetRdmaCqModeFlags(params),
        TTestICCluster::TCheckerFactory(), TDuration::Seconds(2), TNode::DefaultInflight(), GetRdmaSettingsCustomizer(params));
    auto memPool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, {});
    std::unique_ptr<IEventBase> ev(MakeTestEvent(123, memPool.get(), true, true));

    auto receiverPtr = new TReceiveActor([](TEvTestSerialization::TPtr ev) {
        Cerr << "Blob ID: " << ev->Get()->Record.GetBlobID() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBlobID(), 123u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBuffer(), "hello world");
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload().size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].GetSize(), 3499u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[1].GetSize(), 3499u);
        const TString pattern1 = TString(999, 'Z') + TString(2500, 'X');
        const TString pattern2 = TString(2500, 'X') + TString(999, 'Z'); 
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].ConvertToString(), pattern1);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[1].ConvertToString(), pattern2);
    });
    const TActorId receiver = cluster.RegisterActor(receiverPtr, 1);

    Sleep(TDuration::MilliSeconds(1000));

    auto senderPtr = new TSendActor(receiver, std::move(ev));
    cluster.RegisterActor(senderPtr, 2);
    UNIT_ASSERT(receiverPtr->WaitForReceive(1, 20));
}

TEST_P(XdcRdmaTransportTest, SendRdmaWithGlue) {
    const auto params = GetParam();
    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, GetRdmaCqModeFlags(params),
        TTestICCluster::TCheckerFactory(), TDuration::Seconds(2), TNode::DefaultInflight(), GetRdmaSettingsCustomizer(params));
    auto memPool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, {});
    std::unique_ptr<IEventBase> ev(MakeTestEvent(123, memPool.get(), true, false));

    auto receiverPtr = new TReceiveActor([](TEvTestSerialization::TPtr ev) {
        Cerr << "Blob ID: " << ev->Get()->Record.GetBlobID() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBlobID(), 123u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBuffer(), "hello world");
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload().size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].GetSize(), 2500u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[1].GetSize(), 2500u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].ConvertToString(), TString(2500, 'X'));
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[1].ConvertToString(), TString(2500, 'X'));
    });
    const TActorId receiver = cluster.RegisterActor(receiverPtr, 1);

    Sleep(TDuration::MilliSeconds(1000));

    auto senderPtr = new TSendActor(receiver, std::move(ev));
    cluster.RegisterActor(senderPtr, 2);
    UNIT_ASSERT(receiverPtr->WaitForReceive(1, 20));
}

TEST_P(XdcRdmaPoolPressureTest, SendRdmaWithMultiGlue) {
    const auto params = GetParam();
    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, GetRdmaCqModeFlags(params),
        TTestICCluster::TCheckerFactory(), TDuration::Seconds(2), TNode::DefaultInflight(), GetRdmaSettingsCustomizer(params));
    auto memPool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, {});
    std::unique_ptr<IEventBase> ev(MakeMultiGlueTestEvent(123, memPool.get()));

    auto receiverPtr = new TReceiveActor([](TEvTestSerialization::TPtr ev) {
        Cerr << "Blob ID: " << ev->Get()->Record.GetBlobID() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBlobID(), 123u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBuffer(), "hello world");
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload().size(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].GetSize(), 500u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[1].GetSize(), 1500u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[2].GetSize(), 3000u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].ConvertToString(), TString(500, 'X'));
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[1].ConvertToString(), TString(1500, 'Y'));
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[2].ConvertToString(), TString(3000, 'Z'));
    });
    const TActorId receiver = cluster.RegisterActor(receiverPtr, 1);

    Sleep(TDuration::MilliSeconds(1000));

    auto senderPtr = new TSendActor(receiver, std::move(ev));
    cluster.RegisterActor(senderPtr, 2);
    UNIT_ASSERT(receiverPtr->WaitForReceive(1, 20));
}

TEST_P(XdcRdmaPoolPressureTest, RestoreRdmaSession) {
    const auto params = GetParam();
    constexpr TStringBuf RdmaRetryWatchdogPendingSessions = "RdmaRetryWatchdogPendingSessions";

    const NInterconnect::NRdma::TMemPoolSettings settings {
        .SizeLimitMb = 256
    };
    auto pool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, settings);
    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, GetRdmaCqModeFlags(params),
        TTestICCluster::TCheckerFactory(), TDuration::Minutes(9999), TNode::DefaultInflight(),
        GetRdmaSettingsCustomizer(params)); //Disable dead peer detection to parallel activity

    // Create receiver
    ui32 index = 0;
    auto receiverPtr = new TReceiveActor([&index](TEvTestSerialization::TPtr ev) {
        Cerr << "Blob ID: " << ev->Get()->Record.GetBlobID() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBlobID(), index++);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBuffer(), "hello world");
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload().size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].GetSize(), 5000u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].ConvertToString(), TString(5000, 'X'));
    });
    const TActorId receiver = cluster.RegisterActor(receiverPtr, 1);

    Sleep(TDuration::MilliSeconds(1000));

    // Send one packet to establish session
    {
        auto memPool = NInterconnect::NRdma::CreateDummyMemPool();
        std::unique_ptr<IEventBase> ev(MakeTestEvent(0, memPool.get()));
        auto senderPtr = new TSendActor(receiver, std::move(ev));
        cluster.RegisterActor(senderPtr, 2);

        UNIT_ASSERT(receiverPtr->WaitForReceive(1, 20));
    }
    ui64 lastWatchdogPending = 0;
    UNIT_ASSERT_C(WaitForNodeCounterSum(cluster, 2, RdmaRetryWatchdogPendingSessions, 0,
            TDuration::Seconds(5), lastWatchdogPending),
        "last RDMA retry watchdog pending sessions: " << lastWatchdogPending);

    TString lastRdmaStatus;
    UNIT_ASSERT_C(WaitForRdmaChecksumStatus(cluster, 2, 1, GetExpectedRdmaStatus(params), 30, lastRdmaStatus),
        "last RDMA status: " << FormatLastRdmaStatus(lastRdmaStatus));

    // SlotMemPool caches slots per thread. Exhaust the 5 KiB allocation class on node 1's
    // executor thread so the input session cannot satisfy the next RDMA section allocation
    // from its local cache after the global pool has been exhausted.
    auto exhaustedPromise = NThreading::NewPromise<size_t>();
    auto exhaustedFuture = exhaustedPromise.GetFuture();
    auto releasedPromise = NThreading::NewPromise<void>();
    auto releasedFuture = releasedPromise.GetFuture();
    const TActorId poolExhaustionActor = cluster.RegisterActor(new TExhaustRdmaMemPoolActor(
        pool, std::move(exhaustedPromise), std::move(releasedPromise)), 1);
    UNIT_ASSERT(exhaustedFuture.Wait(TDuration::Seconds(30)));
    UNIT_ASSERT(exhaustedFuture.GetValueSync() > 0);

    // Send more
    {
        auto extCtx = std::make_shared<TSendActor::TExtCtx>();
        auto memPool = NInterconnect::NRdma::CreateDummyMemPool();
        std::unique_ptr<IEventBase> ev(MakeTestEvent(1, memPool.get()));
        auto senderPtr = new TSendActor(receiver, std::move(ev), extCtx);
        cluster.RegisterActor(senderPtr, 2);
        // Undelivered because we can't allocate memory on the receiver side
        UNIT_ASSERT(extCtx->WaitForUndelivered(10));
    }

    // The event was not delivered
    UNIT_ASSERT(receiverPtr->WaitForReceive(1, 20));

    // Session is going to be recreated without RDMA,
    // but pending handshake timers are triggered (we can't check it directly in this UT).
    UNIT_ASSERT_C(WaitForRdmaChecksumStatus(cluster, 2, 1, "Off", 30, lastRdmaStatus),
        "last RDMA status: " << FormatLastRdmaStatus(lastRdmaStatus));
    UNIT_ASSERT_STRINGS_EQUAL(lastRdmaStatus.c_str(), "Off");
    lastRdmaStatus.clear();
    UNIT_ASSERT_C(WaitForNodeCounterSum(cluster, 2, RdmaRetryWatchdogPendingSessions, 1,
            TDuration::Seconds(30), lastWatchdogPending),
        "last RDMA retry watchdog pending sessions: " << lastWatchdogPending);

    // Send one more time (will be delivered through TCP)
    {
        auto memPool = NInterconnect::NRdma::CreateDummyMemPool();
        std::unique_ptr<IEventBase> ev(MakeTestEvent(1, memPool.get()));
        auto senderPtr = new TSendActor(receiver, std::move(ev));
        cluster.RegisterActor(senderPtr, 2);
    }
    UNIT_ASSERT(receiverPtr->WaitForReceive(2, 20));
    // Free memory on the same executor thread whose local cache was exhausted.
    cluster.KillActor(1, poolExhaustionActor);
    UNIT_ASSERT(releasedFuture.Wait(TDuration::Seconds(30)));

    // Wait until the delayed RDMA retry closes the TCP-only session, or until RDMA
    // is restored by an already pending reconnect.
    UNIT_ASSERT_C(WaitForRdmaSessionDropOrStatus(cluster, 2, 1, GetExpectedRdmaStatus(params), 45, lastRdmaStatus),
        "last RDMA status before reconnect: " << FormatLastRdmaStatus(lastRdmaStatus));

    {
        auto memPool = NInterconnect::NRdma::CreateDummyMemPool();
        std::unique_ptr<IEventBase> ev(MakeTestEvent(2, memPool.get()));
        auto senderPtr = new TSendActor(receiver, std::move(ev));
        cluster.RegisterActor(senderPtr, 2);
    }
    UNIT_ASSERT(receiverPtr->WaitForReceive(3, 20));
    UNIT_ASSERT_C(WaitForRdmaChecksumStatus(cluster, 2, 1, GetExpectedRdmaStatus(params), 30, lastRdmaStatus),
        "last RDMA status: " << FormatLastRdmaStatus(lastRdmaStatus));
    UNIT_ASSERT_STRINGS_EQUAL(lastRdmaStatus.c_str(), GetExpectedRdmaStatus(params).c_str());
    UNIT_ASSERT_C(WaitForNodeCounterSum(cluster, 2, RdmaRetryWatchdogPendingSessions, 0,
            TDuration::Seconds(10), lastWatchdogPending),
        "last RDMA retry watchdog pending sessions: " << lastWatchdogPending);
}

TEST_P(XdcRdmaTransportTest, SendMix) {
    const auto params = GetParam();
    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, GetRdmaCqModeFlags(params),
        TTestICCluster::TCheckerFactory(), TDuration::Seconds(2), TNode::DefaultInflight(),
        GetRdmaSettingsCustomizer(params));

    ui32 index = 0;
    auto receiverPtr = new TReceiveActor([&index](TEvTestSerialization::TPtr ev) {
        Cerr << "Blob ID: " << ev->Get()->Record.GetBlobID() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBlobID(), index++);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBuffer(), "hello world");
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload().size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].GetSize(), 5000u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].ConvertToString(), TString(5000, 'X'));
    });
    const TActorId receiver = cluster.RegisterActor(receiverPtr, 1);

    Sleep(TDuration::MilliSeconds(1000));

    const ui32 numEvents = 10;
    auto memPool = NInterconnect::NRdma::CreateDummyMemPool();
    for (ui32 i = 0; i < numEvents; ++i) {
        const bool isRdma = i % 2 == 0;
        std::unique_ptr<IEventBase> ev(MakeTestEvent(i, isRdma ? memPool.get() : nullptr));
        auto senderPtr = new TSendActor(receiver, std::move(ev));
        cluster.RegisterActor(senderPtr, 2);
    }

    UNIT_ASSERT(receiverPtr->WaitForReceive(numEvents, 20));
}

TEST_P(XdcRdmaPoolPressureTest, SendMixBig) {
    const auto params = GetParam();
    // Heavy payload validation in this test may starve progress long enough to trip default DeadPeer=2s.
    // Use the same relaxed connectivity envelope as SendMixBigShuffle.
    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, GetRdmaCqModeFlags(params),
        TTestICCluster::TCheckerFactory(), TDuration::Minutes(1), 50u << 20, GetRdmaSettingsCustomizer(params));
    std::mutex mtx;
    mtx.lock();
    TEventsForTest events(500);
    mtx.unlock();

    auto receiverPtr = new TReceiveActor([&events, &mtx](TEvTestSerialization::TPtr ev) {
        ui64 blobId = ev->Get()->Record.GetBlobID();
        {
            std::lock_guard<std::mutex> guard(mtx);
            auto checkIt = events.Checks.find(blobId);
            UNIT_ASSERT(checkIt != events.Checks.end());
            checkIt->second(ev->Get());
            events.Checks.erase(checkIt);
        }
    });
    const TActorId receiver = cluster.RegisterActor(receiverPtr, 1);
    Sleep(TDuration::MilliSeconds(1000));

    for (auto& ev : events.Events) {
        auto senderPtr = new TSendActor(receiver, std::move(ev));
        cluster.RegisterActor(senderPtr, 2);
    }

    for (ui32 attempt = 0; attempt < 50; ++attempt) {
        {
            std::lock_guard<std::mutex> guard(mtx);
            if (events.Checks.empty()) {
                break;
            }
        }
        Sleep(TDuration::MilliSeconds(1000));
    }
    UNIT_ASSERT_VALUES_EQUAL(events.Checks.size(), 0u);
}

TEST_P(XdcRdmaPoolPressureTest, SendMixBigShuffle) {
    const auto params = GetParam();
    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, GetRdmaCqModeFlags(params),
        TTestICCluster::TCheckerFactory(), TDuration::Minutes(1), 50u << 20, GetRdmaSettingsCustomizer(params));
    TEventsForTest events(1000, true);

    auto receiverPtr = new TReceiveActor([&events](TEvTestSerialization::TPtr ev) {
        ui64 blobId = ev->Get()->Record.GetBlobID();
        auto checkIt = events.Checks.find(blobId);
        UNIT_ASSERT(checkIt != events.Checks.end());
        checkIt->second(ev->Get());
        events.Checks.erase(checkIt);
    });
    const TActorId receiver = cluster.RegisterActor(receiverPtr, 1);
    Sleep(TDuration::MilliSeconds(1000));

    for (auto& ev : events.Events) {
        auto senderPtr = new TSendActor(receiver, std::move(ev));
        cluster.RegisterActor(senderPtr, 2);
    }

    for (ui32 attempt = 0; attempt < 10 && !events.Checks.empty(); ++attempt) {
        Sleep(TDuration::MilliSeconds(1000));
    }
    UNIT_ASSERT_VALUES_EQUAL(events.Checks.size(), 0u);
}

static void DoSendHugePayloadsNum(const ui32 numPayloads, const size_t payloadSz, TTestICCluster& cluster,
    std::shared_ptr<NInterconnect::NRdma::IMemPool> pool, const ui32 numRcBufsInRope)
{
    Y_ABORT_UNLESS(numRcBufsInRope > 0 && payloadSz > 0);
    const size_t rcBufSz = std::max(payloadSz / numRcBufsInRope / sizeof(ui32), size_t{1}) * sizeof(ui32);
    Y_ABORT_UNLESS(numRcBufsInRope - 1 <= (payloadSz - 1) / rcBufSz);
    const size_t lastRcBufPos = rcBufSz * (numRcBufsInRope - 1);

    auto ev = std::make_unique<TEvTestSerialization>();
    ev->Record.SetBlobID(0);
    ev->Record.SetBuffer(TStringBuilder{} << "hello world ");
    for (ui32 j = 0; j < numPayloads; ++j) {
        TRope payload;
        size_t writePos = 0;
        while (writePos < lastRcBufPos) {
            auto buf = pool->AllocRcBuf(rcBufSz, NInterconnect::NRdma::IMemPool::PAGE_ALIGNED).value();
            ui32* p = reinterpret_cast<ui32*>(buf.GetDataMut());
            const ui32 value = j * numRcBufsInRope + writePos;
            std::fill(p, p + rcBufSz / sizeof(*p), value);
            payload.Insert(payload.End(), TRope(std::move(buf)));
            writePos += rcBufSz;
        }
        {
            const size_t tailSz = payloadSz - writePos;
            auto buf = pool->AllocRcBuf(tailSz, NInterconnect::NRdma::IMemPool::PAGE_ALIGNED).value();
            char* data = buf.GetDataMut();
            ui32* p = reinterpret_cast<ui32*>(data);
            const size_t words = tailSz / sizeof(*p);
            const ui32 value = j * numRcBufsInRope + writePos;
            std::fill(p, p + words, value);
            std::memcpy(data + words * sizeof(*p), &value, tailSz % sizeof(*p));
            payload.Insert(payload.End(), TRope(std::move(buf)));
        }
        ev->AddPayload(std::move(payload));
    }
    UNIT_ASSERT(ev->AllowExternalDataChannel());

    auto receiverPtr = new TReceiveActor([numPayloads, payloadSz, numRcBufsInRope, rcBufSz, lastRcBufPos](TEvTestSerialization::TPtr ev) {
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayloadCount(), numPayloads);
        for (ui32 j = 0; j < numPayloads; ++j) {
            TRope buf = ev->Get()->GetPayload(j);
            auto span = buf.GetContiguousSpan();
            UNIT_ASSERT_VALUES_EQUAL(span.GetSize(), payloadSz);

            const size_t tailSz = payloadSz % sizeof(j);
            const size_t wordsSz = payloadSz - tailSz;
            for (size_t offset = 0; offset < wordsSz; offset += sizeof(j)) {
                ui32 value;
                std::memcpy(&value, span.GetData() + offset, sizeof(value));
                const size_t rcBufOffset = offset < lastRcBufPos
                    ? offset / rcBufSz * rcBufSz
                    : lastRcBufPos;
                UNIT_ASSERT_VALUES_EQUAL(value, j * numRcBufsInRope + rcBufOffset);
            }
            const ui32 tailValue = j * numRcBufsInRope + lastRcBufPos;
            UNIT_ASSERT_VALUES_EQUAL(std::memcmp(span.GetData() + wordsSz, &tailValue, tailSz), 0);
        }
    });
    const TActorId receiver = cluster.RegisterActor(receiverPtr, 1);

    {
        auto senderPtr = new TSendActor(receiver, std::move(ev));
        cluster.RegisterActor(senderPtr, 2);
    }

    UNIT_ASSERT(receiverPtr->WaitForReceive(1, 20));
}

TEST_P(XdcRdmaPoolPressureTest, Send1Payload) {
    const auto params = GetParam();
    const NInterconnect::NRdma::TMemPoolSettings settings {
        .SizeLimitMb = 256
    };
    auto pool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, settings);

    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, GetRdmaCqModeFlags(params),
        TTestICCluster::TCheckerFactory(), TDuration::Minutes(1), 50u << 20, GetRdmaSettingsCustomizer(params));
    WaitForInterconnectConnection(cluster, 2, 1);

    DoSendHugePayloadsNum(1, 8192, cluster, pool, 1);
}

TEST_P(XdcRdmaPoolPressureTest, Send1Payload2Bufs) {
    const auto params = GetParam();
    const NInterconnect::NRdma::TMemPoolSettings settings {
        .SizeLimitMb = 256
    };
    auto pool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, settings);

    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, GetRdmaCqModeFlags(params),
        TTestICCluster::TCheckerFactory(), TDuration::Minutes(1), 50u << 20, GetRdmaSettingsCustomizer(params));
    WaitForInterconnectConnection(cluster, 2, 1);

    DoSendHugePayloadsNum(1, 8192, cluster, pool, 2);
}

TEST_P(XdcRdmaPoolPressureTest, Send1Payload3BufsUnalign) {
    const auto params = GetParam();
    const NInterconnect::NRdma::TMemPoolSettings settings {
        .SizeLimitMb = 256
    };
    auto pool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, settings);

    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, GetRdmaCqModeFlags(params),
        TTestICCluster::TCheckerFactory(), TDuration::Minutes(1), 50u << 20, GetRdmaSettingsCustomizer(params));
    WaitForInterconnectConnection(cluster, 2, 1);

    DoSendHugePayloadsNum(1, 8195, cluster, pool, 2);
}

TEST_P(XdcRdmaPoolPressureTest, Send1Payload1000BufsUnalign) {
    const auto params = GetParam();
    const NInterconnect::NRdma::TMemPoolSettings settings {
        .SizeLimitMb = 256
    };
    auto pool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, settings);

    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, GetRdmaCqModeFlags(params),
        TTestICCluster::TCheckerFactory(), TDuration::Minutes(1), 50u << 20, GetRdmaSettingsCustomizer(params));
    WaitForInterconnectConnection(cluster, 2, 1);

    DoSendHugePayloadsNum(1, (4 << 20) + 1, cluster, pool, 1000);
}

TEST_P(XdcRdmaPoolPressureTest, Send1Payload9999BufsUnalign) {
    const auto params = GetParam();
    const NInterconnect::NRdma::TMemPoolSettings settings {
        .SizeLimitMb = 256
    };
    auto pool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, settings);

    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, GetRdmaCqModeFlags(params),
        TTestICCluster::TCheckerFactory(), TDuration::Minutes(1), 50u << 20, GetRdmaSettingsCustomizer(params));
    WaitForInterconnectConnection(cluster, 2, 1);

    DoSendHugePayloadsNum(1, (4 << 20) + 1, cluster, pool, 9999);
}

TEST_P(XdcRdmaPoolPressureTest, Send2Payloads) {
    const auto params = GetParam();
    const NInterconnect::NRdma::TMemPoolSettings settings {
        .SizeLimitMb = 256
    };
    auto pool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, settings);

    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, GetRdmaCqModeFlags(params),
        TTestICCluster::TCheckerFactory(), TDuration::Minutes(1), 50u << 20, GetRdmaSettingsCustomizer(params));
    WaitForInterconnectConnection(cluster, 2, 1);

    DoSendHugePayloadsNum(2, 8192, cluster, pool, 1);
}

TEST_P(XdcRdmaPoolPressureTest, Send250Payloads) {
    const auto params = GetParam();
    const NInterconnect::NRdma::TMemPoolSettings settings {
        .SizeLimitMb = 256
    };
    auto pool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, settings);

    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, GetRdmaCqModeFlags(params),
        TTestICCluster::TCheckerFactory(), TDuration::Minutes(1), 50u << 20, GetRdmaSettingsCustomizer(params));
    WaitForInterconnectConnection(cluster, 2, 1);

    DoSendHugePayloadsNum(250, 512, cluster, pool, 1);
}

TEST_P(XdcRdmaPoolPressureTest, Send500Payloads) {
    const auto params = GetParam();
    const NInterconnect::NRdma::TMemPoolSettings settings {
        .SizeLimitMb = 256
    };
    auto pool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, settings);

    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, GetRdmaCqModeFlags(params),
        TTestICCluster::TCheckerFactory(), TDuration::Minutes(1), 50u << 20, GetRdmaSettingsCustomizer(params));
    WaitForInterconnectConnection(cluster, 2, 1);

    DoSendHugePayloadsNum(500, 512, cluster, pool, 1);
}

TEST_P(XdcRdmaPoolPressureTest, Send20Payload100BufsUnalign) {
    const auto params = GetParam();
    const NInterconnect::NRdma::TMemPoolSettings settings {
        .SizeLimitMb = 256
    };
    auto pool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, settings);

    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, GetRdmaCqModeFlags(params),
        TTestICCluster::TCheckerFactory(), TDuration::Minutes(1), 50u << 20, GetRdmaSettingsCustomizer(params));
    WaitForInterconnectConnection(cluster, 2, 1);

    DoSendHugePayloadsNum(20, (2 << 20) + 1, cluster, pool, 100);
}

TEST_P(XdcRdmaPoolPressureTest, Send4000Payloads) {
    const auto params = GetParam();
    const NInterconnect::NRdma::TMemPoolSettings settings {
        .SizeLimitMb = 256
    };
    auto pool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, settings);

    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, GetRdmaCqModeFlags(params),
        TTestICCluster::TCheckerFactory(), TDuration::Minutes(1), 50u << 20, GetRdmaSettingsCustomizer(params));
    WaitForInterconnectConnection(cluster, 2, 1);

    DoSendHugePayloadsNum(4000, 512, cluster, pool, 1);
}

TEST_P(XdcRdmaPoolPressureTest, Send16000Payloads) {
    const auto params = GetParam();
    const NInterconnect::NRdma::TMemPoolSettings settings {
        .SizeLimitMb = 256
    };
    auto pool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, settings);

    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, GetRdmaCqModeFlags(params),
        TTestICCluster::TCheckerFactory(), TDuration::Minutes(1), 50u << 20, GetRdmaSettingsCustomizer(params));
    WaitForInterconnectConnection(cluster, 2, 1);

    DoSendHugePayloadsNum(16000, 512, cluster, pool, 1);
}

TEST_P(XdcRdmaPoolPressureTest, Send32000Payloads) {
    const auto params = GetParam();
    const NInterconnect::NRdma::TMemPoolSettings settings {
        .SizeLimitMb = 256
    };
    auto pool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, settings);

    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, GetRdmaCqModeFlags(params),
        TTestICCluster::TCheckerFactory(), TDuration::Minutes(1), 50u << 20, GetRdmaSettingsCustomizer(params));
    WaitForInterconnectConnection(cluster, 2, 1);

    DoSendHugePayloadsNum(32000, 512, cluster, pool, 1);
}

TEST_P(XdcRdmaPoolPressureTest, SendXPayloads) {
    const auto params = GetParam();
    const NInterconnect::NRdma::TMemPoolSettings settings {
        .SizeLimitMb = 256
    };
    auto pool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, settings);

    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, GetRdmaCqModeFlags(params),
        TTestICCluster::TCheckerFactory(), TDuration::Minutes(1), 50u << 20, GetRdmaSettingsCustomizer(params));
    WaitForInterconnectConnection(cluster, 2, 1);

    for (size_t i = 640; i < 650; i++) {
        DoSendHugePayloadsNum(i, 512, cluster, pool, 1);
    }
}

TEST_P(XdcRdmaPoolPressureTest, SendXPayloadsWithRandSize) {
    const auto params = GetParam();
    const NInterconnect::NRdma::TMemPoolSettings settings {
        .SizeLimitMb = 256
    };
    auto pool = NInterconnect::NRdma::CreateSlotMemPool(nullptr, settings);

    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, GetRdmaCqModeFlags(params),
        TTestICCluster::TCheckerFactory(), TDuration::Minutes(1), 50u << 20, GetRdmaSettingsCustomizer(params));
    WaitForInterconnectConnection(cluster, 2, 1);

    for (size_t i = 640; i < 650; i++) {
        DoSendHugePayloadsNum(i, 512 + (RandomNumber<ui16>(4096) * 4), cluster, pool, 1);
    }
}

INSTANTIATE_TEST_SUITE_P(
    XdcRdmaTest,
    XdcRdmaTransportTest,
    ::testing::Values(
        TRdmaTransportTestParams{NInterconnect::NRdma::ECqMode::POLLING, false},
        TRdmaTransportTestParams{NInterconnect::NRdma::ECqMode::EVENT, false},
        TRdmaTransportTestParams{NInterconnect::NRdma::ECqMode::POLLING, true},
        TRdmaTransportTestParams{NInterconnect::NRdma::ECqMode::EVENT, true}
    ),
    [](const testing::TestParamInfo<TRdmaTransportTestParams>& info) {
        const NInterconnect::NRdma::TMemPoolSettings settings {
            .SizeLimitMb = 256
        };
        NInterconnect::NRdma::CreateSlotMemPool(nullptr, settings);
        return FormatRdmaTransportParam(info.param);
    }
);

INSTANTIATE_TEST_SUITE_P(
    XdcRdmaTest,
    XdcRdmaPoolPressureTest,
    ::testing::Values(
        TRdmaTransportTestParams{NInterconnect::NRdma::ECqMode::POLLING, false},
        TRdmaTransportTestParams{NInterconnect::NRdma::ECqMode::EVENT, false}
    ),
    [](const testing::TestParamInfo<TRdmaTransportTestParams>& info) {
        return FormatRdmaTransportParam(info.param);
    }
);
