#include "rdma_xdc_test_common.h"

namespace {

class THistogramSampleCountConsumer : public NMonitoring::ICountableConsumer {
public:
    explicit THistogramSampleCountConsumer(TStringBuf histogramName)
        : HistogramName(histogramName)
    {}

    void OnCounter(const TString&, const TString&, const NMonitoring::TCounterForPtr*) override {
    }

    void OnHistogram(const TString&, const TString& labelValue,
            NMonitoring::IHistogramSnapshotPtr snapshot, bool) override {
        if (labelValue == HistogramName) {
            for (ui32 i = 0; i != snapshot->Count(); ++i) {
                SampleCount += snapshot->Value(i);
            }
        }
    }

    void OnGroupBegin(const TString&, const TString&, const NMonitoring::TDynamicCounters*) override {
    }

    void OnGroupEnd(const TString&, const TString&, const NMonitoring::TDynamicCounters*) override {
    }

    const TString HistogramName;
    ui64 SampleCount = 0;
};

ui64 GetNodeHistogramSampleCount(TTestICCluster& cluster, ui32 nodeId, TStringBuf histogramName) {
    const auto nodeCounters = cluster.GetCounters()->FindSubgroup("nodeId", ToString(nodeId));
    if (!nodeCounters) {
        return 0;
    }

    THistogramSampleCountConsumer consumer(histogramName);
    nodeCounters->Accept({}, {}, consumer);
    return consumer.SampleCount;
}

} // namespace

TEST_P(RdmaSendReceiveTestCqMode, MainChannelTraffic) {
    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, GetRdmaCqModeFlags(GetParam()),
        TTestICCluster::TCheckerFactory(), TDuration::Seconds(30), TNode::DefaultInflight(), EnableRdmaSendReceive);

    ui32 index = 0;
    auto receiverPtr = new TReceiveActor([&index](TEvTestSerialization::TPtr ev) {
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBlobID(), index);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBuffer(), TStringBuilder{} << "send receive " << index);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload().size(), 0u);
        ++index;
    });
    const TActorId receiver = cluster.RegisterActor(receiverPtr, 1);

    WaitForInterconnectConnection(cluster, 2, 1);
    TString lastRdmaStatus;
    UNIT_ASSERT_C(WaitForRdmaChecksumStatus(cluster, 2, 1, "On | SoftwareChecksum | SendReceive", 20, lastRdmaStatus),
        "last RDMA status: " << FormatLastRdmaStatus(lastRdmaStatus));
    const ui64 bytesWrittenToSocketBefore = WaitForSessionCounter(cluster, 2, 1, "BytesWrittenToSocket");

    constexpr ui32 numEvents = 16;
    for (ui32 i = 0; i < numEvents; ++i) {
        auto ev = std::make_unique<TEvTestSerialization>();
        ev->Record.SetBlobID(i);
        ev->Record.SetBuffer(TStringBuilder{} << "send receive " << i);
        cluster.RegisterActor(new TSendActor(receiver, std::move(ev)), 2);
    }

    UNIT_ASSERT(receiverPtr->WaitForReceive(numEvents, 20));
    UNIT_ASSERT_VALUES_EQUAL(GetSessionCounter(cluster, 2, 1, "BytesWrittenToSocket"), bytesWrittenToSocketBefore);
}

TEST_P(RdmaSendReceiveTestCqMode, MainChannelAliasedPayload) {
    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, GetRdmaCqModeFlags(GetParam()),
        TTestICCluster::TCheckerFactory(), TDuration::Seconds(30), TNode::DefaultInflight(), EnableRdmaSendReceive);

    constexpr size_t payloadSize = 1024;
    auto receiverPtr = new TReceiveActor([payloadSize](TEvTestSerialization::TPtr ev) {
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBlobID(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload().size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].GetSize(), payloadSize);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].ConvertToString(), TString(payloadSize, 'X'));
    });
    const TActorId receiver = cluster.RegisterActor(receiverPtr, 1);

    WaitForInterconnectConnection(cluster, 2, 1);
    TString lastRdmaStatus;
    UNIT_ASSERT_C(WaitForRdmaChecksumStatus(cluster, 2, 1, "On | SoftwareChecksum | SendReceive", 20, lastRdmaStatus),
        "last RDMA status: " << FormatLastRdmaStatus(lastRdmaStatus));
    const ui64 bytesWrittenToSocketBefore = WaitForSessionCounter(cluster, 2, 1, "BytesWrittenToSocket");

    auto ev = std::make_unique<TEvTestSerialization>();
    ev->Record.SetBlobID(1);
    ev->AddPayload(TRope(TString(payloadSize, 'X')));
    UNIT_ASSERT(!ev->AllowExternalDataChannel());
    cluster.RegisterActor(new TSendActor(receiver, std::move(ev)), 2);

    UNIT_ASSERT(receiverPtr->WaitForReceive(1, 20));
    UNIT_ASSERT_VALUES_EQUAL(GetSessionCounter(cluster, 2, 1, "BytesWrittenToSocket"), bytesWrittenToSocketBefore);
}

TEST_P(RdmaSendReceiveTestCqMode, MainChannelAliasedProtobuf) {
    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, GetRdmaCqModeFlags(GetParam()),
        TTestICCluster::TCheckerFactory(), TDuration::Seconds(30), TNode::DefaultInflight(), EnableRdmaSendReceive);

    constexpr size_t bufferSize = 1024;
    auto receiverPtr = new TReceiveActor([](TEvTestSerialization::TPtr ev) {
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBlobID(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBuffer(), TString(bufferSize, 'X'));
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload().size(), 0u);
    });
    const TActorId receiver = cluster.RegisterActor(receiverPtr, 1);

    WaitForInterconnectConnection(cluster, 2, 1);
    TString lastRdmaStatus;
    UNIT_ASSERT_C(WaitForRdmaChecksumStatus(cluster, 2, 1, "On | SoftwareChecksum | SendReceive", 20, lastRdmaStatus),
        "last RDMA status: " << FormatLastRdmaStatus(lastRdmaStatus));
    const ui64 bytesWrittenToSocketBefore = WaitForSessionCounter(cluster, 2, 1, "BytesWrittenToSocket");

    auto ev = std::make_unique<TEvTestSerialization>();
    ev->Record.SetBlobID(1);
    ev->Record.SetBuffer(TString(bufferSize, 'X'));
    UNIT_ASSERT(!ev->AllowExternalDataChannel());
    cluster.RegisterActor(new TSendActor(receiver, std::move(ev)), 2);

    UNIT_ASSERT(receiverPtr->WaitForReceive(1, 20));
    UNIT_ASSERT_VALUES_EQUAL(GetSessionCounter(cluster, 2, 1, "BytesWrittenToSocket"), bytesWrittenToSocketBefore);
}

// Verifies that a pre-serialized event backed by ordinary memory is copied into
// RDMA memory and delivered through the RDMA main channel.
TEST_P(RdmaSendReceiveTestCqMode, MainChannelPreSerializedBuffer) {
    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, GetRdmaCqModeFlags(GetParam()),
        TTestICCluster::TCheckerFactory(), TDuration::Seconds(30), TNode::DefaultInflight(), EnableRdmaSendReceive);

    constexpr size_t bufferSize = 1024;
    auto receiverPtr = new TReceiveActor([](TEvTestSerialization::TPtr ev) {
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBlobID(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBuffer(), TString(bufferSize, 'X'));
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload().size(), 0u);
    });
    const TActorId receiver = cluster.RegisterActor(receiverPtr, 1);

    WaitForInterconnectConnection(cluster, 2, 1);
    TString lastRdmaStatus;
    UNIT_ASSERT_C(WaitForRdmaChecksumStatus(cluster, 2, 1, "On | SoftwareChecksum | SendReceive", 20, lastRdmaStatus),
        "last RDMA status: " << FormatLastRdmaStatus(lastRdmaStatus));
    const ui64 bytesWrittenToSocketBefore = WaitForSessionCounter(cluster, 2, 1, "BytesWrittenToSocket");

    auto ev = std::make_unique<TEvTestSerialization>();
    ev->Record.SetBlobID(1);
    ev->Record.SetBuffer(TString(bufferSize, 'X'));
    auto serializedRope = ev->SerializeToRope(GetDefaultRcBufAllocator());
    UNIT_ASSERT(serializedRope);

    auto serialized = MakeIntrusive<TEventSerializedData>(
        std::move(*serializedRope), ev->CreateSerializationInfo(false));
    for (auto iter = serialized->GetBeginIter(); iter.Valid(); iter += iter.ContiguousSize()) {
        UNIT_ASSERT(NInterconnect::NRdma::TryExtractFromRcBuf(iter.GetChunk()).Empty());
    }

    UNIT_ASSERT(cluster.GetNode(2)->GetActorSystem()->Send(new IEventHandle(
        TEvTestSerialization::EventType,
        0,
        receiver,
        TActorId(),
        std::move(serialized),
        0)));

    UNIT_ASSERT(receiverPtr->WaitForReceive(1, 20));
    UNIT_ASSERT_VALUES_EQUAL(GetSessionCounter(cluster, 2, 1, "BytesWrittenToSocket"), bytesWrittenToSocketBefore);
}

// Verifies that a pre-serialized event already backed by RDMA memory is
// delivered through the RDMA main channel.
TEST_P(RdmaSendReceiveTestCqMode, MainChannelRdmaPreSerializedBuffer) {
    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, GetRdmaCqModeFlags(GetParam()),
        TTestICCluster::TCheckerFactory(), TDuration::Seconds(30), TNode::DefaultInflight(), EnableRdmaSendReceive);

    constexpr size_t bufferSize = 1024;
    auto receiverPtr = new TReceiveActor([](TEvTestSerialization::TPtr ev) {
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBlobID(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBuffer(), TString(bufferSize, 'X'));
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload().size(), 0u);
    });
    const TActorId receiver = cluster.RegisterActor(receiverPtr, 1);

    WaitForInterconnectConnection(cluster, 2, 1);
    TString lastRdmaStatus;
    UNIT_ASSERT_C(WaitForRdmaChecksumStatus(cluster, 2, 1, "On | SoftwareChecksum | SendReceive", 20, lastRdmaStatus),
        "last RDMA status: " << FormatLastRdmaStatus(lastRdmaStatus));
    const ui64 bytesWrittenToSocketBefore = WaitForSessionCounter(cluster, 2, 1, "BytesWrittenToSocket");

    auto ev = std::make_unique<TEvTestSerialization>();
    ev->Record.SetBlobID(1);
    ev->Record.SetBuffer(TString(bufferSize, 'X'));
    auto serializedRope = ev->SerializeToRope(GetDefaultRcBufAllocator());
    UNIT_ASSERT(serializedRope);
    const TString serializedData = serializedRope->ConvertToString();

    const auto memPool = cluster.GetNode(2)->GetRdmaMemPool();
    auto rdmaBuffer = memPool->AllocRcBuf(serializedData.size(), 0).value();
    memcpy(rdmaBuffer.GetDataMut(), serializedData.data(), serializedData.size());
    const auto memRegion = NInterconnect::NRdma::TryExtractFromRcBuf(rdmaBuffer);
    UNIT_ASSERT(!memRegion.Empty());

    auto serialized = MakeIntrusive<TEventSerializedData>(
        TRope(std::move(rdmaBuffer)), ev->CreateSerializationInfo(false));
    auto iter = serialized->GetBeginIter();
    UNIT_ASSERT(iter.Valid());
    UNIT_ASSERT(iter.ContiguousData() == memRegion.GetAddr());
    UNIT_ASSERT(NInterconnect::NRdma::TryExtractFromRcBuf(iter.GetChunk()).GetMemRegion() == memRegion.GetMemRegion());
    iter += iter.ContiguousSize();
    UNIT_ASSERT(!iter.Valid());

    UNIT_ASSERT(cluster.GetNode(2)->GetActorSystem()->Send(new IEventHandle(
        TEvTestSerialization::EventType,
        0,
        receiver,
        TActorId(),
        std::move(serialized),
        0)));

    UNIT_ASSERT(receiverPtr->WaitForReceive(1, 20));
    UNIT_ASSERT_VALUES_EQUAL(GetSessionCounter(cluster, 2, 1, "BytesWrittenToSocket"), bytesWrittenToSocketBefore);
}

// Verifies successful serialization and reassembly of one event spanning
// multiple RDMA main-channel packets.
TEST_P(RdmaSendReceiveTestCqMode, MainChannelMultiPacketEvent) {
    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, GetRdmaCqModeFlags(GetParam()),
        TTestICCluster::TCheckerFactory(), TDuration::Seconds(30), TNode::DefaultInflight(), EnableRdmaSendReceive);

    constexpr size_t bufferSize = 2 * TTcpPacketBuf::PacketDataLen + 1;
    auto receiverPtr = new TReceiveActor([](TEvTestSerialization::TPtr ev) {
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBlobID(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBuffer(), TString(bufferSize, 'X'));
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload().size(), 0u);
    });
    const TActorId receiver = cluster.RegisterActor(receiverPtr, 1);

    WaitForInterconnectConnection(cluster, 2, 1);
    TString lastRdmaStatus;
    UNIT_ASSERT_C(WaitForRdmaChecksumStatus(cluster, 2, 1, "On | SoftwareChecksum | SendReceive", 20, lastRdmaStatus),
        "last RDMA status: " << FormatLastRdmaStatus(lastRdmaStatus));
    const ui64 bytesWrittenToSocketBefore = WaitForSessionCounter(cluster, 2, 1, "BytesWrittenToSocket");

    auto ev = std::make_unique<TEvTestSerialization>();
    ev->Record.SetBlobID(1);
    ev->Record.SetBuffer(TString(bufferSize, 'X'));
    UNIT_ASSERT(!ev->AllowExternalDataChannel());
    cluster.RegisterActor(new TSendActor(receiver, std::move(ev)), 2);

    UNIT_ASSERT(receiverPtr->WaitForReceive(1, 20));
    UNIT_ASSERT_VALUES_EQUAL(GetSessionCounter(cluster, 2, 1, "BytesWrittenToSocket"), bytesWrittenToSocketBefore);
}

// Verifies that one main-channel packet split at the 16-SGE SEND limit is
// reassembled from multiple RDMA receives without changing payload order.
TEST_P(RdmaSendReceiveTestCqMode, MainChannelSgListBoundary) {
    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, GetRdmaCqModeFlags(GetParam()),
        TTestICCluster::TCheckerFactory(), TDuration::Seconds(30), TNode::DefaultInflight(), EnableRdmaSendReceive);

    constexpr size_t chunkCount = 17;
    constexpr size_t chunkSize = 64;
    constexpr size_t chunkStride = 2 * chunkSize;

    TString expectedPayload;
    for (size_t i = 0; i != chunkCount; ++i) {
        expectedPayload.append(chunkSize, 'A' + i);
    }

    auto receiverPtr = new TReceiveActor([expectedPayload](TEvTestSerialization::TPtr ev) {
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBlobID(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload().size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].ConvertToString(), expectedPayload);
    });
    const TActorId receiver = cluster.RegisterActor(receiverPtr, 1);

    WaitForInterconnectConnection(cluster, 2, 1);
    TString lastRdmaStatus;
    UNIT_ASSERT_C(WaitForRdmaChecksumStatus(cluster, 2, 1, "On | SoftwareChecksum | SendReceive", 20, lastRdmaStatus),
        "last RDMA status: " << FormatLastRdmaStatus(lastRdmaStatus));
    const ui64 bytesWrittenToSocketBefore = WaitForSessionCounter(cluster, 2, 1, "BytesWrittenToSocket");

    const auto memPool = cluster.GetNode(2)->GetRdmaMemPool();
    auto storage = memPool->AllocRcBuf(chunkCount * chunkStride, 0).value();
    TRope payload;
    for (size_t i = 0; i != chunkCount; ++i) {
        char* data = storage.GetDataMut() + i * chunkStride;
        memset(data, 'A' + i, chunkSize);
        payload.Insert(payload.End(), TRope(TRcBuf(TRcBuf::Piece, data, chunkSize, storage)));
    }

    size_t contiguousBlocks = 0;
    for (auto iter = payload.Begin(); iter.Valid(); iter.AdvanceToNextContiguousBlock()) {
        ++contiguousBlocks;
        UNIT_ASSERT(!NInterconnect::NRdma::TryExtractFromRcBuf(iter.GetChunk()).Empty());
    }
    UNIT_ASSERT_VALUES_EQUAL(contiguousBlocks, chunkCount);

    auto ev = std::make_unique<TEvTestSerialization>();
    ev->Record.SetBlobID(1);
    ev->AddPayload(std::move(payload));
    UNIT_ASSERT(!ev->AllowExternalDataChannel());
    cluster.RegisterActor(new TSendActor(receiver, std::move(ev)), 2);

    UNIT_ASSERT(receiverPtr->WaitForReceive(1, 20));
    UNIT_ASSERT_VALUES_EQUAL(GetSessionCounter(cluster, 2, 1, "BytesWrittenToSocket"), bytesWrittenToSocketBefore);
}

// Verifies that a standalone main-channel ACK is delivered over RDMA and
// releases the sender's in-flight data without writing in either TCP direction.
TEST_P(RdmaSendReceiveTestCqMode, MainChannelAck) {
    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, GetRdmaCqModeFlags(GetParam()),
        TTestICCluster::TCheckerFactory(), TDuration::Seconds(30), TNode::DefaultInflight(), EnableRdmaSendReceive);

    auto receiverPtr = new TReceiveActor([](TEvTestSerialization::TPtr ev) {
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBlobID(), 1u);
    });
    const TActorId receiver = cluster.RegisterActor(receiverPtr, 1);

    WaitForInterconnectConnection(cluster, 2, 1);
    TString lastRdmaStatus;
    UNIT_ASSERT_C(WaitForRdmaChecksumStatus(cluster, 2, 1, "On | SoftwareChecksum | SendReceive", 20, lastRdmaStatus),
        "last RDMA status: " << FormatLastRdmaStatus(lastRdmaStatus));

    const ui64 packetsConfirmedBefore = WaitForSessionCounter(cluster, 2, 1, "PacketsConfirmed");
    const ui64 senderBytesWrittenToSocketBefore = GetSessionCounter(cluster, 2, 1, "BytesWrittenToSocket");
    const ui64 receiverBytesWrittenToSocketBefore = WaitForSessionCounter(cluster, 1, 2, "BytesWrittenToSocket");

    auto ev = std::make_unique<TEvTestSerialization>();
    ev->Record.SetBlobID(1);
    ev->Record.SetBuffer("acknowledge this event");
    cluster.RegisterActor(new TSendActor(receiver, std::move(ev)), 2);

    UNIT_ASSERT(receiverPtr->WaitForReceive(1, 20));

    bool confirmed = false;
    ui64 packetsConfirmed = packetsConfirmedBefore;
    ui64 inflightDataAmount = 0;
    const TInstant deadline = TInstant::Now() + TDuration::Seconds(20);
    while (TInstant::Now() < deadline) {
        packetsConfirmed = GetSessionCounter(cluster, 2, 1, "PacketsConfirmed");
        inflightDataAmount = GetSessionCounter(cluster, 2, 1, "InflightDataAmount");
        if (packetsConfirmed > packetsConfirmedBefore && inflightDataAmount == 0) {
            confirmed = true;
            break;
        }
        Sleep(TDuration::MilliSeconds(10));
    }

    UNIT_ASSERT_C(confirmed, "packetsConfirmedBefore# " << packetsConfirmedBefore
        << " packetsConfirmed# " << packetsConfirmed << " inflightDataAmount# " << inflightDataAmount);
    UNIT_ASSERT_VALUES_EQUAL(GetSessionCounter(cluster, 2, 1, "BytesWrittenToSocket"), senderBytesWrittenToSocketBefore);
    UNIT_ASSERT_VALUES_EQUAL(GetSessionCounter(cluster, 1, 2, "BytesWrittenToSocket"), receiverBytesWrittenToSocketBefore);
}

// Verifies that a ping request and response update the RTT histogram while
// both directions of the main channel remain on RDMA.
TEST_P(RdmaSendReceiveTestCqMode, MainChannelPing) {
    auto settingsCustomizer = [](ui32 nodeId, TInterconnectSettings& settings) {
        EnableRdmaSendReceive(nodeId, settings);
        settings.PingPeriod = TDuration::MilliSeconds(100);
    };
    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, GetRdmaCqModeFlags(GetParam()),
        TTestICCluster::TCheckerFactory(), TDuration::Seconds(30), TNode::DefaultInflight(), settingsCustomizer);

    auto receiverPtr = new TReceiveActor([](TEvTestSerialization::TPtr ev) {
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBlobID(), 1u);
    });
    const TActorId receiver = cluster.RegisterActor(receiverPtr, 1);

    WaitForInterconnectConnection(cluster, 2, 1);
    TString lastRdmaStatus;
    UNIT_ASSERT_C(WaitForRdmaChecksumStatus(cluster, 2, 1, "On | SoftwareChecksum | SendReceive", 20, lastRdmaStatus),
        "last RDMA status: " << FormatLastRdmaStatus(lastRdmaStatus));

    const ui64 pingSamplesBefore = GetNodeHistogramSampleCount(cluster, 2, "PingTimeRdmaUs");
    const ui64 senderBytesWrittenToSocketBefore = GetSessionCounter(cluster, 2, 1, "BytesWrittenToSocket");
    const ui64 receiverBytesWrittenToSocketBefore = WaitForSessionCounter(cluster, 1, 2, "BytesWrittenToSocket");

    auto ev = std::make_unique<TEvTestSerialization>();
    ev->Record.SetBlobID(1);
    cluster.RegisterActor(new TSendActor(receiver, std::move(ev)), 2);
    UNIT_ASSERT(receiverPtr->WaitForReceive(1, 20));

    ui64 pingSamples = pingSamplesBefore;
    const TInstant deadline = TInstant::Now() + TDuration::Seconds(20);
    while (TInstant::Now() < deadline) {
        pingSamples = GetNodeHistogramSampleCount(cluster, 2, "PingTimeRdmaUs");
        if (pingSamples > pingSamplesBefore) {
            break;
        }
        Sleep(TDuration::MilliSeconds(10));
    }

    UNIT_ASSERT_C(pingSamples > pingSamplesBefore,
        "pingSamplesBefore# " << pingSamplesBefore << " pingSamples# " << pingSamples);
    UNIT_ASSERT_VALUES_EQUAL(GetSessionCounter(cluster, 2, 1, "BytesWrittenToSocket"), senderBytesWrittenToSocketBefore);
    UNIT_ASSERT_VALUES_EQUAL(GetSessionCounter(cluster, 1, 2, "BytesWrittenToSocket"), receiverBytesWrittenToSocketBefore);
}

// Verifies simultaneous application traffic over the RDMA main channel in
// both directions without falling back to either TCP main channel.
TEST_P(RdmaSendReceiveTestCqMode, BidirectionalMainChannelTraffic) {
    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, GetRdmaCqModeFlags(GetParam()),
        TTestICCluster::TCheckerFactory(), TDuration::Seconds(30), TNode::DefaultInflight(), EnableRdmaSendReceive);

    constexpr ui32 numEvents = 16;
    auto node1ReceiverPtr = new TReceiveActor([](TEvTestSerialization::TPtr ev) {
        const ui32 index = ev->Get()->Record.GetBlobID();
        UNIT_ASSERT(index < numEvents);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBuffer(), TStringBuilder{} << "2 to 1 " << index);
    });
    const TActorId node1Receiver = cluster.RegisterActor(node1ReceiverPtr, 1);

    auto node2ReceiverPtr = new TReceiveActor([](TEvTestSerialization::TPtr ev) {
        const ui32 index = ev->Get()->Record.GetBlobID();
        UNIT_ASSERT(index < numEvents);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBuffer(), TStringBuilder{} << "1 to 2 " << index);
    });
    const TActorId node2Receiver = cluster.RegisterActor(node2ReceiverPtr, 2);

    WaitForInterconnectConnection(cluster, 2, 1);
    WaitForInterconnectConnection(cluster, 1, 2);
    TString node2RdmaStatus;
    UNIT_ASSERT_C(WaitForRdmaChecksumStatus(cluster, 2, 1, "On | SoftwareChecksum | SendReceive", 20, node2RdmaStatus),
        "last node 2 RDMA status: " << FormatLastRdmaStatus(node2RdmaStatus));
    TString node1RdmaStatus;
    UNIT_ASSERT_C(WaitForRdmaChecksumStatus(cluster, 1, 2, "On | SoftwareChecksum | SendReceive", 20, node1RdmaStatus),
        "last node 1 RDMA status: " << FormatLastRdmaStatus(node1RdmaStatus));

    const ui64 node2BytesWrittenToSocketBefore = GetSessionCounter(cluster, 2, 1, "BytesWrittenToSocket");
    const ui64 node1BytesWrittenToSocketBefore = GetSessionCounter(cluster, 1, 2, "BytesWrittenToSocket");

    for (ui32 i = 0; i != numEvents; ++i) {
        auto node2Event = std::make_unique<TEvTestSerialization>();
        node2Event->Record.SetBlobID(i);
        node2Event->Record.SetBuffer(TStringBuilder{} << "2 to 1 " << i);
        cluster.RegisterActor(new TSendActor(node1Receiver, std::move(node2Event)), 2);

        auto node1Event = std::make_unique<TEvTestSerialization>();
        node1Event->Record.SetBlobID(i);
        node1Event->Record.SetBuffer(TStringBuilder{} << "1 to 2 " << i);
        cluster.RegisterActor(new TSendActor(node2Receiver, std::move(node1Event)), 1);
    }

    UNIT_ASSERT(node1ReceiverPtr->WaitForReceive(numEvents, 20));
    UNIT_ASSERT(node2ReceiverPtr->WaitForReceive(numEvents, 20));
    UNIT_ASSERT_VALUES_EQUAL(GetSessionCounter(cluster, 2, 1, "BytesWrittenToSocket"), node2BytesWrittenToSocketBefore);
    UNIT_ASSERT_VALUES_EQUAL(GetSessionCounter(cluster, 1, 2, "BytesWrittenToSocket"), node1BytesWrittenToSocketBefore);
}

// Verifies that MaxSerializedEventSize is inclusive for the RDMA main-channel
// serializer: an event whose serialized size equals the limit is delivered.
TEST_P(RdmaSendReceiveTestCqMode, MaxSizedMainChannelEvent) {
    constexpr size_t bufferSize = 1024;
    auto sizeProbe = std::make_unique<TEvTestSerialization>();
    sizeProbe->Record.SetBlobID(1);
    sizeProbe->Record.SetBuffer(TString(bufferSize, 'X'));
    const ui32 maxSerializedEventSize = sizeProbe->CalculateSerializedSize();

    auto settingsCustomizer = [maxSerializedEventSize](ui32 nodeId, TInterconnectSettings& settings) {
        EnableRdmaSendReceive(nodeId, settings);
        settings.MaxSerializedEventSize = maxSerializedEventSize;
    };
    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, GetRdmaCqModeFlags(GetParam()),
        TTestICCluster::TCheckerFactory(), TDuration::Seconds(30), TNode::DefaultInflight(), settingsCustomizer);

    auto receiverPtr = new TReceiveActor([](TEvTestSerialization::TPtr ev) {
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBlobID(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBuffer(), TString(bufferSize, 'X'));
    });
    const TActorId receiver = cluster.RegisterActor(receiverPtr, 1);

    WaitForInterconnectConnection(cluster, 2, 1);
    TString lastRdmaStatus;
    UNIT_ASSERT_C(WaitForRdmaChecksumStatus(cluster, 2, 1, "On | SoftwareChecksum | SendReceive", 20, lastRdmaStatus),
        "last RDMA status: " << FormatLastRdmaStatus(lastRdmaStatus));
    const ui64 bytesWrittenToSocketBefore = WaitForSessionCounter(cluster, 2, 1, "BytesWrittenToSocket");

    auto ev = std::make_unique<TEvTestSerialization>();
    ev->Record.SetBlobID(1);
    ev->Record.SetBuffer(TString(bufferSize, 'X'));
    UNIT_ASSERT_VALUES_EQUAL(ev->CalculateSerializedSize(), maxSerializedEventSize);
    cluster.RegisterActor(new TSendActor(receiver, std::move(ev)), 2);

    UNIT_ASSERT(receiverPtr->WaitForReceive(1, 20));
    UNIT_ASSERT_VALUES_EQUAL(GetSessionCounter(cluster, 2, 1, "BytesWrittenToSocket"), bytesWrittenToSocketBefore);
}

namespace {

void RunOversizedMainChannelEventTest(NInterconnect::NRdma::ECqMode cqMode,
        ui32 maxSerializedEventSize, size_t payloadSize) {
    auto settingsCustomizer = [maxSerializedEventSize](ui32 nodeId, TInterconnectSettings& settings) {
        EnableRdmaSendReceive(nodeId, settings);
        settings.MaxSerializedEventSize = maxSerializedEventSize;
    };
    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, GetRdmaCqModeFlags(cqMode),
        TTestICCluster::TCheckerFactory(), TDuration::Seconds(30), TNode::DefaultInflight(), settingsCustomizer);

    auto receiverPtr = new TReceiveActor([](TEvTestSerialization::TPtr) {});
    const TActorId receiver = cluster.RegisterActor(receiverPtr, 1);

    WaitForInterconnectConnection(cluster, 2, 1);
    TString lastRdmaStatus;
    UNIT_ASSERT_C(WaitForRdmaChecksumStatus(cluster, 2, 1, "On | SoftwareChecksum | SendReceive", 20, lastRdmaStatus),
        "last RDMA status: " << FormatLastRdmaStatus(lastRdmaStatus));

    auto extCtx = std::make_shared<TSendActor::TExtCtx>();
    auto ev = std::make_unique<TEvTestSerialization>();
    ev->Record.SetBuffer(TString(payloadSize, 'X'));
    UNIT_ASSERT(!ev->AllowExternalDataChannel());
    cluster.RegisterActor(new TSendActor(receiver, std::move(ev), extCtx), 2);

    UNIT_ASSERT(extCtx->WaitForUndelivered(20));
    UNIT_ASSERT_VALUES_EQUAL(receiverPtr->ReceivedEvents.load(std::memory_order_relaxed), 0u);
}

} // namespace

TEST_P(RdmaSendReceiveTestCqMode, OversizedMainChannelEventDoesNotCrash) {
    constexpr ui32 maxSerializedEventSize = 1024;
    RunOversizedMainChannelEventTest(GetParam(), maxSerializedEventSize, 2 * maxSerializedEventSize);
}

TEST_P(RdmaSendReceiveTestCqMode, OversizedMultiPacketMainChannelEventDoesNotCrash) {
    constexpr ui32 maxSerializedEventSize = 2 * TTcpPacketBuf::PacketDataLen;
    RunOversizedMainChannelEventTest(GetParam(), maxSerializedEventSize, 3 * TTcpPacketBuf::PacketDataLen);
}

// Verifies that an oversized external payload terminates the session and is
// reported as undelivered instead of crashing in the external chunk consumer.
TEST_P(RdmaSendReceiveTestCqMode, OversizedExternalChannelEventDoesNotCrash) {
    constexpr ui32 maxSerializedEventSize = 1024;
    auto settingsCustomizer = [](ui32 nodeId, TInterconnectSettings& settings) {
        EnableRdmaSendReceive(nodeId, settings);
        settings.MaxSerializedEventSize = maxSerializedEventSize;
    };
    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, GetRdmaCqModeFlags(GetParam()),
        TTestICCluster::TCheckerFactory(), TDuration::Seconds(30), TNode::DefaultInflight(), settingsCustomizer);

    auto receiverPtr = new TReceiveActor([](TEvTestSerialization::TPtr) {});
    const TActorId receiver = cluster.RegisterActor(receiverPtr, 1);

    WaitForInterconnectConnection(cluster, 2, 1);
    TString lastRdmaStatus;
    UNIT_ASSERT_C(WaitForRdmaChecksumStatus(cluster, 2, 1, "On | SoftwareChecksum | SendReceive", 20, lastRdmaStatus),
        "last RDMA status: " << FormatLastRdmaStatus(lastRdmaStatus));

    auto extCtx = std::make_shared<TSendActor::TExtCtx>();
    auto ev = std::make_unique<TEvTestSerialization>();
    ev->AddPayload(TRope(TString(5000, 'X')));
    UNIT_ASSERT_VALUES_EQUAL(ev->Record.ByteSize(), 0);
    UNIT_ASSERT(ev->AllowExternalDataChannel());
    cluster.RegisterActor(new TSendActor(receiver, std::move(ev), extCtx), 2);

    UNIT_ASSERT(extCtx->WaitForUndelivered(20));
    UNIT_ASSERT_VALUES_EQUAL(receiverPtr->ReceivedEvents.load(std::memory_order_relaxed), 0u);
}

// Verifies that terminating an RDMA session because of an oversized event does
// not prevent a subsequent session from delivering regular traffic.
TEST_P(RdmaSendReceiveTestCqMode, RecoversAfterOversizedMainChannelEvent) {
    constexpr ui32 maxSerializedEventSize = 1024;
    auto settingsCustomizer = [](ui32 nodeId, TInterconnectSettings& settings) {
        EnableRdmaSendReceive(nodeId, settings);
        settings.MaxSerializedEventSize = maxSerializedEventSize;
    };
    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, GetRdmaCqModeFlags(GetParam()),
        TTestICCluster::TCheckerFactory(), TDuration::Seconds(30), TNode::DefaultInflight(), settingsCustomizer);

    auto receiverPtr = new TReceiveActor([](TEvTestSerialization::TPtr ev) {
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBlobID(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBuffer(), "after oversized event");
    });
    const TActorId receiver = cluster.RegisterActor(receiverPtr, 1);

    WaitForInterconnectConnection(cluster, 2, 1);
    TString lastRdmaStatus;
    UNIT_ASSERT_C(WaitForRdmaChecksumStatus(cluster, 2, 1, "On | SoftwareChecksum | SendReceive", 20, lastRdmaStatus),
        "last RDMA status: " << FormatLastRdmaStatus(lastRdmaStatus));

    auto extCtx = std::make_shared<TSendActor::TExtCtx>();
    auto oversized = std::make_unique<TEvTestSerialization>();
    oversized->Record.SetBuffer(TString(2 * maxSerializedEventSize, 'X'));
    UNIT_ASSERT(!oversized->AllowExternalDataChannel());
    cluster.RegisterActor(new TSendActor(receiver, std::move(oversized), extCtx), 2);

    UNIT_ASSERT(extCtx->WaitForUndelivered(20));
    UNIT_ASSERT_VALUES_EQUAL(receiverPtr->ReceivedEvents.load(std::memory_order_relaxed), 0u);

    WaitForInterconnectConnection(cluster, 2, 1);
    lastRdmaStatus.clear();
    UNIT_ASSERT_C(WaitForRdmaChecksumStatus(cluster, 2, 1, "On | SoftwareChecksum | SendReceive", 20, lastRdmaStatus),
        "last RDMA status after reconnect: " << FormatLastRdmaStatus(lastRdmaStatus));

    auto ev = std::make_unique<TEvTestSerialization>();
    ev->Record.SetBlobID(1);
    ev->Record.SetBuffer("after oversized event");
    cluster.RegisterActor(new TSendActor(receiver, std::move(ev)), 2);

    UNIT_ASSERT(receiverPtr->WaitForReceive(1, 20));
}

// Verifies that Send/Receive is not enabled unless both peers advertise it:
// the main channel stays on TCP while an RDMA-capable payload still uses RDMA READ.
TEST_P(RdmaSendReceiveTestCqMode, FallsBackToTcpMainWhenPeerDoesNotSupportSendReceive) {
    auto settingsCustomizer = [](ui32 nodeId, TInterconnectSettings& settings) {
        if (nodeId == 2) {
            EnableRdmaSendReceive(nodeId, settings);
        }
    };
    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, GetRdmaCqModeFlags(GetParam()),
        TTestICCluster::TCheckerFactory(), TDuration::Seconds(30), TNode::DefaultInflight(), settingsCustomizer);

    constexpr size_t payloadSize = 5000;
    auto receiverPtr = new TReceiveActor([](TEvTestSerialization::TPtr ev) {
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBlobID(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBuffer(), "asymmetric send receive support");
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload().size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].ConvertToString(), TString(payloadSize, 'X'));
    });
    const TActorId receiver = cluster.RegisterActor(receiverPtr, 1);

    WaitForInterconnectConnection(cluster, 2, 1);
    TString lastRdmaStatus;
    UNIT_ASSERT_C(WaitForRdmaChecksumStatus(cluster, 2, 1, "On | SoftwareChecksum", 20, lastRdmaStatus),
        "last RDMA status: " << FormatLastRdmaStatus(lastRdmaStatus));

    const ui64 bytesWrittenToSocketBefore = WaitForSessionCounter(cluster, 2, 1, "BytesWrittenToSocket");
    const ui64 rdmaBytesReadScheduledBefore = WaitForSessionCounter(cluster, 1, 2, "RdmaBytesReadScheduled");

    auto payload = cluster.GetNode(2)->GetRdmaMemPool()->AllocRcBuf(payloadSize, 0).value();
    memset(payload.GetDataMut(), 'X', payloadSize);

    auto ev = std::make_unique<TEvTestSerialization>();
    ev->Record.SetBlobID(1);
    ev->Record.SetBuffer("asymmetric send receive support");
    ev->AddPayload(TRope(std::move(payload)));
    UNIT_ASSERT(ev->AllowExternalDataChannel());
    cluster.RegisterActor(new TSendActor(receiver, std::move(ev)), 2);

    UNIT_ASSERT(receiverPtr->WaitForReceive(1, 20));
    UNIT_ASSERT(GetSessionCounter(cluster, 2, 1, "BytesWrittenToSocket") > bytesWrittenToSocketBefore);
    UNIT_ASSERT(GetSessionCounter(cluster, 1, 2, "RdmaBytesReadScheduled") > rdmaBytesReadScheduledBefore);
}

// Verifies the opposite asymmetric handshake direction: a sender without
// Send/Receive support keeps TCP main while the receiver still uses RDMA READ.
TEST_P(RdmaSendReceiveTestCqMode, FallsBackToTcpMainWhenSenderDoesNotSupportSendReceive) {
    auto settingsCustomizer = [](ui32 nodeId, TInterconnectSettings& settings) {
        if (nodeId == 2) {
            EnableRdmaSendReceive(nodeId, settings);
        }
    };
    TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, GetRdmaCqModeFlags(GetParam()),
        TTestICCluster::TCheckerFactory(), TDuration::Seconds(30), TNode::DefaultInflight(), settingsCustomizer);

    constexpr size_t payloadSize = 5000;
    auto receiverPtr = new TReceiveActor([](TEvTestSerialization::TPtr ev) {
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBlobID(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetBuffer(), "sender without send receive support");
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload().size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetPayload()[0].ConvertToString(), TString(payloadSize, 'X'));
    });
    const TActorId receiver = cluster.RegisterActor(receiverPtr, 2);

    WaitForInterconnectConnection(cluster, 1, 2);
    TString lastRdmaStatus;
    UNIT_ASSERT_C(WaitForRdmaChecksumStatus(cluster, 1, 2, "On | SoftwareChecksum", 20, lastRdmaStatus),
        "last RDMA status: " << FormatLastRdmaStatus(lastRdmaStatus));

    const ui64 bytesWrittenToSocketBefore = WaitForSessionCounter(cluster, 1, 2, "BytesWrittenToSocket");
    const ui64 rdmaBytesReadScheduledBefore = WaitForSessionCounter(cluster, 2, 1, "RdmaBytesReadScheduled");

    auto payload = cluster.GetNode(1)->GetRdmaMemPool()->AllocRcBuf(payloadSize, 0).value();
    memset(payload.GetDataMut(), 'X', payloadSize);

    auto ev = std::make_unique<TEvTestSerialization>();
    ev->Record.SetBlobID(1);
    ev->Record.SetBuffer("sender without send receive support");
    ev->AddPayload(TRope(std::move(payload)));
    UNIT_ASSERT(ev->AllowExternalDataChannel());
    cluster.RegisterActor(new TSendActor(receiver, std::move(ev)), 1);

    UNIT_ASSERT(receiverPtr->WaitForReceive(1, 20));
    UNIT_ASSERT(GetSessionCounter(cluster, 1, 2, "BytesWrittenToSocket") > bytesWrittenToSocketBefore);
    UNIT_ASSERT(GetSessionCounter(cluster, 2, 1, "RdmaBytesReadScheduled") > rdmaBytesReadScheduledBefore);
}

INSTANTIATE_TEST_SUITE_P(
    RdmaSendReceive,
    RdmaSendReceiveTestCqMode,
    ::testing::Values(
        NInterconnect::NRdma::ECqMode::POLLING,
        NInterconnect::NRdma::ECqMode::EVENT
    ),
    [](const testing::TestParamInfo<NInterconnect::NRdma::ECqMode>& info) {
        return FormatRdmaCqMode(info.param);
    }
);
