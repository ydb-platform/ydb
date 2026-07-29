#include "rdma_xdc_test_common.h"

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
