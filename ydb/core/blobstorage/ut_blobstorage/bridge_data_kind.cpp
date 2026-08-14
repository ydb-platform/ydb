#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>

namespace {

TNodeLocation GetLocation(ui32 nodeId) {
    NActorsInterconnect::TNodeLocation location;
    if (1 <= nodeId && nodeId <= 8) {
        location.SetBridgePileName("pile_1");
    } else if (9 <= nodeId && nodeId <= 16) {
        location.SetBridgePileName("pile_2");
    } else if (17 <= nodeId && nodeId <= 24) {
        location.SetBridgePileName("pile_3");
    } else {
        Y_ABORT();
    }
    location.SetDataCenter("my_dc");
    location.SetRack(TStringBuilder() << "rack_" << nodeId);
    location.SetUnit(TStringBuilder() << "unit_" << nodeId);
    return TNodeLocation(location);
}

} // namespace

Y_UNIT_TEST_SUITE(BridgeDataKind) {

    // With interpile traffic optimization the bridge proxy hands the blob over to a node inside the
    // remote pile instead of writing it across the link itself. That leg rebuilds the message and
    // then passes it through a protobuf, so the data kind has to survive both hops -- otherwise a
    // system tablet writing to a bridged group is admitted as user data in every pile but its own.
    Y_UNIT_TEST(SurvivesInterpileTrafficOptimization) {
        TFeatureFlags featureFlags;
        featureFlags.SetEnableInterpileTrafficOptimization(true);

        TEnvironmentSetup env{{
            .NodeCount = 8 * 3,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetLocation,
            .FeatureFlags = featureFlags,
            .SelfManagementConfig = true,
            .NumPiles = 3,
            .AutomaticBootstrap = true,
        }};
        env.CreatePool();
        const ui32 groupId = env.GetGroups().front();

        ui32 step = 1;
        auto put = [&](NKikimrBlobStorage::TDataKind::E dataKind) {
            const TString data = "hello";
            const TLogoBlobID id(100500, 1, step++, 0, data.size(), 0);
            const TActorId sender = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
            env.Runtime->WrapInActorContext(sender, [&] {
                SendToBSProxy(sender, groupId, new TEvBlobStorage::TEvPut(TEvBlobStorage::TEvPut::TParameters{
                    .BlobId = id,
                    .Buffer = TRope(data),
                    .Deadline = TInstant::Max(),
                    .DataKind = dataKind,
                }));
            });
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(sender, false);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        };

        // The interpile option is only taken once the queues towards the remote piles have
        // connected, so warm them up first -- otherwise the measured writes may quietly fall back
        // to going across the link and the test would prove nothing.
        put(NKikimrBlobStorage::TDataKind::USER);
        env.Sim(TDuration::Seconds(5));

        for (const auto dataKind : {NKikimrBlobStorage::TDataKind::USER, NKikimrBlobStorage::TDataKind::SYSTEM}) {
            std::vector<NKikimrBlobStorage::TDataKind::E> seen;
            env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
                if (ev->GetTypeRewrite() == TEvBlobStorage::EvInterpilePut) {
                    for (const auto& item : ev->Get<TEvInterpilePut>()->Record.GetItems()) {
                        seen.push_back(item.GetDataKind());
                    }
                }
                return true;
            };

            put(dataKind);

            env.Runtime->FilterFunction = nullptr;

            UNIT_ASSERT_C(!seen.empty(), "no write reached a remote pile the interpile way");
            for (const auto kind : seen) {
                UNIT_ASSERT_EQUAL(kind, dataKind);
            }
        }
    }

}
