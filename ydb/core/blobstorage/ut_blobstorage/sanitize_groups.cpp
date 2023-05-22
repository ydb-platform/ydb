#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/mind/bscontroller/layout_helpers.h>

Y_UNIT_TEST_SUITE(GroupLayoutSanitizer) {

    Y_UNIT_TEST(Test3dc) {
        const ui32 numRacks = 15;
        TBlobStorageGroupType groupType = TBlobStorageGroupType::ErasureMirror3dc;
        std::vector<ui32> nodesPerRack(numRacks);
        std::vector<ui32> nodeToRack;
        for (ui32 numFilledRacks = 0; numFilledRacks < numRacks; ) {
//            const ui32 rackId = RandomNumber(numRacks);
            const ui32 rackId = numFilledRacks;
            nodeToRack.emplace_back(rackId);
            numFilledRacks += !nodesPerRack[rackId]++;
        }
        const ui32 numDatacenters = 3;
        std::vector<ui32> rackToDatacenter;
        for (ui32 i = 0; i < numRacks; ++i) {
            rackToDatacenter.push_back(i % numDatacenters);
        }

        std::vector<TNodeLocation> locations;
        for (ui32 i = 0; i < nodeToRack.size(); ++i) {
            NActorsInterconnect::TNodeLocation proto;
            proto.SetDataCenter(ToString(rackToDatacenter[nodeToRack[i]]));
            proto.SetRack(ToString(nodeToRack[i]));
            proto.SetUnit(ToString(i));
            locations.emplace_back(proto);
        }

        TEnvironmentSetup env{{
            .NodeCount = (ui32)nodeToRack.size(),
            .Erasure = TBlobStorageGroupType::ErasureMirror3dc,
            .LocationGenerator = [&](ui32 nodeId) { return locations[nodeId - 1]; },
        }};


        auto catchSanitizeRequests = [](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::TEvControllerConfigRequest::EventType) {
                const auto& request = ev->Get<TEvBlobStorage::TEvControllerConfigRequest>()->Record.GetRequest();
                for (const auto& command : request.GetCommand()) {
                    UNIT_ASSERT(command.GetCommandCase() != NKikimrBlobStorage::TConfigRequest::TCommand::kSanitizeGroup);
                }
            }
            return true;
        };

        env.Runtime->FilterFunction = catchSanitizeRequests;

        // Initialize runtime
        TGroupGeometryInfo geom = CreateGroupGeometry(groupType);

        const ui32 disksPerNode = 1;
        const ui32 slotsPerDisk = 3;
        env.CreateBoxAndPool(disksPerNode, nodeToRack.size() * disksPerNode * slotsPerDisk / 9);
        env.Sim(TDuration::Seconds(30));

        TString error;
        auto cfg = env.FetchBaseConfig();
        UNIT_ASSERT_C(CheckBaseConfigLayout(geom, cfg, error), error);
        env.Cleanup();

        // Shuffle node locayion, assure that layout error occured
        std::random_shuffle(locations.begin(), locations.end());
        env.Initialize();
        env.Sim(TDuration::Seconds(100));
        cfg = env.FetchBaseConfig();
        CheckBaseConfigLayout(geom, cfg, error);
        Cerr << error << Endl;

        // Sanitize groups
        env.Runtime->FilterFunction = {};
        env.UpdateSettings(true, false, true);
        env.Sim(TDuration::Minutes(15));
        cfg = env.FetchBaseConfig();
        UNIT_ASSERT_C(CheckBaseConfigLayout(geom, cfg, error), error);

        // Assure that sanitizer doesn't send request to fully sane groups
        env.Runtime->FilterFunction = catchSanitizeRequests;
        env.Sim(TDuration::Minutes(3));
        cfg = env.FetchBaseConfig();
        UNIT_ASSERT_C(CheckBaseConfigLayout(geom, cfg, error), error);
    }
}
