#include <library/cpp/testing/unittest/registar.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils/test_server.h>
#include <ydb/core/persqueue/events/global.h>

namespace NKikimr::NPQ {
using namespace NPersQueue;

Y_UNIT_TEST_SUITE(TPQRBDescribes) {

    Y_UNIT_TEST(PartitionLocations) {
        
        NPersQueue::TTestServer server;
        TString topicName = "rt3.dc1--topic";
        TString topicPath = TString("/Root/PQ/") + topicName;
        ui32 totalPartitions = 5;
        server.AnnoyingClient->CreateTopic(topicName, totalPartitions);

        auto pathDescr = server.AnnoyingClient->Ls(topicPath)->Record.GetPathDescription().GetSelf();

        ui64 balancerTabletId = pathDescr.GetBalancerTabletID();
        auto* runtime = server.CleverServer->GetRuntime();
        const auto edge = runtime->AllocateEdgeActor();
        
        auto checkResponse = [&](TEvPersQueue::TEvGetPartitionsLocation* request, bool ok, ui64 partitionsCount = 0) {
            runtime->SendToPipe(balancerTabletId, edge, request);
            auto ev = runtime->GrabEdgeEvent<TEvPersQueue::TEvGetPartitionsLocationResponse>();
            const auto& response = ev->Record;
            Cerr << "response: " << response.DebugString();
            
            UNIT_ASSERT(response.GetStatus() == ok);
            if (!ok) {
                return ev;
            }
            UNIT_ASSERT_VALUES_EQUAL(response.LocationsSize(), partitionsCount);
            THashSet<ui32> partitionsFound;
            for (const auto& partitionInResponse : response.GetLocations()) {
                auto res = partitionsFound.insert(partitionInResponse.GetPartitionId());
                UNIT_ASSERT(res.second);
                UNIT_ASSERT_LT(partitionInResponse.GetPartitionId(), totalPartitions);
                UNIT_ASSERT(partitionInResponse.GetNodeId() > 0);
            }
            return ev;
        };
        auto pollBalancer = [&] (ui64 retriesCount) {
            auto waitTime = TDuration::MilliSeconds(500);
            while (retriesCount) {
                auto* req = new TEvPersQueue::TEvGetPartitionsLocation();
                runtime->SendToPipe(balancerTabletId, edge, req);
                auto ev = runtime->GrabEdgeEvent<TEvPersQueue::TEvGetPartitionsLocationResponse>();
                if (!ev->Record.GetStatus()) {
                    --retriesCount;
                    Sleep(waitTime);
                    waitTime *= 2;
                } else {
                    return;
                }
            }
            UNIT_FAIL("Could not get positive response from balancer"); 

        };
        pollBalancer(5);
        checkResponse(new TEvPersQueue::TEvGetPartitionsLocation(), true, totalPartitions);
        {
            auto* req = new TEvPersQueue::TEvGetPartitionsLocation();
            req->Record.AddPartitions(3);
            auto resp = checkResponse(req, true, 1);
            UNIT_ASSERT_VALUES_EQUAL(resp->Record.GetLocations(0).GetPartitionId(), 3);
        }
        {
            auto* req = new TEvPersQueue::TEvGetPartitionsLocation();
            req->Record.AddPartitions(50);
            checkResponse(req, false);
        }
    
    }
};

}
