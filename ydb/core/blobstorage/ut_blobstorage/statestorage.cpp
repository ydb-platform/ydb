#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_events.h>
#include <library/cpp/testing/unittest/registar.h>
#include <google/protobuf/util/json_util.h>

Y_UNIT_TEST_SUITE(TStateStorageRingGroupState) {
    class StateStorageTest {
        TEnvironmentSetup Env;
        TTestActorSystem& Runtime;
        
        public:
        StateStorageTest()
            : Env{{
                    .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
                    .SelfManagementConfig = true,
                }}
            , Runtime(*Env.Runtime)
        {
            Runtime.SetLogPriority(NKikimrServices::BS_NODE, NLog::PRI_DEBUG);
        }

        TAutoPtr<TEventHandle<NKikimr::NStorage::TEvNodeConfigInvokeOnRootResult>> SendRequest(const TString &cfg) {
            ui32 retry = 0;
            while(retry++ < 5) {
                Env.Sim(TDuration::Seconds(10));
                auto ev = std::make_unique<NKikimr::NStorage::TEvNodeConfigInvokeOnRoot>();
                const auto status = google::protobuf::util::JsonStringToMessage(cfg, &ev->Record);
                UNIT_ASSERT(status.ok());
                TActorId edge = Runtime.AllocateEdgeActor(1);
                const TActorId wardenId = MakeBlobStorageNodeWardenID(1);
                Runtime.WrapInActorContext(edge, [&] {
                    Runtime.Send(new IEventHandle(wardenId, edge, ev.release(), IEventHandle::FlagTrackDelivery));
                });
                auto res = Env.WaitForEdgeActorEvent<NKikimr::NStorage::TEvNodeConfigInvokeOnRootResult>(edge);
                if(res->Get()->Record.GetStatus() == NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK) {
                    return res;
                } else {
                    Cerr << "BadResponse: " << res->Get()->Record << Endl;
                }
            }
            UNIT_ASSERT(false);
            return nullptr;
        }
    };
    
    Y_UNIT_TEST(TestRingGroupState) {
        StateStorageTest test;
        TString req = "{\"GetStateStorageConfig\": {}}";
        auto res = test.SendRequest(req);
        auto record = res->Get()->Record;

        Cerr << "Response: " << record;
    }
}
