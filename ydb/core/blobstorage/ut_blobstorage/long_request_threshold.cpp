#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

#include <util/stream/str.h>

Y_UNIT_TEST_SUITE(LongRequestThreshold) {
    Y_UNIT_TEST(IcbUpdatesAffectExistingProxy) {
        TStringStream log;
        TEnvironmentSetup env({
            .NodeCount = 1,
            .Erasure = TBlobStorageGroupType::ErasureNone,
        });
        env.Runtime->LogStream = &log;
        env.Runtime->SetLogPriority(NKikimrServices::BS_PROXY_GET, NLog::PRI_WARN);
        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Minutes(1));

        const auto groups = env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
        const ui32 groupId = groups.front();
        const TActorId edge = env.Runtime->AllocateEdgeActor(1);
        const TString data = "long request threshold test";
        const TLogoBlobID blobId(1, 1, 1, 0, data.size(), 0);

        // Create the proxy before updating its ICB control.
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvPut(blobId, data, TInstant::Max()));
        });
        auto put = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false);
        UNIT_ASSERT_VALUES_EQUAL(put->Get()->Status, NKikimrProto::OK);

        auto checkGet = [&](ui64 thresholdMs, bool expectWarning) {
            TAtomic previousValue;
            auto icb = env.Runtime->GetNode(1)->AppData->Icb;
            icb->SetValue("DSProxyControls.LongRequestThresholdMs", thresholdMs, previousValue);
            TAtomic currentValue = 0;
            bool exists = false;
            icb->GetValue("DSProxyControls.LongRequestThresholdMs", currentValue, exists);
            UNIT_ASSERT(exists);
            UNIT_ASSERT_VALUES_EQUAL(currentValue, thresholdMs);
            // TMemorizableControlWrapper caches the value for up to 15 seconds.
            env.Sim(TDuration::Seconds(16));
            log.Clear();

            std::vector<std::unique_ptr<IEventHandle>> delayed;
            env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
                if (ev->GetTypeRewrite() == TEvBlobStorage::TEvVGetResult::EventType) {
                    delayed.push_back(std::move(ev));
                    return false;
                }
                return true;
            };
            env.Runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvGet(
                    blobId, 0, data.size(), TInstant::Max(), NKikimrBlobStorage::FastRead));
            });
            env.Sim(TDuration::MilliSeconds(200));
            env.Runtime->FilterFunction = {};
            UNIT_ASSERT(!delayed.empty());
            for (auto& ev : delayed) {
                const ui32 nodeId = ev->GetRecipientRewrite().NodeId();
                env.Runtime->Send(ev.release(), nodeId);
            }

            auto get = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL(get->Get()->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(get->Get()->ResponseSz, 1);
            UNIT_ASSERT_VALUES_EQUAL(get->Get()->Responses[0].Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(get->Get()->Responses[0].Buffer.ConvertToString(), data);
            UNIT_ASSERT_VALUES_EQUAL_C(log.Str().Contains("BPG71"), expectWarning, log.Str());
        };

        checkGet(1000, false);
        checkGet(100, true);
    }
}
