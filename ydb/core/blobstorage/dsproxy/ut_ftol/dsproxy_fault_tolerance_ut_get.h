#pragma once

#include "defs.h"
#include "dsproxy_fault_tolerance_ut_base.h"

namespace NKikimr {

class TGetWithRecoverFaultToleranceTest : public TFaultToleranceTestBase<TGetWithRecoverFaultToleranceTest> {
public:
    using TFaultToleranceTestBase::TFaultToleranceTestBase;

    void RunTestAction() {
        ui32 index = 1;

        for (const TEvBlobStorage::TEvPut::ETactic tactic : {TEvBlobStorage::TEvPut::TacticMinLatency,
                TEvBlobStorage::TEvPut::TacticMaxThroughput}) {
            TString data = "test";
            TLogoBlobID id(1, index++, 1, 1, data.size(), 0);

            // send put request
            Put(id, data, NKikimrProto::OK, tactic);

            // check for correctness
            CheckBlob(id, false, NKikimrProto::OK, data);

            // permute any combinations of faulty disks and check for blob recovery
            for (const auto& disks : FaultsFittingFailModel) {
                if (Info->GetQuorumChecker().CheckFailModelForGroup(disks)) {
                    Delete(id, disks, false);
                    CheckBlob(id, true, NKikimrProto::OK, data);
                }
            }

            // do the same, but for index restore on get query
            for (const auto& disks : FaultsFittingFailModel) {
                if (Info->GetQuorumChecker().CheckFailModelForGroup(disks)) {
                    Delete(id, disks, false);
                    CheckBlob(id, true, NKikimrProto::OK, TString());
                }
            }

            // check for correctness once again
            CheckBlob(id, false, NKikimrProto::OK, data);

            // now create bunch of blobs and check index restore get for them
            TVector<TLogoBlobID> ids;
            for (ui32 i = 0; i < 100000; ++i) {
                TLogoBlobID id(1, index++, 1, 1, data.size(), 0);
                Put(id, data, NKikimrProto::OK, tactic);
                ids.push_back(id);
            }

            TArrayHolder<TEvBlobStorage::TEvGet::TQuery> items(new TEvBlobStorage::TEvGet::TQuery[ids.size()]);
            for (ui32 i = 0; i < ids.size(); ++i) {
                items[i].Set(ids[i]);
            }

            SendToBSProxy(GetActorContext(), Info->GroupID, new TEvBlobStorage::TEvGet(items, ids.size(), TInstant::Max(),
                NKikimrBlobStorage::FastRead, true, true, TEvBlobStorage::TEvGet::TForceBlockTabletData(1, index)));
            auto resp = WaitForSpecificEvent<TEvBlobStorage::TEvGetResult>(&TGetWithRecoverFaultToleranceTest::ProcessUnexpectedEvent);
            TEvBlobStorage::TEvGetResult *msg = resp->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(msg->ResponseSz, ids.size());
            for (ui32 i = 0; i < ids.size(); ++i) {
                const TEvBlobStorage::TEvGetResult::TResponse& item = msg->Responses[i];
                UNIT_ASSERT_VALUES_EQUAL(item.Id, ids[i]);
                UNIT_ASSERT_VALUES_EQUAL(item.Status, NKikimrProto::OK);
            }
        }
    }
};

} // NKikimr
