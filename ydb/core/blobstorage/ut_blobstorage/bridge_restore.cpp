#include "bridge_common.h"

#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

#include <util/system/type_name.h>

using namespace NBridgeTest;

namespace {

    // The blob is in pile 0 only, so a read that must restore it first copies it into pile 1
    struct TBlobInOnePile {
        TEnvironmentSetup Env{{
            .NodeCount = 16,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetBridgedLocation,
            .SelfManagementConfig = true,
            .NumPiles = 2,
            .AutomaticBootstrap = true,
        }};
        ui32 GroupId = 0;
        std::vector<ui32> PileGroups;
        const TString Data = "blob to be restored";
        const TLogoBlobID Id = TLogoBlobID(100500, 1, 1, 0, Data.size(), 0);

        TBlobInOnePile() {
            Env.CreatePool();
            Env.Sim(TDuration::Seconds(30));
            GroupId = Env.GetGroups().front();
            PileGroups = GetPileGroups(Env, GroupId);

            auto res = QueryPileGroup<TEvBlobStorage::TEvPutResult>(Env, PileGroups[0], 1,
                new TEvBlobStorage::TEvPut(Id, Data, TInstant::Max()));
            UNIT_ASSERT_VALUES_EQUAL_C(res->Status, NKikimrProto::OK, res->ToString());
            UNIT_ASSERT_VALUES_EQUAL(ReadFromPileGroup(Env, PileGroups[1], 9, Id, Data), NKikimrProto::NODATA);
        }

        bool IsToBridgeProxy(const IEventHandle& ev) {
            IActor *actor = Env.Runtime->GetActor(ev.GetRecipientRewrite());
            return actor && TypeName(*actor).Contains("TBridgedBlobStorageProxyActor");
        }

        // the index-only read restoring the blob first, as the uncertainty resolver of BlobDepot does
        std::unique_ptr<TEvBlobStorage::TEvGetResult> GetRestoringIndexOnly() {
            const TActorId edge = Env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
            Env.Runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, GroupId, new TEvBlobStorage::TEvGet(Id, 0, 0, TInstant::Max(),
                    NKikimrBlobStorage::FastRead, /*mustRestoreFirst=*/ true, /*isIndexOnly=*/ true));
            });
            auto res = Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false);
            return std::unique_ptr<TEvBlobStorage::TEvGetResult>(res->Release().Release());
        }

        // the blob failed to be restored, the reply must still name it, as the caller matches replies by the blob
        void CheckFailedForBlob(const TEvBlobStorage::TEvGetResult& res) {
            UNIT_ASSERT_VALUES_EQUAL_C(res.Status, NKikimrProto::OK, res.ToString());
            UNIT_ASSERT_VALUES_EQUAL(res.ResponseSz, 1);
            const auto& r = res.Responses[0];
            UNIT_ASSERT_VALUES_EQUAL_C(r.Status, NKikimrProto::ERROR, res.ToString());
            UNIT_ASSERT_VALUES_EQUAL_C(r.Id, Id, res.ToString());
            UNIT_ASSERT_VALUES_EQUAL(r.Shift, 0);
            UNIT_ASSERT_VALUES_EQUAL(r.RequestedSize, 0);
        }
    };

} // anonymous

Y_UNIT_TEST_SUITE(BridgeRestore) {

    // The restore put into pile 1 fails
    Y_UNIT_TEST(FailedRestorePutKeepsBlobId) {
        TBlobInOnePile t;

        ui32 numFailed = 0;
        t.Env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvPutResult && ev->HasEvent() && t.IsToBridgeProxy(*ev)) {
                if (auto *msg = ev->Get<TEvBlobStorage::TEvPutResult>(); msg->Id == t.Id) {
                    msg->Status = NKikimrProto::ERROR;
                    msg->ErrorReason = "restore put failed by the test";
                    ++numFailed;
                }
            }
            return true;
        };
        const auto res = t.GetRestoringIndexOnly();
        t.Env.Runtime->FilterFunction = {};
        UNIT_ASSERT(numFailed);
        t.CheckFailedForBlob(*res);
    }

    // The read of the blob content for the restore finds no blob, while the reply to the whole read is OK
    Y_UNIT_TEST(FailedRestoreGetKeepsBlobId) {
        TBlobInOnePile t;

        ui32 numFailed = 0;
        t.Env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvGetResult && ev->HasEvent() && t.IsToBridgeProxy(*ev)) {
                auto *msg = ev->Get<TEvBlobStorage::TEvGetResult>();
                for (ui32 i = 0; i < msg->ResponseSz; ++i) {
                    // only the read for the restore brings the content, the first one is index-only
                    if (auto& r = msg->Responses[i]; r.Id == t.Id && r.Status == NKikimrProto::OK && r.Buffer.size()) {
                        r.Status = NKikimrProto::NODATA;
                        r.Buffer = {};
                        ++numFailed;
                    }
                }
            }
            return true;
        };
        const auto res = t.GetRestoringIndexOnly();
        t.Env.Runtime->FilterFunction = {};
        UNIT_ASSERT(numFailed);
        t.CheckFailedForBlob(*res);
    }

}
