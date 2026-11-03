#include "bridge_common.h"
#include "bridge_pile_ops.h"

#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/library/testlib/helpers.h>

#include <array>

using namespace NBridgeTest;

namespace {
    enum class ECondition { Healthy, Missing, Corrupt, Error };

    void CheckAllCopies(ECondition condition, bool healthyFirst) {
        TEnvironmentSetup env{{
            .NodeCount = 16,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetBridgedLocation,
            .SelfManagementConfig = true,
            .NumPiles = 2,
            .AutomaticBootstrap = true,
        }};
        const ui32 groupId = CreatePoolAndGetGroup(env);
        const auto groups = GetPileGroups(env, groupId);
        const TLogoBlobID id(100500, 1, 1, 0, 100, 0);
        const auto edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        std::array<std::unique_ptr<IEventHandle>, 2> requests;
        std::unique_ptr<IEventHandle> result;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetRecipientRewrite() == edge) {
                UNIT_ASSERT(!result);
                result = std::move(ev);
                return false;
            }
            if (ev->GetTypeRewrite() == TEvBlobStorage::TEvCheckIntegrity::EventType) {
                for (ui32 pile = 0; pile < 2; ++pile) {
                    if (ev->Recipient == MakeBlobStorageProxyID(groups[pile])) {
                        UNIT_ASSERT(!requests[pile]);
                        requests[pile] = std::move(ev);
                        return false;
                    }
                }
            }
            return true;
        };
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvCheckIntegrity(id, TInstant::Max(),
                NKikimrBlobStorage::FastRead, healthyFirst));
        });
        WaitUntil(env, [&] { return requests[0] && requests[1]; }, "both synchronized copies queried");
        using TResult = TEvBlobStorage::TEvCheckIntegrityResult;
        auto respond = [&](ui32 pile) {
            auto reply = std::make_unique<TResult>(pile && condition == ECondition::Error
                ? NKikimrProto::ERROR : NKikimrProto::OK);
            reply->Id = id;
            reply->Erasure = pile ? TBlobStorageGroupType::ErasureMirror3dc : TBlobStorageGroupType::Erasure4Plus2Block;
            reply->DataInfo = pile ? "second copy diagnostics" : "first copy diagnostics";
            if (pile) {
                switch (condition) {
                    case ECondition::Healthy: break;
                    case ECondition::Missing: reply->PlacementStatus = TResult::PS_BLOB_IS_LOST; break;
                    case ECondition::Corrupt: reply->DataStatus = TResult::DS_ERROR; break;
                    case ECondition::Error: reply->ErrorReason = "check failed"; break;
                }
            }
            const auto& request = requests[pile];
            env.Runtime->Send(new IEventHandle(request->Sender, request->Recipient, reply.release(), 0,
                request->Cookie), request->Sender.NodeId());
            env.Sim(TDuration::MilliSeconds(100));
        };
        respond(healthyFirst ? 0 : 1);
        if (healthyFirst || condition != ECondition::Error) {
            UNIT_ASSERT_C(!result, "a successful check must wait for every synchronized pile");
        }
        respond(healthyFirst ? 1 : 0);
        UNIT_ASSERT(result);
        const auto *response = result->Get<TResult>();
        UNIT_ASSERT_VALUES_EQUAL(response->Id, id);
        if (condition == ECondition::Error) {
            UNIT_ASSERT_VALUES_EQUAL(response->Status, NKikimrProto::ERROR);
            UNIT_ASSERT_STRING_CONTAINS(response->ErrorReason, "check failed");
            UNIT_ASSERT_STRING_CONTAINS(response->ErrorReason, "BridgePileId#");
        } else {
            UNIT_ASSERT_VALUES_EQUAL(response->Status, NKikimrProto::OK);
            UNIT_ASSERT(response->PlacementStatus ==
                (condition == ECondition::Missing ? TResult::PS_BLOB_IS_LOST : TResult::PS_OK));
            UNIT_ASSERT(response->DataStatus ==
                (condition == ECondition::Corrupt ? TResult::DS_ERROR : TResult::DS_OK));
            UNIT_ASSERT_VALUES_EQUAL(response->Erasure, TBlobStorageGroupType::Erasure4Plus2Block);
            UNIT_ASSERT_STRING_CONTAINS(response->DataInfo, "first copy diagnostics");
            UNIT_ASSERT_STRING_CONTAINS(response->DataInfo, "second copy diagnostics");
            UNIT_ASSERT_STRING_CONTAINS(response->DataInfo, "BridgePileId#");
            UNIT_ASSERT_VALUES_EQUAL(response->DataInfo.Contains('\n'), !healthyFirst);
        }
        env.Runtime->FilterFunction = {};
    }
}

Y_UNIT_TEST_SUITE(BridgeCheckIntegrity) {
    Y_UNIT_TEST_TWIN(HealthyCopies, healthyFirst) { CheckAllCopies(ECondition::Healthy, healthyFirst); }
    Y_UNIT_TEST_TWIN(MissingCopy, healthyFirst) { CheckAllCopies(ECondition::Missing, healthyFirst); }
    Y_UNIT_TEST_TWIN(CorruptCopy, healthyFirst) { CheckAllCopies(ECondition::Corrupt, healthyFirst); }
    Y_UNIT_TEST_TWIN(FailedCheck, healthyFirst) { CheckAllCopies(ECondition::Error, healthyFirst); }
}
