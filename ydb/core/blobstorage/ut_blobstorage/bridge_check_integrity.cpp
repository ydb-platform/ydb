#include "bridge_common.h"
#include "bridge_pile_ops.h"

#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/vdisk/scrub/scrub_actor.h>
#include <ydb/library/testlib/helpers.h>

#include <array>

using namespace NBridgeTest;

namespace {
    enum class ECondition { Healthy, Missing, Corrupt, Error };

    TEnvironmentSetup::TSettings MakeSettings(bool enableDeepScrubbing = false) {
        return {
            .NodeCount = 16,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetBridgedLocation,
            .SelfManagementConfig = true,
            .EnableDeepScrubbing = enableDeepScrubbing,
            .NumPiles = 2,
            .AutomaticBootstrap = true,
        };
    }

    void CheckAllCopies(ECondition condition, bool healthyFirst) {
        TEnvironmentSetup env(MakeSettings());
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

    // a part of bridged group serves the requests fenced with its generation only; the ones addressed to it directly
    // without the fence, as monitoring pages send them, get an error instead of stopping the node
    Y_UNIT_TEST(PartWithoutForcedGeneration) {
        TEnvironmentSetup env(MakeSettings());
        const ui32 groupId = CreatePoolAndGetGroup(env);
        const ui32 partGroupId = GetPileGroups(env, groupId).front(); // the part in the pile of node 1
        const ui32 partGeneration = GetGroupInfo(env, partGroupId).GetGroupGeneration();
        const TString data = "data checked in the part";
        const TLogoBlobID id(100500, 1, 1, 0, data.size(), 0);
        const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        auto send = [&](ui32 target, IEventBase *ev) {
            env.Runtime->WrapInActorContext(edge, [&] { SendToBSProxy(edge, target, ev); });
        };

        send(groupId, new TEvBlobStorage::TEvPut(id, data, TInstant::Max()));
        auto putResult = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false);
        UNIT_ASSERT_VALUES_EQUAL_C(putResult->Get()->Status, NKikimrProto::OK, putResult->Get()->ErrorReason);

        send(partGroupId, new TEvBlobStorage::TEvCheckIntegrity(id, TInstant::Max(), NKikimrBlobStorage::FastRead));
        auto checkResult = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvCheckIntegrityResult>(edge, false);
        UNIT_ASSERT_VALUES_EQUAL_C(checkResult->Get()->Status, NKikimrProto::ERROR, checkResult->Get()->ErrorReason);

        send(partGroupId, new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(), NKikimrBlobStorage::FastRead));
        auto getResult = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false);
        UNIT_ASSERT_VALUES_EQUAL_C(getResult->Get()->Status, NKikimrProto::ERROR, getResult->Get()->ErrorReason);

        send(partGroupId, new TEvBlobStorage::TEvRange(id.TabletID(), TLogoBlobID(id.TabletID(), 0, 0, 0, 0, 0),
            TLogoBlobID(id.TabletID(), Max<ui32>(), Max<ui32>(), TLogoBlobID::MaxChannel, TLogoBlobID::MaxBlobSize,
            TLogoBlobID::MaxCookie), false, TInstant::Max(), true));
        auto rangeResult = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvRangeResult>(edge, false);
        UNIT_ASSERT_VALUES_EQUAL_C(rangeResult->Get()->Status, NKikimrProto::ERROR, rangeResult->Get()->ErrorReason);

        // fenced with the generation of the part, the check of the copy in the part is served
        auto ev = std::make_unique<TEvBlobStorage::TEvCheckIntegrity>(id, TInstant::Max(),
            NKikimrBlobStorage::FastRead);
        ev->ForceGroupGeneration = partGeneration;
        send(partGroupId, ev.release());
        checkResult = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvCheckIntegrityResult>(edge, false);
        UNIT_ASSERT_VALUES_EQUAL_C(checkResult->Get()->Status, NKikimrProto::OK, checkResult->Get()->ErrorReason);
        UNIT_ASSERT(checkResult->Get()->PlacementStatus == TEvBlobStorage::TEvCheckIntegrityResult::PS_OK);
        UNIT_ASSERT(checkResult->Get()->DataStatus == TEvBlobStorage::TEvCheckIntegrityResult::DS_OK);
    }

    // deep scrubbing of a VDisk checks the part of bridged group the VDisk belongs to
    Y_UNIT_TEST(DeepScrubbingOfParts) {
        TEnvironmentSetup env(MakeSettings(true));
        const ui32 groupId = CreatePoolAndGetGroup(env);
        const auto groups = GetPileGroups(env, groupId);
        TDataChecker checker(env);
        for (ui32 i = 0; i < 10; ++i) {
            checker.Write(groupId);
        }
        for (ui32 partGroupId : groups) { // the scrub checks the blobs it finds in SSTs
            const auto info = env.GetGroupInfo(partGroupId);
            for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
                env.CompactVDisk(info->GetActorId(i));
            }
        }

        // successful checks are counted per part, the scrub actors being told apart by the parts they send checks to
        THashMap<TActorId, ui32> scrubParts;
        THashMap<ui32, ui32> numChecked;
        std::vector<TString> failures;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::TEvCheckIntegrity::EventType) {
                for (ui32 partGroupId : groups) {
                    if (ev->Recipient == MakeBlobStorageProxyID(partGroupId)) {
                        scrubParts[ev->Sender] = partGroupId;
                    }
                }
            } else if (ev->GetTypeRewrite() == TEvBlobStorage::TEvCheckIntegrityResult::EventType) {
                const auto *msg = ev->Get<TEvBlobStorage::TEvCheckIntegrityResult>();
                if (msg->Status != NKikimrProto::OK) {
                    failures.push_back(TStringBuilder() << NKikimrProto::EReplyStatus_Name(msg->Status) << ' '
                        << msg->ErrorReason);
                } else if (const auto it = scrubParts.find(ev->GetRecipientRewrite()); it != scrubParts.end()) {
                    ++numChecked[it->second];
                }
            }
            return true;
        };

        env.SetScrubPeriodicity(TDuration::Seconds(1));

        for (ui32 partGroupId : groups) { // wait for a full scrub cycle of every VDisk of both parts
            const auto info = env.GetGroupInfo(partGroupId);
            for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
                const TActorId vdiskActorId = info->GetActorId(i);
                const ui32 nodeId = vdiskActorId.NodeId();
                const TActorId edge = env.Runtime->AllocateEdgeActor(nodeId, __FILE__, __LINE__);
                const TInstant deadline = env.Runtime->GetClock() + TDuration::Minutes(5);
                for (;;) {
                    env.Runtime->Send(new IEventHandle(TEvBlobStorage::EvScrubAwait, 0, vdiskActorId, edge, nullptr,
                        RandomNumber<ui64>()), nodeId);
                    auto ev = env.WaitForEdgeActorEvent<TEvScrubNotify>(edge, false, deadline);
                    UNIT_ASSERT_C(ev, "no successful scrub cycle of VDiskId# " << info->GetVDiskId(i));
                    if (ev->Get()->Success) {
                        break;
                    }
                }
            }
        }
        env.Runtime->FilterFunction = {};

        if (!failures.empty()) {
            UNIT_FAIL(failures.size() << " checks failed, the first one: " << failures.front());
        }
        for (ui32 partGroupId : groups) {
            UNIT_ASSERT_C(numChecked[partGroupId], "no blob of the part checked by deep scrubbing GroupId# "
                << partGroupId);
        }
    }
}
