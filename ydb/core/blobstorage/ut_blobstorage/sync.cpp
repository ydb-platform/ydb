#include <ydb/core/blobstorage/ut_blobstorage/lib/ut_helpers.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_private_events.h>
#include <ydb/core/blobstorage/vdisk/repl/blobstorage_repl.h>
#include <ydb/core/blobstorage/vdisk/anubis_osiris/blobstorage_osiris.h>
#include <ydb/core/blobstorage/vdisk/syncer/blobstorage_syncer_localwriter.h>
#include <ydb/core/blobstorage/vdisk/syncer/blobstorage_syncer_recoverlostdata_proxy.h>
#include <ydb/core/blobstorage/vdisk/syncer/guid_proxyobtain.h>
#include <ydb/core/blobstorage/vdisk/syncer/guid_proxywrite.h>
#include <ydb/core/blobstorage/vdisk/syncer/blobstorage_syncer_committer.h>
#include <ydb/core/blobstorage/vdisk/syncer/syncer_job_actor.h>
#include <ydb/core/blobstorage/vdisk/syncer/syncer_job_task.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclog_private_events.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclog_public_events.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclogkeeper_committer.h>
#include <util/random/random.h>
#include <deque>
#include <utility>

namespace {

void WriteLostDataCopies(TEnvironmentSetup& env, const TIntrusivePtr<TBlobStorageGroupInfo>& info,
        ui32 node, ui32 realm, const TLogoBlobID& blobId, const TString& data) {
    for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
        const auto vdisk = info->GetVDiskId(i);
        if (vdisk.FailRealm != realm) {
            continue;
        }
        const ui32 part = info->GetIdxInSubgroup(vdisk, blobId.Hash()) % 3 + 1;
        const auto sender = env.Runtime->AllocateEdgeActor(node);
        env.Runtime->Send(new IEventHandle(info->GetActorId(i), sender,
            new TEvBlobStorage::TEvVPut(TLogoBlobID(blobId, part), TRope(data), vdisk,
                false, nullptr, TInstant::Max(), NKikimrBlobStorage::TabletLog, false)), node);
        const auto result = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVPutResult>(sender, true,
            env.Runtime->GetClock() + TDuration::Seconds(30));
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetStatus(), NKikimrProto::OK);
    }
}

void CheckLostDataPayload(TEnvironmentSetup& env, const TIntrusivePtr<TBlobStorageGroupInfo>& info,
        ui32 node, ui32 index, const TLogoBlobID& blobId, const TString& data) {
    const auto sender = env.Runtime->AllocateEdgeActor(node);
    env.Runtime->Send(new IEventHandle(info->GetActorId(index), sender,
        TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(info->GetVDiskId(index),
            env.Runtime->GetClock() + TDuration::Seconds(30), NKikimrBlobStorage::FastRead,
            TEvBlobStorage::TEvVGet::EFlags::None, {}, {{blobId}}).release()), node);
    const auto result = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVGetResult>(sender, true,
        env.Runtime->GetClock() + TDuration::Seconds(30));
    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetStatus(), NKikimrProto::OK);
    UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetResult().size(), 1);
    const auto& item = result->Get()->Record.GetResult(0);
    UNIT_ASSERT_VALUES_EQUAL(item.GetStatus(), NKikimrProto::OK);
    UNIT_ASSERT_VALUES_EQUAL(result->Get()->GetBlobData(item).ConvertToString(), data);

}

} // anonymous namespace

Y_UNIT_TEST_SUITE(BlobStorageSync) {

    Y_UNIT_TEST(SingleDcReplicationReplanCoalescesUpdates) {
        TEnvironmentSetup env{{.NodeCount = 9, .Erasure = TBlobStorageGroupType::ErasureMirror3dc}};
        TActorId scheduler;
        bool armed = false;
        bool holdFinish = false;
        ui32 scans = 0;
        ui64 oldWakeupTag = 0;
        std::unique_ptr<IEventHandle> finish;
        TActorId heldFinishSender;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvReplFinished::EventType
                    && event->GetRecipientRewrite().NodeId() == 2) {
                if (!armed) {
                    scheduler = event->GetRecipientRewrite();
                } else if (event->GetRecipientRewrite() == scheduler
                        && event->Sender != heldFinishSender) {
                    ++scans;
                }
            }
            if (armed && event->GetRecipientRewrite() == scheduler) {
                if (event->GetTypeRewrite() == TEvents::TEvWakeup::EventType) {
                    oldWakeupTag = event->Get<TEvents::TEvWakeup>()->Tag;
                }
                if (holdFinish && event->GetTypeRewrite() == TEvReplFinished::EventType) {
                    UNIT_ASSERT(!finish);
                    heldFinishSender = event->Sender;
                    finish = std::move(event);
                    return false;
                }
            }
            return true;
        };
        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Minutes(2));
        UNIT_ASSERT(scheduler);
        armed = true;
        holdFinish = true;
        const auto edge = env.Runtime->AllocateEdgeActor(2);
        auto update = [&] {
            env.Runtime->Send(new IEventHandle(TEvBlobStorage::EvCommenceRepl, 0,
                scheduler, edge, nullptr, 1), 2);
        };
        for (ui32 i = 0; i < 100; ++i) {
            update();
        }
        env.Sim(TDuration::Seconds(59));
        UNIT_ASSERT_VALUES_EQUAL(scans, 0);
        env.Sim(TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(scans, 1);
        UNIT_ASSERT(finish);
        UNIT_ASSERT(oldWakeupTag);
        for (ui32 i = 0; i < 100; ++i) {
            update();
        }
        env.Runtime->Send(new IEventHandle(scheduler, edge,
            new TEvents::TEvWakeup(oldWakeupTag)), 2);
        env.Sim(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(scans, 1);
        holdFinish = false;
        env.Runtime->Send(finish.release(), 2);
        env.Sim(TDuration::Minutes(2));
        UNIT_ASSERT_VALUES_EQUAL(scans, 2);
        env.Runtime->FilterFunction = {};
    }


    Y_UNIT_TEST(SingleDcModeChangesDuringLostData) {
        TEnvironmentSetup env{{.NodeCount = 9, .Erasure = TBlobStorageGroupType::ErasureMirror3dc}};
        env.CreateBoxAndPool(1, 1);
        const ui32 groupId = env.GetGroups().front();
        auto info = env.GetGroupInfo(groupId);
        env.Sim(TDuration::Minutes(2));
        ui32 index = 0;
        while (info->GetActorId(index).NodeId() == env.Settings.ControllerNodeId) {
            ++index;
        }
        const ui32 node = info->GetActorId(index).NodeId();
        const ui32 realm = info->GetVDiskId(index).FailRealm;
        const TString data = "LostData mode-change payload";
        const TLogoBlobID blobId(5001, 1, 1, 0, data.size(), 0);
        WriteLostDataCopies(env, info, node, realm, blobId, data);
        env.Sim(TDuration::Minutes(2));

        std::vector<std::unique_ptr<IEventHandle>> oldReplies, newReplies;
        bool changed = false;
        bool release = false;
        ui32 newRequests = 0;
        ui32 newGeneration = 0;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& event) {
            if (!release && event->GetTypeRewrite() == TEvSyncerFullSyncedWithPeer::EventType
                    && event->GetRecipientRewrite().NodeId() == node) {
                (changed ? newReplies : oldReplies).push_back(std::move(event));
                return false;
            }
            if (newGeneration && event->GetTypeRewrite() == TEvBlobStorage::TEvVSyncFull::EventType) {
                const auto& record = event->Get<TEvBlobStorage::TEvVSyncFull>()->Record;
                const auto source = VDiskIDFromVDiskID(record.GetSourceVDiskID());
                const auto target = VDiskIDFromVDiskID(record.GetTargetVDiskID());
                if (source.GroupID.GetRawId() == groupId && source.GroupGeneration > newGeneration
                        && event->Sender.NodeId() == node) {
                    ++newRequests;
                    UNIT_ASSERT_VALUES_EQUAL(source.FailRealm, realm);
                    UNIT_ASSERT_VALUES_EQUAL(target.FailRealm, realm);
                }
            }
            return true;
        };
        NKikimrBlobStorage::TConfigRequest query;
        query.AddCommand()->MutableQueryBaseConfig();
        const auto config = env.Invoke(query);
        UNIT_ASSERT(config.GetSuccess());
        bool wiped = false;
        for (const auto& slot : config.GetStatus(0).GetBaseConfig().GetVSlot()) {
            if (slot.GetGroupId() == groupId && slot.GetVSlotId().GetNodeId() == node) {
                const auto& id = slot.GetVSlotId();
                env.Wipe(node, id.GetPDiskId(), id.GetVSlotId(), info->GetVDiskId(index));
                wiped = true;
                break;
            }
        }
        UNIT_ASSERT(wiped);
        env.Sim(TDuration::Seconds(30));
        UNIT_ASSERT_C(oldReplies.size() >= 2, "LostData full sync was not held");
        info = env.GetGroupInfo(groupId);
        NKikimrBlobStorage::TConfigRequest request;
        auto* command = request.AddCommand()->MutableSetGroupSingleDcMode();
        command->SetGroupId(groupId);
        command->SetGroupGeneration(info->GroupGeneration);
        command->SetEnableSingleDcMode(true);
        command->SetSurvivingDc(realm);
        newGeneration = info->GroupGeneration;
        changed = true;
        const auto response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        info = env.GetGroupInfo(groupId);
        env.Sim(TDuration::Seconds(30));
        UNIT_ASSERT(newRequests);
        UNIT_ASSERT_C(newReplies.size() >= 2, "New LostData quorum was not held");
        auto status = [&] {
            const auto edge = env.Runtime->AllocateEdgeActor(node);
            env.Runtime->Send(new IEventHandle(info->GetActorId(index), edge,
                new TEvBlobStorage::TEvVStatus(info->GetVDiskId(index))), node);
            auto result = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVStatusResult>(edge, true,
                env.Runtime->GetClock() + TDuration::Seconds(30));
            UNIT_ASSERT(result);
            return result->Get()->Record.GetStatus();
        };
        for (auto& reply : oldReplies) {
            env.Runtime->Send(reply.release(), node);
        }
        env.Sim(TDuration::Seconds(1));
        UNIT_ASSERT_C(status() != NKikimrProto::OK, "Old full sync replies completed recovery");
        release = true;
        for (auto& reply : newReplies) {
            env.Runtime->Send(reply.release(), node);
        }
        env.Sim(TDuration::Minutes(1));
        UNIT_ASSERT_VALUES_EQUAL(status(), NKikimrProto::OK);
        env.Sim(TDuration::Minutes(2));
        CheckLostDataPayload(env, info, node, index, blobId, data);
        env.Runtime->FilterFunction = {};
    }


    Y_UNIT_TEST(SingleDcModeChangesDuringLostDataCommit) {
        for (const ui32 phase : {0u, 1u, 2u}) {
            TEnvironmentSetup env{{.NodeCount = 9, .Erasure = TBlobStorageGroupType::ErasureMirror3dc}};
            env.CreateBoxAndPool(1, 1);
            const ui32 groupId = env.GetGroups().front();
            auto info = env.GetGroupInfo(groupId);
            env.Sim(TDuration::Minutes(2));
            ui32 index = 0;
            while (info->GetActorId(index).NodeId() == env.Settings.ControllerNodeId) {
                ++index;
            }
            const ui32 node = info->GetActorId(index).NodeId();
            const ui32 realm = info->GetVDiskId(index).FailRealm;
            const TString data = "LostData mode-change payload";
            const TLogoBlobID blobId(5001, 1, 1, 0, data.size(), 0);
            WriteLostDataCopies(env, info, node, realm, blobId, data);
            env.Sim(TDuration::Minutes(2));

            TActorId recoveryActor;
            bool localCommit = false;
            std::vector<std::unique_ptr<IEventHandle>> acknowledgements;
            std::vector<TActorId> recoveryProxies;
            bool release = false;
            ui32 newRequests = 0;
            ui32 newGeneration = 0;
            env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& event) {
                if (phase == 2 && event->GetTypeRewrite() == TEvSyncerJobDone::EventType
                        && event->GetRecipientRewrite().NodeId() == node
                        && event->Get<TEvSyncerJobDone>()->Task->IsFullRecoveryTask()) {
                    recoveryProxies.push_back(event->GetRecipientRewrite());
                }

                if (event->GetTypeRewrite() == TEvSyncerFullSyncedWithPeer::EventType
                        && event->GetRecipientRewrite().NodeId() == node) {
                    recoveryActor = event->GetRecipientRewrite();
                }
                if (recoveryActor && event->Sender == recoveryActor
                        && event->GetTypeRewrite() == TEvSyncerCommit::EventType
                        && event->Get<TEvSyncerCommit>()->Modif == TEvSyncerCommit::ELocalGuid) {
                    localCommit = true;
                }
                const bool remoteCommit = phase == 2
                    && std::find(recoveryProxies.begin(), recoveryProxies.end(),
                        event->GetRecipientRewrite()) != recoveryProxies.end();
                if (!release && ((remoteCommit
                        && event->GetTypeRewrite() == TEvSyncerCommitDone::EventType)
                    || (recoveryActor && event->GetRecipientRewrite() == recoveryActor
                        && ((phase == 1 && event->GetTypeRewrite() == TEvOsirisDone::EventType)
                            || (phase == 0 && localCommit
                                && event->GetTypeRewrite() == TEvSyncerCommitDone::EventType))))) {
                    acknowledgements.push_back(std::move(event));
                    return false;
                }
                if (newGeneration && event->GetTypeRewrite() == TEvBlobStorage::TEvVSyncFull::EventType) {
                    const auto& record = event->Get<TEvBlobStorage::TEvVSyncFull>()->Record;
                    const auto source = VDiskIDFromVDiskID(record.GetSourceVDiskID());
                    const auto target = VDiskIDFromVDiskID(record.GetTargetVDiskID());
                    if (source.GroupID.GetRawId() == groupId && source.GroupGeneration > newGeneration
                            && event->Sender.NodeId() == node) {
                        ++newRequests;
                        UNIT_ASSERT_VALUES_EQUAL(source.FailRealm, realm);
                        UNIT_ASSERT_VALUES_EQUAL(target.FailRealm, realm);
                    }
                }
                return true;
            };
            NKikimrBlobStorage::TConfigRequest query;
            query.AddCommand()->MutableQueryBaseConfig();
            const auto config = env.Invoke(query);
            UNIT_ASSERT(config.GetSuccess());
            bool wiped = false;
            for (const auto& slot : config.GetStatus(0).GetBaseConfig().GetVSlot()) {
                if (slot.GetGroupId() == groupId && slot.GetVSlotId().GetNodeId() == node) {
                    const auto& id = slot.GetVSlotId();
                    env.Wipe(node, id.GetPDiskId(), id.GetVSlotId(), info->GetVDiskId(index));
                    wiped = true;
                    break;
                }
            }
            UNIT_ASSERT(wiped);
            env.Sim(TDuration::Seconds(30));
            UNIT_ASSERT_C(!acknowledgements.empty(), "LostData completion acknowledgement was not held");
            info = env.GetGroupInfo(groupId);
            NKikimrBlobStorage::TConfigRequest request;
            auto* command = request.AddCommand()->MutableSetGroupSingleDcMode();
            command->SetGroupId(groupId);
            command->SetGroupGeneration(info->GroupGeneration);
            command->SetEnableSingleDcMode(true);
            command->SetSurvivingDc(realm);
            newGeneration = info->GroupGeneration;
            const auto response = env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
            info = env.GetGroupInfo(groupId);
            env.Sim(TDuration::Seconds(30));
            UNIT_ASSERT_VALUES_EQUAL(newRequests, 0);
            auto status = [&] {
                const auto edge = env.Runtime->AllocateEdgeActor(node);
                env.Runtime->Send(new IEventHandle(info->GetActorId(index), edge,
                    new TEvBlobStorage::TEvVStatus(info->GetVDiskId(index))), node);
                auto result = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVStatusResult>(edge, true,
                    env.Runtime->GetClock() + TDuration::Seconds(30));
                UNIT_ASSERT(result);
                return result->Get()->Record.GetStatus();
            };
            UNIT_ASSERT_C(status() != NKikimrProto::OK, "Recovery finished before the local commit acknowledgement");
            release = true;
            for (auto& acknowledgement : acknowledgements) {
                env.Runtime->Send(acknowledgement.release(), node);
            }
            env.Sim(TDuration::Minutes(1));
            UNIT_ASSERT_C(newRequests >= 2, "LostData did not rebuild the selected quorum after the local commit");
            UNIT_ASSERT_VALUES_EQUAL(status(), NKikimrProto::OK);
            env.Sim(TDuration::Minutes(2));
            const auto sender = env.Runtime->AllocateEdgeActor(node);
            env.Runtime->Send(new IEventHandle(info->GetActorId(index), sender,
                TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(info->GetVDiskId(index),
                    env.Runtime->GetClock() + TDuration::Seconds(30), NKikimrBlobStorage::FastRead,
                    TEvBlobStorage::TEvVGet::EFlags::None, {}, {{blobId}}).release()), node);
            const auto result = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVGetResult>(sender, true,
                env.Runtime->GetClock() + TDuration::Seconds(30));
            UNIT_ASSERT(result);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetStatus(), NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetResult().size(), 1);
            const auto& item = result->Get()->Record.GetResult(0);
            UNIT_ASSERT_VALUES_EQUAL(item.GetStatus(), NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->GetBlobData(item).ConvertToString(), data);

            env.Runtime->FilterFunction = {};
        }
    }


    Y_UNIT_TEST(SingleDcModeChangesDuringLocalGuidCommit) {
        for (const auto localState : {NKikimrBlobStorage::TLocalGuidInfo::Selected,
                NKikimrBlobStorage::TLocalGuidInfo::Final}) {
            TEnvironmentSetup env{{.NodeCount = 9, .Erasure = TBlobStorageGroupType::ErasureMirror3dc}};
            const ui32 node = 2;
            TActorId writer;
            bool waitingForCommit = false;
            bool acknowledgementReleased = false;
            std::unique_ptr<IEventHandle> acknowledgement;
            ui32 newQuorumReplies = 0;
            ui32 originalGeneration = 0;
            ui32 realm = 0;
            env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& event) {
                if (event->GetTypeRewrite() == TEvVDiskGuidWritten::EventType
                        && event->GetRecipientRewrite().NodeId() == node) {
                    const auto& msg = *event->Get<TEvVDiskGuidWritten>();
                    if (msg.VDiskId.GroupID.GetRawId() >= 0x80000000) {
                        writer = event->GetRecipientRewrite();
                        if (originalGeneration && msg.VDiskId.GroupGeneration > originalGeneration) {
                            ++newQuorumReplies;
                            UNIT_ASSERT_VALUES_EQUAL(msg.VDiskId.FailRealm, realm);
                        }
                    }
                }
                if (writer && event->Sender == writer
                        && event->GetTypeRewrite() == TEvSyncerCommit::EventType
                        && event->Get<TEvSyncerCommit>()->Modif == TEvSyncerCommit::ELocalGuid
                        && event->Get<TEvSyncerCommit>()->LocalGuidInfo.GetState() == localState
                        && !acknowledgementReleased) {
                    waitingForCommit = true;
                }
                if (waitingForCommit && !acknowledgementReleased
                        && event->GetTypeRewrite() == TEvSyncerCommitDone::EventType
                        && event->GetRecipientRewrite() == writer) {
                    UNIT_ASSERT(!acknowledgement);
                    acknowledgement = std::move(event);
                    return false;
                }
                return true;
            };
            env.CreateBoxAndPool(1, 1);
            const ui32 groupId = env.GetGroups().front();
            auto info = env.GetGroupInfo(groupId);
            ui32 index = 0;
            while (info->GetActorId(index).NodeId() != node) {
                ++index;
            }
            originalGeneration = info->GroupGeneration;
            realm = info->GetVDiskId(index).FailRealm;
            env.Sim(TDuration::Seconds(10));
            UNIT_ASSERT_C(acknowledgement, "The local GUID commit acknowledgement was not held");
            NKikimrBlobStorage::TConfigRequest request;
            auto* command = request.AddCommand()->MutableSetGroupSingleDcMode();
            command->SetGroupId(groupId);
            command->SetGroupGeneration(info->GroupGeneration);
            command->SetEnableSingleDcMode(true);
            command->SetSurvivingDc(realm);
            const auto response = env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
            info = env.GetGroupInfo(groupId);
            env.Sim(TDuration::Seconds(10));
            UNIT_ASSERT_VALUES_EQUAL(newQuorumReplies, 0);
            acknowledgementReleased = true;
            env.Runtime->Send(acknowledgement.release(), node);
            env.Sim(TDuration::Seconds(30));
            UNIT_ASSERT_C(newQuorumReplies >= 2, "GUID was not confirmed on the new quorum after the local commit");
            const auto edge = env.Runtime->AllocateEdgeActor(node);
            env.Runtime->Send(new IEventHandle(info->GetActorId(index), edge,
                new TEvBlobStorage::TEvVStatus(info->GetVDiskId(index))), node);
            const auto result = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVStatusResult>(edge, true,
                env.Runtime->GetClock() + TDuration::Seconds(30));
            UNIT_ASSERT(result);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetStatus(), NKikimrProto::OK);
            env.Runtime->FilterFunction = {};
        }
    }

    Y_UNIT_TEST(SingleDcModeChangesDuringGuidWrite) {
        TEnvironmentSetup env{{.NodeCount = 9, .Erasure = TBlobStorageGroupType::ErasureMirror3dc}};
        const ui32 node = 2;
        std::vector<std::unique_ptr<IEventHandle>> oldReplies;
        std::vector<std::unique_ptr<IEventHandle>> newReplies;
        bool policyChanged = false;
        bool allowReplies = false;
        bool oldRepliesReleased = false;
        bool selectedCommittedFromOldReplies = false;
        TActorId writer;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvVDiskGuidWritten::EventType
                    && event->GetRecipientRewrite().NodeId() == node
                    && event->Get<TEvVDiskGuidWritten>()->VDiskId.GroupID.GetRawId() >= 0x80000000
                    && !allowReplies) {
                writer = event->GetRecipientRewrite();
                (policyChanged ? newReplies : oldReplies).push_back(std::move(event));
                return false;
            }
            if (oldRepliesReleased && !allowReplies && event->Sender == writer
                    && event->GetTypeRewrite() == TEvSyncerCommit::EventType
                    && event->Get<TEvSyncerCommit>()->LocalGuidInfo.GetState()
                        == NKikimrBlobStorage::TLocalGuidInfo::Selected) {
                selectedCommittedFromOldReplies = true;
            }
            return true;
        };
        env.CreateBoxAndPool(1, 1);
        const ui32 groupId = env.GetGroups().front();
        auto info = env.GetGroupInfo(groupId);
        ui32 index = 0;
        while (info->GetActorId(index).NodeId() != node) {
            ++index;
        }
        env.Sim(TDuration::Seconds(10));
        UNIT_ASSERT_C(oldReplies.size() >= 2, "Initial GUID writes were not held");
        NKikimrBlobStorage::TConfigRequest request;
        auto* command = request.AddCommand()->MutableSetGroupSingleDcMode();
        command->SetGroupId(groupId);
        command->SetGroupGeneration(info->GroupGeneration);
        command->SetEnableSingleDcMode(true);
        command->SetSurvivingDc(info->GetVDiskId(index).FailRealm);
        policyChanged = true;
        const auto response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        info = env.GetGroupInfo(groupId);
        env.Sim(TDuration::Seconds(10));
        UNIT_ASSERT_C(newReplies.size() >= 2, "GUID writes to the new quorum were not held");
        oldRepliesReleased = true;
        for (auto& reply : oldReplies) {
            env.Runtime->Send(reply.release(), node);
        }
        env.Sim(TDuration::Seconds(1));
        UNIT_ASSERT_C(!selectedCommittedFromOldReplies, "Old write replies authorized the local GUID commit");
        allowReplies = true;
        for (auto& reply : newReplies) {
            env.Runtime->Send(reply.release(), node);
        }
        env.Sim(TDuration::Seconds(30));
        const auto edge = env.Runtime->AllocateEdgeActor(node);
        env.Runtime->Send(new IEventHandle(info->GetActorId(index), edge,
            new TEvBlobStorage::TEvVStatus(info->GetVDiskId(index))), node);
        const auto result = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVStatusResult>(edge, true,
            env.Runtime->GetClock() + TDuration::Seconds(30));
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetStatus(), NKikimrProto::OK);
        env.Runtime->FilterFunction = {};
    }

    Y_UNIT_TEST(SingleDcModeChangesDuringGuidSurvey) {
        TEnvironmentSetup env{{.NodeCount = 9, .Erasure = TBlobStorageGroupType::ErasureMirror3dc}};
        env.CreateBoxAndPool(1, 1);
        const ui32 groupId = env.GetGroups().front();
        auto info = env.GetGroupInfo(groupId);
        env.Sim(TDuration::Minutes(2));
        ui32 index = 0;
        while (info->GetActorId(index).NodeId() == env.Settings.ControllerNodeId) {
            ++index;
        }
        const ui32 node = info->GetActorId(index).NodeId();
        const ui32 realm = info->GetVDiskId(index).FailRealm;
        const ui32 originalGeneration = info->GroupGeneration;
        std::vector<std::unique_ptr<IEventHandle>> oldReplies;
        std::vector<std::unique_ptr<IEventHandle>> newReplies;
        bool policyChanged = false;
        bool allowReplies = false;
        ui32 selectedSurveyRequests = 0;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvVDiskGuidObtained::EventType
                    && event->GetRecipientRewrite().NodeId() == node
                    && event->Get<TEvVDiskGuidObtained>()->VDiskId.GroupID.GetRawId() == groupId
                    && !allowReplies) {
                (policyChanged ? newReplies : oldReplies).push_back(std::move(event));
                return false;
            }
            if (event->GetTypeRewrite() == TEvBlobStorage::TEvVSyncGuid::EventType) {
                const auto& record = event->Get<TEvBlobStorage::TEvVSyncGuid>()->Record;
                const auto source = VDiskIDFromVDiskID(record.GetSourceVDiskID());
                const auto target = VDiskIDFromVDiskID(record.GetTargetVDiskID());
                if (source.GroupID.GetRawId() == groupId && event->Sender.NodeId() == node
                        && source.GroupGeneration > originalGeneration) {
                    ++selectedSurveyRequests;
                    UNIT_ASSERT_VALUES_EQUAL(source.FailRealm, realm);
                    UNIT_ASSERT_VALUES_EQUAL(target.FailRealm, realm);
                }
            }
            return true;
        };
        env.StopNode(node);
        env.StartNode(node);
        env.Sim(TDuration::Seconds(10));
        UNIT_ASSERT_C(oldReplies.size() >= 2, "Old GUID survey was not held");
        NKikimrBlobStorage::TConfigRequest request;
        auto* command = request.AddCommand()->MutableSetGroupSingleDcMode();
        command->SetGroupId(groupId);
        command->SetGroupGeneration(info->GroupGeneration);
        command->SetEnableSingleDcMode(true);
        command->SetSurvivingDc(realm);
        policyChanged = true;
        const auto response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        info = env.GetGroupInfo(groupId);
        env.Sim(TDuration::Seconds(10));
        UNIT_ASSERT(selectedSurveyRequests);
        UNIT_ASSERT_C(newReplies.size() >= 2, "New GUID survey was not held");
        allowReplies = true;
        for (auto& reply : oldReplies) {
            const auto* msg = reply->Get<TEvVDiskGuidObtained>();
            env.Runtime->Send(new IEventHandle(reply->GetRecipientRewrite(), reply->Sender,
                new TEvVDiskGuidObtained(msg->VDiskId, TVDiskEternalGuid(ui64(msg->Guid) + 1),
                    NKikimrBlobStorage::TSyncGuidInfo::Final)), node);
        }
        auto getStatus = [&] {
            const auto edge = env.Runtime->AllocateEdgeActor(node);
            env.Runtime->Send(new IEventHandle(info->GetActorId(index), edge,
                new TEvBlobStorage::TEvVStatus(info->GetVDiskId(index))), node);
            const auto result = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVStatusResult>(edge, true,
                env.Runtime->GetClock() + TDuration::Seconds(30));
            UNIT_ASSERT(result);
            return result->Get()->Record.GetStatus();
        };
        env.Sim(TDuration::Seconds(1));
        UNIT_ASSERT_C(getStatus() != NKikimrProto::OK, "Old survey replies completed GUID recovery");
        for (auto& reply : newReplies) {
            env.Runtime->Send(reply.release(), node);
        }
        env.Sim(TDuration::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL(getStatus(), NKikimrProto::OK);
        env.Runtime->FilterFunction = {};
    }

    Y_UNIT_TEST(SingleDcModeRejectsLiveOtherRealms) {
        TEnvironmentSetup env{{.NodeCount = 9, .Erasure = TBlobStorageGroupType::ErasureMirror3dc}};
        env.CreateBoxAndPool(1, 1);
        const ui32 groupId = env.GetGroups().front();
        auto info = env.GetGroupInfo(groupId);
        env.Sim(TDuration::Minutes(2));
        auto checkStatus = [&](bool singleDc) {
            for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
                const auto edge = env.Runtime->AllocateEdgeActor(env.Settings.ControllerNodeId);
                env.Runtime->Send(new IEventHandle(info->GetActorId(i), edge,
                    new TEvBlobStorage::TEvVStatus(info->GetVDiskId(i))), env.Settings.ControllerNodeId);
                const auto reply = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVStatusResult>(edge, true,
                    env.Runtime->GetClock() + TDuration::Seconds(30));
                UNIT_ASSERT(reply);
                UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetStatus(),
                    singleDc && info->GetVDiskId(i).FailRealm != 0
                        ? NKikimrProto::VDISK_ERROR_STATE : NKikimrProto::OK);
            }
        };
        checkStatus(false);
        NKikimrBlobStorage::TConfigRequest request;
        auto* command = request.AddCommand()->MutableSetGroupSingleDcMode();
        command->SetGroupId(groupId);
        command->SetGroupGeneration(info->GroupGeneration);
        command->SetEnableSingleDcMode(true);
        command->SetSurvivingDc(0);
        const auto response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        info = env.GetGroupInfo(groupId);
        env.Sim(TDuration::Seconds(10));
        checkStatus(true);
        ui32 observedSyncRequests = 0;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& event) {
            auto checkPeer = [&](const auto& record) {
                const auto source = VDiskIDFromVDiskID(record.GetSourceVDiskID());
                const auto target = VDiskIDFromVDiskID(record.GetTargetVDiskID());
                if (source.GroupID.GetRawId() == groupId) {
                    ++observedSyncRequests;
                    UNIT_ASSERT_VALUES_EQUAL(source.FailRealm, 0);
                    UNIT_ASSERT_VALUES_EQUAL(target.FailRealm, 0);
                }
            };
            switch (event->GetTypeRewrite()) {
                case TEvBlobStorage::TEvVSync::EventType:
                    checkPeer(event->Get<TEvBlobStorage::TEvVSync>()->Record);
                    break;
                case TEvBlobStorage::TEvVSyncFull::EventType:
                    checkPeer(event->Get<TEvBlobStorage::TEvVSyncFull>()->Record);
                    break;
                case TEvBlobStorage::TEvVSyncGuid::EventType:
                    checkPeer(event->Get<TEvBlobStorage::TEvVSyncGuid>()->Record);
                    break;
            }
            return true;
        };
        env.Sim(TDuration::Minutes(2));
        UNIT_ASSERT_C(observedSyncRequests, "No sync traffic after the live mode change");
        env.Runtime->FilterFunction = {};
    }

    Y_UNIT_TEST(SingleDcModeAfterTwoRealmsLost) {
        TEnvironmentSetup env{{
            .NodeCount = 9,
            .Erasure = TBlobStorageGroupType::ErasureMirror3dc,
        }};
        env.CreateBoxAndPool(1, 1);
        const auto groups = env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
        const ui32 groupId = groups.front();
        auto info = env.GetGroupInfo(groupId);
        ui32 survivingRealm = 0;
        const ui32 clientNode = env.Settings.ControllerNodeId;
        for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
            if (info->GetActorId(i).NodeId() == clientNode) {
                survivingRealm = info->GetVDiskId(i).FailRealm;
                break;
            }
        }
        const TString data = "single-dc recovery payload";
        const TLogoBlobID blobId(5000, 1, 1, 0, data.size(), 0);
        const auto edge = env.Runtime->AllocateEdgeActor(clientNode);
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvPut(blobId, data,
                env.Runtime->GetClock() + TDuration::Seconds(30)));
        });
        const auto put = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, true,
            env.Runtime->GetClock() + TDuration::Seconds(30));
        UNIT_ASSERT(put);
        UNIT_ASSERT_VALUES_EQUAL(put->Get()->Status, NKikimrProto::OK);
        env.Sim(TDuration::Minutes(2));

        auto statusActor = [&] {
            ui32 index = 0;
            while (info->GetVDiskId(index).FailRealm != survivingRealm) {
                ++index;
            }
            const auto sender = env.Runtime->AllocateEdgeActor(clientNode);
            env.Runtime->Send(new IEventHandle(info->GetActorId(index), sender,
                new TEvBlobStorage::TEvVStatus(info->GetVDiskId(index))), clientNode);
            const auto result = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVStatusResult>(sender, true,
                env.Runtime->GetClock() + TDuration::Seconds(30));
            UNIT_ASSERT(result);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetStatus(), NKikimrProto::OK);
            return result->Sender;
        };
        const auto ordinaryActor = statusActor();
        std::set<ui32> lostNodes;
        std::set<ui32> survivingNodes;
        for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
            (info->GetVDiskId(i).FailRealm == survivingRealm ? survivingNodes : lostNodes)
                .insert(info->GetActorId(i).NodeId());
        }
        // Keep the controller alive: its static group is outside the group under test.
        UNIT_ASSERT(!lostNodes.contains(env.Settings.ControllerNodeId));
        for (ui32 node : lostNodes) {
            env.StopNode(node);
        }
        // The ordinary group is unavailable before the emergency flag is enabled.
        const auto unavailableEdge = env.Runtime->AllocateEdgeActor(clientNode);
        const TLogoBlobID unavailableBlob(5001, 1, 1, 0, data.size(), 0);
        env.Runtime->WrapInActorContext(unavailableEdge, [&] {
            SendToBSProxy(unavailableEdge, groupId, new TEvBlobStorage::TEvPut(unavailableBlob, data,
                env.Runtime->GetClock() + TDuration::Seconds(5)));
        });
        const auto unavailablePut = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(unavailableEdge, true,
            env.Runtime->GetClock() + TDuration::Seconds(30));
        UNIT_ASSERT(unavailablePut);
        UNIT_ASSERT(unavailablePut->Get()->Status != NKikimrProto::OK);

        NKikimrBlobStorage::TConfigRequest request;
        auto* command = request.AddCommand()->MutableSetGroupSingleDcMode();
        command->SetGroupId(groupId);
        command->SetGroupGeneration(info->GroupGeneration);
        command->SetEnableSingleDcMode(true);
        command->SetSurvivingDc(survivingRealm);
        const auto response = env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        info = env.GetGroupInfo(groupId);
        env.Sim(TDuration::Seconds(10)); // Drain requests from the previous generation.

        ui32 observedGuidRequests = 0;
        TActorId rejectedLocalSyncEdge;
        bool injectRejectedLocalSync = false;
        std::optional<NKikimrProto::EReplyStatus> rejectedLocalSyncStatus;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvLocalSyncDataResult::EventType
                    && rejectedLocalSyncEdge && event->GetRecipientRewrite() == rejectedLocalSyncEdge) {
                rejectedLocalSyncStatus = event->Get<TEvLocalSyncDataResult>()->Status;
                return false;
            }
            if (event->GetTypeRewrite() == TEvLocalSyncData::EventType) {
                const auto& source = event->Get<TEvLocalSyncData>()->VDiskID;
                if (source.GroupID.GetRawId() == groupId && source.FailRealm == survivingRealm) {
                    if (injectRejectedLocalSync && !rejectedLocalSyncEdge) {
                        const auto recipient = event->GetRecipientRewrite();
                        rejectedLocalSyncEdge = env.Runtime->AllocateEdgeActor(recipient.NodeId());
                        ui32 sourceIndex = 0;
                        while (info->GetVDiskId(sourceIndex).FailRealm == survivingRealm) {
                            ++sourceIndex;
                        }
                        env.Runtime->Send(new IEventHandle(recipient, rejectedLocalSyncEdge,
                            new TEvLocalSyncData(info->GetVDiskId(sourceIndex), TSyncState(), TString())),
                            recipient.NodeId());
                    }
                }
            }
            if (event->GetTypeRewrite() == TEvBlobStorage::TEvVSyncGuid::EventType) {
                const auto& record = event->Get<TEvBlobStorage::TEvVSyncGuid>()->Record;
                const auto source = VDiskIDFromVDiskID(record.GetSourceVDiskID());
                const auto target = VDiskIDFromVDiskID(record.GetTargetVDiskID());
                if (source.GroupID.GetRawId() == groupId) {
                    ++observedGuidRequests;
                    UNIT_ASSERT_VALUES_EQUAL(source.FailRealm, survivingRealm);
                    UNIT_ASSERT_VALUES_EQUAL(target.FailRealm, survivingRealm);
                }
            }
            return true;
        };
        // NodeWarden recreates surviving VDisks; no manual node restart is needed.
        env.Sim(TDuration::Minutes(2));
        UNIT_ASSERT(statusActor() != ordinaryActor);
        UNIT_ASSERT(observedGuidRequests);
        {
            auto savedFilter = std::move(env.Runtime->FilterFunction);
            env.Runtime->FilterFunction = {};
            ui32 targetIndex = 0;
            ui32 sourceIndex = 0;
            for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
                if (info->GetVDiskId(i).FailRealm == survivingRealm) {
                    targetIndex = i;
                } else {
                    sourceIndex = i;
                }
            }
            const auto sender = env.Runtime->AllocateEdgeActor(clientNode);
            env.Runtime->Send(new IEventHandle(info->GetActorId(targetIndex), sender,
                new TEvBlobStorage::TEvVSyncGuid(info->GetVDiskId(sourceIndex), info->GetVDiskId(targetIndex))), clientNode);
            const auto result = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVSyncGuidResult>(sender, true,
                env.Runtime->GetClock() + TDuration::Seconds(30));
            UNIT_ASSERT(result);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetStatus(), NKikimrProto::ERROR);
            auto checkRejectedSync = [&]<typename TRequest, typename TResult>() {
                const auto edge = env.Runtime->AllocateEdgeActor(clientNode);
                auto request = std::make_unique<TRequest>();
                VDiskIDFromVDiskID(info->GetVDiskId(sourceIndex), request->Record.MutableSourceVDiskID());
                VDiskIDFromVDiskID(info->GetVDiskId(targetIndex), request->Record.MutableTargetVDiskID());
                env.Runtime->Send(new IEventHandle(info->GetActorId(targetIndex), edge, request.release()), clientNode);
                const auto reply = env.WaitForEdgeActorEvent<TResult>(edge, true,
                    env.Runtime->GetClock() + TDuration::Seconds(30));
                UNIT_ASSERT(reply);
                UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetStatus(), NKikimrProto::ERROR);
            };
            checkRejectedSync.template operator()<TEvBlobStorage::TEvVSync, TEvBlobStorage::TEvVSyncResult>();
            checkRejectedSync.template operator()<TEvBlobStorage::TEvVSyncFull, TEvBlobStorage::TEvVSyncFullResult>();
            env.Runtime->FilterFunction = std::move(savedFilter);
        }
        auto readSurvivingCopies = [&] {
            ui32 copies = 0;
            for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
                const auto vdisk = info->GetVDiskId(i);
                if (vdisk.FailRealm != survivingRealm) {
                    continue;
                }
                const auto actor = info->GetActorId(i);
                const auto sender = env.Runtime->AllocateEdgeActor(clientNode);
                env.Runtime->Send(new IEventHandle(actor, sender,
                    TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(vdisk,
                        env.Runtime->GetClock() + TDuration::Seconds(30),
                        NKikimrBlobStorage::EGetHandleClass::FastRead,
                        TEvBlobStorage::TEvVGet::EFlags::None, {}, {{blobId}}).release()), clientNode);
                const auto result = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVGetResult>(sender, true,
                    env.Runtime->GetClock() + TDuration::Seconds(30));
                UNIT_ASSERT(result);
                UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetStatus(), NKikimrProto::OK);
                for (const auto& item : result->Get()->Record.GetResult()) {
                    if (item.GetStatus() == NKikimrProto::OK) {
                        UNIT_ASSERT_VALUES_EQUAL(result->Get()->GetBlobData(item).ConvertToString(), data);
                        ++copies;
                    }
                }
            }
            UNIT_ASSERT_C(copies, "No surviving copy of the pre-disaster blob");
        };
        readSurvivingCopies();
        // Returning excluded VDisks must not become usable or join GUID recovery.
        for (ui32 node : lostNodes) {
            env.StartNode(node);
        }
        env.Sim(TDuration::Minutes(1));
        for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
            if (info->GetVDiskId(i).FailRealm == survivingRealm) {
                continue;
            }
            const auto sender = env.Runtime->AllocateEdgeActor(clientNode);
            env.Runtime->Send(new IEventHandle(info->GetActorId(i), sender,
                new TEvBlobStorage::TEvVStatus(info->GetVDiskId(i))), clientNode);
            const auto result = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVStatusResult>(sender, true,
                env.Runtime->GetClock() + TDuration::Seconds(30));
            UNIT_ASSERT(result);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetStatus(), NKikimrProto::VDISK_ERROR_STATE);
        }
        readSurvivingCopies();
        auto proxyGet = [&](const TLogoBlobID& id, bool restore = false) {
            const auto sender = env.Runtime->AllocateEdgeActor(clientNode);
            const auto deadline = env.Runtime->GetClock() + TDuration::Seconds(30);
            env.Runtime->WrapInActorContext(sender, [&] {
                SendToBSProxy(sender, groupId, new TEvBlobStorage::TEvGet(id, 0, 0, deadline,
                    NKikimrBlobStorage::EGetHandleClass::FastRead, restore));
            });
            const auto result = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(sender, true, deadline);
            UNIT_ASSERT(result);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->ResponseSz, 1);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Responses[0].Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Responses[0].Buffer.ConvertToString(), data);
        };
        auto proxyRange = [&](const TLogoBlobID& id, bool indexOnly, bool restore, bool expectSuccess, bool empty = false) {
            const auto sender = env.Runtime->AllocateEdgeActor(clientNode);
            const auto deadline = env.Runtime->GetClock() + TDuration::Seconds(30);
            env.Runtime->WrapInActorContext(sender, [&] {
                SendToBSProxy(sender, groupId, new TEvBlobStorage::TEvRange(id.TabletID(), id, id,
                    restore, deadline, indexOnly));
            });
            const auto result = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvRangeResult>(sender, true,
                deadline + TDuration::Seconds(1));
            UNIT_ASSERT(result);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status == NKikimrProto::OK, expectSuccess);
            if (expectSuccess) {
                UNIT_ASSERT_VALUES_EQUAL(result->Get()->Responses.size(), empty ? 0 : 1);
                if (empty) {
                    return;
                }
                UNIT_ASSERT_VALUES_EQUAL(result->Get()->Responses.front().Id, id);
                if (!indexOnly) {
                    UNIT_ASSERT_VALUES_EQUAL(result->Get()->Responses.front().Buffer, data);
                }
            }
        };
        proxyRange(blobId, true, false, true);
        proxyRange(blobId, false, false, true);
        proxyRange(blobId, false, true, true);
        const TLogoBlobID emptyRangeId(5003, 1, 1, 0, data.size(), 0);
        proxyRange(emptyRangeId, true, false, true, true);
        proxyRange(emptyRangeId, false, false, true, true);

        const TLogoBlobID singleCopyId(5002, 1, 1, 0, data.size(), 0);
        ui32 singleCopyIndex = 0;
        while (info->GetVDiskId(singleCopyIndex).FailRealm != survivingRealm) {
            ++singleCopyIndex;
        }
        const auto singleCopyVDisk = info->GetVDiskId(singleCopyIndex);
        const ui32 singleCopyPart = info->GetIdxInSubgroup(singleCopyVDisk, singleCopyId.Hash()) % 3 + 1;
        const auto singleCopyEdge = env.Runtime->AllocateEdgeActor(clientNode);
        env.Runtime->Send(new IEventHandle(info->GetActorId(singleCopyIndex), singleCopyEdge,
            new TEvBlobStorage::TEvVPut(TLogoBlobID(singleCopyId, singleCopyPart), TRope(data), singleCopyVDisk,
                false, nullptr, TInstant::Max(), NKikimrBlobStorage::TabletLog, false)), clientNode);
        const auto singleCopyPut = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVPutResult>(singleCopyEdge, true,
            env.Runtime->GetClock() + TDuration::Seconds(30));
        UNIT_ASSERT(singleCopyPut);
        UNIT_ASSERT_VALUES_EQUAL(singleCopyPut->Get()->Record.GetStatus(), NKikimrProto::OK);
        proxyRange(singleCopyId, true, false, true);
        proxyRange(singleCopyId, false, false, true);

        auto proxyPut = [&](ui32 step, bool expectSuccess) {
            const TLogoBlobID id(5000, 1, step, 0, data.size(), 0);
            const auto sender = env.Runtime->AllocateEdgeActor(clientNode);
            const auto deadline = env.Runtime->GetClock() + TDuration::Seconds(30);
            env.Runtime->WrapInActorContext(sender, [&] {
                SendToBSProxy(sender, groupId, new TEvBlobStorage::TEvPut(id, data, deadline));
            });
            const auto result = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(sender, true,
                deadline + TDuration::Seconds(1));
            UNIT_ASSERT(result);
            if (expectSuccess) {
                UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::OK);
                proxyGet(id);
            } else {
                UNIT_ASSERT_C(result->Get()->Status != NKikimrProto::OK,
                    "A single remaining VDisk must not acknowledge a write");
            }
        };
        auto proxyBlockAndCollect = [&](ui32 counter, bool expectSuccess, ui32 stoppedNode = 0) {
            const TLogoBlobID collectedId(7000, 1, counter, 0, data.size(), 0);
            if (expectSuccess) {
                const auto sender = env.Runtime->AllocateEdgeActor(clientNode);
                const auto deadline = env.Runtime->GetClock() + TDuration::Seconds(30);
                env.Runtime->WrapInActorContext(sender, [&] {
                    SendToBSProxy(sender, groupId, new TEvBlobStorage::TEvPut(collectedId, data, deadline));
                });
                const auto result = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(sender, true, deadline);
                UNIT_ASSERT(result);
                UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::OK);
                proxyGet(collectedId);
            }
            const auto deadline = env.Runtime->GetClock() + TDuration::Seconds(30);
            const auto blockSender = env.Runtime->AllocateEdgeActor(clientNode);
            env.Runtime->WrapInActorContext(blockSender, [&] {
                SendToBSProxy(blockSender, groupId,
                    new TEvBlobStorage::TEvBlock(6000, counter, deadline));
            });
            const auto blockResult = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvBlockResult>(blockSender, true,
                deadline + TDuration::Seconds(1));
            UNIT_ASSERT(blockResult);
            UNIT_ASSERT_VALUES_EQUAL(blockResult->Get()->Status == NKikimrProto::OK, expectSuccess);

            const auto collectSender = env.Runtime->AllocateEdgeActor(clientNode);
            const auto collectDeadline = env.Runtime->GetClock() + TDuration::Seconds(30);
            env.Runtime->WrapInActorContext(collectSender, [&] {
                SendToBSProxy(collectSender, groupId, new TEvBlobStorage::TEvCollectGarbage(
                    7000, 1, counter, 0, true, 1, counter, nullptr, nullptr, collectDeadline, false));
            });
            const auto collectResult = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvCollectGarbageResult>(
                collectSender, true, collectDeadline + TDuration::Seconds(1));
            UNIT_ASSERT(collectResult);
            UNIT_ASSERT_VALUES_EQUAL(collectResult->Get()->Status == NKikimrProto::OK, expectSuccess);
            if (expectSuccess) {
                env.Sim(TDuration::Minutes(2));
                const auto sender = env.Runtime->AllocateEdgeActor(clientNode);
                const auto deadline = env.Runtime->GetClock() + TDuration::Seconds(30);
                env.Runtime->WrapInActorContext(sender, [&] {
                    SendToBSProxy(sender, groupId, new TEvBlobStorage::TEvGet(collectedId, 0, 0, deadline,
                        NKikimrBlobStorage::EGetHandleClass::FastRead));
                });
                const auto result = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(sender, true, deadline);
                UNIT_ASSERT(result);
                if (counter == 1) {
                    UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::OK);
                    UNIT_ASSERT_VALUES_EQUAL(result->Get()->ResponseSz, 1);
                    UNIT_ASSERT_VALUES_EQUAL(result->Get()->Responses[0].Status, NKikimrProto::NODATA);
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::ERROR);
                    for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
                        const auto vdisk = info->GetVDiskId(i);
                        const auto node = info->GetActorId(i).NodeId();
                        if (vdisk.FailRealm != survivingRealm || node == stoppedNode) {
                            continue;
                        }
                        const auto edge = env.Runtime->AllocateEdgeActor(clientNode);
                        const auto deadline = env.Runtime->GetClock() + TDuration::Seconds(30);
                        env.Runtime->Send(new IEventHandle(info->GetActorId(i), edge,
                            TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(vdisk, deadline,
                                NKikimrBlobStorage::EGetHandleClass::FastRead,
                                TEvBlobStorage::TEvVGet::EFlags::ShowInternals, {}, {{collectedId}}).release()), clientNode);
                        const auto reply = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVGetResult>(edge, true, deadline);
                        UNIT_ASSERT(reply);
                        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetStatus(), NKikimrProto::OK);
                        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.ResultSize(), 1);
                        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetResult(0).GetStatus(), NKikimrProto::NODATA);
                    }
                }
            }
        };
        // Absence in the active realm must never authorize phantom deletion.
        {
            const TLogoBlobID absentId(5000, 1, 100, 0, data.size(), 0);
            const auto sender = env.Runtime->AllocateEdgeActor(clientNode);
            const auto deadline = env.Runtime->GetClock() + TDuration::Seconds(30);
            auto* query = new TEvBlobStorage::TEvGet(absentId, 0, 0, deadline,
                NKikimrBlobStorage::EGetHandleClass::FastRead);
            query->PhantomCheck = true;
            env.Runtime->WrapInActorContext(sender, [&] {
                SendToBSProxy(sender, groupId, query);
            });
            const auto result = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(sender, true, deadline);
            UNIT_ASSERT(result);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->ResponseSz, 1);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Responses[0].Status, NKikimrProto::NODATA);
            UNIT_ASSERT(result->Get()->Responses[0].LooksLikePhantom.has_value());
            UNIT_ASSERT(!*result->Get()->Responses[0].LooksLikePhantom);
        }
        proxyGet(blobId);
        proxyGet(blobId, true);
        injectRejectedLocalSync = true;
        proxyPut(2, true);
        proxyBlockAndCollect(1, true);
        env.Sim(TDuration::Minutes(2));
        UNIT_ASSERT_C(rejectedLocalSyncEdge, "No local sync data observed in the surviving realm");
        UNIT_ASSERT_C(rejectedLocalSyncStatus, "No reply to local sync from an inactive realm");
        UNIT_ASSERT_VALUES_EQUAL(*rejectedLocalSyncStatus, NKikimrProto::ERROR);
        std::vector<ui32> faultNodes;
        for (ui32 node : survivingNodes) {
            if (node != clientNode) {
                faultNodes.push_back(node);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(faultNodes.size(), 2);
        env.StopNode(faultNodes[0]);
        proxyRange(blobId, true, false, false);
        proxyRange(blobId, false, false, false);
        proxyPut(3, true);
        proxyBlockAndCollect(2, true, faultNodes[0]);
        env.StopNode(faultNodes[1]);
        proxyPut(4, false);
        proxyBlockAndCollect(3, false);
        env.Runtime->FilterFunction = {};

        // Restore the ordinary group and replicate writes made in single-DC mode.
        for (ui32 node : faultNodes) {
            env.StartNode(node);
        }
        auto ordinaryInfo = env.GetGroupInfo(groupId);
        NKikimrBlobStorage::TConfigRequest disableRequest;
        auto* disable = disableRequest.AddCommand()->MutableSetGroupSingleDcMode();
        disable->SetGroupId(groupId);
        disable->SetGroupGeneration(ordinaryInfo->GroupGeneration);
        disable->SetEnableSingleDcMode(false);
        const auto disableResponse = env.Invoke(disableRequest);
        UNIT_ASSERT_C(disableResponse.GetSuccess(), disableResponse.GetErrorDescription());
        ordinaryInfo = env.GetGroupInfo(groupId);
        // Restart also covers VDisks that entered the controlled error state.
        for (ui32 node : lostNodes) {
            env.StopNode(node);
            env.StartNode(node);
        }
        for (ui32 node : survivingNodes) {
            env.StopNode(node);
            env.StartNode(node);
        }
        const TLogoBlobID emergencyBlob(5000, 1, 2, 0, data.size(), 0);
        proxyGet(emergencyBlob);
        bool restored = false;
        for (ui32 attempt = 0; attempt < 30 && !restored; ++attempt) {
            env.Sim(TDuration::Seconds(10));
            std::array<ui32, 3> copies{};
            for (ui32 i = 0; i < ordinaryInfo->GetTotalVDisksNum(); ++i) {
                const auto vdisk = ordinaryInfo->GetVDiskId(i);
                const auto sender = env.Runtime->AllocateEdgeActor(clientNode);
                const auto deadline = env.Runtime->GetClock() + TDuration::Seconds(30);
                env.Runtime->Send(new IEventHandle(ordinaryInfo->GetActorId(i), sender,
                    TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(vdisk, deadline,
                        NKikimrBlobStorage::EGetHandleClass::FastRead,
                        TEvBlobStorage::TEvVGet::EFlags::ShowInternals, {}, {{emergencyBlob}}).release()), clientNode);
                const auto result = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVGetResult>(sender, true, deadline);
                UNIT_ASSERT(result);
                if (result->Get()->Record.GetStatus() != NKikimrProto::OK) {
                    continue;
                }
                for (const auto& item : result->Get()->Record.GetResult()) {
                    if (item.GetStatus() == NKikimrProto::OK) {
                        UNIT_ASSERT_VALUES_EQUAL(result->Get()->GetBlobData(item).ConvertToString(), data);
                        ++copies[vdisk.FailRealm];
                        break;
                    }
                }
            }
            restored = true;
            for (ui32 realm = 0; realm < 3; ++realm) {
                if (realm != survivingRealm && copies[realm] < 1) {
                    restored = false;
                }
            }
        }
        UNIT_ASSERT_C(restored, "Emergency write was not replicated to both returned realms");
        proxyGet(emergencyBlob);
        const ui32 restoredClient = *lostNodes.begin();
        auto readRestored = [&] {
            const auto sender = env.Runtime->AllocateEdgeActor(restoredClient);
            const auto deadline = env.Runtime->GetClock() + TDuration::Seconds(30);
            env.Runtime->WrapInActorContext(sender, [&] {
                SendToBSProxy(sender, groupId, new TEvBlobStorage::TEvGet(emergencyBlob, 0, 0, deadline,
                    NKikimrBlobStorage::EGetHandleClass::FastRead));
            });
            const auto result = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(sender, true, deadline);
            UNIT_ASSERT(result);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->ResponseSz, 1);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Responses[0].Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Responses[0].Buffer.ConvertToString(), data);
        };
        // Configure the reader before taking down the realm hosting BSC.
        readRestored();
        for (ui32 node : survivingNodes) {
            env.StopNode(node);
        }
        readRestored();
        for (ui32 node : survivingNodes) {
            env.StartNode(node);
        }
        for (ui32 node : lostNodes) {
            env.StopNode(node);
            env.StartNode(node);
        }
        env.Sim(TDuration::Minutes(1));
        readRestored();
        for (ui32 node : survivingNodes) {
            env.StopNode(node);
        }
        readRestored();
    }

    void TestCutting(TBlobStorageGroupType groupType) {
        const ui32 groupSize = groupType.BlobSubgroupSize();

        // for (ui32 mask = 0; mask < (1 << groupSize); ++mask) {  // TIMEOUT
        {
            ui32 mask = RandomNumber(1ull << groupSize);
            for (bool compressChunks : { true, false }) {
                TEnvironmentSetup env{{
                    .NodeCount = groupSize,
                    .Erasure = groupType,
                }};

                env.CreateBoxAndPool(1, 1);
                std::vector<ui32> groups = env.GetGroups();
                UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
                ui32 groupId = groups[0];

                const ui64 tabletId = 5000;
                const ui32 channel = 10;
                ui32 gen = 1;
                ui32 step = 1;
                ui64 cookie = 1;

                ui64 totalSize = 0;

                std::vector<TControlWrapper> cutLocalSyncLogControls;
                std::vector<TControlWrapper> compressChunksControls;
                std::vector<TActorId> edges;

                for (ui32 nodeId = 1; nodeId <= groupSize; ++nodeId) {
                    cutLocalSyncLogControls.emplace_back(0, 0, 1);
                    compressChunksControls.emplace_back(1, 0, 1);
                    TAppData* appData = env.Runtime->GetNode(nodeId)->AppData.get();
                    TControlBoard::RegisterSharedControl(cutLocalSyncLogControls.back(), appData->Icb->VDiskControls.EnableLocalSyncLogDataCutting);
                    TControlBoard::RegisterSharedControl(compressChunksControls.back(), appData->Icb->VDiskControls.EnableSyncLogChunkCompressionHDD);
                    edges.push_back(env.Runtime->AllocateEdgeActor(nodeId));
                }

                for (ui32 i = 0; i < groupSize; ++i) {
                    env.Runtime->WrapInActorContext(edges[i], [&] {
                        SendToBSProxy(edges[i], groupId, new TEvBlobStorage::TEvStatus(TInstant::Max()));
                    });
                    auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvStatusResult>(edges[i], false);
                    UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
                }

                auto writeBlob = [&](ui32 nodeId, ui32 blobSize) {
                    TLogoBlobID blobId(tabletId, gen, step, channel, blobSize, ++cookie);
                    totalSize += blobSize;
                    TString data = MakeData(blobSize);

                    const TActorId& sender = edges[nodeId - 1];
                    env.Runtime->WrapInActorContext(sender, [&] () {
                        SendToBSProxy(sender, groupId, new TEvBlobStorage::TEvPut(blobId, std::move(data), TInstant::Max()));
                    });
                };

                env.Runtime->FilterFunction = [&](ui32/* nodeId*/, std::unique_ptr<IEventHandle>& ev) {
                    switch(ev->Type) {
                        case TEvBlobStorage::TEvPutResult::EventType:
                            UNIT_ASSERT_VALUES_EQUAL(ev->Get<TEvBlobStorage::TEvPutResult>()->Status, NKikimrProto::OK);
                            return false;
                        default:
                            return true;
                    }
                };

                while (totalSize < 16_MB) {
                    writeBlob(GenerateRandom(1, groupSize + 1), GenerateRandom(1, 1_MB));
                }
                env.Sim(TDuration::Minutes(5));

                for (ui32 i = 0; i < groupSize; ++i) {
                    cutLocalSyncLogControls[i] = !!(mask & (1 << i));
                    compressChunksControls[i] = compressChunks;
                }

                while (totalSize < 32_MB) {
                    writeBlob(GenerateRandom(1, groupSize + 1), GenerateRandom(1, 1_MB));
                }

                env.Sim(TDuration::Minutes(5));
            }
        }
    }

    Y_UNIT_TEST(TestSyncLogCuttingMirror3dc) {
        TestCutting(TBlobStorageGroupType::ErasureMirror3dc);
    }

    Y_UNIT_TEST(TestSyncLogCuttingMirror3of4) {
        TestCutting(TBlobStorageGroupType::ErasureMirror3of4);
    }

    Y_UNIT_TEST(TestSyncLogCuttingBlock4Plus2) {
        TestCutting(TBlobStorageGroupType::Erasure4Plus2Block);
    }

    Y_UNIT_TEST(SyncLogDiskOverflowOldSnapshotCreatesDuplicateFreeChunkWithoutRestart) {
        /*
         * Actor-level reproducer for the live duplicate TOneChunk scenario.
         * This test does not manually build SyncLog commit deltas. It writes real VDisk records,
         * forces SyncLog disk spills, and lets the real SyncLog committer actor append swap pages.
         *
         * The bad sequence is:
         *
         * 1. SyncLog has disk chunks, and the last one still has free pages.
         * 2. PrepareCommitData() takes a snapshot of that disk log.
         * 3. The same PrepareCommitData() fixes disk overflow and removes that old last chunk
         *    from the live disk log, putting it into delayed deletion.
         * 4. The committer appends through the old snapshot into old LastChunkIdx().
         * 5. ApplyCommitResult() sees that the live disk log no longer ends with this chunk and
         *    creates a new TOneChunk for the same chunkIdx.
         * 6. Releasing the old snapshot and then trimming the new live chunk naturally produce two
         *    distinct TEvSyncLogFreeChunk events for the same chunkIdx.
         */
        TEnvironmentSetup env{{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::ErasureMirror3of4,
            .VDiskConfigPreprocessor = [](TVDiskConfig& config) {
                config.MaxLogoBlobDataSize = 8_KB;
                config.MinHugeBlobInBytes = 4_KB;
                config.MilestoneHugeBlobInBytes = 6_KB;
                config.SyncLogMaxMemAmount = 128_KB;
                config.SyncLogMaxDiskAmount = 32_KB;
            },
            .PDiskChunkSize = 32_KB,
        }};
        auto& runtime = env.Runtime;

        env.CreateBoxAndPool(1, 1);
        std::vector<ui32> groups = env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
        const TIntrusivePtr<TBlobStorageGroupInfo> info = env.GetGroupInfo(groups.front());
        const TVDiskID vdiskId = info->GetVDiskId(0);
        const TActorId vdiskActorId = info->GetActorId(0);
        const TActorId edge = runtime->AllocateEdgeActor(vdiskActorId.NodeId(), __FILE__, __LINE__);

        TActorId syncLogId;
        TActorId syncLogKeeperId;
        bool gotOwner = false;
        NPDisk::TOwner owner = 0;
        NPDisk::TOwnerRound ownerRound = 0;
        ui64 maxObservedDataLsn = 0;
        ui32 syncLogCommitDoneEvents = 0;
        ui32 freeChunkNotifications = 0;
        bool observedSyncLogDataCommit = false;
        bool observedAppendToDeletedChunk = false;
        bool observedDuplicateFreeChunk = false;
        bool observedDuplicateImmediateFree = false;
        bool observedInvalidChunkDelete = false;
        bool postponeNextSyncLogDataCommit = false;
        bool dataCommitPostponed = false;
        bool reorderRaceFreeChunks = false;
        ui64 postponedDataCommitLsn = 0;
        TVector<TChunkIdx> postponedDataCommitChunks;
        TMaybe<TChunkIdx> raceChunkToFreeFirst;
        std::unique_ptr<IEventHandle> postponedDataCommit;
        std::deque<std::unique_ptr<IEventHandle>> postponedFreeChunkEvents;
        TVector<TString> duplicateFreeChunkDetails;
        TVector<TString> appendToDeletedChunkDetails;
        TVector<TChunkIdx> duplicateImmediateFreeChunks;
        TVector<TString> invalidChunkDeleteDetails;
        TVector<TString> syncLogCommitDetails;
        TVector<TString> syncLogDeleteDetails;
        TVector<TString> postponeCandidateDetails;
        THashMap<TChunkIdx, ui32> freeChunkNotificationCounts;
        THashSet<TChunkIdx> freeChunkNotificationsAwaitingForget;
        THashSet<TChunkIdx> immediatelyFreedChunks;
        THashSet<TString> observedDeleteLogKeys;
        THashMap<NPDisk::TOwner, THashSet<TChunkIdx>> reservedChunksByOwner;
        THashMap<NPDisk::TOwner, THashSet<TChunkIdx>> committedChunksByOwner;
        THashMap<TActorId, std::deque<NPDisk::TOwner>> chunkReserveOwnerByRecipient;
        auto formatFreeChunkNotifications = [&] {
            TStringStream str;
            str << "[";
            bool first = true;
            for (const auto& [chunkIdx, count] : freeChunkNotificationCounts) {
                if (!first) {
                    str << " ";
                }
                first = false;
                str << "{chunkIdx# " << chunkIdx << " count# " << count << "}";
            }
            str << "]";
            return str.Str();
        };
        auto formatChunkSet = [](const THashSet<TChunkIdx>& chunks) {
            TStringStream str;
            str << "[";
            bool first = true;
            for (const TChunkIdx chunkIdx : chunks) {
                if (!first) {
                    str << " ";
                }
                first = false;
                str << chunkIdx;
            }
            str << "]";
            return str.Str();
        };
        auto findChunkOwner = [&](const THashMap<NPDisk::TOwner, THashSet<TChunkIdx>>& chunksByOwner,
                TChunkIdx chunkIdx) {
            TMaybe<NPDisk::TOwner> result;
            for (const auto& [chunkOwner, chunks] : chunksByOwner) {
                if (chunks.contains(chunkIdx)) {
                    result = chunkOwner;
                    break;
                }
            }
            return result;
        };
        auto formatChunkState = [&](NPDisk::TOwner chunkOwner) {
            return TStringBuilder()
                << "{owner# " << ui32(chunkOwner)
                << " reserved# " << formatChunkSet(reservedChunksByOwner[chunkOwner])
                << " committed# " << formatChunkSet(committedChunksByOwner[chunkOwner])
                << "}";
        };
        auto markInvalidChunkDelete = [&](const NPDisk::TEvLog& msg, TChunkIdx chunkIdx, const char *reason) {
            observedInvalidChunkDelete = true;
            invalidChunkDeleteDetails.push_back(TStringBuilder()
                << "{reason# " << reason
                << " lsn# " << msg.Lsn
                << " signature# " << msg.Signature.GetUnmasked()
                << " isStartingPoint# " << msg.CommitRecord.IsStartingPoint
                << " owner# " << ui32(msg.Owner)
                << " reservedOwner# " << findChunkOwner(reservedChunksByOwner, chunkIdx).GetOrElse(0)
                << " committedOwner# " << findChunkOwner(committedChunksByOwner, chunkIdx).GetOrElse(0)
                << " chunkIdx# " << chunkIdx
                << " deleteToDecommitted# " << msg.CommitRecord.DeleteToDecommitted
                << " chunkState# " << formatChunkState(msg.Owner)
                << " chunks# " << FormatList(msg.CommitRecord.DeleteChunks)
                << "}");
        };

        auto observePDiskLog = [&](const NPDisk::TEvLog& msg) {
            const ui32 signature = msg.Signature.GetUnmasked();
            const bool syncLogEntryPointCommit = signature == TLogSignature::SignatureSyncLogIdx &&
                msg.CommitRecord.IsStartingPoint;
            if (signature == TLogSignature::SignatureLogoBlobOpt) {
                owner = msg.Owner;
                ownerRound = msg.OwnerRound;
                gotOwner = true;
                maxObservedDataLsn = Max(maxObservedDataLsn, msg.Lsn);
            }

            if (!msg.Signature.HasCommitRecord()) {
                return false;
            }

            if (syncLogEntryPointCommit && msg.CommitRecord.CommitChunks) {
                observedSyncLogDataCommit = true;
                syncLogCommitDetails.push_back(TStringBuilder()
                    << "{lsn# " << msg.Lsn
                    << " owner# " << ui32(msg.Owner)
                    << " chunks# " << FormatList(msg.CommitRecord.CommitChunks)
                    << "}");
            }

            auto& reservedChunks = reservedChunksByOwner[msg.Owner];
            auto& committedChunks = committedChunksByOwner[msg.Owner];
            for (const TChunkIdx chunkIdx : msg.CommitRecord.CommitChunks) {
                reservedChunks.erase(chunkIdx);
                committedChunks.insert(chunkIdx);
                immediatelyFreedChunks.erase(chunkIdx);
            }

            if (syncLogEntryPointCommit && msg.CommitRecord.DeleteChunks) {
                syncLogDeleteDetails.push_back(TStringBuilder()
                    << "{lsn# " << msg.Lsn
                    << " owner# " << ui32(msg.Owner)
                    << " chunks# " << FormatList(msg.CommitRecord.DeleteChunks)
                    << " deleteToDecommitted# " << msg.CommitRecord.DeleteToDecommitted
                    << "}");
            }

            if (syncLogEntryPointCommit && msg.CommitRecord.CommitChunks && msg.CommitRecord.DeleteChunks) {
                THashSet<TChunkIdx> deletedChunks(msg.CommitRecord.DeleteChunks.begin(), msg.CommitRecord.DeleteChunks.end());
                for (const TChunkIdx chunkIdx : msg.CommitRecord.CommitChunks) {
                    if (deletedChunks.contains(chunkIdx)) {
                        observedAppendToDeletedChunk = true;
                        appendToDeletedChunkDetails.push_back(TStringBuilder()
                            << "{lsn# " << msg.Lsn
                            << " owner# " << ui32(msg.Owner)
                            << " chunkIdx# " << chunkIdx
                            << " commitChunks# " << FormatList(msg.CommitRecord.CommitChunks)
                            << " deleteChunks# " << FormatList(msg.CommitRecord.DeleteChunks)
                            << "}");
                    }
                }
            }

            if (msg.CommitRecord.DeleteChunks) {
                const TString deleteLogKey = TStringBuilder()
                    << msg.Lsn << ":" << msg.Owner << ":" << msg.OwnerRound << ":"
                    << msg.CommitRecord.DeleteToDecommitted << ":"
                    << FormatList(msg.CommitRecord.DeleteChunks);
                if (!observedDeleteLogKeys.insert(deleteLogKey).second) {
                    return false;
                }

                for (const TChunkIdx chunkIdx : msg.CommitRecord.DeleteChunks) {
                    if (msg.CommitRecord.DeleteToDecommitted) {
                        if (immediatelyFreedChunks.contains(chunkIdx)) {
                            markInvalidChunkDelete(msg, chunkIdx, "delete-to-decommitted-after-plain-delete");
                            continue;
                        }

                        if (reservedChunks.contains(chunkIdx)) {
                            continue;
                        }

                        if (committedChunks.erase(chunkIdx)) {
                            reservedChunks.insert(chunkIdx);
                            continue;
                        }

                        markInvalidChunkDelete(msg, chunkIdx, "delete-to-decommitted-missing-chunk");
                    } else {
                        const ui32 erased = reservedChunks.erase(chunkIdx) + committedChunks.erase(chunkIdx);
                        if (!erased) {
                            markInvalidChunkDelete(msg, chunkIdx, "plain-delete-missing-chunk");
                        }

                        if (!immediatelyFreedChunks.insert(chunkIdx).second) {
                            observedDuplicateImmediateFree = true;
                            duplicateImmediateFreeChunks.push_back(chunkIdx);
                        }
                    }
                }
            }

            return observedAppendToDeletedChunk || observedDuplicateImmediateFree || observedInvalidChunkDelete;
        };

        auto shouldPostponeSyncLogDataCommit = [&](const NPDisk::TEvLog& msg) {
            if (postponeNextSyncLogDataCommit &&
                    msg.Signature.GetUnmasked() == TLogSignature::SignatureSyncLogIdx &&
                    msg.CommitRecord.IsStartingPoint &&
                    msg.CommitRecord.CommitChunks) {
                postponeCandidateDetails.push_back(TStringBuilder()
                    << "{lsn# " << msg.Lsn
                    << " owner# " << ui32(msg.Owner)
                    << " expectedOwner# " << ui32(owner)
                    << " gotOwner# " << gotOwner
                    << " chunks# " << FormatList(msg.CommitRecord.CommitChunks)
                    << " deletes# " << FormatList(msg.CommitRecord.DeleteChunks)
                    << "}");
            }
            return postponeNextSyncLogDataCommit &&
                gotOwner &&
                msg.Owner == owner &&
                msg.Signature.GetUnmasked() == TLogSignature::SignatureSyncLogIdx &&
                msg.CommitRecord.IsStartingPoint &&
                msg.CommitRecord.CommitChunks &&
                msg.CommitRecord.DeleteChunks;
        };

        auto shouldDropRaceChunkDelete = [&](const NPDisk::TEvLog& msg) {
            if (!msg.Signature.HasCommitRecord() || !msg.CommitRecord.DeleteChunks ||
                    !msg.CommitRecord.DeleteToDecommitted) {
                return false;
            }

            for (const TChunkIdx chunkIdx : msg.CommitRecord.DeleteChunks) {
                if (immediatelyFreedChunks.contains(chunkIdx)) {
                    markInvalidChunkDelete(msg, chunkIdx, "drop-delete-to-decommitted-after-plain-delete");
                    return true;
                }
            }
            return false;
        };

        runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvBlobStorage::EvSyncLogPut:
                    if (!syncLogId) {
                        syncLogId = ev->Recipient;
                    } else if (!syncLogKeeperId && ev->Recipient != syncLogId) {
                        syncLogKeeperId = ev->Recipient;
                    }
                    break;

                case TEvBlobStorage::EvLog: {
                    const auto *msg = ev->Get<NPDisk::TEvLog>();
                    if (shouldDropRaceChunkDelete(*msg)) {
                        return false;
                    }

                    if (shouldPostponeSyncLogDataCommit(*msg)) {
                        postponeNextSyncLogDataCommit = false;
                        dataCommitPostponed = true;
                        postponedDataCommitLsn = msg->Lsn;
                        postponedDataCommitChunks = msg->CommitRecord.CommitChunks;
                        if (postponedDataCommitChunks) {
                            raceChunkToFreeFirst = postponedDataCommitChunks.back();
                            reorderRaceFreeChunks = true;
                        }
                        postponedDataCommit.reset(ev.release());
                        return false;
                    }

                    if (observePDiskLog(*ev->Get<NPDisk::TEvLog>())) {
                        return false;
                    }
                    break;
                }

                case TEvBlobStorage::EvMultiLog:
                    for (const auto& [log, traceId] : ev->Get<NPDisk::TEvMultiLog>()->Logs) {
                        Y_UNUSED(traceId);
                        if (shouldDropRaceChunkDelete(*log)) {
                            return false;
                        }
                    }

                    if (postponeNextSyncLogDataCommit) {
                        for (const auto& [log, traceId] : ev->Get<NPDisk::TEvMultiLog>()->Logs) {
                            Y_UNUSED(traceId);
                            if (shouldPostponeSyncLogDataCommit(*log)) {
                                postponeNextSyncLogDataCommit = false;
                                dataCommitPostponed = true;
                                postponedDataCommitLsn = log->Lsn;
                                postponedDataCommitChunks = log->CommitRecord.CommitChunks;
                                if (postponedDataCommitChunks) {
                                    raceChunkToFreeFirst = postponedDataCommitChunks.back();
                                    reorderRaceFreeChunks = true;
                                }
                                postponedDataCommit.reset(ev.release());
                                return false;
                            }
                        }
                    }
                    for (const auto& [log, traceId] : ev->Get<NPDisk::TEvMultiLog>()->Logs) {
                        Y_UNUSED(traceId);
                        if (observePDiskLog(*log)) {
                            return false;
                        }
                    }
                    break;

                case TEvBlobStorage::EvChunkReserve: {
                    const auto *msg = ev->Get<NPDisk::TEvChunkReserve>();
                    chunkReserveOwnerByRecipient[ev->Sender].push_back(msg->Owner);
                    break;
                }

                case TEvBlobStorage::EvChunkReserveResult: {
                    const auto *msg = ev->Get<NPDisk::TEvChunkReserveResult>();
                    if (msg->Status == NKikimrProto::OK) {
                        auto it = chunkReserveOwnerByRecipient.find(ev->Recipient);
                        if (it != chunkReserveOwnerByRecipient.end() && !it->second.empty()) {
                            const NPDisk::TOwner chunkOwner = it->second.front();
                            it->second.pop_front();
                            for (const TChunkIdx chunkIdx : msg->ChunkIds) {
                                reservedChunksByOwner[chunkOwner].insert(chunkIdx);
                            }
                        }
                    }
                    break;
                }

                case TEvBlobStorage::EvSyncLogCommitDone:
                    ++syncLogCommitDoneEvents;
                    break;

                case TEvBlobStorage::EvSyncLogFreeChunk: {
                    const auto *msg = ev->Get<NSyncLog::TEvSyncLogFreeChunk>();
                    if (reorderRaceFreeChunks && raceChunkToFreeFirst && msg->ChunkIdx != *raceChunkToFreeFirst) {
                        postponedFreeChunkEvents.emplace_back(ev.release());
                        return false;
                    }
                    if (reorderRaceFreeChunks && raceChunkToFreeFirst && msg->ChunkIdx == *raceChunkToFreeFirst) {
                        reorderRaceFreeChunks = false;
                    }
                    ui32& notificationCount = freeChunkNotificationCounts[msg->ChunkIdx];
                    ++notificationCount;
                    ++freeChunkNotifications;
                    if (!freeChunkNotificationsAwaitingForget.insert(msg->ChunkIdx).second) {
                        observedDuplicateFreeChunk = true;
                        duplicateFreeChunkDetails.push_back(TStringBuilder()
                            << "{chunkIdx# " << msg->ChunkIdx
                            << " count# " << notificationCount
                            << "}");
                    }
                    break;
                }

                case TEvBlobStorage::EvChunkForget:
                    for (const TChunkIdx chunkIdx : ev->Get<NPDisk::TEvChunkForget>()->ForgetChunks) {
                        freeChunkNotificationsAwaitingForget.erase(chunkIdx);
                    }
                    if (const auto *msg = ev->Get<NPDisk::TEvChunkForget>()) {
                        auto& reservedChunks = reservedChunksByOwner[msg->Owner];
                        auto& committedChunks = committedChunksByOwner[msg->Owner];
                        for (const TChunkIdx chunkIdx : msg->ForgetChunks) {
                            reservedChunks.erase(chunkIdx);
                            committedChunks.erase(chunkIdx);
                            immediatelyFreedChunks.insert(chunkIdx);
                        }
                    }
                    break;
            }

            return true;
        };

        auto simUntil = [&](auto predicate, TDuration timeout) {
            const TInstant deadline = runtime->GetClock() + timeout;
            runtime->Sim([&] { return runtime->GetClock() <= deadline && !predicate(); });
            return predicate();
        };

        ui32 nextStep = 1;
        ui64 nextCookie = 1;
        const TString data = MakeData(1);
        auto writeVDiskRecords = [&](const TActorId& queueId, ui32 records, const char *phase) {
            const ui32 batchSize = 4'096;
            for (ui32 firstRecord = 0; firstRecord < records; firstRecord += batchSize) {
                if (observedAppendToDeletedChunk || observedDuplicateFreeChunk ||
                        observedDuplicateImmediateFree || observedInvalidChunkDelete) {
                    return;
                }

                const ui32 batch = Min(batchSize, records - firstRecord);
                auto multiPut = std::make_unique<TEvBlobStorage::TEvVMultiPut>(vdiskId, TInstant::Max(),
                    NKikimrBlobStorage::EPutHandleClass::TabletLog, false);

                for (ui32 i = 0; i < batch; ++i) {
                    const TLogoBlobID blobId(TLogoBlobID(42, 1, nextStep++, 0, data.size(), nextCookie++), 1);
                    multiPut->AddVPut(blobId, TRcBuf(data), nullptr, false, false, false, nullptr, {}, false);
                }

                runtime->Send(new IEventHandle(queueId, edge, multiPut.release()), edge.NodeId());
                auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVMultiPutResult>(edge, false);
                if (res->Get()->Record.GetStatus() != NKikimrProto::OK &&
                        (observedDuplicateFreeChunk || observedDuplicateImmediateFree || observedInvalidChunkDelete)) {
                    return;
                }
                UNIT_ASSERT_C(res->Get()->Record.GetStatus() == NKikimrProto::OK,
                    "TEvVMultiPut failed during " << phase
                    << " status# " << NKikimrProto::EReplyStatus_Name(res->Get()->Record.GetStatus()));
                UNIT_ASSERT_VALUES_EQUAL(res->Get()->Record.ItemsSize(), batch);
                for (ui32 i = 0; i < batch; ++i) {
                    UNIT_ASSERT_C(res->Get()->Record.GetItems(i).GetStatus() == NKikimrProto::OK,
                        "TEvVMultiPut item failed during " << phase
                        << " item# " << i
                        << " status# " << NKikimrProto::EReplyStatus_Name(res->Get()->Record.GetItems(i).GetStatus()));
                }
            }
        };

        auto requestSyncLogCut = [&](const char *phase) {
            runtime->Send(new IEventHandle(syncLogId, edge,
                new NPDisk::TEvCutLog(owner, ownerRound, maxObservedDataLsn + 1, 0, 0, 0, 0)), syncLogId.NodeId());
            Y_UNUSED(phase);
            env.Sim(TDuration::MilliSeconds(1));
        };

        env.WithQueueId(vdiskId, NKikimrBlobStorage::EVDiskQueueId::PutTabletLog, [&](const TActorId& queueId) {
            writeVDiskRecords(queueId, 3'000, "initial sync-log fill");
        });

        UNIT_ASSERT_C(simUntil([&] {
            return syncLogId && syncLogKeeperId && gotOwner && maxObservedDataLsn;
        }, TDuration::Seconds(10)), "failed to discover SyncLog actor, SyncLogKeeper actor, PDisk owner, or data LSN");

        requestSyncLogCut("initial sync-log disk spill");
        UNIT_ASSERT_C(simUntil([&] {
            return observedSyncLogDataCommit && syncLogCommitDoneEvents;
        }, TDuration::Seconds(30)), "initial writes did not move SyncLog data to disk"
            << "; commits# " << FormatList(syncLogCommitDetails)
            << "; deletes# " << FormatList(syncLogDeleteDetails));

        const ui32 commitDoneBeforePostponedDataCommit = syncLogCommitDoneEvents;
        postponeNextSyncLogDataCommit = true;
        env.WithQueueId(vdiskId, NKikimrBlobStorage::EVDiskQueueId::PutTabletLog, [&](const TActorId& queueId) {
            writeVDiskRecords(queueId, 1'024, "pre-race sync-log fill");
        });
        requestSyncLogCut("pre-race sync-log cut and postpone data commit");
        UNIT_ASSERT_C(simUntil([&] {
            return dataCommitPostponed;
        }, TDuration::Seconds(30)), "failed to postpone a natural SyncLog data commit before the large swap"
            << "; candidates# " << FormatList(postponeCandidateDetails)
            << "; commits# " << FormatList(syncLogCommitDetails)
            << "; deletes# " << FormatList(syncLogDeleteDetails));

        UNIT_ASSERT_C(postponedDataCommit, "data commit was marked as postponed but no event was saved");
        runtime->Send(postponedDataCommit.release(), edge.NodeId());
        UNIT_ASSERT_C(simUntil([&] {
            return syncLogCommitDoneEvents > commitDoneBeforePostponedDataCommit ||
                observedAppendToDeletedChunk || observedDuplicateFreeChunk ||
                observedDuplicateImmediateFree || observedInvalidChunkDelete;
        }, TDuration::Seconds(30)), "postponed SyncLog data commit did not complete"
            << "; lsn# " << postponedDataCommitLsn
            << "; chunks# " << FormatList(postponedDataCommitChunks));

        UNIT_ASSERT_C(!observedAppendToDeletedChunk,
            "SyncLog committer appended swap data into a chunk deleted by the same entrypoint commit: "
            << FormatList(appendToDeletedChunkDetails)
            << "; this creates two live TOneChunk objects for one SyncLog chunkIdx without restart"
            << "; commits# " << FormatList(syncLogCommitDetails)
            << "; deletes# " << FormatList(syncLogDeleteDetails));

        UNIT_ASSERT_C(!observedDuplicateFreeChunk,
            "SyncLog produced two TEvSyncLogFreeChunk notifications for the same chunk before TEvChunkForget: "
            << FormatList(duplicateFreeChunkDetails)
            << "; this means two live TOneChunk objects existed for one SyncLog chunkIdx without restart"
            << "; freeChunkNotifications# " << formatFreeChunkNotifications()
            << "; commits# " << FormatList(syncLogCommitDetails)
            << "; deletes# " << FormatList(syncLogDeleteDetails));

        UNIT_ASSERT_C(!observedDuplicateImmediateFree,
            "SyncLog produced a second plain DeleteChunks for already freed chunks: "
            << FormatList(duplicateImmediateFreeChunks)
            << "; real PDisk reports this as BPD77 ownerId != trueOwnerId");

        UNIT_ASSERT_C(!observedInvalidChunkDelete,
            "SyncLog sent DeleteChunks for chunks that PDiskMock no longer considers reserved or committed: "
            << FormatList(invalidChunkDeleteDetails)
            << "; this would make PDiskMock crash in DeleteChunk/UncommitChunk"
            << "; freeChunkNotifications# " << formatFreeChunkNotifications()
            << "; commits# " << FormatList(syncLogCommitDetails)
            << "; deletes# " << FormatList(syncLogDeleteDetails));
    }

    Y_UNIT_TEST(SyncWhenDiskGetsDown) {
        return; // re-enable when protocol issue is resolved

        TEnvironmentSetup env{{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        }};
        auto& runtime = env.Runtime;

        env.CreateBoxAndPool(1, 1);
        auto groups = env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
        const TIntrusivePtr<TBlobStorageGroupInfo> info = env.GetGroupInfo(groups.front());

        const TActorId edge = runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        const TString buffer = "hello, world!";
        TLogoBlobID id(1, 1, 1, 0, buffer.size(), 0);
        runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, info->GroupID, new TEvBlobStorage::TEvPut(id, buffer, TInstant::Max()));
        });
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);

        std::unordered_map<TVDiskID, TActorId, THash<TVDiskID>> queues;
        for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
            const TVDiskID vdiskId = info->GetVDiskId(i);
            queues[vdiskId] = env.CreateQueueActor(vdiskId, NKikimrBlobStorage::EVDiskQueueId::GetFastRead, 1000);
        }

        struct TBlobInfo {
            TVDiskID VDiskId;
            TLogoBlobID BlobId;
            NKikimrProto::EReplyStatus Status;
            std::optional<TIngress> Ingress;
        };
        auto collectBlobInfo = [&] {
            std::vector<TBlobInfo> blobs;
            for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
                const TVDiskID vdiskId = info->GetVDiskId(i);
                const TActorId queueId = queues.at(vdiskId);
                const TActorId edge = runtime->AllocateEdgeActor(queueId.NodeId(), __FILE__, __LINE__);
                runtime->Send(new IEventHandle(queueId, edge, TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(
                    vdiskId, TInstant::Max(), NKikimrBlobStorage::EGetHandleClass::FastRead,
                    TEvBlobStorage::TEvVGet::EFlags::ShowInternals, {}, {{id}}).release()), queueId.NodeId());
                auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVGetResult>(edge);
                auto& record = res->Get()->Record;
                UNIT_ASSERT(record.GetStatus() == NKikimrProto::OK || record.GetStatus() == NKikimrProto::NOTREADY);
                for (auto& result : record.GetResult()) {
                    blobs.push_back({.VDiskId = vdiskId, .BlobId = LogoBlobIDFromLogoBlobID(result.GetBlobID()),
                        .Status = result.GetStatus(), .Ingress = result.HasIngress() ? std::make_optional(
                        TIngress(result.GetIngress())) : std::nullopt});
                }
            }
            return blobs;
        };
        auto dumpBlobs = [&](const char *name, const std::vector<TBlobInfo>& blobs) {
            Cerr << "Blobs(" << name <<"):" << Endl;
            for (const auto& item : blobs) {
                Cerr << item.VDiskId << " " << item.BlobId << " " << NKikimrProto::EReplyStatus_Name(item.Status)
                    << " " << (item.Ingress ? item.Ingress->ToString(&info->GetTopology(), item.VDiskId, item.BlobId) :
                    "none") << Endl;
            }
        };

        env.Sim(TDuration::Seconds(10)); // wait for blob to get synced across the group

        auto blobsInitial = collectBlobInfo();

        runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, info->GroupID, new TEvBlobStorage::TEvCollectGarbage(id.TabletID(), id.Generation(),
                0, id.Channel(), true, id.Generation(), id.Step(), new TVector<TLogoBlobID>(1, id), nullptr, TInstant::Max(),
                false));
        });
        auto res1 = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvCollectGarbageResult>(edge, false);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);

        const ui32 suspendedNodeId = 2;
        env.StopNode(suspendedNodeId);

        runtime->WrapInActorContext(edge, [&] {
            // send the sole do not keep flag
            SendToBSProxy(edge, info->GroupID, new TEvBlobStorage::TEvCollectGarbage(id.TabletID(), 0, 0, 0, false, 0,
                0, nullptr, new TVector<TLogoBlobID>(1, id), TInstant::Max(), false));
        });
        res1 = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvCollectGarbageResult>(edge, false);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);

        // sync barriers and then compact them all
        env.Sim(TDuration::Seconds(10));
        for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
            const TActorId actorId = info->GetActorId(i);
            if (actorId.NodeId() != suspendedNodeId) {
                const auto& sender = env.Runtime->AllocateEdgeActor(actorId.NodeId());
                auto ev = std::make_unique<IEventHandle>(actorId, sender, TEvCompactVDisk::Create(EHullDbType::LogoBlobs));
                ev->Rewrite(TEvBlobStorage::EvForwardToSkeleton, actorId);
                runtime->Send(ev.release(), sender.NodeId());
                auto res = env.WaitForEdgeActorEvent<TEvCompactVDiskResult>(sender);
            }
        }
        env.Sim(TDuration::Minutes(1));
        auto blobsIntermediate = collectBlobInfo();

        env.StartNode(suspendedNodeId);
        env.Sim(TDuration::Minutes(1));

        auto blobsFinal = collectBlobInfo();

        dumpBlobs("initial", blobsInitial);
        dumpBlobs("intermediate", blobsIntermediate);
        dumpBlobs("final", blobsFinal);

        for (auto& item : blobsIntermediate) {
            UNIT_ASSERT(!item.Ingress || item.Ingress->Raw() == 0);
        }

        for (auto& item : blobsFinal) {
            UNIT_ASSERT(!item.Ingress || item.Ingress->Raw() == 0);
        }
    }
}
