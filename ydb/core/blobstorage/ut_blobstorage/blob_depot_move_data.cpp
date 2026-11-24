#include <ydb/core/base/hive.h>
#include <ydb/core/base/tablet.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/blob_depot/blob_depot_tablet.h>
#include <ydb/core/blob_depot/data.h>

#include "blob_depot_event_managers.h"
#include "blob_depot_test_env.h"

using namespace NKikimr;

namespace {

ui64 GetBlobDepotTabletId(TEnvironmentSetup& env, ui32 groupId) {
    const auto baseConfig = env.FetchBaseConfig();
    for (const auto& group : baseConfig.GetGroup()) {
        if (group.GetGroupId() == groupId) {
            const ui64 tabletId = group.GetVirtualGroupInfo().GetBlobDepotId();
            UNIT_ASSERT_C(tabletId, "virtual group has no BlobDepot tablet");
            return tabletId;
        }
    }
    UNIT_FAIL("virtual group not found in base config");
    return 0;
}

TIntrusivePtr<TTabletStorageInfo> GetTabletStorageInfo(TEnvironmentSetup& env, ui64 tabletId) {
    auto& runtime = *env.Runtime;
    const TActorId sender = runtime.AllocateEdgeActor(1);
    runtime.SendToPipe(runtime.GetDomainsInfo()->GetHive(), sender,
        new TEvHive::TEvGetTabletStorageInfo(tabletId), 0, TTestActorSystem::GetPipeConfigWithRetries());

    auto response = env.WaitForEdgeActorEvent<TEvHive::TEvGetTabletStorageInfoResult>(sender);
    UNIT_ASSERT(response);
    UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Record.GetStatus(), NKikimrProto::OK,
        response->Get()->Record.GetStatusMessage());
    return TabletStorageInfoFromProto(response->Get()->Record.GetInfo());
}

struct TReassignment {
    TVector<ui32> OldGroups;
    TVector<ui32> NewGroups;
};

struct TMoveDataTest {
    TBlobDepotTestEnvironment TEnv;
    TEnvironmentSetup& Env;
    const ui32 VirtualGroup;
    const ui64 BlobDepotTabletId;
    TReassignment Reassignment;
    TBSState State;
    const TActorId Edge;
    std::vector<TBlobInfo> Blobs;

    TMoveDataTest()
        : TEnv(1, 12)
        , Env(*TEnv.Env)
        , VirtualGroup(TEnv.BlobDepot)
        , BlobDepotTabletId(GetBlobDepotTabletId(Env, VirtualGroup))
        , Edge(Env.Runtime->AllocateEdgeActor(1))
    {
        Blobs.reserve(64);

        auto& runtime = *Env.Runtime;
        runtime.SetLogPriority(NKikimrServices::BLOB_DEPOT, NLog::PRI_DEBUG);
    }

    TBlobInfo& AddBlob(ui32 cookie, ui32 gen = 1, ui32 step = 1, ui32 channel = 0, ui32 size = 1024) {
        constexpr ui64 tabletId = 100;
        State[tabletId];
        Blobs.emplace_back(TEnv.DataGen(size), tabletId, cookie, gen, step, channel);
        return Blobs.back();
    }

    void Put(TBlobInfo& blob) {
        VerifiedPut(Env, 1, VirtualGroup, blob, State, false);
    }

    void Get(TBlobInfo& blob) {
        VerifiedGet(Env, 1, VirtualGroup, blob, false, false, std::nullopt, State, false);
    }

    void Collect(TBlobInfo& blob) {
        ui32 recordGeneration = 2;
        ui32 perGenerationCounter = 1;

        VerifiedCollectGarbage(Env, 1, VirtualGroup,
            blob.Id.TabletID(), recordGeneration, perGenerationCounter, blob.Id.Channel(),
            true, blob.Id.Generation(),
            blob.Id.Step(), nullptr, nullptr,
            false, true, Blobs, State, false);
    }

    void ReassignAllChannels() {
        auto& runtime = *Env.Runtime;
        const auto oldInfo = GetTabletStorageInfo(Env, BlobDepotTabletId);

        THashSet<ui32> groups;
        for (const auto& channel : oldInfo->Channels) {
            groups.insert(channel.LatestEntry()->GroupID);
        }

        TVector<ui32> availableGroups;
        for (const ui32 groupId : TEnv.RegularGroups) {
            if (!groups.contains(groupId)) {
                availableGroups.push_back(groupId);
            }
        }
        UNIT_ASSERT_C(availableGroups.size() >= oldInfo->Channels.size(),
            "not enough groups to reassign every BlobDepot channel");

        TVector<ui32> channels;
        TVector<ui32> oldGroups, newGroups;
        for (ui32 channel = 0; channel < oldInfo->Channels.size(); ++channel) {
            channels.push_back(channel);
            oldGroups.push_back(oldInfo->Channels[channel].LatestEntry()->GroupID);
            newGroups.push_back(availableGroups[channel]);
        }

        auto sender = runtime.AllocateEdgeActor(1);
        runtime.SendToPipe(runtime.GetDomainsInfo()->GetHive(), sender,
            new TEvHive::TEvReassignTablet(BlobDepotTabletId, channels, newGroups), 0,
            TTestActorSystem::GetPipeConfigWithRetries());
        runtime.DestroyActor(sender);

        for (ui32 attempt = 0; attempt < 60; ++attempt) {
            const auto info = GetTabletStorageInfo(Env, BlobDepotTabletId);
            bool reassigned = (info->Channels.size() == newGroups.size());
            for (ui32 channel = 0; reassigned && channel < info->Channels.size(); ++channel) {
                reassigned = reassigned && (info->Channels[channel].LatestEntry()->GroupID == newGroups[channel]);
            }
            if (reassigned) {
                Reassignment = {
                    std::move(oldGroups),
                    std::move(newGroups),
                };
                return;
            }
            Env.Sim(TDuration::Seconds(1));
        }

        UNIT_FAIL("BlobDepot channels were not reassigned");
    }

    void RestartTablet() {
        auto& runtime = *Env.Runtime;
        auto sender = runtime.AllocateEdgeActor(1);
        runtime.WrapInActorContext(sender, [&] {
            TActivationContext::Register(CreateTabletKiller(BlobDepotTabletId));
        });
        runtime.DestroyActor(sender);
    }

    void SetMaxLoadedTrashRecords(ui64 limit) {
        for (const ui32 node : Env.Runtime->GetNodes()) {
            TControlBoard::SetValue(limit,
                Env.Runtime->GetNode(node)->AppData->Icb->BlobDepotControls.MaxLoadedTrashRecords);
        }
    }

    void SendMoveData(const TVector<ui32>& groups) {
        auto& runtime = *Env.Runtime;
        runtime.SendToPipe(BlobDepotTabletId, Edge, new TEvTablet::TEvMoveData(groups), 0,
            TTestActorSystem::GetPipeConfigWithRetries());
        Env.WaitForEdgeActorEvent<TEvTabletPipe::TEvClientConnected>(Edge, false);
        Env.WaitForEdgeActorEvent<TEvTabletPipe::TEvClientDestroyed>(Edge, false);
    }

    void SendMoveData() {
        SendMoveData(Reassignment.OldGroups);
    }

    void WaitMoveData(NKikimrTabletBase::TEvMoveDataResponse::EStatus status = NKikimrTabletBase::TEvMoveDataResponse::Success) {
        auto response = Env.WaitForEdgeActorEvent<TEvTablet::TEvMoveDataResponse>(Edge);
        UNIT_ASSERT_EQUAL(response->Get()->Record.GetStatus(), status);
        Cerr << response->Get()->Record.GetErrorReason() << Endl;
    }

    void MoveData() {
        SendMoveData();
        WaitMoveData();
    }
};

// Ordinary Put consumes a unique BlobSeqId. Prepare shared references through the
// normal transactional index update instead of attempting to commit that id twice.
class TTxShareBlob : public NTabletFlatExecutor::TTransactionBase<NBlobDepot::TBlobDepot> {
    const TLogoBlobID Source;
    const TLogoBlobID Target;
    bool& Done;

public:
    TTxShareBlob(NBlobDepot::TBlobDepot* self, TLogoBlobID source, TLogoBlobID target, bool& done)
        : TTransactionBase(self)
        , Source(source)
        , Target(target)
        , Done(done)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        using TData = NBlobDepot::TBlobDepot::TData;
        const auto* value = Self->Data->FindKey(TData::TKey(Source));
        UNIT_ASSERT(value);
        UNIT_ASSERT_VALUES_EQUAL(value->ValueChain.size(), 1);
        NKikimrBlobDepot::TEvCommitBlobSeq::TItem item;
        item.MutableBlobLocator()->CopyFrom(value->ValueChain[0].GetBlobLocator());
        Self->Data->UpdateKey(TData::TKey(Target), item, txc, this);
        return true;
    }

    void Complete(const TActorContext&) override {
        Self->Data->CommitTrash(this);
        Done = true;
    }
};

enum class ESharedCopyUpdate {
    None,
    OverwriteFirst,
    OverwriteSecond,
    DeleteSecond,
};

void TestSharedCopy(ESharedCopyUpdate update) {
    using TDepot = NBlobDepot::TBlobDepot;
    using TData = TDepot::TData;
    TMoveDataTest test;
    auto& runtime = *test.Env.Runtime;
    TDepot* depot = nullptr;
    runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& event) {
        if (auto* actor = dynamic_cast<TDepot*>(runtime.GetActor(event->GetRecipientRewrite()))) {
            depot = actor;
        }
        return true;
    };
    auto& first = test.AddBlob(1);
    auto& second = test.AddBlob(2);
    second.Data = first.Data;
    test.Put(first);
    test.Put(second);
    UNIT_ASSERT(depot);

    bool shared = false;
    runtime.WrapInActorContext(depot->SelfId(), [&] {
        depot->Execute(std::make_unique<TTxShareBlob>(depot, first.Id, second.Id, shared));
    });
    runtime.Sim([&] { return !shared; });
    const auto locator = [&](TLogoBlobID id) {
        const auto* value = depot->Data->FindKey(TData::TKey(id));
        UNIT_ASSERT(value);
        UNIT_ASSERT_VALUES_EQUAL(value->ValueChain.size(), 1);
        return value->ValueChain[0].GetBlobLocator().SerializeAsString();
    };
    UNIT_ASSERT_VALUES_EQUAL(locator(first.Id), locator(second.Id));
    test.Get(first);
    test.Get(second);

    test.ReassignAllChannels();
    test.RestartTablet();
    depot = nullptr;

    std::unique_ptr<IEventHandle> updateIndex;
    ui32 copies = 0;
    runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& event) {
        if (event->GetTypeRewrite() == TDepot::TEvMoveDataBlobCopied::EventType) {
            ++copies;
        }
        if (event->GetTypeRewrite() == TDepot::TEvMoveDataContinue::EventType) {
            depot = dynamic_cast<TDepot*>(runtime.GetActor(event->GetRecipientRewrite()));
            UNIT_ASSERT(depot);
            const auto& state = depot->MoveData;
            if (!updateIndex && state.Phase == TDepot::TMoveDataState::EPhase::UpdatingIndex &&
                    state.Key == std::optional<TString>(second.Id.AsBinaryString())) {
                UNIT_ASSERT_VALUES_EQUAL(copies, 1);
                UNIT_ASSERT(state.ProtectedBlobSeqIds.empty());
                updateIndex = std::move(event);
                return false;
            }
        }
        return true;
    };
    test.SendMoveData();
    runtime.Sim([&] { return !updateIndex; });

    const TString movedLocator = locator(first.Id);
    UNIT_ASSERT_VALUES_EQUAL(movedLocator, depot->MoveData.NewBlobLocator.SerializeAsString());
    TString expectedSecondLocator;
    switch (update) {
        case ESharedCopyUpdate::None:
            break;
        case ESharedCopyUpdate::OverwriteFirst:
            // Remove the last reference to the selected target copy.
            test.Put(first);
            test.Env.Sim(TDuration::Seconds(5));
            break;
        case ESharedCopyUpdate::OverwriteSecond:
            test.Put(second);
            expectedSecondLocator = locator(second.Id);
            UNIT_ASSERT_VALUES_UNEQUAL(expectedSecondLocator, movedLocator);
            UNIT_ASSERT(depot->Data->FindKey(TData::TKey(second.Id))->ValueVersion != depot->MoveData.ValueVersion);
            break;
        case ESharedCopyUpdate::DeleteSecond:
            // Preserve the target copy through the first key while deleting
            // only the current key, so the transaction reaches KeyMissing.
            VerifiedCollectGarbage(test.Env, 1, test.VirtualGroup,
                second.Id.TabletID(), 2, 1, second.Id.Channel(), true,
                second.Id.Generation(), second.Id.Step(),
                new TVector<TLogoBlobID>{first.Id}, new TVector<TLogoBlobID>{second.Id},
                false, false, test.Blobs, test.State, false);
            UNIT_ASSERT(!depot->Data->FindKey(TData::TKey(second.Id)));
            break;
    }
    if (update == ESharedCopyUpdate::OverwriteSecond || update == ESharedCopyUpdate::DeleteSecond) {
        UNIT_ASSERT_VALUES_EQUAL(locator(first.Id), movedLocator);
        UNIT_ASSERT(depot->MoveData.RecordTouched);
        UNIT_ASSERT(!depot->MoveData.NeedsAnotherPass);
        UNIT_ASSERT(depot->MoveData.Phase == TDepot::TMoveDataState::EPhase::UpdatingIndex);
    }
    runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& event) {
        if (event->GetTypeRewrite() == TDepot::TEvMoveDataBlobCopied::EventType) {
            ++copies;
        }
        return true;
    };
    const ui32 nodeId = updateIndex->Sender.NodeId();
    runtime.Send(std::move(updateIndex), nodeId);
    test.WaitMoveData();
    UNIT_ASSERT_VALUES_EQUAL(copies, update == ESharedCopyUpdate::OverwriteFirst ? 2 : 1);
    test.Get(first);
    if (update == ESharedCopyUpdate::DeleteSecond) {
        UNIT_ASSERT(!depot->Data->FindKey(TData::TKey(second.Id)));
    } else {
        test.Get(second);
        if (update == ESharedCopyUpdate::OverwriteSecond) {
            UNIT_ASSERT_VALUES_EQUAL(locator(second.Id), expectedSecondLocator);
        } else if (update == ESharedCopyUpdate::None) {
            UNIT_ASSERT_VALUES_EQUAL(locator(second.Id), movedLocator);
        }
    }
    runtime.FilterFunction = {};
}

void TestNeedsAnotherPass(bool changeValueChain) {
    using TDepot = NBlobDepot::TBlobDepot;
    using TData = TDepot::TData;
    TMoveDataTest test;
    auto& runtime = *test.Env.Runtime;
    auto& earlier = test.AddBlob(1);
    auto& source = test.AddBlob(2);
    auto& later = test.AddBlob(3);
    earlier.Data = source.Data;
    test.Put(source);
    test.Put(later);
    test.ReassignAllChannels();
    test.RestartTablet();
    test.Put(earlier); // The scan passes this key without copying it.

    THashSet<ui32> oldGroups;
    for (ui32 groupId : test.Reassignment.OldGroups) {
        oldGroups.insert(groupId);
    }

    std::unique_ptr<IEventHandle> copied;
    runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& event) {
        if (event->GetTypeRewrite() == TDepot::TEvMoveDataBlobCopied::EventType) {
            UNIT_ASSERT(!copied);
            UNIT_ASSERT(event->Get<TDepot::TEvMoveDataBlobCopied>()->Result ==
                TDepot::TEvMoveDataBlobCopied::EResult::OK);
            copied = std::move(event);
            return false;
        }
        return true;
    };
    test.SendMoveData();
    runtime.Sim([&] { return !copied; });

    auto* depot = dynamic_cast<TDepot*>(runtime.GetActor(copied->GetRecipientRewrite()));
    UNIT_ASSERT(depot);
    UNIT_ASSERT(depot->MoveData.Phase == TDepot::TMoveDataState::EPhase::CopyingBlob);
    UNIT_ASSERT(depot->MoveData.Key == std::optional<TString>(source.Id.AsBinaryString()));
    UNIT_ASSERT(depot->MoveData.RecordsScanned >= 1);
    UNIT_ASSERT(!depot->MoveData.NeedsAnotherPass);
    UNIT_ASSERT(!depot->MoveData.RecordTouched);

    const TBlobInfo& updated = changeValueChain ? earlier : later;
    const auto key = TData::TKey(updated.Id);
    const auto* value = depot->Data->FindKey(key);
    UNIT_ASSERT(value);
    UNIT_ASSERT_VALUES_EQUAL(value->ValueChain.size(), 1);
    const ui32 version = value->ValueVersion;
    const TString chain = value->ValueChain[0].SerializeAsString();
    UNIT_ASSERT_VALUES_EQUAL(oldGroups.contains(value->ValueChain[0].GetBlobLocator().GetGroupId()), !changeValueChain);

    if (changeValueChain) {
        // Reintroduce an old-group reference behind the scan cursor. The source
        // key still references that physical blob, so it cannot have been GC'd.
        bool done = false;
        runtime.WrapInActorContext(depot->SelfId(), [&] {
            depot->Execute(std::make_unique<TTxShareBlob>(depot, source.Id, earlier.Id, done));
        });
        runtime.Sim([&] { return !done; });
    } else {
        UNIT_ASSERT(value->KeepState == NKikimrBlobDepot::EKeepState::Default);
        VerifiedCollectGarbage(test.Env, 1, test.VirtualGroup,
            later.Id.TabletID(), 2, 1, later.Id.Channel(), false, 0, 0,
            new TVector<TLogoBlobID>{later.Id}, nullptr, false, false,
            test.Blobs, test.State, false);
    }

    value = depot->Data->FindKey(key);
    UNIT_ASSERT(value);
    if (changeValueChain) {
        UNIT_ASSERT_VALUES_EQUAL(value->ValueVersion, version + 1);
        UNIT_ASSERT_VALUES_UNEQUAL(value->ValueChain[0].SerializeAsString(), chain);
        UNIT_ASSERT_VALUES_EQUAL(value->ValueChain[0].GetBlobLocator().SerializeAsString(),
            depot->MoveData.BlobLocator.SerializeAsString());
    } else {
        UNIT_ASSERT(value->KeepState == NKikimrBlobDepot::EKeepState::Keep);
        UNIT_ASSERT_VALUES_EQUAL(value->ValueVersion, version);
        UNIT_ASSERT_VALUES_EQUAL(value->ValueChain[0].SerializeAsString(), chain);
    }
    UNIT_ASSERT(!depot->MoveData.RecordTouched); // Neither update touches the key being copied.
    UNIT_ASSERT_VALUES_EQUAL(depot->MoveData.NeedsAnotherPass, changeValueChain);

    runtime.FilterFunction = {};
    const ui32 nodeId = copied->GetRecipientRewrite().NodeId();
    runtime.Send(std::move(copied), nodeId);
    test.WaitMoveData();
    for (auto& blob : test.Blobs) {
        test.Get(blob);
        value = depot->Data->FindKey(TData::TKey(blob.Id));
        UNIT_ASSERT(value);
        UNIT_ASSERT_VALUES_EQUAL(value->ValueChain.size(), 1);
        UNIT_ASSERT(!oldGroups.contains(value->ValueChain[0].GetBlobLocator().GetGroupId()));
    }
}

enum class EMovingRecordUpdate {
    Overwrite,
    Keep,
    Delete,
};

void TestRecordTouched(EMovingRecordUpdate update) {
    using TDepot = NBlobDepot::TBlobDepot;
    using TData = TDepot::TData;
    TMoveDataTest test;
    auto& runtime = *test.Env.Runtime;
    auto& blob = test.AddBlob(1);
    test.Put(blob);
    test.ReassignAllChannels();
    test.RestartTablet();

    std::unique_ptr<IEventHandle> copied;
    runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& event) {
        if (event->GetTypeRewrite() == TDepot::TEvMoveDataBlobCopied::EventType) {
            UNIT_ASSERT(!copied);
            UNIT_ASSERT(event->Get<TDepot::TEvMoveDataBlobCopied>()->Result ==
                TDepot::TEvMoveDataBlobCopied::EResult::OK);
            copied = std::move(event);
            return false;
        }
        return true;
    };
    test.SendMoveData();
    runtime.Sim([&] { return !copied; });

    auto* depot = dynamic_cast<TDepot*>(runtime.GetActor(copied->GetRecipientRewrite()));
    UNIT_ASSERT(depot);
    UNIT_ASSERT(depot->MoveData.Phase == TDepot::TMoveDataState::EPhase::CopyingBlob);
    UNIT_ASSERT(depot->MoveData.Key == std::optional<TString>(blob.Id.AsBinaryString()));
    UNIT_ASSERT(!depot->MoveData.RecordTouched);
    UNIT_ASSERT(!depot->MoveData.NeedsAnotherPass);
    const auto key = TData::TKey(blob.Id);
    const auto* value = depot->Data->FindKey(key);
    UNIT_ASSERT(value);
    UNIT_ASSERT_VALUES_EQUAL(value->ValueChain.size(), 1);
    const ui32 version = value->ValueVersion;
    const TString originalLocator = value->ValueChain[0].GetBlobLocator().SerializeAsString();
    TString expectedLocator = copied->Get<TDepot::TEvMoveDataBlobCopied>()->NewLocator.SerializeAsString();

    switch (update) {
        case EMovingRecordUpdate::Overwrite:
            test.Put(blob);
            value = depot->Data->FindKey(key);
            UNIT_ASSERT(value);
            UNIT_ASSERT_VALUES_EQUAL(value->ValueVersion, version + 1);
            expectedLocator = value->ValueChain[0].GetBlobLocator().SerializeAsString();
            UNIT_ASSERT_VALUES_UNEQUAL(expectedLocator, originalLocator);
            break;
        case EMovingRecordUpdate::Keep:
            UNIT_ASSERT(value->KeepState == NKikimrBlobDepot::EKeepState::Default);
            VerifiedCollectGarbage(test.Env, 1, test.VirtualGroup,
                blob.Id.TabletID(), 2, 1, blob.Id.Channel(), false, 0, 0,
                new TVector<TLogoBlobID>{blob.Id}, nullptr, false, false,
                test.Blobs, test.State, false);
            value = depot->Data->FindKey(key);
            UNIT_ASSERT(value);
            UNIT_ASSERT(value->KeepState == NKikimrBlobDepot::EKeepState::Keep);
            UNIT_ASSERT_VALUES_EQUAL(value->ValueVersion, version);
            UNIT_ASSERT_VALUES_EQUAL(value->ValueChain[0].GetBlobLocator().SerializeAsString(), originalLocator);
            break;
        case EMovingRecordUpdate::Delete:
            test.Collect(blob);
            UNIT_ASSERT(!depot->Data->FindKey(key));
            break;
    }
    UNIT_ASSERT_VALUES_EQUAL(depot->MoveData.RecordTouched, update != EMovingRecordUpdate::Keep);
    UNIT_ASSERT(!depot->MoveData.NeedsAnotherPass);

    runtime.FilterFunction = {};
    const ui32 nodeId = copied->GetRecipientRewrite().NodeId();
    runtime.Send(std::move(copied), nodeId);
    test.WaitMoveData();

    if (update == EMovingRecordUpdate::Delete) {
        UNIT_ASSERT(!depot->Data->FindKey(key)); // The completed copy must not resurrect the key.
    } else {
        test.Get(blob);
        value = depot->Data->FindKey(key);
        UNIT_ASSERT(value);
        UNIT_ASSERT_VALUES_EQUAL(value->ValueChain[0].GetBlobLocator().SerializeAsString(), expectedLocator);
        if (update == EMovingRecordUpdate::Keep) {
            UNIT_ASSERT(value->KeepState == NKikimrBlobDepot::EKeepState::Keep);
        }
    }
    UNIT_ASSERT(depot->MoveData.ProtectedBlobSeqIds.empty());
}

enum class ENodataSourceUpdate {
    None,
    Overwrite,
    Delete,
};

void TestCopyNodata(ENodataSourceUpdate update) {
    using TDepot = NBlobDepot::TBlobDepot;
    using TData = TDepot::TData;
    TMoveDataTest test;
    auto& runtime = *test.Env.Runtime;
    auto& blob = test.AddBlob(1);
    test.Put(blob);
    test.ReassignAllChannels();
    test.RestartTablet();

    const bool expectLoss = update == ENodataSourceUpdate::None;
    TDepot* depot = nullptr;
    TActorId copier;
    TLogoBlobID sourceId;
    ui32 sourceGroup = 0;
    std::unique_ptr<IEventHandle> get;
    ui32 getNodeId = 0;
    bool getCaptured = false;
    ui32 nodataReads = 0;
    ui32 nodataCopies = 0;
    ui32 copyPuts = 0;
    bool poison = false;
    std::unique_ptr<IEventHandle> response;

    runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& event) {
        const ui32 type = event->GetTypeRewrite();
        if (!depot && type == TDepot::TEvMoveDataContinue::EventType) {
            depot = dynamic_cast<TDepot*>(runtime.GetActor(event->GetRecipientRewrite()));
            UNIT_ASSERT(depot);
        }
        if (!getCaptured && depot && type == TEvBlobStorage::EvGet && event->Sender == depot->CopyBlobActorId) {
            copier = event->Sender;
            getNodeId = nodeId;
            getCaptured = true;
            get = std::move(event);
            return false;
        }
        if (type == TEvBlobStorage::EvGetResult && event->GetRecipientRewrite() == copier) {
            auto* msg = event->Get<TEvBlobStorage::TEvGetResult>();
            UNIT_ASSERT_VALUES_EQUAL(msg->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(msg->GroupId, sourceGroup);
            UNIT_ASSERT_VALUES_EQUAL(msg->ResponseSz, 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Id, sourceId);
            if (expectLoss) {
                // Simulate lost data while the logical record still references it.
                msg->Responses[0].Status = NKikimrProto::NODATA;
            }
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Status, NKikimrProto::NODATA);
            ++nodataReads;
        } else if (type == TEvBlobStorage::EvPut && event->Sender == copier) {
            ++copyPuts;
        } else if (type == TDepot::TEvMoveDataBlobCopied::EventType && event->Sender == copier) {
            UNIT_ASSERT(event->Get<TDepot::TEvMoveDataBlobCopied>()->Result ==
                TDepot::TEvMoveDataBlobCopied::EResult::NODATA);
            ++nodataCopies;
        } else if (depot && type == TEvents::TSystem::Poison &&
                event->Sender == depot->SelfId() && event->GetRecipientRewrite() == depot->SelfId()) {
            poison = true;
            return false; // Inspect the loss decision without restarting the tablet.
        } else if (type == TEvTablet::EvMoveDataResponse && event->GetRecipientRewrite() == test.Edge) {
            response = std::move(event);
            return false;
        }
        return true;
    };
    test.SendMoveData();

    auto deadline = runtime.GetClock() + TDuration::Seconds(30);
    runtime.Sim([&] { return !get && runtime.GetClock() < deadline; });

    UNIT_ASSERT(get);
    UNIT_ASSERT(depot->MoveData.Phase == TDepot::TMoveDataState::EPhase::CopyingBlob);
    sourceId = depot->MoveData.BlobId;
    sourceGroup = depot->MoveData.BlobLocator.GetGroupId();
    const auto targetSeqId = depot->MoveData.NewBlobSeqId;
    UNIT_ASSERT_VALUES_EQUAL(get->Get<TEvBlobStorage::TEvGet>()->QuerySize, 1);
    UNIT_ASSERT_VALUES_EQUAL(get->Get<TEvBlobStorage::TEvGet>()->Queries[0].Id, sourceId);

    const auto key = TData::TKey(blob.Id);
    const TString originalLocator = depot->Data->FindKey(key)->ValueChain[0].GetBlobLocator().SerializeAsString();
    TString expectedLocator = originalLocator;
    if (update == ENodataSourceUpdate::Overwrite) {
        test.Put(blob);
        expectedLocator = depot->Data->FindKey(key)->ValueChain[0].GetBlobLocator().SerializeAsString();
        UNIT_ASSERT_VALUES_UNEQUAL(expectedLocator, originalLocator);
    } else if (update == ENodataSourceUpdate::Delete) {
        test.Collect(blob);
        UNIT_ASSERT(!depot->Data->FindKey(key));
    }
    UNIT_ASSERT_VALUES_EQUAL(depot->MoveData.RecordTouched, !expectLoss);

    if (!expectLoss) {
        UNIT_ASSERT(!depot->Data->IsBlobReferenced(sourceId));
        const auto gcPending = [&] {
            const auto& record = depot->Data->GetRecordsPerChannelGroup(sourceId.Channel(), sourceGroup);
            return record.Trash.contains(sourceId) || record.CollectGarbageRequestsInFlight;
        };
        deadline = runtime.GetClock() + TDuration::Seconds(60);
        runtime.Sim([&] { return gcPending() && runtime.GetClock() < deadline; });
        UNIT_ASSERT_C(!gcPending(), "source GC did not finish");
        // Collected data can remain readable until VDisk compaction.
        test.Env.Sim(TDuration::Seconds(10));
        const auto info = test.Env.GetGroupInfo(sourceGroup);
        for (const auto& vdisk : info->GetVDisks()) {
            test.Env.CompactVDisk(info->GetActorId(vdisk.VDiskIdShort));
        }
    } else {
        UNIT_ASSERT(depot->Data->IsBlobReferenced(sourceId));
    }

    runtime.Send(std::move(get), getNodeId);
    deadline = runtime.GetClock() + TDuration::Seconds(30);
    runtime.Sim([&] { return !response && !poison && runtime.GetClock() < deadline; });
    UNIT_ASSERT_C(response || poison, "move data neither completed nor reported data loss");
    UNIT_ASSERT_VALUES_EQUAL(nodataReads, 1);
    UNIT_ASSERT_VALUES_EQUAL(nodataCopies, 1);
    UNIT_ASSERT_VALUES_EQUAL(copyPuts, 0);
    UNIT_ASSERT_VALUES_EQUAL(poison, expectLoss);
    UNIT_ASSERT(!depot->Channels[targetSeqId.Channel].AssimilatedBlobsInFlight.contains(targetSeqId.ToSequentialNumber()));
    runtime.FilterFunction = {};
    if (expectLoss) {
        UNIT_ASSERT(!response);
        UNIT_ASSERT_VALUES_EQUAL(depot->Data->FindKey(key)->ValueChain[0].GetBlobLocator().SerializeAsString(), originalLocator);
    } else {
        UNIT_ASSERT(response);
        const ui32 nodeId = response->GetRecipientRewrite().NodeId();
        runtime.Send(std::move(response), nodeId);
        test.WaitMoveData();
        if (update == ENodataSourceUpdate::Delete) {
            UNIT_ASSERT(!depot->Data->FindKey(key));
        } else {
            test.Get(blob);
            UNIT_ASSERT_VALUES_EQUAL(depot->Data->FindKey(key)->ValueChain[0].GetBlobLocator().SerializeAsString(), expectedLocator);
        }
    }
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(BlobDepotMoveData) {

    Y_UNIT_TEST(OneBlob) {
        TMoveDataTest test;
        auto& blob = test.AddBlob(1);
        test.Put(blob);

        test.ReassignAllChannels();
        test.RestartTablet();

        test.MoveData();

        test.Get(blob);
    }

    Y_UNIT_TEST(OldAndNewBlob) {
        TMoveDataTest test;
        auto& oldBlob = test.AddBlob(1);
        test.Put(oldBlob);

        test.ReassignAllChannels();
        test.RestartTablet();

        auto& newBlob = test.AddBlob(2);
        test.Put(newBlob);

        test.MoveData();

        test.Get(oldBlob);
        test.Get(newBlob);
    }

    Y_UNIT_TEST(ManyBlobs) {
        TMoveDataTest test;
        for (ui32 cookie = 1; cookie <= 32; ++cookie) {
            test.Put(test.AddBlob(cookie, 1, 1, 0, 128 + cookie * 31));
        }

        test.ReassignAllChannels();
        test.RestartTablet();

        test.MoveData();

        for (auto& blob : test.Blobs) {
            test.Get(blob);
        }
    }

    Y_UNIT_TEST(BlobDeletedBeforeMove) {
        TMoveDataTest test;

        std::unique_ptr<IEventHandle> eventCopyBlob;
        bool stop = false;
        test.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& event) -> bool {
            if (!stop && event->GetTypeRewrite() == NBlobDepot::TBlobDepot::TEvMoveDataContinue::EventType) {
                eventCopyBlob = std::move(event);
                stop = true;
                return false;
            }
            return true;
        };

        auto& blob = test.AddBlob(1);
        test.Put(blob);

        test.ReassignAllChannels();
        test.RestartTablet();

        test.SendMoveData();

        test.Env.Runtime->Sim([&] {
            return stop;
        });

        test.Collect(blob);

        UNIT_ASSERT(eventCopyBlob);
        auto nodeId = eventCopyBlob->Sender.NodeId();
        test.Env.Runtime->Send(std::move(eventCopyBlob), nodeId);

        test.WaitMoveData();
    }

    Y_UNIT_TEST(BlobDeletedAfterMove) {
        TMoveDataTest test;

        std::unique_ptr<IEventHandle> eventBlobCopied;
        bool stop = false;
        test.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& event) -> bool {
            if (!stop && event->GetTypeRewrite() == NBlobDepot::TBlobDepot::TEvMoveDataBlobCopied::EventType) {
                eventBlobCopied = std::move(event);
                stop = true;
                return false;
            }
            return true;
        };

        auto& blob = test.AddBlob(1);
        test.Put(blob);

        test.ReassignAllChannels();
        test.RestartTablet();

        test.SendMoveData();

        test.Env.Runtime->Sim([&] {
            return stop;
        });

        test.Collect(blob);

        UNIT_ASSERT(eventBlobCopied);
        auto nodeId = eventBlobCopied->Sender.NodeId();
        test.Env.Runtime->Send(std::move(eventBlobCopied), nodeId);

        test.WaitMoveData();
    }

    // the agent checks the blob it has cached the mapping for, while the value has been moved to another blob since;
    // the old blob is reported missing, and the agent resolves the key again to check the moved blob, rather than
    // reporting NODATA
    Y_UNIT_TEST(CheckIntegrityAfterMove) {
        TMoveDataTest test;
        auto& env = test.Env;
        auto& blob = test.AddBlob(1);
        test.Put(blob);

        std::set<TLogoBlobID> checked; // physical blobs checked by the agent
        bool oldBlobsMissing = false;
        ui32 numMissing = 0;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvCheckIntegrity) {
                const TLogoBlobID& id = ev->Get<TEvBlobStorage::TEvCheckIntegrity>()->Id;
                if (id != blob.Id && !oldBlobsMissing) {
                    checked.insert(id);
                }
            } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvCheckIntegrityResult && oldBlobsMissing) {
                auto *msg = ev->Get<TEvBlobStorage::TEvCheckIntegrityResult>();
                if (checked.contains(msg->Id)) {
                    msg->Status = NKikimrProto::NODATA;
                    ++numMissing;
                }
            }
            return true;
        };

        auto check = [&] {
            const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
            env.Runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, test.VirtualGroup, new TEvBlobStorage::TEvCheckIntegrity(blob.Id, TInstant::Max(),
                    NKikimrBlobStorage::FastRead));
            });
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvCheckIntegrityResult>(edge);
            Cerr << "*** " << res->Get()->ToString() << Endl;
            return res->Get()->Status;
        };

        UNIT_ASSERT_VALUES_EQUAL(check(), NKikimrProto::OK); // the agent caches the mapping
        UNIT_ASSERT(!checked.empty());

        test.ReassignAllChannels();
        test.RestartTablet();
        test.MoveData();

        oldBlobsMissing = true;
        UNIT_ASSERT_VALUES_EQUAL(check(), NKikimrProto::OK);
        UNIT_ASSERT(numMissing); // the stale mapping was used indeed
        env.Runtime->FilterFunction = {};
    }

    Y_UNIT_TEST(GetWithStaleLocatorDuringMove) {
        using TDepot = NBlobDepot::TBlobDepot;
        TMoveDataTest test;
        auto& blob = test.AddBlob(1);
        test.Put(blob);
        test.ReassignAllChannels();
        test.RestartTablet();
        test.Get(blob); // Cache the source locator before Move Data starts.

        auto& runtime = *test.Env.Runtime;
        const auto reader = runtime.AllocateEdgeActor(1);
        TDepot* depot = nullptr;
        TActorId agent;
        TLogoBlobID sourceId;
        TLogoBlobID targetId;
        ui32 sourceGroup = 0;
        ui32 targetGroup = 0;
        ui32 targetVersion = 0;
        std::unique_ptr<IEventHandle> get;
        ui32 getNodeId = 0;
        bool captured = false;
        ui32 missingReads = 0;
        ui32 extraResolves = 0;
        ui32 retriedReads = 0;

        runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& event) {
            const ui32 type = event->GetTypeRewrite();
            if (type == TEvBlobStorage::EvGet) {
                const auto* msg = event->Get<TEvBlobStorage::TEvGet>();
                if (!captured && msg->QuerySize == 1 &&
                        msg->Queries[0].Id.TabletID() == test.BlobDepotTabletId &&
                        msg->Queries[0].Id.Channel() >= NBlobDepot::BaseDataChannel) {
                    sourceId = msg->Queries[0].Id;
                    agent = event->Sender;
                    getNodeId = nodeId;
                    captured = true;
                    get = std::move(event);
                    return false;
                }
            } else if (!depot && type == TDepot::TEvMoveDataContinue::EventType) {
                depot = dynamic_cast<TDepot*>(runtime.GetActor(event->GetRecipientRewrite()));
                UNIT_ASSERT(depot);
                UNIT_ASSERT_VALUES_EQUAL(depot->MoveData.BlobId, sourceId);
                sourceGroup = depot->MoveData.BlobLocator.GetGroupId();
            } else if (depot && type == TEvBlobStorage::EvPut && event->Sender == depot->CopyBlobActorId) {
                UNIT_ASSERT(!targetId);
                targetId = event->Get<TEvBlobStorage::TEvPut>()->Id;
            } else if (type == TEvBlobStorage::EvGetResult && event->GetRecipientRewrite() == agent) {
                const auto* msg = event->Get<TEvBlobStorage::TEvGetResult>();
                UNIT_ASSERT_VALUES_EQUAL(msg->Status, NKikimrProto::OK);
                UNIT_ASSERT_VALUES_EQUAL(msg->ResponseSz, 1);
                if (msg->Responses[0].Id == sourceId) {
                    UNIT_ASSERT_VALUES_EQUAL(msg->GroupId, sourceGroup);
                    UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Status, NKikimrProto::NODATA);
                    ++missingReads;
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(extraResolves, 1);
                    UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Id, targetId);
                    UNIT_ASSERT_VALUES_EQUAL(msg->GroupId, targetGroup);
                    UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Status, NKikimrProto::OK);
                    ++retriedReads;
                }
            } else if (type == TEvBlobDepot::EvResolveResult && event->GetRecipientRewrite() == agent) {
                const auto& record = event->Get<TEvBlobDepot::TEvResolveResult>()->Record;
                UNIT_ASSERT_VALUES_EQUAL(missingReads, 1);
                UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrProto::OK);
                UNIT_ASSERT_VALUES_EQUAL(record.ResolvedKeysSize(), 1);
                const auto& key = record.GetResolvedKeys(0);
                UNIT_ASSERT_VALUES_EQUAL(key.GetKey(), blob.Id.AsBinaryString());
                UNIT_ASSERT_VALUES_EQUAL(key.GetValueVersion(), targetVersion);
                UNIT_ASSERT_VALUES_EQUAL(key.ValueChainSize(), 1);
                UNIT_ASSERT_VALUES_EQUAL(LogoBlobIDFromLogoBlobID(key.GetValueChain(0).GetBlobId()), targetId);
                UNIT_ASSERT_VALUES_EQUAL(key.GetValueChain(0).GetGroupId(), targetGroup);
                ++extraResolves;
            }
            return true;
        };

        SendTEvGet(test.Env, reader, test.VirtualGroup, blob.Id);
        const auto deadline = runtime.GetClock() + TDuration::Seconds(30);
        runtime.Sim([&] { return !get && runtime.GetClock() < deadline; });
        UNIT_ASSERT(get);

        // Move Data and its source GC finish while the reader still holds X.
        test.MoveData();
        UNIT_ASSERT(depot);
        UNIT_ASSERT(targetId);
        UNIT_ASSERT_VALUES_UNEQUAL(targetId, sourceId);
        UNIT_ASSERT(!depot->Data->IsBlobReferenced(sourceId));
        const auto* value = depot->Data->FindKey(TDepot::TData::TKey(blob.Id));
        UNIT_ASSERT(value);
        UNIT_ASSERT_VALUES_EQUAL(value->ValueChain.size(), 1);
        targetVersion = value->ValueVersion;
        targetGroup = value->ValueChain[0].GetBlobLocator().GetGroupId();
        UNIT_ASSERT_VALUES_UNEQUAL(targetGroup, sourceGroup);
        UNIT_ASSERT_VALUES_EQUAL(extraResolves, 0);

        // GC'd data can remain readable until the VDisks compact it.
        test.Env.Sim(TDuration::Seconds(10));
        const auto info = test.Env.GetGroupInfo(sourceGroup);
        for (const auto& vdisk : info->GetVDisks()) {
            test.Env.CompactVDisk(info->GetActorId(vdisk.VDiskIdShort));
        }

        runtime.Send(std::move(get), getNodeId);
        const auto result = test.Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(reader);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->ResponseSz, 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Responses[0].Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Responses[0].Id, blob.Id);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Responses[0].Buffer.ConvertToString(), blob.Data);
        UNIT_ASSERT_VALUES_EQUAL(missingReads, 1);
        UNIT_ASSERT_VALUES_EQUAL(extraResolves, 1);
        UNIT_ASSERT_VALUES_EQUAL(retriedReads, 1);
        runtime.FilterFunction = {};
    }

    Y_UNIT_TEST(TrashNotEmptyDuringCheckTrash) {
        TMoveDataTest test;

        auto& blob = test.AddBlob(1);
        test.Put(blob);

        test.ReassignAllChannels();
        test.RestartTablet();
        test.Env.Sim(TDuration::Seconds(5));

        constexpr ui32 firstDataChannel = 2;
        bool blobCopied = false;
        ui32 continuesAfterCopy = 0;
        bool holdGC = true;
        bool responded = false;
        std::vector<std::unique_ptr<IEventHandle>> heldGC;

        test.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& event) -> bool {
            switch (event->GetTypeRewrite()) {
                case NBlobDepot::TBlobDepot::TEvMoveDataBlobCopied::EventType:
                    blobCopied = true;
                    break;

                case NBlobDepot::TBlobDepot::TEvMoveDataContinue::EventType:
                    if (blobCopied) {
                        ++continuesAfterCopy;
                    }
                    break;

                case TEvBlobStorage::EvCollectGarbage:
                    if (holdGC && blobCopied) {
                        auto *msg = event->Get<TEvBlobStorage::TEvCollectGarbage>();
                        if (msg->TabletId == test.BlobDepotTabletId && msg->Channel >= firstDataChannel) {
                            heldGC.push_back(std::move(event));
                            return false;
                        }
                    }
                    break;

                case TEvTablet::EvMoveDataResponse:
                    responded = true;
                    break;
            }
            return true;
        };

        test.SendMoveData();

        // the first continue after copy switches to index update, the second one comes from the final scan
        // and switches to the trash check phase
        test.Env.Runtime->Sim([&] {
            return continuesAfterCopy >= 2 && !heldGC.empty();
        });

        test.Env.Sim(TDuration::Seconds(10));
        UNIT_ASSERT_C(!responded, "move data finished while trash of moved groups was not collected");

        holdGC = false;
        for (auto& event : heldGC) {
            const ui32 nodeId = event->Sender.NodeId();
            test.Env.Runtime->Send(std::move(event), nodeId);
        }
        heldGC.clear();

        test.WaitMoveData();
        test.Get(blob);
    }

    Y_UNIT_TEST(MoveDataBeforeIndexAndTrashLoaded) {
        TMoveDataTest test;

        constexpr ui32 firstDataChannel = 2;
        bool holdGC = false;
        bool responded = false;
        std::vector<std::unique_ptr<IEventHandle>> heldGC;

        test.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& event) -> bool {
            switch (event->GetTypeRewrite()) {
                case TEvBlobStorage::EvCollectGarbage:
                    if (holdGC) {
                        auto *msg = event->Get<TEvBlobStorage::TEvCollectGarbage>();
                        if (msg->TabletId == test.BlobDepotTabletId && msg->Channel >= firstDataChannel) {
                            heldGC.push_back(std::move(event));
                            return false;
                        }
                    }
                    break;

                case TEvTablet::EvMoveDataResponse:
                    responded = true;
                    break;
            }
            return true;
        };

        for (ui32 cookie = 1; cookie <= 4; ++cookie) {
            test.Put(test.AddBlob(cookie, 1, cookie));
        }
        auto& keptBlob = test.AddBlob(10, 1, 10);
        test.Put(keptBlob);

        // keep trash records of collected blobs in the BlobDepot database
        holdGC = true;
        test.Collect(test.Blobs[3]);
        test.Env.Runtime->Sim([&] {
            return !heldGC.empty();
        });

        test.ReassignAllChannels();

        // with this limit trash loading stops after the first record and can't continue while GC is held
        test.SetMaxLoadedTrashRecords(1);
        heldGC.clear();
        test.RestartTablet();

        // no simulation between restart and the request: it may arrive before the index is loaded
        test.SendMoveData();

        test.Env.Sim(TDuration::Seconds(10));
        UNIT_ASSERT_C(!responded, "move data finished while trash was not fully loaded");
        UNIT_ASSERT(!heldGC.empty());

        test.SetMaxLoadedTrashRecords(1'000'000);
        holdGC = false;
        for (auto& event : heldGC) {
            const ui32 nodeId = event->Sender.NodeId();
            test.Env.Runtime->Send(std::move(event), nodeId);
        }
        heldGC.clear();

        test.WaitMoveData();
        test.Get(keptBlob);
    }

    Y_UNIT_TEST(MoveDataCanceledByYellowStop) {
        TMoveDataTest test;
        auto& blob = test.AddBlob(1);
        test.Put(blob);

        test.ReassignAllChannels();
        test.RestartTablet();

        test.Env.SetPDiskStatusFlags(1, 1000, NKikimrBlobStorage::TPDiskSpaceColor::YELLOW);

        test.SendMoveData();
        test.WaitMoveData(NKikimrTabletBase::TEvMoveDataResponse::NotEnoughSpace);
    }

    Y_UNIT_TEST(MoveDataGroupIdMismatch) {
        TMoveDataTest test;
        auto& blob = test.AddBlob(1);
        test.Put(blob);

        test.ReassignAllChannels();
        test.RestartTablet();

        test.SendMoveData({test.Reassignment.NewGroups[0]});
        test.WaitMoveData(NKikimrTabletBase::TEvMoveDataResponse::ErrorGroupIdMismatch);

        test.Get(blob);
    }

    Y_UNIT_TEST(SharedCopy) {
        TestSharedCopy(ESharedCopyUpdate::None);
    }

    Y_UNIT_TEST(SharedCopyOverwrittenBeforeIndexUpdate) {
        TestSharedCopy(ESharedCopyUpdate::OverwriteFirst);
    }

    Y_UNIT_TEST(CurrentKeyOverwrittenBeforeIndexUpdate) {
        TestSharedCopy(ESharedCopyUpdate::OverwriteSecond);
    }

    Y_UNIT_TEST(CurrentKeyDeletedBeforeIndexUpdate) {
        TestSharedCopy(ESharedCopyUpdate::DeleteSecond);
    }

    Y_UNIT_TEST(ValueChainChangeNeedsAnotherPass) {
        TestNeedsAnotherPass(true);
    }

    Y_UNIT_TEST(KeepFlagsDoNotNeedAnotherPass) {
        TestNeedsAnotherPass(false);
    }

    Y_UNIT_TEST(RecordTouchedOnOverwrite) {
        TestRecordTouched(EMovingRecordUpdate::Overwrite);
    }

    Y_UNIT_TEST(RecordTouchedOnDelete) {
        TestRecordTouched(EMovingRecordUpdate::Delete);
    }

    Y_UNIT_TEST(KeepDoesNotTouchMovingRecord) {
        TestRecordTouched(EMovingRecordUpdate::Keep);
    }

    Y_UNIT_TEST(CopyNodataAfterOverwrite) {
        TestCopyNodata(ENodataSourceUpdate::Overwrite);
    }

    Y_UNIT_TEST(CopyNodataAfterDelete) {
        TestCopyNodata(ENodataSourceUpdate::Delete);
    }

    Y_UNIT_TEST(CopyNodataWithUnchangedSource) {
        TestCopyNodata(ENodataSourceUpdate::None);
    }
}
