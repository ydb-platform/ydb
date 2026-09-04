#include "hive_impl.h"
#include "hive_log.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::HIVE

namespace NKikimr {
namespace NHive {

class TTxBlockStorageResult : public TTransactionBase<THive> {
    TEvTabletBase::TEvBlockBlobStorageResult::TPtr Result;
    TTabletId TabletId;
    TSideEffects SideEffects;
public:
    TTxBlockStorageResult(TEvTabletBase::TEvBlockBlobStorageResult::TPtr& ev, THive* hive)
        : TBase(hive)
        , Result(ev)
        , TabletId(Result->Get()->TabletId)
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_BLOCK_STORAGE_RESULT; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        SideEffects.Reset(Self->SelfId());
        TEvTabletBase::TEvBlockBlobStorageResult* msg = Result->Get();
        YDB_LOG_DEBUG("THive::TTxBlockStorageResult::Execute processing block storage result",
            {"logPrefix", GetLogPrefix()},
            {"tabletId", TabletId},
            {"replyStatus", NKikimrProto::EReplyStatus_Name(msg->Status)});
        TLeaderTabletInfo* tablet = Self->FindTabletEvenInDeleting(TabletId);
        if (tablet != nullptr) {
            NIceDb::TNiceDb db(txc.DB);
            if (msg->Status == NKikimrProto::OK
                    || msg->Status == NKikimrProto::ALREADY
                    || msg->Status == NKikimrProto::RACE
                    || msg->Status == NKikimrProto::BLOCKED
                    || msg->Status == NKikimrProto::NO_GROUP) {
                if (tablet->IsDeleting()) {
                    if (msg->Status != NKikimrProto::EReplyStatus::OK) {
                        YDB_LOG_WARN("THive::TTxBlockStorageResult::Execute unexpected status for deleting tablet",
                            {"logPrefix", GetLogPrefix()},
                            {"replyStatus", NKikimrProto::EReplyStatus_Name(msg->Status)},
                            {"tabletId", tablet->Id});
                    }
                    for (TFollowerTabletInfo& follower : tablet->Followers) {
                        follower.InitiateStop(SideEffects);
                    }
                    SideEffects.Send(Self->SelfId(), new TEvHive::TEvInitiateDeleteStorage(tablet->Id));
                } else {
                    tablet->ConfirmedStorageVersion = tablet->TabletStorageInfo->Version;
                    tablet->State = ETabletState::ReadyToWork;
                    db.Table<Schema::Tablet>().Key(tablet->Id).Update(
                        NIceDb::TUpdate<Schema::Tablet::State>(ETabletState::ReadyToWork),
                        NIceDb::TUpdate<Schema::Tablet::ConfirmedStorageVersion>(
                            tablet->ConfirmedStorageVersion));
                    tablet->NotifyStorageInfo(SideEffects);
                    if (tablet->IsBootingSuppressed()) {
                        // Use best effort to kill currently running tablet
                        SideEffects.Register(CreateTabletKiller(TabletId, /* nodeId */ 0, tablet->KnownGeneration));
                    } else {
                        Self->Execute(Self->CreateForceRestartTablet(tablet->GetFullTabletId()));
                    }
                }
            } else if (msg->Status == NKikimrProto::ERROR && !tablet->IsDeleting()) {
                ui32 confirmedVersion = tablet->ConfirmedStorageVersion;
                if (confirmedVersion == Max<ui32>()) {
                    confirmedVersion = tablet->TabletStorageInfo->Version
                        ? tablet->TabletStorageInfo->Version - 1
                        : 0;
                }

                struct THistoryEntry {
                    ui32 Channel;
                    ui32 Generation;
                };
                TVector<THistoryEntry> entries;
                auto rowset = db.Table<Schema::TabletChannelGen>().Range(tablet->Id).Select();
                if (!rowset.IsReady()) {
                    return false;
                }
                while (!rowset.EndOfSet()) {
                    if (!rowset.GetValueOrDefault<Schema::TabletChannelGen::DeletedAtGeneration>()
                            && rowset.GetValueOrDefault<Schema::TabletChannelGen::Version>() > confirmedVersion) {
                        entries.push_back({
                            .Channel = static_cast<ui32>(
                                rowset.GetValue<Schema::TabletChannelGen::Channel>()),
                            .Generation = static_cast<ui32>(
                                rowset.GetValue<Schema::TabletChannelGen::Generation>()),
                        });
                    }
                    if (!rowset.Next()) {
                        return false;
                    }
                }

                std::unordered_set<ui32> affectedChannels;
                for (const auto& entry : entries) {
                    affectedChannels.insert(entry.Channel);
                }
                for (ui32 channel : affectedChannels) {
                    tablet->ReleaseAllocationUnit(channel);
                }
                for (const auto& entry : entries) {
                    db.Table<Schema::TabletChannelGen>().Key(
                        tablet->Id, entry.Channel, entry.Generation).Delete();
                    if (entry.Channel < tablet->TabletStorageInfo->Channels.size()) {
                        auto& history = tablet->TabletStorageInfo->Channels[entry.Channel].History;
                        std::erase_if(history, [&](const auto& item) {
                            return item.FromGeneration == entry.Generation;
                        });
                    }
                }
                for (ui32 channel : affectedChannels) {
                    tablet->ChannelProfileNewGroup.set(channel);
                    db.Table<Schema::TabletChannel>().Key(tablet->Id, channel).Update(
                        NIceDb::TUpdate<Schema::TabletChannel::NeedNewGroup>(true));
                    tablet->AcquireAllocationUnit(channel);
                }

                tablet->ConfirmedStorageVersion = confirmedVersion;
                if (tablet->KnownGeneration <= msg->ActualGeneration) {
                    Y_ABORT_UNLESS(msg->ActualGeneration < Max<ui32>());
                    tablet->KnownGeneration = msg->ActualGeneration + 1;
                }
                tablet->State = ETabletState::GroupAssignment;
                db.Table<Schema::Tablet>().Key(tablet->Id).Update(
                    NIceDb::TUpdate<Schema::Tablet::ConfirmedStorageVersion>(confirmedVersion),
                    NIceDb::TUpdate<Schema::Tablet::KnownGeneration>(tablet->KnownGeneration),
                    NIceDb::TUpdate<Schema::Tablet::State>(ETabletState::GroupAssignment));

                YDB_LOG_WARN("THive::TTxBlockStorageResult::Execute rolling back unconfirmed storage",
                    {"logPrefix", GetLogPrefix()},
                    {"tabletId", TabletId},
                    {"storageVersion", tablet->TabletStorageInfo->Version},
                    {"confirmedStorageVersion", confirmedVersion},
                    {"actualGeneration", msg->ActualGeneration},
                    {"affectedChannels", affectedChannels});
                tablet->InitiateAssignTabletGroups();
            } else {
                YDB_LOG_WARN("THive::TTxBlockStorageResult::Execute retrying block storage operation",
                    {"logPrefix", GetLogPrefix()},
                    {"tabletId", TabletId},
                    {"replyStatus", NKikimrProto::EReplyStatus_Name(msg->Status)},
                    {"errorReason", msg->ErrorReason});
                if (tablet->IsDeleting()) {
                    --Self->DeleteTabletInProgress;
                    Self->UpdateCounterTabletsDeleting();
                }
                SideEffects.Schedule(TDuration::MilliSeconds(1000), new TEvHive::TEvInitiateBlockStorage(tablet->Id));
            }
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        TEvTabletBase::TEvBlockBlobStorageResult* msg = Result->Get();
        YDB_LOG_DEBUG("THive::TTxBlockStorageResult::Complete",
            {"logPrefix", GetLogPrefix()},
            {"tabletId", TabletId},
            {"replyStatus", NKikimrProto::EReplyStatus_Name(msg->Status)});
        SideEffects.Complete(ctx, Self->Requests);
    }
};

ITransaction* THive::CreateBlockStorageResult(TEvTabletBase::TEvBlockBlobStorageResult::TPtr& ev) {
    return new TTxBlockStorageResult(ev, this);
}

} // NHive
} // NKikimr
