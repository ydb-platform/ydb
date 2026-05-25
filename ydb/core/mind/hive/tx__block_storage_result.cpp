#include "hive_impl.h"
#include "hive_log.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

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
        YDB_LOG_DEBUG("THive::TTxBlockStorageResult::Execute(",
            {"GetLogPrefix", GetLogPrefix()},
            {"TabletId", TabletId},
            {"#_NKikimrProto::EReplyStatus_Name(msg->Status)", NKikimrProto::EReplyStatus_Name(msg->Status)});
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
                        YDB_LOG_WARN("THive::TTxBlockStorageResult Complete status was for TabletId",
                            {"GetLogPrefix", GetLogPrefix()},
                            {"#_NKikimrProto::EReplyStatus_Name(msg->Status)", NKikimrProto::EReplyStatus_Name(msg->Status)},
                            {"Id", tablet->Id});
                    }
                    for (TFollowerTabletInfo& follower : tablet->Followers) {
                        follower.InitiateStop(SideEffects);
                    }
                    SideEffects.Send(Self->SelfId(), new TEvHive::TEvInitiateDeleteStorage(tablet->Id));
                } else {
                    tablet->State = ETabletState::ReadyToWork;
                    db.Table<Schema::Tablet>().Key(tablet->Id).Update(NIceDb::TUpdate<Schema::Tablet::State>(ETabletState::ReadyToWork));
                    if (tablet->IsBootingSuppressed()) {
                        // Use best effort to kill currently running tablet
                        SideEffects.Register(CreateTabletKiller(TabletId, /* nodeId */ 0, tablet->KnownGeneration));
                    } else {
                        Self->Execute(Self->CreateRestartTablet(tablet->GetFullTabletId()));
                    }
                }
            } else {
                YDB_LOG_WARN("THive::TTxBlockStorageResult retrying for because of",
                    {"GetLogPrefix", GetLogPrefix()},
                    {"TabletId", TabletId},
                    {"#_NKikimrProto::EReplyStatus_Name(msg->Status)", NKikimrProto::EReplyStatus_Name(msg->Status)},
                    {"ErrorReason", msg->ErrorReason});
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
        YDB_LOG_DEBUG("THive::TTxBlockStorageResult::Complete(",
            {"GetLogPrefix", GetLogPrefix()},
            {"TabletId", TabletId},
            {"#_NKikimrProto::EReplyStatus_Name(msg->Status)", NKikimrProto::EReplyStatus_Name(msg->Status)});
        SideEffects.Complete(ctx);
    }
};

ITransaction* THive::CreateBlockStorageResult(TEvTabletBase::TEvBlockBlobStorageResult::TPtr& ev) {
    return new TTxBlockStorageResult(ev, this);
}

} // NHive
} // NKikimr
