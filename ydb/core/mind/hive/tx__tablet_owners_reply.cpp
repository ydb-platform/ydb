#include "hive_impl.h"
#include "hive_log.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::HIVE

namespace NKikimr {
namespace NHive {

class TTxTabletOwnersReply : public TTransactionBase<THive> {
    THolder<TEvHive::TEvTabletOwnersReply::THandle> Request;

public:
    TTxTabletOwnersReply(THolder<TEvHive::TEvTabletOwnersReply::THandle> event, THive *hive)
        : TBase(hive)
        , Request(std::move(event))
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_TABLET_OWNERS_REPLY; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        const NKikimrHive::TEvTabletOwnersReply& request(Request->Get()->Record);
        YDB_LOG_DEBUG("THive::TTxTabletOwnersReply::Execute",
            {"GetLogPrefix", GetLogPrefix()});
        NIceDb::TNiceDb db(txc.DB);
        for (const NKikimrHive::TTabletOwnerRecord& protoTabletOwner : request.GetTabletOwners()) {
            TOwnershipKeeper::TOwnerType ownerId = protoTabletOwner.GetOwnerID();
            TSequencer::TSequence seq(protoTabletOwner.GetBegin(), protoTabletOwner.GetEnd());
            if (Self->Keeper.AddOwnedSequence(ownerId, seq)) {
                YDB_LOG_DEBUG("THive::TTxTabletOwnersReply::Execute - add new owned sequence (",
                    {"GetLogPrefix", GetLogPrefix()},
                    {"Begin", seq.Begin},
                    {"End", seq.End},
                    {"ownerId", ownerId});
                db.Table<Schema::TabletOwners>().Key(seq.Begin, seq.End).Update<Schema::TabletOwners::OwnerId>(ownerId);
            }
        }
        db.Table<Schema::State>().Key(TSchemeIds::State::TabletOwnersSynced).Update<Schema::State::Value>(true);
        Self->TabletOwnersSynced = true;
        return true;
    }

    void Complete(const TActorContext&) override {
        YDB_LOG_DEBUG("THive::TTxTabletOwnersReply::Complete",
            {"GetLogPrefix", GetLogPrefix()});
    }
};

ITransaction* THive::CreateTabletOwnersReply(TEvHive::TEvTabletOwnersReply::TPtr event) {
    return new TTxTabletOwnersReply(THolder(std::move(event.Release())), this);
}

} // NHive
} // NKikimr
