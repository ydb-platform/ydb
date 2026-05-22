#include "hive_impl.h"
#include "hive_log.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::HIVE

namespace NKikimr {
namespace NHive {

class TTxResponseTabletSequence : public TTransactionBase<THive> {
protected:
    TEvHive::TEvResponseTabletIdSequence::TPtr Event;
    TSequencer::TOwnerType Owner;
    TSequencer::TSequence Sequence;

public:
    TTxResponseTabletSequence(TEvHive::TEvResponseTabletIdSequence::TPtr event, THive *hive)
        : TBase(hive)
        , Event(std::move(event))
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_RESPONSE_TABLET_SEQUENCE; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        YDB_LOG_DEBUG("THive::TTxResponseTabletSequence()::Execute",
            {"GetLogPrefix", GetLogPrefix()});
        const auto& pbRecord(Event->Get()->Record);
        if (!pbRecord.HasOwner()) {
            YDB_LOG_ERROR("Invalid response received",
                {"GetLogPrefix", GetLogPrefix()});
            return true;
        }
        if (pbRecord.GetBeginId() != pbRecord.GetEndId()) {
            Y_ABORT_UNLESS(pbRecord.GetOwner().GetOwner() == Self->TabletID());
            Owner = {TSequencer::NO_OWNER, pbRecord.GetOwner().GetOwnerIdx()};
            Sequence = {pbRecord.GetBeginId(), pbRecord.GetEndId()};
            YDB_LOG_DEBUG("Received sequence",
                {"GetLogPrefix", GetLogPrefix()},
                {"Sequence", Sequence});
            if (Self->Sequencer.AddFreeSequence(Owner, Sequence)) {
                NIceDb::TNiceDb db(txc.DB);
                db.Table<Schema::Sequences>()
                        .Key(Owner)
                        .Update<Schema::Sequences::Begin, Schema::Sequences::Next, Schema::Sequences::End>(Sequence.Begin, Sequence.Next, Sequence.End);
                // we keep ownership of the sequence in case tablets will be deleted multiple times
                Self->Keeper.AddOwnedSequence(Self->TabletID(), Sequence);
                db.Table<Schema::TabletOwners>()
                        .Key(Sequence.Begin, Sequence.End)
                        .Update<Schema::TabletOwners::OwnerId>(Self->TabletID());
            } else {
                YDB_LOG_DEBUG("This sequence already exists",
                    {"GetLogPrefix", GetLogPrefix()},
                    {"Sequence", Sequence});
                Sequence.Clear();
            }
        } else {
            YDB_LOG_DEBUG("Received empty sequence",
                {"GetLogPrefix", GetLogPrefix()});
        }
        if (pbRecord.GetOwner().GetOwnerIdx() >= Self->RequestingSequenceIndex) {
            Self->RequestingSequenceNow = false;
        }
        return true;
    }

    void Complete(const TActorContext&) override {
        YDB_LOG_DEBUG("THive::TTxResponseTabletSequence()::Complete",
            {"GetLogPrefix", GetLogPrefix()});
        if (!Sequence.Empty()) {
            Self->ProcessPendingOperations();
        }
    }
};

ITransaction* THive::CreateResponseTabletSequence(TEvHive::TEvResponseTabletIdSequence::TPtr event) {
    return new TTxResponseTabletSequence(std::move(event), this);
}

} // NHive
} // NKikimr
