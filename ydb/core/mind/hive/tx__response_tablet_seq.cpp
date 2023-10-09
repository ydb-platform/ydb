#include "hive_impl.h"
#include "hive_log.h"

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
        BLOG_D("THive::TTxResponseTabletSequence()::Execute");
        const auto& pbRecord(Event->Get()->Record);
        if (!pbRecord.HasOwner()) {
            BLOG_ERROR("Invalid response received");
            return true;
        }
        if (pbRecord.GetBeginId() != pbRecord.GetEndId()) {
            Y_ABORT_UNLESS(pbRecord.GetOwner().GetOwner() == Self->TabletID());
            Owner = {TSequencer::NO_OWNER, pbRecord.GetOwner().GetOwnerIdx()};
            Sequence = {pbRecord.GetBeginId(), pbRecord.GetEndId()};
            BLOG_D("Received sequence " << Sequence);
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
                BLOG_D("This sequence " << Sequence << " already exists");
                Sequence.Clear();
            }
        } else {
            BLOG_D("Received empty sequence");
        }
        if (pbRecord.GetOwner().GetOwnerIdx() >= Self->RequestingSequenceIndex) {
            Self->RequestingSequenceNow = false;
        }
        return true;
    }

    void Complete(const TActorContext&) override {
        BLOG_D("THive::TTxResponseTabletSequence()::Complete");
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
