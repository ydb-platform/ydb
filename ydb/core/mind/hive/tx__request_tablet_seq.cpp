#include "hive_impl.h"
#include "hive_log.h"

namespace NKikimr {
namespace NHive {

class TTxRequestTabletSequence : public TTransactionBase<THive> {
protected:
    TEvHive::TEvRequestTabletIdSequence::TPtr Event;
    TSequencer::TOwnerType Owner;
    TSequencer::TSequence Sequence;
    size_t Size;

public:
    TTxRequestTabletSequence(TEvHive::TEvRequestTabletIdSequence::TPtr event, THive *hive)
        : TBase(hive)
        , Event(std::move(event))
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_REQUEST_TABLET_SEQUENCE; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        BLOG_D("THive::TTxRequestTabletSequence()::Execute");
        const auto& pbRecord(Event->Get()->Record);
        Size = pbRecord.GetSize();
        if (Size == 0) {
            Size = Self->GetMinRequestSequenceSize();
        }
        if (Size > Self->GetMaxRequestSequenceSize()) {
            Size = Self->GetMaxRequestSequenceSize();
        }
        Owner = {pbRecord.GetOwner().GetOwner(), pbRecord.GetOwner().GetOwnerIdx()};
        std::vector<TSequencer::TOwnerType> modified;
        Sequence = Self->Sequencer.AllocateSequence(Owner, Size, modified);
        if (Sequence != TSequencer::NO_SEQUENCE) {
            NIceDb::TNiceDb db(txc.DB);
            for (auto owner : modified) {
                auto sequence = Self->Sequencer.GetSequence(owner);
                db.Table<Schema::Sequences>()
                        .Key(owner)
                        .Update<Schema::Sequences::Begin, Schema::Sequences::Next, Schema::Sequences::End>(sequence.Begin, sequence.Next, sequence.End);

            }
            TSequencer::TElementType nextElement = Self->Sequencer.GetNextElement();
            if (nextElement != TSequencer::NO_ELEMENT) {
                Self->NextTabletId = Max(Self->NextTabletId, nextElement);
                db.Table<Schema::State>().Key(TSchemeIds::State::NextTabletId).Update<Schema::State::Value>(Self->NextTabletId);
            }
            db.Table<Schema::TabletOwners>().Key(Sequence.Begin, Sequence.End).Update<Schema::TabletOwners::OwnerId>(Owner.first);
            Self->Keeper.AddOwnedSequence(Owner.first, Sequence);
        }
        return true;
    }

    void Complete(const TActorContext&) override {
        BLOG_D("THive::TTxRequestTabletSequence()::Complete");
        if (Sequence == TSequencer::NO_SEQUENCE) {
            BLOG_CRIT("Could not allocate sequence of " << Size << " elements for " << Owner);
        } else {
            BLOG_D("Respond with sequence " << Sequence << " to " << Owner);
            THolder<TEvHive::TEvResponseTabletIdSequence> response = MakeHolder<TEvHive::TEvResponseTabletIdSequence>();
            const auto& pbRecord(Event->Get()->Record);
            response->Record.MutableOwner()->CopyFrom(pbRecord.GetOwner());
            response->Record.SetBeginId(Sequence.Begin);
            response->Record.SetEndId(Sequence.End);
            Self->Send(Event->Sender, std::move(response), 0, Event->Cookie);
        }
    }
};

ITransaction* THive::CreateRequestTabletSequence(TEvHive::TEvRequestTabletIdSequence::TPtr event) {
    return new TTxRequestTabletSequence(std::move(event), this);
}

} // NHive
} // NKikimr
