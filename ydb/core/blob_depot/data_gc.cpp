#include "data.h"
#include "schema.h"

namespace NKikimr::NBlobDepot {

    using TData = TBlobDepot::TData;

    class TData::TTxIssueGC : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
        const ui8 Channel;
        const ui32 GroupId;
        const TGenStep IssuedGenStep;
        std::unique_ptr<TEvBlobStorage::TEvCollectGarbage> CollectGarbage;

    public:
        TTxIssueGC(TBlobDepot *self, ui8 channel, ui32 groupId, TGenStep issuedGenStep,
                std::unique_ptr<TEvBlobStorage::TEvCollectGarbage> collectGarbage)
            : TTransactionBase(self)
            , Channel(channel)
            , GroupId(groupId)
            , IssuedGenStep(issuedGenStep)
            , CollectGarbage(std::move(collectGarbage))
        {}

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            NIceDb::TNiceDb db(txc.DB);
            db.Table<Schema::GC>().Key(Channel, GroupId).Update<Schema::GC::IssuedGenStep>(ui64(IssuedGenStep));
            return true;
        }

        void Complete(const TActorContext&) override {
            SendToBSProxy(Self->SelfId(), GroupId, CollectGarbage.release(), GroupId);
        }
    };

    void TData::ExecuteIssueGC(ui8 channel, ui32 groupId, TGenStep issuedGenStep,
            std::unique_ptr<TEvBlobStorage::TEvCollectGarbage> collectGarbage) {
        Self->Execute(std::make_unique<TTxIssueGC>(Self, channel, groupId, issuedGenStep, std::move(collectGarbage)));
    }

    class TData::TTxConfirmGC : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
        const ui8 Channel;
        const ui32 GroupId;
        std::vector<TLogoBlobID> TrashDeleted;
        const TGenStep ConfirmedGenStep;

        static constexpr ui32 MaxKeysToProcessAtOnce = 10'000;

    public:
        TTxConfirmGC(TBlobDepot *self, ui8 channel, ui32 groupId, std::vector<TLogoBlobID> trashDeleted, TGenStep confirmedGenStep)
            : TTransactionBase(self)
            , Channel(channel)
            , GroupId(groupId)
            , TrashDeleted(std::move(trashDeleted))
            , ConfirmedGenStep(confirmedGenStep)
        {}

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            NIceDb::TNiceDb db(txc.DB);

            for (ui32 i = 0; i < TrashDeleted.size() && i < MaxKeysToProcessAtOnce; ++i) {
                db.Table<Schema::Trash>().Key(TKey(TrashDeleted[i]).MakeBinaryKey()).Delete();
            }
            if (TrashDeleted.size() <= MaxKeysToProcessAtOnce) {
                TrashDeleted.clear();
                db.Table<Schema::GC>().Key(Channel, GroupId).Update<Schema::GC::ConfirmedGenStep>(ui64(ConfirmedGenStep));
            } else {
                std::vector<TLogoBlobID> temp;
                temp.insert(temp.end(), TrashDeleted.begin() + MaxKeysToProcessAtOnce, TrashDeleted.end());
                temp.swap(TrashDeleted);
            }

            return true;
        }

        void Complete(const TActorContext&) override {
            if (TrashDeleted.empty()) {
                Self->Data->OnCommitConfirmedGC(Channel, GroupId);
            } else { // resume transaction
                Self->Data->ExecuteConfirmGC(Channel, GroupId, std::move(TrashDeleted), ConfirmedGenStep);
            }
        }
    };

    void TData::ExecuteConfirmGC(ui8 channel, ui32 groupId, std::vector<TLogoBlobID> trashDeleted, TGenStep confirmedGenStep) {
        Self->Execute(std::make_unique<TTxConfirmGC>(Self, channel, groupId, std::move(trashDeleted), confirmedGenStep));
    }

} // NKikimr::NBlobDepot
