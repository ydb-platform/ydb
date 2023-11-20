#include "data.h"
#include "schema.h"

namespace NKikimr::NBlobDepot {

    using TData = TBlobDepot::TData;

    class TData::TTxIssueGC : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
        const ui8 Channel;
        const ui32 GroupId;
        const TGenStep IssuedGenStep;
        std::unique_ptr<TEvBlobStorage::TEvCollectGarbage> CollectGarbage;
        const ui64 Cookie;

    public:
        TTxType GetTxType() const override { return NKikimrBlobDepot::TXTYPE_ISSUE_GC; }

        TTxIssueGC(TBlobDepot *self, ui8 channel, ui32 groupId, TGenStep issuedGenStep,
                std::unique_ptr<TEvBlobStorage::TEvCollectGarbage> collectGarbage, ui64 cookie)
            : TTransactionBase(self)
            , Channel(channel)
            , GroupId(groupId)
            , IssuedGenStep(issuedGenStep)
            , CollectGarbage(std::move(collectGarbage))
            , Cookie(cookie)
        {}

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            NIceDb::TNiceDb db(txc.DB);
            db.Table<Schema::GC>().Key(Channel, GroupId).Update<Schema::GC::IssuedGenStep>(ui64(IssuedGenStep));
            return true;
        }

        void Complete(const TActorContext&) override {
            SendToBSProxy(Self->SelfId(), GroupId, CollectGarbage.release(), Cookie);
        }
    };

    void TData::ExecuteIssueGC(ui8 channel, ui32 groupId, TGenStep issuedGenStep,
            std::unique_ptr<TEvBlobStorage::TEvCollectGarbage> collectGarbage, ui64 cookie) {
        Self->Execute(std::make_unique<TTxIssueGC>(Self, channel, groupId, issuedGenStep, std::move(collectGarbage), cookie));
    }

    class TData::TTxConfirmGC : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
        const ui8 Channel;
        const ui32 GroupId;
        std::vector<TLogoBlobID> TrashDeleted;
        size_t Index;
        const TGenStep ConfirmedGenStep;

        static constexpr ui32 MaxKeysToProcessAtOnce = 10'000;

    public:
        TTxType GetTxType() const override { return NKikimrBlobDepot::TXTYPE_CONFIRM_GC; }

        TTxConfirmGC(TBlobDepot *self, ui8 channel, ui32 groupId, std::vector<TLogoBlobID> trashDeleted, size_t index,
                TGenStep confirmedGenStep)
            : TTransactionBase(self)
            , Channel(channel)
            , GroupId(groupId)
            , TrashDeleted(std::move(trashDeleted))
            , Index(index)
            , ConfirmedGenStep(confirmedGenStep)
        {}

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            NIceDb::TNiceDb db(txc.DB);

            for (ui32 i = 0; Index < TrashDeleted.size() && i < MaxKeysToProcessAtOnce; ++i, ++Index) {
                db.Table<Schema::Trash>().Key(TKey(TrashDeleted[Index]).MakeBinaryKey()).Delete();
            }
            if (Index == TrashDeleted.size()) {
                db.Table<Schema::GC>().Key(Channel, GroupId).Update<Schema::GC::ConfirmedGenStep>(ui64(ConfirmedGenStep));
            }

            return true;
        }

        void Complete(const TActorContext&) override {
            if (Index == TrashDeleted.size()) {
                Self->Data->OnCommitConfirmedGC(Channel, GroupId, std::move(TrashDeleted));
            } else { // resume transaction
                Self->Data->ExecuteConfirmGC(Channel, GroupId, std::move(TrashDeleted), Index, ConfirmedGenStep);
            }
        }
    };

    void TData::ExecuteConfirmGC(ui8 channel, ui32 groupId, std::vector<TLogoBlobID> trashDeleted, size_t index,
            TGenStep confirmedGenStep) {
        Self->Execute(std::make_unique<TTxConfirmGC>(Self, channel, groupId, std::move(trashDeleted), index,
            confirmedGenStep));
    }

} // NKikimr::NBlobDepot
