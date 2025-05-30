#include "data.h"
#include "schema.h"
#include "garbage_collection.h"

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

    class TData::TTxHardGC : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
        const ui8 Channel;
        const ui32 GroupId;
        const TGenStep HardGenStep;
        bool MoreToGo = false;
        std::vector<TLogoBlobID> TrashDeleted;

        static constexpr ui32 MaxKeysToProcessAtOnce = 10'000;

    public:
        TTxType GetTxType() const override { return NKikimrBlobDepot::TXTYPE_HARD_GC; }

        TTxHardGC(TBlobDepot *self, ui8 channel, ui32 groupId, TGenStep hardGenStep)
            : TTransactionBase(self)
            , Channel(channel)
            , GroupId(groupId)
            , HardGenStep(hardGenStep)
        {}

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            NIceDb::TNiceDb db(txc.DB);

            std::function<bool(TLogoBlobID)> callback = [&](TLogoBlobID id) -> bool {
                if (TrashDeleted.size() == MaxKeysToProcessAtOnce) {
                    MoreToGo = true;
                    return false;
                }
                db.Table<Schema::Trash>().Key(TKey(id).MakeBinaryKey()).Delete();
                TrashDeleted.push_back(id);
                return true;
            };
            Self->Data->CollectTrashByHardBarrier(Channel, GroupId, HardGenStep, callback);

            return true;
        }

        void Complete(const TActorContext&) override {
            Self->Data->TrimChannelHistory(Channel, GroupId, std::move(TrashDeleted));
            if (MoreToGo) {
                Self->Data->ExecuteHardGC(Channel, GroupId, HardGenStep);
            } else {
                Self->Data->OnCommitHardGC(Channel, GroupId, HardGenStep);
            }
        }
    };

    void TData::ExecuteHardGC(ui8 channel, ui32 groupId, TGenStep hardGenStep) {
        Self->Execute(std::make_unique<TTxHardGC>(Self, channel, groupId, hardGenStep));
    }

    std::optional<TString> TData::CheckKeyAgainstBarrier(const TKey& key) {
        const auto& v = key.AsVariant();
        if (const auto *id = std::get_if<TLogoBlobID>(&v)) {
            bool underSoft, underHard;
            Self->BarrierServer->GetBlobBarrierRelation(*id, &underSoft, &underHard);
            if (underHard) {
                return TStringBuilder() << "under hard barrier# " << Self->BarrierServer->ToStringBarrier(
                    id->TabletID(), id->Channel(), true);
            } else if (underSoft) {
                const TData::TValue *value = Self->Data->FindKey(key);
                if (!value || value->KeepState != NKikimrBlobDepot::EKeepState::Keep) {
                    return TStringBuilder() << "under soft barrier# " << Self->BarrierServer->ToStringBarrier(
                        id->TabletID(), id->Channel(), false);
                }
            }
        }
        return std::nullopt;
    }

} // NKikimr::NBlobDepot
