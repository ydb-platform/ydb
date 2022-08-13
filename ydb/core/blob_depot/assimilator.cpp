#include "blob_depot_tablet.h"
#include "assimilator.h"
#include "blocks.h"
#include "garbage_collection.h"
#include "data.h"

namespace NKikimr::NBlobDepot {

    using TAssimilator = TBlobDepot::TGroupAssimilator;

    struct TStateBits {
        enum {
            Blocks = 1,
            Barriers = 2,
            Blobs = 4,
        };
    };

    void TAssimilator::Bootstrap() {
        if (Token.expired()) {
            return PassAway();
        }

        const std::optional<TString>& state = Self->AssimilatorState;
        if (state) {
            TStringInput stream(*state);
            ui8 stateBits;
            Load(&stream, stateBits);
            if (stateBits & TStateBits::Blocks) {
                Load(&stream, SkipBlocksUpTo.emplace());
            }
            if (stateBits & TStateBits::Barriers) {
                Load(&stream, SkipBarriersUpTo.emplace());
            }
            if (stateBits & TStateBits::Blobs) {
                Load(&stream, SkipBlobsUpTo.emplace());
            }
        }

        SendRequest();
        Become(&TThis::StateFunc);
    }

    void TAssimilator::PassAway() {
    }

    STATEFN(TAssimilator::StateFunc) {
        if (Token.expired()) {
            return PassAway();
        }

        switch (const ui32 type = ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvAssimilateResult, Handle);

            default:
                Y_VERIFY_DEBUG(false, "unexpected event Type# %08" PRIx32, type);
                STLOG(PRI_CRIT, BLOB_DEPOT, BDTxx, "unexpected event", (Id, Self->GetLogId()), (Type, type));
                break;
        }
    }

    void TAssimilator::SendRequest() {
        SendToBSProxy(SelfId(), Self->Config.GetDecommitGroupId(), new TEvBlobStorage::TEvAssimilate(SkipBlocksUpTo,
            SkipBarriersUpTo, SkipBlobsUpTo));
    }

    void TAssimilator::Handle(TEvBlobStorage::TEvAssimilateResult::TPtr ev) {
        class TTxPutAssimilatedData : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
            TAssimilator *Self;
            std::unique_ptr<TEvBlobStorage::TEvAssimilateResult> Ev;
            bool UnblockRegisterActorQ = false;

        public:
            TTxPutAssimilatedData(TAssimilator *self, TEvBlobStorage::TEvAssimilateResult::TPtr ev)
                : TTransactionBase(self->Self)
                , Self(self)
                , Ev(ev->Release().Release())
            {}

            bool Execute(TTransactionContext& txc, const TActorContext&) override {
                NIceDb::TNiceDb db(txc.DB);

                const bool done = Ev->Blocks.empty() && Ev->Barriers.empty() && Ev->Blobs.empty();
                const bool blocksFinished = Ev->Blocks.empty() || !Ev->Barriers.empty() || !Ev->Blobs.empty() || done;
                const bool barriersFinished = Ev->Barriers.empty() || !Ev->Blobs.empty() || done;

                if (const auto& blocks = Ev->Blocks; !blocks.empty()) {
                    Self->SkipBlocksUpTo = blocks.back().TabletId;
                }
                if (const auto& barriers = Ev->Barriers; !barriers.empty()) {
                    Self->SkipBarriersUpTo = {barriers.back().TabletId, barriers.back().Channel};
                }
                if (const auto& blobs = Ev->Blobs; !blobs.empty()) {
                    Self->SkipBlobsUpTo = blobs.back().Id;
                }

                for (const auto& block : Ev->Blocks) {
                    STLOG(PRI_DEBUG, BLOB_DEPOT, BDTxx, "assimilated block", (Id, Self->Self->GetLogId()), (Block, block));
                    Self->Self->BlocksManager->AddBlockOnDecommit(block, txc);
                }
                for (const auto& barrier : Ev->Barriers) {
                    STLOG(PRI_DEBUG, BLOB_DEPOT, BDTxx, "assimilated barrier", (Id, Self->Self->GetLogId()), (Barrier, barrier));
                    Self->Self->BarrierServer->AddBarrierOnDecommit(barrier, txc);
                }
                for (const auto& blob : Ev->Blobs) {
                    STLOG(PRI_DEBUG, BLOB_DEPOT, BDTxx, "assimilated blob", (Id, Self->Self->GetLogId()), (Blob, blob));
                    Self->Self->Data->AddDataOnDecommit(blob, txc);
                }

                auto& decommitState = Self->Self->DecommitState;
                if (blocksFinished && decommitState < EDecommitState::BlocksFinished) {
                    decommitState = EDecommitState::BlocksFinished;
                    UnblockRegisterActorQ = true;
                }
                if (barriersFinished && decommitState < EDecommitState::BarriersFinished) {
                    decommitState = EDecommitState::BarriersFinished;
                }
                if (done && decommitState < EDecommitState::BlobsFinished) {
                    decommitState = EDecommitState::BlobsFinished;
                }

                db.Table<Schema::Config>().Key(Schema::Config::Key::Value).Update(
                    NIceDb::TUpdate<Schema::Config::DecommitState>(decommitState),
                    NIceDb::TUpdate<Schema::Config::AssimilatorState>(Self->SerializeAssimilatorState())
                );

                return true;
            }

            void Complete(const TActorContext&) override {
                if (UnblockRegisterActorQ) {
                    STLOG(PRI_INFO, BLOB_DEPOT, BDT35, "blocks assimilation complete", (Id, Self->Self->GetLogId()),
                        (DecommitGroupId, Self->Self->Config.GetDecommitGroupId()));
                    Self->Self->ProcessRegisterAgentQ();
                }

                if (EDecommitState::BlobsFinished <= Self->Self->DecommitState) {
                    // finished metadata replication
                } else {
                    Self->SendRequest();
                }
            }
        };

        Self->Execute(std::make_unique<TTxPutAssimilatedData>(this, ev));
    }

    TString TAssimilator::SerializeAssimilatorState() const {
        TStringStream stream;

        const ui8 stateBits = (SkipBlocksUpTo ? TStateBits::Blocks : 0)
            | (SkipBarriersUpTo ? TStateBits::Barriers : 0)
            | (SkipBlocksUpTo ? TStateBits::Blocks : 0);

        Save(&stream, stateBits);

        if (SkipBlocksUpTo) {
            Save(&stream, *SkipBlocksUpTo);
        }
        if (SkipBarriersUpTo) {
            Save(&stream, *SkipBarriersUpTo);
        }
        if (SkipBlobsUpTo) {
            Save(&stream, *SkipBlobsUpTo);
        }

        return stream.Str();
    }

    void TBlobDepot::StartGroupAssimilator() {
        if (Config.HasDecommitGroupId()) {
           Y_VERIFY(!GroupAssimilatorId);
           GroupAssimilatorId = RegisterWithSameMailbox(new TGroupAssimilator(this));
        }
    }

} // NKikimr::NBlobDepot
