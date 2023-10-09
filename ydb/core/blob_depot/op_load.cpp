#include "blob_depot_tablet.h"
#include "schema.h"
#include "blocks.h"
#include "data.h"
#include "garbage_collection.h"

namespace NKikimr::NBlobDepot {

    void TBlobDepot::ExecuteTxLoad() {
        class TTxLoad : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
        public:
            TTxType GetTxType() const override { return NKikimrBlobDepot::TXTYPE_LOAD; }

            TTxLoad(TBlobDepot *self)
                : TTransactionBase(self)
            {}

            bool Execute(TTransactionContext& txc, const TActorContext&) override {
                STLOG(PRI_DEBUG, BLOB_DEPOT, BDT19, "TTxLoad::Execute", (Id, Self->GetLogId()));

                NIceDb::TNiceDb db(txc.DB);

                if (!Precharge(db)) {
                    return false;
                }

                // Config
                {
                    auto table = db.Table<Schema::Config>().Key(Schema::Config::Key::Value).Select();
                    if (!table.IsReady()) {
                        return false;
                    } else if (table.IsValid()) {
                        if (table.HaveValue<Schema::Config::ConfigProtobuf>()) {
                            Self->Configured = Self->Config.ParseFromString(table.GetValue<Schema::Config::ConfigProtobuf>());
                            Y_ABORT_UNLESS(Self->Configured);
                        }
                        Self->DecommitState = table.GetValueOrDefault<Schema::Config::DecommitState>();
                        if (table.HaveValue<Schema::Config::AssimilatorState>()) {
                            Self->AssimilatorState.emplace(table.GetValue<Schema::Config::AssimilatorState>());
                        }
                    }
                }

                // Blocks
                {
                    auto table = db.Table<Schema::Blocks>().Select();
                    if (!table.IsReady()) {
                        return false;
                    }
                    while (table.IsValid()) {
                        Self->BlocksManager->AddBlockOnLoad(
                            table.GetValue<Schema::Blocks::TabletId>(),
                            table.GetValue<Schema::Blocks::BlockedGeneration>(),
                            table.GetValue<Schema::Blocks::IssuerGuid>()
                        );
                        if (!table.Next()) {
                            return false;
                        }
                    }
                }

                // Barriers
                {
                    auto table = db.Table<Schema::Barriers>().Select();
                    if (!table.IsReady()) {
                        return false;
                    }
                    while (table.IsValid()) {
                        Self->BarrierServer->AddBarrierOnLoad(
                            table.GetValue<Schema::Barriers::TabletId>(),
                            table.GetValue<Schema::Barriers::Channel>(),
                            TGenStep(table.GetValue<Schema::Barriers::SoftGenCtr>()),
                            TGenStep(table.GetValue<Schema::Barriers::Soft>()),
                            TGenStep(table.GetValue<Schema::Barriers::HardGenCtr>()),
                            TGenStep(table.GetValue<Schema::Barriers::Hard>())
                        );
                        if (!table.Next()) {
                            return false;
                        }
                    }
                }

                // GC
                {
                    auto table = db.Table<Schema::GC>().Select();
                    if (!table.IsReady()) {
                        return false;
                    }
                    while (table.IsValid()) {
                        Self->Data->AddGenStepOnLoad(
                            table.GetValue<Schema::GC::Channel>(),
                            table.GetValue<Schema::GC::GroupId>(),
                            TGenStep(table.GetValueOrDefault<Schema::GC::IssuedGenStep>()),
                            TGenStep(table.GetValueOrDefault<Schema::GC::ConfirmedGenStep>())
                        );
                        if (!table.Next()) {
                            return false;
                        }
                    }
                }

                return true;
            }

            bool Precharge(NIceDb::TNiceDb& db) {
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wbitwise-instead-of-logical"
                return db.Table<Schema::Config>().Precharge()
                    & db.Table<Schema::Blocks>().Precharge()
                    & db.Table<Schema::Barriers>().Precharge()
                    & db.Table<Schema::GC>().Precharge();
#pragma clang diagnostic pop
            }

            void Complete(const TActorContext&) override {
                STLOG(PRI_DEBUG, BLOB_DEPOT, BDT20, "TTxLoad::Complete", (Id, Self->GetLogId()),
                    (Configured, Self->Configured));

                if (Self->Configured) {
                    Self->StartOperation();
                }

                Self->OnLoadFinished();
            }
        };

        Execute(std::make_unique<TTxLoad>(this));
    }

} // NKikimr::NBlobDepot
