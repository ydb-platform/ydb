#include "blob_depot_tablet.h"
#include "schema.h"
#include "blocks.h"
#include "data.h"
#include "garbage_collection.h"

namespace NKikimr::NBlobDepot {

    void TBlobDepot::ExecuteTxLoad() {
        class TTxLoad : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
        public:
            TTxLoad(TBlobDepot *self)
                : TTransactionBase(self)
            {}

            bool Execute(TTransactionContext& txc, const TActorContext&) override {
                STLOG(PRI_DEBUG, BLOB_DEPOT, BDT19, "TTxLoad::Execute", (TabletId, Self->TabletID()));

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
                            Y_VERIFY(Self->Configured);
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
                            table.GetValue<Schema::Barriers::RecordGeneration>(),
                            table.GetValue<Schema::Barriers::PerGenerationCounter>(),
                            TGenStep(table.GetValue<Schema::Barriers::Soft>()),
                            TGenStep(table.GetValue<Schema::Barriers::Hard>())
                        );
                        if (!table.Next()) {
                            return false;
                        }
                    }
                }

                return true;
            }

            bool Precharge(NIceDb::TNiceDb& db) {
                return db.Table<Schema::Config>().Precharge()
                    & db.Table<Schema::Blocks>().Precharge()
                    & db.Table<Schema::Barriers>().Precharge();
            }

            void Complete(const TActorContext&) override {
                STLOG(PRI_DEBUG, BLOB_DEPOT, BDT20, "TTxLoad::Complete", (TabletId, Self->TabletID()),
                    (Configured, Self->Configured));

                if (Self->Configured) {
                    Self->StartOperation();
                }

                Self->OnLoadFinished();
                Self->Data->StartLoad(); // we need at least Config to start correct loading of data
            }
        };

        Execute(std::make_unique<TTxLoad>(this));
    }

} // NKikimr::NBlobDepot
