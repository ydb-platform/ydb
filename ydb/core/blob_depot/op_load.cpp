#include "blob_depot_tablet.h"
#include "schema.h"

namespace NKikimr::NBlobDepot {

    void TBlobDepot::ExecuteTxLoad() {
        class TTxLoad : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
        public:
            TTxLoad(TBlobDepot *self)
                : TTransactionBase(self)
            {}

            bool Execute(TTransactionContext& txc, const TActorContext&) override {
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
                            const bool success = Self->Config.ParseFromString(table.GetValue<Schema::Config::ConfigProtobuf>());
                            Y_VERIFY(success);
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
                        Self->AddBlockOnLoad(
                            table.GetValue<Schema::Blocks::TabletId>(),
                            table.GetValue<Schema::Blocks::BlockedGeneration>()
                        );
                        if (!table.Next()) {
                            return false;
                        }
                    }
                }

                return true;
            }

            bool Precharge(NIceDb::TNiceDb& db) {
                auto config = db.Table<Schema::Config>().Select();
                auto blocks = db.Table<Schema::Blocks>().Select();
                return config.IsReady() && blocks.IsReady();
            }

            void Complete(const TActorContext&) override {
                Self->InitChannelKinds();
            }
        };

        Execute(std::make_unique<TTxLoad>(this));
    }

} // NKikimr::NBlobDepot
