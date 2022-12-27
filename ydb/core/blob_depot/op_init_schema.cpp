#include "blob_depot_tablet.h"
#include "data.h"
#include "schema.h"

namespace NKikimr::NBlobDepot {

    void TBlobDepot::ExecuteTxInitSchema() {
        class TTxInitSchema : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
        public:
            TTxType GetTxType() const override { return NKikimrBlobDepot::TXTYPE_INIT_SCHEMA; }

            TTxInitSchema(TBlobDepot *self)
                : TTransactionBase(self)
            {}

            bool Execute(TTransactionContext& txc, const TActorContext&) override {
                NIceDb::TNiceDb db(txc.DB);
                db.Materialize<Schema>();
                return true;
            }

            void Complete(const TActorContext&) override {
                Self->ExecuteTxLoad();
            }
        };

        Execute(std::make_unique<TTxInitSchema>(this));
    }

} // NKikimr::NBlobDepot
