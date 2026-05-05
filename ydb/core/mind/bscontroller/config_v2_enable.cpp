#include "impl.h"

namespace NKikimr::NBsController {

    class TBlobStorageController::TTxUpdateEnableConfigV2 : public TTransactionBase<TBlobStorageController> {
        bool Value;
    public:
        TTxUpdateEnableConfigV2(TBlobStorageController *self, bool value)
            : TTransactionBase(self)
            , Value(value)
        {}

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            STLOG(PRI_DEBUG, BS_CONTROLLER, BSCTXEUC01, "TTxUpdateEnableConfigV2 Execute", (Value, Value));
            NIceDb::TNiceDb(txc.DB).Table<Schema::State>().Key(true).Update<Schema::State::EnableConfigV2>(Value);
            return true;
        }

        void Complete(const TActorContext&) override {
            Self->EnableConfigV2 = Value;
        }
    };

    ITransaction* TBlobStorageController::CreateTxUpdateEnableConfigV2(bool value) {
        return new TTxUpdateEnableConfigV2(this, value);
    }

} // NKikimr::NBsController
