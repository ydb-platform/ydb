#include "impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT BS_CONTROLLER

namespace NKikimr::NBsController {

    class TBlobStorageController::TTxUpdateEnableConfigV2 : public TTransactionBase<TBlobStorageController> {
        bool Value;
        std::function<void()> OnComplete;
    public:
        TTxUpdateEnableConfigV2(TBlobStorageController *self, bool value, std::function<void()> onComplete)
            : TTransactionBase(self)
            , Value(value)
            , OnComplete(std::move(onComplete))
        {}

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            YDB_LOG_DEBUG("TTxUpdateEnableConfigV2 Execute",
                {"marker", "BSCTXEUC01"},
                {"value", Value});
            NIceDb::TNiceDb(txc.DB).Table<Schema::State>().Key(true).Update<Schema::State::EnableConfigV2>(Value);
            return true;
        }

        void Complete(const TActorContext&) override {
            Self->EnableConfigV2 = Value;
            if (OnComplete) {
                OnComplete();
            }
        }
    };

    ITransaction* TBlobStorageController::CreateTxUpdateEnableConfigV2(bool value, std::function<void()> onComplete) {
        return new TTxUpdateEnableConfigV2(this, value, std::move(onComplete));
    }

} // NKikimr::NBsController
