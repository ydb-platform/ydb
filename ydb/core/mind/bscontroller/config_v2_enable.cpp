#include "impl.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT BS_CONTROLLER

namespace NKikimr::NBsController {

    class TBlobStorageController::TTxUpdateEnableConfigV2 : public TTransactionBase<TBlobStorageController> {
        bool Value;
    public:
        TTxUpdateEnableConfigV2(TBlobStorageController *self, bool value)
            : TTransactionBase(self)
            , Value(value)
        {}

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            YDB_LOG_DEBUG("TTxUpdateEnableConfigV2 Execute",
                {"Marker", "BSCTXEUC01"},
                {"Value", Value});
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
