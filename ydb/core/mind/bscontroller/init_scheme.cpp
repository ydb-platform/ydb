#include "impl.h"

namespace NKikimr {
namespace NBsController {

class TBlobStorageController::TTxInitScheme : public TTransactionBase<TBlobStorageController> {
    bool Failed = false;

public:
    TTxInitScheme(TBlobStorageController *controller)
        : TBase(controller)
    {}

    TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_INIT_SCHEME; }

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSCTXIS01, "TTxInitScheme Execute");

        // check if we have State table and a row with valid SchemaVersion, ensure that this version does not exceed this one
        if (txc.DB.GetScheme().GetTableInfo(Schema::State::TableId)) {
            NIceDb::TNiceDb db(txc.DB);
            auto state = db.Table<Schema::State>().Select<Schema::State::SchemaVersion>();
            if (!state.IsReady()) {
                return false;
            } else if (state.IsValid()) {
                const ui32 version = state.GetValue<Schema::State::SchemaVersion>();
                if (version > Schema::CurrentSchemaVersion) {
                    STLOG(PRI_ERROR, BS_CONTROLLER, BSCTXIS02, "Stored scheme version is newer than supported",
                        (SchemeVersion, version), (SupportedVersion, ui32(Schema::CurrentSchemaVersion)));
                    Failed = true;
                    return true;
                }
            }
        }

        NIceDb::TNiceDb(txc.DB).Materialize<Schema>();
        return true;
    }

    void Complete(const TActorContext&) override {
        if (Failed) {
            TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, Self->SelfId(), {}, {}, 0));
        } else {
            STLOG(PRI_DEBUG, BS_CONTROLLER, BSCTXIS03, "TTxInitScheme Complete");
            Self->Execute(Self->CreateTxMigrate());
        }
    }
};

ITransaction* TBlobStorageController::CreateTxInitScheme() {
    return new TTxInitScheme(this);
}

} // NBlobStorageController
} // NKikimr
