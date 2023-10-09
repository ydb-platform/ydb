#include "test_shard_impl.h"
#include "scheme.h"

namespace NKikimr::NTestShard {

    class TTestShard::TTxLoadEverything : public TTransactionBase<TTestShard> {
    public:
        TTxLoadEverything(TTestShard *self)
            : TTransactionBase(self)
        {}

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            NIceDb::TNiceDb db(txc.DB);
            {
                using T = Schema::State;
                auto table = db.Table<T>().Key(T::Key::Default).Select();
                if (!table.IsReady()) {
                    return false;
                } else if (table.IsValid()) {
                    const TString settings = table.GetValue<T::Settings>();
                    Self->Settings.emplace();
                    const bool success = Self->Settings->ParseFromString(settings);
                    Y_ABORT_UNLESS(success);
                }
            }
            return true;
        }

        void Complete(const TActorContext&) override {
            Self->OnLoadComplete();
        }
    };

    ITransaction *TTestShard::CreateTxLoadEverything() {
        return new TTxLoadEverything(this);
    }

} // NKikimr::NTestShard
