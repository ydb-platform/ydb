#include "test_shard_impl.h"
#include "scheme.h"

namespace NKikimr::NTestShard {

    class TTestShard::TTxInitialize : public TTransactionBase<TTestShard> {
        const TActorId Sender;
        const ui64 Cookie;
        NKikimrClient::TTestShardControlRequest::TCmdInitialize Cmd;

    public:
        TTxInitialize(TTestShard *self, const NKikimrClient::TTestShardControlRequest::TCmdInitialize& cmd,
                TActorId sender, ui64 cookie)
            : TTransactionBase(self)
            , Sender(sender)
            , Cookie(cookie)
            , Cmd(cmd)
        {}

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            TString settings;
            const bool success = Cmd.SerializeToString(&settings);
            Y_ABORT_UNLESS(success);

            NIceDb::TNiceDb db(txc.DB);
            db.Table<Schema::State>().Key(Schema::State::Key::Default).Update(
                NIceDb::TUpdate<Schema::State::Settings>(settings));

            return true;
        }

        void Complete(const TActorContext& ctx) override {
            ctx.Send(Sender, new TEvControlResponse, 0, Cookie);
            Self->Settings = Cmd;
            STLOG(PRI_DEBUG, TEST_SHARD, TS30, "TTxInitialize::Complete", (TabletId, Self->TabletID()));
            Self->StartActivities();
        }
    };

    ITransaction *TTestShard::CreateTxInitialize(const NKikimrClient::TTestShardControlRequest::TCmdInitialize& cmd,
            TActorId sender, ui64 cookie) {
        return new TTxInitialize(this, cmd, sender, cookie);
    }

} // NKikimr::NTestShard
