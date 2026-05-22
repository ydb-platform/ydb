#include "sequenceshard_impl.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::SEQUENCESHARD

namespace NKikimr {
namespace NSequenceShard {

    struct TSequenceShard::TTxInitSchema : public TTxBase {
        explicit TTxInitSchema(TSelf* self)
            : TTxBase(self)
        { }

        TTxType GetTxType() const override { return TXTYPE_INIT_SCHEMA; }

        bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
            YDB_LOG_TRACE("TTxInitSchema.Execute",
                {"LogPrefix", LogPrefix});
            NIceDb::TNiceDb db(txc.DB);
            db.Materialize<Schema>();
            Self->RunTxInit(ctx);
            return true;
        }

        void Complete(const TActorContext&) override {
            YDB_LOG_TRACE("TTxInitSchema.Complete",
                {"LogPrefix", LogPrefix});
        }
    };

    void TSequenceShard::RunTxInitSchema(const TActorContext& ctx) {
        Execute(new TTxInitSchema(this), ctx);
    }

} // namespace NSequenceShard
} // namespace NKikimr
