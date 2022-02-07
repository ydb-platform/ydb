#include "sequenceshard_impl.h"

namespace NKikimr {
namespace NSequenceShard {

    struct TSequenceShard::TTxInitSchema : public TTxBase {
        explicit TTxInitSchema(TSelf* self)
            : TTxBase(self)
        { }

        TTxType GetTxType() const override { return TXTYPE_INIT_SCHEMA; }

        bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
            SLOG_T("TTxInitSchema.Execute");
            NIceDb::TNiceDb db(txc.DB);
            db.Materialize<Schema>();
            Self->RunTxInit(ctx);
            return true;
        }

        void Complete(const TActorContext&) override {
            SLOG_T("TTxInitSchema.Complete");
        }
    };

    void TSequenceShard::RunTxInitSchema(const TActorContext& ctx) {
        Execute(new TTxInitSchema(this), ctx);
    }

} // namespace NSequenceShard
} // namespace NKikimr
