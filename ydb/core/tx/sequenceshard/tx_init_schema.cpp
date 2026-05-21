#include "sequenceshard_impl.h"

namespace NKikimr {
namespace NSequenceShard {

    struct TSequenceShard::TTxInitSchema : public TTxBase {
        explicit TTxInitSchema(TSelf* self)
            : TTxBase(self)
        { }

        TTxType GetTxType() const override { return TXTYPE_INIT_SCHEMA; }

        bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
            LOG_TRACE_S(*TlsActivationContext, NKikimrServices::SEQUENCESHARD, LogPrefix <<"TTxInitSchema.Execute");
            NIceDb::TNiceDb db(txc.DB);
            db.Materialize<Schema>();
            Self->RunTxInit(ctx);
            return true;
        }

        void Complete(const TActorContext&) override {
            LOG_TRACE_S(*TlsActivationContext, NKikimrServices::SEQUENCESHARD, LogPrefix <<"TTxInitSchema.Complete");
        }
    };

    void TSequenceShard::RunTxInitSchema(const TActorContext& ctx) {
        Execute(new TTxInitSchema(this), ctx);
    }

} // namespace NSequenceShard
} // namespace NKikimr
