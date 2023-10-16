#include "sequenceshard_impl.h"

namespace NKikimr {
namespace NSequenceShard {

    struct TSequenceShard::TTxMarkSchemeShardPipe : public TTxBase {
        TTxMarkSchemeShardPipe(TSelf* self, ui64 schemeShardId, ui64 generation, ui64 round)
            : TTxBase(self)
            , SchemeShardId(schemeShardId)
            , Generation(generation)
            , Round(round)
        { }

        TTxType GetTxType() const override { return TXTYPE_MARK_SCHEMESHARD_PIPE; }

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            SLOG_T("TTxMarkSchemeShardPipe.Execute"
                << " SchemeShardId# " << SchemeShardId
                << " Generation# " << Generation
                << " Round# " << Round);

            // Make sure actual value was updated before this tx executed
            auto savedValue = std::make_pair(Generation, Round);
            auto& currentValue = Self->SchemeShardRounds[SchemeShardId];
            Y_DEBUG_ABORT_UNLESS(currentValue >= savedValue);

            NIceDb::TNiceDb db(txc.DB);
            db.Table<Schema::SchemeShardRounds>().Key(SchemeShardId).Update(
                NIceDb::TUpdate<Schema::SchemeShardRounds::Generation>(Generation),
                NIceDb::TUpdate<Schema::SchemeShardRounds::Round>(Round));
            return true;
        }

        void Complete(const TActorContext&) override {
            SLOG_T("TTxMarkSchemeShardPipe.Complete");
        }

        const ui64 SchemeShardId;
        const ui64 Generation;
        const ui64 Round;
    };

    void TSequenceShard::Handle(TEvSequenceShard::TEvMarkSchemeShardPipe::TPtr& ev, const TActorContext& ctx) {
        auto* msg = ev->Get();
        auto serverId = ev->Recipient;
        ui64 schemeShardId = msg->Record.GetSchemeShardId();
        ui64 generation = msg->Record.GetGeneration();
        ui64 round = msg->Record.GetRound();

        auto it = PipeInfos.find(serverId);
        Y_DEBUG_ABORT_UNLESS(it != PipeInfos.end());
        if (Y_LIKELY(it != PipeInfos.end())) {
            it->second.SchemeShardId = schemeShardId;
            it->second.Generation = generation;
            it->second.Round = round;
        }

        auto pipeValue = std::make_pair(generation, round);
        auto& currentValue = SchemeShardRounds[schemeShardId];
        if (currentValue < pipeValue) {
            currentValue = pipeValue;
            Execute(new TTxMarkSchemeShardPipe(this, schemeShardId, generation, round), ctx);
        }
    }

} // namespace NSequenceShard
} // namespace NKikimr
