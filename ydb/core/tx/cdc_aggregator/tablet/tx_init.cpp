#include "tablet_impl.h"

namespace NKikimr::NCdcAggregator {

class TCdcAggregator::TTxInit : public TTxBase {
    static bool LoadSources(NIceDb::TNiceDb& db, THashSet<ui64>& sources) {
        auto rowset = db.Table<Schema::Sources>().Select();
        if (!rowset.IsReady()) {
            return false;
        }

        while (!rowset.EndOfSet()) {
            sources.insert(rowset.GetValue<Schema::Sources::DataShardId>());

            if (!rowset.Next()) {
                return false;
            }
        }

        return true;
    }

public:
    explicit TTxInit(TCdcAggregator* self)
        : TTxBase(self)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_INIT;
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);

        auto state = db.Table<Schema::State>().Key(Schema::State::Id::Value).Select();
        if (!state.IsReady()) {
            return false;
        }
        if (!state.IsValid()) {
            return true;
        }

        THashSet<ui64> sources;
        if (!LoadSources(db, sources)) {
            return false;
        }

        NKikimrCdcAggregator::TTrackingConfig proto;
        Y_ENSURE(proto.ParseFromString(state.GetValue<Schema::State::TrackingConfig>()), "Malformed tracking config");
        TTrackingConfig config;
        TString error;
        Y_ENSURE(ParseTrackingConfig(proto, config, error), "Invalid tracking config: " << error);

        Self->Stream = TStream::Restore(std::move(config), std::move(sources));
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        Self->SwitchToWork(ctx);
    }
};

void TCdcAggregator::RunTxInit(const TActorContext& ctx) {
    Execute(new TTxInit(this), ctx);
}

} // namespace NKikimr::NCdcAggregator
