#include "tablet_impl.h"

#include <ydb/core/tx/cdc_aggregator/stream/usecases.h>

namespace NKikimr::NCdcAggregator {

class TCdcAggregator::TTxInit : public TTxBase {
    static bool LoadSources(NIceDb::TNiceDb& db, THashMap<ui64, std::optional<TRowVersion>>& sources) {
        auto rowset = db.Table<Schema::Sources>().Select();
        if (!rowset.IsReady()) {
            return false;
        }

        while (!rowset.EndOfSet()) {
            std::optional<TRowVersion> lastHeartbeat;
            if (rowset.HaveValue<Schema::Sources::LastHeartbeatStep>()) {
                lastHeartbeat = TRowVersion(
                    rowset.GetValue<Schema::Sources::LastHeartbeatStep>(),
                    rowset.GetValue<Schema::Sources::LastHeartbeatTxId>());
            }
            sources.emplace(rowset.GetValue<Schema::Sources::DataShardId>(), lastHeartbeat);

            if (!rowset.Next()) {
                return false;
            }
        }

        return true;
    }

    static bool LoadHeartbeats(NIceDb::TNiceDb& db, TMap<TRowVersion, TString>& heartbeats) {
        auto rowset = db.Table<Schema::Heartbeats>().Select();
        if (!rowset.IsReady()) {
            return false;
        }

        while (!rowset.EndOfSet()) {
            const auto version = TRowVersion(
                rowset.GetValue<Schema::Heartbeats::VersionStep>(),
                rowset.GetValue<Schema::Heartbeats::VersionTxId>());
            heartbeats.emplace(version, rowset.GetValue<Schema::Heartbeats::Data>());

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

        THashMap<ui64, std::optional<TRowVersion>> sources;
        TMap<TRowVersion, TString> heartbeats;
        if (!LoadSources(db, sources) || !LoadHeartbeats(db, heartbeats)) {
            return false;
        }

        NKikimrCdcAggregator::TTrackingConfig proto;
        Y_ENSURE(proto.ParseFromString(state.GetValue<Schema::State::TrackingConfig>()), "Malformed tracking config");
        TTrackingConfig config;
        TString error;
        Y_ENSURE(ParseTrackingConfig(proto, config, error), "Invalid tracking config: " << error);

        std::optional<NPQ::THeartbeat> currentHeartbeat;
        if (state.HaveValue<Schema::State::CurrentHeartbeatStep>()) {
            currentHeartbeat = NPQ::THeartbeat{
                .Version = TRowVersion(
                    state.GetValue<Schema::State::CurrentHeartbeatStep>(),
                    state.GetValue<Schema::State::CurrentHeartbeatTxId>()),
                .Data = state.GetValue<Schema::State::CurrentHeartbeatData>(),
            };
        }

        Self->Stream = TStream::Restore(std::move(config), std::move(sources), heartbeats, std::move(currentHeartbeat));
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
