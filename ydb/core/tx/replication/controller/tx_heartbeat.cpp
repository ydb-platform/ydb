#include "controller_impl.h"

namespace NKikimr::NReplication::NController {

class TController::TTxHeartbeat: public TTxBase {
public:
    explicit TTxHeartbeat(TController* self)
        : TTxBase("TxHeartbeat", self)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_HEARTBEAT;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        CLOG_D(ctx, "Execute"
            << ": pending# " << Self->PendingHeartbeats.size());

        if (Self->Workers.empty()) {
            CLOG_W(ctx, "There are no workers");
            return true;
        }

        const auto prevMinVersion = !Self->WorkersByHeartbeat.empty()
            ? std::make_optional<TRowVersion>(Self->WorkersByHeartbeat.begin()->first)
            : std::nullopt; 

        NIceDb::TNiceDb db(txc.DB);

        for (const auto& [id, version] : Self->PendingHeartbeats) {
            if (!Self->Workers.contains(id)) {
                continue;
            }

            auto& worker = Self->Workers[id];
            if (worker.HasHeartbeat()) {
                auto it = Self->WorkersByHeartbeat.find(worker.GetHeartbeat());
                if (it != Self->WorkersByHeartbeat.end()) {
                    it->second.erase(id);
                    if (it->second.empty()) {
                        Self->WorkersByHeartbeat.erase(it);
                    }
                }
            }

            worker.SetHeartbeat(version);
            Self->WorkersWithHeartbeat.insert(id);
            Self->WorkersByHeartbeat[version].insert(id);

            db.Table<Schema::Workers>().Key(id.ReplicationId(), id.TargetId(), id.WorkerId()).Update(
                NIceDb::TUpdate<Schema::Workers::HeartbeatVersionStep>(version.Step),
                NIceDb::TUpdate<Schema::Workers::HeartbeatVersionTxId>(version.TxId)
            );
        }

        if (Self->Workers.size() != Self->WorkersWithHeartbeat.size()) {
            return true;
        }

        Y_ABORT_UNLESS(!Self->WorkersByHeartbeat.empty());
        const auto newMinVersion = Self->WorkersByHeartbeat.begin()->first;

        if (newMinVersion <= prevMinVersion.value_or(TRowVersion::Min())) {
            return true;
        }

        CLOG_N(ctx, "Min version has been changed"
            << ": prev# " << prevMinVersion.value_or(TRowVersion::Min())
            << ", new# " << newMinVersion);

        // TODO: run commit
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        CLOG_D(ctx, "Complete"
            << ": pending# " << Self->PendingHeartbeats.size());

        if (Self->PendingHeartbeats) {
            Self->Execute(new TTxHeartbeat(Self), ctx);
        } else {
            Self->ProcessHeartbeatsInFlight = false;
        }
    }

}; // TTxHeartbeat

void TController::RunTxHeartbeat(const TActorContext& ctx) {
    if (!ProcessHeartbeatsInFlight) {
        ProcessHeartbeatsInFlight = true;
        Execute(new TTxHeartbeat(this), ctx);
    }
}

}
