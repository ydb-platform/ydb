#include "actors_factory.h"
#include "executer_actor.h"
#include "finalizer_actor.h"
#include "resources_cleaner_actor.h"
#include "result_writer_actor.h"
#include "status_tracker_actor.h"
#include "stopper_actor.h"
#include "ydb_connector_actor.h"

#include <ydb/core/fq/libs/compute/common/pinger.h>

namespace NFq {

struct TActorFactory : public IActorFactory {
    TActorFactory(const NFq::TRunActorParams& params, const ::NYql::NCommon::TServiceCounters& counters)
        : Params(params)
        , Counters(counters)
    {}

    std::unique_ptr<NActors::IActor> CreatePinger(const NActors::TActorId& parent) const override {
        return std::unique_ptr<NActors::IActor>(CreatePingerActor(
            Params.TenantName,
            Params.Scope,
            Params.UserId,
            Params.QueryId,
            Params.Owner,
            parent,
            Params.Config.GetPinger(),
            Params.Deadline,
            Counters,
            Params.CreatedAt,
            true
        ));
    }

    std::unique_ptr<NActors::IActor> CreateConnector() const override {
        return CreateConnectorActor(Params);
    }

    std::unique_ptr<NActors::IActor> CreateExecuter(const NActors::TActorId &parent,
                                                    const NActors::TActorId &connector,
                                                    const NActors::TActorId &pinger) const override {
        return CreateExecuterActor(Params, parent, connector, pinger, Counters);
    }

    std::unique_ptr<NActors::IActor> CreateStatusTracker(const NActors::TActorId &parent,
                                                         const NActors::TActorId &connector,
                                                         const NActors::TActorId &pinger,
                                                         const NYdb::TOperation::TOperationId& operationId) const override {
        return CreateStatusTrackerActor(Params, parent, connector, pinger, operationId, Counters);
    }

    std::unique_ptr<NActors::IActor> CreateResultWriter(const NActors::TActorId& parent,
                                                        const NActors::TActorId& connector,
                                                        const NActors::TActorId& pinger,
                                                        const TString& executionId) const override {
        return CreateResultWriterActor(Params, parent, connector, pinger, executionId, Counters);
    }

    std::unique_ptr<NActors::IActor> CreateResourcesCleaner(const NActors::TActorId& parent,
                                                            const NActors::TActorId& connector,
                                                            const NYdb::TOperation::TOperationId& operationId) const override {
        return CreateResourcesCleanerActor(Params, parent, connector, operationId, Counters);
    }

    std::unique_ptr<NActors::IActor> CreateFinalizer(const NActors::TActorId& parent,
                                                     const NActors::TActorId& pinger,
                                                     NYdb::NQuery::EExecStatus execStatus) const override {
        return CreateFinalizerActor(Params, parent, pinger, execStatus, Counters);
    }

    std::unique_ptr<NActors::IActor> CreateStopper(const NActors::TActorId& parent,
                                                   const NActors::TActorId& connector,
                                                   const NYdb::TOperation::TOperationId& operationId) const override {
        return CreateStopperActor(Params, parent, connector, operationId, Counters);
    }

private:
    NFq::TRunActorParams Params;
    ::NYql::NCommon::TServiceCounters Counters;
};

IActorFactory::TPtr CreateActorFactory(const NFq::TRunActorParams& params, const ::NYql::NCommon::TServiceCounters& counters) {
    return MakeIntrusive<TActorFactory>(params, counters);
}

}
