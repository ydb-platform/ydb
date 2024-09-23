#pragma once

#include <ydb/core/fq/libs/compute/common/run_actor_params.h>
#include <ydb/core/fq/libs/metrics/status_code_counters.h>

#include <ydb/library/yql/providers/common/metrics/service_counters.h>

#include <ydb/public/sdk/cpp/client/ydb_query/query.h>

#include <util/generic/ptr.h>

namespace NFq {

struct IActorFactory : public TThrRefBase {
    using TPtr = TIntrusivePtr<IActorFactory>;

    virtual std::unique_ptr<NActors::IActor> CreatePinger(const NActors::TActorId& parent) const = 0;
    virtual std::unique_ptr<NActors::IActor> CreateConnector() const = 0;

    virtual std::unique_ptr<NActors::IActor> CreateInitializer(const NActors::TActorId& parent,
                                                               const NActors::TActorId& pinger) const = 0;
    virtual std::unique_ptr<NActors::IActor> CreateExecuter(const NActors::TActorId &parent,
                                                            const NActors::TActorId &connector,
                                                            const NActors::TActorId &pinger) const = 0;
    virtual std::unique_ptr<NActors::IActor> CreateStatusTracker(const NActors::TActorId &parent,
                                                                 const NActors::TActorId &connector,
                                                                 const NActors::TActorId &pinger,
                                                                 const NYdb::TOperation::TOperationId& operationId) const = 0;
    virtual std::unique_ptr<NActors::IActor> CreateResultWriter(const NActors::TActorId& parent,
                                                                const NActors::TActorId& connector,
                                                                const NActors::TActorId& pinger,
                                                                const NKikimr::NOperationId::TOperationId& operationId,
                                                                bool operationEntryExpected) const = 0;
    virtual std::unique_ptr<NActors::IActor> CreateResourcesCleaner(const NActors::TActorId& parent,
                                                                    const NActors::TActorId& connector,
                                                                    const NYdb::TOperation::TOperationId& operationId) const = 0;
    virtual std::unique_ptr<NActors::IActor> CreateFinalizer(const NFq::TRunActorParams& params,
                                                             const NActors::TActorId& parent,
                                                             const NActors::TActorId& pinger,
                                                             NYdb::NQuery::EExecStatus execStatus,
                                                             FederatedQuery::QueryMeta::ComputeStatus status) const = 0;
    virtual std::unique_ptr<NActors::IActor> CreateStopper(const NActors::TActorId& parent,
                                                           const NActors::TActorId& connector,
                                                           const NActors::TActorId& pinger,
                                                           const NYdb::TOperation::TOperationId& operationId) const = 0;
};

IActorFactory::TPtr CreateActorFactory(const TRunActorParams& params, const ::NYql::NCommon::TServiceCounters& serviceCounters, const NFq::TStatusCodeByScopeCounters::TPtr& failedStatusCodeCounters);

}
