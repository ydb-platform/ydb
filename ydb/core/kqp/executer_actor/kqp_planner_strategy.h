#pragma once

#include <ydb/core/kqp/rm_service/kqp_resource_estimation.h>
#include <ydb/core/protos/kqp.pb.h>

#include <ydb/library/actors/core/actorid.h>

#include <util/generic/hash.h>

#include <optional>

namespace NKikimr::NKqp {

class IKqpPlannerStrategy {
public:
    virtual ~IKqpPlannerStrategy() = default;

    using TLogFunc = std::function<void(TStringBuf message)>;

    void SetLogFunc(TLogFunc&& logFunc) {
        LogFunc = std::move(logFunc);
    }

    struct TResult {
        ui32 NodeId;
        NActors::TActorId ResourceManager;
        TVector<ui64> TaskIds;
    };

    using TFixedAssignments = THashMap<ui64 /* taskId */, ui64 /* nodeId */>;

    // Only returns placements for free tasks. A plan with no free tasks can still succeed.
    virtual std::optional<TVector<TResult>> Plan(const TVector<const NKikimrKqp::TKqpNodeResources*>& nodeResources,
        const TVector<TTaskResourceEstimation>& estimatedResources, const TFixedAssignments& fixedAssignments) = 0;

protected:
    TLogFunc LogFunc;
};

THolder<IKqpPlannerStrategy> CreateKqpGreedyPlanner();

THolder<IKqpPlannerStrategy> CreateKqpMockEmptyPlanner();

} // namespace NKikimr::NKqp
