#pragma once

#include <ydb/core/kqp/rm_service/kqp_resource_estimation.h>
#include <ydb/core/protos/kqp.pb.h>

#include <ydb/library/actors/core/actorid.h>

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

    virtual TVector<TResult> Plan(const TVector<const NKikimrKqp::TKqpNodeResources*>& nodeResources,
        const TVector<TTaskResourceEstimation>& estimatedResources) = 0;

protected:
    TLogFunc LogFunc;
};

THolder<IKqpPlannerStrategy> CreateKqpGreedyPlanner();

THolder<IKqpPlannerStrategy> CreateKqpMockEmptyPlanner();

} // namespace NKikimr::NKqp
