#pragma once

#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/kqp/rm_service/kqp_resource_estimation.h>

#include <library/cpp/actors/core/actorid.h>

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

    virtual TVector<TResult> Plan(const TVector<NKikimrKqp::TKqpNodeResources>& nodeResources,
        const TVector<TTaskResourceEstimation>& estimatedResources) = 0;

protected:
    TLogFunc LogFunc;
};

THolder<IKqpPlannerStrategy> CreateKqpGreedyPlanner();

} // namespace NKikimr::NKqp
