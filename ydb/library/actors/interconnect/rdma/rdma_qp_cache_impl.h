#pragma once

#include <memory>
#include "rdma.h"

namespace NInterconnect::NRdma {

class IQpCache {
public:
    using TPtr = std::shared_ptr<IQpCache>;
    virtual ~IQpCache() = default;
    virtual ICq::TGetQpResult GetQp(const NActors::TActorContext& actorCtx, ICq::TPtr cq) noexcept = 0;
    virtual void Stop() noexcept = 0;
};

}