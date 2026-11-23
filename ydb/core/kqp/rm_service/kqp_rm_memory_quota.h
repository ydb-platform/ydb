#pragma once

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>

namespace NKikimr::NKqp::NRm {

class IKqpResourceManager;

NYql::NDq::IMemoryQuotaManager::TPtr CreateMemoryQuotaManager(std::shared_ptr<IKqpResourceManager> resourceManager);

} // namespace NKikimr::NKqp::NRm
