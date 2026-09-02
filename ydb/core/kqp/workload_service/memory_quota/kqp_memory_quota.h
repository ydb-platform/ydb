#pragma once

// WLM-owned memory quota managers for KQP compute actors.
//
// Placement rationale: ydb/services/workload_manager lives above ydb/core in the
// dependency hierarchy and cannot be PEERDIR'd from ydb/core/kqp/node_service.
// The quota managers are KQP-side WLM pieces, so they live here under
// ydb/core/kqp/workload_service/, consistent with the pattern used by other
// KQP subsystems (rm_service, node_service, …).
//
// Provider boundary: callers pass IKqpResourceManager (the RM interface) and
// TTxState; no RM implementation details appear in this header.

#include <memory>

#include <ydb/core/kqp/rm_service/kqp_rm_service.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

namespace NKikimr::NKqp {

// Per-task quota manager: tracks one task's MkQL memory against the RM.
// NOT thread-safe.
NYql::NDq::IMemoryQuotaManager::TPtr CreateTaskQuotaManager(
    std::shared_ptr<NRm::IKqpResourceManager> resourceManager,
    TIntrusivePtr<NRm::TTxState> tx,
    ui64 taskId,
    ui64 initialMemoryLimit);

// Per-TX channel quota manager: tracks channel buffers against the RM.
// Thread-safe (all fields are atomic or immutable after construction).
NYql::NDq::IMemoryQuotaManager::TPtr CreateChannelQuotaManager(
    std::shared_ptr<NRm::IKqpResourceManager> resourceManager,
    TIntrusivePtr<NRm::TTxState> tx,
    ui64 initialMemoryLimit,
    ui64 allocationStep = 1_MB);

} // namespace NKikimr::NKqp
