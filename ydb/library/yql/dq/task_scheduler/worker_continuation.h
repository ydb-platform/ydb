#pragma once

namespace NYql::NDq {

class TTaskWorkPool;

// A worker has finished its slice and will select more work in this activation.
// Availability/inline-completion callbacks on this thread can leave one ready
// task for that selection instead of waking another worker to race for it.
// The caller must Take again, or Reschedule after leaving this scope. Do not
// cover RunSlice: a running producer must still wake parallel consumers.
class TWorkerContinuationScope {
public:
    explicit TWorkerContinuationScope(const TTaskWorkPool& pool) noexcept;
    ~TWorkerContinuationScope();
    TWorkerContinuationScope(const TWorkerContinuationScope&) = delete;
    TWorkerContinuationScope& operator=(const TWorkerContinuationScope&) = delete;

    static bool IsActive(const TTaskWorkPool& pool) noexcept;

private:
    const TTaskWorkPool* const Previous;
};

} // namespace NYql::NDq
