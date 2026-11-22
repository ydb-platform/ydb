#pragma once

#include <util/generic/size_literals.h>

#include <functional>
#include <memory>
#include <utility>

namespace NKikimr::NTxProxy {

class TUploadMemoryReservation {
public:
    using TRetainer = std::function<std::shared_ptr<void>(ui64)>;

    TUploadMemoryReservation() = default;

    TUploadMemoryReservation(std::shared_ptr<void> attempt, std::shared_ptr<void> batch, TRetainer retain, ui64 rows)
        : Attempt(std::move(attempt))
        , Batch(std::move(batch))
        , Retain(std::move(retain))
        , UnroutedSerialization(Retain ? Retain(rows * SerializationOverhead) : nullptr)
    {}

    explicit operator bool() const {
        return bool(Attempt);
    }

    template <class TRecord>
    std::shared_ptr<void> RetainRequest(const TRecord& record) const {
        if (!Retain) {
            return Attempt;
        }
        // The protobuf and its sender-side serialization can coexist.
        const ui64 bytes = record.SpaceUsedLong() + record.ByteSizeLong() + SerializationOverhead;
        return std::make_shared<std::pair<std::shared_ptr<void>, std::shared_ptr<void>>>(
            Attempt, Retain(bytes));
    }

    std::shared_ptr<void> RetainRoutingMemory(ui64 bytes) const {
        return Retain ? Retain(bytes) : nullptr;
    }

    void FinishRouting() {
        // Each routed request must own its charge before the estimate is released.
        UnroutedSerialization.reset();
    }

private:
    static constexpr ui64 SerializationOverhead = 4_KB;

    // Requests share the attempt marker. Only the actor holds the batch charge.
    std::shared_ptr<void> Attempt;
    std::shared_ptr<void> Batch;
    TRetainer Retain;
    std::shared_ptr<void> UnroutedSerialization;
};

} // namespace NKikimr::NTxProxy
