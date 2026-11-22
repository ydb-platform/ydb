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

    TUploadMemoryReservation(std::shared_ptr<void> attempt, std::shared_ptr<void> batch, TRetainer retain)
        : Attempt(std::move(attempt))
        , Batch(std::move(batch))
        , Retain(std::move(retain))
    {}

    explicit operator bool() const {
        return bool(Attempt);
    }

    std::shared_ptr<void> RetainRequestOverhead() const {
        return Retain ? Retain(SerializationOverhead) : nullptr;
    }

    template <class TRecord>
    std::shared_ptr<void> RetainRequest(const TRecord& record, std::shared_ptr<void> overhead) const {
        if (!Attempt) {
            return {};
        }
        if (!Retain) {
            return std::make_shared<TRequestReservation>(Attempt, Batch, nullptr);
        }
        // The protobuf and its sender-side serialization can coexist.
        const ui64 bytes = record.SpaceUsedLong() + record.ByteSizeLong();
        return std::make_shared<TRequestReservation>(Attempt, Retain(bytes), std::move(overhead));
    }

    std::shared_ptr<void> RetainRoutingMemory(ui64 bytes) const {
        return Retain ? Retain(bytes) : nullptr;
    }

private:
    struct TRequestReservation {
        std::shared_ptr<void> Attempt;
        std::shared_ptr<void> Payload;
        std::shared_ptr<void> Overhead;
    };

    static constexpr ui64 SerializationOverhead = 4_KB;

    // Requests share the attempt marker. With byte accounting the batch charge is actor-owned.
    std::shared_ptr<void> Attempt;
    std::shared_ptr<void> Batch;
    TRetainer Retain;
};

} // namespace NKikimr::NTxProxy
