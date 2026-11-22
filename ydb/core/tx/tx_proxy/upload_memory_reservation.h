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

    TUploadMemoryReservation(std::shared_ptr<void> batch, TRetainer retain, ui64 rows)
        : Batch(std::move(batch))
        , Retain(std::move(retain))
        , UnroutedSerialization(Retain ? Retain(rows * SerializationOverhead) : nullptr)
    {}

    explicit operator bool() const {
        return bool(Batch);
    }

    std::shared_ptr<void> RetainRequests(ui64 requests) {
        // Routing has finished. Reserve one partial serialization chunk per
        // request before sending it, and let the requests own that charge.
        UnroutedSerialization.reset();
        if (!Retain || !requests) {
            return Batch;
        }
        return std::make_shared<std::pair<std::shared_ptr<void>, std::shared_ptr<void>>>(
            Batch, Retain(requests * SerializationOverhead));
    }

private:
    static constexpr ui64 SerializationOverhead = 4_KB;

    std::shared_ptr<void> Batch;
    TRetainer Retain;
    std::shared_ptr<void> UnroutedSerialization;
};

} // namespace NKikimr::NTxProxy
