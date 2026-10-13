#include "tvm.h"

#include <atomic>

namespace NFq::NWasmServices {
namespace {

class TMockTvmRequest final : public ITvmTicketRequest {
public:
    TMockTvmRequest(std::chrono::steady_clock::time_point readyAt, TMockTvmClock clock)
        : ReadyAt(readyAt), Clock(std::move(clock)) {}

    TTvmTicketResult Poll() override {
        if (Cancelled)
            return {.Status = ETvmTicketStatus::Failed};
        if (Clock() < ReadyAt)
            return {.Status = ETvmTicketStatus::Pending};
        return {.Status = ETvmTicketStatus::Ready, .Ticket = "fqrun-mock-tvm-service-ticket"};
    }

    void Cancel() noexcept override { Cancelled = true; }

private:
    const std::chrono::steady_clock::time_point ReadyAt;
    const TMockTvmClock Clock;
    std::atomic<bool> Cancelled{false};
};

class TMockTvmProvider final : public ITvmTicketProvider {
public:
    TMockTvmProvider(TDuration delay, TMockTvmClock clock)
        : Delay(delay.MicroSeconds()), Clock(std::move(clock)) {}

    std::shared_ptr<ITvmTicketRequest> Request(const TTvmCredentials&, TInstant) override {
        return std::make_shared<TMockTvmRequest>(Clock() + Delay, Clock);
    }

private:
    const std::chrono::microseconds Delay;
    const TMockTvmClock Clock;
};

} // namespace

std::shared_ptr<ITvmTicketProvider> CreateMockTvmTicketProvider(TDuration delay, TMockTvmClock clock) {
    return std::make_shared<TMockTvmProvider>(delay, std::move(clock));
}

} // namespace NFq::NWasmServices
