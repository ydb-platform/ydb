#pragma once
#include "query_helpers.h"

#include <ydb/core/fq/libs/wasm_services/mock/tvm.h>

namespace NFq::NWasmServices::NTestHelpers {
struct TTvmTestState {
    std::atomic<bool> Pending{ false };
    std::atomic<bool> Fail{ false };
    std::atomic<size_t> Cancelled{ 0 };
    std::mutex Mutex;
    TVector<TTvmCredentials> Calls;
};

class TTvmTestRequest final: public ITvmTicketRequest {
    std::shared_ptr<TTvmTestState> State;
    TString Ticket;
    std::mutex Mutex;
    ITvmTicketProvider::TCompletion Completion;
    bool Cancelled = false;

public:
    TTvmTestRequest(std::shared_ptr<TTvmTestState> state, TString ticket, ITvmTicketProvider::TCompletion completion)
        : State(std::move(state))
        , Ticket(std::move(ticket))
        , Completion(std::move(completion))
    {
    }

    void Complete() {
        ITvmTicketProvider::TCompletion completion;
        {
            std::lock_guard lock(Mutex);
            completion = std::exchange(Completion, {});
        }
        if (completion) {
            completion({ .Status = State->Fail ? ETvmTicketStatus::Failed : ETvmTicketStatus::Ready, .Ticket = Ticket });
        }
    }

    void Cancel() noexcept override {
        std::lock_guard lock(Mutex);
        Completion = {};
        if (!std::exchange(Cancelled, true)) {
            ++State->Cancelled;
        }
    }
};

class TTvmTestProvider final: public ITvmTicketProvider {
public:
    std::shared_ptr<TTvmTestState> State = std::make_shared<TTvmTestState>();
    TString Prefix = "test-service-ticket-";
    TVector<std::weak_ptr<TTvmTestRequest>> Requests;
    TCompletion LastCompletion;

    std::shared_ptr<ITvmTicketRequest> Request(const TTvmCredentials& credentials, TInstant deadline, TCompletion completion) override {
        UNIT_ASSERT(deadline > TInstant::Now());
        std::shared_ptr<TTvmTestRequest> request;
        bool pending;
        {
            std::lock_guard lock(State->Mutex);
            State->Calls.push_back(credentials);
            LastCompletion = completion;
            request = std::make_shared<TTvmTestRequest>(State, Prefix + ToString(State->Calls.size()), std::move(completion));
            Requests.push_back(request);
            pending = State->Pending;
        }
        if (!pending) {
            request->Complete();   // exercise inline completion before Request returns
        }
        return request;
    }

    void Ready() {
        TVector<std::weak_ptr<TTvmTestRequest>> requests;
        {
            std::lock_guard lock(State->Mutex);
            State->Pending = false;
            requests = Requests;
        }
        for (auto& weak : requests) {
            if (auto request = weak.lock()) {
                request->Complete();
            }
        }
    }

    size_t Count() {
        std::lock_guard lock(State->Mutex);
        return State->Calls.size();
    }
};

struct TControlledTvmScheduler {
    struct TTimer {
        std::mutex Mutex;
        std::function<void()> Callback;

        void Fire() {
            std::function<void()> callback;
            {
                std::lock_guard lock(Mutex);
                callback = std::exchange(Callback, {});
            }
            if (callback) {
                callback();
            }
        }

        void Cancel() {
            std::lock_guard lock(Mutex);
            Callback = {};
        }
    };

    std::mutex Mutex;
    TVector<std::shared_ptr<TTimer>> Timers;
    std::atomic<bool> OnUserPool{ true };

    TMockTvmScheduler Scheduler() {
        return [this](TDuration delay, std::function<void()> callback) {
            UNIT_ASSERT_VALUES_EQUAL(delay, TDuration::Seconds(30));
            if (!NActors::TlsThreadContext || NActors::TlsThreadContext->PoolId() != 1) {
                OnUserPool = false;
            }
            auto timer = std::make_shared<TTimer>();
            timer->Callback = std::move(callback);
            {
                std::lock_guard lock(Mutex);
                Timers.push_back(timer);
            }
            return [timer] {
                timer->Cancel();
            };
        };
    }

    size_t Count() {
        std::lock_guard lock(Mutex);
        return Timers.size();
    }

    void Fire() {
        TVector<std::shared_ptr<TTimer>> timers;
        {
            std::lock_guard lock(Mutex);
            timers = Timers;
        }
        for (auto& timer : timers) {
            timer->Fire();
        }
    }
};

inline void SetTvm(TBinding& binding, std::shared_ptr<ITvmTicketProvider> provider) {
    binding.Headers.clear();
    binding.TvmProvider = std::move(provider);
    binding.TvmCredentials = { 2020001, 2020002, "test-tvm-source-secret" };
}

}   // namespace NFq::NWasmServices::NTestHelpers
