#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/retry/retry.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/time/time.h>
#include <ydb/public/sdk/cpp/src/client/impl/observability/span.h>

#include <library/cpp/threading/future/future.h>
#include <util/generic/function.h>
#include <util/generic/ptr.h>
#include <util/string/cast.h>
#include <util/system/type_name.h>

#include <chrono>
#include <cstdint>
#include <exception>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>

namespace NYdb::inline Dev::NRetry {

    enum class NextStep {
        RetryImmediately,
        RetryFastBackoff,
        RetrySlowBackoff,
        Finish,
    };

    struct TRetryDecision {
        NextStep Step;
        bool ResetSession = false;
    };

    TRetryDecision GetRetryDecision(EStatus status, const TRetryOperationSettings& settings);
    std::chrono::microseconds CalcBackoffTime(const TBackoffSettings& settings, std::uint32_t retryNumber);

    inline bool ShouldRetryStatus(EStatus status, const TRetryOperationSettings& settings) {
        return GetRetryDecision(status, settings).Step != NextStep::Finish;
    }

    template <typename TResult>
    TResult MakeRetryResultFromStatus(TStatus status) {
        return TResult(std::move(status));
    }

    template <typename TResult>
    const TStatus& GetRetryStatus(const TResult& result) {
        if constexpr (std::is_base_of_v<TStatus, TResult>) {
            return result;
        } else {
            return result.Status();
        }
    }

    template <typename TResult>
    EStatus GetRetryStatusCode(const TResult& result) {
        return GetRetryStatus(result).GetStatus();
    }

    template <typename TClient>
    class TRetryDeadlineHelper {
    public:
        static void SetDeadline(typename TClient::TSession& session, const TDeadline& deadline) {
            session.SetPropagatedDeadline(deadline);
        }
    };

    template <typename TClient>
    class TInRetryOperationContextClientGuard {
    public:
        explicit TInRetryOperationContextClientGuard(TClient& client)
            : Client_(client)
            , Previous_(client.GetInRetryOperationContext())
        {
            Client_.SetInRetryOperationContext(true);
        }

        ~TInRetryOperationContextClientGuard() {
            Client_.SetInRetryOperationContext(Previous_);
        }

    private:
        TClient& Client_;
        bool Previous_;
    };

    // The same session acquisition and operation dispatch serve both execution modes.
    template <typename TClient, bool WithSession>
    struct TRetrySessionState {
        void Reset() {
        }

        template <typename TContext, typename F>
        void Execute(TContext& context, F&& operation) {
            operation(context.GetClient());
        }
    };

    template <typename TClient>
    struct TRetrySessionState<TClient, true> {
        std::optional<typename TClient::TSession> Session;

        void Reset() {
            Session.reset();
        }

        template <typename TContext, typename F>
        void Execute(TContext& context, F operation) {
            if (Session) {
                operation(*Session);
                return;
            }
            auto settings = typename TClient::TCreateSessionSettings();
            settings.ClientTimeout(context.GetSettings().GetSessionClientTimeout_).Deadline(context.GetDeadline());
            context.Await(context.GetClient().GetSession(settings),
                          [this, &context, operation = std::move(operation)](const auto& result) mutable {
                              if (!result.IsSuccess()) {
                                  context.OnAttemptReady(MakeRetryResultFromStatus<typename TContext::TStatusType>(TStatus(result)));
                                  return;
                              }
                              Session = result.GetSession();
                              TRetryDeadlineHelper<TClient>::SetDeadline(*Session, context.GetDeadline());
                              if (!context.FinishIfStopped()) {
                                  operation(*Session);
                              }
                          });
        }
    };

    // Child hooks: ExecuteImpl, WaitAndExecute, StopRequested, GetRetryDelay, OnReady.
    template <typename TDerived, typename TResult>
    class TRetryContextBase: TNonCopyable {
    public:
        explicit TRetryContextBase(const TRetryOperationSettings& settings)
            : Settings_(settings)
            , Deadline_(TDeadline::AfterDuration(settings.MaxTimeout_))
        {
        }

        const TRetryOperationSettings& GetSettings() const {
            return Settings_;
        }

        const TDeadline& GetDeadline() const {
            return Deadline_;
        }

        bool IsFinished() const {
            return Finished_;
        }

        bool FinishIfStopped() {
            if (!Finished_ && Child().StopRequested()) {
                Finish(MakeRetryResultFromStatus<TResult>(TStatus(EStatus::CLIENT_CANCELLED, NIssue::TIssues{})));
            }
            return Finished_;
        }

    protected:
        void Start() {
            Guarded([&] {
                ParentSpan_ = Child().GetClientImpl()->CreateRetryRootSpan();
                RetryStartTime_ = TInstant::Now();
                RunAttempt();
            });
        }

        void DoNext(TResult result) {
            if (Finished_) {
                return;
            }
            const auto& status = GetRetryStatus(result);
            EndAttemptSpan(status.GetStatus());
            if (FinishIfStopped()) {
                return;
            }
            if (status.IsSuccess() || RetryNumber_ >= Settings_.MaxRetries_ || TInstant::Now() - RetryStartTime_ >= Settings_.MaxTimeout_) {
                Finish(std::move(result));
                return;
            }
            const auto decision = GetRetryDecision(status.GetStatus(), Settings_);
            if (decision.ResetSession) {
                Child().ResetSession();
            }
            if (decision.Step == NextStep::Finish) {
                Finish(std::move(result));
                return;
            }
            const auto delay = Child().GetRetryDelay(decision.Step);
            ++RetryNumber_;
            Child().CollectRetryStat(status.GetStatus());
            if (Settings_.Verbose_) {
                std::cerr << "Previous query attempt was finished with unsuccessful status "
                          << ToString(status.GetStatus()) << ": " << status.GetIssues().ToString(true) << std::endl;
                std::cerr << "Sending retry attempt " << RetryNumber_ << " of " << Settings_.MaxRetries_ << std::endl;
            }
            LastBackoffMs_ = std::chrono::duration_cast<std::chrono::milliseconds>(delay).count();
            Child().WaitAndExecute(delay, [this] { RunAttempt(); });
        }

        std::chrono::microseconds GetRetryDelay(NextStep step, std::uint32_t retryNumber) const {
            if (step == NextStep::RetryImmediately) {
                return {};
            }
            return CalcBackoffTime(step == NextStep::RetryFastBackoff
                                       ? Settings_.FastBackoffSettings_
                                       : Settings_.SlowBackoffSettings_, retryNumber);
        }

        template <typename TOperation, typename TTarget>
        auto InvokeOperation(TOperation& operation, TTarget& target) {
            TInRetryOperationContextClientGuard guard(Child().GetClient());
            if constexpr (TFunctionArgs<std::decay_t<TOperation>>::Length == 1) {
                return operation(target);
            } else {
                return operation(target, Settings_.MaxTimeout_ - (TInstant::Now() - RetryStartTime_));
            }
        }

        template <typename F>
        void Guarded(F&& fn) {
            try {
                [[maybe_unused]] auto scope = AttemptSpan_ ? AttemptSpan_->Activate() : nullptr;
                fn();
            } catch (const NStatusHelpers::TYdbRangeErrorException& e) {
                Child().OnAttemptReady(MakeRetryResultFromStatus<TResult>(TStatus(e.GetStatus())));
            } catch (...) {
                Finish(std::current_exception());
            }
        }

        TRetryOperationSettings Settings_;
        std::uint32_t RetryNumber_ = 0;

    private:
        TDerived& Child() {
            return static_cast<TDerived&>(*this);
        }

        void RunAttempt() {
            Guarded([&] {
                if (FinishIfStopped()) {
                    return;
                }
                [[maybe_unused]] auto parentScope = ParentSpan_ ? ParentSpan_->Activate() : nullptr;
                AttemptSpan_ = Child().GetClientImpl()->CreateRetryAttemptSpan(RetryNumber_, LastBackoffMs_, ParentSpan_);
                [[maybe_unused]] auto attemptScope = AttemptSpan_ ? AttemptSpan_->Activate() : nullptr;
                Child().ExecuteImpl();
            });
        }

        void EndAttemptSpan(EStatus status) {
            if (auto span = std::exchange(AttemptSpan_, nullptr)) {
                span->End(status);
            }
        }

        template <typename T>
        void Finish(T&& result) {
            if (std::exchange(Finished_, true)) {
                return;
            }
            if (ParentSpan_) {
                ParentSpan_->SetRetryCount(RetryNumber_);
            }
            if constexpr (std::is_same_v<std::decay_t<T>, std::exception_ptr>) {
                EndAttemptSpan(EStatus::CLIENT_INTERNAL_ERROR);
                if (ParentSpan_) {
                    try {
                        std::rethrow_exception(result);
                    } catch (const std::exception& e) {
                        ParentSpan_->EndWithException(TypeName(e).c_str(), e.what());
                    } catch (...) {
                        ParentSpan_->EndWithException("unknown", "unknown exception");
                    }
                }
            } else {
                EndAttemptSpan(GetRetryStatusCode(result));
                if (ParentSpan_) {
                    ParentSpan_->End(GetRetryStatusCode(result));
                }
            }
            Child().OnReady(std::forward<T>(result));
        }

        const TDeadline Deadline_;
        TInstant RetryStartTime_;
        std::shared_ptr<NObservability::TRequestSpan> ParentSpan_;
        std::shared_ptr<NObservability::TRequestSpan> AttemptSpan_;
        std::int64_t LastBackoffMs_ = 0;
        bool Finished_ = false;
    };

} // namespace NYdb::inline Dev::NRetry
