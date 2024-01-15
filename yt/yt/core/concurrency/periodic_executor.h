#pragma once

#include "periodic_executor_base.h"
#include "public.h"

#include <yt/yt/core/actions/callback.h>
#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/misc/backoff_strategy.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

struct TPeriodicExecutorOptions
{
    static constexpr double DefaultJitter = 0.2;

    //! Interval between usual consequent invocations.
    //! If nullopt then no invocations will be happening.
    std::optional<TDuration> Period;
    TDuration Splay;
    double Jitter = 0.0;

    //! Sets #Period and Applies set#DefaultJitter.
    static TPeriodicExecutorOptions WithJitter(TDuration period);
};

class TPeriodicExecutorOptionsSerializer
    : public NYTree::TExternalizedYsonStruct<TPeriodicExecutorOptions>
{
public:
    REGISTER_EXTERNALIZED_YSON_STRUCT(TPeriodicExecutorOptions, TPeriodicExecutorOptionsSerializer);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

class TDefaultInvocationTimePolicy
    : private TPeriodicExecutorOptions
{
public:
    using TCallbackResult = void;
    using TOptions = TPeriodicExecutorOptions;

    explicit TDefaultInvocationTimePolicy(const TOptions& options);

    void ProcessResult();

    TInstant KickstartDeadline();

    bool IsEnabled();

    bool ShouldKickstart(const TOptions& newOptions);

    void SetOptions(TOptions newOptions);

    bool ShouldKickstart(const std::optional<TDuration>& period);

    void SetOptions(std::optional<TDuration> period);

    TInstant NextDeadline();

    bool IsOutOfBandProhibited();

    void Reset();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

//! Helps to perform certain actions periodically.
class TPeriodicExecutor
    : public NDetail::TPeriodicExecutorBase<NDetail::TDefaultInvocationTimePolicy>
{
public:
    //! Initializes an instance.
    /*!
     *  \note
     *  We must call #Start to activate the instance.
     *
     *  \param invoker Invoker used for wrapping actions.
     *  \param callback Callback to invoke periodically.
     *  \param options Period, splay, etc.
     */
    TPeriodicExecutor(
        IInvokerPtr invoker,
        TPeriodicCallback callback,
        NConcurrency::TPeriodicExecutorOptions options);

    TPeriodicExecutor(
        IInvokerPtr invoker,
        TPeriodicCallback callback,
        std::optional<TDuration> period = {});

    //! Changes execution period.
    void SetPeriod(std::optional<TDuration> period);

private:
    using TBase = NDetail::TPeriodicExecutorBase<NDetail::TDefaultInvocationTimePolicy>;

};

DEFINE_REFCOUNTED_TYPE(TPeriodicExecutor)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

ASSIGN_EXTERNAL_YSON_SERIALIZER(NYT::NConcurrency::TPeriodicExecutorOptions, NYT::NConcurrency::TPeriodicExecutorOptionsSerializer);
