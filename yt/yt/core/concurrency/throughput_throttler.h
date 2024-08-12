#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Enables throttling sync and async operations.
/*!
 *  This interface and its implementations are vastly inspired by the "token bucket" algorithm and
 *  |DataTransferThrottler| class from Hadoop.
 *
 *  Thread affinity: any
 */
struct IThroughputThrottler
    : public virtual TRefCounted
{
    //! Assuming that we are about to utilize #amount units of some resource (e.g. bytes or requests),
    //! returns a future that is set when enough time has passed
    //! to ensure proper bandwidth utilization.
    /*!
     *  \note Thread affinity: any
     */
    virtual TFuture<void> Throttle(i64 amount) = 0;

    //! Tries to acquire #amount units for utilization.
    //! Returns |true| if the request could be served without overdraft.
    /*!
     *  \note Thread affinity: any
     */
    virtual bool TryAcquire(i64 amount) = 0;

    //! Tries to acquire #amount units for utilization.
    //! Returns number of bytes that could be served without overdraft.
    /*!
     *  \note Thread affinity: any
     */
    virtual i64 TryAcquireAvailable(i64 amount) = 0;

    //! Unconditionally acquires #amount units for utilization.
    //! This request could easily lead to an overdraft.
    /*!
     *  \note Thread affinity: any
     */
    virtual void Acquire(i64 amount) = 0;

    //! Releases #amount units back under control of the throttler.
    //! This method should be used cautiously as in current implementation
    //! it may locally disrupt fifo ordering or fairness of throttling requests.
    /*!
     *  \note Thread affinity: any
     */
    virtual void Release(i64 amount) = 0;

    //! Returns |true| if the throttling limit has been exceeded.
    /*!
     *  \note Thread affinity: any
     */
    virtual bool IsOverdraft() = 0;

    //! Returns total byte amount of all waiting requests.
    /*!
     *  \note Thread affinity: any
     */
    virtual i64 GetQueueTotalAmount() const = 0;

    //! Returns estimated duration to drain current request queue.
    /*!
     *  \note Thread affinity: any
     */
    virtual TDuration GetEstimatedOverdraftDuration() const = 0;

    //! Returns number of bytes in bucket, can be negative.
    /*!
     *  \note Thread affinity: any
     */
    virtual i64 GetAvailable() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IThroughputThrottler)

////////////////////////////////////////////////////////////////////////////////

//! Enables dynamic changes of throttling configuration.
/*!
 *  Thread affinity: any
 */
struct IReconfigurableThroughputThrottler
    : public IThroughputThrottler
{
    //! Updates the configuration.
    virtual void Reconfigure(TThroughputThrottlerConfigPtr config) = 0;

    //! Updates the limit.
    //! See TThroughputThrottlerConfig::Limit.
    virtual void SetLimit(std::optional<double> limit) = 0;

    //! Returns a future that is set when throttler has become available.
    virtual TFuture<void> GetAvailableFuture() = 0;

    //! Returns current throttler config.
    virtual TThroughputThrottlerConfigPtr GetConfig() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IReconfigurableThroughputThrottler)

////////////////////////////////////////////////////////////////////////////////

//! An interface for unit test purpose.
/*!
 *  Thread affinity: any
 */
struct ITestableReconfigurableThroughputThrottler
    : public IReconfigurableThroughputThrottler
{
    virtual void SetLastUpdated(TInstant lastUpdated) = 0;
};

DEFINE_REFCOUNTED_TYPE(ITestableReconfigurableThroughputThrottler)

////////////////////////////////////////////////////////////////////////////////

//! Constructs a throttler from #config.
IReconfigurableThroughputThrottlerPtr CreateReconfigurableThroughputThrottler(
    TThroughputThrottlerConfigPtr config,
    const NLogging::TLogger& logger = NLogging::TLogger(),
    const NProfiling::TProfiler& profiler = {});

//! Constructs a throttler from #config and initializes logger and profiler.
IReconfigurableThroughputThrottlerPtr CreateNamedReconfigurableThroughputThrottler(
    TThroughputThrottlerConfigPtr config,
    const TString& name,
    NLogging::TLogger logger,
    NProfiling::TProfiler profiler = {});

//! Returns a throttler that imposes no throughput limit.
IReconfigurableThroughputThrottlerPtr GetUnlimitedThrottler();

//! Returns a throttler that imposes no throughput limit and profiles throughput.
IReconfigurableThroughputThrottlerPtr CreateNamedUnlimitedThroughputThrottler(
    const TString& name,
    NProfiling::TProfiler profiler = {});

//! This throttler is DEPRECATED. Use TFairThrottler instead.
IThroughputThrottlerPtr CreateCombinedThrottler(
    const std::vector<IThroughputThrottlerPtr>& throttlers);

//! This throttler is DEPRECATED. Use TFairThrottler instead.
IThroughputThrottlerPtr CreateStealingThrottler(
    IThroughputThrottlerPtr stealer,
    IThroughputThrottlerPtr underlying);

//! This throttler limits RPS for the underlying throttler.
//! If this throttler's invocation RPS is higher than the specified limit,
//! throttling amounts are batched in a "prefetching" manner so that
//! a single request to the underlying throttler serves multiple incoming throttling requests.
IThroughputThrottlerPtr CreatePrefetchingThrottler(
    const TPrefetchingThrottlerConfigPtr& config,
    const IThroughputThrottlerPtr& underlying,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
