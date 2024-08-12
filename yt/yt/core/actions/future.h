#pragma once

#include "public.h"
#include "callback.h"
#include "invoker.h"

#include <yt/yt/core/misc/error.h>

#include <optional>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

/*
 *  Futures and Promises come in pairs and provide means for one party
 *  to wait for the result of the computation performed by the other party.
 *
 *  TPromise<T> encapsulates the value-returning mechanism while
 *  TFuture<T> enables the clients to wait for this value.
 *  The value type is always TErrorOr<T> (which reduces to just TError for |T = void|).
 *
 *  TPromise<T> is implicitly convertible to TFuture<T> while the reverse conversion
 *  is not allowed. This prevents a "malicious" client from setting the value
 *  by itself.
 *
 *  TPromise<T> and TFuture<T> are lightweight refcounted handles pointing to the internal
 *  shared state. TFuture<T> acts as a weak reference while TPromise<T> acts as
 *  a strong reference. When no outstanding strong references (i.e. promises) to
 *  the shared state remain, the state automatically becomes failed
 *  with NYT::EErrorCode::Canceled error code.
 *
 *  Promises support advisory cancellation. Consumer that holds a future might call Cancel() to notify
 *  producer that value is no longer needed. By default, Cancel() just Set()-s the associated shared state,
 *  and is equivalent to TrySet(TError(...)). If promise has associated cancelation handlers, Cancel()
 *  invokes them instead. It is cancelation handler's job to call TrySet() on the corresponding promise.
 *
 *  Futures and Promises are thread-safe.
 */

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class T>
class TPromiseState;
template <class T>
void Ref(TPromiseState<T>* state);
template <class T>
void Unref(TPromiseState<T>* state);

class TCancelableStateBase;
void Ref(TCancelableStateBase* state);
void Unref(TCancelableStateBase* state);

template <class T>
class TFutureState;
template <class T>
void Ref(TFutureState<T>* state);
template <class T>
void Unref(TFutureState<T>* state);

//! Constructs a well-known pre-set future like #VoidFuture.
//! For such futures ref-counting is essentially disabled.
template <class T>
[[nodiscard]] TFuture<T> MakeWellKnownFuture(TErrorOr<T> value);

template <class T>
constexpr bool IsFuture = false;

template <class T>
constexpr bool IsFuture<TFuture<T>> = true;

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

//! Creates an empty (unset) promise.
template <class T>
[[nodiscard]] TPromise<T> NewPromise();

//! Constructs a pre-set promise.
template <class T>
[[nodiscard]] TPromise<T> MakePromise(TErrorOr<T> value);
template <class T>
[[nodiscard]] TPromise<T> MakePromise(T value);

//! Constructs a successful pre-set future.
template <class T>
[[nodiscard]] TFuture<T> MakeFuture(TErrorOr<T> value);
template <class T>
[[nodiscard]] TFuture<T> MakeFuture(T value);

////////////////////////////////////////////////////////////////////////////////
// Comparison and swap.

template <class T>
bool operator==(const TFuture<T>& lhs, const TFuture<T>& rhs);
template <class T>
void swap(TFuture<T>& lhs, TFuture<T>& rhs);

template <class T>
bool operator==(const TPromise<T>& lhs, const TPromise<T>& rhs);
template <class T>
void swap(TPromise<T>& lhs, TPromise<T>& rhs);

////////////////////////////////////////////////////////////////////////////////
// A bunch of widely-used preset futures.

//! A pre-set successful |void| future.
extern const TFuture<void> VoidFuture;

//! A pre-set successful |bool| future with |true| value.
extern const TFuture<bool> TrueFuture;

//! A pre-set successful |bool| future with |false| value.
extern const TFuture<bool> FalseFuture;

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TFutureBase;

template <class T>
class TPromiseBase;

////////////////////////////////////////////////////////////////////////////////

//! A handle able of canceling some future.
class TCancelable
{
public:
    //! Creates a null cancelable.
    TCancelable() = default;

    //! Checks if the cancelable is null.
    explicit operator bool() const;

    //! Drops underlying associated state resetting the cancelable to null.
    void Reset();

    //! Notifies the producer that the promised value is no longer needed.
    //! Returns |true| if succeeded, |false| is the promise was already set or canceled.
    bool Cancel(const TError& error) const;

private:
    explicit TCancelable(TIntrusivePtr<NYT::NDetail::TCancelableStateBase> impl);

    TIntrusivePtr<NYT::NDetail::TCancelableStateBase> Impl_;

    friend bool operator==(const TCancelable& lhs, const TCancelable& rhs);
    friend void swap(TCancelable& lhs, TCancelable& rhs);
    template <class U>
    friend struct ::THash;
    template <class U>
    friend class TFutureBase;
};

////////////////////////////////////////////////////////////////////////////////

//! An opaque future callback id.
using TFutureCallbackCookie = int;
constexpr TFutureCallbackCookie NullFutureCallbackCookie = -1;

////////////////////////////////////////////////////////////////////////////////

struct TFutureTimeoutOptions
{
    //! If set to a non-trivial error, timeout or cancelation errors
    //! are enveloped into this error.
    TError Error;

    //! An invoker the timeout event is handled in (DelayedExecutor is null).
    IInvokerPtr Invoker;
};

////////////////////////////////////////////////////////////////////////////////

//! A base class for both TFuture<T> and its specialization TFuture<void>.
/*!
 *  The resulting value can be accessed by either subscribing (#Subscribe)
 *  for it or retrieving it explicitly (#Get, #TryGet). Also it is possible
 *  to move the value out of the future state (#SubscribeUnique, #GetUnique, #TryGetUnique).
 *  In the latter case, however, at most one extraction is possible;
 *  further attempts to access the value will result in UB.
 *  In particular, at most one call to #SubscribeUnique, #GetUnique, and #TryGetUnique (expect
 *  for calls returning null) must happen to any future state (possibly shared by multiple
 *  TFuture instances).
 */
template <class T>
class TFutureBase
{
public:
    using TValueType = T;

    //! Creates a null future.
    TFutureBase() = default;

    //! Checks if the future is null.
    explicit operator bool() const;

    //! Drops underlying associated state resetting the future to null.
    void Reset();

    //! Checks if the value is set.
    bool IsSet() const;

    //! Gets the value.
    /*!
     *  This call will block until the value is set.
     */
    const TErrorOr<T>& Get() const;

    //! Extracts the value by moving it out of the future state.
    /*!
     *  This call will block until the value is set.
     */
    TErrorOr<T> GetUnique() const;

    //! Waits for the value to become set.
    /*!
     *  This call blocks until either the value is set or #timeout (if given) expires.
     */
    bool Wait(TDuration timeout = TDuration::Max()) const;

    //! Waits for the value to become set.
    /*!
     *  This call blocks until either the value is set or #deadline is reached.
     */
    bool Wait(TInstant deadline) const;

    //! Gets the value; returns null if the value is not set yet.
    /*!
     *  This call does not block.
     */
    std::optional<TErrorOr<T>> TryGet() const;

    //! Extracts the value by moving it out of the future state; returns null if the value is not set yet.
    /*!
     *  This call does not block.
     */
    std::optional<TErrorOr<T>> TryGetUnique() const;

    //! Attaches a result handler.
    /*!
     *  \param handler A callback to call when the value gets set
     *  (passing the value as a parameter).
     *
     *  \returns a cookie that can later be passed to #Unsubscribe to remove the handler.
     *
     *  \note
     *  If the value is set before the call to #Subscribe, then
     *  #callback gets called synchronously. In this case the returned
     *  cookie is #NullFutureCallbackCookie.
     *
     *  \note
     *  If the callback throws an exception, the program terminates with
     *  a call to std::terminate. This is because the subscribers are notified synchronously
     *  and thus we have to ensure that the promise state remains valid by correctly
     *  finishing the Set call.
     */
    TFutureCallbackCookie Subscribe(TCallback<void(const TErrorOr<T>&)> handler) const;

    //! Unsubscribes a previously subscribed callback.
    /*!
     *  Callback cookies are recycled; don't retry calls to this function.
     */
    void Unsubscribe(TFutureCallbackCookie cookie) const;

    //! Similar to #Subscribe but enables moving the value to the handler.
    /*!
     *  Normally at most one such handler could be attached.
     */
    void SubscribeUnique(TCallback<void(TErrorOr<T>&&)> handler) const;

    //! Notifies the producer that the promised value is no longer needed.
    //! Returns |true| if succeeded, |false| is the promise was already set or canceled.
    bool Cancel(const TError& error) const;

    //! Returns a wrapper that suppresses cancellation attempts.
    TFuture<T> ToUncancelable() const;

    //! Returns a wrapper that handles cancellation requests by immediately becoming set
    //! with NYT::EErrorCode::Canceled code.
    TFuture<T> ToImmediatelyCancelable() const;

    //! Returns a future that is either set to an actual value (if the original one is set in timely manner)
    //! or to |EErrorCode::Timeout| (in case the deadline is reached).
    TFuture<T> WithDeadline(
        TInstant deadline,
        TFutureTimeoutOptions options = {}) const;

    //! Returns a future that is either set to an actual value (if the original one is set in timely manner)
    //! or to |EErrorCode::Timeout| (in case of timeout).
    TFuture<T> WithTimeout(
        TDuration timeout,
        TFutureTimeoutOptions options = {}) const;
    TFuture<T> WithTimeout(
        std::optional<TDuration> timeout,
        TFutureTimeoutOptions options = {}) const;

    //! Chains the asynchronous computation with another one.
    template <class R>
    TFuture<R> Apply(TCallback<R(const TErrorOr<T>&)> callback) const;
    template <class R>
    TFuture<R> Apply(TCallback<TErrorOr<R>(const TErrorOr<T>&)> callback) const;
    template <class R>
    TFuture<R> Apply(TCallback<TFuture<R>(const TErrorOr<T>&)> callback) const;

    //! Same as #Apply but assumes that this chaining will be the only subscriber.
    template <class R>
    TFuture<R> ApplyUnique(TCallback<R(TErrorOr<T>&&)> callback) const;
    template <class R>
    TFuture<R> ApplyUnique(TCallback<TErrorOr<R>(TErrorOr<T>&&)> callback) const;
    template <class R>
    TFuture<R> ApplyUnique(TCallback<TFuture<R>(TErrorOr<T>&&)> callback) const;

    //! Converts (successful) result to |U|; propagates errors as is.
    template <class U>
    TFuture<U> As() const;

    //! Converts to TFuture<void> by discarding the value; propagates errors as is.
    TFuture<void> AsVoid() const;

    //! Converts to TCancelable interface.
    TCancelable AsCancelable() const;

protected:
    explicit TFutureBase(TIntrusivePtr<NYT::NDetail::TFutureState<T>> impl);

    TIntrusivePtr<NYT::NDetail::TFutureState<T>> Impl_;

    template <class U>
    friend bool operator==(const TFuture<U>& lhs, const TFuture<U>& rhs);
    template <class U>
    friend void swap(TFuture<U>& lhs, TFuture<U>& rhs);
    template <class U>
    friend struct ::THash;
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
class [[nodiscard]] TFuture
    : public TFutureBase<T>
{
public:
    TFuture() = default;
    TFuture(std::nullopt_t);

    //! Chains the asynchronous computation with another one.
    template <class R>
    TFuture<R> Apply(TCallback<R(const T&)> callback) const;
    template <class R>
    TFuture<R> Apply(TCallback<R(T)> callback) const;
    template <class R>
    TFuture<R> Apply(TCallback<TFuture<R>(const T&)> callback) const;
    template <class R>
    TFuture<R> Apply(TCallback<TFuture<R>(T)> callback) const;

    //! Same as #Apply but assumes that this chaining will be the only subscriber.
    template <class R>
    TFuture<R> ApplyUnique(TCallback<R(T&&)> callback) const;
    template <class R>
    TFuture<R> ApplyUnique(TCallback<TFuture<R>(T&&)> callback) const;

    using TFutureBase<T>::Apply;
    using TFutureBase<T>::ApplyUnique;

private:
    explicit TFuture(TIntrusivePtr<NYT::NDetail::TFutureState<T>> impl);

    template <class U>
    friend TFuture<U> MakeFuture(TErrorOr<U> value);
    template <class U>
    friend TFuture<U> NDetail::MakeWellKnownFuture(TErrorOr<U> value);
    template <class U>
    friend TFuture<U> MakeFuture(U value);
    template <class U>
    friend class TFutureBase;
    template <class U>
    friend class TPromiseBase;
};

////////////////////////////////////////////////////////////////////////////////

template <>
class [[nodiscard]] TFuture<void>
    : public TFutureBase<void>
{
public:
    TFuture() = default;
    TFuture(std::nullopt_t);

    //! Chains the asynchronous computation with another one.
    template <class R>
    TFuture<R> Apply(TCallback<R()> callback) const;
    template <class R>
    TFuture<R> Apply(TCallback<TFuture<R>()> callback) const;

    using TFutureBase<void>::Apply;

private:
    explicit TFuture(const TIntrusivePtr<NYT::NDetail::TFutureState<void>> impl);

    template <class U>
    friend TFuture<U> MakeFuture(TErrorOr<U> value);
    template <class U>
    friend TFuture<U> NDetail::MakeWellKnownFuture(TErrorOr<U> value);
    template <class U>
    // XXX(babenko): 'NYT::' is a workaround; cf. https://gcc.gnu.org/bugzilla/show_bug.cgi?id=52625
    friend class NYT::TFutureBase;
    template <class U>
    friend class TPromiseBase;
};

////////////////////////////////////////////////////////////////////////////////

//! A base class for both TPromise<T> and its specialization TPromise<void>.
template <class T>
class TPromiseBase
{
public:
    using TValueType = T;

    //! Creates a null promise.
    TPromiseBase() = default;

    //! Checks if the promise is null.
    explicit operator bool() const;

    //! Drops underlying associated state resetting the promise to null.
    void Reset();

    //! Checks if the value is set.
    bool IsSet() const;

    //! Sets the value.
    /*!
     *  Calling this method also invokes all the subscribers.
     */
    void Set(const TErrorOr<T>& value) const;
    void Set(TErrorOr<T>&& value) const;

    //! Sets the value when #another future is set.
    template <class U>
    void SetFrom(const TFuture<U>& another) const;

    //! Atomically invokes |Set|, if not already set or canceled.
    //! Returns |true| if succeeded, |false| is the promise was already set or canceled.
    bool TrySet(const TErrorOr<T>& value) const;
    bool TrySet(TErrorOr<T>&& value) const;

    //! Similar to #SetFrom but calls #TrySet instead of #Set.
    template <class U>
    void TrySetFrom(TFuture<U> another) const;

    //! Gets the value.
    /*!
     *  This call will block until the value is set.
     */
    const TErrorOr<T>& Get() const;

    //! Gets the value if set.
    /*!
     *  This call does not block.
     */
    std::optional<TErrorOr<T>> TryGet() const;

    //! Checks if the promise is canceled.
    bool IsCanceled() const;

    //! Attaches a cancellation handler.
    /*!
     *  \param handler A callback to call when TFuture<T>::Cancel is triggered
     *  by the client.
     *
     *  Returns true if handler was successfully registered or was invoked inline.
     *
     *  \note
     *  If the value is set before the call to #handlered, then
     *  #handler is discarded.
     */
    bool OnCanceled(TCallback<void(const TError&)> handler) const;

    //! Converts promise into future.
    operator TFuture<T>() const;
    TFuture<T> ToFuture() const;

protected:
    explicit TPromiseBase(TIntrusivePtr<NYT::NDetail::TPromiseState<T>> impl);

    TIntrusivePtr<NYT::NDetail::TPromiseState<T>> Impl_;

    template <class U>
    friend bool operator==(const TPromise<U>& lhs, const TPromise<U>& rhs);
    template <class U>
    friend void swap(TPromise<U>& lhs, TPromise<U>& rhs);
    template <class U>
    friend struct ::hash;
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TPromise
    : public TPromiseBase<T>
{
public:
    TPromise() = default;
    TPromise(std::nullopt_t);

    void Set(const T& value) const;
    void Set(T&& value) const;
    void Set(const TError& error) const;
    void Set(TError&& error) const;
    using TPromiseBase<T>::Set;

    bool TrySet(const T& value) const;
    bool TrySet(T&& value) const;
    bool TrySet(const TError& error) const;
    bool TrySet(TError&& error) const;
    using TPromiseBase<T>::TrySet;

private:
    explicit TPromise(TIntrusivePtr<NYT::NDetail::TPromiseState<T>> impl);

    template <class U>
    friend TPromise<U> NewPromise();
    template <class U>
    friend TPromise<U> MakePromise(TErrorOr<U> value);
    template <class U>
    friend TPromise<U> MakePromise(U value);
};

////////////////////////////////////////////////////////////////////////////////

template <>
class TPromise<void>
    : public TPromiseBase<void>
{
public:
    TPromise() = default;
    TPromise(std::nullopt_t);

    void Set() const;
    using TPromiseBase<void>::Set;

    bool TrySet() const;
    using TPromiseBase<void>::TrySet;

private:
    explicit TPromise(TIntrusivePtr<NYT::NDetail::TPromiseState<void>> state);

    template <class U>
    friend TPromise<U> NewPromise();
    template <class U>
    friend TPromise<U> MakePromise(TErrorOr<U> value);
};

////////////////////////////////////////////////////////////////////////////////

//! Provides a noncopyable but movable wrapper around TFuture<T> whose destructor
//! cancels the underlying future.
/*!
 *  TFutureHolder wraps a (typically resource-consuming) computation and cancels it on scope exit
 *  thus preventing leaking this computation.
 */
template <class T>
class TFutureHolder
{
public:
    //! Constructs an empty holder.
    TFutureHolder() = default;

    //! Constructs an empty holder.
    TFutureHolder(std::nullopt_t);

    //! Wraps #future into a holder.
    TFutureHolder(TFuture<T> future);

    //! Cancels the underlying future (if any).
    ~TFutureHolder();

    TFutureHolder(const TFutureHolder<T>& other) = delete;
    TFutureHolder(TFutureHolder<T>&& other) = default;

    TFutureHolder& operator = (const TFutureHolder<T>& other) = delete;
    TFutureHolder& operator = (TFutureHolder<T>&& other) = default;

    //! Returns |true| if the holder has an underlying future.
    explicit operator bool() const;

    //! Returns the underlying future.
    const TFuture<T>& Get() const;

    //! Returns the underlying future.
    TFuture<T>& Get();

    //! Returns the underlying future.
    const TFuture<T>& operator*() const; // noexcept

    //! Returns the underlying future.
    TFuture<T>& operator*(); // noexcept

    //! Returns the underlying future.
    const TFuture<T>* operator->() const; // noexcept

    //! Returns the underlying future.
    TFuture<T>* operator->(); // noexcept

private:
    TFuture<T> Future_;
};

////////////////////////////////////////////////////////////////////////////////
// Future combiners: take a set of futures and meld them into a new one.

template <class T>
struct TFutureCombinerTraits
{
    using TCombinedVector = std::vector<T>;
};

template <>
struct TFutureCombinerTraits<void>
{
    using TCombinedVector = void;
};

struct TFutureCombinerOptions
{
    //! If true, canceling the future returned from the combiner
    //! automatically cancels the original input futures.
    bool PropagateCancelationToInput = true;

    //! If true, the combiner cancels all irrelevant input futures
    //! when the combined future gets set. E.g. when #AnySucceeded
    //! notices some of its inputs being set, it cancels the others.
    bool CancelInputOnShortcut = true;
};

//! Returns the future that gets set when any of #futures is set.
//! The value of the returned future is set to the value of that first-set
//! future among #futures.
//! Individual errors are ignored; if all input futures fail then an error is reported.
template <class T>
TFuture<T> AnySucceeded(
    std::vector<TFuture<T>> futures,
    TFutureCombinerOptions options = {});

//! Same as above.
//! Errors happening in #futures are regarded as regular values; the first-set (either
//! successfully or not) future completes the whole computation.
template <class T>
TFuture<T> AnySet(
    std::vector<TFuture<T>> futures,
    TFutureCombinerOptions options = {});

//! Returns the future that gets set when all of #futures are set.
//! The values of #futures are collected and returned in the
//! value of the combined future (the order matches that of #futures).
//! When some of #futures fail, its error is immediately propagated into the combined future
//! and thus interrupts the whole computation.
template <class T>
TFuture<typename TFutureCombinerTraits<T>::TCombinedVector> AllSucceeded(
    std::vector<TFuture<T>> futures,
    TFutureCombinerOptions options = {});

//! Same as above.
//! The values of #futures are wrapped in #TErrorOr; individual
//! errors are propagated as-is and do not interrupt the whole computation.
template <class T>
TFuture<std::vector<TErrorOr<T>>> AllSet(
    std::vector<TFuture<T>> futures,
    TFutureCombinerOptions options = {});

//! Same as above, but with an additional timeout parameter: a timeout error is returned for futures
//! that don't complete within the given duration.
//! The timeout event is handled in #invoker (DelayedExecutor is null).
template <class T>
TFuture<std::vector<TErrorOr<T>>> AllSetWithTimeout(
    std::vector<TFuture<T>> futures,
    TDuration timeout,
    TFutureCombinerOptions options = {},
    IInvokerPtr invoker = nullptr);

//! Returns the future that gets set when #n of #futures are set.
//! The values of #futures are collected and returned in the
//! value of the combined future (in the order of their fulfillment,
//! which is typically racy).
//! Individual errors are ignored; if less than #n successful results
//! could be collected then an error is reported.
template <class T>
TFuture<typename TFutureCombinerTraits<T>::TCombinedVector> AnyNSucceeded(
    std::vector<TFuture<T>> futures,
    int n,
    TFutureCombinerOptions options = {});

//! Same as above.
//! The values of #futures are wrapped in TErrorOr; individual
//! errors are propagated as-is and do not interrupt the whole computation.
template <class T>
TFuture<std::vector<TErrorOr<T>>> AnyNSet(
    std::vector<TFuture<T>> futures,
    int n,
    TFutureCombinerOptions options = {});

////////////////////////////////////////////////////////////////////////////////

// TODO(akozhikhov): Drop this version in favor of the one below.
//! Executes given #callbacks, allowing up to #concurrencyLimit simultaneous invocations.
template <class T>
TFuture<std::vector<TErrorOr<T>>> RunWithBoundedConcurrency(
    std::vector<TCallback<TFuture<T>()>> callbacks,
    int concurrencyLimit);

//! Same as above but supports cancelation.
template <class T>
TFuture<std::vector<TErrorOr<T>>> CancelableRunWithBoundedConcurrency(
    std::vector<TCallback<TFuture<T>()>> callbacks,
    int concurrencyLimit,
    bool failOnError = false);

//! Same as above but cancels all futures if at least one task fails.
// (same as CancelableRunWithBoundedConcurrency with failOnError=true)
template <class T>
TFuture<std::vector<TErrorOr<T>>> RunWithAllSucceededBoundedConcurrency(
    std::vector<TCallback<TFuture<T>()>> callbacks,
    int concurrencyLimit);

////////////////////////////////////////////////////////////////////////////////

template <class T>
concept CFuture = NDetail::IsFuture<T>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

////////////////////////////////////////////////////////////////////////////////

//! Used for marking unused futures for easier search.
#define YT_UNUSED_FUTURE(var) Y_UNUSED(var)

////////////////////////////////////////////////////////////////////////////////

#define FUTURE_INL_H_
#include "future-inl.h"
#undef FUTURE_INL_H_
