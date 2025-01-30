#pragma once

#include "callback.h"

#include <library/cpp/yt/compact_containers/compact_vector.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

/*!
 *  A client may subscribe to a list (adding a new handler to it),
 *  unsubscribe from it (removing an earlier added handler),
 *  and fire it thus invoking the callbacks added so far.
 *
 *  Thread affinity: any.
 */
template <class TSignature>
class TCallbackList
{ };

template <class TResult, class... TArgs>
class TCallbackList<TResult(TArgs...)>
{
public:
    using TCallback = NYT::TCallback<TResult(TArgs...)>;

    //! Adds a new handler to the list.
    /*!
     * \param callback A handler to be added.
     */
    void Subscribe(const TCallback& callback);

    //! Removes a handler from the list.
    /*!
     * \param callback A handler to be removed.
     */
    void Unsubscribe(const TCallback& callback);

    //! Returns the vector of currently added callbacks.
    std::vector<TCallback> ToVector() const;

    //! Returns the number of handlers.
    int Size() const;

    //! Returns |true| if there are no handlers.
    bool IsEmpty() const;

    //! Clears the list of handlers.
    void Clear();

    //! Runs all handlers in the list.
    //! The return values (if any) are ignored.
    template <class... TCallArgs>
    void Fire(TCallArgs&&... args) const;

    //! Runs all handlers in the list and clears the list.
    //! The return values (if any) are ignored.
    template <class... TCallArgs>
    void FireAndClear(TCallArgs&&... args);

private:
    std::atomic<bool> Empty_ = true;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SpinLock_);
    using TCallbackVector = TCompactVector<TCallback, 4>;
    TCallbackVector Callbacks_;
};

////////////////////////////////////////////////////////////////////////////////

/*!
 *  Similar to TCallbackList, but single-threaded and copyable.
 *
 *  Cannot be used from multiple threads.
 */
template <class TSignature>
class TSimpleCallbackList
{ };

template <class TResult, class... TArgs>
class TSimpleCallbackList<TResult(TArgs...)>
{
public:
    using TCallback = NYT::TCallback<TResult(TArgs...)>;

    //! Adds a new handler to the list.
    /*!
     * \param callback A handler to be added.
     */
    void Subscribe(const TCallback& callback);

    //! Removes a handler from the list.
    /*!
     * \param callback A handler to be removed.
     */
    void Unsubscribe(const TCallback& callback);

    //! Runs all handlers in the list.
    //! The return values (if any) are ignored.
    template <class... TCallArgs>
    void Fire(TCallArgs&&... args) const;

private:
    using TCallbackVector = TCompactVector<TCallback, 4>;
    TCallbackVector Callbacks_;
};

////////////////////////////////////////////////////////////////////////////////

/*!
 *  Similar to TCallbackList but can only be fired once.
 *  When fired, captures the arguments and in subsequent calls
 *  to Subscribe instantly invokes the subscribers.
 *
 *  Thread affinity: any.
 */
template <class TSignature>
class TSingleShotCallbackList
{ };

template <class TResult, class... TArgs>
class TSingleShotCallbackList<TResult(TArgs...)>
{
public:
    using TCallback = NYT::TCallback<TResult(TArgs...)>;

    //! Adds a new handler to the list.
    /*!
     *  If the list was already fired then #callback is invoked in situ.
     *  \param callback A handler to be added.
     */
    void Subscribe(const TCallback& callback);

    //! Tries to add a new handler to the list.
    /*!
     *  If the list was already fired then returns |false|.
     *  Otherwise atomically installs the handler.
     *  \param callback A handler to be added.
     */
    bool TrySubscribe(const TCallback& callback);

    //! Removes a handler from the list.
    /*!
     *  \param callback A handler to be removed.
     */
    void Unsubscribe(const TCallback& callback);

    //! Returns the vector of currently added callbacks.
    std::vector<TCallback> ToVector() const;

    //! Runs all handlers in the list.
    //! The return values (if any) are ignored.
    /*!
     *  \returns |true| if this is the first attempt to fire the list.
     */
    template <class... TCallArgs>
    bool Fire(TCallArgs&&... args);

    //! \returns |true| if the list was fired.
    bool IsFired() const;

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SpinLock_);
    std::atomic<bool> Fired_ = false;
    using TCallbackVector = TCompactVector<TCallback, 4>;
    TCallbackVector Callbacks_;
    std::tuple<typename std::decay<TArgs>::type...> Args_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define CALLBACK_LIST_INL_H_
#include "callback_list-inl.h"
#undef CALLBACK_LIST_INL_H_
