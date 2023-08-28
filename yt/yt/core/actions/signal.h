#pragma once

#include "callback.h"

#include <library/cpp/yt/small_containers/compact_vector.h>

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
    bool Empty() const;

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

#define DEFINE_SIGNAL(TSignature, name) \
protected: \
    ::NYT::TCallbackList<TSignature> name##_; \
public: \
    void Subscribe##name(const ::NYT::TCallback<TSignature>& callback) \
    { \
        name##_.Subscribe(callback); \
    } \
    \
    void Unsubscribe##name(const ::NYT::TCallback<TSignature>& callback) \
    { \
        name##_.Unsubscribe(callback); \
    } \
    static_assert(true)

#define DEFINE_SIGNAL_SIMPLE(TSignature, name) \
protected: \
    ::NYT::TSimpleCallbackList<TSignature> name##_; \
public: \
    void Subscribe##name(const ::NYT::TCallback<TSignature>& callback) \
    { \
        name##_.Subscribe(callback); \
    } \
    \
    void Unsubscribe##name(const ::NYT::TCallback<TSignature>& callback) \
    { \
        name##_.Unsubscribe(callback); \
    } \
    static_assert(true)

#define DEFINE_SIGNAL_WITH_ACCESSOR(TSignature, name) \
    DEFINE_SIGNAL(TSignature, name); \
public: \
    ::NYT::TCallbackList<TSignature>* Get##name##Signal() \
    { \
        return &name##_; \
    } \
    static_assert(true)

#define DEFINE_SIGNAL_OVERRIDE(TSignature, name) \
protected: \
    ::NYT::TCallbackList<TSignature> name##_; \
public: \
    virtual void Subscribe##name(const ::NYT::TCallback<TSignature>& callback) override \
    { \
        name##_.Subscribe(callback); \
    } \
    \
    virtual void Unsubscribe##name(const ::NYT::TCallback<TSignature>& callback) override \
    { \
        name##_.Unsubscribe(callback); \
    } \
    static_assert(true)

#define DECLARE_SIGNAL(TSignature, name) \
    void Subscribe##name(const ::NYT::TCallback<TSignature>& callback); \
    void Unsubscribe##name(const ::NYT::TCallback<TSignature>& callback)

#define DECLARE_SIGNAL_WITH_ACCESSOR(TSignature, name) \
    DECLARE_SIGNAL(TSignature, name); \
    ::NYT::TCallbackList<TSignature>* Get##name##Signal()

#define DECLARE_SIGNAL_OVERRIDE(TSignature, name) \
    virtual void Subscribe##name(const ::NYT::TCallback<TSignature>& callback) override; \
    virtual void Unsubscribe##name(const ::NYT::TCallback<TSignature>& callback) override

#define DECLARE_INTERFACE_SIGNAL(TSignature, name) \
    virtual void Subscribe##name(const ::NYT::TCallback<TSignature>& callback) = 0; \
    virtual void Unsubscribe##name(const ::NYT::TCallback<TSignature>& callback) = 0

#define DELEGATE_SIGNAL_WITH_RENAME(declaringType, TSignature, name, delegateTo, delegateName) \
    void declaringType::Subscribe##name(const ::NYT::TCallback<TSignature>& callback) \
    { \
        (delegateTo).Subscribe##delegateName(callback); \
    } \
    \
    void declaringType::Unsubscribe##name(const ::NYT::TCallback<TSignature>& callback) \
    { \
        (delegateTo).Unsubscribe##delegateName(callback); \
    } \
    static_assert(true)

#define DELEGATE_SIGNAL(declaringType, TSignature, name, delegateTo) \
    DELEGATE_SIGNAL_WITH_RENAME(declaringType, TSignature, name, delegateTo, name)

#define DELEGATE_SIGNAL_WITH_ACCESSOR(declaringType, TSignature, name, delegateTo) \
    DELEGATE_SIGNAL(declaringType, TSignature, name, delegateTo); \
    ::NYT::TCallbackList<TSignature>* declaringType::Get##name##Signal() \
    { \
        return (delegateTo).Get##name##Signal(); \
    } \
    static_assert(true)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define SIGNAL_INL_H_
#include "signal-inl.h"
#undef SIGNAL_INL_H_
