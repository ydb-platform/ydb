#pragma once

#include "callback.h"
#include "callback_list.h"

namespace NYT {

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
    void Subscribe##name(const ::NYT::TCallback<TSignature>& callback) override \
    { \
        name##_.Subscribe(callback); \
    } \
    \
    void Unsubscribe##name(const ::NYT::TCallback<TSignature>& callback) override \
    { \
        name##_.Unsubscribe(callback); \
    } \
    static_assert(true)

#define DEFINE_SIGNAL_WITH_ACCESSOR_OVERRIDE(TSignature, name) \
    DEFINE_SIGNAL_OVERRIDE(TSignature, name); \
    ::NYT::TCallbackList<TSignature>* Get##name##Signal() override \
    { \
        return &name##_; \
    } \
    static_assert(true)

#define DECLARE_SIGNAL(TSignature, name) \
    void Subscribe##name(const ::NYT::TCallback<TSignature>& callback); \
    void Unsubscribe##name(const ::NYT::TCallback<TSignature>& callback)

#define DECLARE_SIGNAL_WITH_ACCESSOR(TSignature, name) \
    DECLARE_SIGNAL(TSignature, name); \
    ::NYT::TCallbackList<TSignature>* Get##name##Signal()

#define DECLARE_SIGNAL_OVERRIDE(TSignature, name) \
    void Subscribe##name(const ::NYT::TCallback<TSignature>& callback) override; \
    void Unsubscribe##name(const ::NYT::TCallback<TSignature>& callback) override

#define DECLARE_INTERFACE_SIGNAL(TSignature, name) \
    virtual void Subscribe##name(const ::NYT::TCallback<TSignature>& callback) = 0; \
    virtual void Unsubscribe##name(const ::NYT::TCallback<TSignature>& callback) = 0

#define DECLARE_INTERFACE_SIGNAL_WITH_ACCESSOR(TSignature, name) \
    DECLARE_INTERFACE_SIGNAL(TSignature, name); \
    virtual ::NYT::TCallbackList<TSignature>* Get##name##Signal() = 0

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
