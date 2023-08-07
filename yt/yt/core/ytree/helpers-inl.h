#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
// For the sake of sane code completion.
#include "helpers.h"
#endif

#include "attribute_consumer.h"
#include "serialize.h"
#include "convert.h"

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

template <class T>
T IAttributeDictionary::Get(TStringBuf key) const
{
    auto yson = GetYson(key);
    try {
        return ConvertTo<T>(yson);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error parsing attribute %Qv",
            key)
            << ex;
    }
}

template <class T>
T IAttributeDictionary::GetAndRemove(const TString& key)
{
    auto result = Get<T>(key);
    Remove(key);
    return result;
}

template <class T>
T IAttributeDictionary::Get(TStringBuf key, const T& defaultValue) const
{
    return Find<T>(key).value_or(defaultValue);
}

template <class T>
T IAttributeDictionary::GetAndRemove(const TString& key, const T& defaultValue)
{
    auto result = Find<T>(key);
    if (result) {
        Remove(key);
        return *result;
    } else {
        return defaultValue;
    }
}

template <class T>
typename TOptionalTraits<T>::TOptional IAttributeDictionary::Find(TStringBuf key) const
{
    auto yson = FindYson(key);
    if (!yson) {
        return typename TOptionalTraits<T>::TOptional();
    }
    try {
        return ConvertTo<T>(yson);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error parsing attribute %Qv",
            key)
            << ex;
    }
}

template <class T>
typename TOptionalTraits<T>::TOptional IAttributeDictionary::FindAndRemove(const TString& key)
{
    auto result = Find<T>(key);
    if (result) {
        Remove(key);
    }
    return result;
}

template <class T>
void IAttributeDictionary::Set(const TString& key, const T& value)
{
    auto yson = ConvertToYsonString(value, NYson::EYsonFormat::Binary);
    SetYson(key, yson);
}

////////////////////////////////////////////////////////////////////////////////

template <class T, class R>
IYPathServicePtr IYPathService::FromMethod(
    R (std::remove_cv_t<T>::*method) () const,
    const TWeakPtr<T>& weak)
{
    auto boundProducer = NYson::TYsonProducer(BIND_NO_PROPAGATE([=] (NYson::IYsonConsumer* consumer) {
        auto strong = weak.Lock();
        if (strong) {
            Serialize((strong.Get()->*method)(), consumer);
        } else {
            consumer->OnBeginAttributes();
            consumer->OnKeyedItem("object_destroyed");
            consumer->OnBooleanScalar(true);
            consumer->OnEndAttributes();
            consumer->OnEntity();
        }
    }));

    return FromProducer(std::move(boundProducer));
}

template <class T>
IYPathServicePtr IYPathService::FromMethod(
    void (std::remove_cv_t<T>::*producer) (NYson::IYsonConsumer*) const,
    const TWeakPtr<T>& weak)
{
    auto boundProducer = NYson::TYsonProducer(BIND_NO_PROPAGATE([=] (NYson::IYsonConsumer* consumer) {
        auto strong = weak.Lock();
        if (strong) {
            (strong.Get()->*producer)(consumer);
        } else {
            consumer->OnBeginAttributes();
            consumer->OnKeyedItem("object_destroyed");
            consumer->OnBooleanScalar(true);
            consumer->OnEndAttributes();
            consumer->OnEntity();
        }
    }));

    return FromProducer(std::move(boundProducer));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
