#pragma once

// Public API is in sensors_owner.h

#include <library/cpp/yt/memory/new.h>

#include <util/generic/ptr.h>

namespace NYT::NProfiling::NSensorsOwnerPrivate {

////////////////////////////////////////////////////////////////////////////////

template <class TChild>
struct TChildTypeTraits
{
    using TValue = TChild;

    template <typename... TArgs>
    static TChild Create(TArgs&&... args)
    {
        return TChild{std::forward<TArgs>(args)...};
    }
};

template <class TValue_>
struct TChildTypeTraits<TIntrusivePtr<TValue_>>
{
    using TValue = TValue_;

    template <typename... TArgs>
    static TIntrusivePtr<TValue> Create(TArgs&&... args)
    {
        return New<TValue>(std::forward<TArgs>(args)...);
    }
};

template <typename TValue_, typename TPtr>
struct TChildPtrTypeTraits
{
    using TValue = TValue_;

    template <typename... TArgs>
    static TPtr Create(TArgs&&... args)
    {
        return TPtr{new TValue{std::forward<TArgs>(args)...}};
    }
};

template <class TValue>
struct TChildTypeTraits<TAtomicSharedPtr<TValue>>
    : public TChildPtrTypeTraits<TValue, TAtomicSharedPtr<TValue>>
{ };

template <class TValue>
struct TChildTypeTraits<std::shared_ptr<TValue>>
    : public TChildPtrTypeTraits<TValue, std::shared_ptr<TValue>>
{ };

template <class TChild>
concept CHasKeyField = requires(TChild c) {
    c.Key;
};
template <class TChild>
concept CHasKeyAlias = requires {
    typename TChild::TKey;
};

template <class TValue>
struct TKeyTraits
{
    static constexpr bool HasKey = false;
};

template <CHasKeyField TValue>
struct TKeyTraits<TValue>
{
    using TKey = std::decay_t<decltype(std::declval<TValue>().Key)>;

    static constexpr bool HasKey = true;
};

template <CHasKeyAlias TValue>
struct TKeyTraits<TValue>
{
    using TKey = typename TValue::TKey;

    static constexpr bool HasKey = true;
};

template <class TChild>
struct TChildTraits
    : public TChildTypeTraits<TChild>
    , public TKeyTraits<typename TChildTypeTraits<TChild>::TValue>
{
    static_assert(std::is_same_v<TChild, std::decay_t<TChild>>);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling::NSensorsOwnerPrivate
