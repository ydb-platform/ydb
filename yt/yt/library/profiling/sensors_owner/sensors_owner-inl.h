#ifndef ALLOW_INCLUDE_SENSORS_OWNER_INL_H
    #error "Direct inclusion of this file is not allowed, must be included from sensors_owner.h only! Include sensors_owner.h"
#endif

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

namespace NSensorsOwnerPrivate {

////////////////////////////////////////////////////////////////////////////////

struct TTagSetKey
{
    TTagSet Tags;

    operator ui64() const;
    bool operator==(const TTagSetKey& key) const;
};

template <typename TMap>
struct TOwnedMapWrapper
{
    mutable TMap Map;

    TOwnedMapWrapper(const TProfiler&)
    { }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NSensorsOwnerPrivate

template <typename TChild, typename TFindKey, typename... TExtraConstructionArgs>
const TChild& TSensorsOwner::Get(const TFindKey& key, const TExtraConstructionArgs&... extraArgs) const
{
    constexpr bool childHasKey = NSensorsOwnerPrivate::TChildTraits<TChild>::HasKey;

    auto childConstructor = [&]<typename... TArgs>(TArgs&&... args) {
        return NSensorsOwnerPrivate::TChildTraits<TChild>::Create(State_->Profiler, std::forward<TArgs>(args)..., extraArgs...);
    };

    if constexpr (std::is_same_v<TFindKey, std::monostate>) {
        static_assert(!childHasKey);

        struct TWrapper : public TRefCounted
        {
            TChild Child;

            TWrapper(decltype(childConstructor)& childConstructor_)
                : Child(childConstructor_())
            { }
        };

        auto* wrapperPtr = State_->Children
            .FindOrInsert(
                std::type_index(typeid(TChild)),
                [&] {
                    return New<TWrapper>(childConstructor);
                })
            .first->Get();
        return static_cast<TWrapper*>(wrapperPtr)->Child;
    } else {
        using TChildKey = typename NSensorsOwnerPrivate::TChildTraits<TChild>::TKey;
        using TMap = NConcurrency::TSyncMap<TChildKey, TChild>;

        static_assert(childHasKey);
        static_assert(!std::is_same_v<TChildKey, TTagSet>, "Use GetWithTags() method");

        auto& childMap = Get<NSensorsOwnerPrivate::TOwnedMapWrapper<TMap>>().Map;

        return *childMap
            .FindOrInsert(
                key,
                [&] {
                    return childConstructor(TChildKey{key});
                })
            .first;
    }
}

template <typename TChild>
    requires(!NSensorsOwnerPrivate::TChildTraits<TChild>::HasKey)
const TChild& TSensorsOwner::GetWithTags(const TTagSet& tags) const
{
    struct TSensors
    {
        using TKey = NSensorsOwnerPrivate::TTagSetKey;

        TChild Child;

        TSensors(const TProfiler& profiler, const NSensorsOwnerPrivate::TTagSetKey& key)
            : Child{profiler.WithTags(key.Tags)}
        { }
    };

    return Get<TSensors>(NSensorsOwnerPrivate::TTagSetKey{tags}).Child;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
