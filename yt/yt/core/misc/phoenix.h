#pragma once

#include "id_generator.h"
#include "mpl.h"
#include "serialize.h"

#include <yt/yt/core/actions/callback.h>

#include <yt/yt/core/phoenix/context.h>
#include <yt/yt/core/phoenix/polymorphic.h>
#include <yt/yt/core/phoenix/type_decl.h>
#include <yt/yt/core/phoenix/type_def.h>

#include <typeinfo>

namespace NYT::NPhoenix {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 InlineObjectIdMask = 0x80000000;
constexpr ui32 NullObjectId       = 0x00000000;

////////////////////////////////////////////////////////////////////////////////

using NPhoenix2::NDetail::TSerializer;
using NPhoenix2::TLoadContext;
using NPhoenix2::TSaveContext;

using TDynamicTag = NPhoenix2::TPolymorphicBase;

////////////////////////////////////////////////////////////////////////////////

template <class TFactory>
struct TFactoryTag
{ };

////////////////////////////////////////////////////////////////////////////////

template <class T, class = void>
struct TPolymorphicTraits
{
    static const bool Dynamic = false;
    using TBase = T;
};

template <class T>
struct TPolymorphicTraits<
    T,
    typename std::enable_if_t<
        std::is_convertible_v<T&, TDynamicTag&>
    >
>
{
    static const bool Dynamic = true;
    using TBase = TDynamicTag;
};

////////////////////////////////////////////////////////////////////////////////

struct TNullFactory
{
    template <class T>
    static T* Instantiate()
    {
        YT_ABORT();
    }
};

struct TSimpleFactory
{
    template <class T>
    static T* Instantiate()
    {
        return new T();
    }
};

struct TRefCountedFactory
{
    template <class T>
    static T* Instantiate()
    {
        return New<T>().Release();
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class T, class = void>
struct TFactoryTraits
{
    using TFactory = TSimpleFactory;
};

template <class T>
struct TFactoryTraits<
    T,
    typename std::enable_if_t<
        std::conjunction_v<
            std::is_convertible<T*, NYT::TRefCountedBase*>,
            std::negation<std::is_convertible<T*, TFactoryTag<TNullFactory>*>>
        >
    >
>
{
    using TFactory = TRefCountedFactory;
};

template <class T>
struct TFactoryTraits<
    T,
    typename std::enable_if_t<
        std::is_convertible_v<T*, TFactoryTag<TNullFactory>*>
    >
>
{
    using TFactory = TNullFactory;
};

////////////////////////////////////////////////////////////////////////////////

template <class T, class = void>
struct TIdClass
{
    using TType = T;
};

template <class T>
struct TIdClass<
    T,
    typename std::enable_if_t<
        std::is_convertible_v<T*, NYT::TRefCountedBase*>
    >
>
{
    using TType = NYT::TRefCountedWrapper<T>;
};


////////////////////////////////////////////////////////////////////////////////

class TProfiler
{
public:
    static TProfiler* Get();

    ui32 GetTag(const std::type_info& typeInfo);

    template <class T>
    T* Instantiate(ui32 tag);

    template <class T>
    void Register(ui32 tag);

private:
    struct TEntry
    {
        const std::type_info* TypeInfo;
        ui32 Tag;
        std::function<void*()> Factory;
    };

    THashMap<const std::type_info*, TEntry*> TypeInfoToEntry_;
    THashMap<ui32, TEntry> TagToEntry_;

    TProfiler();

    const TEntry& GetEntry(ui32 tag);
    const TEntry& GetEntry(const std::type_info& typeInfo);

    template <class T>
    static void* DoInstantiate();

    Y_DECLARE_SINGLETON_FRIEND()
};

////////////////////////////////////////////////////////////////////////////////

template <
    class TType,
    ui32 tag,
    class TFactory = typename TFactoryTraits<TType>::TFactory
>
struct TDynamicInitializer
{
    TDynamicInitializer()
    {
        TProfiler::Get()->Register<TType>(tag);
    }
};

template <
    class TType,
    ui32 tag
>
struct TDynamicInitializer<TType, tag, TRefCountedFactory>
{
    TDynamicInitializer()
    {
        TProfiler::Get()->Register<TType>(tag);
    }
};

#define DECLARE_DYNAMIC_PHOENIX_TYPE(...)                             \
    static ::NYT::NPhoenix::TDynamicInitializer<__VA_ARGS__>          \
        DynamicPhoenixInitializer;                                    \
    PHOENIX_DECLARE_OPAQUE_TYPE(__VA_ARGS__)

// __VA_ARGS__ are used because sometimes we want a template type
// to be an argument but the single macro argument may not contain
// commas. Dat preprocessor :/
#define DEFINE_DYNAMIC_PHOENIX_TYPE(...)                              \
    decltype(__VA_ARGS__::DynamicPhoenixInitializer)                  \
        __VA_ARGS__::DynamicPhoenixInitializer;                       \
    PHOENIX_DEFINE_OPAQUE_TYPE(__VA_ARGS__)

#define INHERIT_DYNAMIC_PHOENIX_TYPE(baseType, type, tag)             \
class type                                                            \
    : public baseType                                                 \
{                                                                     \
public:                                                               \
    using baseType::baseType;                                         \
                                                                      \
private:                                                              \
    DECLARE_DYNAMIC_PHOENIX_TYPE(type, tag);                          \
}

#define INHERIT_DYNAMIC_PHOENIX_TYPE_TEMPLATED(baseType, type, tag, ...) \
class type                                                               \
    : public baseType<__VA_ARGS__>                                       \
{                                                                        \
public:                                                                  \
    using baseType::baseType;                                            \
                                                                         \
private:                                                                 \
    DECLARE_DYNAMIC_PHOENIX_TYPE(type, tag);                             \
}

////////////////////////////////////////////////////////////////////////////////

template <class C>
struct ICustomPersistent
    : public virtual TDynamicTag
{
    virtual ~ICustomPersistent() = default;
    virtual void Persist(const C& context) = 0;
};

using TPersistenceContext = TCustomPersistenceContext<TSaveContext, TLoadContext>;
using IPersistent = ICustomPersistent<TPersistenceContext>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPhoenix

#define PHOENIX_INL_H_
#include "phoenix-inl.h"
#undef PHOENIX_INL_H_
