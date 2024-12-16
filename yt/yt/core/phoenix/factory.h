#pragma once

#include "public.h"
#include "polymorphic.h"

#include <library/cpp/yt/memory/ref_counted.h>
#include <library/cpp/yt/memory/new.h>

#include <concepts>

namespace NYT::NPhoenix2::NDetail {

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TNonconstructableFactory
{
    static constexpr TPolymorphicConstructor PolymorphicConstructor = nullptr;
    static constexpr TConcreteConstructor ConcreteConstructor = nullptr;
};

template <class T, class TCrtpFactory>
struct TConstructableFactoryBase
{
    static constexpr TPolymorphicConstructor PolymorphicConstructor = [] () -> TPolymorphicBase* {
        if constexpr(TPolymorphicTraits<T>::Polymorphic) {
            return TCrtpFactory::Construct();
        } else {
            return nullptr;
        }
    };

    static constexpr TConcreteConstructor ConcreteConstructor = [] () -> void* {
        return TCrtpFactory::Construct();
    };
};

template <class T>
struct TSimpleFactory
    : public TConstructableFactoryBase<T, TSimpleFactory<T>>
{
    static T* Construct()
    {
        return new T();
    }
};

template <class T>
struct TRefCountedFactory
    : public TConstructableFactoryBase<T, TRefCountedFactory<T>>
{
    static T* Construct()
    {
        return NYT::New<T>().Release();
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TFactoryTraits
{
    using TFactory = TNonconstructableFactory<T>;
};

template <class T>
    requires (!std::derived_from<T, TRefCounted>) && (requires { T(); })
struct TFactoryTraits<T>
{
    using TFactory = TSimpleFactory<T>;
};

template <class T>
struct TRefCountedWrapperMock
    : public T
{
    using T::T;

    void DestroyRefCounted() override
    { }
};

template <class T>
    requires std::derived_from<T, TRefCounted> && (requires { TRefCountedWrapperMock<T>(); })
struct TFactoryTraits<T>
{
    using TFactory = TRefCountedFactory<T>;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPhoenix2::NDetail
