#pragma once

#include "public.h"

#include <library/cpp/yt/memory/ref_counted.h>
#include <library/cpp/yt/memory/new.h>

#include <concepts>

namespace NYT::NPhoenix2::NDetail {

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TNonconstructableFactory
{
    static constexpr TConstructor Constructor = nullptr;
};

template <class T>
struct TSimpleFactory
{
    static constexpr TConstructor Constructor = [] () -> void* {
        return new T();
    };
};

template <class T>
struct TRefCountedFactory
{
    static constexpr TConstructor Constructor = [] () -> void* {
        return NYT::New<T>().Release();
    };
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
