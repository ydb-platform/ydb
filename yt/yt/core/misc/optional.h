#pragma once

#include <util/string/cast.h>

#include <optional>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TOptionalTraits
{
    using TOptional = std::optional<T>;
    using TValue = T;
};

template <class T>
struct TOptionalTraits<std::optional<T>>
{
    using TOptional = std::optional<T>;
    using TValue = T;
};

template <class T>
struct TOptionalTraits<T*>
{
    using TOptional = T*;
    using TValue = T*;
};

template <class T>
struct TOptionalTraits<TIntrusivePtr<T>>
{
    using TOptional = TIntrusivePtr<T>;
    using TValue = TIntrusivePtr<T>;
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TStdOptionalTraits
{
    static constexpr bool IsStdOptional = false;
    using TValueType = T;
};

template <class T>
struct TStdOptionalTraits<std::optional<T>>
{
    static constexpr bool IsStdOptional = true;
    using TValueType = T;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

template <class T>
TString ToString(const std::optional<T>& nullable)
{
    return nullable ? ToString(*nullable) : "<Null>";
}

template <class T>
struct THash<std::optional<T>>
{
    size_t operator()(const std::optional<T>& nullable) const
    {
        return nullable ? THash<T>()(*nullable) : 0;
    }
};
