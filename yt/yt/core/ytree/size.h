#pragma once

#include "public.h"

#include <yt/yt/core/yson/public.h>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

//! Semi-strong typedef for integral type with advanced parsing from string.
//! Constructor/FromString<TSize> parses "2M" as 2'000'000 and "1Ki" as 1024.
//! Supported suffixes: K, Ki, M, Mi, G, Gi, T, Ti, P, Pi, E, Ei.
class TSize
{
public:
    using TUnderlying = i64;

    constexpr TSize();

    constexpr explicit TSize(TUnderlying value);

    TSize(const TSize&) = default;
    TSize(TSize&&) = default;

    static TSize FromString(TStringBuf serializedValue);

    TSize& operator=(const TSize&) = default;
    TSize& operator=(TSize&&) = default;

    constexpr operator const TUnderlying&() const;
    constexpr operator TUnderlying&();

    constexpr auto operator<=>(const TSize& rhs) const = default;

    constexpr TUnderlying& Underlying() &;
    constexpr const TUnderlying& Underlying() const &;
    constexpr TUnderlying Underlying() &&;

private:
    TUnderlying Underlying_;
};

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TSize& value, NYson::IYsonConsumer* consumer);
void Deserialize(TSize& value, INodePtr node);
void Deserialize(TSize& value, NYson::TYsonPullParserCursor* cursor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree

#define YTREE_SIZE_INL_H_
#include "size-inl.h"
#undef YTREE_SIZE_INL_H_
