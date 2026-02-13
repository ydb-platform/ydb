#pragma once

#include "rich.h"

namespace NYT::NYPath {

////////////////////////////////////////////////////////////////////////////////

template <class... TValidator>
class TConstrainedRichYPath
{
public:
    TConstrainedRichYPath(TRichYPath path);

    constexpr operator const TRichYPath&() const;
    operator TRichYPath() &&;

    bool operator==(const TConstrainedRichYPath& other) const = default;

private:
    TRichYPath Path_;

    void Validate() const;
};

////////////////////////////////////////////////////////////////////////////////

template <const char... AttributeKey[]>
struct TRequiredAttributesValidator
{
    void operator()(const TRichYPath& path) const;
};

////////////////////////////////////////////////////////////////////////////////

template <const char... AttributeKey[]>
struct TWhitelistAttributesValidator
{
    void operator()(const TRichYPath& path) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYPath

#define RICH_CONSTRAINED_INL_H_
#include "rich_constrained-inl.h"
#undef RICH_CONSTRAINED_INL_H_
