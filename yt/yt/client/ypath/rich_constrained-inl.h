#ifndef RICH_CONSTRAINED_INL_H_
#error "Direct inclusion of this file is not allowed, include rich_constrained.h"
// For the sake of sane code completion.
#include "rich_constrained.h"
#endif
#undef RICH_CONSTRAINED_INL_H_

#include <library/cpp/yt/error/error.h>

namespace NYT::NYPath {

////////////////////////////////////////////////////////////////////////////////

template <class... TValidator>
TConstrainedRichYPath<TValidator...>::TConstrainedRichYPath(TRichYPath path)
    : Path_(std::move(path))
{
    Validate();
}

template <class... TValidator>
void TConstrainedRichYPath<TValidator...>::Validate() const
{
    (TValidator()(Path_), ...);
}

template <class... TValidator>
constexpr TConstrainedRichYPath<TValidator...>::operator const TRichYPath&() const
{
    return Path_;
}

template <class... TValidator>
TConstrainedRichYPath<TValidator...>::operator TRichYPath() &&
{
    return std::move(Path_);
}

////////////////////////////////////////////////////////////////////////////////

template <const char... AttributeKey[]>
void TRequiredAttributesValidator<AttributeKey...>::operator()(const TRichYPath& path) const
{
    auto validateOne = [&] (const char* attributeName) {
        THROW_ERROR_EXCEPTION_IF(!path.Attributes().Contains(attributeName), "YPath %Qv does not have attribute %Qv", path, attributeName);
    };
    (validateOne(AttributeKey), ...);
}

////////////////////////////////////////////////////////////////////////////////

template <const char... AttributeKey[]>
void TWhitelistAttributesValidator<AttributeKey...>::operator()(const TRichYPath& path) const
{
    for (const auto& key : path.Attributes().ListKeys()) {
        if (((key == AttributeKey) || ...)) {
            continue;
        }

        THROW_ERROR_EXCEPTION("YPath %Qv has unexpected attribute %Qv", path, key);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYPath
