#include "io_tags.h"

#include <yt/yt/core/ytree/helpers.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

std::string FormatIOTag(ERawIOTag tag)
{
    return FormatEnum(tag);
}

std::string FormatIOTag(EAggregateIOTag tag)
{
    return FormatEnum(tag) + "@";
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
void AddTagToBaggage(const NYTree::IAttributeDictionaryPtr& baggage, T tag, TStringBuf value)
{
    baggage->Set(FormatIOTag(tag), value);
}

template void AddTagToBaggage<ERawIOTag>(const NYTree::IAttributeDictionaryPtr& baggage, ERawIOTag tag, TStringBuf value);
template void AddTagToBaggage<EAggregateIOTag>(const NYTree::IAttributeDictionaryPtr& baggage, EAggregateIOTag tag, TStringBuf value);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
