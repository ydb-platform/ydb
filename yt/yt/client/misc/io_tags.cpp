#include "io_tags.h"

#include <yt/yt/core/ytree/helpers.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TString FormatIOTag(ERawIOTag tag)
{
    return FormatEnum(tag);
}

TString FormatIOTag(EAggregateIOTag tag)
{
    return FormatEnum(tag) + "@";
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
void AddTagToBaggage(const NYTree::IAttributeDictionaryPtr& baggage, T tag, const TStringBuf& value)
{
    baggage->Set(FormatIOTag(tag), value);
}

template void AddTagToBaggage<ERawIOTag>(const NYTree::IAttributeDictionaryPtr& baggage, ERawIOTag tag, const TStringBuf& value);
template void AddTagToBaggage<EAggregateIOTag>(const NYTree::IAttributeDictionaryPtr& baggage, EAggregateIOTag tag, const TStringBuf& value);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
