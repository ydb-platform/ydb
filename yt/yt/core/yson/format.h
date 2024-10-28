#pragma once

#include "token.h"

#include <yt/yt/core/ytree/serialize.h>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

//! Indicates the beginning of a list.
const ETokenType BeginListToken = ETokenType::LeftBracket;
//! Indicates the end of a list.
const ETokenType EndListToken = ETokenType::RightBracket;

//! Indicates the beginning of a map.
const ETokenType BeginMapToken = ETokenType::LeftBrace;
//! Indicates the end of a map.
const ETokenType EndMapToken = ETokenType::RightBrace;

//! Indicates the beginning of an attribute map.
const ETokenType BeginAttributesToken = ETokenType::LeftAngle;
//! Indicates the end of an attribute map.
const ETokenType EndAttributesToken = ETokenType::RightAngle;

//! Separates items in maps and lists.
const ETokenType ItemSeparatorToken = ETokenType::Semicolon;
//! Separates keys from values in maps.
const ETokenType KeyValueSeparatorToken = ETokenType::Equals;

//! Indicates an entity.
const ETokenType EntityToken = ETokenType::Hash;

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TYsonFormatTraits
{
    static constexpr bool UseYsonFormatter = false;
};

struct TYsonTextFormatTraits
{
    static constexpr bool UseYsonFormatter = true;
    static constexpr EYsonFormat YsonFormat = EYsonFormat::Text;
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
concept CYsonFormattable =
    NYTree::CYsonSerializable<T> &&
    TYsonFormatTraits<T>::UseYsonFormatter &&
    requires {
        { TYsonFormatTraits<T>::YsonFormat } -> std::same_as<const EYsonFormat&>;
    };

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson

#define FORMAT_INL_H_
#include "format-inl.h"
#undef FORMAT_INL_H_
