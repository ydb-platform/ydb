#pragma once

#include "public.h"

#include <yt/yt/client/table_client/public.h>
#include <yt/yt/client/table_client/unversioned_writer.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/misc/property.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/attributes.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

class TFormat
{
public:
    TFormat();
    TFormat(const TFormat& other);
    TFormat(EFormatType type, const NYTree::IAttributeDictionary* attributes = nullptr);

    TFormat& operator = (const TFormat& other);

    DEFINE_BYVAL_RO_PROPERTY(EFormatType, Type);

    const NYTree::IAttributeDictionary& Attributes() const;

private:
    NYTree::IAttributeDictionaryPtr Attributes_;

};

void Serialize(const TFormat& value, NYson::IYsonConsumer* consumer);
void Deserialize(TFormat& value, NYTree::INodePtr node);
void Deserialize(TFormat& value, NYson::TYsonPullParserCursor* cursor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
