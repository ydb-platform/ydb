#include "format.h"

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/table_consumer.h>

#include <yt/yt/library/skiff_ext/schema_match.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/yson/writer.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/yson/forwarding_consumer.h>

#include <yt/yt/core/json/json_parser.h>
#include <yt/yt/core/json/json_writer.h>

namespace NYT::NFormats {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TFormat::TFormat()
    : Type_(EFormatType::Null)
    , Attributes_(CreateEphemeralAttributes())
{ }

TFormat::TFormat(EFormatType type, const IAttributeDictionary* attributes)
    : Type_(type)
    , Attributes_(attributes ? attributes->Clone() : CreateEphemeralAttributes())
{ }

TFormat::TFormat(const TFormat& other)
    : Type_(other.Type_)
    , Attributes_(other.Attributes_->Clone())
{ }

TFormat& TFormat::operator=(const TFormat& other)
{
    if (this != &other) {
        Type_ = other.Type_;
        Attributes_ = other.Attributes_ ? other.Attributes_->Clone() : nullptr;
    }
    return *this;
}

const IAttributeDictionary& TFormat::Attributes() const
{
    return *Attributes_;
}

void Serialize(const TFormat& value, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginAttributes()
            .Items(value.Attributes())
        .EndAttributes()
        .Value(value.GetType());
}

void Deserialize(TFormat& value, INodePtr node)
{
    if (node->GetType() != ENodeType::String) {
        THROW_ERROR_EXCEPTION("Format name must be a string");
    }

    auto typeStr = node->GetValue<TString>();
    EFormatType type;
    try {
        type = ParseEnum<EFormatType>(typeStr);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Invalid format name %Qv",
            typeStr);
    }

    value = TFormat(type, &node->Attributes());
}

void Deserialize(TFormat& value, NYson::TYsonPullParserCursor* cursor)
{
    Deserialize(value, ExtractTo<INodePtr>(cursor));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
