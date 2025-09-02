#include "attribute_consumer.h"
#include "attributes.h"

#include <yt/yt/core/misc/error.h>

namespace NYT::NYTree {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TAttributeConsumer::TAttributeConsumer(IAttributeDictionary* attributes, std::optional<THashSet<TString>> keyWhitelist)
    : Attributes_(attributes)
    , KeyWhitelist_(std::move(keyWhitelist))
{ }

IAttributeDictionary* TAttributeConsumer::GetAttributes() const
{
    return Attributes_;
}

void TAttributeConsumer::OnMyKeyedItem(TStringBuf key)
{
    Writer_.reset(new TBufferedBinaryYsonWriter(&Output_));
    Forward(Writer_.get(), [this, key = TString(key)] {
        Writer_->Flush();
        Writer_.reset();
        if (!KeyWhitelist_ || KeyWhitelist_->contains(key)) {
            Attributes_->SetYson(key, TYsonString(Output_.Str()));
        }
        Output_.clear();
    });
}

void TAttributeConsumer::OnMyBeginMap()
{ }

void TAttributeConsumer::OnMyEndMap()
{ }

void TAttributeConsumer::OnMyBeginAttributes()
{ }

void TAttributeConsumer::OnMyEndAttributes()
{ }

void TAttributeConsumer::OnMyStringScalar(TStringBuf /*value*/)
{
    ThrowMapExpected();
}

void TAttributeConsumer::OnMyInt64Scalar(i64 /*value*/)
{
    ThrowMapExpected();
}

void TAttributeConsumer::OnMyUint64Scalar(ui64 /*value*/)
{
    ThrowMapExpected();
}

void TAttributeConsumer::OnMyDoubleScalar(double /*value*/)
{
    ThrowMapExpected();
}

void TAttributeConsumer::OnMyBooleanScalar(bool /*value*/)
{
    ThrowMapExpected();
}

void TAttributeConsumer::OnMyEntity()
{
    ThrowMapExpected();
}

void TAttributeConsumer::OnMyBeginList()
{
    ThrowMapExpected();
}

void TAttributeConsumer::ThrowMapExpected()
{
    THROW_ERROR_EXCEPTION("Attributes can only be set from a map");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
