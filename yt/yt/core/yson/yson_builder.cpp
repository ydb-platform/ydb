#include "yson_builder.h"

#include "writer.h"

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

IYsonConsumer* IYsonBuilder::operator->()
{
    return GetConsumer();
}

////////////////////////////////////////////////////////////////////////////////

TYsonStringBuilder::TYsonStringBuilder(EYsonFormat format, EYsonType type, bool enableRaw)
    : Output_(ValueString_)
    , Writer_(CreateYsonWriter(&Output_, format, type, enableRaw))
{ }

IYsonConsumer* TYsonStringBuilder::GetConsumer()
{
    return Writer_.get();
}

IYsonBuilder::TCheckpoint TYsonStringBuilder::CreateCheckpoint()
{
    Writer_->Flush();
    return IYsonBuilder::TCheckpoint(std::ssize(ValueString_));
}

void TYsonStringBuilder::RestoreCheckpoint(IYsonBuilder::TCheckpoint checkpoint)
{
    Writer_->Flush();
    int checkpointSize = checkpoint.Underlying();
    YT_VERIFY(checkpointSize >= 0 && checkpointSize <= std::ssize(ValueString_));
    ValueString_.resize(static_cast<size_t>(checkpointSize));
}

TYsonString TYsonStringBuilder::GetYsonString() const
{
    Writer_->Flush();
    return TYsonString(ValueString_);
}

TYsonString TYsonStringBuilder::Flush()
{
    auto result = GetYsonString();
    ValueString_.clear();
    return result;
}

bool TYsonStringBuilder::IsEmpty()
{
    Writer_->Flush();
    return ValueString_.empty();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
