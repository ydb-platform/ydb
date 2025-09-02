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

TYsonString TYsonStringBuilder::Flush()
{
    Writer_->Flush();
    auto result = TYsonString(ValueString_);
    ValueString_.clear();
    return result;
}

bool TYsonStringBuilder::IsEmpty()
{
    Writer_->Flush();
    return ValueString_.empty();
}

////////////////////////////////////////////////////////////////////////////////

TYsonBuilder::TYsonBuilder(
    EYsonBuilderForwardingPolicy policy,
    IYsonBuilder* underlying,
    IYsonConsumer* consumer)
    : Policy_(policy)
    , Underlying_(underlying)
    , Consumer_(consumer)
{ }

IYsonConsumer* TYsonBuilder::GetConsumer()
{
    return Consumer_;
}

IYsonBuilder::TCheckpoint TYsonBuilder::CreateCheckpoint()
{
    switch (Policy_) {
        case EYsonBuilderForwardingPolicy::Forward:
            return Underlying_->CreateCheckpoint();
        case EYsonBuilderForwardingPolicy::Crash:
            return IYsonBuilder::TCheckpoint{};
        case EYsonBuilderForwardingPolicy::Ignore:
            return IYsonBuilder::TCheckpoint{};
    }
}

void TYsonBuilder::RestoreCheckpoint(TCheckpoint checkpoint)
{
    switch (Policy_) {
        case EYsonBuilderForwardingPolicy::Forward:
            Underlying_->RestoreCheckpoint(checkpoint);
            break;
        case EYsonBuilderForwardingPolicy::Crash:
            YT_ABORT();
        case EYsonBuilderForwardingPolicy::Ignore:
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
